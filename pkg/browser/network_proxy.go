package browser

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

const browserProxyMaxResolvedAddresses = 32

type browserProxyLookup func(context.Context, string, string) ([]net.IP, error)

type browserProxyDial func(context.Context, string, string) (net.Conn, error)

type browserNetworkPolicy struct {
	mode           string
	allowedOrigins map[string]struct{}
	lookupIP       browserProxyLookup
	dial           browserProxyDial
}

type browserNetworkProxy struct {
	listener net.Listener
	server   *http.Server
	policy   *browserNetworkPolicy

	denials atomic.Uint64
	done    chan struct{}

	mu          sync.Mutex
	serveErr    error
	closing     bool
	connections map[net.Conn]struct{}
	closeOnce   sync.Once
}

func startBrowserNetworkProxy(
	profile config.BrowserProfileConfig,
	lookupIP browserProxyLookup,
	dial browserProxyDial,
) (*browserNetworkProxy, error) {
	policy, err := newBrowserNetworkPolicy(profile, lookupIP, dial)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, ErrWorkerUnavailable
	}
	proxy := &browserNetworkProxy{
		listener:    listener,
		policy:      policy,
		done:        make(chan struct{}),
		connections: make(map[net.Conn]struct{}),
	}
	proxy.server = &http.Server{
		Handler:           proxy,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go proxy.serve()
	return proxy, nil
}

func newBrowserNetworkPolicy(
	profile config.BrowserProfileConfig,
	lookupIP browserProxyLookup,
	dial browserProxyDial,
) (*browserNetworkPolicy, error) {
	mode := profile.EffectiveNetworkMode()
	if mode != config.BrowserNetworkExactOrigins && mode != config.BrowserNetworkPublicWeb &&
		mode != config.BrowserNetworkAnyHTTP {
		return nil, ErrDenied
	}
	if lookupIP == nil {
		lookupIP = net.DefaultResolver.LookupIP
	}
	if dial == nil {
		dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
		dial = dialer.DialContext
	}
	allowed := make(map[string]struct{}, len(profile.AllowedOrigins))
	for _, rawOrigin := range profile.AllowedOrigins {
		origin, err := config.NormalizeBrowserOrigin(rawOrigin)
		if err != nil {
			return nil, ErrDenied
		}
		allowed[origin] = struct{}{}
	}
	if mode == config.BrowserNetworkExactOrigins && len(allowed) == 0 {
		return nil, ErrDenied
	}
	if (mode == config.BrowserNetworkPublicWeb || mode == config.BrowserNetworkAnyHTTP) &&
		len(allowed) != 0 {
		return nil, ErrDenied
	}
	return &browserNetworkPolicy{
		mode:           mode,
		allowedOrigins: allowed,
		lookupIP:       lookupIP,
		dial:           dial,
	}, nil
}

func (proxy *browserNetworkProxy) serve() {
	err := proxy.server.Serve(proxy.listener)
	proxy.mu.Lock()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		proxy.serveErr = err
	}
	proxy.mu.Unlock()
	close(proxy.done)
}

func (proxy *browserNetworkProxy) URL() string {
	if proxy == nil || proxy.listener == nil {
		return ""
	}
	return "http://" + proxy.listener.Addr().String()
}

func (proxy *browserNetworkProxy) Available() bool {
	if proxy == nil {
		return false
	}
	select {
	case <-proxy.done:
		return false
	default:
		return true
	}
}

func (proxy *browserNetworkProxy) Denials() uint64 {
	if proxy == nil {
		return 0
	}
	return proxy.denials.Load()
}

func (proxy *browserNetworkProxy) Close() error {
	if proxy == nil {
		return nil
	}
	proxy.closeOnce.Do(func() {
		proxy.mu.Lock()
		proxy.closing = true
		connections := make([]net.Conn, 0, len(proxy.connections))
		for connection := range proxy.connections {
			connections = append(connections, connection)
		}
		proxy.mu.Unlock()
		_ = proxy.server.Close()
		for _, connection := range connections {
			_ = connection.Close()
		}
	})
	<-proxy.done
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if proxy.serveErr != nil {
		return ErrWorkerUnavailable
	}
	return nil
}

func (proxy *browserNetworkProxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request == nil || request.URL == nil {
		proxy.deny(writer)
		return
	}
	if request.Method == http.MethodConnect {
		proxy.serveConnect(writer, request)
		return
	}
	if request.URL.Scheme != "http" && request.URL.Scheme != "https" {
		proxy.deny(writer)
		return
	}
	if request.URL.User != nil || request.URL.Host == "" {
		proxy.deny(writer)
		return
	}
	requestOrigin, requestOriginErr := proxy.policy.normalizeOrigin(
		request.URL.Scheme + "://" + request.URL.Host,
	)
	hostOrigin, hostOriginErr := proxy.policy.normalizeOrigin(
		request.URL.Scheme + "://" + request.Host,
	)
	if requestOriginErr != nil || hostOriginErr != nil || requestOrigin != hostOrigin {
		proxy.deny(writer)
		return
	}
	destination, err := proxy.policy.destination(request.Context(), request.URL.Scheme, request.URL.Host)
	if err != nil {
		proxy.deny(writer)
		return
	}
	if headerHasToken(request.Header, "Connection", "upgrade") && request.Header.Get("Upgrade") != "" {
		proxy.serveUpgrade(writer, request, destination)
		return
	}
	proxy.serveHTTP(writer, request, destination)
}

func (proxy *browserNetworkProxy) serveConnect(writer http.ResponseWriter, request *http.Request) {
	destination, err := proxy.policy.destination(request.Context(), "https", request.Host)
	if err != nil {
		proxy.deny(writer)
		return
	}
	upstream, err := proxy.policy.dialDestination(request.Context(), "tcp", destination)
	if err != nil {
		proxy.fail(writer)
		return
	}
	client, buffered, err := hijackBrowserProxy(writer)
	if err != nil {
		_ = upstream.Close()
		return
	}
	if _, err = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	if err = buffered.Flush(); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	proxy.tunnelWithBufferedClient(client, buffered, upstream)
}

func (proxy *browserNetworkProxy) serveUpgrade(
	writer http.ResponseWriter,
	request *http.Request,
	destination []string,
) {
	upstream, err := proxy.policy.dialDestination(request.Context(), "tcp", destination)
	if err != nil {
		proxy.fail(writer)
		return
	}
	client, buffered, err := hijackBrowserProxy(writer)
	if err != nil {
		_ = upstream.Close()
		return
	}
	outbound := request.Clone(request.Context())
	outbound.RequestURI = ""
	outbound.URL.Scheme = ""
	outbound.URL.Host = ""
	removeBrowserProxyCredentials(outbound.Header)
	if err = outbound.Write(upstream); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	proxy.tunnelWithBufferedClient(client, buffered, upstream)
}

func (proxy *browserNetworkProxy) serveHTTP(
	writer http.ResponseWriter,
	request *http.Request,
	destination []string,
) {
	outbound := request.Clone(request.Context())
	outbound.RequestURI = ""
	removeProxyHeaders(outbound.Header)
	transport := &http.Transport{
		Proxy:             nil,
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return proxy.policy.dialDestination(ctx, network, destination)
		},
	}
	response, err := transport.RoundTrip(outbound)
	if err != nil {
		proxy.fail(writer)
		return
	}
	defer response.Body.Close()
	copyBrowserProxyHeaders(writer.Header(), response.Header)
	writer.WriteHeader(response.StatusCode)
	_, _ = io.Copy(writer, response.Body)
}

func (proxy *browserNetworkProxy) tunnelWithBufferedClient(
	client net.Conn,
	buffered *bufio.ReadWriter,
	upstream net.Conn,
) {
	if !proxy.track(client, upstream) {
		return
	}
	defer proxy.untrackAndClose(client, upstream)
	clientReader := io.Reader(client)
	if buffered != nil && buffered.Reader.Buffered() > 0 {
		clientReader = buffered.Reader
	}
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		_, _ = io.Copy(upstream, clientReader)
		closeBrowserProxyWrite(upstream)
	}()
	go func() {
		defer wait.Done()
		_, _ = io.Copy(client, upstream)
		closeBrowserProxyWrite(client)
	}()
	wait.Wait()
}

func (proxy *browserNetworkProxy) track(connections ...net.Conn) bool {
	proxy.mu.Lock()
	if proxy.closing {
		proxy.mu.Unlock()
		for _, connection := range connections {
			_ = connection.Close()
		}
		return false
	}
	for _, connection := range connections {
		proxy.connections[connection] = struct{}{}
	}
	proxy.mu.Unlock()
	return true
}

func (proxy *browserNetworkProxy) untrackAndClose(connections ...net.Conn) {
	proxy.mu.Lock()
	for _, connection := range connections {
		delete(proxy.connections, connection)
	}
	proxy.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (proxy *browserNetworkProxy) deny(writer http.ResponseWriter) {
	proxy.denials.Add(1)
	http.Error(writer, "browser network policy denied the request", http.StatusForbidden)
}

func (*browserNetworkProxy) fail(writer http.ResponseWriter) {
	http.Error(writer, "browser destination is unavailable", http.StatusBadGateway)
}

func (policy *browserNetworkPolicy) destination(
	ctx context.Context,
	scheme string,
	authority string,
) ([]string, error) {
	if policy == nil || (scheme != "http" && scheme != "https") || authority == "" {
		return nil, ErrDenied
	}
	origin, err := policy.normalizeOrigin(scheme + "://" + authority)
	if err != nil {
		return nil, ErrDenied
	}
	if policy.mode == config.BrowserNetworkExactOrigins {
		if _, ok := policy.allowedOrigins[origin]; !ok {
			return nil, ErrDenied
		}
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Hostname() == "" {
		return nil, ErrDenied
	}
	port := parsed.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	host := parsed.Hostname()
	if address, addressErr := netip.ParseAddr(host); addressErr == nil {
		if policy.mode != config.BrowserNetworkAnyHTTP &&
			!config.IsPublicBrowserIP(net.IP(address.AsSlice())) {
			return nil, ErrDenied
		}
		return []string{net.JoinHostPort(address.String(), port)}, nil
	}
	addresses, err := policy.lookupIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 || len(addresses) > browserProxyMaxResolvedAddresses {
		return nil, ErrDenied
	}
	validated := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if address == nil || (address.To4() == nil && address.To16() == nil) ||
			(policy.mode != config.BrowserNetworkAnyHTTP && !config.IsPublicBrowserIP(address)) {
			return nil, ErrDenied
		}
		validated = append(validated, net.JoinHostPort(address.String(), port))
	}
	return validated, nil
}

func (policy *browserNetworkPolicy) normalizeOrigin(raw string) (string, error) {
	if policy != nil && policy.mode == config.BrowserNetworkAnyHTTP {
		return config.NormalizeBrowserHTTPOrigin(raw)
	}
	return config.NormalizeBrowserOrigin(raw)
}

func (policy *browserNetworkPolicy) dialDestination(
	ctx context.Context,
	network string,
	destinations []string,
) (net.Conn, error) {
	if policy == nil || len(destinations) == 0 {
		return nil, ErrDenied
	}
	for _, destination := range destinations {
		connection, err := policy.dial(ctx, network, destination)
		if err == nil {
			return connection, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("all validated browser destinations are unavailable")
}

func hijackBrowserProxy(writer http.ResponseWriter) (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("browser proxy response does not support hijacking")
	}
	connection, buffered, err := hijacker.Hijack()
	if err != nil {
		return nil, nil, fmt.Errorf("hijack browser proxy connection: %w", err)
	}
	return connection, buffered, nil
}

func removeBrowserProxyCredentials(headers http.Header) {
	for _, name := range []string{
		"Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization",
	} {
		headers.Del(name)
	}
}

func removeProxyHeaders(headers http.Header) {
	for _, token := range strings.Split(headers.Get("Connection"), ",") {
		if token = strings.TrimSpace(token); token != "" {
			headers.Del(token)
		}
	}
	for _, name := range []string{
		"Connection", "Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		headers.Del(name)
	}
}

func copyBrowserProxyHeaders(destination, source http.Header) {
	for name, values := range source {
		for _, value := range values {
			destination.Add(name, value)
		}
	}
	for _, token := range strings.Split(source.Get("Connection"), ",") {
		if token = strings.TrimSpace(token); token != "" {
			destination.Del(token)
		}
	}
	for _, name := range []string{
		"Connection", "Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		destination.Del(name)
	}
}

func headerHasToken(headers http.Header, name, expected string) bool {
	for _, token := range strings.Split(headers.Get(name), ",") {
		if strings.EqualFold(strings.TrimSpace(token), expected) {
			return true
		}
	}
	return false
}

func closeBrowserProxyWrite(connection net.Conn) {
	if closer, ok := connection.(interface{ CloseWrite() error }); ok {
		_ = closer.CloseWrite()
	}
}
