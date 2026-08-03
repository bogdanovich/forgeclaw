package browser

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

func TestBrowserNetworkPolicyPinsValidatedPublicAddress(t *testing.T) {
	lookupCalls := 0
	policy, err := newBrowserNetworkPolicy(
		config.BrowserProfileConfig{NetworkMode: config.BrowserNetworkPublicWeb},
		func(_ context.Context, network, host string) ([]net.IP, error) {
			lookupCalls++
			if network != "ip" || host != "public.example" {
				t.Fatalf("lookup = %q, %q", network, host)
			}
			return []net.IP{net.ParseIP("8.8.8.8"), net.ParseIP("1.1.1.1")}, nil
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := policy.destination(context.Background(), "https", "public.example:8443")
	if err != nil || strings.Join(destination, ",") != "8.8.8.8:8443,1.1.1.1:8443" || lookupCalls != 1 {
		t.Fatalf("destination = %v, calls = %d, error = %v", destination, lookupCalls, err)
	}
	destination, err = policy.destination(context.Background(), "https", "8.8.8.8")
	if err != nil || strings.Join(destination, ",") != "8.8.8.8:443" || lookupCalls != 1 {
		t.Fatalf("IP destination = %v, calls = %d, error = %v", destination, lookupCalls, err)
	}
}

func TestBrowserNetworkPolicyTriesEveryValidatedAddressWithoutResolvingAgain(t *testing.T) {
	lookupCalls := 0
	var attempts []string
	var peer net.Conn
	policy, err := newBrowserNetworkPolicy(
		config.BrowserProfileConfig{NetworkMode: config.BrowserNetworkPublicWeb},
		func(context.Context, string, string) ([]net.IP, error) {
			lookupCalls++
			return []net.IP{net.ParseIP("8.8.8.8"), net.ParseIP("1.1.1.1")}, nil
		},
		func(_ context.Context, _, address string) (net.Conn, error) {
			attempts = append(attempts, address)
			if address == "8.8.8.8:443" {
				return nil, errors.New("first address unavailable")
			}
			connection, other := net.Pipe()
			peer = other
			return connection, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	destinations, err := policy.destination(context.Background(), "https", "public.example")
	if err != nil {
		t.Fatal(err)
	}
	connection, err := policy.dialDestination(context.Background(), "tcp", destinations)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	_ = peer.Close()
	if lookupCalls != 1 || strings.Join(attempts, ",") != "8.8.8.8:443,1.1.1.1:443" {
		t.Fatalf("lookup calls = %d, dial attempts = %v", lookupCalls, attempts)
	}
}

func TestBrowserNetworkPolicyDeniesUnlistedAndNonPublicDestinations(t *testing.T) {
	tests := []struct {
		name      string
		profile   config.BrowserProfileConfig
		authority string
		addresses []net.IP
	}{
		{
			name: "unlisted exact origin",
			profile: config.BrowserProfileConfig{
				AllowedOrigins: []string{"https://allowed.example"},
			},
			authority: "other.example",
			addresses: []net.IP{net.ParseIP("8.8.8.8")},
		},
		{
			name: "mixed public and private answers",
			profile: config.BrowserProfileConfig{
				NetworkMode: config.BrowserNetworkPublicWeb,
			},
			authority: "mixed.example",
			addresses: []net.IP{net.ParseIP("8.8.8.8"), net.ParseIP("127.0.0.1")},
		},
		{
			name: "metadata address",
			profile: config.BrowserProfileConfig{
				NetworkMode: config.BrowserNetworkPublicWeb,
			},
			authority: "169.254.169.254",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := newBrowserNetworkPolicy(
				test.profile,
				func(context.Context, string, string) ([]net.IP, error) {
					return test.addresses, nil
				},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if destination, destinationErr := policy.destination(
				context.Background(), "https", test.authority,
			); destinationErr == nil {
				t.Fatalf("destination = %q, want denial", destination)
			}
		})
	}
}

func TestBrowserNetworkPolicyDeniesAzureWireServerWithoutDialing(t *testing.T) {
	var lookups atomic.Int64
	var dials atomic.Int64
	policy, err := newBrowserNetworkPolicy(
		config.BrowserProfileConfig{NetworkMode: config.BrowserNetworkPublicWeb},
		func(context.Context, string, string) ([]net.IP, error) {
			lookups.Add(1)
			return []net.IP{net.ParseIP("168.63.129.16")}, nil
		},
		func(context.Context, string, string) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("unexpected dial")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, authority := range []string{"168.63.129.16", "wireserver.example"} {
		if destination, destinationErr := policy.destination(
			context.Background(), "http", authority,
		); !errors.Is(destinationErr, ErrDenied) {
			t.Fatalf("destination(%q) = %v, %v, want ErrDenied", authority, destination, destinationErr)
		}
	}
	if lookups.Load() != 1 || dials.Load() != 0 {
		t.Fatalf("lookups = %d, dials = %d, want 1, 0", lookups.Load(), dials.Load())
	}
}

func TestBrowserNetworkProxyEnforcesRedirectsAndFreshDNS(t *testing.T) {
	var secretRequests atomic.Int64
	fixture := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/ok":
			if request.Host == "" || !strings.HasPrefix(request.Host, "public.test:") {
				t.Errorf("fixture host = %q", request.Host)
			}
			_, _ = io.WriteString(writer, "ok")
		case "/redirect":
			port := strings.Split(request.Host, ":")[1]
			http.Redirect(writer, request, "http://private.test:"+port+"/secret", http.StatusFound)
		case "/secret":
			secretRequests.Add(1)
			_, _ = io.WriteString(writer, "secret")
		default:
			http.NotFound(writer, request)
		}
	}))
	defer fixture.Close()
	fixtureURL, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	var publicLookups atomic.Int64
	var dials atomic.Int64
	proxy, err := startBrowserNetworkProxy(
		config.BrowserProfileConfig{NetworkMode: config.BrowserNetworkPublicWeb},
		func(_ context.Context, _ string, host string) ([]net.IP, error) {
			if host == "private.test" {
				return []net.IP{net.ParseIP("127.0.0.1")}, nil
			}
			if host != "public.test" {
				return nil, fmt.Errorf("unexpected host %q", host)
			}
			if publicLookups.Add(1) >= 3 {
				return []net.IP{net.ParseIP("10.0.0.1")}, nil
			}
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			if !strings.HasPrefix(address, "8.8.8.8:") {
				return nil, fmt.Errorf("proxy dialed unvalidated address %q", address)
			}
			return (&net.Dialer{}).DialContext(ctx, network, fixture.Listener.Addr().String())
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close() })
	proxyURL, err := url.Parse(proxy.URL())
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{
		Proxy:             http.ProxyURL(proxyURL),
		DisableKeepAlives: true,
	}}
	baseURL := "http://public.test:" + fixtureURL.Port()
	response, err := client.Get(baseURL + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("public response = %d, %q", response.StatusCode, body)
	}
	response, err = client.Get(baseURL + "/redirect")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden || secretRequests.Load() != 0 || proxy.Denials() != 1 {
		t.Fatalf(
			"redirect response = %d, secret requests = %d, denials = %d",
			response.StatusCode,
			secretRequests.Load(),
			proxy.Denials(),
		)
	}
	response, err = client.Get(baseURL + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden || dials.Load() != 2 || proxy.Denials() != 2 {
		t.Fatalf("rebound response = %d, dials = %d, denials = %d", response.StatusCode, dials.Load(), proxy.Denials())
	}
	mismatchedHost, err := http.NewRequest(http.MethodGet, baseURL+"/ok", nil)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedHost.Host = "other.test:" + fixtureURL.Port()
	response, err = client.Do(mismatchedHost)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden || dials.Load() != 2 || proxy.Denials() != 3 {
		t.Fatalf(
			"mismatched Host response = %d, dials = %d, denials = %d",
			response.StatusCode,
			dials.Load(),
			proxy.Denials(),
		)
	}
}

func TestBrowserNetworkProxyEnforcesExactOriginRedirect(t *testing.T) {
	var unlistedRequests atomic.Int64
	fixture := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/unlisted" {
			unlistedRequests.Add(1)
			return
		}
		port := request.URL.Query().Get("port")
		http.Redirect(writer, request, "http://unlisted.test:"+port+"/unlisted", http.StatusFound)
	}))
	defer fixture.Close()
	fixtureURL, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	allowedOrigin := "http://allowed.test:" + fixtureURL.Port()
	var dials atomic.Int64
	proxy, err := startBrowserNetworkProxy(
		config.BrowserProfileConfig{AllowedOrigins: []string{allowedOrigin}},
		func(context.Context, string, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
		func(ctx context.Context, network, _ string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, fixture.Listener.Addr().String())
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close() })
	client := browserProxyHTTPClient(t, proxy)
	response, err := client.Get(allowedOrigin + "/redirect?port=" + fixtureURL.Port())
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden || unlistedRequests.Load() != 0 ||
		dials.Load() != 1 || proxy.Denials() != 1 {
		t.Fatalf(
			"exact redirect = status %d, unlisted requests %d, dials %d, denials %d",
			response.StatusCode,
			unlistedRequests.Load(),
			dials.Load(),
			proxy.Denials(),
		)
	}
}

func TestBrowserNetworkProxyCarriesPublicHTTPS(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, "secure")
	}))
	defer fixture.Close()
	fixtureURL, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := startBrowserNetworkProxy(
		config.BrowserProfileConfig{NetworkMode: config.BrowserNetworkPublicWeb},
		func(context.Context, string, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
		func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, fixture.Listener.Addr().String())
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close() })
	client := browserProxyHTTPClient(t, proxy)
	roots := x509.NewCertPool()
	roots.AddCert(fixture.Certificate())
	client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: fixture.Certificate().DNSNames[0],
	}
	response, err := client.Get("https://public.test:" + fixtureURL.Port() + "/secure")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "secure" {
		t.Fatalf("HTTPS response = %d, %q", response.StatusCode, body)
	}
}

func TestBrowserNetworkProxySupportsConnectAndUpgrade(t *testing.T) {
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			connection, acceptErr := echoListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()
	upgradeServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !headerHasToken(request.Header, "Connection", "upgrade") || request.Header.Get("Upgrade") != "websocket" {
			t.Errorf("upgrade headers = %+v", request.Header)
			http.Error(writer, "upgrade required", http.StatusBadRequest)
			return
		}
		connection, buffered, hijackErr := writer.(http.Hijacker).Hijack()
		if hijackErr != nil {
			t.Error(hijackErr)
			return
		}
		defer connection.Close()
		_, _ = buffered.WriteString(
			"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
		)
		_ = buffered.Flush()
		line, _ := buffered.ReadString('\n')
		_, _ = buffered.WriteString(line)
		_ = buffered.Flush()
	}))
	defer upgradeServer.Close()
	upgradeURL, err := url.Parse(upgradeServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := startBrowserNetworkProxy(
		config.BrowserProfileConfig{NetworkMode: config.BrowserNetworkPublicWeb},
		func(context.Context, string, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			port := strings.Split(address, ":")[1]
			target := upgradeServer.Listener.Addr().String()
			if port == strings.Split(echoListener.Addr().String(), ":")[1] {
				target = echoListener.Addr().String()
			}
			return (&net.Dialer{}).DialContext(ctx, network, target)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	connection := dialBrowserProxyTest(t, proxy)
	_, _ = fmt.Fprintf(
		connection,
		"CONNECT public.test:%d HTTP/1.1\r\nHost: public.test:%d\r\n\r\nearly\n",
		echoListener.Addr().(*net.TCPAddr).Port,
		echoListener.Addr().(*net.TCPAddr).Port,
	)
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT response = %+v, %v", response, err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || line != "early\n" {
		t.Fatalf("buffered CONNECT echo = %q, %v", line, err)
	}
	_, _ = io.WriteString(connection, "ping\n")
	line, err = reader.ReadString('\n')
	_ = connection.Close()
	if err != nil || line != "ping\n" {
		t.Fatalf("CONNECT echo = %q, %v", line, err)
	}

	connection = dialBrowserProxyTest(t, proxy)
	_, _ = fmt.Fprintf(
		connection,
		"GET http://public.test:%s/chat HTTP/1.1\r\nHost: public.test:%s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
		upgradeURL.Port(),
		upgradeURL.Port(),
	)
	reader = bufio.NewReader(connection)
	response, err = http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade response = %+v, %v", response, err)
	}
	_, _ = io.WriteString(connection, "frame\n")
	line, err = reader.ReadString('\n')
	_ = connection.Close()
	if err != nil || line != "frame\n" {
		t.Fatalf("upgrade echo = %q, %v", line, err)
	}
}

func TestBrowserNetworkProxyCloseEndsAvailability(t *testing.T) {
	proxy, err := startBrowserNetworkProxy(
		config.BrowserProfileConfig{NetworkMode: config.BrowserNetworkPublicWeb}, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !proxy.Available() || proxy.URL() == "" {
		t.Fatalf("started proxy = available %t, URL %q", proxy.Available(), proxy.URL())
	}
	if err = proxy.Close(); err != nil {
		t.Fatal(err)
	}
	if proxy.Available() {
		t.Fatal("closed proxy remained available")
	}
	if err = proxy.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func dialBrowserProxyTest(t *testing.T, proxy *browserNetworkProxy) net.Conn {
	t.Helper()
	parsed, err := url.Parse(proxy.URL())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTimeout("tcp", parsed.Host, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	return connection
}

func browserProxyHTTPClient(t *testing.T, proxy *browserNetworkProxy) *http.Client {
	t.Helper()
	proxyURL, err := url.Parse(proxy.URL())
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Transport: &http.Transport{
		Proxy:             http.ProxyURL(proxyURL),
		DisableKeepAlives: true,
	}}
}
