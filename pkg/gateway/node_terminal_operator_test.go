package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

type fakeNodeTerminalOperatorOpener struct {
	requests []tools.NodeTerminalOperatorOpenRequest
	result   tools.NodeTerminalOperatorOpenResult
	err      error
	open     func(tools.NodeTerminalOperatorOpenRequest) (tools.NodeTerminalOperatorOpenResult, error)
}

func (opener *fakeNodeTerminalOperatorOpener) Open(
	_ context.Context,
	request tools.NodeTerminalOperatorOpenRequest,
) (tools.NodeTerminalOperatorOpenResult, error) {
	opener.requests = append(opener.requests, request)
	if opener.open != nil {
		return opener.open(request)
	}
	return opener.result, opener.err
}

type fakeNodeTerminalOperatorStream struct {
	events   chan nodes.TerminalEvent
	controls chan nodes.TerminalControlRequest
	closed   chan struct{}
	once     sync.Once
}

func newFakeNodeTerminalOperatorStream() *fakeNodeTerminalOperatorStream {
	return &fakeNodeTerminalOperatorStream{
		events:   make(chan nodes.TerminalEvent, 8),
		controls: make(chan nodes.TerminalControlRequest, 8),
		closed:   make(chan struct{}),
	}
}

func (stream *fakeNodeTerminalOperatorStream) Receive(ctx context.Context) (nodes.TerminalEvent, error) {
	select {
	case <-ctx.Done():
		return nodes.TerminalEvent{}, ctx.Err()
	case <-stream.closed:
		return nodes.TerminalEvent{}, errors.New("closed")
	case event := <-stream.events:
		return event, nil
	}
}

func (stream *fakeNodeTerminalOperatorStream) Control(
	ctx context.Context,
	request nodes.TerminalControlRequest,
) error {
	if err := request.Validate(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.closed:
		return errors.New("closed")
	case stream.controls <- request:
		return nil
	}
}

func (stream *fakeNodeTerminalOperatorStream) Close(context.Context) error {
	stream.once.Do(func() {
		close(stream.closed)
	})
	return nil
}

type fakeNodeTerminalOperatorSource struct {
	mu            sync.Mutex
	stream        *fakeNodeTerminalOperatorStream
	metadata      nodes.TerminalMetadata
	attachOwner   nodes.TerminalOwner
	attachID      string
	attachCount   int
	attachStarted chan struct{}
	attachRelease <-chan struct{}
	attachErr     error
	closeCount    int
	closeStarted  chan struct{}
	closeRelease  <-chan struct{}
}

type fakeNodeTerminalOperatorRoutes struct {
	handlers map[string]http.Handler
}

func (routes *fakeNodeTerminalOperatorRoutes) RegisterHTTPHandler(path string, handler http.Handler) error {
	if routes.handlers == nil {
		routes.handlers = make(map[string]http.Handler)
	}
	if _, exists := routes.handlers[path]; exists {
		return errors.New("route already registered")
	}
	routes.handlers[path] = handler
	return nil
}

func (routes *fakeNodeTerminalOperatorRoutes) ReplaceHTTPHandler(path string, handler http.Handler) error {
	if _, exists := routes.handlers[path]; !exists {
		return errors.New("route is not registered")
	}
	routes.handlers[path] = handler
	return nil
}

func (routes *fakeNodeTerminalOperatorRoutes) UnregisterHTTPHandler(path string) {
	delete(routes.handlers, path)
}

func (source *fakeNodeTerminalOperatorSource) attachOperatorTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodeTerminalOperatorStream, nodes.TerminalMetadata, error) {
	source.mu.Lock()
	source.attachOwner = owner
	source.attachID = terminalID
	source.attachCount++
	started := source.attachStarted
	release := source.attachRelease
	attachErr := source.attachErr
	stream := source.stream
	metadata := source.metadata
	source.mu.Unlock()
	if started != nil {
		close(started)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, nodes.TerminalMetadata{}, ctx.Err()
		}
	}
	return stream, metadata, attachErr
}

func (source *fakeNodeTerminalOperatorSource) terminalOperatorStatus(
	context.Context,
	nodes.TerminalOwner,
	string,
) (nodes.TerminalMetadata, error) {
	return source.metadata, nil
}

func (source *fakeNodeTerminalOperatorSource) closeOperatorTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) (nodes.TerminalMetadata, error) {
	source.mu.Lock()
	if owner != source.metadata.Owner || terminalID != source.metadata.TerminalID {
		source.mu.Unlock()
		return nodes.TerminalMetadata{}, nodes.ErrGatewayTerminalConflict
	}
	started := source.closeStarted
	release := source.closeRelease
	source.mu.Unlock()
	if started != nil {
		close(started)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nodes.TerminalMetadata{}, ctx.Err()
		}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	source.closeCount++
	source.metadata.State = string(nodes.GatewayTerminalClosed)
	source.metadata.Reason = "close"
	source.metadata.CompletedAt = source.metadata.StartedAt + 1
	source.metadata.TerminationConfirmed = true
	return source.metadata, nil
}

func TestNodeTerminalOperatorRequiresAuthSessionAndOrigin(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", []string{"https://operator.example"})
	source, owner := newFakeNodeTerminalOperatorFixture()
	if err := hub.bind(source, owner, source.metadata.TerminalID, "mint-session"); err != nil {
		t.Fatal(err)
	}
	if err := hub.bind(
		source,
		owner,
		source.metadata.TerminalID,
		"other-session",
	); !errors.Is(err, nodes.ErrGatewayTerminalConflict) {
		t.Fatalf("second operator session bind error = %v", err)
	}
	server := httptest.NewServer(hub)
	defer server.Close()

	endpoint := server.URL + "?session_id=mint-session&terminal_id=" + source.metadata.TerminalID
	response, err := http.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || source.attachCount != 0 {
		t.Fatalf("unauthenticated response = %d, attaches = %d", response.StatusCode, source.attachCount)
	}

	header := http.Header{"Authorization": []string{"Bearer operator-secret"}}
	wrongSession := strings.Replace(endpoint, "mint-session", "other-session", 1)
	response, err = doTerminalOperatorRequest(wrongSession, header)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound || source.attachCount != 0 {
		t.Fatalf("wrong-session response = %d, attaches = %d", response.StatusCode, source.attachCount)
	}

	wsEndpoint := "ws" + strings.TrimPrefix(endpoint, "http")
	badOriginHeader := header.Clone()
	badOriginHeader.Set("Origin", "https://attacker.example")
	if connection, response, dialErr := websocket.DefaultDialer.Dial(wsEndpoint, badOriginHeader); dialErr == nil {
		connection.Close()
		t.Fatal("cross-origin operator websocket was accepted")
	} else if response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin response = %#v, error = %v", response, dialErr)
	}

	validHeader := header.Clone()
	validHeader.Set("Origin", "https://operator.example")
	connection, response, err := websocket.DefaultDialer.Dial(wsEndpoint, validHeader)
	if err != nil {
		t.Fatalf("valid operator websocket: response=%#v error=%v", response, err)
	}
	defer connection.Close()
	var attached nodeTerminalOperatorAttached
	if err := connection.ReadJSON(&attached); err != nil {
		t.Fatal(err)
	}
	if attached.Type != "attached" ||
		attached.TerminalID != source.metadata.TerminalID ||
		source.attachCount != 1 ||
		source.attachOwner != owner ||
		source.attachID != source.metadata.TerminalID {
		t.Fatalf("attached = %#v, source = %#v", attached, source)
	}
}

func TestNodeTerminalOperatorOrdersControlsAndKeepsBytesOnSocket(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", nil)
	source, owner := newFakeNodeTerminalOperatorFixture()
	if err := hub.bind(source, owner, source.metadata.TerminalID, "mint-session"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub)
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") +
		"?session_id=mint-session&terminal_id=" + source.metadata.TerminalID
	header := http.Header{"Authorization": []string{"Bearer operator-secret"}}
	connection, response, err := websocket.DefaultDialer.Dial(endpoint, header)
	if err != nil {
		t.Fatalf("dial operator websocket: response=%#v error=%v", response, err)
	}
	var attached nodeTerminalOperatorAttached
	if err := connection.ReadJSON(&attached); err != nil {
		t.Fatal(err)
	}

	writeOperatorRequest(t, connection, nodeTerminalOperatorRequest{
		Version: nodes.TerminalProtocolVersion, Type: "input", Sequence: 1,
		IdempotencyKey: "input_1", InputBase64: base64.StdEncoding.EncodeToString([]byte("secret terminal bytes")),
	})
	assertTerminalControl(t, source.stream.controls, func(control nodes.TerminalControlRequest) bool {
		return control.Sequence == 1 && control.InputBase64 != "" && control.Owner == owner
	})
	writeOperatorRequest(t, connection, nodeTerminalOperatorRequest{
		Version: nodes.TerminalProtocolVersion, Type: "resize", Sequence: 2,
		IdempotencyKey: "resize_2", Columns: 120, Rows: 40,
	})
	assertTerminalControl(t, source.stream.controls, func(control nodes.TerminalControlRequest) bool {
		return control.Sequence == 2 && control.Columns == 120 && control.Rows == 40
	})
	if err := hub.signal(t.Context(), owner, source.metadata.TerminalID, "INT"); err != nil {
		t.Fatal(err)
	}
	assertTerminalControl(t, source.stream.controls, func(control nodes.TerminalControlRequest) bool {
		return control.Sequence == 3 && control.Signal == "INT"
	})

	output := base64.StdEncoding.EncodeToString([]byte("private output"))
	source.stream.events <- nodes.TerminalEvent{
		Version: nodes.TerminalProtocolVersion, Type: "output",
		TerminalID: source.metadata.TerminalID, Cursor: 14, DataBase64: output,
	}
	var event nodes.TerminalEvent
	if err := connection.ReadJSON(&event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "output" || event.DataBase64 != output {
		t.Fatalf("operator output event = %#v", event)
	}

	writeOperatorRequest(t, connection, nodeTerminalOperatorRequest{
		Version: nodes.TerminalProtocolVersion, Type: "input", Sequence: 3,
		IdempotencyKey: "duplicate_3", InputBase64: base64.StdEncoding.EncodeToString([]byte("must not replay")),
	})
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("sequence replay left the operator websocket open")
	}
	select {
	case <-source.stream.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("sequence replay did not fail closed")
	}
	select {
	case replay := <-source.stream.controls:
		t.Fatalf("sequence replay reached the terminal: %#v", replay)
	default:
	}
}

func TestNodeTerminalOperatorAcceptsBrowserSubprotocolAuthentication(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", nil)
	source, owner := newFakeNodeTerminalOperatorFixture()
	if err := hub.bind(source, owner, source.metadata.TerminalID, "mint-session"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub)
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") +
		"?session_id=mint-session&terminal_id=" + source.metadata.TerminalID
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"token.operator-secret"}
	connection, response, err := dialer.Dial(endpoint, nil)
	if err != nil {
		t.Fatalf("dial browser operator websocket: response=%#v error=%v", response, err)
	}
	defer connection.Close()
	var attached nodeTerminalOperatorAttached
	if err := connection.ReadJSON(&attached); err != nil {
		t.Fatal(err)
	}
	if connection.Subprotocol() != "token.operator-secret" || attached.Type != "attached" {
		t.Fatalf("browser operator attachment = protocol %q, event %#v", connection.Subprotocol(), attached)
	}
}

func TestNodeTerminalOperatorOpenRequiresAuthenticationOriginAndIsolatesSessions(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", []string{"https://operator.example"})
	opener := &fakeNodeTerminalOperatorOpener{result: tools.NodeTerminalOperatorOpenResult{
		TerminalID: "terminal_test", State: string(nodes.GatewayTerminalPendingAttach),
		AttachBefore: time.Now().Add(30 * time.Second).Unix(),
	}}
	hub.configureOpener(opener, "/workspace/main")
	server := httptest.NewServer(hub)
	defer server.Close()

	body := `{"version":1,"session_id":"operator-one","request_id":"request-one",` +
		`"target":"vpn-smoke","profile":"owner-test","working_scope":"workspace",` +
		`"columns":100,"rows":31}`
	request, err := http.NewRequest(http.MethodPost, server.URL+nodeTerminalOperatorOpenPath, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || len(opener.requests) != 0 {
		t.Fatalf("unauthenticated open = %d, calls = %d", response.StatusCode, len(opener.requests))
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+nodeTerminalOperatorOpenPath, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Origin", "https://attacker.example")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || len(opener.requests) != 0 {
		t.Fatalf("cross-origin open = %d, calls = %d", response.StatusCode, len(opener.requests))
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+nodeTerminalOperatorOpenPath, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Origin", "https://operator.example")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated || len(opener.requests) != 1 {
		t.Fatalf("authenticated open = %d, calls = %d", response.StatusCode, len(opener.requests))
	}
	first := opener.requests[0]
	if first.OperatorSessionID != "operator-one" || first.AgentID != "main" ||
		first.Owner.Target != "vpn-smoke" || first.Owner.Profile != "owner-test" ||
		first.Owner.Validate() != nil {
		t.Fatalf("operator request = %#v", first)
	}

	secondBody := strings.Replace(body, "operator-one", "operator-two", 1)
	request, err = http.NewRequest(
		http.MethodPost,
		server.URL+nodeTerminalOperatorOpenPath,
		strings.NewReader(secondBody),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Origin", "https://operator.example")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated || len(opener.requests) != 2 ||
		opener.requests[1].Owner == first.Owner {
		t.Fatalf("second session open = %d, requests = %#v", response.StatusCode, opener.requests)
	}
}

func TestNodeTerminalOperatorModelCloseSharesOrderedStream(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", nil)
	source, owner := newFakeNodeTerminalOperatorFixture()
	if err := hub.bind(source, owner, source.metadata.TerminalID, "mint-session"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub)
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") +
		"?session_id=mint-session&terminal_id=" + source.metadata.TerminalID
	header := http.Header{"Authorization": []string{"Bearer operator-secret"}}
	connection, response, err := websocket.DefaultDialer.Dial(endpoint, header)
	if err != nil {
		t.Fatalf("dial operator websocket: response=%#v error=%v", response, err)
	}
	defer connection.Close()
	var attached nodeTerminalOperatorAttached
	if err := connection.ReadJSON(&attached); err != nil {
		t.Fatal(err)
	}

	closeCtx, closeCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer closeCancel()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- hub.closeTerminal(closeCtx, owner, source.metadata.TerminalID)
	}()
	assertTerminalControl(t, source.stream.controls, func(control nodes.TerminalControlRequest) bool {
		return control.Sequence == 1 && control.Close && control.Owner == owner
	})
	source.stream.events <- nodes.TerminalEvent{
		Version: nodes.TerminalProtocolVersion, Type: "closed",
		TerminalID: source.metadata.TerminalID, State: "closed", Reason: "close",
		StartedAt: source.metadata.StartedAt, CompletedAt: source.metadata.StartedAt + 1,
		TerminationConfirmed: true,
	}
	var closed nodes.TerminalEvent
	if err := connection.ReadJSON(&closed); err != nil {
		t.Fatal(err)
	}
	if closed.Type != "closed" || !closed.TerminationConfirmed {
		t.Fatalf("operator close event = %#v", closed)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("model close did not observe stream completion")
	}
}

func TestNodeTerminalOperatorConfigRotationFailsClosed(t *testing.T) {
	routes := &fakeNodeTerminalOperatorRoutes{}
	runtime := &nodeAdmissionRuntime{routes: routes}
	cfg := config.DefaultConfig()
	enableTestMintClawOperator(t, cfg, "first-token", []string{"https://first.example"})
	if err := runtime.configureTerminalOperator(cfg, nil); err != nil {
		t.Fatal(err)
	}
	first := runtime.terminalOperatorHub()
	if first == nil || routes.handlers[nodeTerminalOperatorPath] != first ||
		routes.handlers[nodeTerminalOperatorOpenPath] != first {
		t.Fatal("authenticated operator route was not mounted")
	}
	source, owner := newFakeNodeTerminalOperatorFixture()
	session := &nodeTerminalOperatorSession{
		owner: owner, terminalID: source.metadata.TerminalID,
		stream: source.stream, next: 1, finished: make(chan struct{}),
	}
	first.active[terminalOperatorKey("mint-session", source.metadata.TerminalID)] = session

	enableTestMintClawOperator(t, cfg, "second-token", []string{"https://second.example"})
	if err := runtime.configureTerminalOperator(cfg, nil); err != nil {
		t.Fatal(err)
	}
	second := runtime.terminalOperatorHub()
	if second == nil || second == first || routes.handlers[nodeTerminalOperatorPath] != second ||
		routes.handlers[nodeTerminalOperatorOpenPath] != second {
		t.Fatal("operator token rotation did not replace the transport")
	}
	select {
	case <-source.stream.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("operator token rotation retained an active terminal stream")
	}
	first.mu.Lock()
	firstActive := len(first.active)
	first.mu.Unlock()
	if firstActive != 0 {
		t.Fatal("operator token rotation retained old session authority")
	}

	if err := runtime.configureTerminalOperator(nil, nil); err != nil {
		t.Fatal(err)
	}
	if runtime.terminalOperatorHub() != nil ||
		runtime.terminalMounted ||
		routes.handlers[nodeTerminalOperatorPath] != nil ||
		routes.handlers[nodeTerminalOperatorOpenPath] != nil {
		t.Fatal("operator transport remained mounted after authentication was disabled")
	}
}

func TestNodeTerminalOperatorPublishedGenerationBindsItsOwnHub(t *testing.T) {
	oldHub := newNodeTerminalOperatorHub("operator-secret", nil)
	newHub := newNodeTerminalOperatorHub("operator-secret", nil)
	source, owner := newFakeNodeTerminalOperatorFixture()
	boundSource := &nodeTerminalHubSource{
		nodeTerminalSource: &nodeTerminalSource{},
		hub:                newHub,
	}
	// Keep runtime.terminalHub on the old generation while the already-published
	// new handler opens. This is the route-publication/runtime-pointer
	// interleaving that previously allowed a new request to bind the old hub.
	opener := &fakeNodeTerminalOperatorOpener{open: func(
		request tools.NodeTerminalOperatorOpenRequest,
	) (tools.NodeTerminalOperatorOpenResult, error) {
		if err := boundSource.BindTerminalOperator(
			owner,
			source.metadata.TerminalID,
			request.OperatorSessionID,
		); err != nil {
			return tools.NodeTerminalOperatorOpenResult{}, err
		}
		return tools.NodeTerminalOperatorOpenResult{
			TerminalID:   source.metadata.TerminalID,
			State:        string(nodes.GatewayTerminalPendingAttach),
			AttachBefore: time.Now().Add(30 * time.Second).Unix(),
		}, nil
	}}
	newHub.configureOpener(opener, "/workspace/main")
	runtime := &nodeAdmissionRuntime{terminalHub: oldHub}
	published := make(chan http.Handler, 1)
	releasePublication := make(chan struct{})
	publicationDone := make(chan struct{})
	go func() {
		// Model ReplaceHTTPHandler publishing the new route before
		// configureTerminalOperator updates runtime.terminalHub.
		published <- newHub
		<-releasePublication
		runtime.registryMu.Lock()
		runtime.terminalHub = newHub
		runtime.registryMu.Unlock()
		close(publicationDone)
	}()
	publishedHandler := <-published
	requestBody := `{"version":1,"session_id":"mint-session","request_id":"request-one",` +
		`"target":"target_test","profile":"profile_test","working_scope":"workspace",` +
		`"columns":100,"rows":31}`
	request := httptest.NewRequest(http.MethodPost, nodeTerminalOperatorOpenPath, strings.NewReader(requestBody))
	request.Header.Set("Authorization", "Bearer operator-secret")
	response := httptest.NewRecorder()
	publishedHandler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("published generation open = %d: %s", response.Code, response.Body.String())
	}
	key := terminalOperatorKey("mint-session", source.metadata.TerminalID)
	newHub.mu.Lock()
	_, boundNew := newHub.pending[key]
	newHub.mu.Unlock()
	oldHub.mu.Lock()
	_, boundOld := oldHub.pending[key]
	oldHub.mu.Unlock()
	if !boundNew || boundOld || runtime.terminalOperatorHub() != oldHub {
		t.Fatalf("generation binding: new=%t old=%t", boundNew, boundOld)
	}
	close(releasePublication)
	<-publicationDone
	if runtime.terminalOperatorHub() != newHub {
		t.Fatal("publication barrier did not release the new runtime generation")
	}
}

func TestNodeTerminalOperatorShutdownTerminatesPendingAttach(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", nil)
	source, owner := newFakeNodeTerminalOperatorFixture()
	if err := hub.bind(source, owner, source.metadata.TerminalID, "mint-session"); err != nil {
		t.Fatal(err)
	}
	hub.shutdown()
	if source.closeCount != 1 {
		t.Fatalf("pending terminal close count = %d", source.closeCount)
	}
	if err := hub.bind(
		source,
		owner,
		source.metadata.TerminalID,
		"mint-session",
	); err == nil {
		t.Fatal("shut down operator transport accepted a new binding")
	}
}

func TestNodeTerminalOperatorShutdownTerminatesClaimBeforeAttachDispatch(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", nil)
	source, owner := newFakeNodeTerminalOperatorFixture()
	if err := hub.bind(source, owner, source.metadata.TerminalID, "mint-session"); err != nil {
		t.Fatal(err)
	}
	key := terminalOperatorKey("mint-session", source.metadata.TerminalID)
	if _, found := hub.claim(key, time.Now()); !found {
		t.Fatal("pending terminal was not claimed")
	}
	hub.shutdown()
	if source.closeCount != 1 {
		t.Fatalf("claimed terminal close count = %d", source.closeCount)
	}
}

func TestNodeTerminalOperatorShutdownTerminatesClaimDuringAttach(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", nil)
	source, owner := newFakeNodeTerminalOperatorFixture()
	source.attachStarted = make(chan struct{})
	source.attachRelease = make(chan struct{})
	if err := hub.bind(source, owner, source.metadata.TerminalID, "mint-session"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub)
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") +
		"?session_id=mint-session&terminal_id=" + source.metadata.TerminalID
	header := http.Header{"Authorization": []string{"Bearer operator-secret"}}
	connection, response, err := websocket.DefaultDialer.Dial(endpoint, header)
	if err != nil {
		t.Fatalf("dial operator websocket: response=%#v error=%v", response, err)
	}
	defer connection.Close()
	waitForTerminalCleanupStart(t, source.attachStarted)
	hub.shutdown()
	source.mu.Lock()
	attachCount := source.attachCount
	closeCount := source.closeCount
	source.mu.Unlock()
	if attachCount != 1 || closeCount != 1 {
		t.Fatalf("attach count = %d, close count = %d", attachCount, closeCount)
	}
}

func TestNodeTerminalOperatorShutdownDrainsPendingAttachesConcurrently(t *testing.T) {
	hub := newNodeTerminalOperatorHub("operator-secret", nil)
	release := make(chan struct{})
	first, firstOwner := newFakeNodeTerminalOperatorFixture()
	first.closeStarted = make(chan struct{})
	first.closeRelease = release
	second, secondOwner := newFakeNodeTerminalOperatorFixture()
	secondOwner.SessionID = "session_second"
	secondOwner.RouteID = "route_second"
	second.metadata.TerminalID = "terminal_second"
	second.metadata.Owner = secondOwner
	second.closeStarted = make(chan struct{})
	second.closeRelease = release
	if err := hub.bind(first, firstOwner, first.metadata.TerminalID, "mint-session-one"); err != nil {
		t.Fatal(err)
	}
	if err := hub.bind(second, secondOwner, second.metadata.TerminalID, "mint-session-two"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		hub.shutdown()
		close(done)
	}()
	waitForTerminalCleanupStart(t, first.closeStarted)
	waitForTerminalCleanupStart(t, second.closeStarted)
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("parallel pending terminal cleanup did not finish")
	}
	if first.closeCount != 1 || second.closeCount != 1 {
		t.Fatalf("pending close counts = (%d, %d)", first.closeCount, second.closeCount)
	}
}

func waitForTerminalCleanupStart(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("pending terminal cleanup did not start")
	}
}

func newFakeNodeTerminalOperatorFixture() (*fakeNodeTerminalOperatorSource, nodes.TerminalOwner) {
	owner := nodes.TerminalOwner{
		ActorID: "actor_test", AgentID: "agent_test", RouteID: "route_test",
		SessionID: "session_test", WorkspaceID: "workspace_test",
		Target: "target_test", Profile: "profile_test",
	}
	return &fakeNodeTerminalOperatorSource{
		stream: newFakeNodeTerminalOperatorStream(),
		metadata: nodes.TerminalMetadata{
			TerminalID: "terminal_test", Owner: owner,
			State: string(nodes.GatewayTerminalPendingAttach), StartedAt: time.Now().Unix(),
		},
	}, owner
}

func doTerminalOperatorRequest(endpoint string, header http.Header) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	request.Header = header
	return http.DefaultClient.Do(request)
}

func writeOperatorRequest(
	t *testing.T,
	connection *websocket.Conn,
	request nodeTerminalOperatorRequest,
) {
	t.Helper()
	if err := connection.WriteJSON(request); err != nil {
		t.Fatal(err)
	}
}

func assertTerminalControl(
	t *testing.T,
	controls <-chan nodes.TerminalControlRequest,
	matches func(nodes.TerminalControlRequest) bool,
) {
	t.Helper()
	select {
	case control := <-controls:
		if !matches(control) {
			value, _ := json.Marshal(control)
			t.Fatalf("unexpected terminal control: %s", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminal control was not delivered")
	}
}
