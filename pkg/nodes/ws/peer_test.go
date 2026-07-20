package ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sipeed/picoclaw/pkg/nodes/protocol"
)

func TestPeerDiscardsResponseAfterRequestCancellation(t *testing.T) {
	requestSeen := make(chan struct{})
	sendResponse := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := upgrader.Upgrade(writer, request, nil)
			if err != nil {
				return
			}
			defer connection.Close()
			_, data, err := connection.ReadMessage()
			if err != nil {
				return
			}
			envelope, err := protocol.Decode(data)
			if err != nil {
				return
			}
			close(requestSeen)
			<-sendResponse
			ok := true
			response, err := protocol.Encode(protocol.Envelope{
				Type:   protocol.FrameResponse,
				ID:     envelope.ID,
				OK:     &ok,
				Result: []byte(`{}`),
			})
			if err == nil {
				_ = connection.WriteMessage(websocket.TextMessage, response)
			}
		}),
	)
	defer server.Close()

	connection, handshakeResponse, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"),
		nil,
	)
	if handshakeResponse != nil && handshakeResponse.Body != nil {
		defer handshakeResponse.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	session := newPeer(connection)
	defer session.Close()
	session.markReady()
	ctx, cancel := context.WithCancel(t.Context())
	requestDone := make(chan error, 1)
	go func() {
		_, requestErr := session.request(ctx, "node.invoke", []byte(`{}`))
		requestDone <- requestErr
	}()
	<-requestSeen
	cancel()
	if requestErr := <-requestDone; !errors.Is(requestErr, context.Canceled) {
		t.Fatalf("request error = %v", requestErr)
	}
	close(sendResponse)
	_, responseData, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.Decode(responseData)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.handleResponse(response); err != nil {
		t.Fatalf("late response error = %v", err)
	}
	if err := session.handleResponse(response); !errors.Is(err, ErrUnexpectedResponse) {
		t.Fatalf("duplicate late response error = %v", err)
	}
}

func TestPeerRetainsCanceledRequestIDsUntilResponse(t *testing.T) {
	session := newPeer(newStubPeerConnection())
	for index := range maxOutstandingRequests {
		id := fmt.Sprintf("req_%d", index)
		if err := session.acquireRequestSlot(t.Context()); err != nil {
			t.Fatal(err)
		}
		session.pending[id] = make(chan responseResult, 1)
		session.abandon(id)
	}
	if len(session.abandoned) != maxOutstandingRequests {
		t.Fatalf("abandoned requests = %d", len(session.abandoned))
	}
	if err := session.acquireRequestSlot(t.Context()); !errors.Is(err, ErrRequestLimit) {
		t.Fatalf("overflow request error = %v", err)
	}
	ok := true
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse,
		ID:   "req_0",
		OK:   &ok,
	}); err != nil {
		t.Fatalf("oldest late response error = %v", err)
	}
	if err := session.acquireRequestSlot(t.Context()); err != nil {
		t.Fatalf("request after late response = %v", err)
	}
}

func TestPeerRequestCancellationWhileWaitingForWriter(t *testing.T) {
	session := newPeer(newStubPeerConnection())
	session.markReady()
	session.writeSlot <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	requestDone := make(chan error, 1)
	go func() {
		_, err := session.request(ctx, "node.invoke", []byte(`{}`))
		requestDone <- err
	}()
	waitForPeerPending(t, session, 1)
	cancel()
	if err := <-requestDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("request error = %v", err)
	}
	if len(session.pending) != 0 || len(session.requestSlots) != 0 {
		t.Fatalf("canceled queued request leaked state")
	}
}

func TestPeerRequestCancellationInterruptsBlockedWrite(t *testing.T) {
	connection := newStubPeerConnection()
	connection.blockWrites = true
	session := newPeer(connection)
	session.markReady()
	ctx, cancel := context.WithCancel(t.Context())
	requestDone := make(chan error, 1)
	go func() {
		_, err := session.request(ctx, "node.invoke", []byte(`{}`))
		requestDone <- err
	}()
	<-connection.writeStarted
	cancel()
	select {
	case err := <-requestDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked write did not observe request cancellation")
	}
	select {
	case <-session.closed:
	default:
		t.Fatal("failed write left WebSocket generation usable")
	}
}

func TestPeerCancellationBurstPreservesLateResponses(t *testing.T) {
	requestsSeen := make(chan string)
	sendResponses := make(chan struct{})
	finalRequestSeen := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		ids := make([]string, 0, maxOutstandingRequests)
		for range maxOutstandingRequests {
			_, data, readErr := connection.ReadMessage()
			if readErr != nil {
				return
			}
			envelope, decodeErr := protocol.Decode(data)
			if decodeErr != nil {
				return
			}
			ids = append(ids, envelope.ID)
			requestsSeen <- envelope.ID
		}
		<-sendResponses
		ok := true
		for _, id := range ids {
			data, encodeErr := protocol.Encode(protocol.Envelope{
				Type:   protocol.FrameResponse,
				ID:     id,
				OK:     &ok,
				Result: []byte(`{}`),
			})
			if encodeErr != nil || connection.WriteMessage(websocket.TextMessage, data) != nil {
				return
			}
		}
		_, data, readErr := connection.ReadMessage()
		if readErr != nil {
			return
		}
		envelope, decodeErr := protocol.Decode(data)
		if decodeErr != nil {
			return
		}
		close(finalRequestSeen)
		response, encodeErr := protocol.Encode(protocol.Envelope{
			Type:   protocol.FrameResponse,
			ID:     envelope.ID,
			OK:     &ok,
			Result: []byte(`{}`),
		})
		if encodeErr == nil {
			_ = connection.WriteMessage(websocket.TextMessage, response)
		}
	}))
	defer server.Close()

	connection, response, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"), nil,
	)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	session := newPeer(connection)
	defer session.Close()
	session.markReady()
	for range maxOutstandingRequests {
		ctx, cancel := context.WithCancel(t.Context())
		requestDone := make(chan error, 1)
		go func() {
			_, requestErr := session.request(ctx, "node.invoke", []byte(`{}`))
			requestDone <- requestErr
		}()
		<-requestsSeen
		cancel()
		if requestErr := <-requestDone; !errors.Is(requestErr, context.Canceled) {
			t.Fatalf("canceled request error = %v", requestErr)
		}
	}
	if _, overflowErr := session.request(t.Context(), "node.invoke", []byte(`{}`)); !errors.Is(
		overflowErr,
		ErrRequestLimit,
	) {
		t.Fatalf("overflow request error = %v", overflowErr)
	}
	close(sendResponses)
	for range maxOutstandingRequests {
		_, responseData, readErr := connection.ReadMessage()
		if readErr != nil {
			t.Fatal(readErr)
		}
		envelope, decodeErr := protocol.Decode(responseData)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if responseErr := session.handleResponse(envelope); responseErr != nil {
			t.Fatalf("late response error = %v", responseErr)
		}
	}
	finalDone := make(chan error, 1)
	go func() {
		_, requestErr := session.request(t.Context(), "node.invoke", []byte(`{}`))
		finalDone <- requestErr
	}()
	<-finalRequestSeen
	_, responseData, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.Decode(responseData)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.handleResponse(envelope); err != nil {
		t.Fatal(err)
	}
	if err := <-finalDone; err != nil {
		t.Fatalf("request after cancellation burst = %v", err)
	}
}

func waitForPeerPending(t *testing.T, session *peer, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		session.pendingMu.Lock()
		got := len(session.pending)
		session.pendingMu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending request count did not reach %d", want)
}

type stubPeerConnection struct {
	blockWrites  bool
	writeStarted chan struct{}
	unblockWrite chan struct{}
	startOnce    sync.Once
	unblockOnce  sync.Once
}

func newStubPeerConnection() *stubPeerConnection {
	return &stubPeerConnection{
		writeStarted: make(chan struct{}),
		unblockWrite: make(chan struct{}),
	}
}

func (connection *stubPeerConnection) Close() error {
	connection.unblockOnce.Do(func() { close(connection.unblockWrite) })
	return nil
}

func (*stubPeerConnection) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("not implemented")
}

func (*stubPeerConnection) SetPongHandler(func(string) error) {}

func (*stubPeerConnection) SetReadDeadline(time.Time) error { return nil }

func (connection *stubPeerConnection) SetWriteDeadline(deadline time.Time) error {
	if !deadline.After(time.Now()) {
		connection.unblockOnce.Do(func() { close(connection.unblockWrite) })
	}
	return nil
}

func (*stubPeerConnection) WriteControl(int, []byte, time.Time) error { return nil }

func (connection *stubPeerConnection) WriteMessage(int, []byte) error {
	connection.startOnce.Do(func() { close(connection.writeStarted) })
	if !connection.blockWrites {
		return nil
	}
	<-connection.unblockWrite
	return errors.New("write interrupted")
}
