package ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func TestPeerBoundsAbandonedRequestTombstones(t *testing.T) {
	session := &peer{
		pending:   make(map[string]chan responseResult),
		abandoned: make(map[string]struct{}),
	}
	for index := range maxAbandonedRequests + 1 {
		id := fmt.Sprintf("req_%d", index)
		session.pending[id] = make(chan responseResult, 1)
		session.abandon(id)
	}
	if len(session.abandoned) != maxAbandonedRequests ||
		len(session.abandonedOrder) != maxAbandonedRequests {
		t.Fatalf(
			"abandoned tombstones = %d, order = %d",
			len(session.abandoned),
			len(session.abandonedOrder),
		)
	}
	if _, exists := session.abandoned["req_0"]; exists {
		t.Fatal("oldest abandoned request was not evicted")
	}
}
