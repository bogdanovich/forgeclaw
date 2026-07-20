package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sipeed/picoclaw/pkg/nodes/protocol"
)

var (
	ErrNodeDisconnected   = errors.New("node is not connected")
	ErrUnexpectedResponse = errors.New("node returned an unexpected response")
)

type responseResult struct {
	envelope protocol.Envelope
	err      error
}

// peer owns all application writes and response correlation for one
// authenticated WebSocket generation.
type peer struct {
	connection *websocket.Conn
	ready      chan struct{}
	closed     chan struct{}
	readyOnce  sync.Once
	closeOnce  sync.Once
	writeMu    sync.Mutex
	pendingMu  sync.Mutex
	pending    map[string]chan responseResult
	sequence   atomic.Uint64
}

func newPeer(connection *websocket.Conn) *peer {
	return &peer{
		connection: connection,
		ready:      make(chan struct{}),
		closed:     make(chan struct{}),
		pending:    make(map[string]chan responseResult),
	}
}

func (session *peer) markReady() {
	session.readyOnce.Do(func() { close(session.ready) })
}

func (session *peer) Close() error {
	var closeErr error
	session.closeOnce.Do(func() {
		close(session.closed)
		closeErr = session.connection.Close()
		session.pendingMu.Lock()
		pending := session.pending
		session.pending = make(map[string]chan responseResult)
		session.pendingMu.Unlock()
		for _, result := range pending {
			result <- responseResult{err: ErrNodeDisconnected}
		}
	})
	return closeErr
}

func (session *peer) request(
	ctx context.Context,
	method string,
	params json.RawMessage,
) (protocol.Envelope, error) {
	select {
	case <-ctx.Done():
		return protocol.Envelope{}, ctx.Err()
	case <-session.closed:
		return protocol.Envelope{}, ErrNodeDisconnected
	case <-session.ready:
	}

	id := fmt.Sprintf("req_%d", session.sequence.Add(1))
	result := make(chan responseResult, 1)
	session.pendingMu.Lock()
	select {
	case <-session.closed:
		session.pendingMu.Unlock()
		return protocol.Envelope{}, ErrNodeDisconnected
	default:
		session.pending[id] = result
	}
	session.pendingMu.Unlock()
	defer session.removePending(id)

	if err := session.writeEnvelope(protocol.Envelope{
		Type:   protocol.FrameRequest,
		ID:     id,
		Method: method,
		Params: params,
	}); err != nil {
		return protocol.Envelope{}, err
	}
	select {
	case <-ctx.Done():
		return protocol.Envelope{}, ctx.Err()
	case <-session.closed:
		return protocol.Envelope{}, ErrNodeDisconnected
	case response := <-result:
		return response.envelope, response.err
	}
}

func (session *peer) handleResponse(envelope protocol.Envelope) error {
	session.pendingMu.Lock()
	result := session.pending[envelope.ID]
	if result != nil {
		delete(session.pending, envelope.ID)
	}
	session.pendingMu.Unlock()
	if result == nil {
		return ErrUnexpectedResponse
	}
	result <- responseResult{envelope: envelope}
	return nil
}

func (session *peer) removePending(id string) {
	session.pendingMu.Lock()
	delete(session.pending, id)
	session.pendingMu.Unlock()
}

func (session *peer) writeEnvelope(envelope protocol.Envelope) error {
	data, err := protocol.Encode(envelope)
	if err != nil {
		return err
	}
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	select {
	case <-session.closed:
		return ErrNodeDisconnected
	default:
	}
	return session.connection.WriteMessage(websocket.TextMessage, data)
}

func (session *peer) writeControl(messageType int, data []byte, deadline time.Time) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	return session.connection.WriteControl(messageType, data, deadline)
}
