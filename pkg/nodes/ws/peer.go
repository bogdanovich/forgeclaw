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
	ErrRequestLimit       = errors.New("node request limit reached")
)

const (
	maxOutstandingRequests = 1024
	defaultWriteTimeout    = 15 * time.Second
)

type peerConnection interface {
	Close() error
	ReadMessage() (int, []byte, error)
	SetPongHandler(func(string) error)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	WriteControl(int, []byte, time.Time) error
	WriteMessage(int, []byte) error
}

type responseResult struct {
	envelope protocol.Envelope
	err      error
}

// peer owns all application writes and response correlation for one
// authenticated WebSocket generation.
type peer struct {
	connection   peerConnection
	ready        chan struct{}
	closed       chan struct{}
	readyOnce    sync.Once
	closeOnce    sync.Once
	writeSlot    chan struct{}
	requestSlots chan struct{}
	pendingMu    sync.Mutex
	pending      map[string]chan responseResult
	abandoned    map[string]struct{}
	sequence     atomic.Uint64
}

func newPeer(connection peerConnection) *peer {
	return &peer{
		connection:   connection,
		ready:        make(chan struct{}),
		closed:       make(chan struct{}),
		writeSlot:    make(chan struct{}, 1),
		requestSlots: make(chan struct{}, maxOutstandingRequests),
		pending:      make(map[string]chan responseResult),
		abandoned:    make(map[string]struct{}),
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
		session.abandoned = make(map[string]struct{})
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
	if err := session.acquireRequestSlot(ctx); err != nil {
		return protocol.Envelope{}, err
	}

	id := fmt.Sprintf("req_%d", session.sequence.Add(1))
	result := make(chan responseResult, 1)
	session.pendingMu.Lock()
	select {
	case <-session.closed:
		session.pendingMu.Unlock()
		session.releaseRequestSlot()
		return protocol.Envelope{}, ErrNodeDisconnected
	default:
		session.pending[id] = result
	}
	session.pendingMu.Unlock()
	if err := session.writeEnvelope(ctx, protocol.Envelope{
		Type:   protocol.FrameRequest,
		ID:     id,
		Method: method,
		Params: params,
	}); err != nil {
		session.removePending(id)
		return protocol.Envelope{}, err
	}
	select {
	case <-ctx.Done():
		session.abandon(id)
		return protocol.Envelope{}, ctx.Err()
	case <-session.closed:
		session.removePending(id)
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
	_, abandoned := session.abandoned[envelope.ID]
	if abandoned {
		delete(session.abandoned, envelope.ID)
	}
	session.pendingMu.Unlock()
	if abandoned {
		session.releaseRequestSlot()
		return nil
	}
	if result == nil {
		return ErrUnexpectedResponse
	}
	session.releaseRequestSlot()
	result <- responseResult{envelope: envelope}
	return nil
}

func (session *peer) abandon(id string) {
	session.pendingMu.Lock()
	if _, exists := session.pending[id]; !exists {
		session.pendingMu.Unlock()
		return
	}
	delete(session.pending, id)
	session.abandoned[id] = struct{}{}
	session.pendingMu.Unlock()
}

func (session *peer) removePending(id string) {
	session.pendingMu.Lock()
	_, exists := session.pending[id]
	if exists {
		delete(session.pending, id)
	}
	session.pendingMu.Unlock()
	if exists {
		session.releaseRequestSlot()
	}
}

func (session *peer) acquireRequestSlot(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.closed:
		return ErrNodeDisconnected
	default:
	}
	select {
	case session.requestSlots <- struct{}{}:
		return nil
	default:
		return ErrRequestLimit
	}
}

func (session *peer) releaseRequestSlot() {
	<-session.requestSlots
}

func (session *peer) writeEnvelope(ctx context.Context, envelope protocol.Envelope) error {
	data, err := protocol.Encode(envelope)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, defaultWriteTimeout)
	defer cancel()
	select {
	case session.writeSlot <- struct{}{}:
		defer func() { <-session.writeSlot }()
	case <-writeCtx.Done():
		return writeCtx.Err()
	case <-session.closed:
		return ErrNodeDisconnected
	}
	select {
	case <-session.closed:
		return ErrNodeDisconnected
	case <-writeCtx.Done():
		return writeCtx.Err()
	default:
	}
	deadline, _ := writeCtx.Deadline()
	if err := session.connection.SetWriteDeadline(deadline); err != nil {
		_ = session.Close()
		return err
	}
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(writeCtx, func() {
		_ = session.connection.SetWriteDeadline(time.Now())
		close(cancelDone)
	})
	writeErr := session.connection.WriteMessage(websocket.TextMessage, data)
	if !stopCancel() {
		<-cancelDone
	}
	if writeErr != nil {
		_ = session.Close()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return writeErr
}

func (session *peer) writeControl(messageType int, data []byte, deadline time.Time) error {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	select {
	case session.writeSlot <- struct{}{}:
		defer func() { <-session.writeSlot }()
	case <-ctx.Done():
		return ctx.Err()
	case <-session.closed:
		return ErrNodeDisconnected
	}
	select {
	case <-session.closed:
		return ErrNodeDisconnected
	default:
	}
	return session.connection.WriteControl(messageType, data, deadline)
}
