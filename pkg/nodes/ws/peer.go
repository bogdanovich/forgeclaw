package ws

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

var (
	ErrNodeDisconnected          = errors.New("node is not connected")
	ErrUnexpectedResponse        = errors.New("node returned an unexpected response")
	ErrRequestLimit              = errors.New("node request limit reached")
	ErrTerminalEventBackpressure = errors.New("terminal event transport exceeded its bounded buffer")
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

type terminalEventResult struct {
	event nodes.TerminalEvent
	size  int
}

type terminalEventSubscription struct {
	request nodes.TerminalSessionRequest
	events  chan terminalEventResult

	mu          sync.Mutex
	queuedBytes int
	closed      bool
	err         error
}

// peer owns all application writes and response correlation for one
// authenticated WebSocket generation.
type peer struct {
	connection         peerConnection
	ready              chan struct{}
	closed             chan struct{}
	readyOnce          sync.Once
	closeOnce          sync.Once
	writeSlot          chan struct{}
	requestSlots       chan struct{}
	pendingMu          sync.Mutex
	pending            map[string]chan responseResult
	abandoned          map[string]struct{}
	sequence           atomic.Uint64
	terminalMu         sync.Mutex
	terminals          map[string]*terminalEventSubscription
	abandonedTerminals map[string]struct{}
}

func newPeer(connection peerConnection) *peer {
	return &peer{
		connection:         connection,
		ready:              make(chan struct{}),
		closed:             make(chan struct{}),
		writeSlot:          make(chan struct{}, 1),
		requestSlots:       make(chan struct{}, maxOutstandingRequests),
		pending:            make(map[string]chan responseResult),
		abandoned:          make(map[string]struct{}),
		terminals:          make(map[string]*terminalEventSubscription),
		abandonedTerminals: make(map[string]struct{}),
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
		session.terminalMu.Lock()
		terminals := session.terminals
		session.terminals = make(map[string]*terminalEventSubscription)
		session.abandonedTerminals = make(map[string]struct{})
		session.terminalMu.Unlock()
		for _, result := range pending {
			result <- responseResult{err: ErrNodeDisconnected}
		}
		for _, subscription := range terminals {
			subscription.fail(ErrNodeDisconnected)
		}
	})
	return closeErr
}

func (session *peer) subscribeTerminal(
	request nodes.TerminalSessionRequest,
) (*terminalEventSubscription, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	subscription := &terminalEventSubscription{
		request: request,
		events: make(
			chan terminalEventResult,
			nodes.MaxTerminalTransportBuffer/nodes.MinTerminalTransportEventCharge,
		),
	}
	session.terminalMu.Lock()
	defer session.terminalMu.Unlock()
	select {
	case <-session.closed:
		return nil, ErrNodeDisconnected
	default:
	}
	if session.terminals[request.TerminalID] != nil {
		return nil, errors.New("terminal event subscription already exists")
	}
	delete(session.abandonedTerminals, request.TerminalID)
	session.terminals[request.TerminalID] = subscription
	return subscription, nil
}

func (session *peer) unsubscribeTerminal(
	terminalID string,
	subscription *terminalEventSubscription,
	err error,
) {
	session.terminalMu.Lock()
	if session.terminals[terminalID] == subscription {
		delete(session.terminals, terminalID)
		session.abandonedTerminals[terminalID] = struct{}{}
	}
	session.terminalMu.Unlock()
	subscription.fail(err)
}

func (session *peer) handleTerminalEvent(
	envelope protocol.Envelope,
) (*nodes.TerminalSessionRequest, error) {
	if envelope.Event != "node.terminal.event" {
		return nil, errors.New("node sent an unsupported event")
	}
	var payload struct {
		TerminalID string              `json:"terminal_id"`
		Event      nodes.TerminalEvent `json:"event"`
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode terminal event: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode terminal event: trailing data")
	}
	if payload.TerminalID == "" || payload.Event.TerminalID != payload.TerminalID {
		return nil, errors.New("node sent an unrelated terminal event")
	}
	size := 1
	if payload.Event.DataBase64 != "" {
		data, err := base64.StdEncoding.Strict().DecodeString(payload.Event.DataBase64)
		if err != nil || len(data) == 0 || len(data) > nodes.MaxTerminalTransportFrameBytes {
			return nil, errors.New("node sent an invalid terminal output frame")
		}
		size = len(data)
	}
	session.terminalMu.Lock()
	subscription := session.terminals[payload.TerminalID]
	_, abandoned := session.abandonedTerminals[payload.TerminalID]
	session.terminalMu.Unlock()
	if subscription == nil {
		if abandoned {
			return nil, nil
		}
		return nil, errors.New("node sent an event for an unattached terminal")
	}
	if err := subscription.offer(payload.Event, size); err != nil {
		request := subscription.request
		session.unsubscribeTerminal(payload.TerminalID, subscription, err)
		return &request, err
	}
	return nil, nil
}

func (subscription *terminalEventSubscription) offer(
	event nodes.TerminalEvent,
	size int,
) error {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		return subscription.err
	}
	charge := max(size, nodes.MinTerminalTransportEventCharge)
	if size <= 0 || subscription.queuedBytes+charge > nodes.MaxTerminalTransportBuffer {
		return ErrTerminalEventBackpressure
	}
	select {
	case subscription.events <- terminalEventResult{event: event, size: charge}:
		subscription.queuedBytes += charge
		return nil
	default:
		return ErrTerminalEventBackpressure
	}
}

func (subscription *terminalEventSubscription) receive(
	ctx context.Context,
) (nodes.TerminalEvent, error) {
	select {
	case <-ctx.Done():
		return nodes.TerminalEvent{}, ctx.Err()
	case result, ok := <-subscription.events:
		if !ok {
			subscription.mu.Lock()
			err := subscription.err
			subscription.mu.Unlock()
			if err == nil {
				err = ErrNodeDisconnected
			}
			return nodes.TerminalEvent{}, err
		}
		subscription.mu.Lock()
		subscription.queuedBytes -= result.size
		subscription.mu.Unlock()
		return result.event, nil
	}
}

func (subscription *terminalEventSubscription) fail(err error) {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		return
	}
	subscription.closed = true
	subscription.err = err
	close(subscription.events)
}

func (session *peer) request(
	ctx context.Context,
	method string,
	params json.RawMessage,
	idempotencyKey string,
	dispatch func(func() error) error,
) (protocol.Envelope, bool, error) {
	select {
	case <-ctx.Done():
		return protocol.Envelope{}, false, ctx.Err()
	case <-session.closed:
		return protocol.Envelope{}, false, ErrNodeDisconnected
	case <-session.ready:
	}
	if err := session.acquireRequestSlot(ctx); err != nil {
		return protocol.Envelope{}, false, err
	}

	id := fmt.Sprintf("req_%d", session.sequence.Add(1))
	result := make(chan responseResult, 1)
	session.pendingMu.Lock()
	select {
	case <-session.closed:
		session.pendingMu.Unlock()
		session.releaseRequestSlot()
		return protocol.Envelope{}, false, ErrNodeDisconnected
	default:
		session.pending[id] = result
	}
	session.pendingMu.Unlock()
	dispatched, err := session.writeEnvelopeAtDispatch(ctx, protocol.Envelope{
		Type:           protocol.FrameRequest,
		ID:             id,
		Method:         method,
		Params:         params,
		IdempotencyKey: idempotencyKey,
	}, dispatch)
	if err != nil {
		if dispatched {
			session.abandon(id)
		} else {
			session.removePending(id)
		}
		return protocol.Envelope{}, dispatched, err
	}
	select {
	case <-ctx.Done():
		session.abandon(id)
		return protocol.Envelope{}, true, ctx.Err()
	case <-session.closed:
		session.removePending(id)
		return protocol.Envelope{}, true, ErrNodeDisconnected
	case response := <-result:
		return response.envelope, true, response.err
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
	_, err := session.writeEnvelopeAtDispatch(ctx, envelope, nil)
	return err
}

func (session *peer) writeEnvelopeAtDispatch(
	ctx context.Context,
	envelope protocol.Envelope,
	dispatch func(func() error) error,
) (bool, error) {
	data, err := protocol.Encode(envelope)
	if err != nil {
		return false, err
	}
	writeCtx, cancel := context.WithTimeout(ctx, defaultWriteTimeout)
	defer cancel()
	select {
	case session.writeSlot <- struct{}{}:
		defer func() { <-session.writeSlot }()
	case <-writeCtx.Done():
		return false, writeCtx.Err()
	case <-session.closed:
		return false, ErrNodeDisconnected
	}
	select {
	case <-session.closed:
		return false, ErrNodeDisconnected
	case <-writeCtx.Done():
		return false, writeCtx.Err()
	default:
	}
	deadline, _ := writeCtx.Deadline()
	if err := session.connection.SetWriteDeadline(deadline); err != nil {
		_ = session.Close()
		return false, err
	}
	dispatched := false
	var transportErr error
	write := func() error {
		if dispatched {
			return errors.New("node request frame write called more than once")
		}
		dispatched = true
		cancelDone := make(chan struct{})
		stopCancel := context.AfterFunc(writeCtx, func() {
			_ = session.connection.SetWriteDeadline(time.Now())
			close(cancelDone)
		})
		transportErr = session.connection.WriteMessage(websocket.TextMessage, data)
		if !stopCancel() {
			<-cancelDone
		}
		return transportErr
	}
	var writeErr error
	if dispatch == nil {
		writeErr = write()
	} else {
		writeErr = dispatch(write)
		if writeErr == nil && !dispatched {
			writeErr = errors.New("node dispatch completed without writing request frame")
		}
	}
	if transportErr != nil {
		_ = session.Close()
		if ctx.Err() != nil {
			return dispatched, ctx.Err()
		}
	}
	return dispatched, writeErr
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
