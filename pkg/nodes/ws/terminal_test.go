package ws

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestTerminalEventSubscriptionAppliesByteAccurateBackpressure(t *testing.T) {
	subscription := &terminalEventSubscription{
		request: nodes.TerminalSessionRequest{
			TerminalID: "terminal_test",
			Owner:      testTerminalOwner(),
		},
		events: make(
			chan terminalEventResult,
			nodes.MaxTerminalTransportBuffer/nodes.MinTerminalTransportEventCharge,
		),
	}
	frameSize := nodes.MaxTerminalTransportFrameBytes
	frame := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", frameSize)))
	var cursor uint64
	for index := 0; index < nodes.MaxTerminalTransportBuffer/frameSize; index++ {
		cursor += uint64(frameSize)
		if err := subscription.offer(nodes.TerminalEvent{
			Version:    nodes.TerminalProtocolVersion,
			TerminalID: "terminal_test",
			Type:       "output",
			Cursor:     cursor,
			DataBase64: frame,
		}); err != nil {
			t.Fatalf("offer frame %d: %v", index, err)
		}
	}
	if err := subscription.offer(nodes.TerminalEvent{
		Version:          nodes.TerminalProtocolVersion,
		TerminalID:       "terminal_test",
		Type:             "ack",
		State:            "live",
		AcceptedSequence: 1,
	}); !errors.Is(err, ErrTerminalEventBackpressure) {
		t.Fatalf("overflow offer error = %v", err)
	}
	if _, err := subscription.receive(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := subscription.offer(nodes.TerminalEvent{
		Version:          nodes.TerminalProtocolVersion,
		TerminalID:       "terminal_test",
		Type:             "ack",
		State:            "live",
		AcceptedSequence: 1,
	}); err != nil {
		t.Fatalf("released bytes did not admit bounded event: %v", err)
	}
}

func TestTerminalSubscriptionFailureDoesNotBlockOrDropQueuedEvents(t *testing.T) {
	subscription := &terminalEventSubscription{
		request: nodes.TerminalSessionRequest{
			TerminalID: "terminal_test",
			Owner:      testTerminalOwner(),
		},
		events: make(chan terminalEventResult, 2),
	}
	want := nodes.TerminalEvent{
		Version: nodes.TerminalProtocolVersion, TerminalID: "terminal_test",
		Type: "ack", State: "live", AcceptedSequence: 1,
	}
	if err := subscription.offer(want); err != nil {
		t.Fatal(err)
	}
	subscription.fail(ErrNodeDisconnected)
	got, err := subscription.receive(t.Context())
	if err != nil || got != want {
		t.Fatalf("queued event = (%#v, %v)", got, err)
	}
	if _, err := subscription.receive(context.Background()); !errors.Is(err, ErrNodeDisconnected) {
		t.Fatalf("terminal failure = %v", err)
	}
}

func TestTerminalSubscriptionRejectsInvalidEventsAndCursorDiscontinuity(t *testing.T) {
	subscription := &terminalEventSubscription{
		request: nodes.TerminalSessionRequest{
			TerminalID: "terminal_test",
			Owner:      testTerminalOwner(),
		},
		events: make(chan terminalEventResult, 4),
	}
	if err := subscription.offer(nodes.TerminalEvent{
		Version: nodes.TerminalProtocolVersion, TerminalID: "terminal_test",
		Type: "mystery",
	}); !errors.Is(err, nodes.ErrInvalidTerminal) {
		t.Fatalf("unknown event error = %v", err)
	}
	data := base64.StdEncoding.EncodeToString([]byte("x"))
	if err := subscription.offer(nodes.TerminalEvent{
		Version: nodes.TerminalProtocolVersion, TerminalID: "terminal_test",
		Type: "output", Cursor: 1, DataBase64: data,
	}); err != nil {
		t.Fatal(err)
	}
	if err := subscription.offer(nodes.TerminalEvent{
		Version: nodes.TerminalProtocolVersion, TerminalID: "terminal_test",
		Type: "output", Cursor: 3, DataBase64: data,
	}); !errors.Is(err, nodes.ErrInvalidTerminal) {
		t.Fatalf("discontinuous cursor error = %v", err)
	}
}

func TestGuaranteedTerminalDetachFailClosesPeerWhenRequestSlotsSaturated(t *testing.T) {
	connection := newStubPeerConnection()
	session := newPeer(connection)
	session.markReady()
	for index := 0; index < maxOutstandingRequests; index++ {
		session.requestSlots <- struct{}{}
	}
	closed, err := session.detachTerminalGuaranteed(nodes.TerminalSessionRequest{
		TerminalID: "terminal_test",
		Owner:      testTerminalOwner(),
	})
	if !closed || !errors.Is(err, ErrRequestLimit) {
		t.Fatalf("guaranteed detach = (closed %v, error %v)", closed, err)
	}
	select {
	case <-session.closed:
	default:
		t.Fatal("saturated detach failure left authenticated peer open")
	}
}

func TestTerminalStreamCloseUsesInternalCleanupAfterCallerCancellation(t *testing.T) {
	connection := newTerminalRecordingConnection()
	session := newPeer(connection)
	session.markReady()
	request := nodes.TerminalSessionRequest{
		TerminalID: "terminal_test",
		Owner:      testTerminalOwner(),
	}
	subscription, err := session.subscribeTerminal(request)
	if err != nil {
		t.Fatal(err)
	}
	stream := &TerminalStream{
		session: session, subscription: subscription, request: request,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- stream.Close(ctx) }()
	detach := <-connection.writes
	if detach.Method != "node.terminal.detach" {
		t.Fatalf("cleanup method = %q", detach.Method)
	}
	ok := true
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: detach.ID, OK: &ok,
		Result: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.closed:
		t.Fatal("confirmed detach unnecessarily closed authenticated peer")
	default:
	}
}

func TestAttachPostDispatchCancellationPerformsOwnerBoundDetach(t *testing.T) {
	connection := newTerminalRecordingConnection()
	session := newPeer(connection)
	session.markReady()
	hub := NewSessionHub()
	release, err := hub.Claim(nodes.ID("node_test"), session, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	handler := &AdmissionHandler{sessions: hub}
	request := nodes.TerminalSessionRequest{
		TerminalID: "terminal_test",
		Owner:      testTerminalOwner(),
	}
	ctx, cancel := context.WithCancel(t.Context())
	attachDone := make(chan error, 1)
	go func() {
		_, _, attachErr := handler.AttachTerminal(ctx, nodes.ID("node_test"), request)
		attachDone <- attachErr
	}()
	attach := <-connection.writes
	if attach.Method != "node.terminal.attach" {
		t.Fatalf("first method = %q", attach.Method)
	}
	cancel()
	detach := <-connection.writes
	if detach.Method != "node.terminal.detach" {
		t.Fatalf("post-dispatch cleanup method = %q", detach.Method)
	}
	ok := true
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: detach.ID, OK: &ok,
		Result: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-attachDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("AttachTerminal() error = %v", err)
	}
	session.terminalMu.Lock()
	_, attached := session.terminals[request.TerminalID]
	session.terminalMu.Unlock()
	if attached {
		t.Fatal("failed attach retained terminal event subscription")
	}
}

func testTerminalOwner() nodes.TerminalOwner {
	return nodes.TerminalOwner{
		ActorID:     "actor_test",
		AgentID:     "agent_test",
		RouteID:     "route_test",
		SessionID:   "session_test",
		WorkspaceID: "workspace_test",
		Target:      "target_test",
		Profile:     "owner",
	}
}

type terminalRecordingConnection struct {
	writes    chan protocol.Envelope
	closed    chan struct{}
	closeOnce sync.Once
}

func newTerminalRecordingConnection() *terminalRecordingConnection {
	return &terminalRecordingConnection{
		writes: make(chan protocol.Envelope, 4),
		closed: make(chan struct{}),
	}
}

func (connection *terminalRecordingConnection) Close() error {
	connection.closeOnce.Do(func() { close(connection.closed) })
	return nil
}

func (*terminalRecordingConnection) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("not implemented")
}

func (*terminalRecordingConnection) SetPongHandler(func(string) error)         {}
func (*terminalRecordingConnection) SetReadDeadline(time.Time) error           { return nil }
func (*terminalRecordingConnection) SetWriteDeadline(time.Time) error          { return nil }
func (*terminalRecordingConnection) WriteControl(int, []byte, time.Time) error { return nil }

func (connection *terminalRecordingConnection) WriteMessage(
	messageType int,
	data []byte,
) error {
	if messageType != websocket.TextMessage {
		return errors.New("unexpected non-text message")
	}
	envelope, err := protocol.Decode(data)
	if err != nil {
		return err
	}
	select {
	case connection.writes <- envelope:
		return nil
	case <-connection.closed:
		return ErrNodeDisconnected
	}
}
