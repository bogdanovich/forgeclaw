package ws

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestTerminalDispatchConsumesResponseAfterCommittedWarning(t *testing.T) {
	connection := newTerminalRecordingConnection()
	session := newPeer(connection)
	session.markReady()
	commitCause := errors.New("directory sync failed")
	commitErr := &fileutil.CommittedWriteError{Err: commitCause}
	var commitWarning error
	done := make(chan struct {
		response   protocol.Envelope
		dispatched bool
		err        error
	}, 1)
	go func() {
		response, dispatched, requestErr := session.request(
			t.Context(),
			"node.terminal.open",
			json.RawMessage(`{}`),
			"idem_warning",
			func(write func() error) error {
				return commitTerminalDispatch(
					func() error { return commitErr },
					write,
					&commitWarning,
				)
			},
		)
		done <- struct {
			response   protocol.Envelope
			dispatched bool
			err        error
		}{response: response, dispatched: dispatched, err: requestErr}
	}()

	open := <-connection.writes
	if open.Method != "node.terminal.open" {
		t.Fatalf("method = %q", open.Method)
	}
	ok := true
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: open.ID, OK: &ok, Result: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if !got.dispatched ||
		got.response.ID != open.ID ||
		got.err != nil ||
		!errors.Is(commitWarning, commitCause) ||
		!fileutil.IsCommittedWriteError(commitWarning) {
		t.Fatalf(
			"terminal dispatch = (response %#v, dispatched %v, error %v, warning %v)",
			got.response,
			got.dispatched,
			got.err,
			commitWarning,
		)
	}
	session.pendingMu.Lock()
	_, abandoned := session.abandoned[open.ID]
	session.pendingMu.Unlock()
	if abandoned {
		t.Fatal("terminal response correlation was abandoned after committed warning")
	}
}

func TestTerminalOpenResponseRetainsValidatedIdentityForInvalidState(t *testing.T) {
	owner := testTerminalOwner()
	result, err := json.Marshal(nodes.TerminalMetadata{
		TerminalID: "terminal_invalid_state",
		Owner:      owner,
		State:      "live",
		StartedAt:  time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ok := true
	metadata, err := decodeTerminalOpenResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: "req_open", OK: &ok, Result: result,
	}, owner)
	if err == nil || metadata.TerminalID != "terminal_invalid_state" ||
		metadata.Owner != owner {
		t.Fatalf("decodeTerminalOpenResponse() = (%#v, %v)", metadata, err)
	}
}

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

func TestTerminateTerminalAttachesClosesAndConfirmsStatus(t *testing.T) {
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
		TerminalID: "terminal_cleanup",
		Owner:      testTerminalOwner(),
	}
	done := make(chan struct {
		metadata nodes.TerminalMetadata
		err      error
	}, 1)
	go func() {
		metadata, terminateErr := handler.TerminateTerminal(
			t.Context(),
			nodes.ID("node_test"),
			request,
		)
		done <- struct {
			metadata nodes.TerminalMetadata
			err      error
		}{metadata: metadata, err: terminateErr}
	}()
	attach := <-connection.writes
	if attach.Method != "node.terminal.attach" {
		t.Fatalf("first method = %q", attach.Method)
	}
	ok := true
	live, err := json.Marshal(nodes.TerminalMetadata{
		TerminalID: request.TerminalID, Owner: request.Owner,
		State: "live", StartedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: attach.ID, OK: &ok, Result: live,
	}); err != nil {
		t.Fatal(err)
	}
	detach := <-connection.writes
	if detach.Method != "node.terminal.detach" {
		t.Fatalf("second method = %q", detach.Method)
	}
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: detach.ID, OK: &ok,
		Result: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	status := <-connection.writes
	if status.Method != "node.terminal.status" {
		t.Fatalf("third method = %q", status.Method)
	}
	closed, err := json.Marshal(nodes.TerminalMetadata{
		TerminalID: request.TerminalID, Owner: request.Owner,
		State: "closed", Reason: "close", StartedAt: 1, CompletedAt: 2,
		TerminationConfirmed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: status.ID, OK: &ok, Result: closed,
	}); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil ||
		result.metadata.State != "closed" ||
		!result.metadata.TerminationConfirmed {
		t.Fatalf("TerminateTerminal() = (%#v, %v)", result.metadata, result.err)
	}
}

func TestRejectedTerminalAttachDoesNotRetainTombstone(t *testing.T) {
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
		TerminalID: "terminal_rejected",
		Owner:      testTerminalOwner(),
	}
	attachDone := make(chan error, 1)
	go func() {
		_, _, attachErr := handler.AttachTerminal(t.Context(), nodes.ID("node_test"), request)
		attachDone <- attachErr
	}()
	attach := <-connection.writes
	ok := false
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: attach.ID, OK: &ok,
		Error: &protocol.Error{Code: "TERMINAL_NOT_FOUND", Message: "not found"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-attachDone; err == nil {
		t.Fatal("rejected terminal attach succeeded")
	}
	session.terminalMu.Lock()
	abandoned := len(session.abandonedTerminals)
	session.terminalMu.Unlock()
	if abandoned != 0 {
		t.Fatalf("definitively rejected attach retained %d tombstones", abandoned)
	}
	select {
	case <-session.closed:
		t.Fatal("definitively rejected attach closed healthy peer")
	default:
	}
}

func TestTerminalTombstoneLimitFailClosesPeer(t *testing.T) {
	connection := newStubPeerConnection()
	session := newPeer(connection)
	session.markReady()
	session.terminalMu.Lock()
	for index := 0; index < maxAbandonedTerminals; index++ {
		session.abandonedTerminals[fmt.Sprintf("terminal_abandoned_%d", index)] = struct{}{}
	}
	session.terminalMu.Unlock()
	request := nodes.TerminalSessionRequest{
		TerminalID: "terminal_overflow",
		Owner:      testTerminalOwner(),
	}
	subscription, err := session.subscribeTerminal(request)
	if err != nil {
		t.Fatal(err)
	}
	session.unsubscribeTerminal(request.TerminalID, subscription, ErrNodeDisconnected, true)
	select {
	case <-session.closed:
	default:
		t.Fatal("terminal tombstone overflow left authenticated peer open")
	}
	session.terminalMu.Lock()
	abandoned := len(session.abandonedTerminals)
	session.terminalMu.Unlock()
	if abandoned > maxAbandonedTerminals {
		t.Fatalf("terminal tombstones grew to %d", abandoned)
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

func TestAttachUnrelatedSuccessPerformsOwnerBoundDetach(t *testing.T) {
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
	attachDone := make(chan error, 1)
	go func() {
		_, _, attachErr := handler.AttachTerminal(t.Context(), nodes.ID("node_test"), request)
		attachDone <- attachErr
	}()
	attach := <-connection.writes
	if attach.Method != "node.terminal.attach" {
		t.Fatalf("first method = %q", attach.Method)
	}
	result, err := json.Marshal(nodes.TerminalMetadata{
		TerminalID: "terminal_other", Owner: request.Owner,
		State: "live", StartedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ok := true
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: attach.ID, OK: &ok, Result: result,
	}); err != nil {
		t.Fatal(err)
	}
	detach := <-connection.writes
	if detach.Method != "node.terminal.detach" {
		t.Fatalf("unrelated-success cleanup method = %q", detach.Method)
	}
	if err := session.handleResponse(protocol.Envelope{
		Type: protocol.FrameResponse, ID: detach.ID, OK: &ok,
		Result: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-attachDone; err == nil {
		t.Fatal("unrelated successful attachment was accepted")
	}
	session.terminalMu.Lock()
	_, attached := session.terminals[request.TerminalID]
	session.terminalMu.Unlock()
	if attached {
		t.Fatal("unrelated successful attach retained terminal event subscription")
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
