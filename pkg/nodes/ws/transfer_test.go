package ws

import (
	"context"
	"crypto/sha256"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestTransferStreamUsesAuthenticatedPeerGeneration(t *testing.T) {
	t.Parallel()
	hub := NewSessionHub()
	nodeID := nodes.ID("n_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	connection := &transferRecordingConnection{}
	session := newPeer(connection)
	session.markReady()
	release, err := hub.Claim(nodeID, session, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = release()
		_ = hub.Close(context.Background())
	})
	binding := testTransferBinding()
	stream, err := hub.OpenTransfer(t.Context(), nodeID, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	outbound := testTransferFrame(binding, protocol.TransferFrameChunk, 1, []byte("payload"))
	if sendErr := stream.Send(t.Context(), outbound); sendErr != nil {
		t.Fatal(sendErr)
	}
	messageType, data := connection.lastWrite()
	if messageType != websocket.BinaryMessage {
		t.Fatalf("transfer message type = %d", messageType)
	}
	decoded, err := protocol.DecodeTransferFrame(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.TransferID != binding.TransferID ||
		decoded.Sequence != 1 ||
		string(decoded.Payload) != "payload" {
		t.Fatalf("decoded outbound frame = %#v", decoded)
	}

	inbound := testTransferFrame(binding, protocol.TransferFrameAck, 1, nil)
	if handleErr := session.handleTransferFrame(inbound); handleErr != nil {
		t.Fatal(handleErr)
	}
	received, err := stream.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if received.Type != protocol.TransferFrameAck || received.Sequence != 1 {
		t.Fatalf("received frame = %#v", received)
	}

	replacement := newPeer(&transferRecordingConnection{})
	replacement.markReady()
	releaseReplacement, err := hub.Claim(nodeID, replacement, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = releaseReplacement() }()
	if _, err := stream.Receive(t.Context()); !errors.Is(err, ErrNodeDisconnected) {
		t.Fatalf("Receive() after peer replacement error = %v", err)
	}
}

func TestTransferStreamRejectsBindingAndSequenceChanges(t *testing.T) {
	t.Parallel()
	binding := testTransferBinding()
	_, session, stream := openTestTransferStream(t, &transferRecordingConnection{}, binding)

	changed := testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil)
	changed.PolicyRevision = "other-policy"
	if err := stream.Send(t.Context(), changed); !errors.Is(err, protocol.ErrInvalidTransferFrame) {
		t.Fatalf("Send() changed binding error = %v", err)
	}
	gap := testTransferFrame(binding, protocol.TransferFrameChunk, 2, []byte("payload"))
	if err := stream.Send(t.Context(), gap); !errors.Is(err, protocol.ErrInvalidTransferFrame) {
		t.Fatalf("Send() sequence gap error = %v", err)
	}
	if err := session.handleTransferFrame(gap); !errors.Is(err, protocol.ErrInvalidTransferFrame) {
		t.Fatalf("handleTransferFrame() sequence gap error = %v", err)
	}
}

func TestTransferStreamRejectsChunksBeyondDeclaredSize(t *testing.T) {
	t.Parallel()
	binding := testTransferBinding()
	_, session, stream := openTestTransferStream(t, &transferRecordingConnection{}, binding)

	full := testTransferFrame(binding, protocol.TransferFrameChunk, 1, []byte("payload"))
	if err := stream.Send(t.Context(), full); err != nil {
		t.Fatal(err)
	}
	overflow := testTransferFrame(binding, protocol.TransferFrameChunk, 2, []byte("x"))
	if err := stream.Send(t.Context(), overflow); !errors.Is(err, protocol.ErrInvalidTransferFrame) {
		t.Fatalf("Send() oversized stream error = %v", err)
	}
	if err := session.handleTransferFrame(full); err != nil {
		t.Fatal(err)
	}
	if err := session.handleTransferFrame(overflow); !errors.Is(err, protocol.ErrInvalidTransferFrame) {
		t.Fatalf("handleTransferFrame() oversized stream error = %v", err)
	}
}

func TestTransferStreamInvalidAcknowledgementFailsClosed(t *testing.T) {
	t.Parallel()
	binding := testTransferBinding()
	_, session, stream := openTestTransferStream(t, &transferRecordingConnection{}, binding)

	chunk := testTransferFrame(binding, protocol.TransferFrameChunk, 1, []byte("payload"))
	if sendErr := stream.Send(t.Context(), chunk); sendErr != nil {
		t.Fatal(sendErr)
	}
	invalidAck := testTransferFrame(binding, protocol.TransferFrameAck, 2, nil)
	if handleErr := session.handleTransferFrame(invalidAck); handleErr != nil {
		t.Fatal(handleErr)
	}
	if _, receiveErr := stream.Receive(t.Context()); !errors.Is(
		receiveErr,
		protocol.ErrInvalidTransferFrame,
	) {
		t.Fatalf("Receive() invalid acknowledgement error = %v", receiveErr)
	}
	status := testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil)
	if sendErr := stream.Send(t.Context(), status); !errors.Is(sendErr, ErrNodeDisconnected) {
		t.Fatalf("Send() after invalid acknowledgement error = %v", sendErr)
	}
	if _, subscribeErr := session.subscribeTransfer(binding); subscribeErr == nil {
		t.Fatal("invalid acknowledgement allowed transfer identity reuse")
	}
}

func TestTransferSubscriptionEnforcesByteBackpressure(t *testing.T) {
	t.Parallel()
	session := newPeer(&transferRecordingConnection{})
	binding := testTransferBinding()
	binding.TotalSize = 5 * protocol.MaxTransferChunkBytes
	subscription, err := session.subscribeTransfer(binding)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, protocol.MaxTransferChunkBytes)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		frame := testTransferFrame(binding, protocol.TransferFrameChunk, sequence, payload)
		if err := session.handleTransferFrame(frame); err != nil {
			t.Fatalf("frame %d error = %v", sequence, err)
		}
	}
	overflow := testTransferFrame(binding, protocol.TransferFrameChunk, 5, payload)
	if err := session.handleTransferFrame(overflow); !errors.Is(err, ErrTransferFrameBackpressure) {
		t.Fatalf("overflow error = %v", err)
	}
	for range 4 {
		if _, err := subscription.receive(t.Context()); err != nil {
			t.Fatalf("drain queued frame error = %v", err)
		}
	}
	if _, err := subscription.receive(t.Context()); !errors.Is(err, ErrTransferFrameBackpressure) {
		t.Fatalf("receive after overflow error = %v", err)
	}
}

func TestTransferStreamTombstonesLateFrames(t *testing.T) {
	t.Parallel()
	binding := testTransferBinding()
	_, session, stream := openTestTransferStream(t, &transferRecordingConnection{}, binding)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, subscribeErr := session.subscribeTransfer(binding); subscribeErr == nil {
		t.Fatal("tombstoned transfer identity was reopened")
	}
	late := testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil)
	if err := session.handleTransferFrame(late); err != nil {
		t.Fatalf("late tombstoned frame error = %v", err)
	}
	unknown := late
	unknown.TransferID = "unknown_transfer"
	if err := session.handleTransferFrame(unknown); err == nil {
		t.Fatal("unattached transfer frame was accepted")
	}
}

func TestTransferSendCancellationInterruptsWrite(t *testing.T) {
	t.Parallel()
	connection := newCancelBlockingTransferConnection()
	binding := testTransferBinding()
	_, _, stream := openTestTransferStream(t, connection, binding)
	ctx, cancel := context.WithCancel(t.Context())
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- stream.Send(
			ctx,
			testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil),
		)
	}()
	<-connection.writeStarted
	cancel()
	select {
	case sendErr := <-sendDone:
		if !errors.Is(sendErr, context.Canceled) {
			t.Fatalf("Send() cancellation error = %v", sendErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled transfer write remained blocked")
	}
	if connection.deadlineCalls.Load() < 2 {
		t.Fatal("canceled transfer write did not advance its deadline")
	}
}

func TestTransferWriteKeepsExactPeerGenerationStable(t *testing.T) {
	t.Parallel()
	connection := newGatedTransferConnection()
	defer connection.release()
	binding := testTransferBinding()
	hub, _, stream := openTestTransferStream(t, connection, binding)
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- stream.Send(
			t.Context(),
			testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil),
		)
	}()
	<-connection.writeStarted

	replacement := newPeer(&transferRecordingConnection{})
	replacement.markReady()
	type claimResult struct {
		release func() (bool, error)
		err     error
	}
	claimDone := make(chan claimResult, 1)
	go func() {
		release, claimErr := hub.Claim(testTransferNodeID(), replacement, nil, nil)
		claimDone <- claimResult{release: release, err: claimErr}
	}()
	select {
	case result := <-claimDone:
		t.Fatalf("replacement claimed during transfer write: %v", result.err)
	case <-time.After(25 * time.Millisecond):
	}

	connection.release()
	if sendErr := <-sendDone; sendErr != nil {
		t.Fatal(sendErr)
	}
	result := <-claimDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer func() { _, _ = result.release() }()
	status := testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil)
	if sendErr := stream.Send(t.Context(), status); !errors.Is(sendErr, ErrNodeDisconnected) {
		t.Fatalf("superseded transfer Send() error = %v", sendErr)
	}
}

func TestTransferDeliverySerializesWithNormalClose(t *testing.T) {
	t.Parallel()
	binding := testTransferBinding()
	_, session, stream := openTestTransferStream(t, &transferRecordingConnection{}, binding)
	stream.subscription.mu.Lock()
	offerBlocked := true
	defer func() {
		if offerBlocked {
			stream.subscription.mu.Unlock()
		}
	}()
	handleDone := make(chan error, 1)
	go func() {
		handleDone <- session.handleTransferFrame(
			testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil),
		)
	}()
	waitForMutexHeld(t, &session.transferMu)
	closeDone := make(chan error, 1)
	go func() { closeDone <- stream.Close() }()
	select {
	case closeErr := <-closeDone:
		t.Fatalf("Close() bypassed in-progress delivery: %v", closeErr)
	case <-time.After(25 * time.Millisecond):
	}
	stream.subscription.mu.Unlock()
	offerBlocked = false
	if handleErr := <-handleDone; handleErr != nil {
		t.Fatalf("frame delivery racing Close() error = %v", handleErr)
	}
	if closeErr := <-closeDone; closeErr != nil {
		t.Fatal(closeErr)
	}
	late := testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil)
	if handleErr := session.handleTransferFrame(late); handleErr != nil {
		t.Fatalf("late frame after Close() error = %v", handleErr)
	}
}

func TestTransferSendAndReceiveUseOneLockOrder(t *testing.T) {
	t.Parallel()
	binding := testTransferBinding()
	_, session, stream := openTestTransferStream(t, &transferRecordingConnection{}, binding)
	inbound := testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil)
	if handleErr := session.handleTransferFrame(inbound); handleErr != nil {
		t.Fatal(handleErr)
	}

	stream.stateMu.Lock()
	stateHeld := true
	defer func() {
		if stateHeld {
			stream.stateMu.Unlock()
		}
	}()
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- stream.Send(
			t.Context(),
			testTransferFrame(binding, protocol.TransferFrameStatus, 0, nil),
		)
	}()
	waitForMutexHeld(t, &stream.slot.lifecycle)
	type receiveResult struct {
		frame protocol.TransferFrame
		err   error
	}
	receiveDone := make(chan receiveResult, 1)
	go func() {
		frame, receiveErr := stream.Receive(t.Context())
		receiveDone <- receiveResult{frame: frame, err: receiveErr}
	}()
	select {
	case sendErr := <-sendDone:
		t.Fatalf("Send() bypassed held stream state: %v", sendErr)
	case result := <-receiveDone:
		t.Fatalf("Receive() bypassed generation lease: %#v", result)
	case <-time.After(25 * time.Millisecond):
	}
	stream.stateMu.Unlock()
	stateHeld = false
	if sendErr := <-sendDone; sendErr != nil {
		t.Fatal(sendErr)
	}
	result := <-receiveDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.frame.Type != protocol.TransferFrameStatus {
		t.Fatalf("Receive() frame = %#v", result.frame)
	}
}

func TestTransferReceiveSerializesBeforeDequeue(t *testing.T) {
	t.Parallel()
	binding := testTransferBinding()
	_, session, stream := openTestTransferStream(t, &transferRecordingConnection{}, binding)
	first := testTransferFrame(binding, protocol.TransferFrameChunk, 1, []byte("abc"))
	second := testTransferFrame(binding, protocol.TransferFrameChunk, 2, []byte("defg"))
	if handleErr := session.handleTransferFrame(first); handleErr != nil {
		t.Fatal(handleErr)
	}
	if handleErr := session.handleTransferFrame(second); handleErr != nil {
		t.Fatal(handleErr)
	}

	stream.slot.lifecycle.Lock()
	lifecycleHeld := true
	defer func() {
		if lifecycleHeld {
			stream.slot.lifecycle.Unlock()
		}
	}()
	type receiveResult struct {
		frame protocol.TransferFrame
		err   error
	}
	firstDone := make(chan receiveResult, 1)
	go func() {
		frame, receiveErr := stream.Receive(t.Context())
		firstDone <- receiveResult{frame: frame, err: receiveErr}
	}()
	waitForFrameQueueLength(t, stream.subscription.frames, 1)
	secondDone := make(chan receiveResult, 1)
	go func() {
		frame, receiveErr := stream.Receive(t.Context())
		secondDone <- receiveResult{frame: frame, err: receiveErr}
	}()
	select {
	case result := <-secondDone:
		t.Fatalf("second Receive() bypassed receiver serialization: %#v", result)
	case <-time.After(25 * time.Millisecond):
	}
	if queued := len(stream.subscription.frames); queued != 1 {
		t.Fatalf("queued frames = %d; second Receive() dequeued early", queued)
	}
	stream.slot.lifecycle.Unlock()
	lifecycleHeld = false
	firstResult := <-firstDone
	if firstResult.err != nil {
		t.Fatal(firstResult.err)
	}
	secondResult := <-secondDone
	if secondResult.err != nil {
		t.Fatal(secondResult.err)
	}
	if firstResult.frame.Sequence != 1 || secondResult.frame.Sequence != 2 {
		t.Fatalf(
			"Receive() order = %d, %d",
			firstResult.frame.Sequence,
			secondResult.frame.Sequence,
		)
	}
}

func openTestTransferStream(
	t *testing.T,
	connection peerConnection,
	binding TransferBinding,
) (*SessionHub, *peer, *TransferStream) {
	t.Helper()
	hub := NewSessionHub()
	session := newPeer(connection)
	session.markReady()
	release, err := hub.Claim(testTransferNodeID(), session, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := hub.OpenTransfer(t.Context(), testTransferNodeID(), binding)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stream.Close()
		_, _ = release()
		_ = hub.Close(context.Background())
	})
	return hub, session, stream
}

func testTransferNodeID() nodes.ID {
	return nodes.ID("n_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
}

func waitForMutexHeld(t *testing.T, mutex *sync.Mutex) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if mutex.TryLock() {
			mutex.Unlock()
			runtime.Gosched()
			continue
		}
		return
	}
	t.Fatal("mutex was not acquired by competing operation")
}

func waitForFrameQueueLength(
	t *testing.T,
	frames <-chan protocol.TransferFrame,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(frames) == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("queued frames = %d; want %d", len(frames), want)
}

func testTransferBinding() TransferBinding {
	return TransferBinding{
		TransferID:     "transfer_1",
		Direction:      protocol.TransferUpload,
		PolicyRevision: "files-v1",
		TotalSize:      7,
		SHA256:         sha256.Sum256([]byte("payload")),
	}
}

func testTransferFrame(
	binding TransferBinding,
	frameType protocol.TransferFrameType,
	sequence uint64,
	payload []byte,
) protocol.TransferFrame {
	return protocol.TransferFrame{
		Type:           frameType,
		Direction:      binding.Direction,
		TransferID:     binding.TransferID,
		PolicyRevision: binding.PolicyRevision,
		Sequence:       sequence,
		TotalSize:      binding.TotalSize,
		SHA256:         binding.SHA256,
		Payload:        payload,
	}
}

type transferRecordingConnection struct {
	mu          sync.Mutex
	closed      bool
	messageType int
	data        []byte
}

func (connection *transferRecordingConnection) Close() error {
	connection.mu.Lock()
	connection.closed = true
	connection.mu.Unlock()
	return nil
}

func (*transferRecordingConnection) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("read unavailable")
}

func (*transferRecordingConnection) SetPongHandler(func(string) error) {}

func (*transferRecordingConnection) SetReadDeadline(time.Time) error {
	return nil
}

func (*transferRecordingConnection) SetWriteDeadline(time.Time) error {
	return nil
}

func (*transferRecordingConnection) WriteControl(int, []byte, time.Time) error {
	return nil
}

func (connection *transferRecordingConnection) WriteMessage(
	messageType int,
	data []byte,
) error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closed {
		return ErrNodeDisconnected
	}
	connection.messageType = messageType
	connection.data = append([]byte(nil), data...)
	return nil
}

func (connection *transferRecordingConnection) lastWrite() (int, []byte) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.messageType, append([]byte(nil), connection.data...)
}

type cancelBlockingTransferConnection struct {
	transferRecordingConnection

	writeStarted     chan struct{}
	writeInterrupted chan struct{}
	startOnce        sync.Once
	interruptOnce    sync.Once
	deadlineCalls    atomic.Int32
}

func newCancelBlockingTransferConnection() *cancelBlockingTransferConnection {
	return &cancelBlockingTransferConnection{
		writeStarted:     make(chan struct{}),
		writeInterrupted: make(chan struct{}),
	}
}

func (connection *cancelBlockingTransferConnection) SetWriteDeadline(time.Time) error {
	if connection.deadlineCalls.Add(1) > 1 {
		connection.interruptOnce.Do(func() { close(connection.writeInterrupted) })
	}
	return nil
}

func (connection *cancelBlockingTransferConnection) WriteMessage(int, []byte) error {
	connection.startOnce.Do(func() { close(connection.writeStarted) })
	<-connection.writeInterrupted
	return context.DeadlineExceeded
}

type gatedTransferConnection struct {
	transferRecordingConnection

	writeStarted chan struct{}
	allowWrite   chan struct{}
	startOnce    sync.Once
	releaseOnce  sync.Once
}

func newGatedTransferConnection() *gatedTransferConnection {
	return &gatedTransferConnection{
		writeStarted: make(chan struct{}),
		allowWrite:   make(chan struct{}),
	}
}

func (connection *gatedTransferConnection) WriteMessage(
	messageType int,
	data []byte,
) error {
	connection.startOnce.Do(func() { close(connection.writeStarted) })
	<-connection.allowWrite
	return connection.transferRecordingConnection.WriteMessage(messageType, data)
}

func (connection *gatedTransferConnection) release() {
	connection.releaseOnce.Do(func() { close(connection.allowWrite) })
}
