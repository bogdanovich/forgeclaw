package ws

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
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
	session := newPeer(&transferRecordingConnection{})
	session.markReady()
	binding := testTransferBinding()
	subscription, err := session.subscribeTransfer(binding)
	if err != nil {
		t.Fatal(err)
	}
	stream := &TransferStream{
		session: session, subscription: subscription, binding: binding,
	}
	defer stream.Close()

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
	session := newPeer(&transferRecordingConnection{})
	session.markReady()
	binding := testTransferBinding()
	subscription, err := session.subscribeTransfer(binding)
	if err != nil {
		t.Fatal(err)
	}
	stream := &TransferStream{
		session: session, subscription: subscription, binding: binding,
	}
	defer stream.Close()

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
	session := newPeer(&transferRecordingConnection{})
	binding := testTransferBinding()
	subscription, err := session.subscribeTransfer(binding)
	if err != nil {
		t.Fatal(err)
	}
	stream := &TransferStream{
		session: session, subscription: subscription, binding: binding,
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
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
