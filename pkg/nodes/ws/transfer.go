package ws

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

// TransferBinding is the immutable transfer identity admitted on one
// authenticated peer generation.
type TransferBinding struct {
	TransferID     string
	Direction      protocol.TransferDirection
	PolicyRevision string
	TotalSize      uint64
	SHA256         [32]byte
}

func (binding TransferBinding) Validate() error {
	return binding.ValidateFrame(protocol.TransferFrame{
		Type:           protocol.TransferFrameStatus,
		Direction:      binding.Direction,
		TransferID:     binding.TransferID,
		PolicyRevision: binding.PolicyRevision,
		TotalSize:      binding.TotalSize,
		SHA256:         binding.SHA256,
	})
}

func (binding TransferBinding) ValidateFrame(frame protocol.TransferFrame) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	if frame.TransferID != binding.TransferID ||
		frame.Direction != binding.Direction ||
		frame.PolicyRevision != binding.PolicyRevision ||
		frame.TotalSize != binding.TotalSize ||
		!bytes.Equal(frame.SHA256[:], binding.SHA256[:]) {
		return protocol.ErrInvalidTransferFrame
	}
	return nil
}

type TransferStream struct {
	session      *peer
	subscription *transferFrameSubscription
	binding      TransferBinding

	stateMu               sync.Mutex
	sentChunkSequence     uint64
	sentChunkBytes        uint64
	receivedChunkSequence uint64
	sentAckSequence       uint64
	receivedAckSequence   uint64
	closed                bool
}

// OpenTransfer binds one transfer to the exact currently authenticated node
// generation. Peer replacement closes the stream rather than moving it.
func (hub *SessionHub) OpenTransfer(
	ctx context.Context,
	nodeID nodes.ID,
	binding TransferBinding,
) (*TransferStream, error) {
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	hub.mu.Lock()
	slot := hub.sessions[nodeID]
	if hub.closed || slot == nil || slot.current == nil ||
		!slot.current.active || slot.current.peer == nil {
		hub.mu.Unlock()
		return nil, ErrNodeDisconnected
	}
	session := slot.current.peer
	hub.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-session.closed:
		return nil, ErrNodeDisconnected
	case <-session.ready:
	}
	subscription, err := session.subscribeTransfer(binding)
	if err != nil {
		return nil, err
	}
	return &TransferStream{
		session:      session,
		subscription: subscription,
		binding:      binding,
	}, nil
}

func (stream *TransferStream) Send(
	ctx context.Context,
	frame protocol.TransferFrame,
) error {
	if stream == nil || stream.session == nil || stream.subscription == nil {
		return ErrNodeDisconnected
	}
	if err := stream.binding.ValidateFrame(frame); err != nil {
		return err
	}
	stream.stateMu.Lock()
	defer stream.stateMu.Unlock()
	if stream.closed {
		return ErrNodeDisconnected
	}
	if frame.Type == protocol.TransferFrameChunk {
		if frame.Sequence != stream.sentChunkSequence+1 {
			return protocol.ErrInvalidTransferFrame
		}
		if stream.sentChunkBytes+uint64(len(frame.Payload)) > stream.binding.TotalSize {
			return protocol.ErrInvalidTransferFrame
		}
	}
	if frame.Type == protocol.TransferFrameAck {
		if frame.Sequence < stream.sentAckSequence ||
			frame.Sequence > stream.receivedChunkSequence {
			return protocol.ErrInvalidTransferFrame
		}
	}
	if err := stream.session.writeTransferFrame(ctx, frame); err != nil {
		return err
	}
	if frame.Type == protocol.TransferFrameChunk {
		stream.sentChunkSequence = frame.Sequence
		stream.sentChunkBytes += uint64(len(frame.Payload))
	}
	if frame.Type == protocol.TransferFrameAck {
		stream.sentAckSequence = frame.Sequence
	}
	return nil
}

func (stream *TransferStream) Receive(
	ctx context.Context,
) (protocol.TransferFrame, error) {
	if stream == nil || stream.subscription == nil {
		return protocol.TransferFrame{}, ErrNodeDisconnected
	}
	frame, err := stream.subscription.receive(ctx)
	if err != nil {
		return protocol.TransferFrame{}, err
	}
	stream.stateMu.Lock()
	if stream.closed {
		stream.stateMu.Unlock()
		return protocol.TransferFrame{}, ErrNodeDisconnected
	}
	switch frame.Type {
	case protocol.TransferFrameChunk:
		stream.receivedChunkSequence = frame.Sequence
	case protocol.TransferFrameAck:
		if frame.Sequence < stream.receivedAckSequence ||
			frame.Sequence > stream.sentChunkSequence {
			stream.closed = true
			stream.stateMu.Unlock()
			stream.session.unsubscribeTransfer(
				stream.binding.TransferID,
				stream.subscription,
				protocol.ErrInvalidTransferFrame,
				true,
			)
			return protocol.TransferFrame{}, protocol.ErrInvalidTransferFrame
		}
		stream.receivedAckSequence = frame.Sequence
	}
	stream.stateMu.Unlock()
	return frame, nil
}

func (stream *TransferStream) Close() error {
	if stream == nil || stream.session == nil || stream.subscription == nil {
		return nil
	}
	stream.stateMu.Lock()
	if stream.closed {
		stream.stateMu.Unlock()
		return nil
	}
	stream.closed = true
	stream.stateMu.Unlock()
	stream.session.unsubscribeTransfer(
		stream.binding.TransferID,
		stream.subscription,
		errors.New("transfer stream closed"),
		true,
	)
	return nil
}
