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
	hub          *SessionHub
	slot         *sessionSlot
	entry        *sessionEntry
	session      *peer
	subscription *transferFrameSubscription
	binding      TransferBinding
	receiveSlot  chan struct{}

	// Lock order is receiveSlot, slot.lifecycle, then stateMu. Subscription
	// delivery may take transferMu and subscription.mu after stateMu.
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
	entry := slot.current
	session := entry.peer
	hub.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-session.closed:
		return nil, ErrNodeDisconnected
	case <-session.ready:
	}
	var subscription *transferFrameSubscription
	err := hub.withTransferGeneration(slot, entry, func() error {
		var subscribeErr error
		subscription, subscribeErr = session.subscribeTransfer(binding)
		return subscribeErr
	})
	if err != nil {
		return nil, err
	}
	return &TransferStream{
		hub:          hub,
		slot:         slot,
		entry:        entry,
		session:      session,
		subscription: subscription,
		binding:      binding,
		receiveSlot:  make(chan struct{}, 1),
	}, nil
}

func (hub *SessionHub) withTransferGeneration(
	slot *sessionSlot,
	entry *sessionEntry,
	operation func() error,
) error {
	if hub == nil || slot == nil || entry == nil || operation == nil {
		return ErrNodeDisconnected
	}
	slot.lifecycle.Lock()
	defer slot.lifecycle.Unlock()
	hub.mu.Lock()
	current := !hub.closed &&
		slot.current == entry &&
		entry.active &&
		entry.peer != nil
	hub.mu.Unlock()
	if !current {
		return ErrNodeDisconnected
	}
	return operation()
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
	return stream.hub.withTransferGeneration(stream.slot, stream.entry, func() error {
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
	})
}

func (stream *TransferStream) Receive(
	ctx context.Context,
) (protocol.TransferFrame, error) {
	if stream == nil || stream.subscription == nil || stream.receiveSlot == nil {
		return protocol.TransferFrame{}, ErrNodeDisconnected
	}
	if err := stream.acquireReceiveSlot(ctx); err != nil {
		return protocol.TransferFrame{}, err
	}
	defer func() { <-stream.receiveSlot }()
	frame, err := stream.subscription.receive(ctx)
	if err != nil {
		return protocol.TransferFrame{}, err
	}
	err = stream.hub.withTransferGeneration(stream.slot, stream.entry, func() error {
		stream.stateMu.Lock()
		defer stream.stateMu.Unlock()
		if stream.closed {
			return ErrNodeDisconnected
		}
		switch frame.Type {
		case protocol.TransferFrameChunk:
			stream.receivedChunkSequence = frame.Sequence
		case protocol.TransferFrameAck:
			if frame.Sequence < stream.receivedAckSequence ||
				frame.Sequence > stream.sentChunkSequence {
				stream.closed = true
				stream.session.unsubscribeTransfer(
					stream.binding.TransferID,
					stream.subscription,
					protocol.ErrInvalidTransferFrame,
					true,
				)
				return protocol.ErrInvalidTransferFrame
			}
			stream.receivedAckSequence = frame.Sequence
		}
		return nil
	})
	if err != nil {
		return protocol.TransferFrame{}, err
	}
	return frame, nil
}

func (stream *TransferStream) acquireReceiveSlot(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.session.closed:
		return ErrNodeDisconnected
	default:
	}
	select {
	case stream.receiveSlot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.session.closed:
		return ErrNodeDisconnected
	}
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
