package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

type TerminalStream struct {
	session      *peer
	subscription *terminalEventSubscription
	request      nodes.TerminalSessionRequest
	closeMu      sync.Mutex
	closed       bool
}

// OpenTerminal dispatches one immutable, owner-bound terminal plan through the
// current authenticated node generation. commit is the approval/durable-state
// boundary and must complete before the request frame is written.
func (handler *AdmissionHandler) OpenTerminal(
	ctx context.Context,
	nodeID nodes.ID,
	plan nodes.TerminalOpenPlan,
	commit func() error,
) (nodes.TerminalMetadata, bool, error) {
	approval, err := handler.validateTerminalPreflight(nodeID, plan)
	if err != nil {
		return nodes.TerminalMetadata{}, false, err
	}
	params, err := json.Marshal(plan)
	if err != nil {
		return nodes.TerminalMetadata{}, false, fmt.Errorf("encode terminal open plan: %w", err)
	}
	response, dispatched, err := handler.sessions.Request(
		ctx,
		nodeID,
		"node.terminal.open",
		params,
		plan.IdempotencyKey,
		func(write func() error) error {
			_, leaseErr := handler.authenticator.WithApprovedCommand(
				nodeID,
				"shell.exec.v1",
				func(current nodes.CommandApproval) error {
					if validationErr := validateTerminalApproval(current, nodeID, plan); validationErr != nil {
						return validationErr
					}
					if commit != nil {
						if commitErr := commit(); commitErr != nil {
							if !fileutil.IsCommittedWriteError(commitErr) {
								return commitErr
							}
							return errors.Join(commitErr, write())
						}
					}
					return write()
				},
			)
			return leaseErr
		},
	)
	if err != nil {
		return nodes.TerminalMetadata{}, dispatched, err
	}
	metadata, err := decodeTerminalMetadata(response, plan.Owner)
	if err != nil {
		return nodes.TerminalMetadata{}, true, err
	}
	if metadata.State != "pending_attach" {
		return nodes.TerminalMetadata{}, true, errors.New("node returned an invalid terminal open state")
	}
	_ = approval
	return metadata, true, nil
}

func (handler *AdmissionHandler) validateTerminalPreflight(
	nodeID nodes.ID,
	plan nodes.TerminalOpenPlan,
) (nodes.CommandApproval, error) {
	approval, err := handler.authenticator.ApprovedCommand(nodeID, "shell.exec.v1")
	if err != nil {
		return nodes.CommandApproval{}, err
	}
	if err := plan.Validate(); err != nil {
		return nodes.CommandApproval{}, err
	}
	now := time.Now().Unix()
	if now < plan.PreparedAt || now >= plan.ExpiresAt {
		return nodes.CommandApproval{}, fmt.Errorf("%w: terminal plan is stale", nodes.ErrCommandDenied)
	}
	if err := validateTerminalApproval(approval, nodeID, plan); err != nil {
		return nodes.CommandApproval{}, err
	}
	return approval, nil
}

func validateTerminalApproval(
	approval nodes.CommandApproval,
	nodeID nodes.ID,
	plan nodes.TerminalOpenPlan,
) error {
	contract := approval.Descriptor.ModelContract
	if plan.NodeID != nodeID ||
		plan.CatalogHash != approval.CatalogHash ||
		contract == nil ||
		contract.Availability != nodes.ModelAvailable ||
		contract.AuthorityDigest != plan.AuthorityDigest ||
		!slices.Contains(contract.Constraints.ProfileAliases, plan.Owner.Profile) ||
		!slices.Contains(contract.Constraints.WorkingScopes, plan.WorkingScope) {
		return fmt.Errorf("%w: terminal plan does not match current approval", nodes.ErrCommandDenied)
	}
	return nil
}

func (handler *AdmissionHandler) AttachTerminal(
	ctx context.Context,
	nodeID nodes.ID,
	request nodes.TerminalSessionRequest,
) (*TerminalStream, nodes.TerminalMetadata, error) {
	if err := request.Validate(); err != nil {
		return nil, nodes.TerminalMetadata{}, err
	}
	params, err := json.Marshal(request)
	if err != nil {
		return nil, nodes.TerminalMetadata{}, err
	}
	session, subscription, err := handler.sessions.subscribeTerminal(nodeID, request)
	if err != nil {
		return nil, nodes.TerminalMetadata{}, err
	}
	response, dispatched, err := session.request(ctx, "node.terminal.attach", params, "", nil)
	if err != nil {
		if dispatched {
			_, cleanupErr := session.detachTerminalGuaranteed(request)
			err = errors.Join(err, cleanupErr)
		}
		session.unsubscribeTerminal(request.TerminalID, subscription, err, dispatched)
		return nil, nodes.TerminalMetadata{}, err
	}
	metadata, err := decodeTerminalMetadata(response, request.Owner)
	if err != nil {
		retainTombstone := response.OK == nil || *response.OK
		if retainTombstone {
			_, cleanupErr := session.detachTerminalGuaranteed(request)
			err = errors.Join(err, cleanupErr)
		}
		session.unsubscribeTerminal(
			request.TerminalID,
			subscription,
			err,
			retainTombstone,
		)
		return nil, nodes.TerminalMetadata{}, err
	}
	if metadata.TerminalID != request.TerminalID || metadata.State != "live" {
		err = errors.New("node returned an unrelated terminal attachment")
		_, cleanupErr := session.detachTerminalGuaranteed(request)
		err = errors.Join(err, cleanupErr)
		session.unsubscribeTerminal(request.TerminalID, subscription, err, true)
		return nil, nodes.TerminalMetadata{}, err
	}
	return &TerminalStream{
		session:      session,
		subscription: subscription,
		request:      request,
	}, metadata, nil
}

func (handler *AdmissionHandler) TerminalStatus(
	ctx context.Context,
	nodeID nodes.ID,
	request nodes.TerminalSessionRequest,
) (nodes.TerminalMetadata, error) {
	if err := request.Validate(); err != nil {
		return nodes.TerminalMetadata{}, err
	}
	params, err := json.Marshal(request)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	response, _, err := handler.sessions.Request(
		ctx,
		nodeID,
		"node.terminal.status",
		params,
		"",
		nil,
	)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	return decodeTerminalMetadata(response, request.Owner)
}

func (stream *TerminalStream) Receive(ctx context.Context) (nodes.TerminalEvent, error) {
	if stream == nil || stream.subscription == nil {
		return nodes.TerminalEvent{}, ErrNodeDisconnected
	}
	return stream.subscription.receive(ctx)
}

func (stream *TerminalStream) Control(
	ctx context.Context,
	request nodes.TerminalControlRequest,
) error {
	if stream == nil || stream.session == nil {
		return ErrNodeDisconnected
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if request.TerminalID != stream.request.TerminalID ||
		request.Owner != stream.request.Owner {
		return nodes.ErrCommandDenied
	}
	params, err := json.Marshal(request)
	if err != nil {
		return err
	}
	response, _, err := stream.session.request(
		ctx,
		"node.terminal.control",
		params,
		request.IdempotencyKey,
		nil,
	)
	if err != nil {
		return err
	}
	return terminalResponseError(response)
}

func (stream *TerminalStream) Close(_ context.Context) error {
	if stream == nil || stream.session == nil {
		return nil
	}
	stream.closeMu.Lock()
	defer stream.closeMu.Unlock()
	if stream.closed {
		return nil
	}
	closed, err := stream.session.detachTerminalGuaranteed(stream.request)
	if closed {
		stream.closed = true
		stream.session.unsubscribeTerminal(
			stream.request.TerminalID,
			stream.subscription,
			ErrNodeDisconnected,
			true,
		)
	}
	return err
}

func (session *peer) detachTerminalGuaranteed(
	request nodes.TerminalSessionRequest,
) (bool, error) {
	params, err := json.Marshal(request)
	if err != nil {
		return false, err
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), defaultWriteTimeout)
	defer cancel()
	response, dispatched, requestErr := session.request(
		cleanupCtx,
		"node.terminal.detach",
		params,
		"",
		nil,
	)
	if requestErr == nil {
		requestErr = terminalResponseError(response)
	}
	if requestErr == nil {
		return true, nil
	}
	closeErr := session.Close()
	return true, errors.Join(
		requestErr,
		fmt.Errorf("terminal detach failed; peer fail-closed (dispatched=%t)", dispatched),
		closeErr,
	)
}

func (hub *SessionHub) subscribeTerminal(
	nodeID nodes.ID,
	request nodes.TerminalSessionRequest,
) (*peer, *terminalEventSubscription, error) {
	hub.mu.Lock()
	slot := hub.sessions[nodeID]
	if hub.closed || slot == nil || slot.current == nil ||
		!slot.current.active || slot.current.peer == nil {
		hub.mu.Unlock()
		return nil, nil, ErrNodeDisconnected
	}
	session := slot.current.peer
	subscription, err := session.subscribeTerminal(request)
	hub.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return session, subscription, nil
}

func decodeTerminalMetadata(
	response protocol.Envelope,
	owner nodes.TerminalOwner,
) (nodes.TerminalMetadata, error) {
	if response.OK == nil {
		return nodes.TerminalMetadata{}, errors.New("node returned a malformed terminal response")
	}
	if !*response.OK {
		return nodes.TerminalMetadata{}, fmt.Errorf(
			"node terminal request failed (%s): %s",
			response.Error.Code,
			response.Error.Message,
		)
	}
	var metadata nodes.TerminalMetadata
	decoder := json.NewDecoder(bytes.NewReader(response.Result))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return nodes.TerminalMetadata{}, fmt.Errorf("decode terminal metadata: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nodes.TerminalMetadata{}, errors.New("decode terminal metadata: trailing data")
	}
	if metadata.TerminalID == "" || metadata.Owner != owner || metadata.StartedAt <= 0 {
		return nodes.TerminalMetadata{}, errors.New("node returned unrelated terminal metadata")
	}
	return metadata, nil
}

func terminalResponseError(response protocol.Envelope) error {
	if response.OK == nil {
		return errors.New("node returned a malformed terminal response")
	}
	if !*response.OK {
		return fmt.Errorf(
			"node terminal request failed (%s): %s",
			response.Error.Code,
			response.Error.Message,
		)
	}
	return nil
}
