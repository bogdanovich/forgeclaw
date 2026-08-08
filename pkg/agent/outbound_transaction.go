package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

var errOutboundPublicationInFlight = errors.New("durable outbound publication is still in flight")

type outboundTransaction struct {
	sourceID string
	ordinal  atomic.Int64

	mu  sync.Mutex
	err error
}

type outboundTransactionKey struct{}

type durableMessageAdmission struct {
	message     bus.OutboundMessage
	coordinator *outbox.Coordinator
	lease       outbox.DispatchLease
	durable     bool
	dispatch    bool
}

type durableMediaAdmission struct {
	message     bus.OutboundMediaMessage
	coordinator *outbox.Coordinator
	lease       outbox.DispatchLease
	durable     bool
	dispatch    bool
}

func withOutboundTransaction(ctx context.Context, sourceID string) context.Context {
	sourceID = strings.TrimSpace(sourceID)
	if ctx == nil || sourceID == "" {
		return ctx
	}
	ctx = context.WithValue(ctx, outboundTransactionKey{}, &outboundTransaction{sourceID: sourceID})
	return toolshared.WithToolRecoverableOutbound(ctx, true)
}

func inheritOutboundTransaction(dst, src context.Context) context.Context {
	transaction := outboundTransactionFromContext(src)
	if dst == nil || transaction == nil {
		return dst
	}
	dst = context.WithValue(dst, outboundTransactionKey{}, transaction)
	return toolshared.WithToolRecoverableOutbound(dst, true)
}

func outboundTransactionFromContext(ctx context.Context) *outboundTransaction {
	if ctx == nil {
		return nil
	}
	transaction, _ := ctx.Value(outboundTransactionKey{}).(*outboundTransaction)
	if transaction == nil || strings.TrimSpace(transaction.sourceID) == "" {
		return nil
	}
	return transaction
}

func hasOutboundTransaction(ctx context.Context) bool {
	return outboundTransactionFromContext(ctx) != nil
}

func (t *outboundTransaction) nextIdentity(kind outbox.Kind, channel, chatID, sessionKey string) outbox.Identity {
	return outbox.Identity{
		SourceID:   t.sourceID,
		Ordinal:    int(t.ordinal.Add(1) - 1),
		Kind:       kind,
		Channel:    channel,
		ChatID:     chatID,
		SessionKey: sessionKey,
	}
}

func (t *outboundTransaction) fail(err error) {
	if t == nil || err == nil {
		return
	}
	t.mu.Lock()
	if t.err == nil {
		t.err = err
	}
	t.mu.Unlock()
}

func (t *outboundTransaction) failure() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func transactionAdmission(ctx context.Context, admission finalResponseAdmission) finalResponseAdmission {
	transaction := outboundTransactionFromContext(ctx)
	if transaction == nil {
		return admission
	}
	if err := transaction.failure(); err != nil {
		return rejectedFinalResponseAdmission(err)
	}
	return admission
}

func (al *AgentLoop) SetOutboundOutbox(coordinator *outbox.Coordinator) {
	if al == nil {
		return
	}
	al.mu.Lock()
	al.outboundOutbox = coordinator
	al.mu.Unlock()
}

func (al *AgentLoop) closeOutboundOutbox() error {
	if al == nil {
		return nil
	}
	al.mu.Lock()
	coordinator := al.outboundOutbox
	al.outboundOutbox = nil
	al.mu.Unlock()
	if coordinator == nil {
		return nil
	}
	return coordinator.Close()
}

func (al *AgentLoop) outboundCoordinator() *outbox.Coordinator {
	if al == nil {
		return nil
	}
	al.mu.RLock()
	defer al.mu.RUnlock()
	return al.outboundOutbox
}

func (al *AgentLoop) admitDurableMessage(
	ctx context.Context,
	workspace string,
	msg bus.OutboundMessage,
) (durableMessageAdmission, error) {
	result := durableMessageAdmission{message: msg}
	transaction := outboundTransactionFromContext(ctx)
	if transaction == nil {
		return result, nil
	}
	coordinator := al.outboundCoordinator()
	if coordinator == nil {
		err := errors.New("durable outbound coordinator is unavailable")
		transaction.fail(err)
		return result, err
	}
	admission, err := coordinator.AdmitMessage(
		workspace,
		transaction.nextIdentity(outbox.KindMessage, msg.Channel, msg.ChatID, msg.SessionKey),
		msg,
	)
	if err != nil {
		transaction.fail(err)
		return result, err
	}
	if admission.Intent.Message == nil {
		err = errors.New("durable outbound intent has no message payload")
		transaction.fail(err)
		return result, err
	}
	if admission.InFlight {
		transaction.fail(errOutboundPublicationInFlight)
		return result, errOutboundPublicationInFlight
	}
	result.message = *admission.Intent.Message
	result.coordinator = coordinator
	result.lease = admission.Lease
	result.durable = true
	result.dispatch = admission.Dispatch
	return result, nil
}

func (al *AgentLoop) admitDurableMedia(
	ctx context.Context,
	workspace string,
	msg bus.OutboundMediaMessage,
) (durableMediaAdmission, error) {
	result := durableMediaAdmission{message: msg}
	transaction := outboundTransactionFromContext(ctx)
	if transaction == nil {
		return result, nil
	}
	coordinator := al.outboundCoordinator()
	if coordinator == nil {
		err := errors.New("durable outbound coordinator is unavailable")
		transaction.fail(err)
		return result, err
	}
	admission, err := coordinator.AdmitMedia(
		workspace,
		transaction.nextIdentity(outbox.KindMedia, msg.Channel, msg.ChatID, msg.SessionKey),
		msg,
	)
	if err != nil {
		transaction.fail(err)
		return result, err
	}
	if admission.Intent.Media == nil {
		err = errors.New("durable outbound intent has no media payload")
		transaction.fail(err)
		return result, err
	}
	if admission.InFlight {
		transaction.fail(errOutboundPublicationInFlight)
		return result, errOutboundPublicationInFlight
	}
	result.message = *admission.Intent.Media
	result.coordinator = coordinator
	result.lease = admission.Lease
	result.durable = true
	result.dispatch = admission.Dispatch
	return result, nil
}

func releaseDurableAdmission(
	ctx context.Context,
	coordinator *outbox.Coordinator,
	lease outbox.DispatchLease,
	cause error,
) error {
	releaseErr := error(nil)
	if coordinator != nil {
		releaseErr = coordinator.ReleaseAdmission(lease)
	}
	result := errors.Join(cause, releaseErr)
	if transaction := outboundTransactionFromContext(ctx); transaction != nil {
		transaction.fail(result)
	}
	return result
}

func (al *AgentLoop) publishTransactionMessage(
	ctx context.Context,
	workspace string,
	msg bus.OutboundMessage,
) (bool, error) {
	return al.publishTransactionMessageAtBoundary(ctx, workspace, msg, nil)
}

func (al *AgentLoop) publishTransactionMessageAtBoundary(
	ctx context.Context,
	workspace string,
	msg bus.OutboundMessage,
	commit func(context.Context) error,
) (bool, error) {
	admission, err := al.admitDurableMessage(ctx, workspace, msg)
	if err != nil {
		return false, err
	}
	if admission.durable && !admission.dispatch {
		if commit != nil {
			if err = commit(ctx); err != nil {
				if transaction := outboundTransactionFromContext(ctx); transaction != nil {
					transaction.fail(err)
				}
				return false, err
			}
		}
		return false, nil
	}
	if al == nil || al.bus == nil {
		err = errors.New("message bus is unavailable")
		if admission.durable {
			err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
		}
		return false, err
	}
	if commit != nil {
		if err = commit(ctx); err != nil {
			if admission.durable {
				err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
			}
			return false, err
		}
	}
	if admission.durable {
		if err = admission.coordinator.PrepareAdmission(admission.lease); err != nil {
			err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
			return false, err
		}
	}
	if err = al.bus.PublishOutbound(ctx, admission.message); err != nil {
		if admission.durable {
			err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
		}
		return false, err
	}
	if admission.durable {
		if err = admission.coordinator.CommitAdmission(admission.lease); err != nil {
			if transaction := outboundTransactionFromContext(ctx); transaction != nil {
				transaction.fail(err)
			}
			return true, err
		}
	}
	return true, nil
}

func (al *AgentLoop) publishTransactionMedia(
	ctx context.Context,
	workspace string,
	msg bus.OutboundMediaMessage,
) (bool, error) {
	return al.publishTransactionMediaAtBoundary(ctx, workspace, msg, nil)
}

func (al *AgentLoop) publishTransactionMediaAtBoundary(
	ctx context.Context,
	workspace string,
	msg bus.OutboundMediaMessage,
	commit func(context.Context) error,
) (bool, error) {
	admission, err := al.admitDurableMedia(ctx, workspace, msg)
	if err != nil {
		return false, err
	}
	if admission.durable && !admission.dispatch {
		if commit != nil {
			if err = commit(ctx); err != nil {
				if transaction := outboundTransactionFromContext(ctx); transaction != nil {
					transaction.fail(err)
				}
				return false, err
			}
		}
		return false, nil
	}
	if al == nil || al.bus == nil {
		err = errors.New("message bus is unavailable")
		if admission.durable {
			err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
		}
		return false, err
	}
	if commit != nil {
		if err = commit(ctx); err != nil {
			if admission.durable {
				err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
			}
			return false, err
		}
	}
	if admission.durable {
		if err = admission.coordinator.PrepareAdmission(admission.lease); err != nil {
			err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
			return false, err
		}
	}
	if err = al.bus.PublishOutboundMedia(ctx, admission.message); err != nil {
		if admission.durable {
			err = releaseDurableAdmission(ctx, admission.coordinator, admission.lease, err)
		}
		return false, err
	}
	if admission.durable {
		if err = admission.coordinator.CommitAdmission(admission.lease); err != nil {
			if transaction := outboundTransactionFromContext(ctx); transaction != nil {
				transaction.fail(err)
			}
			return true, err
		}
	}
	return true, nil
}
