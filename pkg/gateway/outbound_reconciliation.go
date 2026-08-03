package gateway

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/logger"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
)

type gatewayOutboundReconciler struct {
	cancel context.CancelFunc
	done   <-chan struct{}
	once   sync.Once
}

func startGatewayOutboundReconciler(
	parent context.Context,
	coordinator *outbox.Coordinator,
	msgBus *bus.MessageBus,
	admissions []outbox.Admission,
) (*gatewayOutboundReconciler, error) {
	if parent == nil {
		parent = context.Background()
	}
	if coordinator == nil || msgBus == nil {
		return nil, errors.New("outbound reconciliation dependencies are unavailable")
	}

	pending := append([]outbox.Admission(nil), admissions...)
	sort.SliceStable(pending, func(i, j int) bool {
		return recoveryDispatchAt(pending[i].Intent).Before(recoveryDispatchAt(pending[j].Intent))
	})
	reconcileCtx, cancel := context.WithCancel(parent)
	now := time.Now().UTC()
	firstDelayed := len(pending)
	for index, admission := range pending {
		if recoveryDispatchAt(admission.Intent).After(now) {
			firstDelayed = index
			break
		}
		if err := publishRecoveredAdmission(reconcileCtx, coordinator, msgBus, admission); err != nil {
			cancel()
			releaseRecoveredAdmissions(coordinator, pending[index+1:])
			return nil, fmt.Errorf("publish recovered outbound intent %q: %w", admission.Intent.ID, err)
		}
	}

	done := make(chan struct{})
	reconciler := &gatewayOutboundReconciler{cancel: cancel, done: done}
	delayed := pending[firstDelayed:]
	go func() {
		defer close(done)
		for index, admission := range delayed {
			if err := waitForRecoveredAdmission(reconcileCtx, admission.Intent); err != nil {
				releaseRecoveredAdmissions(coordinator, delayed[index:])
				return
			}
			if err := publishRecoveredAdmission(reconcileCtx, coordinator, msgBus, admission); err != nil {
				logger.ErrorCF("gateway", "Failed to publish scheduled outbound recovery", map[string]any{
					"delivery_id": admission.Intent.ID,
					"error":       err.Error(),
				})
				releaseRecoveredAdmissions(coordinator, delayed[index+1:])
				return
			}
		}
	}()

	if len(pending) > 0 {
		logger.InfoCF("gateway", "Reconciled durable outbound intents", map[string]any{
			"due":       firstDelayed,
			"scheduled": len(delayed),
			"total":     len(pending),
		})
	}
	return reconciler, nil
}

func (r *gatewayOutboundReconciler) stop() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		if r.done != nil {
			<-r.done
		}
	})
}

func recoveryDispatchAt(intent outbox.Intent) time.Time {
	if intent.Status == outbox.StatusDefinitelyFailed && !intent.RetryAfter.IsZero() {
		return intent.RetryAfter.UTC()
	}
	return time.Time{}
}

func waitForRecoveredAdmission(ctx context.Context, intent outbox.Intent) error {
	delay := time.Until(recoveryDispatchAt(intent))
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func publishRecoveredAdmission(
	ctx context.Context,
	coordinator *outbox.Coordinator,
	msgBus *bus.MessageBus,
	admission outbox.Admission,
) error {
	if !admission.Dispatch {
		return errors.New("recovered outbound admission does not own dispatch")
	}
	if err := coordinator.PrepareAdmission(admission.Lease); err != nil {
		return err
	}

	var publishErr error
	switch admission.Intent.Identity.Kind {
	case outbox.KindMessage:
		if admission.Intent.Message == nil {
			publishErr = errors.New("recovered outbound intent has no message payload")
		} else {
			publishErr = msgBus.PublishOutbound(ctx, *admission.Intent.Message)
		}
	case outbox.KindMedia:
		if admission.Intent.Media == nil {
			publishErr = errors.New("recovered outbound intent has no media payload")
		} else {
			publishErr = msgBus.PublishOutboundMedia(ctx, *admission.Intent.Media)
		}
	default:
		publishErr = fmt.Errorf("recovered outbound intent has unsupported kind %q", admission.Intent.Identity.Kind)
	}
	if publishErr != nil {
		return errors.Join(publishErr, coordinator.ReleaseAdmission(admission.Lease))
	}
	return coordinator.CommitAdmission(admission.Lease)
}

func releaseRecoveredAdmissions(coordinator *outbox.Coordinator, admissions []outbox.Admission) {
	for _, admission := range admissions {
		if err := coordinator.ReleaseAdmission(admission.Lease); err != nil {
			logger.WarnCF("gateway", "Failed to release outbound recovery admission", map[string]any{
				"delivery_id": admission.Intent.ID,
				"error":       err.Error(),
			})
		}
	}
}
