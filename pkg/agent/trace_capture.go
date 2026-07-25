package agent

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	"github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	tracePersistBuffer            = 128
	traceShutdownAdmissionTimeout = 5 * time.Second
	traceShutdownDrainTimeout     = 5 * time.Second
)

type traceCaptureManager struct {
	mu      sync.Mutex
	closed  bool
	startMu sync.Mutex

	settings    traceCaptureSettings
	turns       *turnTraceProjector
	tasks       *taskTraceProjector
	coordinator *evalcapture.Coordinator
}

func newTraceCaptureManager(cfg *config.Config, eventBus events.Bus) *traceCaptureManager {
	settings := traceCaptureSettingsFromConfig(cfg)
	manager := &traceCaptureManager{settings: settings}
	manager.turns = newTurnTraceProjector(settings, eventBus, manager.enqueuePersist)
	manager.tasks = newTaskTraceProjector(settings, manager.coordinator)
	if settings.enabled {
		manager.start()
	}
	return manager
}

func (m *traceCaptureManager) start() {
	if m == nil {
		return
	}
	m.startMu.Lock()
	defer m.startMu.Unlock()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	if m.coordinator == nil {
		m.coordinator = evalcapture.NewCoordinator(evalcapture.CoordinatorOptions{
			PendingCapacity: tracePersistBuffer,
			Writer: evalcapture.Options{
				Capacity:  tracePersistBuffer,
				EventSink: logTraceWriterEvent,
			},
		})
		m.tasks.setCoordinator(m.coordinator)
	}
	turns := m.turns
	m.mu.Unlock()

	turns.start()
}

func (m *traceCaptureManager) updateConfig(cfg *config.Config) {
	if m == nil {
		return
	}
	updated := traceCaptureSettingsFromConfig(cfg)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.settings = updated
	turns, tasks := m.turns, m.tasks
	m.mu.Unlock()

	if updated.enabled {
		m.start()
	}
	turns.updateSettings(updated)
	tasks.updateSettings(updated)
}

func (m *traceCaptureManager) enabled() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed && m.settings.enabled
}

func (m *traceCaptureManager) close() {
	m.closeWithTimeouts(
		traceShutdownAdmissionTimeout,
		traceShutdownDrainTimeout,
	)
}

func (m *traceCaptureManager) closeWithTimeouts(
	admissionTimeout, drainTimeout time.Duration,
) {
	if m == nil {
		return
	}
	m.startMu.Lock()
	defer m.startMu.Unlock()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	turns, tasks, coordinator := m.turns, m.tasks, m.coordinator
	m.mu.Unlock()

	turns.close()
	tasks.stop()
	admissionCtx, admissionCancel := context.WithTimeout(
		context.Background(),
		admissionTimeout,
	)
	if coordinator != nil {
		if err := coordinator.Close(admissionCtx, drainTimeout); err != nil {
			logger.WarnCF(
				"evaltrace",
				"Durable trace coordinator did not drain before shutdown deadline",
				map[string]any{"error": err.Error()},
			)
		}
	}
	admissionCancel()
	tasks.finish()
}

func (m *traceCaptureManager) enqueuePersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) error {
	finalized, policy, err := prepareTrace(settings, trace)
	if err != nil {
		return err
	}
	m.mu.Lock()
	coordinator := m.coordinator
	m.mu.Unlock()
	if coordinator == nil {
		return &evalcapture.AdmissionError{
			Reason: evalcapture.ReasonClosed, TraceID: finalized.TraceID,
			Class: evalcapture.ClassCritical,
		}
	}
	err = coordinator.Submit(policy, finalized, evalcapture.ClassCritical)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to admit finalized evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
	}
	return err
}

func prepareTrace(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) (evaltrace.Trace, evalcapture.Policy, error) {
	if trace == nil || strings.TrimSpace(trace.workspace) == "" {
		return evaltrace.Trace{}, evalcapture.Policy{}, &evalcapture.AdmissionError{
			Reason: evalcapture.ReasonInvalidTrace,
			Class:  evalcapture.ClassCritical,
		}
	}
	finalized, err := trace.builder.Finalize()
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to finalize evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
		return evaltrace.Trace{}, evalcapture.Policy{}, err
	}
	policy := evalcapture.Policy{
		Root:      traceStoreRoot(settings, trace.workspace),
		Retention: settings.retention,
		MaxTraces: settings.maxTraces,
	}
	return finalized, policy, nil
}

func logTraceWriterEvent(event evalcapture.Event) {
	if event.Kind == evalcapture.EventPersisted {
		return
	}
	fields := map[string]any{
		"event": string(event.Kind), "reason": string(event.Reason),
		"trace_id": event.TraceID, "class": string(event.Class),
	}
	if event.Attempt > 0 {
		fields["attempt"] = event.Attempt
	}
	if event.Removed > 0 {
		fields["removed"] = event.Removed
	}
	if event.Dropped > 0 {
		fields["dropped"] = event.Dropped
	}
	if event.Err != nil {
		fields["error"] = event.Err.Error()
	}
	logger.WarnCF("evaltrace", "Evaluation trace writer event", fields)
}
