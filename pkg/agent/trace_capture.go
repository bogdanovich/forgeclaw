package agent

import (
	"context"
	"errors"
	"os"
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
)

type traceCaptureManager struct {
	mu      sync.Mutex
	closed  bool
	startMu sync.Mutex

	settings traceCaptureSettings
	turns    *turnTraceProjector
	tasks    *taskTraceProjector
	writer   *evalcapture.Writer
}

func newTraceCaptureManager(cfg *config.Config, eventBus events.Bus) *traceCaptureManager {
	settings := traceCaptureSettingsFromConfig(cfg)
	manager := &traceCaptureManager{settings: settings}
	manager.turns = newTurnTraceProjector(settings, eventBus, manager.enqueuePersist)
	manager.tasks = newTaskTraceProjector(
		settings,
		manager.enqueueTaskPersist,
		manager.enqueueTaskPersistWait,
	)
	manager.tasks.awaitPersistence = true
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
	if m.writer == nil {
		m.writer = evalcapture.NewWriter(evalcapture.Options{
			Capacity:  tracePersistBuffer,
			EventSink: m.handleTraceWriterEvent,
		})
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
	turns, tasks := m.turns, m.tasks
	m.mu.Unlock()

	turns.close()
	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		traceShutdownAdmissionTimeout,
	)
	defer shutdownCancel()
	if err := tasks.closeWithContext(shutdownCtx); err != nil {
		logger.WarnCF("evaltrace", "Deferred task trace persistence to registry during shutdown", map[string]any{
			"error": err.Error(),
		})
	}

	m.mu.Lock()
	writer := m.writer
	m.writer = nil
	m.mu.Unlock()
	if writer != nil {
		if err := writer.Close(shutdownCtx); err != nil {
			logger.WarnCF("evaltrace", "Trace writer did not drain before shutdown deadline", map[string]any{
				"error": err.Error(),
			})
		}
	}
	tasks.finishClose()
}

func (m *traceCaptureManager) enqueuePersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) error {
	finalized, policy, writer, err := m.preparePersist(settings, trace)
	if err != nil {
		return err
	}
	err = writer.Submit(policy, finalized, evalcapture.ClassCritical)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to admit finalized evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
	}
	return err
}

func (m *traceCaptureManager) enqueueTaskPersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) error {
	finalized, policy, writer, err := m.preparePersist(settings, trace)
	if err != nil {
		return err
	}
	finalized, persist, err := reconcileStoredTaskTrace(policy, finalized)
	if err != nil || !persist {
		if err == nil {
			return errTaskTraceAlreadyDurable
		}
		return err
	}
	err = writer.SubmitTracked(
		policy,
		finalized,
		evalcapture.ClassCritical,
		trace.submissionID,
	)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to admit finalized task trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
	}
	return err
}

func (m *traceCaptureManager) enqueueTaskPersistWait(
	ctx context.Context,
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) error {
	finalized, policy, writer, err := m.preparePersist(settings, trace)
	if err != nil {
		return err
	}
	finalized, persist, err := reconcileStoredTaskTrace(policy, finalized)
	if err != nil || !persist {
		if err == nil {
			return errTaskTraceAlreadyDurable
		}
		return err
	}
	err = writer.SubmitWaitTracked(
		ctx,
		policy,
		finalized,
		evalcapture.ClassCritical,
		trace.submissionID,
	)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to admit finalized task trace during shutdown", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
	}
	return err
}

func reconcileStoredTaskTrace(
	policy evalcapture.Policy,
	candidate evaltrace.Trace,
) (evaltrace.Trace, bool, error) {
	existing, err := (evaltrace.Store{Root: policy.Root}).Load(candidate.TraceID)
	if errors.Is(err, os.ErrNotExist) {
		return candidate, true, nil
	}
	var corrupt *evaltrace.CorruptTraceError
	if errors.As(err, &corrupt) {
		logger.WarnCF("evaltrace", "Replacing corrupt stored task trace", map[string]any{
			"trace_id": candidate.TraceID,
			"error":    corrupt.Error(),
		})
		return candidate, true, nil
	}
	if err != nil {
		return evaltrace.Trace{}, false, err
	}
	selected, persist := reconcileTaskTraceCandidate(existing, candidate)
	return selected, persist, nil
}

func (m *traceCaptureManager) preparePersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) (evaltrace.Trace, evalcapture.Policy, *evalcapture.Writer, error) {
	if m == nil || trace == nil || strings.TrimSpace(trace.workspace) == "" {
		return evaltrace.Trace{}, evalcapture.Policy{}, nil, &evalcapture.AdmissionError{
			Reason: evalcapture.ReasonInvalidTrace,
			Class:  evalcapture.ClassCritical,
		}
	}
	finalized, err := trace.builder.Finalize()
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to finalize evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
		return evaltrace.Trace{}, evalcapture.Policy{}, nil, err
	}
	m.mu.Lock()
	writer := m.writer
	m.mu.Unlock()
	if writer == nil {
		return evaltrace.Trace{}, evalcapture.Policy{}, nil, &evalcapture.AdmissionError{
			Reason:  evalcapture.ReasonClosed,
			TraceID: trace.builder.TraceID(),
			Class:   evalcapture.ClassCritical,
		}
	}
	policy := evalcapture.Policy{
		Root:      traceStoreRoot(settings, trace.workspace),
		Retention: settings.retention,
		MaxTraces: settings.maxTraces,
	}
	return finalized, policy, writer, nil
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

func (m *traceCaptureManager) handleTraceWriterEvent(event evalcapture.Event) {
	logTraceWriterEvent(event)
	if m != nil && m.tasks != nil {
		m.tasks.observeWriterEvent(event)
	}
}
