package agent

import (
	"context"
	"strings"
	"sync"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const tracePersistBuffer = 128

type traceCaptureManager struct {
	mu      sync.Mutex
	closed  bool
	startMu sync.Mutex

	settings     traceCaptureSettings
	turns        *turnTraceProjector
	tasks        *taskTraceProjector
	interactions *interactionTraceProjector
	writer       *evalcapture.Writer
}

func newTraceCaptureManager(cfg *config.Config, eventBus events.Bus) *traceCaptureManager {
	settings := traceCaptureSettingsFromConfig(cfg)
	manager := &traceCaptureManager{settings: settings}
	manager.turns = newTurnTraceProjector(settings, eventBus, manager.enqueuePersist)
	manager.tasks = newTaskTraceProjector(settings, manager.enqueuePersist)
	manager.interactions = newInteractionTraceProjector(settings, manager.enqueuePersist)
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
			EventSink: logTraceWriterEvent,
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
	turns, tasks, interactions := m.turns, m.tasks, m.interactions
	m.mu.Unlock()

	if updated.enabled {
		m.start()
	}
	turns.updateSettings(updated)
	tasks.updateSettings(updated)
	interactions.updateSettings(updated)
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
	turns, tasks, interactions := m.turns, m.tasks, m.interactions
	m.mu.Unlock()

	turns.close()
	tasks.close()
	interactions.close()

	m.mu.Lock()
	writer := m.writer
	m.writer = nil
	m.mu.Unlock()
	if writer != nil {
		_ = writer.Close(context.Background())
	}
}

func (m *traceCaptureManager) enqueuePersist(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) error {
	if m == nil || trace == nil || strings.TrimSpace(trace.workspace) == "" {
		return &evalcapture.AdmissionError{
			Reason: evalcapture.ReasonInvalidTrace,
			Class:  evalcapture.ClassCritical,
		}
	}
	finalized, err := trace.builder.Finalize()
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to finalize evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
		return err
	}
	m.mu.Lock()
	writer := m.writer
	m.mu.Unlock()
	if writer == nil {
		return &evalcapture.AdmissionError{
			Reason:  evalcapture.ReasonClosed,
			TraceID: trace.builder.TraceID(),
			Class:   evalcapture.ClassCritical,
		}
	}
	err = writer.Submit(evalcapture.Policy{
		Root:      traceStoreRoot(settings, trace.workspace),
		Retention: settings.retention,
		MaxTraces: settings.maxTraces,
	}, finalized, evalcapture.ClassCritical)
	if err != nil {
		logger.WarnCF("evaltrace", "Failed to admit finalized evaluation trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
		return err
	}
	return nil
}

func logTraceWriterEvent(event evalcapture.Event) {
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
