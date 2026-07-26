package agent

import (
	"strings"
	"sync"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/diagnosticcapture"
	"github.com/sipeed/picoclaw/pkg/diagnostictrace"
	"github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const tracePersistBuffer = 128

type traceCaptureManager struct {
	mu      sync.Mutex
	closed  bool
	startMu sync.Mutex

	settings traceCaptureSettings
	turns    *turnTraceProjector
	writer   *diagnosticcapture.Writer
}

func newTraceCaptureManager(cfg *config.Config, eventBus events.Bus) *traceCaptureManager {
	settings := traceCaptureSettingsFromConfig(cfg)
	manager := &traceCaptureManager{settings: settings}
	manager.turns = newTurnTraceProjector(settings, eventBus, manager.enqueuePersist)
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
		m.writer = diagnosticcapture.NewWriter(diagnosticcapture.Options{
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
	turns := m.turns
	m.mu.Unlock()

	if updated.enabled {
		m.start()
	}
	turns.updateSettings(updated)
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
	turns, writer := m.turns, m.writer
	m.mu.Unlock()

	turns.close()
	if writer != nil {
		writer.Close()
	}
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
	writer := m.writer
	m.mu.Unlock()
	if writer == nil {
		return &diagnosticcapture.DropError{
			Reason: diagnosticcapture.ReasonClosed, TraceID: finalized.TraceID,
		}
	}
	err = writer.Submit(policy, finalized)
	if err != nil {
		logger.WarnCF("diagnostictrace", "Failed to admit finalized diagnostic trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
	}
	return err
}

func prepareTrace(
	settings traceCaptureSettings,
	trace *activeTraceCapture,
) (diagnostictrace.Trace, diagnosticcapture.Policy, error) {
	if trace == nil || strings.TrimSpace(trace.workspace) == "" {
		return diagnostictrace.Trace{}, diagnosticcapture.Policy{}, &diagnosticcapture.DropError{
			Reason: diagnosticcapture.ReasonInvalidTrace,
		}
	}
	finalized, err := trace.builder.Finalize()
	if err != nil {
		logger.WarnCF("diagnostictrace", "Failed to finalize diagnostic trace", map[string]any{
			"trace_id": trace.builder.TraceID(), "error": err.Error(),
		})
		return diagnostictrace.Trace{}, diagnosticcapture.Policy{}, err
	}
	policy := diagnosticcapture.Policy{
		Root:      traceStoreRoot(settings, trace.workspace),
		Retention: settings.retention,
		MaxTraces: settings.maxTraces,
	}
	return finalized, policy, nil
}

func logTraceWriterEvent(event diagnosticcapture.Event) {
	if event.Kind == diagnosticcapture.EventPersisted {
		return
	}
	fields := map[string]any{
		"event": string(event.Kind), "reason": string(event.Reason),
		"trace_id": event.TraceID,
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
	logger.WarnCF("diagnostictrace", "Diagnostic trace writer event", fields)
}
