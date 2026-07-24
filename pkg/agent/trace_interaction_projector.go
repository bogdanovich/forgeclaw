package agent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/evalcapture"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const interactionTraceSubscriptionRetryDelay = time.Second

type interactionRegistrySubscription struct {
	registry    *interactions.Registry
	unsubscribe func()
}

type interactionTraceEvidence struct {
	Complete      bool
	FirstSequence int64
	LastSequence  int64
	LastRevision  int64
}

// interactionTraceProjector owns interaction-domain observation and source
// registration. The shared coordinator exclusively owns persistence scheduling,
// writer receipts, retry, capacity, and drain.
type interactionTraceProjector struct {
	mu          sync.Mutex
	closed      bool
	settings    traceCaptureSettings
	coordinator *evalcapture.Coordinator

	registries map[string]*interactions.Registry
	sources    map[string]*interactionTraceSource
	subs       map[string]interactionRegistrySubscription
	retryTimer *time.Timer
}

type interactionTraceSource struct {
	mu        sync.RWMutex
	workspace string
	registry  *interactions.Registry
	settings  traceCaptureSettings
}

func newInteractionTraceProjector(
	settings traceCaptureSettings,
	coordinator *evalcapture.Coordinator,
) *interactionTraceProjector {
	return &interactionTraceProjector{
		settings:    settings,
		coordinator: coordinator,
		registries:  make(map[string]*interactions.Registry),
		sources:     make(map[string]*interactionTraceSource),
		subs:        make(map[string]interactionRegistrySubscription),
	}
}

func (p *interactionTraceProjector) setCoordinator(
	coordinator *evalcapture.Coordinator,
) {
	if p == nil || coordinator == nil {
		return
	}
	p.mu.Lock()
	if p.coordinator == nil {
		p.coordinator = coordinator
	}
	p.mu.Unlock()
}

func (m *traceCaptureManager) attachInteractionRegistry(
	workspace string,
	registry *interactions.Registry,
) {
	if m != nil && m.interactions != nil {
		m.interactions.attach(workspace, registry)
	}
}

func (p *interactionTraceProjector) attach(
	workspace string,
	registry *interactions.Registry,
) {
	if p == nil || registry == nil {
		return
	}
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if existing := p.registries[workspace]; existing != nil {
		retry := existing == registry && p.settings.enabled &&
			p.sources[workspace] == nil
		p.mu.Unlock()
		if retry {
			p.install(workspace, registry)
		}
		return
	}
	p.registries[workspace] = registry
	enabled := p.settings.enabled
	protectionUpdated := true
	if !enabled {
		protectionUpdated = setInteractionRegistryTraceProtection(
			map[string]*interactions.Registry{workspace: registry},
			false,
			0,
		)
	}
	p.mu.Unlock()
	if enabled {
		p.install(workspace, registry)
		return
	}
	if !protectionUpdated {
		p.scheduleProtectionRetry()
	}
}

func (p *interactionTraceProjector) updateSettings(settings traceCaptureSettings) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	wasEnabled := p.settings.enabled
	p.settings = settings
	if wasEnabled && !settings.enabled {
		subs, sources := p.detachLocked()
		registries := cloneInteractionRegistries(p.registries)
		protectionUpdated := setInteractionRegistryTraceProtection(
			registries,
			false,
			0,
		)
		p.mu.Unlock()
		unsubscribeInteractionRegistries(subs)
		p.unregisterSources(sources)
		if !protectionUpdated {
			p.scheduleProtectionRetry()
		}
		return
	}
	if wasEnabled {
		for _, source := range p.sources {
			source.updateSettings(settings)
		}
		registries := cloneInteractionRegistries(p.registries)
		protectionUpdated := setInteractionRegistryTraceProtection(
			registries,
			true,
			settings.limits.MaxRecords,
		)
		p.mu.Unlock()
		if !protectionUpdated {
			p.scheduleProtectionRetry()
		}
		return
	}
	if !settings.enabled {
		p.mu.Unlock()
		return
	}
	registries := cloneInteractionRegistries(p.registries)
	p.mu.Unlock()
	for workspace, registry := range registries {
		p.install(workspace, registry)
	}
}

//nolint:dupl // Typed registry hooks mirror task installation; lifecycle is shared.
func (p *interactionTraceProjector) install(
	workspace string,
	registry *interactions.Registry,
) {
	p.mu.Lock()
	source := &interactionTraceSource{
		workspace: workspace,
		registry:  registry,
		settings:  p.settings,
	}
	sourceID := interactionTraceSourceID(workspace)
	activate := bindDurableProjectionSource(
		!p.closed && p.settings.enabled &&
			p.registries[workspace] == registry &&
			p.sources[workspace] == nil &&
			p.coordinator != nil,
		"interaction",
		workspace,
		p.coordinator,
		sourceID,
		source,
		p.settings.limits.MaxRecords,
		func(maxEvents int) error {
			return registry.SetTraceCaptureProtection(true, maxEvents)
		},
		func() (interactions.ObservationSnapshot, func(), func()) {
			return registry.SubscribeSnapshot(
				func(observation interactions.EventObservation) {
					p.observe(workspace, sourceID, observation)
				},
			)
		},
		func(source *interactionTraceSource, unsubscribe func()) {
			p.sources[workspace] = source
			p.subs[workspace] = interactionRegistrySubscription{
				registry: registry, unsubscribe: unsubscribe,
			}
		},
		p.requestSnapshotLocked,
		p.scheduleRetryLocked,
	)
	p.mu.Unlock()
	if activate != nil {
		activate()
	}
}

func (p *interactionTraceProjector) requestSnapshotLocked(
	sourceID string,
	snapshot interactions.ObservationSnapshot,
) {
	records := append([]interactions.Record(nil), snapshot.Records...)
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	for _, record := range records {
		if record.TraceCapturePending &&
			interactions.IsTerminalStatus(record.Status) {
			p.request(sourceID, record.ID)
		}
	}
}

func (p *interactionTraceProjector) observe(
	workspace, sourceID string,
	observation interactions.EventObservation,
) {
	if p == nil || !interactions.IsTerminalStatus(observation.Record.Status) {
		return
	}
	p.mu.Lock()
	active := !p.closed && p.settings.enabled &&
		p.sources[workspace] != nil
	p.mu.Unlock()
	if active {
		p.request(sourceID, observation.Record.ID)
	}
}

func (p *interactionTraceProjector) request(sourceID, interactionID string) {
	if p == nil || p.coordinator == nil {
		return
	}
	err := p.coordinator.Request(
		sourceID,
		encodeInteractionTraceProjectionKey(interactionID),
	)
	var admission *evalcapture.AdmissionError
	if err != nil && !errors.As(err, &admission) {
		logger.WarnCF("evaltrace", "Failed to request interaction trace projection", map[string]any{
			"source": sourceID,
			"error":  err.Error(),
		})
	}
}

func (p *interactionTraceProjector) scheduleRetryLocked() {
	if p.retryTimer != nil || p.closed {
		return
	}
	p.retryTimer = time.AfterFunc(
		interactionTraceSubscriptionRetryDelay,
		p.retryInstallations,
	)
}

func (p *interactionTraceProjector) retryInstallations() {
	p.mu.Lock()
	p.retryTimer = nil
	if p.closed {
		p.mu.Unlock()
		return
	}
	enabled := p.settings.enabled
	maxEvents := p.settings.limits.MaxRecords
	all := cloneInteractionRegistries(p.registries)
	missing := make(map[string]*interactions.Registry)
	for workspace, registry := range p.registries {
		if p.sources[workspace] == nil {
			missing[workspace] = registry
		}
	}
	protectionUpdated := setInteractionRegistryTraceProtection(
		all,
		enabled,
		maxEvents,
	)
	p.mu.Unlock()
	if !protectionUpdated {
		p.scheduleProtectionRetry()
	}
	if !enabled {
		return
	}
	for workspace, registry := range missing {
		p.install(workspace, registry)
	}
}

func (p *interactionTraceProjector) scheduleProtectionRetry() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.scheduleRetryLocked()
	p.mu.Unlock()
}

// stop detaches live observers while leaving sources registered for the
// coordinator's final recovery scan.
func (p *interactionTraceProjector) stop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	if p.retryTimer != nil {
		p.retryTimer.Stop()
		p.retryTimer = nil
	}
	subs := make([]interactionRegistrySubscription, 0, len(p.subs))
	for workspace, sub := range p.subs {
		subs = append(subs, sub)
		delete(p.subs, workspace)
	}
	p.mu.Unlock()
	unsubscribeInteractionRegistries(subs)
}

func (p *interactionTraceProjector) finish() {
	if p == nil {
		return
	}
	p.mu.Lock()
	sources := make(map[string]*interactionTraceSource, len(p.sources))
	for workspace, source := range p.sources {
		sources[workspace] = source
	}
	p.sources = nil
	p.registries = nil
	p.subs = nil
	p.mu.Unlock()
	p.unregisterSources(sources)
}

func (p *interactionTraceProjector) detachLocked() (
	[]interactionRegistrySubscription,
	map[string]*interactionTraceSource,
) {
	if p.retryTimer != nil {
		p.retryTimer.Stop()
		p.retryTimer = nil
	}
	subs := make([]interactionRegistrySubscription, 0, len(p.subs))
	for _, sub := range p.subs {
		subs = append(subs, sub)
	}
	sources := make(map[string]*interactionTraceSource, len(p.sources))
	for workspace, source := range p.sources {
		sources[workspace] = source
	}
	p.subs = make(map[string]interactionRegistrySubscription)
	p.sources = make(map[string]*interactionTraceSource)
	return subs, sources
}

func (p *interactionTraceProjector) unregisterSources(
	sources map[string]*interactionTraceSource,
) {
	if p == nil || p.coordinator == nil {
		return
	}
	for workspace := range sources {
		p.coordinator.UnregisterSource(interactionTraceSourceID(workspace))
	}
}

func (s *interactionTraceSource) updateSettings(settings traceCaptureSettings) {
	s.mu.Lock()
	s.settings = settings
	s.mu.Unlock()
}

func (s *interactionTraceSource) currentSettings() traceCaptureSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

func (s *interactionTraceSource) Pending(
	ctx context.Context,
	limit int,
) ([]string, error) {
	if s == nil || s.registry == nil || limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records, err := s.registry.ListPendingTraceCaptures(ctx, limit)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, min(limit, len(records)))
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keys = append(keys, encodeInteractionTraceProjectionKey(record.ID))
	}
	return keys, nil
}

func (s *interactionTraceSource) LoadLatest(
	ctx context.Context,
	key string,
) (evalcapture.DurableCandidate, bool, error) {
	if err := ctx.Err(); err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	interactionID, err := decodeInteractionTraceProjectionKey(key)
	if err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	record, events, exists, err := s.registry.LoadTraceCapture(ctx, interactionID)
	if err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	if !exists || !record.TraceCapturePending ||
		!interactions.IsTerminalStatus(record.Status) {
		return evalcapture.DurableCandidate{}, false, nil
	}
	if record.LastEventSeq <= 0 {
		return evalcapture.DurableCandidate{}, false,
			errors.New("terminal interaction trace has no revision")
	}
	settings := s.currentSettings()
	trace, _ := buildInteractionTrace(settings, s.workspace, record, events)
	finalized, policy, err := prepareTrace(settings, trace)
	if err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	finalized, _, err = reconcileStoredInteractionTrace(policy, finalized)
	if err != nil {
		return evalcapture.DurableCandidate{}, false, err
	}
	return evalcapture.DurableCandidate{
		Revision: uint64(record.LastEventSeq),
		Policy:   policy,
		Trace:    finalized,
		Persist:  true,
	}, true, nil
}

func (s *interactionTraceSource) Confirm(
	ctx context.Context,
	key string,
	revision uint64,
) (evalcapture.Confirmation, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if revision > math.MaxInt64 {
		return "", fmt.Errorf("interaction trace revision exceeds int64")
	}
	interactionID, err := decodeInteractionTraceProjectionKey(key)
	if err != nil {
		return "", err
	}
	record, confirmed, err := s.registry.ConfirmTraceCapturePersisted(
		ctx,
		interactionID,
		int64(revision),
	)
	if err != nil {
		if errors.Is(err, interactions.ErrNotFound) {
			return evalcapture.ConfirmationGone, nil
		}
		return "", err
	}
	if confirmed {
		return evalcapture.ConfirmationCurrent, nil
	}
	if record.LastEventSeq != int64(revision) {
		return evalcapture.ConfirmationStale, nil
	}
	return "", fmt.Errorf("interaction trace revision %d was not confirmed", revision)
}

func interactionTraceSourceID(workspace string) string {
	sum := sha256.Sum256([]byte(workspace))
	return fmt.Sprintf("interaction:%x", sum[:12])
}

func encodeInteractionTraceProjectionKey(interactionID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(interactionID))
}

func decodeInteractionTraceProjectionKey(key string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(key))
	if err != nil || len(decoded) == 0 {
		return "", errors.New("invalid interaction trace projection key")
	}
	return string(decoded), nil
}

func unsubscribeInteractionRegistries(subs []interactionRegistrySubscription) {
	for _, sub := range subs {
		if sub.unsubscribe != nil {
			sub.unsubscribe()
		}
	}
}

func setInteractionRegistryTraceProtection(
	registries map[string]*interactions.Registry,
	enabled bool,
	maxEvents int,
) bool {
	success := true
	for workspace, registry := range registries {
		if err := registry.SetTraceCaptureProtection(
			enabled,
			maxEvents,
		); err != nil {
			success = false
			logger.WarnCF("evaltrace", "Failed to update interaction trace retention protection", map[string]any{
				"workspace": workspace,
				"enabled":   enabled,
				"error":     err.Error(),
			})
		}
	}
	return success
}

func cloneInteractionRegistries(
	registries map[string]*interactions.Registry,
) map[string]*interactions.Registry {
	cloned := make(map[string]*interactions.Registry, len(registries))
	for workspace, registry := range registries {
		cloned[workspace] = registry
	}
	return cloned
}

func buildInteractionTrace(
	settings traceCaptureSettings,
	workspace string,
	record interactions.Record,
	events []interactions.Event,
) (*activeTraceCapture, interactionTraceEvidence) {
	startedAt := time.UnixMilli(record.CreatedAt)
	if record.CreatedAt <= 0 {
		startedAt = time.UnixMilli(1)
	}
	metadataSettings := settings
	metadataSettings.contentMode = evaltrace.ContentMetadataOnly
	active := &activeTraceCapture{
		workspace: workspace,
		turnID:    record.Origin.TurnID,
		startedAt: startedAt,
		builder: evalcapture.NewTraceBuilder(evaltrace.Trace{
			SchemaVersion: evaltrace.SchemaVersionV1,
			TraceID: opaqueTraceID(
				"interaction",
				strings.TrimSpace(workspace)+"\x00"+record.ID,
				startedAt,
			),
			CreatedAt: startedAt.UTC(),
			Policy: evaltrace.CapturePolicy{
				ContentMode: evaltrace.ContentMetadataOnly,
			},
			Limits: settings.limits,
			Metadata: evaltrace.Metadata{
				TraceKind:          evaltrace.TraceKindInteraction,
				RootTurnID:         record.Origin.TurnID,
				SessionHash:        safeHash(metadataSettings, record.Route.SessionKey),
				AgentID:            record.Route.AgentID,
				ProjectionRevision: uint64(max(0, record.LastEventSeq)),
			},
			Records: make([]evaltrace.Record, 0, len(events)),
		}),
	}
	history := append([]interactions.Event(nil), events...)
	sortInteractionEvents(history)
	evidence := interactionTraceEvidence{}
	historyComplete := true
	lastOffset := int64(0)
	for _, event := range history {
		if event.InteractionID != record.ID || event.Sequence <= evidence.LastSequence {
			continue
		}
		if evidence.FirstSequence == 0 {
			evidence.FirstSequence = event.Sequence
			if event.Sequence > 1 {
				historyComplete = false
				active.builder.MarkIncomplete(
					"interaction_history_prefix_missing",
					int(event.Sequence-1),
				)
			}
		} else if event.Sequence > evidence.LastSequence+1 {
			historyComplete = false
			active.builder.MarkIncomplete(
				"interaction_event_sequence_gap",
				int(event.Sequence-evidence.LastSequence-1),
			)
		}
		item := normalizedInteractionEventRecord(
			metadataSettings, active, record, event,
		)
		if item.OffsetNanos < lastOffset {
			item.OffsetNanos = lastOffset
		}
		appendCaptureRecord(active, item, true)
		lastOffset = item.OffsetNanos
		evidence.LastSequence = event.Sequence
		evidence.LastRevision = event.Revision
	}
	if evidence.LastSequence < record.LastEventSeq {
		historyComplete = false
		active.builder.MarkIncomplete(
			"interaction_history_suffix_missing",
			int(record.LastEventSeq-evidence.LastSequence),
		)
	}
	if len(history) == 0 && record.LastEventSeq <= 0 {
		historyComplete = false
		active.builder.MarkIncomplete("interaction_history_missing", 1)
	}
	evidence.Complete = historyComplete && evidence.FirstSequence == 1 &&
		evidence.LastSequence == record.LastEventSeq &&
		evidence.LastRevision == record.Revision
	if !evidence.Complete && evidence.LastSequence == record.LastEventSeq &&
		evidence.LastRevision != record.Revision {
		active.builder.MarkIncomplete("interaction_revision_evidence_missing", 0)
	}
	if interactions.IsTerminalStatus(record.Status) {
		active.builder.SetOutcome(evaltrace.Outcome{
			Status: string(record.Status), ErrorCode: interactionErrorCode(record),
		})
	}
	return active, evidence
}

func normalizedInteractionEventRecord(
	settings traceCaptureSettings,
	active *activeTraceCapture,
	record interactions.Record,
	event interactions.Event,
) evaltrace.Record {
	payload := evaltrace.InteractionPayload{
		EventType:      string(event.Type),
		Kind:           string(record.Kind),
		From:           string(event.From),
		Status:         string(event.To),
		Outcome:        string(event.Outcome),
		Revision:       event.Revision,
		Sequence:       event.Sequence,
		CommitSequence: event.CommitSequence,
		CodeHash:       safeHash(settings, event.Code),
		Success:        event.Success,
	}
	data, _ := json.Marshal(payload)
	return evaltrace.Record{
		OffsetNanos: max(
			0,
			time.UnixMilli(event.EmittedAt).Sub(active.startedAt).Nanoseconds(),
		),
		Kind:   evaltrace.RecordInteractionTransition,
		Origin: evaltrace.Origin{Kind: "interaction_event", ID: event.EventID},
		Scope: evaltrace.Scope{
			AgentID:     record.Route.AgentID,
			SessionHash: safeHash(settings, record.Route.SessionKey),
			TurnID:      record.Origin.TurnID,
			TaskID:      record.Origin.TaskID,
			Channel:     record.Route.Channel,
			TargetHash: safeHash(
				settings,
				targetKey(record.Route.Channel, record.Route.ChatID),
			),
		},
		Correlation: evaltrace.Correlation{
			InteractionID: record.ID,
			ToolCallID:    record.Origin.ToolCallID,
			EventID:       event.EventID,
		},
		Data: data,
	}
}

func reconcileStoredInteractionTrace(
	policy evalcapture.Policy,
	candidate evaltrace.Trace,
) (evaltrace.Trace, bool, error) {
	existing, err := (evaltrace.Store{Root: policy.Root}).Load(candidate.TraceID)
	if errors.Is(err, os.ErrNotExist) {
		return candidate, true, nil
	}
	var corrupt *evaltrace.CorruptTraceError
	if errors.As(err, &corrupt) {
		logger.WarnCF("evaltrace", "Replacing corrupt stored interaction trace", map[string]any{
			"trace_id": candidate.TraceID,
			"error":    corrupt.Error(),
		})
		return candidate, true, nil
	}
	if err != nil {
		return evaltrace.Trace{}, false, fmt.Errorf(
			"interaction trace storage: %w",
			err,
		)
	}
	existingRevision := existing.Metadata.ProjectionRevision
	candidateRevision := candidate.Metadata.ProjectionRevision
	if candidateRevision > existingRevision {
		return candidate, true, nil
	}
	if candidateRevision < existingRevision {
		return existing, false, nil
	}
	if !existing.Truncation.Incomplete && candidate.Truncation.Incomplete {
		return existing, false, nil
	}
	if existing.Truncation.Incomplete && !candidate.Truncation.Incomplete {
		return candidate, true, nil
	}
	if traceRecordsExtend(existing.Records, candidate.Records) {
		improves := len(candidate.Records) > len(existing.Records) ||
			existing.Truncation.Incomplete != candidate.Truncation.Incomplete ||
			existing.Truncation.DroppedRecords != candidate.Truncation.DroppedRecords
		return candidate, improves, nil
	}
	return existing, false, nil
}

func interactionErrorCode(record interactions.Record) string {
	if record.Status == interactions.StatusFailed {
		return "interaction_failed"
	}
	if record.Outcome == interactions.OutcomeDeliveryUnknown {
		return "delivery_unknown"
	}
	return ""
}

func sortInteractionEvents(events []interactions.Event) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Sequence != events[j].Sequence {
			return events[i].Sequence < events[j].Sequence
		}
		return events[i].EventID < events[j].EventID
	})
}
