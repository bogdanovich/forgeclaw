package nodes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const (
	gatewayInvocationStoreVersion      = 1
	DefaultGatewayInvocationLimit      = 256
	DefaultGatewayInvocationStoreBytes = 32 * 1024 * 1024
	DefaultGatewayInvocationRetention  = 7 * 24 * time.Hour
	maxGatewayToolCallIDLength         = 512
	maxGatewayTargetLength             = 64
)

var (
	ErrGatewayInvocationConflict  = errors.New("gateway node invocation conflicts with durable state")
	ErrGatewayInvocationNotFound  = errors.New("gateway node invocation not found")
	ErrGatewayInvocationStoreFull = errors.New("gateway node invocation store is full")
	gatewayTargetPattern          = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
)

type GatewayInvocationState string

const (
	GatewayInvocationPrepared   GatewayInvocationState = "prepared"
	GatewayInvocationDispatched GatewayInvocationState = "dispatched"
)

// GatewayInvocationRecord is the gateway-owned authority that links one model
// tool call to one immutable execution plan. ExpectedPlanHash is stored
// separately so a mutated plan cannot validate itself.
type GatewayInvocationRecord struct {
	Target           string                 `json:"target"`
	ToolCallID       string                 `json:"tool_call_id"`
	Plan             ExecutionPlan          `json:"plan"`
	ExpectedPlanHash string                 `json:"expected_plan_hash"`
	State            GatewayInvocationState `json:"state"`
	CreatedAt        int64                  `json:"created_at"`
	UpdatedAt        int64                  `json:"updated_at"`
	DispatchedAt     int64                  `json:"dispatched_at,omitempty"`
}

type gatewayInvocationDocument struct {
	Version int                                `json:"version"`
	Records map[string]GatewayInvocationRecord `json:"records"`
}

// GatewayInvocationStore persists prepared plan ownership across gateway
// restarts. The gateway service is the single writer; atomic replacement keeps
// crash recovery from observing a partially written snapshot.
type GatewayInvocationStore struct {
	path       string
	maxRecords int
	maxBytes   int
	retention  time.Duration
	now        func() time.Time
	writeFile  func(string, []byte, os.FileMode) error

	mu      sync.Mutex
	records map[string]GatewayInvocationRecord
}

func GatewayInvocationStorePath(workspace string) string {
	return filepath.Join(workspace, "state", "node_invocations.json")
}

func NewGatewayInvocationStore(
	path string,
	maxRecords int,
	maxBytes int,
) (*GatewayInvocationStore, error) {
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return nil, errors.New("gateway node invocation store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create gateway node invocation store directory: %w", err)
	}
	store := newGatewayInvocationStore(path, maxRecords, maxBytes, time.Now)
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func newGatewayInvocationStore(
	path string,
	maxRecords int,
	maxBytes int,
	now func() time.Time,
) *GatewayInvocationStore {
	if maxRecords <= 0 {
		maxRecords = DefaultGatewayInvocationLimit
	}
	if maxBytes <= 0 {
		maxBytes = DefaultGatewayInvocationStoreBytes
	}
	if now == nil {
		now = time.Now
	}
	return &GatewayInvocationStore{
		path:       path,
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
		retention:  DefaultGatewayInvocationRetention,
		now:        now,
		writeFile:  fileutil.WriteFileAtomic,
		records:    make(map[string]GatewayInvocationRecord),
	}
}

func (store *GatewayInvocationStore) Prepare(
	target string,
	toolCallID string,
	plan ExecutionPlan,
) (GatewayInvocationRecord, error) {
	now := store.now()
	record := GatewayInvocationRecord{
		Target:           strings.TrimSpace(target),
		ToolCallID:       strings.TrimSpace(toolCallID),
		Plan:             cloneExecutionPlan(plan),
		ExpectedPlanHash: plan.PlanHash,
		State:            GatewayInvocationPrepared,
		CreatedAt:        now.UnixNano(),
		UpdatedAt:        now.UnixNano(),
	}
	if err := record.validate(); err != nil {
		return GatewayInvocationRecord{}, err
	}
	if now.Unix() >= plan.ExpiresAt {
		return GatewayInvocationRecord{}, fmt.Errorf(
			"%w: execution plan expired before persistence",
			ErrInvalidInvocation,
		)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	previous := cloneGatewayInvocationRecords(store.records)
	store.pruneLocked(now)
	for _, existing := range store.records {
		if sameGatewayToolCall(existing, plan.AgentID, plan.SessionID, record.ToolCallID) {
			if existing.Target == record.Target &&
				existing.ExpectedPlanHash == record.ExpectedPlanHash {
				return cloneGatewayInvocationRecord(existing), nil
			}
			store.records = previous
			return GatewayInvocationRecord{}, ErrGatewayInvocationConflict
		}
	}
	if existing, found := store.records[plan.InvocationID]; found {
		store.records = previous
		if existing.ExpectedPlanHash == record.ExpectedPlanHash {
			return cloneGatewayInvocationRecord(existing), nil
		}
		return GatewayInvocationRecord{}, ErrGatewayInvocationConflict
	}
	if len(store.records) >= store.maxRecords {
		store.records = previous
		return GatewayInvocationRecord{}, ErrGatewayInvocationStoreFull
	}
	store.records[plan.InvocationID] = record
	if err := store.saveLocked(); err != nil {
		store.records = previous
		return GatewayInvocationRecord{}, fmt.Errorf("persist prepared node invocation: %w", err)
	}
	return cloneGatewayInvocationRecord(record), nil
}

func (store *GatewayInvocationStore) ByToolCall(
	agentID string,
	sessionID string,
	toolCallID string,
) (GatewayInvocationRecord, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, record := range store.records {
		if sameGatewayToolCall(record, agentID, sessionID, toolCallID) {
			return cloneGatewayInvocationRecord(record), true
		}
	}
	return GatewayInvocationRecord{}, false
}

func (store *GatewayInvocationStore) Lookup(
	agentID string,
	sessionID string,
	invocationID string,
) (GatewayInvocationRecord, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	record, found := store.records[invocationID]
	if !found || record.Plan.AgentID != agentID || record.Plan.SessionID != sessionID {
		return GatewayInvocationRecord{}, false
	}
	return cloneGatewayInvocationRecord(record), true
}

func (store *GatewayInvocationStore) MarkDispatched(
	invocationID string,
	expectedPlanHash string,
) (GatewayInvocationRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	record, found := store.records[invocationID]
	if !found {
		return GatewayInvocationRecord{}, ErrGatewayInvocationNotFound
	}
	if err := record.Plan.ValidateAgainstHash(expectedPlanHash); err != nil ||
		record.ExpectedPlanHash != expectedPlanHash {
		return GatewayInvocationRecord{}, ErrGatewayInvocationConflict
	}
	if record.State == GatewayInvocationDispatched {
		return cloneGatewayInvocationRecord(record), nil
	}
	previous := cloneGatewayInvocationRecords(store.records)
	now := store.now().UnixNano()
	record.State = GatewayInvocationDispatched
	record.DispatchedAt = now
	record.UpdatedAt = now
	store.records[invocationID] = record
	if err := store.saveLocked(); err != nil {
		store.records = previous
		return GatewayInvocationRecord{}, fmt.Errorf("persist dispatched node invocation: %w", err)
	}
	return cloneGatewayInvocationRecord(record), nil
}

func (store *GatewayInvocationStore) load() error {
	info, err := os.Stat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat gateway node invocation store: %w", err)
	}
	if info.Size() > int64(store.maxBytes) {
		return ErrGatewayInvocationStoreFull
	}
	file, err := os.Open(store.path)
	if err != nil {
		return fmt.Errorf("open gateway node invocation store: %w", err)
	}
	defer file.Close()
	var document gatewayInvocationDocument
	decoder := json.NewDecoder(io.LimitReader(file, int64(store.maxBytes)+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode gateway node invocation store: %w", err)
	}
	if trailingErr := decoder.Decode(new(any)); !errors.Is(trailingErr, io.EOF) {
		return errors.New("decode gateway node invocation store: trailing data")
	}
	if document.Version != gatewayInvocationStoreVersion ||
		document.Records == nil ||
		len(document.Records) > store.maxRecords {
		return errors.New("gateway node invocation store has invalid metadata")
	}
	for invocationID, record := range document.Records {
		if invocationID != record.Plan.InvocationID {
			return errors.New("gateway node invocation store has mismatched record identity")
		}
		if err := record.validate(); err != nil {
			return fmt.Errorf("validate gateway node invocation %q: %w", invocationID, err)
		}
	}
	store.records = cloneGatewayInvocationRecords(document.Records)
	return nil
}

func (store *GatewayInvocationStore) saveLocked() error {
	document := gatewayInvocationDocument{
		Version: gatewayInvocationStoreVersion,
		Records: store.records,
	}
	data, err := json.Marshal(document)
	if err != nil {
		return err
	}
	if len(data)+1 > store.maxBytes {
		return ErrGatewayInvocationStoreFull
	}
	if store.path == "" {
		return nil
	}
	return store.writeFile(store.path, append(data, '\n'), 0o600)
}

func (store *GatewayInvocationStore) pruneLocked(now time.Time) {
	retentionBefore := now.Add(-store.retention).UnixNano()
	for invocationID, record := range store.records {
		if record.State == GatewayInvocationPrepared && now.Unix() >= record.Plan.ExpiresAt {
			delete(store.records, invocationID)
			continue
		}
		if record.State == GatewayInvocationDispatched && record.UpdatedAt < retentionBefore {
			delete(store.records, invocationID)
		}
	}
}

func (record GatewayInvocationRecord) validate() error {
	if len(record.Target) == 0 || len(record.Target) > maxGatewayTargetLength ||
		!gatewayTargetPattern.MatchString(record.Target) {
		return fmt.Errorf("%w: malformed execution target", ErrInvalidInvocation)
	}
	if len(record.ToolCallID) == 0 || len(record.ToolCallID) > maxGatewayToolCallIDLength {
		return fmt.Errorf("%w: malformed tool call identity", ErrInvalidInvocation)
	}
	if err := record.Plan.ValidateAgainstHash(record.ExpectedPlanHash); err != nil {
		return err
	}
	switch record.State {
	case GatewayInvocationPrepared:
		if record.DispatchedAt != 0 {
			return fmt.Errorf("%w: prepared invocation has dispatch time", ErrInvalidInvocation)
		}
	case GatewayInvocationDispatched:
		if record.DispatchedAt <= 0 {
			return fmt.Errorf("%w: dispatched invocation lacks dispatch time", ErrInvalidInvocation)
		}
	default:
		return fmt.Errorf("%w: invalid gateway invocation state", ErrInvalidInvocation)
	}
	if record.CreatedAt <= 0 || record.UpdatedAt < record.CreatedAt {
		return fmt.Errorf("%w: invalid gateway invocation timestamps", ErrInvalidInvocation)
	}
	return nil
}

func sameGatewayToolCall(
	record GatewayInvocationRecord,
	agentID string,
	sessionID string,
	toolCallID string,
) bool {
	return record.Plan.AgentID == strings.TrimSpace(agentID) &&
		record.Plan.SessionID == strings.TrimSpace(sessionID) &&
		record.ToolCallID == strings.TrimSpace(toolCallID)
}

func cloneGatewayInvocationRecords(
	records map[string]GatewayInvocationRecord,
) map[string]GatewayInvocationRecord {
	cloned := make(map[string]GatewayInvocationRecord, len(records))
	for invocationID, record := range records {
		cloned[invocationID] = cloneGatewayInvocationRecord(record)
	}
	return cloned
}

func cloneGatewayInvocationRecord(record GatewayInvocationRecord) GatewayInvocationRecord {
	record.Plan = cloneExecutionPlan(record.Plan)
	return record
}

func cloneExecutionPlan(plan ExecutionPlan) ExecutionPlan {
	plan.Input = bytes.Clone(plan.Input)
	return plan
}
