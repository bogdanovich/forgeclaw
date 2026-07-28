package nodes

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

const (
	gatewayTerminalStoreVersion      = 1
	DefaultGatewayTerminalLimit      = 256
	DefaultGatewayTerminalStoreBytes = 4 * 1024 * 1024
	DefaultGatewayTerminalRetention  = 30 * 24 * time.Hour
)

var (
	ErrGatewayTerminalConflict  = errors.New("gateway terminal conflicts with durable state")
	ErrGatewayTerminalNotFound  = errors.New("gateway terminal not found")
	ErrGatewayTerminalStoreFull = errors.New("gateway terminal store is full")
)

type GatewayTerminalState string

const (
	GatewayTerminalPrepared      GatewayTerminalState = "prepared"
	GatewayTerminalDispatched    GatewayTerminalState = "dispatched"
	GatewayTerminalPendingAttach GatewayTerminalState = "pending_attach"
	GatewayTerminalLive          GatewayTerminalState = "live"
	GatewayTerminalClosed        GatewayTerminalState = "closed"
	GatewayTerminalUnknown       GatewayTerminalState = "unknown"
)

// GatewayTerminalRecord is the gateway-owned durable authority for one
// attached terminal. It intentionally retains no terminal input or output.
type GatewayTerminalRecord struct {
	Plan                 TerminalOpenPlan     `json:"plan"`
	ExpectedPlanHash     string               `json:"expected_plan_hash"`
	TerminalID           string               `json:"terminal_id,omitempty"`
	State                GatewayTerminalState `json:"state"`
	Reason               string               `json:"reason,omitempty"`
	CreatedAt            int64                `json:"created_at"`
	UpdatedAt            int64                `json:"updated_at"`
	DispatchedAt         int64                `json:"dispatched_at,omitempty"`
	StartedAt            int64                `json:"started_at,omitempty"`
	CompletedAt          int64                `json:"completed_at,omitempty"`
	ExitCode             int                  `json:"exit_code,omitempty"`
	Signal               string               `json:"signal,omitempty"`
	TerminationConfirmed bool                 `json:"termination_confirmed,omitempty"`
}

type gatewayTerminalDocument struct {
	Version int                              `json:"version"`
	Records map[string]GatewayTerminalRecord `json:"records"`
}

// GatewayTerminalStore persists terminal plans and redacted lifecycle
// metadata. Opening the store converts any previously active terminal to
// unknown because P1 deliberately has no reconnect or replay.
type GatewayTerminalStore struct {
	path       string
	maxRecords int
	maxBytes   int
	retention  time.Duration
	now        func() time.Time
	writeFile  func(string, []byte, os.FileMode) error

	mu      sync.Mutex
	records map[string]GatewayTerminalRecord
}

func GatewayTerminalStorePath(workspace string) string {
	return filepath.Join(workspace, "state", "node_terminals.json")
}

func NewGatewayTerminalStore(path string, maxRecords, maxBytes int) (*GatewayTerminalStore, error) {
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return nil, errors.New("gateway terminal store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create gateway terminal store directory: %w", err)
	}
	store := newGatewayTerminalStore(path, maxRecords, maxBytes, time.Now)
	store.mu.Lock()
	defer store.mu.Unlock()
	release, err := store.lockAndReloadLocked()
	if err != nil {
		return nil, err
	}
	defer release()
	previous := cloneGatewayTerminalRecords(store.records)
	now := store.now()
	store.pruneLocked(now)
	store.recoverActiveLocked(now)
	if !sameGatewayTerminalRecords(previous, store.records) {
		if err := store.persistMutationLocked(previous); err != nil {
			return nil, fmt.Errorf("recover gateway terminal store: %w", err)
		}
	}
	return store, nil
}

func newGatewayTerminalStore(
	path string,
	maxRecords int,
	maxBytes int,
	now func() time.Time,
) *GatewayTerminalStore {
	if maxRecords <= 0 {
		maxRecords = DefaultGatewayTerminalLimit
	}
	if maxBytes <= 0 {
		maxBytes = DefaultGatewayTerminalStoreBytes
	}
	if now == nil {
		now = time.Now
	}
	return &GatewayTerminalStore{
		path:       path,
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
		retention:  DefaultGatewayTerminalRetention,
		now:        now,
		writeFile:  fileutil.WriteFileAtomic,
		records:    make(map[string]GatewayTerminalRecord),
	}
}

func (store *GatewayTerminalStore) Prepare(
	plan TerminalOpenPlan,
) (GatewayTerminalRecord, bool, error) {
	if err := plan.Validate(); err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	now := store.now()
	if now.Unix() < plan.PreparedAt || now.Unix() >= plan.ExpiresAt {
		return GatewayTerminalRecord{}, false, fmt.Errorf("%w: terminal plan is stale", ErrInvalidTerminal)
	}
	record := GatewayTerminalRecord{
		Plan:             plan,
		ExpectedPlanHash: plan.PlanHash,
		State:            GatewayTerminalPrepared,
		CreatedAt:        now.UnixNano(),
		UpdatedAt:        now.UnixNano(),
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	release, err := store.lockAndReloadLocked()
	if err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	defer release()
	previous := cloneGatewayTerminalRecords(store.records)
	now = store.now()
	store.pruneLocked(now)
	if now.Unix() < plan.PreparedAt || now.Unix() >= plan.ExpiresAt {
		store.records = previous
		return GatewayTerminalRecord{}, false, fmt.Errorf("%w: terminal plan is stale", ErrInvalidTerminal)
	}
	for _, existing := range store.records {
		if existing.Plan.IdempotencyKey == plan.IdempotencyKey {
			if sameGatewayTerminalBinding(existing, record) {
				if !sameGatewayTerminalRecords(previous, store.records) {
					if err := store.persistMutationLocked(previous); err != nil {
						return GatewayTerminalRecord{}, false, err
					}
				}
				return existing, false, nil
			}
			store.records = previous
			return GatewayTerminalRecord{}, false, ErrGatewayTerminalConflict
		}
	}
	if existing, found := store.records[plan.OpenID]; found {
		if sameGatewayTerminalBinding(existing, record) {
			if !sameGatewayTerminalRecords(previous, store.records) {
				if err := store.persistMutationLocked(previous); err != nil {
					return GatewayTerminalRecord{}, false, err
				}
			}
			return existing, false, nil
		}
		store.records = previous
		return GatewayTerminalRecord{}, false, ErrGatewayTerminalConflict
	}
	if len(store.records) >= store.maxRecords {
		store.records = previous
		return GatewayTerminalRecord{}, false, ErrGatewayTerminalStoreFull
	}
	record.CreatedAt = now.UnixNano()
	record.UpdatedAt = record.CreatedAt
	store.records[plan.OpenID] = record
	if err := store.persistMutationLocked(previous); err != nil {
		return record, fileutil.IsCommittedWriteError(err), fmt.Errorf("persist prepared terminal: %w", err)
	}
	return record, true, nil
}

func (store *GatewayTerminalStore) Lookup(
	owner TerminalOwner,
	terminalRef string,
) (GatewayTerminalRecord, bool, error) {
	if err := owner.Validate(); err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	release, err := store.lockAndReloadLocked()
	if err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	defer release()
	if pruneErr := store.pruneAndPersistLocked(store.now()); pruneErr != nil {
		return GatewayTerminalRecord{}, false, pruneErr
	}
	record, found := store.findLocked(strings.TrimSpace(terminalRef))
	if !found || record.Plan.Owner != owner {
		return GatewayTerminalRecord{}, false, nil
	}
	return record, true, nil
}

func (store *GatewayTerminalStore) MarkDispatched(
	owner TerminalOwner,
	openID string,
	expectedPlanHash string,
) (GatewayTerminalRecord, bool, error) {
	return store.transition(owner, openID, func(record *GatewayTerminalRecord, now int64) (bool, error) {
		if record.ExpectedPlanHash != expectedPlanHash ||
			record.Plan.ValidateAgainstHash(expectedPlanHash) != nil {
			return false, ErrGatewayTerminalConflict
		}
		switch record.State {
		case GatewayTerminalPrepared:
			record.State = GatewayTerminalDispatched
			record.DispatchedAt = now
			return true, nil
		case GatewayTerminalDispatched:
			return false, nil
		default:
			return false, ErrGatewayTerminalConflict
		}
	})
}

func (store *GatewayTerminalStore) RecordOpened(
	owner TerminalOwner,
	openID string,
	metadata TerminalMetadata,
) (GatewayTerminalRecord, bool, error) {
	return store.transition(owner, openID, func(record *GatewayTerminalRecord, _ int64) (bool, error) {
		if err := validateGatewayTerminalMetadata(metadata, owner); err != nil ||
			metadata.State != string(GatewayTerminalPendingAttach) {
			return false, ErrGatewayTerminalConflict
		}
		if record.State == GatewayTerminalPendingAttach {
			return false, metadataMatchesGatewayTerminal(*record, metadata)
		}
		if record.State != GatewayTerminalDispatched || record.TerminalID != "" {
			return false, ErrGatewayTerminalConflict
		}
		record.TerminalID = metadata.TerminalID
		record.State = GatewayTerminalPendingAttach
		applyGatewayTerminalMetadata(record, metadata)
		return true, nil
	})
}

func (store *GatewayTerminalStore) RecordLifecycle(
	owner TerminalOwner,
	terminalID string,
	metadata TerminalMetadata,
) (GatewayTerminalRecord, bool, error) {
	return store.transition(owner, terminalID, func(record *GatewayTerminalRecord, _ int64) (bool, error) {
		if err := validateGatewayTerminalMetadata(metadata, owner); err != nil ||
			metadata.TerminalID != record.TerminalID {
			return false, ErrGatewayTerminalConflict
		}
		next := GatewayTerminalState(metadata.State)
		switch next {
		case GatewayTerminalLive:
			if record.State == GatewayTerminalLive {
				return false, metadataMatchesGatewayTerminal(*record, metadata)
			}
			if record.State != GatewayTerminalPendingAttach {
				return false, ErrGatewayTerminalConflict
			}
		case GatewayTerminalClosed, GatewayTerminalUnknown:
			if record.State == GatewayTerminalClosed || record.State == GatewayTerminalUnknown {
				return false, metadataMatchesGatewayTerminal(*record, metadata)
			}
			if record.State != GatewayTerminalPendingAttach && record.State != GatewayTerminalLive {
				return false, ErrGatewayTerminalConflict
			}
		default:
			return false, ErrGatewayTerminalConflict
		}
		record.State = next
		applyGatewayTerminalMetadata(record, metadata)
		return true, nil
	})
}

func (store *GatewayTerminalStore) transition(
	owner TerminalOwner,
	terminalRef string,
	mutate func(*GatewayTerminalRecord, int64) (bool, error),
) (GatewayTerminalRecord, bool, error) {
	if err := owner.Validate(); err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	release, err := store.lockAndReloadLocked()
	if err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	defer release()
	if pruneErr := store.pruneAndPersistLocked(store.now()); pruneErr != nil {
		return GatewayTerminalRecord{}, false, pruneErr
	}
	record, found := store.findLocked(strings.TrimSpace(terminalRef))
	if !found {
		return GatewayTerminalRecord{}, false, ErrGatewayTerminalNotFound
	}
	if record.Plan.Owner != owner {
		return GatewayTerminalRecord{}, false, ErrGatewayTerminalConflict
	}
	previous := cloneGatewayTerminalRecords(store.records)
	now, err := nextGatewayTerminalTimestamp(record.UpdatedAt, store.now().UnixNano())
	if err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	changed, err := mutate(&record, now)
	if err != nil {
		return GatewayTerminalRecord{}, false, err
	}
	if !changed {
		return record, false, nil
	}
	record.UpdatedAt = now
	store.records[record.Plan.OpenID] = record
	if err := store.persistMutationLocked(previous); err != nil {
		return record, fileutil.IsCommittedWriteError(err), err
	}
	return record, true, nil
}

func (store *GatewayTerminalStore) findLocked(ref string) (GatewayTerminalRecord, bool) {
	if record, found := store.records[ref]; found {
		return record, true
	}
	for _, record := range store.records {
		if record.TerminalID != "" && record.TerminalID == ref {
			return record, true
		}
	}
	return GatewayTerminalRecord{}, false
}

func (store *GatewayTerminalStore) lockAndReloadLocked() (func(), error) {
	if store.path == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(filepath.Dir(store.path), 0o700); err != nil {
		return nil, fmt.Errorf("create gateway terminal store directory: %w", err)
	}
	release, err := acquireRegistryFileLock(store.path + ".lock")
	if err != nil {
		return nil, err
	}
	if err := store.loadLocked(); err != nil {
		release()
		return nil, fmt.Errorf("reload gateway terminal store under lock: %w", err)
	}
	return release, nil
}

func (store *GatewayTerminalStore) loadLocked() error {
	info, err := os.Stat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		store.records = make(map[string]GatewayTerminalRecord)
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat gateway terminal store: %w", err)
	}
	if info.Size() > int64(store.maxBytes) {
		return ErrGatewayTerminalStoreFull
	}
	file, err := os.Open(store.path)
	if err != nil {
		return fmt.Errorf("open gateway terminal store: %w", err)
	}
	defer file.Close()
	var document gatewayTerminalDocument
	decoder := json.NewDecoder(io.LimitReader(file, int64(store.maxBytes)+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode gateway terminal store: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("decode gateway terminal store: trailing data")
	}
	if document.Version != gatewayTerminalStoreVersion ||
		document.Records == nil ||
		len(document.Records) > store.maxRecords {
		return errors.New("gateway terminal store has invalid metadata")
	}
	terminalIDs := make(map[string]string, len(document.Records))
	idempotency := make(map[string]string, len(document.Records))
	for openID, record := range document.Records {
		if openID != record.Plan.OpenID {
			return errors.New("gateway terminal store has mismatched record identity")
		}
		if err := record.validate(); err != nil {
			return fmt.Errorf("validate gateway terminal %q: %w", openID, err)
		}
		if existing := idempotency[record.Plan.IdempotencyKey]; existing != "" {
			return fmt.Errorf("gateway terminals %q and %q share idempotency authority", existing, openID)
		}
		idempotency[record.Plan.IdempotencyKey] = openID
		if record.TerminalID != "" {
			if existing := terminalIDs[record.TerminalID]; existing != "" {
				return fmt.Errorf("gateway terminals %q and %q share terminal identity", existing, openID)
			}
			terminalIDs[record.TerminalID] = openID
		}
	}
	store.records = cloneGatewayTerminalRecords(document.Records)
	return nil
}

func (store *GatewayTerminalStore) saveLocked() error {
	data, err := json.Marshal(gatewayTerminalDocument{
		Version: gatewayTerminalStoreVersion,
		Records: store.records,
	})
	if err != nil {
		return err
	}
	if len(data)+1 > store.maxBytes {
		return ErrGatewayTerminalStoreFull
	}
	if store.path == "" {
		return nil
	}
	return store.writeFile(store.path, append(data, '\n'), 0o600)
}

func (store *GatewayTerminalStore) recoverActiveLocked(now time.Time) {
	for openID, record := range store.records {
		switch record.State {
		case GatewayTerminalDispatched, GatewayTerminalPendingAttach, GatewayTerminalLive:
			updatedAt, err := nextGatewayTerminalTimestamp(record.UpdatedAt, now.UnixNano())
			if err != nil {
				updatedAt = record.UpdatedAt
			}
			record.State = GatewayTerminalUnknown
			record.Reason = "gateway_restarted"
			record.CompletedAt = now.Unix()
			record.TerminationConfirmed = false
			record.UpdatedAt = updatedAt
			store.records[openID] = record
		}
	}
}

func (store *GatewayTerminalStore) pruneLocked(now time.Time) {
	retentionBefore := now.Add(-store.retention).UnixNano()
	for openID, record := range store.records {
		if record.State == GatewayTerminalPrepared && now.Unix() >= record.Plan.ExpiresAt {
			delete(store.records, openID)
			continue
		}
		if (record.State == GatewayTerminalClosed || record.State == GatewayTerminalUnknown) &&
			record.UpdatedAt < retentionBefore {
			delete(store.records, openID)
		}
	}
}

func (store *GatewayTerminalStore) pruneAndPersistLocked(now time.Time) error {
	previous := cloneGatewayTerminalRecords(store.records)
	store.pruneLocked(now)
	if sameGatewayTerminalRecords(previous, store.records) {
		return nil
	}
	return store.persistMutationLocked(previous)
}

func (store *GatewayTerminalStore) persistMutationLocked(previous map[string]GatewayTerminalRecord) error {
	err := store.saveLocked()
	if err != nil && !fileutil.IsCommittedWriteError(err) {
		store.records = previous
	}
	return err
}

func (record GatewayTerminalRecord) validate() error {
	if err := record.Plan.ValidateAgainstHash(record.ExpectedPlanHash); err != nil {
		return err
	}
	if record.CreatedAt <= 0 || record.UpdatedAt < record.CreatedAt {
		return fmt.Errorf("%w: invalid gateway terminal timestamps", ErrInvalidTerminal)
	}
	switch record.State {
	case GatewayTerminalPrepared:
		if record.DispatchedAt != 0 || record.TerminalID != "" ||
			!record.hasEmptyLifecycle() {
			return fmt.Errorf("%w: prepared terminal has runtime metadata", ErrInvalidTerminal)
		}
	case GatewayTerminalDispatched:
		if record.DispatchedAt <= 0 || record.TerminalID != "" ||
			!record.hasEmptyLifecycle() {
			return fmt.Errorf("%w: invalid dispatched terminal", ErrInvalidTerminal)
		}
	case GatewayTerminalPendingAttach, GatewayTerminalLive:
		if record.DispatchedAt <= 0 || record.TerminalID == "" || record.StartedAt <= 0 ||
			record.CompletedAt != 0 {
			return fmt.Errorf("%w: invalid active terminal", ErrInvalidTerminal)
		}
	case GatewayTerminalClosed, GatewayTerminalUnknown:
		if record.DispatchedAt <= 0 || record.CompletedAt <= 0 {
			return fmt.Errorf("%w: invalid terminal outcome", ErrInvalidTerminal)
		}
	default:
		return fmt.Errorf("%w: invalid gateway terminal state", ErrInvalidTerminal)
	}
	if record.TerminalID != "" {
		if err := (TerminalSessionRequest{TerminalID: record.TerminalID, Owner: record.Plan.Owner}).Validate(); err != nil {
			return err
		}
		metadata := TerminalMetadata{
			TerminalID:           record.TerminalID,
			Owner:                record.Plan.Owner,
			State:                string(record.State),
			Reason:               record.Reason,
			StartedAt:            record.StartedAt,
			CompletedAt:          record.CompletedAt,
			ExitCode:             record.ExitCode,
			Signal:               record.Signal,
			TerminationConfirmed: record.TerminationConfirmed,
		}
		if err := validateGatewayTerminalMetadata(metadata, record.Plan.Owner); err != nil {
			return fmt.Errorf("%w: invalid retained terminal metadata", ErrInvalidTerminal)
		}
	} else if record.State == GatewayTerminalUnknown &&
		(record.Reason != "gateway_restarted" ||
			record.StartedAt != 0 ||
			record.ExitCode != 0 ||
			record.Signal != "" ||
			record.TerminationConfirmed) {
		return fmt.Errorf("%w: invalid pre-response terminal outcome", ErrInvalidTerminal)
	}
	return nil
}

func (record GatewayTerminalRecord) hasEmptyLifecycle() bool {
	return record.Reason == "" &&
		record.StartedAt == 0 &&
		record.CompletedAt == 0 &&
		record.ExitCode == 0 &&
		record.Signal == "" &&
		!record.TerminationConfirmed
}

func validateGatewayTerminalMetadata(metadata TerminalMetadata, owner TerminalOwner) error {
	if err := (TerminalSessionRequest{TerminalID: metadata.TerminalID, Owner: metadata.Owner}).Validate(); err != nil {
		return err
	}
	if metadata.Owner != owner || metadata.StartedAt <= 0 {
		return ErrGatewayTerminalConflict
	}
	switch GatewayTerminalState(metadata.State) {
	case GatewayTerminalPendingAttach, GatewayTerminalLive:
		if metadata.Reason != "" ||
			metadata.CompletedAt != 0 ||
			metadata.ExitCode != 0 ||
			metadata.Signal != "" ||
			metadata.TerminationConfirmed {
			return ErrGatewayTerminalConflict
		}
	case GatewayTerminalClosed:
		if !validInvocationIdentifier(metadata.Reason) ||
			metadata.CompletedAt < metadata.StartedAt ||
			!metadata.TerminationConfirmed ||
			!validTerminalSignalMetadata(metadata.Signal) {
			return ErrGatewayTerminalConflict
		}
	case GatewayTerminalUnknown:
		if !validInvocationIdentifier(metadata.Reason) ||
			metadata.CompletedAt < metadata.StartedAt ||
			metadata.ExitCode != 0 ||
			metadata.Signal != "" ||
			metadata.TerminationConfirmed {
			return ErrGatewayTerminalConflict
		}
	default:
		return ErrGatewayTerminalConflict
	}
	return nil
}

func applyGatewayTerminalMetadata(record *GatewayTerminalRecord, metadata TerminalMetadata) {
	record.Reason = metadata.Reason
	record.StartedAt = metadata.StartedAt
	record.CompletedAt = metadata.CompletedAt
	record.ExitCode = metadata.ExitCode
	record.Signal = metadata.Signal
	record.TerminationConfirmed = metadata.TerminationConfirmed
}

func metadataMatchesGatewayTerminal(record GatewayTerminalRecord, metadata TerminalMetadata) error {
	if record.TerminalID != metadata.TerminalID ||
		record.State != GatewayTerminalState(metadata.State) ||
		record.Reason != metadata.Reason ||
		record.StartedAt != metadata.StartedAt ||
		record.CompletedAt != metadata.CompletedAt ||
		record.ExitCode != metadata.ExitCode ||
		record.Signal != metadata.Signal ||
		record.TerminationConfirmed != metadata.TerminationConfirmed {
		return ErrGatewayTerminalConflict
	}
	return nil
}

func sameGatewayTerminalBinding(left, right GatewayTerminalRecord) bool {
	return left.ExpectedPlanHash == right.ExpectedPlanHash &&
		left.Plan.OpenID == right.Plan.OpenID &&
		left.Plan.IdempotencyKey == right.Plan.IdempotencyKey &&
		left.Plan.Owner == right.Plan.Owner
}

func nextGatewayTerminalTimestamp(previous, now int64) (int64, error) {
	if now > previous {
		return now, nil
	}
	if previous == math.MaxInt64 {
		return 0, fmt.Errorf("%w: terminal timestamp exhausted", ErrInvalidTerminal)
	}
	return previous + 1, nil
}

func cloneGatewayTerminalRecords(
	records map[string]GatewayTerminalRecord,
) map[string]GatewayTerminalRecord {
	cloned := make(map[string]GatewayTerminalRecord, len(records))
	for openID, record := range records {
		cloned[openID] = record
	}
	return cloned
}

func sameGatewayTerminalRecords(
	left map[string]GatewayTerminalRecord,
	right map[string]GatewayTerminalRecord,
) bool {
	if len(left) != len(right) {
		return false
	}
	for openID, record := range left {
		if right[openID] != record {
			return false
		}
	}
	return true
}
