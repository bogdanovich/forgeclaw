package companion

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
	"github.com/sipeed/picoclaw/pkg/nodes"
)

const (
	invocationLedgerVersion      = 1
	DefaultInvocationLedgerLimit = 256
	DefaultInvocationLedgerBytes = 32 * 1024 * 1024
)

var (
	ErrInvocationConflict    = errors.New("node invocation idempotency conflict")
	ErrInvocationNotFound    = errors.New("node invocation not found")
	ErrInvocationLedgerFull  = errors.New("node invocation ledger is full")
	ErrInvocationLedgerOwned = errors.New("node invocation ledger is owned by another process")
)

type invocationLedgerDocument struct {
	Version int                               `json:"version"`
	Records map[string]nodes.InvocationRecord `json:"records"`
}

// InvocationLedger owns the bounded, instance-local proof that an invocation
// was accepted before execution. A nil path is used only by unit tests.
type InvocationLedger struct {
	path        string
	maxRecords  int
	maxBytes    int
	now         func() time.Time
	writeFile   func(string, []byte, os.FileMode) error
	releaseLock func()

	mu          sync.Mutex
	records     map[string]nodes.InvocationRecord
	idempotency map[string]string
}

func InvocationLedgerPath(stateDir string) string {
	return filepath.Join(stateDir, "invocations.json")
}

func NewFileInvocationLedger(
	path string,
	maxRecords int,
	maxBytes int,
) (*InvocationLedger, error) {
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return nil, errors.New("node invocation ledger path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create node invocation ledger directory: %w", err)
	}
	release, err := acquireInvocationLedgerLock(path + ".lock")
	if err != nil {
		return nil, err
	}
	ledger := newInvocationLedger(path, maxRecords, maxBytes, time.Now)
	ledger.releaseLock = release
	if err := ledger.load(); err != nil {
		ledger.Close()
		return nil, err
	}
	if err := ledger.recoverUnfinished(); err != nil {
		ledger.Close()
		return nil, err
	}
	return ledger, nil
}

// Close releases this process's exclusive ownership of the ledger. The lock
// file remains in place so a successor cannot race a newly created inode.
func (ledger *InvocationLedger) Close() {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	release := ledger.releaseLock
	ledger.releaseLock = nil
	ledger.mu.Unlock()
	if release != nil {
		release()
	}
}

func newMemoryInvocationLedger() *InvocationLedger {
	return newInvocationLedger(
		"",
		DefaultInvocationLedgerLimit,
		DefaultInvocationLedgerBytes,
		time.Now,
	)
}

func newInvocationLedger(
	path string,
	maxRecords int,
	maxBytes int,
	now func() time.Time,
) *InvocationLedger {
	if maxRecords <= 0 {
		maxRecords = DefaultInvocationLedgerLimit
	}
	if maxBytes <= 0 {
		maxBytes = DefaultInvocationLedgerBytes
	}
	if now == nil {
		now = time.Now
	}
	return &InvocationLedger{
		path:        path,
		maxRecords:  maxRecords,
		maxBytes:    maxBytes,
		now:         now,
		writeFile:   fileutil.WriteFileAtomic,
		records:     make(map[string]nodes.InvocationRecord),
		idempotency: make(map[string]string),
	}
}

func (ledger *InvocationLedger) Accept(
	plan nodes.ExecutionPlan,
) (nodes.InvocationRecord, bool, error) {
	if err := plan.Validate(); err != nil {
		return nodes.InvocationRecord{}, false, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := ledger.sweepExpiredAcceptedLocked(); err != nil {
		return nodes.InvocationRecord{}, false, err
	}
	if existing, found, err := ledger.existingLocked(plan); found || err != nil {
		return existing, found, err
	}
	if ledger.now().Unix() >= plan.ExpiresAt {
		return nodes.InvocationRecord{}, false, fmt.Errorf(
			"%w: execution plan expired",
			nodes.ErrInvalidInvocation,
		)
	}
	previous := cloneInvocationRecords(ledger.records)
	for len(ledger.records) >= ledger.maxRecords {
		if !ledger.pruneOldestExpiredLocked("") {
			return nodes.InvocationRecord{}, false, ErrInvocationLedgerFull
		}
	}
	now := ledger.now().UnixNano()
	record := nodes.InvocationRecord{
		InvocationID:   plan.InvocationID,
		IdempotencyKey: plan.IdempotencyKey,
		PlanHash:       plan.PlanHash,
		NodeID:         plan.NodeID,
		CatalogHash:    plan.CatalogHash,
		Command:        plan.Command,
		Risk:           plan.Risk,
		State:          nodes.InvocationAccepted,
		AcceptedAt:     now,
		UpdatedAt:      now,
		ExpiresAt:      plan.ExpiresAt,
	}
	if err := record.Validate(); err != nil {
		ledger.records = previous
		ledger.rebuildIdempotencyLocked()
		return nodes.InvocationRecord{}, false, err
	}
	ledger.records[record.InvocationID] = record
	ledger.idempotency[record.IdempotencyKey] = record.InvocationID
	if err := ledger.persistLocked(record.InvocationID); err != nil {
		ledger.rollbackIfUncommittedLocked(previous, err)
		return nodes.InvocationRecord{}, false, fmt.Errorf("persist accepted invocation: %w", err)
	}
	return cloneInvocationRecord(record), false, nil
}

func (ledger *InvocationLedger) Existing(
	plan nodes.ExecutionPlan,
) (nodes.InvocationRecord, bool, error) {
	if err := plan.Validate(); err != nil {
		return nodes.InvocationRecord{}, false, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := ledger.sweepExpiredAcceptedLocked(); err != nil {
		return nodes.InvocationRecord{}, false, err
	}
	return ledger.existingLocked(plan)
}

func (ledger *InvocationLedger) existingLocked(
	plan nodes.ExecutionPlan,
) (nodes.InvocationRecord, bool, error) {
	if existing, found := ledger.records[plan.InvocationID]; found {
		if existing.IdempotencyKey != plan.IdempotencyKey || existing.PlanHash != plan.PlanHash {
			return nodes.InvocationRecord{}, false, ErrInvocationConflict
		}
		return cloneInvocationRecord(existing), true, nil
	}
	if invocationID, found := ledger.idempotency[plan.IdempotencyKey]; found {
		existing := ledger.records[invocationID]
		if existing.InvocationID != plan.InvocationID || existing.PlanHash != plan.PlanHash {
			return nodes.InvocationRecord{}, false, ErrInvocationConflict
		}
		return cloneInvocationRecord(existing), true, nil
	}
	return nodes.InvocationRecord{}, false, nil
}

func (ledger *InvocationLedger) MarkRunning(invocationID string) (nodes.InvocationRecord, error) {
	return ledger.transition(invocationID, func(record *nodes.InvocationRecord, now int64) error {
		if record.State != nodes.InvocationAccepted {
			return fmt.Errorf("%w: invocation is %s", nodes.ErrInvalidInvocationRecord, record.State)
		}
		record.State = nodes.InvocationRunning
		record.UpdatedAt = now
		return nil
	})
}

func (ledger *InvocationLedger) CompleteSuccess(
	invocationID string,
	result json.RawMessage,
) (nodes.InvocationRecord, error) {
	return ledger.transition(invocationID, func(record *nodes.InvocationRecord, now int64) error {
		if record.State != nodes.InvocationRunning {
			return fmt.Errorf("%w: invocation is %s", nodes.ErrInvalidInvocationRecord, record.State)
		}
		record.State = nodes.InvocationSucceeded
		record.UpdatedAt = now
		record.CompletedAt = now
		record.Result = append(json.RawMessage(nil), result...)
		record.Cancellation = nil
		return nil
	})
}

func (ledger *InvocationLedger) CompleteFailure(
	invocationID string,
	failure nodes.InvocationFailure,
) (nodes.InvocationRecord, error) {
	return ledger.transition(invocationID, func(record *nodes.InvocationRecord, now int64) error {
		if record.State != nodes.InvocationRunning {
			return fmt.Errorf("%w: invocation is %s", nodes.ErrInvalidInvocationRecord, record.State)
		}
		record.State = nodes.InvocationFailed
		record.UpdatedAt = now
		record.CompletedAt = now
		record.Failure = &nodes.InvocationFailure{Code: failure.Code, Message: failure.Message}
		record.Cancellation = nil
		return nil
	})
}

func (ledger *InvocationLedger) RequestCancellation(
	invocationID string,
) (nodes.InvocationRecord, error) {
	return ledger.transitionIf(
		invocationID,
		func(record *nodes.InvocationRecord, now int64) (bool, error) {
			switch record.State {
			case nodes.InvocationAccepted:
				record.State = nodes.InvocationCanceled
				record.UpdatedAt = now
				record.CompletedAt = now
				record.Failure = &nodes.InvocationFailure{
					Code:    "CANCELED",
					Message: "node command canceled before execution",
				}
				record.Cancellation = &nodes.InvocationCancellation{
					RequestedAt:          now,
					TerminationConfirmed: true,
				}
				return true, nil
			case nodes.InvocationRunning:
				if record.Cancellation != nil {
					return false, nil
				}
				record.UpdatedAt = now
				record.Cancellation = &nodes.InvocationCancellation{RequestedAt: now}
				return true, nil
			case nodes.InvocationSucceeded, nodes.InvocationFailed, nodes.InvocationCanceled:
				return false, nil
			default:
				return false, fmt.Errorf(
					"%w: invocation is %s",
					nodes.ErrInvalidInvocationRecord,
					record.State,
				)
			}
		},
	)
}

func (ledger *InvocationLedger) CompleteCancellation(
	invocationID string,
) (nodes.InvocationRecord, error) {
	return ledger.transition(invocationID, func(record *nodes.InvocationRecord, now int64) error {
		if record.State != nodes.InvocationRunning || record.Cancellation == nil {
			return fmt.Errorf("%w: invocation is %s", nodes.ErrInvalidInvocationRecord, record.State)
		}
		record.State = nodes.InvocationCanceled
		record.UpdatedAt = now
		record.CompletedAt = now
		record.Failure = &nodes.InvocationFailure{
			Code:    "CANCELED",
			Message: "node command canceled",
		}
		record.Cancellation.TerminationConfirmed = true
		return nil
	})
}

func (ledger *InvocationLedger) Get(invocationID string) (nodes.InvocationRecord, bool) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	record, found := ledger.records[invocationID]
	return cloneInvocationRecord(record), found
}

// Lookup returns the durable externally visible state after applying lazy
// time-based transitions. Get remains an internal raw-state inspection helper.
func (ledger *InvocationLedger) Lookup(
	invocationID string,
) (nodes.InvocationRecord, bool, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := ledger.sweepExpiredAcceptedLocked(); err != nil {
		return nodes.InvocationRecord{}, false, err
	}
	record, found := ledger.records[invocationID]
	return cloneInvocationRecord(record), found, nil
}

func (ledger *InvocationLedger) transition(
	invocationID string,
	update func(*nodes.InvocationRecord, int64) error,
) (nodes.InvocationRecord, error) {
	return ledger.transitionIf(
		invocationID,
		func(record *nodes.InvocationRecord, now int64) (bool, error) {
			if err := update(record, now); err != nil {
				return false, err
			}
			return true, nil
		},
	)
}

func (ledger *InvocationLedger) transitionIf(
	invocationID string,
	update func(*nodes.InvocationRecord, int64) (bool, error),
) (nodes.InvocationRecord, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	record, found := ledger.records[invocationID]
	if !found {
		return nodes.InvocationRecord{}, ErrInvocationNotFound
	}
	previous := cloneInvocationRecords(ledger.records)
	changed, err := update(&record, ledger.now().UnixNano())
	if err != nil {
		return nodes.InvocationRecord{}, err
	}
	if !changed {
		return cloneInvocationRecord(record), nil
	}
	if err := record.Validate(); err != nil {
		return nodes.InvocationRecord{}, err
	}
	ledger.records[invocationID] = record
	if err := ledger.persistLocked(invocationID); err != nil {
		ledger.rollbackIfUncommittedLocked(previous, err)
		return nodes.InvocationRecord{}, fmt.Errorf("persist invocation transition: %w", err)
	}
	return cloneInvocationRecord(record), nil
}

func (ledger *InvocationLedger) recoverUnfinished() error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	previous := cloneInvocationRecords(ledger.records)
	nowTime := ledger.now()
	now := nowTime.UnixNano()
	changed := ledger.expireAcceptedLocked(nowTime)
	for id, record := range ledger.records {
		switch {
		case record.State == nodes.InvocationRunning:
			record.State = nodes.InvocationUnknown
			record.UpdatedAt = now
		default:
			continue
		}
		ledger.records[id] = record
		changed = true
	}
	if !changed {
		return nil
	}
	if err := ledger.persistLocked(""); err != nil {
		ledger.rollbackIfUncommittedLocked(previous, err)
		return fmt.Errorf("persist recovered invocation ledger: %w", err)
	}
	return nil
}

func (ledger *InvocationLedger) sweepExpiredAcceptedLocked() error {
	previous := cloneInvocationRecords(ledger.records)
	if !ledger.expireAcceptedLocked(ledger.now()) {
		return nil
	}
	if err := ledger.persistLocked(""); err != nil {
		ledger.rollbackIfUncommittedLocked(previous, err)
		return fmt.Errorf("persist expired accepted invocations: %w", err)
	}
	return nil
}

func (ledger *InvocationLedger) expireAcceptedLocked(nowTime time.Time) bool {
	changed := false
	now := nowTime.UnixNano()
	for id, record := range ledger.records {
		if record.State != nodes.InvocationAccepted || nowTime.Unix() < record.ExpiresAt {
			continue
		}
		record.State = nodes.InvocationCanceled
		record.UpdatedAt = now
		record.CompletedAt = now
		record.Failure = &nodes.InvocationFailure{
			Code:    "PLAN_EXPIRED",
			Message: "accepted invocation expired before execution",
		}
		ledger.records[id] = record
		changed = true
	}
	return changed
}

func (ledger *InvocationLedger) load() error {
	if ledger.path == "" {
		return nil
	}
	file, openErr := os.Open(ledger.path)
	if errors.Is(openErr, os.ErrNotExist) {
		return nil
	}
	if openErr != nil {
		return fmt.Errorf("open node invocation ledger: %w", openErr)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, int64(ledger.maxBytes)+1))
	decoder.DisallowUnknownFields()
	var document invocationLedgerDocument
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode node invocation ledger: %w", err)
	}
	if err := ensureConfigEOF(decoder); err != nil {
		return fmt.Errorf("decode node invocation ledger: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat node invocation ledger: %w", err)
	}
	if info.Size() > int64(ledger.maxBytes) {
		return ErrInvocationLedgerFull
	}
	if document.Version != invocationLedgerVersion || document.Records == nil ||
		len(document.Records) > ledger.maxRecords {
		return errors.New("invalid node invocation ledger document")
	}
	idempotency := make(map[string]string, len(document.Records))
	for id, record := range document.Records {
		if id != record.InvocationID {
			return errors.New("node invocation ledger key does not match record")
		}
		if err := record.Validate(); err != nil {
			return fmt.Errorf("validate node invocation ledger record: %w", err)
		}
		if _, duplicate := idempotency[record.IdempotencyKey]; duplicate {
			return errors.New("node invocation ledger contains duplicate idempotency key")
		}
		idempotency[record.IdempotencyKey] = id
	}
	ledger.records = cloneInvocationRecords(document.Records)
	ledger.idempotency = idempotency
	return nil
}

func (ledger *InvocationLedger) persistLocked(protectedID string) error {
	if ledger.path == "" {
		return nil
	}
	for {
		data, err := json.Marshal(invocationLedgerDocument{
			Version: invocationLedgerVersion,
			Records: ledger.records,
		})
		if err != nil {
			return fmt.Errorf("encode node invocation ledger: %w", err)
		}
		if len(data) <= ledger.maxBytes {
			if err := ledger.writeFile(ledger.path, append(data, '\n'), 0o600); err != nil {
				return fmt.Errorf("save node invocation ledger: %w", err)
			}
			return nil
		}
		if !ledger.pruneOldestExpiredLocked(protectedID) {
			return ErrInvocationLedgerFull
		}
	}
}

// pruneOldestExpiredLocked never removes an identity while its original plan
// could still pass authorization. Capacity pressure therefore fails closed
// instead of turning a previously accepted plan into executable work again.
func (ledger *InvocationLedger) pruneOldestExpiredLocked(protectedID string) bool {
	oldestID := ""
	var oldestAt int64
	now := ledger.now().Unix()
	for id, record := range ledger.records {
		if id == protectedID || record.ExpiresAt > now ||
			(!record.State.Terminal() && record.State != nodes.InvocationUnknown) {
			continue
		}
		if oldestID == "" || record.UpdatedAt < oldestAt ||
			(record.UpdatedAt == oldestAt && id < oldestID) {
			oldestID = id
			oldestAt = record.UpdatedAt
		}
	}
	if oldestID == "" {
		return false
	}
	record := ledger.records[oldestID]
	delete(ledger.records, oldestID)
	delete(ledger.idempotency, record.IdempotencyKey)
	return true
}

func (ledger *InvocationLedger) rollbackIfUncommittedLocked(
	previous map[string]nodes.InvocationRecord,
	err error,
) {
	if fileutil.IsCommittedWriteError(err) {
		return
	}
	ledger.records = previous
	ledger.rebuildIdempotencyLocked()
}

func (ledger *InvocationLedger) rebuildIdempotencyLocked() {
	ledger.idempotency = make(map[string]string, len(ledger.records))
	for id, record := range ledger.records {
		ledger.idempotency[record.IdempotencyKey] = id
	}
}

func cloneInvocationRecords(
	records map[string]nodes.InvocationRecord,
) map[string]nodes.InvocationRecord {
	cloned := make(map[string]nodes.InvocationRecord, len(records))
	for id, record := range records {
		cloned[id] = cloneInvocationRecord(record)
	}
	return cloned
}

func cloneInvocationRecord(record nodes.InvocationRecord) nodes.InvocationRecord {
	record.Result = append(json.RawMessage(nil), record.Result...)
	if record.Failure != nil {
		failure := *record.Failure
		record.Failure = &failure
	}
	if record.Cancellation != nil {
		cancellation := *record.Cancellation
		record.Cancellation = &cancellation
	}
	return record
}

func decodeLedgerDocument(data []byte) (invocationLedgerDocument, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document invocationLedgerDocument
	if err := decoder.Decode(&document); err != nil {
		return invocationLedgerDocument{}, err
	}
	if err := ensureConfigEOF(decoder); err != nil {
		return invocationLedgerDocument{}, err
	}
	return document, nil
}
