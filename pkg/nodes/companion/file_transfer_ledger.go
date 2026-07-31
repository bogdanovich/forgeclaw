package companion

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

const (
	fileTransferLedgerVersion      = 1
	DefaultFileTransferLedgerLimit = 256
	DefaultFileTransferLedgerBytes = 16 * 1024 * 1024
	DefaultFileTransferRetention   = 7 * 24 * time.Hour
	MaxFileTransferRetention       = 30 * 24 * time.Hour
)

var (
	ErrFileTransferConflict   = errors.New("node file transfer conflicts with durable state")
	ErrFileTransferNotFound   = errors.New("node file transfer not found")
	ErrFileTransferLedgerFull = errors.New("node file transfer ledger is full")
)

type FileTransferState string

const (
	FileTransferAccepted        FileTransferState = "accepted"
	FileTransferStreaming       FileTransferState = "streaming"
	FileTransferStaged          FileTransferState = "staged"
	FileTransferCommitRequested FileTransferState = "commit_requested"
	FileTransferPublished       FileTransferState = "published"
	FileTransferReceived        FileTransferState = "received"
	FileTransferCommitted       FileTransferState = "committed"
	FileTransferFailed          FileTransferState = "failed"
	FileTransferCanceled        FileTransferState = "canceled"
	FileTransferExpired         FileTransferState = "expired"
	FileTransferUnknown         FileTransferState = "unknown"
)

func (state FileTransferState) terminal() bool {
	switch state {
	case FileTransferPublished,
		FileTransferCommitted,
		FileTransferFailed,
		FileTransferCanceled,
		FileTransferExpired,
		FileTransferUnknown:
		return true
	default:
		return false
	}
}

type FileTransferRecord struct {
	TransferID     string                     `json:"transfer_id"`
	Direction      protocol.TransferDirection `json:"direction"`
	Operation      string                     `json:"operation"`
	ProfileAlias   string                     `json:"profile_alias"`
	PolicyRevision string                     `json:"policy_revision"`
	Path           string                     `json:"path"`
	Publication    string                     `json:"publication,omitempty"`
	TotalSize      uint64                     `json:"total_size"`
	SHA256         string                     `json:"sha256"`
	ExpiresAt      int64                      `json:"expires_at"`
	State          FileTransferState          `json:"state"`
	Sequence       uint64                     `json:"sequence,omitempty"`
	ObservedBytes  uint64                     `json:"observed_bytes,omitempty"`
	StageName      string                     `json:"stage_name,omitempty"`
	StageIdentity  fileIdentity               `json:"stage_identity,omitempty"`
	SourceIdentity fileIdentity               `json:"source_identity,omitempty"`
	SourceModified int64                      `json:"source_modified,omitempty"`
	Result         json.RawMessage            `json:"result,omitempty"`
	FailureCode    string                     `json:"failure_code,omitempty"`
	CreatedAt      int64                      `json:"created_at"`
	UpdatedAt      int64                      `json:"updated_at"`
	CompletedAt    int64                      `json:"completed_at,omitempty"`
}

type fileTransferLedgerDocument struct {
	Version int                           `json:"version"`
	Records map[string]FileTransferRecord `json:"records"`
}

type FileTransferLedger struct {
	path        string
	maxRecords  int
	maxBytes    int
	now         func() time.Time
	retention   time.Duration
	writeFile   func(string, []byte, os.FileMode) error
	releaseLock func()

	mu      sync.Mutex
	records map[string]FileTransferRecord
}

func FileTransferLedgerPath(stateDir string) string {
	return filepath.Join(stateDir, "file-transfers.json")
}

func NewFileTransferLedger(
	path string,
	maxRecords int,
	maxBytes int,
) (*FileTransferLedger, error) {
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return nil, errors.New("node file transfer ledger path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create node file transfer ledger directory: %w", err)
	}
	release, err := acquireInvocationLedgerLock(path + ".lock")
	if err != nil {
		return nil, err
	}
	ledger := newFileTransferLedger(path, maxRecords, maxBytes, time.Now)
	ledger.releaseLock = release
	if err := ledger.load(); err != nil {
		ledger.Close()
		return nil, err
	}
	return ledger, nil
}

func newMemoryFileTransferLedger() *FileTransferLedger {
	return newFileTransferLedger(
		"",
		DefaultFileTransferLedgerLimit,
		DefaultFileTransferLedgerBytes,
		time.Now,
	)
}

func newFileTransferLedger(
	path string,
	maxRecords int,
	maxBytes int,
	now func() time.Time,
) *FileTransferLedger {
	if maxRecords <= 0 {
		maxRecords = DefaultFileTransferLedgerLimit
	}
	if maxBytes <= 0 {
		maxBytes = DefaultFileTransferLedgerBytes
	}
	if now == nil {
		now = time.Now
	}
	return &FileTransferLedger{
		path:       path,
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
		now:        now,
		retention:  DefaultFileTransferRetention,
		writeFile:  fileutil.WriteFileAtomic,
		records:    make(map[string]FileTransferRecord),
	}
}

func (ledger *FileTransferLedger) Close() {
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

func (ledger *FileTransferLedger) Accept(
	record FileTransferRecord,
) (FileTransferRecord, bool, error) {
	now := ledger.now()
	if record.CreatedAt == 0 {
		record.CreatedAt = now.UnixNano()
	}
	record.UpdatedAt = record.CreatedAt
	if record.State == "" {
		record.State = FileTransferAccepted
	}
	if err := validateFileTransferRecord(record); err != nil {
		return FileTransferRecord{}, false, err
	}
	if record.State != FileTransferAccepted &&
		!(record.Operation == fileOperationInfo &&
			record.State == FileTransferCommitted) {
		return FileTransferRecord{}, false, ErrFileTransferConflict
	}
	if now.Unix() >= record.ExpiresAt {
		return FileTransferRecord{}, false, ErrFileTransferConflict
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if existing, found := ledger.records[record.TransferID]; found {
		if !sameFileTransferBinding(existing, record) {
			return FileTransferRecord{}, false, ErrFileTransferConflict
		}
		return cloneFileTransferRecord(existing), true, nil
	}
	previous := cloneFileTransferRecords(ledger.records)
	ledger.pruneRetainedLocked(now, record.TransferID)
	for len(ledger.records) >= ledger.maxRecords &&
		ledger.pruneOldestCapacityLocked(record.TransferID, now) {
	}
	if len(ledger.records) >= ledger.maxRecords {
		ledger.records = previous
		return FileTransferRecord{}, false, ErrFileTransferLedgerFull
	}
	ledger.records[record.TransferID] = cloneFileTransferRecord(record)
	if err := ledger.persistLocked(record.TransferID); err != nil {
		ledger.rollbackIfUncommittedLocked(previous, err)
		return FileTransferRecord{}, false, err
	}
	return cloneFileTransferRecord(record), false, nil
}

func (ledger *FileTransferLedger) Lookup(
	transferID string,
) (FileTransferRecord, bool, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	record, found := ledger.records[transferID]
	return cloneFileTransferRecord(record), found, nil
}

func (ledger *FileTransferLedger) Records() []FileTransferRecord {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	records := make([]FileTransferRecord, 0, len(ledger.records))
	for _, record := range ledger.records {
		records = append(records, cloneFileTransferRecord(record))
	}
	sort.Slice(records, func(left, right int) bool {
		return records[left].TransferID < records[right].TransferID
	})
	return records
}

func (ledger *FileTransferLedger) Transition(
	transferID string,
	update func(*FileTransferRecord, time.Time) error,
) (FileTransferRecord, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	record, found := ledger.records[transferID]
	if !found {
		return FileTransferRecord{}, ErrFileTransferNotFound
	}
	record = cloneFileTransferRecord(record)
	previousRecord := cloneFileTransferRecord(record)
	previous := cloneFileTransferRecords(ledger.records)
	now := ledger.now()
	if err := update(&record, now); err != nil {
		return FileTransferRecord{}, err
	}
	record.UpdatedAt = now.UnixNano()
	if record.State.terminal() && record.CompletedAt == 0 {
		record.CompletedAt = record.UpdatedAt
	}
	if err := validateFileTransferTransition(previousRecord, record); err != nil {
		return FileTransferRecord{}, err
	}
	if err := validateFileTransferRecord(record); err != nil {
		return FileTransferRecord{}, err
	}
	ledger.records[transferID] = record
	if err := ledger.persistLocked(transferID); err != nil {
		ledger.rollbackIfUncommittedLocked(previous, err)
		return FileTransferRecord{}, err
	}
	return cloneFileTransferRecord(record), nil
}

func validateFileTransferTransition(
	previous FileTransferRecord,
	next FileTransferRecord,
) error {
	if !sameFileTransferBinding(previous, next) ||
		previous.CreatedAt != next.CreatedAt ||
		previous.StageName != next.StageName ||
		previous.StageIdentity != next.StageIdentity ||
		previous.SourceIdentity != next.SourceIdentity ||
		previous.SourceModified != next.SourceModified ||
		next.Sequence < previous.Sequence ||
		next.ObservedBytes < previous.ObservedBytes {
		return ErrFileTransferConflict
	}
	if previous.State == next.State {
		switch next.State {
		case FileTransferAccepted, FileTransferStreaming:
			return nil
		default:
			return ErrFileTransferConflict
		}
	}
	if previous.State.terminal() {
		return ErrFileTransferConflict
	}
	switch previous.Direction {
	case protocol.TransferUpload:
		return validateUploadTransferTransition(previous.State, next.State)
	case protocol.TransferDownload:
		return validateDownloadTransferTransition(previous.State, next.State)
	default:
		return ErrFileTransferConflict
	}
}

func validateUploadTransferTransition(
	previous FileTransferState,
	next FileTransferState,
) error {
	switch previous {
	case FileTransferAccepted:
		if next == FileTransferStreaming ||
			next == FileTransferStaged ||
			fileTransferPreCommitTerminal(next) {
			return nil
		}
	case FileTransferStreaming:
		if next == FileTransferStaged ||
			fileTransferPreCommitTerminal(next) {
			return nil
		}
	case FileTransferStaged:
		if next == FileTransferCommitRequested ||
			fileTransferPreCommitTerminal(next) {
			return nil
		}
	case FileTransferCommitRequested:
		if next == FileTransferPublished ||
			next == FileTransferFailed ||
			next == FileTransferUnknown {
			return nil
		}
	}
	return ErrFileTransferConflict
}

func validateDownloadTransferTransition(
	previous FileTransferState,
	next FileTransferState,
) error {
	switch previous {
	case FileTransferAccepted:
		if next == FileTransferStreaming ||
			next == FileTransferReceived ||
			fileTransferPreCommitTerminal(next) {
			return nil
		}
	case FileTransferStreaming:
		if next == FileTransferReceived ||
			fileTransferPreCommitTerminal(next) {
			return nil
		}
	case FileTransferReceived:
		if next == FileTransferCommitRequested ||
			fileTransferPreCommitTerminal(next) {
			return nil
		}
	case FileTransferCommitRequested:
		if next == FileTransferCommitted ||
			next == FileTransferUnknown {
			return nil
		}
	}
	return ErrFileTransferConflict
}

func fileTransferPreCommitTerminal(state FileTransferState) bool {
	switch state {
	case FileTransferFailed,
		FileTransferCanceled,
		FileTransferExpired,
		FileTransferUnknown:
		return true
	default:
		return false
	}
}

func (ledger *FileTransferLedger) load() error {
	if ledger.path == "" {
		return nil
	}
	file, err := os.Open(ledger.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open node file transfer ledger: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, int64(ledger.maxBytes)+1))
	decoder.DisallowUnknownFields()
	var document fileTransferLedgerDocument
	if decodeErr := decoder.Decode(&document); decodeErr != nil {
		return fmt.Errorf("decode node file transfer ledger: %w", decodeErr)
	}
	if eofErr := ensureConfigEOF(decoder); eofErr != nil {
		return fmt.Errorf("decode node file transfer ledger: %w", eofErr)
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat node file transfer ledger: %w", err)
	}
	if info.Size() > int64(ledger.maxBytes) ||
		document.Version != fileTransferLedgerVersion ||
		document.Records == nil ||
		len(document.Records) > ledger.maxRecords {
		return errors.New("invalid node file transfer ledger")
	}
	for id, record := range document.Records {
		if id != record.TransferID {
			return errors.New("node file transfer ledger key mismatch")
		}
		if err := validateFileTransferRecord(record); err != nil {
			return fmt.Errorf("validate node file transfer ledger: %w", err)
		}
	}
	ledger.records = cloneFileTransferRecords(document.Records)
	previousCount := len(ledger.records)
	ledger.pruneRetainedLocked(ledger.now(), "")
	if len(ledger.records) != previousCount {
		if err := ledger.persistLocked(""); err != nil {
			return fmt.Errorf("persist pruned node file transfer ledger: %w", err)
		}
	}
	return nil
}

func (ledger *FileTransferLedger) persistLocked(protectedID string) error {
	if ledger.path == "" {
		return nil
	}
	for {
		data, err := json.Marshal(fileTransferLedgerDocument{
			Version: fileTransferLedgerVersion,
			Records: ledger.records,
		})
		if err != nil {
			return fmt.Errorf("encode node file transfer ledger: %w", err)
		}
		if len(data) <= ledger.maxBytes {
			if err := ledger.writeFile(ledger.path, append(data, '\n'), 0o600); err != nil {
				return fmt.Errorf("save node file transfer ledger: %w", err)
			}
			return nil
		}
		if !ledger.pruneOldestCapacityLocked(
			protectedID,
			ledger.now(),
		) {
			return ErrFileTransferLedgerFull
		}
	}
}

func (ledger *FileTransferLedger) pruneRetainedLocked(
	now time.Time,
	protectedID string,
) {
	for ledger.pruneOldestRetainedLockedAt(protectedID, now) {
	}
}

func (ledger *FileTransferLedger) pruneOldestRetainedLockedAt(
	protectedID string,
	now time.Time,
) bool {
	oldestID := ""
	var oldestAt int64
	retention := ledger.retention
	if retention <= 0 || retention > MaxFileTransferRetention {
		retention = DefaultFileTransferRetention
	}
	cutoff := now.Add(-retention).UnixNano()
	for id, record := range ledger.records {
		if id == protectedID ||
			!record.State.terminal() ||
			record.CompletedAt > cutoff {
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
	delete(ledger.records, oldestID)
	return true
}

func (ledger *FileTransferLedger) pruneOldestCapacityLocked(
	protectedID string,
	now time.Time,
) bool {
	oldestID := ""
	var oldestAt int64
	for id, record := range ledger.records {
		if id == protectedID ||
			!record.State.terminal() ||
			record.ExpiresAt > now.Unix() {
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
	delete(ledger.records, oldestID)
	return true
}

func (ledger *FileTransferLedger) rollbackIfUncommittedLocked(
	previous map[string]FileTransferRecord,
	err error,
) {
	if fileutil.IsCommittedWriteError(err) {
		return
	}
	ledger.records = previous
}

func validateFileTransferRecord(record FileTransferRecord) error {
	digest, err := decodeTransferDigest(record.SHA256)
	if err != nil {
		return ErrFileTransferConflict
	}
	frame := protocol.TransferFrame{
		Type:           protocol.TransferFrameStatus,
		Direction:      record.Direction,
		TransferID:     record.TransferID,
		PolicyRevision: record.PolicyRevision,
		TotalSize:      record.TotalSize,
		SHA256:         digest,
	}
	if err := frame.Validate(); err != nil ||
		record.Operation == "" ||
		record.ProfileAlias == "" ||
		record.Path == "" ||
		record.ExpiresAt <= 0 ||
		record.CreatedAt <= 0 ||
		record.UpdatedAt < record.CreatedAt ||
		record.ObservedBytes > record.TotalSize ||
		record.Sequence > maxTransferChunks(record.TotalSize) ||
		len(record.Result) > protocol.MaxTransferMetadataBytes ||
		len(record.FailureCode) > 64 ||
		(record.State.terminal() && record.CompletedAt <= 0) ||
		(!record.State.terminal() &&
			(record.CompletedAt != 0 ||
				len(record.Result) != 0 ||
				record.FailureCode != "")) {
		return ErrFileTransferConflict
	}
	switch record.State {
	case FileTransferPublished, FileTransferCommitted:
		if len(record.Result) == 0 || record.FailureCode != "" {
			return ErrFileTransferConflict
		}
	case FileTransferFailed,
		FileTransferCanceled,
		FileTransferExpired,
		FileTransferUnknown:
		if len(record.Result) != 0 || record.FailureCode == "" {
			return ErrFileTransferConflict
		}
	}
	switch record.Operation {
	case fileOperationInfo:
		if record.Direction != protocol.TransferDownload ||
			record.Publication != "" ||
			record.StageName != "" {
			return ErrFileTransferConflict
		}
	case fileOperationDownload:
		if record.Direction != protocol.TransferDownload ||
			record.Publication != "" ||
			record.StageName != "" {
			return ErrFileTransferConflict
		}
	case fileOperationUpload:
		if record.Direction != protocol.TransferUpload ||
			(record.Publication != filePublicationCreate &&
				record.Publication != filePublicationReplace) ||
			record.StageName == "" ||
			record.StageIdentity.Device == 0 ||
			record.StageIdentity.Inode == 0 ||
			record.StageIdentity.Links != 1 {
			return ErrFileTransferConflict
		}
	default:
		return ErrFileTransferConflict
	}
	if strings.ContainsRune(record.Path, 0) || len(record.Path) > MaxFilePathBytes {
		return ErrFileTransferConflict
	}
	return nil
}

func sameFileTransferBinding(left, right FileTransferRecord) bool {
	return left.TransferID == right.TransferID &&
		left.Direction == right.Direction &&
		left.Operation == right.Operation &&
		left.ProfileAlias == right.ProfileAlias &&
		left.PolicyRevision == right.PolicyRevision &&
		left.Path == right.Path &&
		left.Publication == right.Publication &&
		left.TotalSize == right.TotalSize &&
		left.SHA256 == right.SHA256 &&
		left.ExpiresAt == right.ExpiresAt
}

func cloneFileTransferRecord(record FileTransferRecord) FileTransferRecord {
	record.Result = append(json.RawMessage(nil), record.Result...)
	return record
}

func cloneFileTransferRecords(
	records map[string]FileTransferRecord,
) map[string]FileTransferRecord {
	cloned := make(map[string]FileTransferRecord, len(records))
	for id, record := range records {
		cloned[id] = cloneFileTransferRecord(record)
	}
	return cloned
}

func decodeFileTransferLedgerDocument(
	data []byte,
) (fileTransferLedgerDocument, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document fileTransferLedgerDocument
	if err := decoder.Decode(&document); err != nil {
		return fileTransferLedgerDocument{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fileTransferLedgerDocument{}, errors.New("file transfer ledger has trailing data")
	}
	return document, nil
}

func maxTransferChunks(total uint64) uint64 {
	if total == 0 {
		return 0
	}
	return (total + protocol.MaxTransferChunkBytes - 1) /
		protocol.MaxTransferChunkBytes
}
