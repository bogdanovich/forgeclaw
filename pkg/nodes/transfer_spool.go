package nodes

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

const (
	transferSpoolVersion            = 1
	transferSpoolIndexName          = ".transfer-spool.json"
	transferSpoolLockName           = ".transfer-spool.lock"
	TransferArtifactRefPrefix       = "transfer-artifact://"
	DefaultGatewayTransferLimit     = 256
	DefaultGatewayTransferSpoolSize = int64(4 * 1024 * 1024 * 1024)
	DefaultGatewayTransferRetention = 24 * time.Hour
	MaxGatewayActiveTransfers       = 8
	MaxTargetProfileActiveTransfers = 2
	MaxGatewayTransferRetention     = 7 * 24 * time.Hour
	MaxGatewayTransferLifetime      = time.Hour
	MaxTransferArtifactBytes        = int64(1024 * 1024 * 1024)
	MaxTransferArtifactChunkBytes   = 256 * 1024
)

var (
	ErrTransferArtifactConflict = errors.New("transfer artifact conflicts with durable state")
	ErrTransferArtifactNotFound = errors.New("transfer artifact not found")
	ErrTransferSpoolFull        = errors.New("transfer spool is full")
	ErrTransferSpoolInUse       = errors.New("transfer spool is already in use")
	ErrTransferSpoolClosed      = errors.New("transfer spool is closed")
	ErrTransferChunkSequence    = errors.New("transfer chunk sequence is invalid")
	ErrTransferSizeExceeded     = errors.New("transfer artifact exceeds declared size")
	ErrTransferDigestMismatch   = errors.New("transfer artifact digest mismatch")
)

type TransferDirection string

const (
	TransferDirectionUpload   TransferDirection = "upload"
	TransferDirectionDownload TransferDirection = "download"
)

type TransferArtifactState string

const (
	TransferArtifactStaging   TransferArtifactState = "staging"
	TransferArtifactCommitted TransferArtifactState = "committed"
)

// TransferArtifactOwner is the complete gateway-side ownership tuple. The
// opaque artifact reference is only correlation and never bypasses this tuple.
type TransferArtifactOwner struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	ActorID     string `json:"actor_id"`
	RouteID     string `json:"route_id"`
	SessionID   string `json:"session_id"`
	ToolCallID  string `json:"tool_call_id"`
}

func (owner TransferArtifactOwner) Validate() error {
	if !validInvocationIdentifier(owner.WorkspaceID) ||
		!validInvocationIdentifier(owner.AgentID) ||
		!validInvocationIdentifier(owner.ActorID) ||
		!validInvocationIdentifier(owner.RouteID) ||
		!validInvocationIdentifier(owner.SessionID) ||
		len(strings.TrimSpace(owner.ToolCallID)) == 0 ||
		len(strings.TrimSpace(owner.ToolCallID)) > maxGatewayToolCallIDLength {
		return fmt.Errorf("%w: malformed transfer artifact owner", ErrInvalidInvocation)
	}
	return nil
}

type TransferArtifactSpec struct {
	TransferID      string            `json:"transfer_id"`
	Direction       TransferDirection `json:"direction"`
	Target          string            `json:"target"`
	ProfileRevision string            `json:"profile_revision"`
	Filename        string            `json:"filename"`
	ContentType     string            `json:"content_type,omitempty"`
	DeclaredSize    int64             `json:"declared_size"`
	SHA256          string            `json:"sha256"`
	ExpiresAt       int64             `json:"expires_at"`
}

func (spec TransferArtifactSpec) Validate() error {
	if !validInvocationIdentifier(spec.TransferID) ||
		!validInvocationIdentifier(spec.Target) ||
		!validInvocationIdentifier(spec.ProfileRevision) ||
		!validTransferArtifactFilename(spec.Filename) ||
		!validTransferContentType(spec.ContentType) ||
		spec.DeclaredSize < 0 ||
		spec.DeclaredSize > MaxTransferArtifactBytes ||
		!validSHA256Digest(spec.SHA256) ||
		spec.ExpiresAt <= 0 {
		return fmt.Errorf("%w: malformed transfer artifact specification", ErrInvalidInvocation)
	}
	switch spec.Direction {
	case TransferDirectionUpload, TransferDirectionDownload:
		return nil
	default:
		return fmt.Errorf("%w: malformed transfer direction", ErrInvalidInvocation)
	}
}

type TransferArtifactRecord struct {
	ArtifactID   string                `json:"artifact_id"`
	Ref          string                `json:"ref"`
	Owner        TransferArtifactOwner `json:"owner"`
	Spec         TransferArtifactSpec  `json:"spec"`
	State        TransferArtifactState `json:"state"`
	StagingName  string                `json:"staging_name,omitempty"`
	DataName     string                `json:"data_name"`
	ObservedSize int64                 `json:"observed_size"`
	CreatedAt    int64                 `json:"created_at"`
	UpdatedAt    int64                 `json:"updated_at"`
	CommittedAt  int64                 `json:"committed_at,omitempty"`
}

type transferSpoolDocument struct {
	Version int                               `json:"version"`
	Records map[string]TransferArtifactRecord `json:"records"`
}

// GatewayTransferSpool owns bounded gateway-side transfer bytes. One process
// holds the anchored directory lease at a time so restart reconciliation can
// classify staging records without racing another live writer.
type GatewayTransferSpool struct {
	root       string
	maxRecords int
	maxBytes   int64
	retention  time.Duration
	now        func() time.Time
	directory  *anchoredDirectory
	release    func()
	writeIndex func([]byte) error

	mu      sync.Mutex
	records map[string]TransferArtifactRecord
	active  map[string]*TransferArtifactWriter
	closed  bool
}

type TransferArtifactWriter struct {
	store      *GatewayTransferSpool
	artifactID string
	file       *os.File
	digest     hash.Hash

	mu       sync.Mutex
	sequence uint64
	observed int64
	closed   bool
}

func NewGatewayTransferSpool(
	root string,
	maxRecords int,
	maxBytes int64,
	retention time.Duration,
) (*GatewayTransferSpool, error) {
	return newGatewayTransferSpool(root, maxRecords, maxBytes, retention, time.Now)
}

func newGatewayTransferSpool(
	root string,
	maxRecords int,
	maxBytes int64,
	retention time.Duration,
	now func() time.Time,
) (*GatewayTransferSpool, error) {
	root = filepath.Clean(root)
	if root == "." || root == string(filepath.Separator) {
		return nil, errors.New("gateway transfer spool path is required")
	}
	if maxRecords <= 0 {
		maxRecords = DefaultGatewayTransferLimit
	}
	if maxBytes <= 0 {
		maxBytes = DefaultGatewayTransferSpoolSize
	}
	if retention <= 0 {
		retention = DefaultGatewayTransferRetention
	}
	if retention > MaxGatewayTransferRetention {
		return nil, errors.New("gateway transfer spool retention exceeds hard limit")
	}
	if now == nil {
		now = time.Now
	}
	if err := ensureTransferSpoolRoot(root); err != nil {
		return nil, err
	}
	directory, err := openAnchoredDirectory(root)
	if err != nil {
		return nil, fmt.Errorf("open gateway transfer spool: %w", err)
	}
	release, err := directory.tryAcquireLock(transferSpoolLockName)
	if err != nil {
		_ = directory.close()
		if errors.Is(err, errAnchoredDirectoryLockBusy) {
			return nil, errors.Join(ErrTransferSpoolInUse, err)
		}
		return nil, err
	}
	store := &GatewayTransferSpool{
		root:       root,
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
		retention:  retention,
		now:        now,
		directory:  directory,
		release:    release,
		records:    make(map[string]TransferArtifactRecord),
		active:     make(map[string]*TransferArtifactWriter),
	}
	store.writeIndex = func(data []byte) error {
		return store.directory.writeFileAtomic(transferSpoolIndexName, data, 0o600)
	}
	if err := store.loadAndReconcile(); err != nil {
		release()
		_ = directory.close()
		return nil, err
	}
	return store, nil
}

func GatewayTransferSpoolPath(workspace string) string {
	return filepath.Join(workspace, "state", "node_transfers")
}

func (store *GatewayTransferSpool) Begin(
	owner TransferArtifactOwner,
	spec TransferArtifactSpec,
) (*TransferArtifactWriter, TransferArtifactRecord, bool, error) {
	if err := owner.Validate(); err != nil {
		return nil, TransferArtifactRecord{}, false, err
	}
	if err := spec.Validate(); err != nil {
		return nil, TransferArtifactRecord{}, false, err
	}
	now := store.now()
	if spec.ExpiresAt <= now.Unix() ||
		spec.ExpiresAt > now.Add(MaxGatewayTransferLifetime).Unix() {
		return nil, TransferArtifactRecord{}, false, fmt.Errorf(
			"%w: transfer artifact lifetime is outside bounds",
			ErrInvalidInvocation,
		)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, TransferArtifactRecord{}, false, ErrTransferSpoolClosed
	}
	if err := store.cleanupExpiredLocked(now); err != nil {
		return nil, TransferArtifactRecord{}, false, err
	}
	for _, existing := range store.records {
		if !sameTransferArtifactIdentity(existing, owner, spec) {
			continue
		}
		if sameTransferArtifactBinding(existing, owner, spec) &&
			existing.State == TransferArtifactCommitted {
			return nil, cloneTransferArtifactRecord(existing), false, nil
		}
		return nil, TransferArtifactRecord{}, false, ErrTransferArtifactConflict
	}
	if len(store.records) >= store.maxRecords ||
		store.reservedBytesLocked()+spec.DeclaredSize > store.maxBytes ||
		store.activeTransferCountLocked() >= MaxGatewayActiveTransfers ||
		store.targetProfileActiveTransferCountLocked(
			spec.Target,
			spec.ProfileRevision,
		) >= MaxTargetProfileActiveTransfers {
		return nil, TransferArtifactRecord{}, false, ErrTransferSpoolFull
	}
	artifactID, err := randomTransferArtifactID()
	if err != nil {
		return nil, TransferArtifactRecord{}, false, err
	}
	stagingName := "artifact_" + artifactID + ".part"
	dataName := "artifact_" + artifactID + ".data"
	file, err := store.directory.createRegularExclusive(stagingName, 0o600)
	if err != nil {
		return nil, TransferArtifactRecord{}, false, fmt.Errorf(
			"create transfer artifact staging file: %w",
			err,
		)
	}
	record := TransferArtifactRecord{
		ArtifactID:  artifactID,
		Ref:         TransferArtifactRefPrefix + artifactID,
		Owner:       owner,
		Spec:        spec,
		State:       TransferArtifactStaging,
		StagingName: stagingName,
		DataName:    dataName,
		CreatedAt:   now.UnixNano(),
		UpdatedAt:   now.UnixNano(),
	}
	previous := cloneTransferArtifactRecords(store.records)
	store.records[artifactID] = record
	persistErr := store.persistMutationLocked(previous)
	if persistErr != nil && !fileutil.IsCommittedWriteError(persistErr) {
		_ = file.Close()
		_ = store.directory.removeRegular(stagingName)
		return nil, TransferArtifactRecord{}, false, fmt.Errorf(
			"persist staged transfer artifact: %w",
			persistErr,
		)
	}
	writer := &TransferArtifactWriter{
		store:      store,
		artifactID: artifactID,
		file:       file,
		digest:     sha256.New(),
	}
	store.active[artifactID] = writer
	if persistErr != nil {
		persistErr = fmt.Errorf("persist staged transfer artifact: %w", persistErr)
	}
	return writer, cloneTransferArtifactRecord(record), true, persistErr
}

func (writer *TransferArtifactWriter) WriteChunk(sequence uint64, data []byte) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed || writer.store == nil || writer.file == nil {
		return ErrTransferSpoolClosed
	}
	if sequence != writer.sequence+1 ||
		len(data) == 0 ||
		len(data) > MaxTransferArtifactChunkBytes {
		return ErrTransferChunkSequence
	}
	writer.store.mu.Lock()
	record, found := writer.store.records[writer.artifactID]
	closed := writer.store.closed
	writer.store.mu.Unlock()
	if !found || closed {
		return ErrTransferSpoolClosed
	}
	if writer.observed+int64(len(data)) > record.Spec.DeclaredSize {
		return ErrTransferSizeExceeded
	}
	if _, err := writer.file.Write(data); err != nil {
		return fmt.Errorf("write transfer artifact chunk: %w", err)
	}
	if _, err := writer.digest.Write(data); err != nil {
		return fmt.Errorf("hash transfer artifact chunk: %w", err)
	}
	writer.sequence = sequence
	writer.observed += int64(len(data))
	return nil
}

func (writer *TransferArtifactWriter) Commit() (TransferArtifactRecord, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed || writer.store == nil || writer.file == nil {
		return TransferArtifactRecord{}, ErrTransferSpoolClosed
	}
	store := writer.store
	store.mu.Lock()
	record, found := store.records[writer.artifactID]
	storeClosed := store.closed
	store.mu.Unlock()
	if !found || storeClosed {
		return TransferArtifactRecord{}, ErrTransferSpoolClosed
	}
	if writer.observed != record.Spec.DeclaredSize ||
		hex.EncodeToString(writer.digest.Sum(nil)) != record.Spec.SHA256 {
		return TransferArtifactRecord{}, ErrTransferDigestMismatch
	}
	if err := writer.file.Chmod(0o600); err != nil {
		return TransferArtifactRecord{}, fmt.Errorf("set transfer artifact mode: %w", err)
	}
	if err := writer.file.Sync(); err != nil {
		return TransferArtifactRecord{}, fmt.Errorf("sync transfer artifact: %w", err)
	}
	if err := writer.file.Close(); err != nil {
		return TransferArtifactRecord{}, fmt.Errorf("close transfer artifact: %w", err)
	}
	writer.file = nil
	publicationErr := store.directory.publishRegularNoReplace(
		record.StagingName,
		record.DataName,
	)
	if publicationErr != nil && !fileutil.IsCommittedWriteError(publicationErr) {
		_ = store.directory.removeRegular(record.StagingName)
		writer.closed = true
		store.mu.Lock()
		delete(store.active, writer.artifactID)
		delete(store.records, writer.artifactID)
		persistErr := store.persistLocked()
		store.mu.Unlock()
		return TransferArtifactRecord{}, errors.Join(publicationErr, persistErr)
	}
	if fileutil.IsCommittedWriteError(publicationErr) {
		_ = store.directory.removeRegular(record.StagingName)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now()
	record = store.records[writer.artifactID]
	record.State = TransferArtifactCommitted
	record.StagingName = ""
	record.ObservedSize = writer.observed
	record.UpdatedAt = now.UnixNano()
	record.CommittedAt = now.UnixNano()
	store.records[writer.artifactID] = record
	delete(store.active, writer.artifactID)
	writer.closed = true
	persistErr := store.persistLocked()
	if persistErr != nil {
		persistErr = &fileutil.CommittedWriteError{Err: fmt.Errorf(
			"persist committed transfer artifact: %w",
			persistErr,
		)}
	}
	return cloneTransferArtifactRecord(record), errors.Join(publicationErr, persistErr)
}

func (writer *TransferArtifactWriter) Abort() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed {
		return nil
	}
	writer.closed = true
	if writer.file != nil {
		_ = writer.file.Close()
		writer.file = nil
	}
	if writer.store == nil {
		return nil
	}
	store := writer.store
	store.mu.Lock()
	defer store.mu.Unlock()
	record, found := store.records[writer.artifactID]
	delete(store.active, writer.artifactID)
	if !found || record.State != TransferArtifactStaging {
		return nil
	}
	previous := cloneTransferArtifactRecords(store.records)
	delete(store.records, writer.artifactID)
	persistErr := store.persistMutationLocked(previous)
	if persistErr != nil && !fileutil.IsCommittedWriteError(persistErr) {
		return persistErr
	}
	removeErr := store.directory.removeRegular(record.StagingName)
	return errors.Join(persistErr, removeErr)
}

func (store *GatewayTransferSpool) Resolve(
	owner TransferArtifactOwner,
	spec TransferArtifactSpec,
	ref string,
) (*os.File, TransferArtifactRecord, error) {
	if err := owner.Validate(); err != nil {
		return nil, TransferArtifactRecord{}, err
	}
	if err := spec.Validate(); err != nil {
		return nil, TransferArtifactRecord{}, err
	}
	artifactID, err := parseTransferArtifactRef(ref)
	if err != nil {
		return nil, TransferArtifactRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, TransferArtifactRecord{}, ErrTransferSpoolClosed
	}
	if cleanupErr := store.cleanupExpiredLocked(store.now()); cleanupErr != nil {
		return nil, TransferArtifactRecord{}, cleanupErr
	}
	record, found := store.records[artifactID]
	if !found ||
		record.State != TransferArtifactCommitted ||
		!sameTransferArtifactBinding(record, owner, spec) {
		return nil, TransferArtifactRecord{}, ErrTransferArtifactNotFound
	}
	file, info, err := store.directory.openRegular(record.DataName)
	if err != nil {
		return nil, TransferArtifactRecord{}, ErrTransferArtifactNotFound
	}
	if info.Size() != record.Spec.DeclaredSize {
		_ = file.Close()
		return nil, TransferArtifactRecord{}, ErrTransferDigestMismatch
	}
	digest, err := hashOpenFile(file)
	if err != nil {
		_ = file.Close()
		return nil, TransferArtifactRecord{}, err
	}
	if digest != record.Spec.SHA256 {
		_ = file.Close()
		return nil, TransferArtifactRecord{}, ErrTransferDigestMismatch
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, TransferArtifactRecord{}, err
	}
	return file, cloneTransferArtifactRecord(record), nil
}

func (store *GatewayTransferSpool) Release(
	owner TransferArtifactOwner,
	spec TransferArtifactSpec,
	ref string,
) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	artifactID, err := parseTransferArtifactRef(ref)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrTransferSpoolClosed
	}
	record, found := store.records[artifactID]
	if !found ||
		!sameTransferArtifactBinding(record, owner, spec) ||
		store.active[artifactID] != nil {
		return ErrTransferArtifactNotFound
	}
	previous := cloneTransferArtifactRecords(store.records)
	delete(store.records, artifactID)
	persistErr := store.persistMutationLocked(previous)
	if persistErr != nil && !fileutil.IsCommittedWriteError(persistErr) {
		return persistErr
	}
	removeErr := store.directory.removeRegular(record.DataName)
	return errors.Join(persistErr, removeErr)
}

func (store *GatewayTransferSpool) Cleanup() (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrTransferSpoolClosed
	}
	before := len(store.records)
	err := store.cleanupExpiredLocked(store.now())
	return before - len(store.records), err
}

func (store *GatewayTransferSpool) Close() error {
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return nil
	}
	store.closed = true
	writers := make([]*TransferArtifactWriter, 0, len(store.active))
	for _, writer := range store.active {
		writers = append(writers, writer)
	}
	store.mu.Unlock()
	var closeErr error
	for _, writer := range writers {
		closeErr = errors.Join(closeErr, writer.Abort())
	}
	store.mu.Lock()
	store.active = make(map[string]*TransferArtifactWriter)
	store.mu.Unlock()
	if store.release != nil {
		store.release()
		store.release = nil
	}
	closeErr = errors.Join(closeErr, store.directory.close())
	return closeErr
}

func (store *GatewayTransferSpool) loadAndReconcile() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	file, info, err := store.directory.openRegular(transferSpoolIndexName)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return fmt.Errorf("open gateway transfer spool index: %w", err)
	default:
		if info.Size() > DefaultGatewayInvocationStoreBytes {
			_ = file.Close()
			return errors.New("gateway transfer spool index exceeds size limit")
		}
		data, readErr := io.ReadAll(io.LimitReader(file, DefaultGatewayInvocationStoreBytes+1))
		_ = file.Close()
		if readErr != nil {
			return fmt.Errorf("read gateway transfer spool index: %w", readErr)
		}
		document, decodeErr := decodeTransferSpoolDocument(data)
		if decodeErr != nil {
			return decodeErr
		}
		store.records = document.Records
	}
	indexChanged := false
	removeNames := make([]string, 0)
	now := store.now()
	for artifactID, record := range store.records {
		if validationErr := validateTransferArtifactRecord(record); validationErr != nil ||
			record.ArtifactID != artifactID {
			return fmt.Errorf("invalid gateway transfer spool record %q", artifactID)
		}
		switch record.State {
		case TransferArtifactStaging:
			recovered, recoverErr := store.recoverStagingLocked(record, now)
			if recoverErr != nil {
				return recoverErr
			}
			if recovered == nil {
				delete(store.records, artifactID)
				removeNames = append(removeNames, record.StagingName, record.DataName)
			} else {
				store.records[artifactID] = *recovered
			}
			indexChanged = true
		case TransferArtifactCommitted:
			file, info, openErr := store.directory.openRegular(record.DataName)
			removeRecord := openErr != nil
			if openErr == nil && info.Size() == record.Spec.DeclaredSize {
				digest, hashErr := hashOpenFile(file)
				if hashErr != nil {
					_ = file.Close()
					return hashErr
				}
				removeRecord = digest != record.Spec.SHA256
			} else if openErr == nil {
				removeRecord = true
			}
			if removeRecord {
				if file != nil {
					_ = file.Close()
				}
				delete(store.records, artifactID)
				removeNames = append(removeNames, record.DataName)
				indexChanged = true
			} else {
				_ = file.Close()
			}
		default:
			return fmt.Errorf("invalid gateway transfer spool state %q", record.State)
		}
	}
	for artifactID, record := range store.records {
		if store.artifactExpiredLocked(record, now) {
			delete(store.records, artifactID)
			removeNames = append(removeNames, record.StagingName, record.DataName)
			indexChanged = true
		}
	}
	if len(store.records) > store.maxRecords || store.reservedBytesLocked() > store.maxBytes {
		return ErrTransferSpoolFull
	}

	names, err := store.directory.listNames()
	if err != nil {
		return fmt.Errorf("enumerate gateway transfer spool: %w", err)
	}
	referenced := make(map[string]struct{}, len(store.records)*2)
	for _, record := range store.records {
		if record.StagingName != "" {
			referenced[record.StagingName] = struct{}{}
		}
		referenced[record.DataName] = struct{}{}
	}
	for _, name := range names {
		if !isTransferArtifactDataName(name) {
			continue
		}
		if _, found := referenced[name]; !found {
			removeNames = append(removeNames, name)
		}
	}
	removeNames = uniqueNonemptyNames(removeNames)
	if len(removeNames) != 0 {
		indexChanged = true
	}
	if !indexChanged {
		return nil
	}
	persistErr := store.persistLocked()
	if persistErr != nil && !fileutil.IsCommittedWriteError(persistErr) {
		return persistErr
	}
	removeErr := store.removeRegularFilesLocked(removeNames)
	return errors.Join(persistErr, removeErr)
}

func (store *GatewayTransferSpool) recoverStagingLocked(
	record TransferArtifactRecord,
	now time.Time,
) (*TransferArtifactRecord, error) {
	file, info, err := store.directory.openRegular(record.DataName)
	if err == nil {
		defer file.Close()
		if info.Size() != record.Spec.DeclaredSize {
			return nil, nil
		}
		digest, hashErr := hashOpenFile(file)
		if hashErr != nil {
			return nil, hashErr
		}
		if digest != record.Spec.SHA256 {
			return nil, nil
		}
		record.State = TransferArtifactCommitted
		record.StagingName = ""
		record.ObservedSize = record.Spec.DeclaredSize
		record.UpdatedAt = now.UnixNano()
		record.CommittedAt = now.UnixNano()
		return &record, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return nil, nil
}

func (store *GatewayTransferSpool) cleanupExpiredLocked(now time.Time) error {
	previous := cloneTransferArtifactRecords(store.records)
	if err := store.cleanupExpiredWithoutPersistLocked(now); err != nil {
		return err
	}
	if sameTransferArtifactRecords(previous, store.records) {
		return nil
	}
	persistErr := store.persistMutationLocked(previous)
	if persistErr != nil && !fileutil.IsCommittedWriteError(persistErr) {
		return persistErr
	}
	removeNames := make([]string, 0, (len(previous)-len(store.records))*2)
	for artifactID, record := range previous {
		if _, retained := store.records[artifactID]; retained {
			continue
		}
		removeNames = append(removeNames, record.StagingName, record.DataName)
	}
	removeErr := store.removeRegularFilesLocked(removeNames)
	return errors.Join(persistErr, removeErr)
}

func (store *GatewayTransferSpool) cleanupExpiredWithoutPersistLocked(now time.Time) error {
	for artifactID, record := range store.records {
		if store.active[artifactID] != nil {
			continue
		}
		if store.artifactExpiredLocked(record, now) {
			delete(store.records, artifactID)
		}
	}
	return nil
}

func (store *GatewayTransferSpool) artifactExpiredLocked(
	record TransferArtifactRecord,
	now time.Time,
) bool {
	expiresAt := time.Unix(record.Spec.ExpiresAt, 0)
	if record.State == TransferArtifactCommitted {
		expiresAt = time.Unix(0, record.CommittedAt).Add(store.retention)
	}
	return !now.Before(expiresAt)
}

func (store *GatewayTransferSpool) persistLocked() error {
	document := transferSpoolDocument{
		Version: transferSpoolVersion,
		Records: cloneTransferArtifactRecords(store.records),
	}
	data, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("encode gateway transfer spool index: %w", err)
	}
	if len(data) > DefaultGatewayInvocationStoreBytes {
		return ErrTransferSpoolFull
	}
	return store.writeIndex(data)
}

func (store *GatewayTransferSpool) persistMutationLocked(
	previous map[string]TransferArtifactRecord,
) error {
	err := store.persistLocked()
	if err != nil && !fileutil.IsCommittedWriteError(err) {
		store.records = previous
	}
	return err
}

func (store *GatewayTransferSpool) removeRegularFilesLocked(names []string) error {
	var removeErr error
	for _, name := range uniqueNonemptyNames(names) {
		removeErr = errors.Join(removeErr, store.directory.removeRegular(name))
	}
	return removeErr
}

func (store *GatewayTransferSpool) reservedBytesLocked() int64 {
	var total int64
	for _, record := range store.records {
		total += record.Spec.DeclaredSize
	}
	return total
}

func (store *GatewayTransferSpool) activeTransferCountLocked() int {
	count := 0
	for _, record := range store.records {
		if record.State == TransferArtifactStaging {
			count++
		}
	}
	return count
}

func (store *GatewayTransferSpool) targetProfileActiveTransferCountLocked(
	target string,
	profileRevision string,
) int {
	count := 0
	for _, record := range store.records {
		if record.State == TransferArtifactStaging &&
			record.Spec.Target == target &&
			record.Spec.ProfileRevision == profileRevision {
			count++
		}
	}
	return count
}

func ensureTransferSpoolRoot(root string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create gateway transfer spool: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("gateway transfer spool root is linked or non-directory")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return errors.New("gateway transfer spool root permissions are too broad")
	}
	return nil
}

func decodeTransferSpoolDocument(data []byte) (transferSpoolDocument, error) {
	value, err := jsonstrict.Decode(data)
	if err != nil {
		return transferSpoolDocument{}, fmt.Errorf("decode gateway transfer spool index: %w", err)
	}
	object, ok := value.(map[string]any)
	if !ok || len(object) != 2 {
		return transferSpoolDocument{}, errors.New("invalid gateway transfer spool index")
	}
	if _, ok := object["version"]; !ok {
		return transferSpoolDocument{}, errors.New("gateway transfer spool index has no version")
	}
	if _, ok := object["records"]; !ok {
		return transferSpoolDocument{}, errors.New("gateway transfer spool index has no records")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document transferSpoolDocument
	if err := decoder.Decode(&document); err != nil {
		return transferSpoolDocument{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return transferSpoolDocument{}, errors.New("gateway transfer spool index has trailing data")
	}
	if document.Version != transferSpoolVersion || document.Records == nil {
		return transferSpoolDocument{}, errors.New("unsupported gateway transfer spool index")
	}
	return document, nil
}

func validateTransferArtifactRecord(record TransferArtifactRecord) error {
	if !validInvocationIdentifier(record.ArtifactID) ||
		record.Ref != TransferArtifactRefPrefix+record.ArtifactID ||
		record.DataName != "artifact_"+record.ArtifactID+".data" ||
		record.CreatedAt <= 0 ||
		record.UpdatedAt < record.CreatedAt ||
		record.ObservedSize < 0 ||
		record.ObservedSize > record.Spec.DeclaredSize {
		return ErrTransferArtifactConflict
	}
	if err := record.Owner.Validate(); err != nil {
		return err
	}
	if err := record.Spec.Validate(); err != nil {
		return err
	}
	switch record.State {
	case TransferArtifactStaging:
		if record.StagingName != "artifact_"+record.ArtifactID+".part" ||
			record.CommittedAt != 0 {
			return ErrTransferArtifactConflict
		}
	case TransferArtifactCommitted:
		if record.StagingName != "" ||
			record.ObservedSize != record.Spec.DeclaredSize ||
			record.CommittedAt < record.CreatedAt {
			return ErrTransferArtifactConflict
		}
	default:
		return ErrTransferArtifactConflict
	}
	return nil
}

func sameTransferArtifactBinding(
	record TransferArtifactRecord,
	owner TransferArtifactOwner,
	spec TransferArtifactSpec,
) bool {
	return record.Owner == owner && record.Spec == spec
}

func sameTransferArtifactIdentity(
	record TransferArtifactRecord,
	owner TransferArtifactOwner,
	spec TransferArtifactSpec,
) bool {
	return record.Owner == owner &&
		record.Spec.TransferID == spec.TransferID &&
		record.Spec.Direction == spec.Direction &&
		record.Spec.Target == spec.Target &&
		record.Spec.ProfileRevision == spec.ProfileRevision
}

func isTransferArtifactDataName(name string) bool {
	const (
		prefix = "artifact_"
		part   = ".part"
		data   = ".data"
	)
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	var suffix string
	switch {
	case strings.HasSuffix(name, part):
		suffix = part
	case strings.HasSuffix(name, data):
		suffix = data
	default:
		return false
	}
	artifactID := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if len(artifactID) != 32 {
		return false
	}
	_, err := hex.DecodeString(artifactID)
	return err == nil
}

func uniqueNonemptyNames(names []string) []string {
	unique := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, found := seen[name]; found {
			continue
		}
		seen[name] = struct{}{}
		unique = append(unique, name)
	}
	return unique
}

func cloneTransferArtifactRecord(record TransferArtifactRecord) TransferArtifactRecord {
	return record
}

func cloneTransferArtifactRecords(
	records map[string]TransferArtifactRecord,
) map[string]TransferArtifactRecord {
	cloned := make(map[string]TransferArtifactRecord, len(records))
	for key, record := range records {
		cloned[key] = cloneTransferArtifactRecord(record)
	}
	return cloned
}

func sameTransferArtifactRecords(
	left map[string]TransferArtifactRecord,
	right map[string]TransferArtifactRecord,
) bool {
	if len(left) != len(right) {
		return false
	}
	for key, record := range left {
		if right[key] != record {
			return false
		}
	}
	return true
}

func randomTransferArtifactID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func parseTransferArtifactRef(ref string) (string, error) {
	if !strings.HasPrefix(ref, TransferArtifactRefPrefix) {
		return "", ErrTransferArtifactNotFound
	}
	artifactID := strings.TrimPrefix(ref, TransferArtifactRefPrefix)
	if len(artifactID) != 32 || !validInvocationIdentifier(artifactID) {
		return "", ErrTransferArtifactNotFound
	}
	return artifactID, nil
}

func validTransferArtifactFilename(value string) bool {
	if value == "" ||
		len(value) > 255 ||
		!utf8.ValidString(value) ||
		filepath.Base(value) != value ||
		strings.ContainsAny(value, `/\`) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validTransferContentType(value string) bool {
	if len(value) > 255 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}

func hashOpenFile(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
