package companion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

const (
	fileOperationInfo     = "info"
	fileOperationUpload   = "upload"
	fileOperationDownload = "download"

	filePublicationCreate  = "create"
	filePublicationReplace = "replace"
)

var emptyTransferDigest = sha256.Sum256(nil)

type fileTransferPrepare struct {
	Operation   string `json:"operation"`
	Path        string `json:"path"`
	Publication string `json:"publication,omitempty"`
	ExpiresAt   int64  `json:"expires_at"`
}

type fileTransferResult struct {
	State       FileTransferState `json:"state"`
	Type        string            `json:"type,omitempty"`
	Size        uint64            `json:"size,omitempty"`
	Mode        uint32            `json:"mode,omitempty"`
	ModifiedAt  int64             `json:"modified_at,omitempty"`
	SHA256      string            `json:"sha256,omitempty"`
	Sequence    uint64            `json:"sequence,omitempty"`
	Transferred uint64            `json:"transferred,omitempty"`
	Code        string            `json:"code,omitempty"`
}

type fileProfileRuntime struct {
	profile       FilePolicyProfile
	readableRoots []*fileRoot
	writableRoots []*fileRoot
}

type activeFileTransfer struct {
	mu           sync.Mutex
	record       FileTransferRecord
	profile      *fileProfileRuntime
	parent       *resolvedParent
	stage        *stagedFile
	source       *resolvedFile
	hasher       hash.Hash
	chunkDigests map[uint64][32]byte
	acknowledged chan uint64
	pendingAck   uint64
	lastAck      uint64
	ctx          context.Context
	cancel       context.CancelFunc
	received     chan struct{}
	done         chan struct{}
	receivedOnce sync.Once
	doneOnce     sync.Once
}

type FileTransferRuntime struct {
	ledger      *FileTransferLedger
	descriptors []nodes.CommandDescriptor
	profiles    map[string]*fileProfileRuntime
	now         func() time.Time

	activeMu sync.Mutex
	active   map[string]*activeFileTransfer
	closeMu  sync.Mutex
	closed   bool
}

func NewFileTransferRuntime(
	policies FilePolicies,
	ledger *FileTransferLedger,
) (*FileTransferRuntime, error) {
	if len(policies) == 0 {
		return nil, errors.New("node file transfer policies are required")
	}
	if ledger == nil {
		return nil, errors.New("node file transfer ledger is required")
	}
	descriptors, err := fileCapabilityDescriptors(policies)
	if err != nil {
		return nil, err
	}
	if len(descriptors) == 0 {
		return nil, errors.New("node file transfer policies grant no enabled profile")
	}
	runtime := &FileTransferRuntime{
		ledger:      ledger,
		descriptors: descriptors,
		profiles:    make(map[string]*fileProfileRuntime),
		active:      make(map[string]*activeFileTransfer),
		now:         time.Now,
	}
	aliases := make([]string, 0, len(policies))
	for alias := range policies {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, alias := range aliases {
		profile := policies[alias]
		if !profile.Enabled {
			continue
		}
		opened, openErr := openFileProfile(profile)
		if openErr != nil {
			runtime.closeProfiles()
			return nil, fmt.Errorf("open file policy %q: %w", alias, openErr)
		}
		if runtime.profiles[profile.Revision] != nil {
			opened.close()
			runtime.closeProfiles()
			return nil, errors.New("duplicate enabled file policy revision")
		}
		runtime.profiles[profile.Revision] = opened
	}
	if err := runtime.reconcile(); err != nil {
		runtime.closeProfiles()
		return nil, err
	}
	return runtime, nil
}

func openFileProfile(profile FilePolicyProfile) (*fileProfileRuntime, error) {
	runtime := &fileProfileRuntime{profile: profile}
	for _, path := range profile.ReadableRoots {
		root, err := openFileRoot(path)
		if err != nil {
			runtime.close()
			return nil, err
		}
		runtime.readableRoots = append(runtime.readableRoots, root)
	}
	for _, path := range profile.WritableRoots {
		root, err := openFileRoot(path)
		if err != nil {
			runtime.close()
			return nil, err
		}
		runtime.writableRoots = append(runtime.writableRoots, root)
	}
	sort.Slice(runtime.readableRoots, func(left, right int) bool {
		return len(runtime.readableRoots[left].path) >
			len(runtime.readableRoots[right].path)
	})
	sort.Slice(runtime.writableRoots, func(left, right int) bool {
		return len(runtime.writableRoots[left].path) >
			len(runtime.writableRoots[right].path)
	})
	return runtime, nil
}

func (profile *fileProfileRuntime) close() {
	if profile == nil {
		return
	}
	for _, root := range profile.readableRoots {
		_ = root.close()
	}
	for _, root := range profile.writableRoots {
		_ = root.close()
	}
	profile.readableRoots = nil
	profile.writableRoots = nil
}

func (runtime *FileTransferRuntime) closeProfiles() {
	for _, profile := range runtime.profiles {
		profile.close()
	}
}

func (runtime *FileTransferRuntime) Close() {
	if runtime == nil {
		return
	}
	runtime.closeMu.Lock()
	if runtime.closed {
		runtime.closeMu.Unlock()
		return
	}
	runtime.closed = true
	runtime.closeMu.Unlock()
	runtime.activeMu.Lock()
	active := make([]*activeFileTransfer, 0, len(runtime.active))
	for _, transfer := range runtime.active {
		active = append(active, transfer)
	}
	runtime.active = make(map[string]*activeFileTransfer)
	runtime.activeMu.Unlock()
	for _, transfer := range active {
		runtime.disconnectTransfer(transfer)
	}
	runtime.closeProfiles()
}

func (runtime *FileTransferRuntime) Descriptors() []nodes.CommandDescriptor {
	if runtime == nil {
		return nil
	}
	result := make([]nodes.CommandDescriptor, len(runtime.descriptors))
	for index, descriptor := range runtime.descriptors {
		result[index] = descriptor
		result[index].InputSchema = append(
			json.RawMessage(nil),
			descriptor.InputSchema...,
		)
		result[index].OutputSchema = append(
			json.RawMessage(nil),
			descriptor.OutputSchema...,
		)
		result[index].ModelContract = cloneModelContract(descriptor.ModelContract)
		result[index].FileProfiles = cloneFileProfileDescriptors(descriptor.FileProfiles)
	}
	return result
}

func (runtime *FileTransferRuntime) HandleTransferFrame(
	ctx context.Context,
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	if runtime == nil || send == nil {
		return errors.New("node file transfer runtime is unavailable")
	}
	if err := frame.Validate(); err != nil {
		return err
	}
	runtime.closeMu.Lock()
	closed := runtime.closed
	runtime.closeMu.Unlock()
	if closed {
		return errors.New("node file transfer runtime is closed")
	}
	switch frame.Type {
	case protocol.TransferFrameCommit,
		protocol.TransferFrameCancel,
		protocol.TransferFrameStatus:
		if len(frame.Payload) != 0 {
			return protocol.ErrInvalidTransferFrame
		}
	}
	profile := runtime.profiles[frame.PolicyRevision]
	if profile == nil {
		return runtime.sendDenial(frame, send, "PROFILE_DENIED")
	}
	switch frame.Type {
	case protocol.TransferFramePrepare:
		return runtime.prepare(ctx, profile, frame, send)
	case protocol.TransferFrameChunk:
		return runtime.receiveUploadChunk(frame, send)
	case protocol.TransferFrameAck:
		return runtime.receiveDownloadAcknowledgement(frame)
	case protocol.TransferFrameCommit:
		return runtime.commit(frame, send)
	case protocol.TransferFrameCancel:
		return runtime.cancel(frame, send)
	case protocol.TransferFrameStatus:
		return runtime.status(frame, send)
	default:
		return protocol.ErrInvalidTransferFrame
	}
}

func (runtime *FileTransferRuntime) prepare(
	connectionContext context.Context,
	profile *fileProfileRuntime,
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	var request fileTransferPrepare
	if err := decodeStrictJSON(frame.Payload, &request); err != nil ||
		request.ExpiresAt <= 0 {
		return runtime.sendDenial(frame, send, "INVALID_PREPARE")
	}
	if err := validateFilePath(request.Path); err != nil {
		return runtime.sendDenial(frame, send, "PATH_DENIED")
	}
	if existing, found, err := runtime.ledger.Lookup(frame.TransferID); err != nil {
		return err
	} else if found {
		candidate := runtime.recordForPrepare(profile, frame, request)
		if !sameFileTransferBinding(existing, candidate) {
			return runtime.sendDenial(frame, send, "TRANSFER_CONFLICT")
		}
		return runtime.sendExisting(frame, existing, send)
	}
	now := runtime.now()
	if now.Unix() >= request.ExpiresAt ||
		request.ExpiresAt > now.Add(time.Hour).Unix() {
		return runtime.sendDenial(frame, send, "INVALID_PREPARE")
	}
	if request.Operation == fileOperationInfo {
		return runtime.prepareInfo(
			connectionContext,
			profile,
			frame,
			request,
			send,
		)
	}
	switch request.Operation {
	case fileOperationUpload:
		return runtime.prepareUpload(
			connectionContext,
			profile,
			frame,
			request,
			send,
		)
	case fileOperationDownload:
		return runtime.prepareDownload(
			connectionContext,
			profile,
			frame,
			request,
			send,
		)
	default:
		return runtime.sendDenial(frame, send, "OPERATION_DENIED")
	}
}

func (runtime *FileTransferRuntime) prepareInfo(
	ctx context.Context,
	profile *fileProfileRuntime,
	frame protocol.TransferFrame,
	request fileTransferPrepare,
	send func(protocol.TransferFrame) error,
) error {
	if frame.Direction != protocol.TransferDownload ||
		frame.TotalSize != 0 ||
		frame.SHA256 != emptyTransferDigest ||
		request.Publication != "" {
		return runtime.sendDenial(frame, send, "INVALID_METADATA_BINDING")
	}
	source, err := profile.openReadable(request.Path)
	if err != nil {
		return runtime.sendFileAccessDenial(frame, send, err)
	}
	defer source.file.Close()
	digest, err := hashOpenedFile(ctx, source.file)
	if err != nil {
		return runtime.sendDenial(frame, send, "SOURCE_CHANGED")
	}
	info, err := source.file.Stat()
	if err != nil || !sameOpenedFile(source, info) {
		return runtime.sendDenial(frame, send, "SOURCE_CHANGED")
	}
	result := fileTransferResult{
		State:      FileTransferCommitted,
		Type:       "regular_file",
		Size:       uint64(info.Size()),
		Mode:       uint32(info.Mode().Perm()),
		ModifiedAt: info.ModTime().UTC().Truncate(time.Second).Unix(),
		SHA256:     hex.EncodeToString(digest[:]),
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	record := runtime.recordForPrepare(profile, frame, request)
	record.State = FileTransferCommitted
	record.Result = payload
	record.CompletedAt = time.Now().UnixNano()
	accepted, existing, err := runtime.ledger.Accept(record)
	if err != nil {
		return err
	}
	if existing {
		return runtime.sendExisting(frame, accepted, send)
	}
	return send(responseTransferFrame(frame, protocol.TransferFrameCommitted, payload))
}

func (runtime *FileTransferRuntime) prepareUpload(
	connectionContext context.Context,
	profile *fileProfileRuntime,
	frame protocol.TransferFrame,
	request fileTransferPrepare,
	send func(protocol.TransferFrame) error,
) error {
	if frame.Direction != protocol.TransferUpload ||
		uint64(profile.profile.MaxFileBytes) < frame.TotalSize ||
		(request.Publication != filePublicationCreate &&
			request.Publication != filePublicationReplace) ||
		(request.Publication == filePublicationCreate &&
			!profile.profile.AllowCreate) ||
		(request.Publication == filePublicationReplace &&
			!profile.profile.AllowOverwrite) {
		return runtime.sendDenial(frame, send, "WRITE_DENIED")
	}
	if !runtime.hasActiveCapacity(frame.PolicyRevision) {
		return runtime.sendDenial(frame, send, "CAPACITY_EXCEEDED")
	}
	parent, err := profile.resolveWritableParent(request.Path)
	if err != nil {
		return runtime.sendFileAccessDenial(frame, send, err)
	}
	stage, err := parent.createStage(frame.TransferID)
	if err != nil {
		_ = parent.close()
		return runtime.sendFileAccessDenial(frame, send, err)
	}
	record := runtime.recordForPrepare(profile, frame, request)
	record.StageName = stage.name
	record.StageIdentity = stage.identity
	accepted, existing, err := runtime.ledger.Accept(record)
	if err != nil || existing {
		_ = stage.file.Close()
		_ = parent.removeStage(stage.identity, stage.name)
		_ = parent.close()
		if err != nil {
			return err
		}
		return runtime.sendExisting(frame, accepted, send)
	}
	transferContext, cancel := context.WithCancel(connectionContext)
	active := &activeFileTransfer{
		record:       accepted,
		profile:      profile,
		parent:       parent,
		stage:        stage,
		hasher:       sha256.New(),
		chunkDigests: make(map[uint64][32]byte),
		ctx:          transferContext,
		cancel:       cancel,
		received:     make(chan struct{}),
		done:         make(chan struct{}),
	}
	if err := runtime.addActive(active); err != nil {
		runtime.cancelTransfer(active, FileTransferCanceled)
		return runtime.sendDenial(frame, send, "CAPACITY_EXCEEDED")
	}
	go runtime.watchConnection(active, accepted.ExpiresAt)
	payload := mustFileTransferResult(fileTransferResult{
		State: FileTransferAccepted,
	})
	if err := send(responseTransferFrame(frame, protocol.TransferFrameAccept, payload)); err != nil {
		runtime.cancelTransfer(active, FileTransferCanceled)
		return err
	}
	return nil
}

func (runtime *FileTransferRuntime) prepareDownload(
	connectionContext context.Context,
	profile *fileProfileRuntime,
	frame protocol.TransferFrame,
	request fileTransferPrepare,
	send func(protocol.TransferFrame) error,
) error {
	if frame.Direction != protocol.TransferDownload ||
		frame.TotalSize > uint64(profile.profile.MaxFileBytes) ||
		request.Publication != "" {
		return runtime.sendDenial(frame, send, "READ_DENIED")
	}
	if !runtime.hasActiveCapacity(frame.PolicyRevision) {
		return runtime.sendDenial(frame, send, "CAPACITY_EXCEEDED")
	}
	source, err := profile.openReadable(request.Path)
	if err != nil {
		return runtime.sendFileAccessDenial(frame, send, err)
	}
	info, err := source.file.Stat()
	if err != nil ||
		!sameOpenedFile(source, info) ||
		uint64(info.Size()) != frame.TotalSize ||
		uint64(info.Size()) > uint64(profile.profile.MaxFileBytes) {
		_ = source.file.Close()
		return runtime.sendDenial(frame, send, "SOURCE_CHANGED")
	}
	record := runtime.recordForPrepare(profile, frame, request)
	record.SourceIdentity = source.identity
	record.SourceModified = info.ModTime().UnixNano()
	accepted, existing, err := runtime.ledger.Accept(record)
	if err != nil || existing {
		_ = source.file.Close()
		if err != nil {
			return err
		}
		return runtime.sendExisting(frame, accepted, send)
	}
	transferContext, cancel := context.WithCancel(connectionContext)
	active := &activeFileTransfer{
		record:       accepted,
		profile:      profile,
		source:       source,
		hasher:       sha256.New(),
		acknowledged: make(chan uint64, 1),
		ctx:          transferContext,
		cancel:       cancel,
		received:     make(chan struct{}),
		done:         make(chan struct{}),
	}
	if err := runtime.addActive(active); err != nil {
		runtime.cancelTransfer(active, FileTransferCanceled)
		return runtime.sendDenial(frame, send, "CAPACITY_EXCEEDED")
	}
	if err := send(responseTransferFrame(
		frame,
		protocol.TransferFrameAccept,
		mustFileTransferResult(fileTransferResult{State: FileTransferAccepted}),
	)); err != nil {
		runtime.cancelTransfer(active, FileTransferCanceled)
		return err
	}
	go runtime.streamDownload(active, frame, send)
	go runtime.watchConnection(active, accepted.ExpiresAt)
	return nil
}

func (runtime *FileTransferRuntime) receiveUploadChunk(
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	active := runtime.getActive(frame.TransferID)
	if active == nil {
		return runtime.sendFailure(frame, send, "TRANSFER_NOT_ACTIVE")
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	if active.record.Direction != protocol.TransferUpload ||
		!sameRecordFrameBinding(active.record, frame) ||
		active.record.State.terminal() ||
		active.stage == nil ||
		active.stage.file == nil {
		return protocol.ErrInvalidTransferFrame
	}
	if runtime.enforceExpiryLocked(active) {
		return runtime.sendExisting(frame, active.record, send)
	}
	chunkDigest := sha256.Sum256(frame.Payload)
	if prior, duplicate := active.chunkDigests[frame.Sequence]; duplicate {
		if prior != chunkDigest || frame.Sequence > active.record.Sequence {
			return protocol.ErrInvalidTransferFrame
		}
		return send(responseTransferFrame(frame, protocol.TransferFrameAck, nil))
	}
	if frame.Sequence != active.record.Sequence+1 ||
		active.record.ObservedBytes+uint64(len(frame.Payload)) >
			active.record.TotalSize {
		return protocol.ErrInvalidTransferFrame
	}
	if _, err := active.stage.file.Write(frame.Payload); err != nil {
		runtime.failLocked(active, "WRITE_FAILED")
		return runtime.sendFailure(frame, send, "WRITE_FAILED")
	}
	if _, err := active.hasher.Write(frame.Payload); err != nil {
		runtime.failLocked(active, "WRITE_FAILED")
		return runtime.sendFailure(frame, send, "WRITE_FAILED")
	}
	if err := active.stage.file.Sync(); err != nil {
		runtime.failLocked(active, "WRITE_FAILED")
		return runtime.sendFailure(frame, send, "WRITE_FAILED")
	}
	record, err := runtime.ledger.Transition(frame.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		if record.State != FileTransferAccepted &&
			record.State != FileTransferStreaming {
			return ErrFileTransferConflict
		}
		record.State = FileTransferStreaming
		record.Sequence = frame.Sequence
		record.ObservedBytes += uint64(len(frame.Payload))
		return nil
	})
	if err != nil {
		runtime.failLocked(active, "LEDGER_FAILED")
		return err
	}
	active.record = record
	active.chunkDigests[frame.Sequence] = chunkDigest
	return send(responseTransferFrame(frame, protocol.TransferFrameAck, nil))
}

func (runtime *FileTransferRuntime) receiveDownloadAcknowledgement(
	frame protocol.TransferFrame,
) error {
	active := runtime.getActive(frame.TransferID)
	if active == nil {
		return nil
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	if active.record.Direction != protocol.TransferDownload ||
		!sameRecordFrameBinding(active.record, frame) ||
		active.record.State.terminal() ||
		active.acknowledged == nil ||
		frame.Sequence == 0 ||
		frame.Sequence > active.record.Sequence+1 {
		return protocol.ErrInvalidTransferFrame
	}
	if runtime.enforceExpiryLocked(active) {
		return nil
	}
	if frame.Sequence <= active.record.Sequence ||
		frame.Sequence == active.lastAck {
		return nil
	}
	if frame.Sequence != active.pendingAck {
		return protocol.ErrInvalidTransferFrame
	}
	active.lastAck = frame.Sequence
	select {
	case active.acknowledged <- frame.Sequence:
		return nil
	default:
		return protocol.ErrInvalidTransferFrame
	}
}

func (runtime *FileTransferRuntime) streamDownload(
	active *activeFileTransfer,
	binding protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) {
	buffer := make([]byte, protocol.MaxTransferChunkBytes)
	for {
		if err := active.ctx.Err(); err != nil {
			runtime.cancelTransfer(active, FileTransferCanceled)
			return
		}
		count, readErr := active.source.file.Read(buffer)
		if count > 0 {
			active.mu.Lock()
			if runtime.enforceExpiryLocked(active) {
				record := active.record
				active.mu.Unlock()
				_ = runtime.sendExisting(binding, record, send)
				return
			}
			sequence := active.record.Sequence + 1
			active.pendingAck = sequence
			active.mu.Unlock()
			chunk := append([]byte(nil), buffer[:count]...)
			if _, err := active.hasher.Write(chunk); err != nil {
				runtime.failDownload(
					active,
					binding,
					send,
					"SOURCE_CHANGED",
				)
				return
			}
			frame := responseTransferFrame(
				binding,
				protocol.TransferFrameChunk,
				chunk,
			)
			frame.Sequence = sequence
			if err := send(frame); err != nil {
				runtime.cancelTransfer(active, FileTransferCanceled)
				return
			}
			select {
			case <-active.ctx.Done():
				runtime.cancelTransfer(active, FileTransferCanceled)
				return
			case acknowledged := <-active.acknowledged:
				if acknowledged != sequence {
					runtime.failDownload(
						active,
						binding,
						send,
						"SEQUENCE_MISMATCH",
					)
					return
				}
			}
			active.mu.Lock()
			if runtime.enforceExpiryLocked(active) {
				record := active.record
				active.mu.Unlock()
				_ = runtime.sendExisting(binding, record, send)
				return
			}
			record, err := runtime.ledger.Transition(
				active.record.TransferID,
				func(record *FileTransferRecord, _ time.Time) error {
					if record.State != FileTransferAccepted &&
						record.State != FileTransferStreaming {
						return ErrFileTransferConflict
					}
					record.State = FileTransferStreaming
					record.Sequence = sequence
					record.ObservedBytes += uint64(count)
					return nil
				},
			)
			if err != nil {
				active.mu.Unlock()
				runtime.failDownload(
					active,
					binding,
					send,
					"LEDGER_FAILED",
				)
				return
			}
			active.record = record
			active.pendingAck = 0
			active.mu.Unlock()
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			runtime.failDownload(active, binding, send, "SOURCE_CHANGED")
			return
		}
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	if runtime.enforceExpiryLocked(active) {
		_ = runtime.sendExisting(binding, active.record, send)
		return
	}
	info, err := active.source.file.Stat()
	digest := active.hasher.Sum(nil)
	if err != nil ||
		!sameOpenedFile(active.source, info) ||
		active.record.ObservedBytes != active.record.TotalSize ||
		!bytes.Equal(digest, binding.SHA256[:]) {
		runtime.failLocked(active, "SOURCE_CHANGED")
		_ = send(responseTransferFrame(
			binding,
			protocol.TransferFrameFailure,
			mustFileTransferResult(fileTransferResult{
				State: FileTransferFailed,
				Code:  "SOURCE_CHANGED",
			}),
		))
		return
	}
	record, err := runtime.ledger.Transition(
		active.record.TransferID,
		func(record *FileTransferRecord, _ time.Time) error {
			if record.State != FileTransferStreaming &&
				!(record.State == FileTransferAccepted && record.TotalSize == 0) {
				return ErrFileTransferConflict
			}
			record.State = FileTransferReceived
			return nil
		},
	)
	if err != nil {
		runtime.failLocked(active, "LEDGER_FAILED")
		_ = send(responseTransferFrame(
			binding,
			protocol.TransferFrameFailure,
			mustFileTransferResult(fileTransferResult{
				State: FileTransferFailed,
				Code:  "LEDGER_FAILED",
			}),
		))
		return
	}
	active.record = record
	active.receivedOnce.Do(func() { close(active.received) })
	if err := send(responseTransferFrame(
		binding,
		protocol.TransferFrameStatus,
		mustFileTransferResult(fileTransferResult{
			State:       FileTransferReceived,
			Size:        active.record.TotalSize,
			SHA256:      active.record.SHA256,
			Sequence:    active.record.Sequence,
			Transferred: active.record.ObservedBytes,
		}),
	)); err != nil {
		runtime.disconnectLocked(active)
	}
}

func (runtime *FileTransferRuntime) commit(
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	active := runtime.getActive(frame.TransferID)
	if active == nil {
		record, found, err := runtime.ledger.Lookup(frame.TransferID)
		if err != nil {
			return err
		}
		if !found || !sameRecordFrameBinding(record, frame) {
			return runtime.sendFailure(frame, send, "TRANSFER_NOT_FOUND")
		}
		return runtime.sendExisting(frame, record, send)
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	if !sameRecordFrameBinding(active.record, frame) {
		return protocol.ErrInvalidTransferFrame
	}
	if runtime.enforceExpiryLocked(active) {
		return runtime.sendExisting(frame, active.record, send)
	}
	if active.record.Direction == protocol.TransferUpload {
		return runtime.commitUploadLocked(active, frame, send)
	}
	return runtime.commitDownloadLocked(active, frame, send)
}

func (runtime *FileTransferRuntime) commitUploadLocked(
	active *activeFileTransfer,
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	if active.record.State == FileTransferPublished {
		return runtime.sendExisting(frame, active.record, send)
	}
	if active.record.State != FileTransferStreaming &&
		!(active.record.State == FileTransferAccepted &&
			active.record.TotalSize == 0) {
		return runtime.sendFailure(frame, send, "TRANSFER_NOT_STAGED")
	}
	if active.record.ObservedBytes != active.record.TotalSize ||
		!bytes.Equal(active.hasher.Sum(nil), frame.SHA256[:]) {
		runtime.failLocked(active, "DIGEST_MISMATCH")
		return runtime.sendFailure(frame, send, "DIGEST_MISMATCH")
	}
	record, err := runtime.ledger.Transition(frame.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferStaged
		return nil
	})
	if err != nil {
		return err
	}
	active.record = record
	record, err = runtime.ledger.Transition(frame.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferCommitRequested
		return nil
	})
	if err != nil {
		return err
	}
	active.record = record
	if publishErr := active.stage.publish(active.record.Publication); publishErr != nil {
		if _, committed := publishErr.(*committedFileMutationError); committed {
			runtime.markUnknownLocked(active, "PUBLICATION_UNCERTAIN")
			return runtime.sendFailure(frame, send, "PUBLICATION_UNCERTAIN")
		}
		runtime.failLocked(active, safeFileFailureCode(publishErr))
		return runtime.sendFailure(
			frame,
			send,
			safeFileFailureCode(publishErr),
		)
	}
	result := mustFileTransferResult(fileTransferResult{
		State:       FileTransferPublished,
		Size:        active.record.TotalSize,
		SHA256:      active.record.SHA256,
		Sequence:    active.record.Sequence,
		Transferred: active.record.ObservedBytes,
	})
	record, err = runtime.ledger.Transition(frame.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferPublished
		record.Result = result
		return nil
	})
	if err != nil {
		return err
	}
	active.record = record
	runtime.finishActiveLocked(active)
	return send(responseTransferFrame(frame, protocol.TransferFrameCommitted, result))
}

func (runtime *FileTransferRuntime) commitDownloadLocked(
	active *activeFileTransfer,
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	if active.record.State == FileTransferCommitted {
		return runtime.sendExisting(frame, active.record, send)
	}
	if active.record.State != FileTransferReceived {
		return runtime.sendFailure(frame, send, "TRANSFER_NOT_RECEIVED")
	}
	record, err := runtime.ledger.Transition(frame.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferCommitRequested
		return nil
	})
	if err != nil {
		return err
	}
	active.record = record
	result := mustFileTransferResult(fileTransferResult{
		State:       FileTransferCommitted,
		Size:        active.record.TotalSize,
		SHA256:      active.record.SHA256,
		Sequence:    active.record.Sequence,
		Transferred: active.record.ObservedBytes,
	})
	record, err = runtime.ledger.Transition(frame.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferCommitted
		record.Result = result
		return nil
	})
	if err != nil {
		return err
	}
	active.record = record
	runtime.finishActiveLocked(active)
	return send(responseTransferFrame(frame, protocol.TransferFrameCommitted, result))
}

func (runtime *FileTransferRuntime) cancel(
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	active := runtime.getActive(frame.TransferID)
	if active != nil {
		active.mu.Lock()
		if !sameRecordFrameBinding(active.record, frame) {
			active.mu.Unlock()
			return protocol.ErrInvalidTransferFrame
		}
		switch active.record.State {
		case FileTransferPublished, FileTransferCommitted:
		default:
			runtime.cancelLocked(active, FileTransferCanceled)
		}
		record := active.record
		active.mu.Unlock()
		return runtime.sendExisting(frame, record, send)
	}
	record, found, err := runtime.ledger.Lookup(frame.TransferID)
	if err != nil {
		return err
	}
	if !found || !sameRecordFrameBinding(record, frame) {
		return runtime.sendFailure(frame, send, "TRANSFER_NOT_FOUND")
	}
	return runtime.sendExisting(frame, record, send)
}

func (runtime *FileTransferRuntime) status(
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	record, found, err := runtime.ledger.Lookup(frame.TransferID)
	if err != nil {
		return err
	}
	if !found || !sameRecordFrameBinding(record, frame) {
		return runtime.sendFailure(frame, send, "TRANSFER_NOT_FOUND")
	}
	return runtime.sendExisting(frame, record, send)
}

func (runtime *FileTransferRuntime) addActive(active *activeFileTransfer) error {
	runtime.activeMu.Lock()
	defer runtime.activeMu.Unlock()
	if runtime.active[active.record.TransferID] != nil {
		return ErrFileTransferConflict
	}
	if len(runtime.active) >= nodes.MaxGatewayActiveTransfers {
		return ErrFileTransferLedgerFull
	}
	profileCount := 0
	for _, existing := range runtime.active {
		if existing.record.PolicyRevision == active.record.PolicyRevision {
			profileCount++
		}
	}
	if profileCount >= nodes.MaxTargetProfileActiveTransfers {
		return ErrFileTransferLedgerFull
	}
	runtime.active[active.record.TransferID] = active
	return nil
}

func (runtime *FileTransferRuntime) hasActiveCapacity(
	policyRevision string,
) bool {
	runtime.activeMu.Lock()
	defer runtime.activeMu.Unlock()
	if len(runtime.active) >= nodes.MaxGatewayActiveTransfers {
		return false
	}
	profileCount := 0
	for _, existing := range runtime.active {
		if existing.record.PolicyRevision == policyRevision {
			profileCount++
		}
	}
	return profileCount < nodes.MaxTargetProfileActiveTransfers
}

func (runtime *FileTransferRuntime) getActive(
	transferID string,
) *activeFileTransfer {
	runtime.activeMu.Lock()
	defer runtime.activeMu.Unlock()
	return runtime.active[transferID]
}

func (runtime *FileTransferRuntime) removeActive(active *activeFileTransfer) {
	runtime.activeMu.Lock()
	if runtime.active[active.record.TransferID] == active {
		delete(runtime.active, active.record.TransferID)
	}
	runtime.activeMu.Unlock()
}

func (runtime *FileTransferRuntime) watchConnection(
	active *activeFileTransfer,
	expiresAt int64,
) {
	delay := time.Until(time.Unix(expiresAt, 0))
	if delay <= 0 {
		runtime.cancelTransfer(active, FileTransferExpired)
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-active.ctx.Done():
		runtime.disconnectTransfer(active)
	case <-timer.C:
		active.mu.Lock()
		if active.record.Direction == protocol.TransferDownload &&
			active.record.State == FileTransferReceived {
			runtime.markUnknownLocked(active, "COMMIT_UNCERTAIN")
		} else {
			runtime.cancelLocked(active, FileTransferExpired)
		}
		active.mu.Unlock()
	}
}

func (runtime *FileTransferRuntime) enforceExpiryLocked(
	active *activeFileTransfer,
) bool {
	if active == nil ||
		active.record.State.terminal() ||
		runtime.now().Unix() < active.record.ExpiresAt {
		return false
	}
	if active.record.Direction == protocol.TransferDownload &&
		active.record.State == FileTransferReceived {
		runtime.markUnknownLocked(active, "COMMIT_UNCERTAIN")
		return true
	}
	runtime.cancelLocked(active, FileTransferExpired)
	return true
}

func (runtime *FileTransferRuntime) disconnectTransfer(
	active *activeFileTransfer,
) {
	if active == nil {
		return
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	runtime.disconnectLocked(active)
}

func (runtime *FileTransferRuntime) disconnectLocked(
	active *activeFileTransfer,
) {
	if active.record.Direction == protocol.TransferDownload &&
		active.record.State == FileTransferReceived {
		runtime.markUnknownLocked(active, "COMMIT_UNCERTAIN")
		return
	}
	runtime.cancelLocked(active, FileTransferCanceled)
}

func (runtime *FileTransferRuntime) cancelTransfer(
	active *activeFileTransfer,
	state FileTransferState,
) {
	if active == nil {
		return
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	runtime.cancelLocked(active, state)
}

func (runtime *FileTransferRuntime) cancelLocked(
	active *activeFileTransfer,
	state FileTransferState,
) {
	if active.record.State.terminal() {
		runtime.finishActiveLocked(active)
		return
	}
	if active.record.State == FileTransferCommitRequested {
		runtime.markUnknownLocked(active, "COMMIT_UNCERTAIN")
		return
	}
	cleanupErr := runtime.cleanupActiveLocked(active)
	if cleanupErr != nil {
		runtime.markUnknownLocked(active, "CLEANUP_UNCERTAIN")
		return
	}
	record, err := runtime.ledger.Transition(
		active.record.TransferID,
		func(record *FileTransferRecord, _ time.Time) error {
			record.State = state
			if state == FileTransferExpired {
				record.FailureCode = "EXPIRED"
			} else {
				record.FailureCode = "CANCELED"
			}
			return nil
		},
	)
	if err == nil {
		active.record = record
	}
	runtime.finishActiveLocked(active)
}

func (runtime *FileTransferRuntime) failTransfer(
	active *activeFileTransfer,
	code string,
) {
	active.mu.Lock()
	defer active.mu.Unlock()
	runtime.failLocked(active, code)
}

func (runtime *FileTransferRuntime) failDownload(
	active *activeFileTransfer,
	binding protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
	code string,
) {
	runtime.failTransfer(active, code)
	_ = send(responseTransferFrame(
		binding,
		protocol.TransferFrameFailure,
		mustFileTransferResult(fileTransferResult{
			State: FileTransferFailed,
			Code:  code,
		}),
	))
}

func (runtime *FileTransferRuntime) failLocked(
	active *activeFileTransfer,
	code string,
) {
	if active.record.State.terminal() {
		return
	}
	if cleanupErr := runtime.cleanupActiveLocked(active); cleanupErr != nil {
		runtime.markUnknownLocked(active, "CLEANUP_UNCERTAIN")
		return
	}
	record, err := runtime.ledger.Transition(
		active.record.TransferID,
		func(record *FileTransferRecord, _ time.Time) error {
			record.State = FileTransferFailed
			record.FailureCode = code
			return nil
		},
	)
	if err == nil {
		active.record = record
	}
	runtime.finishActiveLocked(active)
}

func (runtime *FileTransferRuntime) markUnknownLocked(
	active *activeFileTransfer,
	code string,
) {
	record, err := runtime.ledger.Transition(
		active.record.TransferID,
		func(record *FileTransferRecord, _ time.Time) error {
			record.State = FileTransferUnknown
			record.FailureCode = code
			return nil
		},
	)
	if err == nil {
		active.record = record
	}
	runtime.finishActiveLocked(active)
}

func (runtime *FileTransferRuntime) cleanupActiveLocked(
	active *activeFileTransfer,
) error {
	if active.cancel != nil {
		active.cancel()
	}
	if active.source != nil && active.source.file != nil {
		if err := active.source.file.Close(); err != nil {
			return err
		}
		active.source.file = nil
	}
	if active.stage != nil {
		if active.stage.file != nil {
			_ = active.stage.file.Close()
			active.stage.file = nil
		}
		if active.parent != nil {
			if err := active.parent.removeStage(
				active.record.StageIdentity,
				active.record.StageName,
			); err != nil {
				return err
			}
		}
	}
	if active.parent != nil {
		if err := active.parent.close(); err != nil {
			return err
		}
		active.parent = nil
	}
	return nil
}

func (runtime *FileTransferRuntime) finishActiveLocked(
	active *activeFileTransfer,
) {
	if active.cancel != nil {
		active.cancel()
		active.cancel = nil
	}
	if active.source != nil && active.source.file != nil {
		_ = active.source.file.Close()
		active.source.file = nil
	}
	if active.stage != nil && active.stage.file != nil {
		_ = active.stage.file.Close()
		active.stage.file = nil
	}
	if active.parent != nil {
		_ = active.parent.close()
		active.parent = nil
	}
	runtime.removeActive(active)
	active.doneOnce.Do(func() { close(active.done) })
}

func (runtime *FileTransferRuntime) reconcile() error {
	for _, record := range runtime.ledger.Records() {
		if record.State.terminal() {
			continue
		}
		profile := runtime.profiles[record.PolicyRevision]
		if profile == nil || profile.profile.normalizedAlias != record.ProfileAlias {
			if _, err := runtime.ledger.Transition(
				record.TransferID,
				func(current *FileTransferRecord, _ time.Time) error {
					current.State = FileTransferUnknown
					current.FailureCode = "PROFILE_STALE"
					return nil
				},
			); err != nil {
				return err
			}
			continue
		}
		if record.Direction == protocol.TransferDownload {
			state := FileTransferCanceled
			code := "RESTART_CANCELED"
			if record.State == FileTransferReceived ||
				record.State == FileTransferCommitRequested {
				state = FileTransferUnknown
				code = "COMMIT_UNCERTAIN"
			}
			if _, err := runtime.ledger.Transition(
				record.TransferID,
				func(current *FileTransferRecord, _ time.Time) error {
					current.State = state
					current.FailureCode = code
					return nil
				},
			); err != nil {
				return err
			}
			continue
		}
		if err := runtime.reconcileUpload(profile, record); err != nil {
			return err
		}
	}
	return nil
}

func (runtime *FileTransferRuntime) reconcileUpload(
	profile *fileProfileRuntime,
	record FileTransferRecord,
) error {
	parent, err := profile.resolveWritableParent(record.Path)
	if err != nil {
		_, transitionErr := runtime.ledger.Transition(
			record.TransferID,
			func(current *FileTransferRecord, _ time.Time) error {
				current.State = FileTransferUnknown
				current.FailureCode = "RECONCILIATION_DENIED"
				return nil
			},
		)
		return transitionErr
	}
	defer parent.close()
	if record.State == FileTransferCommitRequested {
		final, finalErr := parent.openFinalRegular()
		if finalErr == nil &&
			final.identity.Device == record.StageIdentity.Device &&
			final.identity.Inode == record.StageIdentity.Inode &&
			uint64(final.info.Size()) == record.TotalSize {
			digest, digestErr := hashOpenedFile(context.Background(), final.file)
			_ = final.file.Close()
			expected, expectedErr := decodeTransferDigest(record.SHA256)
			if digestErr != nil || expectedErr != nil || digest != expected {
				_, transitionErr := runtime.ledger.Transition(
					record.TransferID,
					func(current *FileTransferRecord, _ time.Time) error {
						current.State = FileTransferUnknown
						current.FailureCode = "PUBLICATION_UNCERTAIN"
						return nil
					},
				)
				return transitionErr
			}
			if cleanupErr := parent.removePublishedStage(
				record.StageIdentity,
				record.StageName,
			); cleanupErr != nil {
				_, transitionErr := runtime.ledger.Transition(
					record.TransferID,
					func(current *FileTransferRecord, _ time.Time) error {
						current.State = FileTransferUnknown
						current.FailureCode = "CLEANUP_UNCERTAIN"
						return nil
					},
				)
				return transitionErr
			}
			result := mustFileTransferResult(fileTransferResult{
				State:       FileTransferPublished,
				Size:        record.TotalSize,
				SHA256:      record.SHA256,
				Sequence:    record.Sequence,
				Transferred: record.ObservedBytes,
			})
			_, err = runtime.ledger.Transition(
				record.TransferID,
				func(current *FileTransferRecord, _ time.Time) error {
					current.State = FileTransferPublished
					current.Result = result
					return nil
				},
			)
			return err
		}
		stageExists, stageErr := parent.stageMatches(
			record.StageIdentity,
			record.StageName,
			true,
		)
		if stageErr != nil || !stageExists {
			_, transitionErr := runtime.ledger.Transition(
				record.TransferID,
				func(current *FileTransferRecord, _ time.Time) error {
					current.State = FileTransferUnknown
					current.FailureCode = "PUBLICATION_UNCERTAIN"
					return nil
				},
			)
			return transitionErr
		}
		if cleanupErr := parent.removeStage(
			record.StageIdentity,
			record.StageName,
		); cleanupErr != nil {
			_, transitionErr := runtime.ledger.Transition(
				record.TransferID,
				func(current *FileTransferRecord, _ time.Time) error {
					current.State = FileTransferUnknown
					current.FailureCode = "CLEANUP_UNCERTAIN"
					return nil
				},
			)
			return transitionErr
		}
		_, transitionErr := runtime.ledger.Transition(
			record.TransferID,
			func(current *FileTransferRecord, _ time.Time) error {
				current.State = FileTransferUnknown
				current.FailureCode = "PUBLICATION_UNPROVEN"
				return nil
			},
		)
		return transitionErr
	}
	if cleanupErr := parent.removeStage(
		record.StageIdentity,
		record.StageName,
	); cleanupErr != nil {
		_, transitionErr := runtime.ledger.Transition(
			record.TransferID,
			func(current *FileTransferRecord, _ time.Time) error {
				current.State = FileTransferUnknown
				current.FailureCode = "CLEANUP_UNCERTAIN"
				return nil
			},
		)
		return transitionErr
	}
	_, err = runtime.ledger.Transition(
		record.TransferID,
		func(current *FileTransferRecord, _ time.Time) error {
			current.State = FileTransferCanceled
			current.FailureCode = "RESTART_CANCELED"
			return nil
		},
	)
	return err
}

func (runtime *FileTransferRuntime) recordForPrepare(
	profile *fileProfileRuntime,
	frame protocol.TransferFrame,
	request fileTransferPrepare,
) FileTransferRecord {
	return FileTransferRecord{
		TransferID:     frame.TransferID,
		Direction:      frame.Direction,
		Operation:      request.Operation,
		ProfileAlias:   profile.profile.normalizedAlias,
		PolicyRevision: frame.PolicyRevision,
		Path:           request.Path,
		Publication:    request.Publication,
		TotalSize:      frame.TotalSize,
		SHA256:         hex.EncodeToString(frame.SHA256[:]),
		ExpiresAt:      request.ExpiresAt,
		State:          FileTransferAccepted,
	}
}

func (runtime *FileTransferRuntime) sendExisting(
	frame protocol.TransferFrame,
	record FileTransferRecord,
	send func(protocol.TransferFrame) error,
) error {
	payload := record.Result
	if len(payload) == 0 {
		payload = mustFileTransferResult(fileTransferResult{
			State:       record.State,
			Size:        record.TotalSize,
			SHA256:      record.SHA256,
			Sequence:    record.Sequence,
			Transferred: record.ObservedBytes,
			Code:        record.FailureCode,
		})
	}
	frameType := protocol.TransferFrameStatus
	switch record.State {
	case FileTransferAccepted, FileTransferStreaming, FileTransferStaged:
		frameType = protocol.TransferFrameAccept
	case FileTransferPublished, FileTransferCommitted:
		frameType = protocol.TransferFrameCommitted
	case FileTransferFailed,
		FileTransferCanceled,
		FileTransferExpired,
		FileTransferUnknown:
		frameType = protocol.TransferFrameFailure
	}
	return send(responseTransferFrame(frame, frameType, payload))
}

func (runtime *FileTransferRuntime) sendDenial(
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
	code string,
) error {
	return send(responseTransferFrame(
		frame,
		protocol.TransferFrameDeny,
		mustFileTransferResult(fileTransferResult{
			State: FileTransferFailed,
			Code:  code,
		}),
	))
}

func (runtime *FileTransferRuntime) sendFailure(
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
	code string,
) error {
	return send(responseTransferFrame(
		frame,
		protocol.TransferFrameFailure,
		mustFileTransferResult(fileTransferResult{
			State: FileTransferFailed,
			Code:  code,
		}),
	))
}

func (runtime *FileTransferRuntime) sendFileAccessDenial(
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
	err error,
) error {
	return runtime.sendDenial(frame, send, safeFileFailureCode(err))
}

func responseTransferFrame(
	request protocol.TransferFrame,
	frameType protocol.TransferFrameType,
	payload []byte,
) protocol.TransferFrame {
	response := protocol.TransferFrame{
		Type:           frameType,
		Direction:      request.Direction,
		TransferID:     request.TransferID,
		PolicyRevision: request.PolicyRevision,
		TotalSize:      request.TotalSize,
		SHA256:         request.SHA256,
		Payload:        append([]byte(nil), payload...),
	}
	if frameType == protocol.TransferFrameAck {
		response.Sequence = request.Sequence
	}
	return response
}

func mustFileTransferResult(result fileTransferResult) []byte {
	payload, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	return payload
}

func safeFileFailureCode(err error) string {
	switch {
	case errors.Is(err, ErrFileNotFound):
		return "NOT_FOUND"
	case errors.Is(err, ErrFileConflict):
		return "FILE_CONFLICT"
	case errors.Is(err, ErrFileAccessDenied):
		return "PATH_DENIED"
	default:
		return "FILE_OPERATION_FAILED"
	}
}

func (profile *fileProfileRuntime) openReadable(
	path string,
) (*resolvedFile, error) {
	for _, root := range profile.readableRoots {
		if _, err := root.relativePath(path); err != nil {
			continue
		}
		return root.openRegular(
			path,
			profile.profile.MaxFileBytes,
			profile.profile.CrossMounts,
		)
	}
	return nil, ErrFileAccessDenied
}

func (profile *fileProfileRuntime) resolveWritableParent(
	path string,
) (*resolvedParent, error) {
	for _, root := range profile.writableRoots {
		if _, err := root.relativePath(path); err != nil {
			continue
		}
		return root.resolveParent(path, profile.profile.CrossMounts)
	}
	return nil, ErrFileAccessDenied
}

func hashOpenedFile(ctx context.Context, file *os.File) ([32]byte, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return [32]byte{}, err
	}
	hasher := sha256.New()
	buffer := make([]byte, protocol.MaxTransferChunkBytes)
	for {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, err
		}
		count, err := file.Read(buffer)
		if count > 0 {
			if _, writeErr := hasher.Write(buffer[:count]); writeErr != nil {
				return [32]byte{}, writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return [32]byte{}, err
		}
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func sameOpenedFile(source *resolvedFile, info os.FileInfo) bool {
	if source == nil || info == nil || !info.Mode().IsRegular() ||
		info.Size() != source.info.Size() ||
		!info.ModTime().Equal(source.info.ModTime()) {
		return false
	}
	identity, err := identityFromInfo(info)
	return err == nil &&
		identity.Device == source.identity.Device &&
		identity.Inode == source.identity.Inode
}

func sameRecordFrameBinding(
	record FileTransferRecord,
	frame protocol.TransferFrame,
) bool {
	digest, err := decodeTransferDigest(record.SHA256)
	return err == nil &&
		record.TransferID == frame.TransferID &&
		record.Direction == frame.Direction &&
		record.PolicyRevision == frame.PolicyRevision &&
		record.TotalSize == frame.TotalSize &&
		digest == frame.SHA256
}

func decodeTransferDigest(value string) ([32]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [32]byte{}, ErrFileTransferConflict
	}
	var digest [32]byte
	copy(digest[:], decoded)
	return digest, nil
}
