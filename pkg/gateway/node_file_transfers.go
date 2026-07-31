package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

const nodeFileTransferRecoveryTimeout = 3 * time.Second

type nodeFileTransferSource struct {
	*nodeInvocationSource
	spool     *nodes.GatewayTransferSpool
	workspace string
}

type nodeFileTransferPlanInput struct {
	Path              string  `json:"path,omitempty"`
	Destination       string  `json:"destination,omitempty"`
	Source            string  `json:"source,omitempty"`
	Publication       string  `json:"publication,omitempty"`
	ArtifactRef       string  `json:"artifact_ref,omitempty"`
	SourceArtifactID  string  `json:"source_artifact_id,omitempty"`
	Size              float64 `json:"size,omitempty"`
	SHA256            string  `json:"sha256,omitempty"`
	Filename          string  `json:"filename,omitempty"`
	ContentType       string  `json:"content_type,omitempty"`
	Deliver           bool    `json:"deliver,omitempty"`
	Channel           string  `json:"channel,omitempty"`
	ChatID            string  `json:"chat_id,omitempty"`
	TopicID           string  `json:"topic_id,omitempty"`
	RouteID           string  `json:"route_id"`
	DiscoveryRevision string  `json:"discovery_revision"`
}

type nodeFileTransferPrepare struct {
	Operation   string `json:"operation"`
	Path        string `json:"path"`
	Publication string `json:"publication,omitempty"`
	ExpiresAt   int64  `json:"expires_at"`
}

type idempotentNodeTransferMediaStore interface {
	StoreIdempotentOwned(
		localPath string,
		meta media.MediaMeta,
		scope string,
		key string,
		owner media.MediaOwner,
	) (string, error)
}

type ownedNodeTransferMediaStore interface {
	ResolveOwnedWithMeta(
		ref string,
		owner media.MediaOwner,
	) (localPath string, meta media.MediaMeta, err error)
}

func newNodeFileTransferSource(
	cfg *config.Config,
	runtime *nodeAdmissionRuntime,
) (*nodeFileTransferSource, error) {
	if cfg == nil || !cfg.Nodes.Enabled || !configuredNodeFileTarget(cfg) {
		return nil, nil
	}
	invocations, err := newNodeInvocationSource(cfg, runtime)
	if err != nil || invocations == nil {
		return nil, err
	}
	workspace := cfg.WorkspacePath()
	spool, err := runtime.gatewayTransferSpool(nodes.GatewayTransferSpoolPath(workspace))
	if err != nil {
		return nil, fmt.Errorf("open gateway node transfer spool: %w", err)
	}
	return &nodeFileTransferSource{
		nodeInvocationSource: invocations,
		spool:                spool,
		workspace:            workspace,
	}, nil
}

func newNodeFileTransferRecoverySource(
	cfg *config.Config,
	runtime *nodeAdmissionRuntime,
) (*nodeFileTransferSource, error) {
	if cfg == nil || !cfg.Nodes.Enabled || runtime == nil {
		return nil, nil
	}
	path := nodes.GatewayTransferSpoolPath(cfg.WorkspacePath())
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	invocations, err := newNodeInvocationSource(cfg, runtime)
	if err != nil || invocations == nil {
		return nil, err
	}
	spool, err := runtime.gatewayTransferSpool(path)
	if err != nil {
		return nil, err
	}
	return &nodeFileTransferSource{
		nodeInvocationSource: invocations,
		spool:                spool,
		workspace:            cfg.WorkspacePath(),
	}, nil
}

func configuredNodeFileTarget(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	for _, target := range cfg.Execution.Targets {
		if strings.TrimSpace(target.FileProfile) != "" {
			return true
		}
	}
	return false
}

func (source *nodeFileTransferSource) SnapshotUploadArtifact(
	ctx context.Context,
	owner nodes.TransferArtifactOwner,
	transferID string,
	target string,
	profileRevision string,
	expiresAt int64,
	maxBytes int64,
	artifactRef string,
	store media.MediaStore,
	mediaOwner media.MediaOwner,
) (nodes.TransferArtifactRecord, error) {
	if source == nil || source.spool == nil || store == nil {
		return nodes.TransferArtifactRecord{}, errNodeDiscoveryAuthorityUnavailable
	}
	if maxBytes <= 0 || maxBytes > nodes.MaxTransferArtifactBytes {
		return nodes.TransferArtifactRecord{}, nodes.ErrTransferSizeExceeded
	}
	ownedStore, ok := store.(ownedNodeTransferMediaStore)
	if !ok {
		return nodes.TransferArtifactRecord{}, nodes.ErrTransferArtifactNotFound
	}
	localPath, meta, resolveErr := ownedStore.ResolveOwnedWithMeta(
		strings.TrimSpace(artifactRef),
		mediaOwner,
	)
	if resolveErr != nil {
		return nodes.TransferArtifactRecord{}, nodes.ErrTransferArtifactNotFound
	}
	file, initial, openErr := openNodeTransferMedia(localPath)
	if openErr != nil {
		return nodes.TransferArtifactRecord{}, openErr
	}
	defer file.Close()
	digest := sha256.New()
	size, copyErr := io.Copy(digest, io.LimitReader(file, maxBytes+1))
	if copyErr != nil || size > maxBytes {
		return nodes.TransferArtifactRecord{}, nodes.ErrTransferSizeExceeded
	}
	afterHash, statErr := file.Stat()
	if statErr != nil || !os.SameFile(initial, afterHash) || afterHash.Size() != size {
		return nodes.TransferArtifactRecord{}, errors.New("gateway upload artifact changed while hashing")
	}
	if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
		return nodes.TransferArtifactRecord{}, seekErr
	}
	spec := nodes.TransferArtifactSpec{
		TransferID:      transferID,
		Direction:       nodes.TransferDirectionUpload,
		Target:          target,
		ProfileRevision: profileRevision,
		Filename:        safeNodeTransferFilename(meta.Filename, localPath),
		ContentType:     safeNodeTransferContentType(meta.ContentType),
		DeclaredSize:    size,
		SHA256:          hex.EncodeToString(digest.Sum(nil)),
		ExpiresAt:       expiresAt,
	}
	writer, retained, created, beginErr := source.spool.Begin(owner, spec)
	if beginErr != nil || !created {
		return retained, beginErr
	}
	committed := false
	defer func() {
		if !committed {
			_ = writer.Abort()
		}
	}()
	reader := bufio.NewReaderSize(file, nodes.MaxTransferArtifactChunkBytes)
	buffer := make([]byte, nodes.MaxTransferArtifactChunkBytes)
	var sequence uint64
	for {
		count, readErr := reader.Read(buffer)
		if count > 0 {
			sequence++
			if writeErr := writer.WriteChunk(sequence, buffer[:count]); writeErr != nil {
				return nodes.TransferArtifactRecord{}, writeErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nodes.TransferArtifactRecord{}, readErr
		}
		select {
		case <-ctx.Done():
			return nodes.TransferArtifactRecord{}, ctx.Err()
		default:
		}
	}
	finalInfo, finalStatErr := file.Stat()
	if finalStatErr != nil || !os.SameFile(initial, finalInfo) || finalInfo.Size() != size {
		return nodes.TransferArtifactRecord{}, errors.New("gateway upload artifact changed while snapshotting")
	}
	retained, commitErr := writer.Commit()
	if commitErr == nil || fileutil.IsCommittedWriteError(commitErr) {
		committed = true
	}
	return retained, commitErr
}

func (source *nodeFileTransferSource) InspectFile(
	ctx context.Context,
	nodeID nodes.ID,
	binding tools.NodeFileTransferBinding,
) (tools.NodeFileTransferResult, error) {
	stream, frame, openErr := source.openTransfer(ctx, nodeID, binding)
	if openErr != nil {
		return tools.NodeFileTransferResult{}, openErr
	}
	defer stream.Close()
	payload, marshalErr := json.Marshal(nodeFileTransferPrepare{
		Operation: "info",
		Path:      binding.Path,
		ExpiresAt: binding.ExpiresAt,
	})
	if marshalErr != nil {
		return tools.NodeFileTransferResult{}, marshalErr
	}
	frame.Type = protocol.TransferFramePrepare
	frame.Payload = payload
	if sendErr := stream.Send(ctx, frame); sendErr != nil {
		return tools.NodeFileTransferResult{}, sendErr
	}
	response, receiveErr := stream.Receive(ctx)
	if receiveErr != nil {
		return tools.NodeFileTransferResult{}, receiveErr
	}
	return decodeNodeFileTransferResponse(response)
}

func (source *nodeFileTransferSource) DispatchFileTransfer(
	ctx context.Context,
	owner nodes.GatewayInvocationOwner,
	record nodes.GatewayInvocationRecord,
) (tools.NodeFileTransferResult, bool, error) {
	if source == nil || source.spool == nil {
		return tools.NodeFileTransferResult{}, false, errNodeDiscoveryAuthorityUnavailable
	}
	if record.State == nodes.GatewayInvocationDispatched {
		return tools.NodeFileTransferResult{}, true, nodes.ErrGatewayInvocationDispatched
	}
	input, binding, err := retainedNodeFileTransfer(record)
	if err != nil {
		return tools.NodeFileTransferResult{}, false, err
	}
	if !gatewayInvocationMatchesOwner(record, owner) {
		return tools.NodeFileTransferResult{}, false, nodes.ErrGatewayInvocationConflict
	}
	switch record.Plan.Command {
	case "file.info.v1":
		return source.dispatchFileInfo(ctx, owner, record, input, binding)
	case "file.upload.v1":
		return source.dispatchFileUpload(ctx, owner, record, input, binding)
	case "file.download.v1":
		return source.dispatchFileDownload(ctx, owner, record, input, binding)
	default:
		return tools.NodeFileTransferResult{}, false, nodes.ErrGatewayInvocationConflict
	}
}

func (source *nodeFileTransferSource) dispatchFileInfo(
	ctx context.Context,
	owner nodes.GatewayInvocationOwner,
	record nodes.GatewayInvocationRecord,
	input nodeFileTransferPlanInput,
	binding tools.NodeFileTransferBinding,
) (tools.NodeFileTransferResult, bool, error) {
	stream, frame, openErr := source.openTransfer(ctx, record.Plan.NodeID, binding)
	if openErr != nil {
		return tools.NodeFileTransferResult{}, false, openErr
	}
	defer stream.Close()
	if dispatched, dispatchErr := source.markFileTransferDispatched(owner, record); dispatchErr != nil {
		return tools.NodeFileTransferResult{}, dispatched, dispatchErr
	}
	payload, marshalErr := json.Marshal(nodeFileTransferPrepare{
		Operation: "info",
		Path:      input.Path,
		ExpiresAt: record.Plan.ExpiresAt,
	})
	if marshalErr != nil {
		return tools.NodeFileTransferResult{}, true, marshalErr
	}
	frame.Type = protocol.TransferFramePrepare
	frame.Payload = payload
	if sendErr := stream.Send(ctx, frame); sendErr != nil {
		return tools.NodeFileTransferResult{}, true, sendErr
	}
	response, receiveErr := stream.Receive(ctx)
	if receiveErr != nil {
		return tools.NodeFileTransferResult{}, true, receiveErr
	}
	result, decodeErr := decodeNodeFileTransferResponse(response)
	return result, true, decodeErr
}

func (source *nodeFileTransferSource) dispatchFileUpload(
	ctx context.Context,
	owner nodes.GatewayInvocationOwner,
	record nodes.GatewayInvocationRecord,
	input nodeFileTransferPlanInput,
	binding tools.NodeFileTransferBinding,
) (tools.NodeFileTransferResult, bool, error) {
	artifactOwner, ownerErr := transferArtifactOwnerFromRecord(record, input.RouteID)
	if ownerErr != nil {
		return tools.NodeFileTransferResult{}, false, ownerErr
	}
	file, artifact, resolveErr := source.spool.ResolveOwned(artifactOwner, input.ArtifactRef)
	if resolveErr != nil {
		return tools.NodeFileTransferResult{}, false, resolveErr
	}
	defer file.Close()
	if artifact.Spec.TransferID != record.Plan.InvocationID ||
		artifact.Spec.Direction != nodes.TransferDirectionUpload ||
		uint64(artifact.Spec.DeclaredSize) != binding.TotalSize ||
		artifact.Spec.SHA256 != input.SHA256 {
		return tools.NodeFileTransferResult{}, false, nodes.ErrTransferArtifactConflict
	}
	stream, frame, openErr := source.openTransfer(ctx, record.Plan.NodeID, binding)
	if openErr != nil {
		return tools.NodeFileTransferResult{}, false, openErr
	}
	defer stream.Close()
	if dispatched, dispatchErr := source.markFileTransferDispatched(owner, record); dispatchErr != nil {
		return tools.NodeFileTransferResult{}, dispatched, dispatchErr
	}
	payload, marshalErr := json.Marshal(nodeFileTransferPrepare{
		Operation:   "upload",
		Path:        input.Destination,
		Publication: input.Publication,
		ExpiresAt:   record.Plan.ExpiresAt,
	})
	if marshalErr != nil {
		return tools.NodeFileTransferResult{}, true, marshalErr
	}
	frame.Type = protocol.TransferFramePrepare
	frame.Payload = payload
	if sendErr := stream.Send(ctx, frame); sendErr != nil {
		return tools.NodeFileTransferResult{}, true, sendErr
	}
	response, receiveErr := stream.Receive(ctx)
	if receiveErr != nil {
		return tools.NodeFileTransferResult{}, true, receiveErr
	}
	if response.Type != protocol.TransferFrameAccept {
		result, decodeErr := decodeNodeFileTransferResponse(response)
		return result, true, decodeErr
	}
	buffer := make([]byte, nodes.MaxTransferArtifactChunkBytes)
	var sequence uint64
	for {
		count, readErr := file.Read(buffer)
		if count > 0 {
			sequence++
			chunk := frame
			chunk.Type = protocol.TransferFrameChunk
			chunk.Sequence = sequence
			chunk.Payload = append([]byte(nil), buffer[:count]...)
			if sendErr := stream.Send(ctx, chunk); sendErr != nil {
				source.cancelFileTransferBestEffort(record.Plan.NodeID, binding)
				return tools.NodeFileTransferResult{}, true, sendErr
			}
			ack, ackErr := stream.Receive(ctx)
			if ackErr != nil || ack.Type != protocol.TransferFrameAck || ack.Sequence != sequence {
				source.cancelFileTransferBestEffort(record.Plan.NodeID, binding)
				if ackErr == nil {
					ackErr = protocol.ErrInvalidTransferFrame
				}
				return tools.NodeFileTransferResult{}, true, ackErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			source.cancelFileTransferBestEffort(record.Plan.NodeID, binding)
			return tools.NodeFileTransferResult{}, true, readErr
		}
	}
	commit := frame
	commit.Type = protocol.TransferFrameCommit
	commit.Payload = nil
	if sendErr := stream.Send(ctx, commit); sendErr != nil {
		return tools.NodeFileTransferResult{}, true, sendErr
	}
	response, receiveErr = stream.Receive(ctx)
	if receiveErr != nil {
		return tools.NodeFileTransferResult{}, true, receiveErr
	}
	result, decodeErr := decodeNodeFileTransferResponse(response)
	return result, true, decodeErr
}

func (source *nodeFileTransferSource) dispatchFileDownload(
	ctx context.Context,
	owner nodes.GatewayInvocationOwner,
	record nodes.GatewayInvocationRecord,
	input nodeFileTransferPlanInput,
	binding tools.NodeFileTransferBinding,
) (tools.NodeFileTransferResult, bool, error) {
	artifactOwner, ownerErr := transferArtifactOwnerFromRecord(record, input.RouteID)
	if ownerErr != nil {
		return tools.NodeFileTransferResult{}, false, ownerErr
	}
	spec := nodes.TransferArtifactSpec{
		TransferID:      record.Plan.InvocationID,
		Direction:       nodes.TransferDirectionDownload,
		Target:          record.Target,
		ProfileRevision: record.Plan.PolicyRevision,
		Filename:        input.Filename,
		ContentType:     input.ContentType,
		DeclaredSize:    int64(binding.TotalSize),
		SHA256:          input.SHA256,
		ExpiresAt:       record.Plan.ExpiresAt,
	}
	writer, retained, created, beginErr := source.spool.Begin(artifactOwner, spec)
	if beginErr != nil {
		return tools.NodeFileTransferResult{}, false, beginErr
	}
	if !created {
		_, _ = source.markFileTransferDispatched(owner, record)
		return committedNodeDownloadResult(retained), true, nil
	}
	keepWriter := false
	defer func() {
		if !keepWriter {
			_ = writer.Abort()
		}
	}()
	stream, frame, openErr := source.openTransfer(ctx, record.Plan.NodeID, binding)
	if openErr != nil {
		return tools.NodeFileTransferResult{}, false, openErr
	}
	defer stream.Close()
	if dispatched, dispatchErr := source.markFileTransferDispatched(owner, record); dispatchErr != nil {
		return tools.NodeFileTransferResult{}, dispatched, dispatchErr
	}
	payload, marshalErr := json.Marshal(nodeFileTransferPrepare{
		Operation: "download",
		Path:      input.Source,
		ExpiresAt: record.Plan.ExpiresAt,
	})
	if marshalErr != nil {
		return tools.NodeFileTransferResult{}, true, marshalErr
	}
	frame.Type = protocol.TransferFramePrepare
	frame.Payload = payload
	if sendErr := stream.Send(ctx, frame); sendErr != nil {
		return tools.NodeFileTransferResult{}, true, sendErr
	}
	for {
		response, receiveErr := stream.Receive(ctx)
		if receiveErr != nil {
			return tools.NodeFileTransferResult{}, true, receiveErr
		}
		switch response.Type {
		case protocol.TransferFrameAccept:
			continue
		case protocol.TransferFrameChunk:
			if writeErr := writer.WriteChunk(response.Sequence, response.Payload); writeErr != nil {
				source.cancelFileTransferBestEffort(record.Plan.NodeID, binding)
				return tools.NodeFileTransferResult{}, true, writeErr
			}
			ack := frame
			ack.Type = protocol.TransferFrameAck
			ack.Sequence = response.Sequence
			ack.Payload = nil
			if sendErr := stream.Send(ctx, ack); sendErr != nil {
				return tools.NodeFileTransferResult{}, true, sendErr
			}
		case protocol.TransferFrameStatus:
			status, decodeErr := decodeNodeFileTransferResponse(response)
			if decodeErr != nil {
				return tools.NodeFileTransferResult{}, true, decodeErr
			}
			if status.State != "received" {
				return status, true, nil
			}
			retained, commitErr := writer.Commit()
			if commitErr != nil {
				if fileutil.IsCommittedWriteError(commitErr) {
					keepWriter = true
				}
				return tools.NodeFileTransferResult{}, true, commitErr
			}
			keepWriter = true
			commit := frame
			commit.Type = protocol.TransferFrameCommit
			commit.Payload = nil
			if sendErr := stream.Send(ctx, commit); sendErr == nil {
				_, _ = stream.Receive(ctx)
			}
			return committedNodeDownloadResult(retained), true, nil
		case protocol.TransferFrameDeny,
			protocol.TransferFrameFailure,
			protocol.TransferFrameCommitted:
			result, decodeErr := decodeNodeFileTransferResponse(response)
			return result, true, decodeErr
		default:
			return tools.NodeFileTransferResult{}, true, protocol.ErrInvalidTransferFrame
		}
	}
}

func (source *nodeFileTransferSource) QueryFileTransfer(
	ctx context.Context,
	principal nodes.GatewayInvocationPrincipal,
	record nodes.GatewayInvocationRecord,
) (tools.NodeFileTransferResult, error) {
	if source == nil || source.spool == nil ||
		record.State != nodes.GatewayInvocationDispatched {
		return tools.NodeFileTransferResult{}, nodes.ErrGatewayInvocationConflict
	}
	retained, found, lookupErr := source.store.Lookup(principal, record.Plan.InvocationID)
	if lookupErr != nil || !found || retained.ExpectedPlanHash != record.ExpectedPlanHash {
		return tools.NodeFileTransferResult{}, nodes.ErrGatewayInvocationConflict
	}
	input, binding, err := retainedNodeFileTransfer(retained)
	if err != nil {
		return tools.NodeFileTransferResult{}, err
	}
	if retained.Plan.Command == "file.download.v1" {
		owner, ownerErr := transferArtifactOwnerFromRecord(retained, input.RouteID)
		if ownerErr != nil {
			return tools.NodeFileTransferResult{}, ownerErr
		}
		artifact, committed, lookupErr := source.spool.LookupTransfer(
			owner,
			retained.Plan.InvocationID,
		)
		if lookupErr != nil {
			return tools.NodeFileTransferResult{}, lookupErr
		}
		if committed && artifact.State == nodes.TransferArtifactCommitted {
			return committedNodeDownloadResult(artifact), nil
		}
	}
	return source.queryRemoteFileTransfer(ctx, retained.Plan.NodeID, binding)
}

func (source *nodeFileTransferSource) CancelFileTransfer(
	ctx context.Context,
	principal nodes.GatewayInvocationPrincipal,
	record nodes.GatewayInvocationRecord,
) (tools.NodeFileTransferResult, bool, error) {
	if source == nil || record.State != nodes.GatewayInvocationDispatched {
		return tools.NodeFileTransferResult{}, false, nodes.ErrGatewayInvocationConflict
	}
	retained, found, err := source.store.Lookup(principal, record.Plan.InvocationID)
	if err != nil || !found || retained.ExpectedPlanHash != record.ExpectedPlanHash {
		return tools.NodeFileTransferResult{}, false, nodes.ErrGatewayInvocationConflict
	}
	input, binding, planErr := retainedNodeFileTransfer(retained)
	if planErr != nil {
		return tools.NodeFileTransferResult{}, false, planErr
	}
	if retained.Plan.Command == "file.download.v1" {
		owner, ownerErr := transferArtifactOwnerFromRecord(retained, input.RouteID)
		if ownerErr != nil {
			return tools.NodeFileTransferResult{}, false, ownerErr
		}
		if artifact, found, lookupErr := source.spool.LookupTransfer(
			owner,
			retained.Plan.InvocationID,
		); lookupErr != nil {
			return tools.NodeFileTransferResult{}, false, lookupErr
		} else if found && artifact.State == nodes.TransferArtifactCommitted {
			result := committedNodeDownloadResult(artifact)
			result.RecoveryAction = "already_committed"
			return result, false, nil
		}
	}
	stream, frame, openErr := source.openTransfer(ctx, retained.Plan.NodeID, binding)
	if openErr != nil {
		return tools.NodeFileTransferResult{}, false, openErr
	}
	defer stream.Close()
	frame.Type = protocol.TransferFrameCancel
	frame.Payload = nil
	if sendErr := stream.Send(ctx, frame); sendErr != nil {
		return tools.NodeFileTransferResult{}, true, sendErr
	}
	response, receiveErr := stream.Receive(ctx)
	if receiveErr != nil {
		return tools.NodeFileTransferResult{}, true, receiveErr
	}
	result, decodeErr := decodeNodeFileTransferResponse(response)
	return result, true, decodeErr
}

func (source *nodeFileTransferSource) HandoffDownloadedArtifact(
	ctx context.Context,
	owner nodes.TransferArtifactOwner,
	ref string,
	store media.MediaStore,
	mediaOwner media.MediaOwner,
) (string, bool, error) {
	if source == nil || source.spool == nil || store == nil {
		return "", false, errNodeDiscoveryAuthorityUnavailable
	}
	idempotentStore, ok := store.(idempotentNodeTransferMediaStore)
	if !ok {
		return "", false, errors.New("persistent idempotent media store is required")
	}
	file, artifact, resolveErr := source.spool.ResolveOwned(owner, ref)
	if resolveErr != nil {
		return "", false, resolveErr
	}
	defer file.Close()
	deliveryKey := nodeFileDeliveryKey(owner, artifact)
	localPath := filepath.Join(
		source.workspace,
		"state",
		"media",
		"node-transfers",
		deliveryKey+".data",
	)
	if copyErr := copyNodeTransferDelivery(ctx, file, artifact, localPath); copyErr != nil {
		return "", false, copyErr
	}
	mediaRef, storeErr := idempotentStore.StoreIdempotentOwned(
		localPath,
		media.MediaMeta{
			Filename:      artifact.Spec.Filename,
			ContentType:   artifact.Spec.ContentType,
			Source:        "tool:nodes_download",
			CleanupPolicy: media.CleanupPolicyDeleteOnCleanup,
		},
		owner.SessionID,
		deliveryKey,
		mediaOwner,
	)
	if storeErr != nil {
		return "", false, storeErr
	}
	_, claimed, err := source.spool.ClaimDelivery(owner, ref, mediaRef, deliveryKey)
	return mediaRef, claimed, err
}

func (source *nodeFileTransferSource) markFileTransferDispatched(
	owner nodes.GatewayInvocationOwner,
	record nodes.GatewayInvocationRecord,
) (bool, error) {
	var transitioned bool
	err := source.runtime.withInvocationHandler(
		source.registryPath,
		source.generation,
		func(nodeAdmissionHandler) error {
			_, changed, markErr := source.store.MarkDispatched(
				owner,
				record.Plan.InvocationID,
				record.ExpectedPlanHash,
			)
			transitioned = changed
			return markErr
		},
	)
	if err != nil {
		if transitioned || fileutil.IsCommittedWriteError(err) {
			return true, errors.Join(nodes.ErrGatewayInvocationDispatched, err)
		}
		return false, err
	}
	if !transitioned {
		return true, nodes.ErrGatewayInvocationDispatched
	}
	return true, nil
}

func (source *nodeFileTransferSource) queryRemoteFileTransfer(
	ctx context.Context,
	nodeID nodes.ID,
	binding tools.NodeFileTransferBinding,
) (tools.NodeFileTransferResult, error) {
	stream, frame, openErr := source.openTransfer(ctx, nodeID, binding)
	if openErr != nil {
		return tools.NodeFileTransferResult{}, openErr
	}
	defer stream.Close()
	frame.Type = protocol.TransferFrameStatus
	frame.Payload = nil
	if sendErr := stream.Send(ctx, frame); sendErr != nil {
		return tools.NodeFileTransferResult{}, sendErr
	}
	response, receiveErr := stream.Receive(ctx)
	if receiveErr != nil {
		return tools.NodeFileTransferResult{}, receiveErr
	}
	return decodeNodeFileTransferResponse(response)
}

func (source *nodeFileTransferSource) cancelFileTransferBestEffort(
	nodeID nodes.ID,
	binding tools.NodeFileTransferBinding,
) {
	ctx, cancel := context.WithTimeout(context.Background(), nodeFileTransferRecoveryTimeout)
	defer cancel()
	stream, frame, err := source.openTransfer(ctx, nodeID, binding)
	if err != nil {
		return
	}
	defer stream.Close()
	frame.Type = protocol.TransferFrameCancel
	frame.Payload = nil
	if stream.Send(ctx, frame) == nil {
		_, _ = stream.Receive(ctx)
	}
}

func (source *nodeFileTransferSource) openTransfer(
	ctx context.Context,
	nodeID nodes.ID,
	binding tools.NodeFileTransferBinding,
) (*nodews.TransferStream, protocol.TransferFrame, error) {
	if source == nil || source.runtime == nil {
		return nil, protocol.TransferFrame{}, errNodeDiscoveryAuthorityUnavailable
	}
	sessions, err := source.runtime.transferSessionsSnapshot(
		source.registryPath,
		source.generation,
	)
	if err != nil {
		return nil, protocol.TransferFrame{}, err
	}
	frame := protocol.TransferFrame{
		Direction:      binding.Direction,
		TransferID:     binding.TransferID,
		PolicyRevision: binding.PolicyRevision,
		TotalSize:      binding.TotalSize,
		SHA256:         binding.SHA256,
	}
	stream, err := sessions.OpenTransfer(ctx, nodeID, nodews.TransferBinding{
		TransferID:     frame.TransferID,
		Direction:      frame.Direction,
		PolicyRevision: frame.PolicyRevision,
		TotalSize:      frame.TotalSize,
		SHA256:         frame.SHA256,
	})
	return stream, frame, err
}

func retainedNodeFileTransfer(
	record nodes.GatewayInvocationRecord,
) (nodeFileTransferPlanInput, tools.NodeFileTransferBinding, error) {
	if err := record.Plan.ValidateAgainstHash(record.ExpectedPlanHash); err != nil {
		return nodeFileTransferPlanInput{}, tools.NodeFileTransferBinding{}, err
	}
	if len(record.Descriptor.FileProfiles) != 1 {
		return nodeFileTransferPlanInput{}, tools.NodeFileTransferBinding{},
			nodes.ErrGatewayInvocationConflict
	}
	var input nodeFileTransferPlanInput
	if err := decodeStrictNodeFileJSON(record.Plan.Input, &input); err != nil {
		return nodeFileTransferPlanInput{}, tools.NodeFileTransferBinding{}, err
	}
	totalSize, err := exactNodeFileTransferSize(input.Size)
	if err != nil {
		return nodeFileTransferPlanInput{}, tools.NodeFileTransferBinding{}, err
	}
	digest, err := decodeNodeFileTransferDigest(input.SHA256)
	if record.Plan.Command == "file.info.v1" {
		digest = sha256.Sum256(nil)
		err = nil
	}
	if err != nil {
		return nodeFileTransferPlanInput{}, tools.NodeFileTransferBinding{}, err
	}
	direction := protocol.TransferDownload
	path := input.Path
	switch record.Plan.Command {
	case "file.info.v1":
	case "file.upload.v1":
		direction = protocol.TransferUpload
		path = input.Destination
	case "file.download.v1":
		path = input.Source
	default:
		return nodeFileTransferPlanInput{}, tools.NodeFileTransferBinding{},
			nodes.ErrGatewayInvocationConflict
	}
	return input, tools.NodeFileTransferBinding{
		TransferID:     record.Plan.InvocationID,
		Direction:      direction,
		ProfileAlias:   record.Descriptor.FileProfiles[0].Alias,
		PolicyRevision: record.Plan.PolicyRevision,
		Path:           path,
		Publication:    input.Publication,
		TotalSize:      totalSize,
		SHA256:         digest,
		ExpiresAt:      record.Plan.ExpiresAt,
		Filename:       input.Filename,
		ContentType:    input.ContentType,
	}, nil
}

func exactNodeFileTransferSize(value float64) (uint64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 ||
		value > float64(nodes.MaxTransferArtifactBytes) || math.Trunc(value) != value {
		return 0, nodes.ErrTransferSizeExceeded
	}
	return uint64(value), nil
}

func transferArtifactOwnerFromRecord(
	record nodes.GatewayInvocationRecord,
	routeID string,
) (nodes.TransferArtifactOwner, error) {
	owner := nodes.TransferArtifactOwner{
		WorkspaceID: record.WorkspaceID,
		AgentID:     record.Plan.AgentID,
		ActorID:     record.Plan.ActorID,
		RouteID:     routeID,
		SessionID:   record.Plan.SessionID,
		ToolCallID:  record.ToolCallID,
	}
	return owner, owner.Validate()
}

func decodeNodeFileTransferResponse(
	frame protocol.TransferFrame,
) (tools.NodeFileTransferResult, error) {
	switch frame.Type {
	case protocol.TransferFrameAccept,
		protocol.TransferFrameDeny,
		protocol.TransferFrameCommitted,
		protocol.TransferFrameStatus,
		protocol.TransferFrameFailure:
	default:
		return tools.NodeFileTransferResult{}, protocol.ErrInvalidTransferFrame
	}
	var result tools.NodeFileTransferResult
	if err := decodeStrictNodeFileJSON(frame.Payload, &result); err != nil {
		return tools.NodeFileTransferResult{}, protocol.ErrInvalidTransferFrame
	}
	if !validNodeFileTransferState(result.State) ||
		!validNodeFileTransferCode(result.Code) ||
		!validNodeFileTransferDigest(result.SHA256) ||
		(result.Type != "" && result.Type != "regular_file") ||
		result.Size > uint64(nodes.MaxTransferArtifactBytes) ||
		result.Transferred > result.Size ||
		result.Mode&^uint32(0o777) != 0 ||
		result.ModifiedAt < 0 ||
		result.TransferID != "" ||
		result.Path != "" ||
		result.ArtifactRef != "" ||
		result.Filename != "" ||
		result.ContentType != "" ||
		result.PolicyRevision != "" ||
		result.DeliveryState != "" ||
		result.RecoveryAction != "" {
		return tools.NodeFileTransferResult{}, protocol.ErrInvalidTransferFrame
	}
	result.TransferID = frame.TransferID
	return result, nil
}

func validNodeFileTransferState(state string) bool {
	switch state {
	case "accepted",
		"streaming",
		"staged",
		"commit_requested",
		"published",
		"received",
		"committed",
		"failed",
		"canceled",
		"expired",
		"unknown":
		return true
	default:
		return false
	}
}

func validNodeFileTransferCode(code string) bool {
	if len(code) > 64 {
		return false
	}
	for _, character := range code {
		if character != '_' && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func validNodeFileTransferDigest(value string) bool {
	if value == "" {
		return true
	}
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := decodeNodeFileTransferDigest(value)
	return err == nil
}

func decodeStrictNodeFileJSON(data []byte, target any) error {
	if len(data) == 0 || len(data) > protocol.MaxTransferMetadataBytes {
		return protocol.ErrInvalidTransferFrame
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return protocol.ErrInvalidTransferFrame
	}
	return nil
}

func decodeNodeFileTransferDigest(value string) ([sha256.Size]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, nodes.ErrTransferDigestMismatch
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return digest, nil
}

func committedNodeDownloadResult(
	artifact nodes.TransferArtifactRecord,
) tools.NodeFileTransferResult {
	return tools.NodeFileTransferResult{
		TransferID:  artifact.Spec.TransferID,
		State:       "committed",
		Size:        uint64(artifact.Spec.DeclaredSize),
		SHA256:      artifact.Spec.SHA256,
		ArtifactRef: artifact.Ref,
		Filename:    artifact.Spec.Filename,
		ContentType: artifact.Spec.ContentType,
	}
}

func safeNodeTransferFilename(configured string, localPath string) string {
	name := strings.TrimSpace(configured)
	if name == "" {
		name = filepath.Base(localPath)
	}
	if name == "" ||
		name == "." ||
		name == string(filepath.Separator) ||
		len(name) > 255 ||
		!utf8.ValidString(name) ||
		strings.ContainsAny(name, `/\`) {
		return "artifact.bin"
	}
	return name
}

func safeNodeTransferContentType(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 255 || !utf8.ValidString(value) {
		return ""
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return ""
		}
	}
	return value
}

func nodeFileDeliveryKey(
	owner nodes.TransferArtifactOwner,
	artifact nodes.TransferArtifactRecord,
) string {
	sum := sha256.Sum256([]byte(
		owner.WorkspaceID + "\x00" +
			owner.AgentID + "\x00" +
			owner.ActorID + "\x00" +
			owner.RouteID + "\x00" +
			owner.SessionID + "\x00" +
			owner.ToolCallID + "\x00" +
			artifact.Spec.TransferID + "\x00" +
			artifact.Ref,
	))
	return "delivery_" + hex.EncodeToString(sum[:16])
}

func copyNodeTransferDelivery(
	ctx context.Context,
	source *os.File,
	artifact nodes.TransferArtifactRecord,
	destination string,
) error {
	if source == nil {
		return nodes.ErrTransferArtifactNotFound
	}
	directory := filepath.Dir(destination)
	if mkdirErr := os.MkdirAll(directory, 0o700); mkdirErr != nil {
		return mkdirErr
	}
	directoryInfo, lstatErr := os.Lstat(directory)
	if lstatErr != nil ||
		!directoryInfo.IsDir() ||
		directoryInfo.Mode()&os.ModeSymlink != 0 ||
		directoryInfo.Mode().Perm()&0o077 != 0 {
		return errors.New("node transfer delivery directory is not private")
	}
	if existing, _, existingErr := openNodeTransferMedia(destination); existingErr == nil {
		defer existing.Close()
		return verifyNodeTransferDelivery(existing, artifact)
	} else if !errors.Is(existingErr, os.ErrNotExist) {
		return existingErr
	}
	temp, createErr := os.CreateTemp(directory, ".node-transfer-*")
	if createErr != nil {
		return createErr
	}
	tempName := temp.Name()
	removeTemp := true
	defer func() {
		_ = temp.Close()
		if removeTemp {
			_ = os.Remove(tempName)
		}
	}()
	if chmodErr := temp.Chmod(0o600); chmodErr != nil {
		return chmodErr
	}
	hasher := sha256.New()
	written, copyErr := io.Copy(
		io.MultiWriter(temp, hasher),
		&contextBoundedReader{
			ctx:       ctx,
			reader:    source,
			remaining: artifact.Spec.DeclaredSize,
		},
	)
	if copyErr != nil ||
		written != artifact.Spec.DeclaredSize ||
		hex.EncodeToString(hasher.Sum(nil)) != artifact.Spec.SHA256 {
		if copyErr == nil {
			copyErr = nodes.ErrTransferDigestMismatch
		}
		return copyErr
	}
	var extra [1]byte
	if count, readErr := source.Read(extra[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return nodes.ErrTransferSizeExceeded
	}
	if syncErr := temp.Sync(); syncErr != nil {
		return syncErr
	}
	if closeErr := temp.Close(); closeErr != nil {
		return closeErr
	}
	if renameErr := os.Rename(tempName, destination); renameErr != nil {
		return renameErr
	}
	removeTemp = false
	return syncNodeTransferDirectory(directory)
}

func verifyNodeTransferDelivery(
	file *os.File,
	artifact nodes.TransferArtifactRecord,
) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() ||
		info.Size() != artifact.Spec.DeclaredSize {
		return nodes.ErrTransferArtifactConflict
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return err
	}
	if hex.EncodeToString(digest.Sum(nil)) != artifact.Spec.SHA256 {
		return nodes.ErrTransferDigestMismatch
	}
	return nil
}

type contextBoundedReader struct {
	ctx       context.Context
	reader    io.Reader
	remaining int64
}

func (reader *contextBoundedReader) Read(buffer []byte) (int, error) {
	select {
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	default:
	}
	if reader.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(buffer)) > reader.remaining {
		buffer = buffer[:reader.remaining]
	}
	count, err := reader.reader.Read(buffer)
	reader.remaining -= int64(count)
	return count, err
}
