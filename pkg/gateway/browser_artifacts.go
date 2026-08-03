package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

const (
	browserScreenshotFilename   = "browser-screenshot.png"
	browserScreenshotSourceKind = "browser_screenshot"
)

type browserScreenshotCopyFunc func(
	context.Context,
	*os.File,
	nodes.TransferArtifactRecord,
	string,
	string,
) (string, bool, error)

func (source *gatewayBrowserToolSource) LookupScreenshot(
	ctx context.Context,
	owner browser.Owner,
	requestID string,
	sessionID string,
) (browser.ScreenshotArtifact, bool, error) {
	if source == nil || source.services == nil || source.services.NodeAdmission == nil ||
		source.services.MediaStore == nil || source.workspace == "" || owner.Validate() != nil {
		return browser.ScreenshotArtifact{}, false, browser.ErrWorkerUnavailable
	}
	transferOwner, mediaOwner, err := browserScreenshotOwners(
		ctx, source.workspace, sessionID, requestID,
	)
	if err != nil {
		return browser.ScreenshotArtifact{}, false, browser.ErrDenied
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(
		nodes.GatewayTransferSpoolPath(source.workspace),
	)
	if err != nil {
		return browser.ScreenshotArtifact{}, false, browser.ErrWorkerUnavailable
	}
	record, found, err := spool.LookupTransfer(transferOwner, requestID)
	if err != nil || !found {
		return browser.ScreenshotArtifact{}, found, err
	}
	if !validBrowserScreenshotRecord(record) {
		return browser.ScreenshotArtifact{}, false, nodes.ErrTransferArtifactConflict
	}
	if record.DeliveryAt != 0 {
		return browserScreenshotArtifact(record, record.MediaRef), true, nil
	}
	mediaRef, err := source.registerBrowserScreenshot(
		ctx, spool, transferOwner, record, source.services.MediaStore,
		mediaOwner, source.workspace,
	)
	if err != nil {
		return browser.ScreenshotArtifact{}, false, err
	}
	return browserScreenshotArtifact(record, mediaRef), true, nil
}

func (source *gatewayBrowserToolSource) ClaimScreenshotDelivery(
	ctx context.Context,
	request browser.ScreenshotDeliveryRequest,
) error {
	if source == nil || source.services == nil || source.services.NodeAdmission == nil ||
		source.workspace == "" || request.Owner.Validate() != nil {
		return browser.ErrWorkerUnavailable
	}
	owner, _, err := browserScreenshotOwners(
		ctx, source.workspace, request.SessionID, request.RequestID,
	)
	if err != nil {
		return browser.ErrDenied
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(
		nodes.GatewayTransferSpoolPath(source.workspace),
	)
	if err != nil {
		return browser.ErrWorkerUnavailable
	}
	retained, found, err := spool.LookupTransfer(owner, request.RequestID)
	if err != nil || !found || retained.Ref != request.Ref || !validBrowserScreenshotRecord(retained) {
		if err != nil {
			return err
		}
		return nodes.ErrTransferArtifactNotFound
	}
	record, claimed, claimErr := spool.ClaimDelivery(
		owner, request.Ref, request.MediaRef, nodeFileDeliveryKey(owner, retained),
	)
	if claimErr != nil && !(claimed && record.DeliveryAt != 0 && fileutil.IsCommittedWriteError(claimErr)) {
		return claimErr
	}
	if !claimed {
		return browser.ErrConflict
	}
	return nil
}

func validBrowserScreenshotRecord(record nodes.TransferArtifactRecord) bool {
	return record.State == nodes.TransferArtifactCommitted &&
		record.Spec.SourceKind == browserScreenshotSourceKind &&
		record.Spec.Filename == browserScreenshotFilename &&
		record.Spec.ContentType == "image/png"
}

func (source *gatewayBrowserToolSource) CaptureScreenshot(
	ctx context.Context,
	request browser.ScreenshotRequest,
) (browser.ScreenshotArtifact, error) {
	return withGatewayBrowserBroker(
		ctx,
		source,
		func(ctx context.Context, broker *browser.Broker) (browser.ScreenshotArtifact, error) {
			capture, err := broker.CaptureScreenshot(ctx, request)
			if err != nil {
				return browser.ScreenshotArtifact{}, err
			}
			return source.retainScreenshot(ctx, request, capture)
		},
	)
}

func (source *gatewayBrowserToolSource) retainScreenshot(
	ctx context.Context,
	request browser.ScreenshotRequest,
	capture browser.ScreenshotCapture,
) (browser.ScreenshotArtifact, error) {
	if source == nil || source.services == nil || source.services.NodeAdmission == nil ||
		source.services.MediaStore == nil || source.workspace == "" || len(capture.Data) == 0 {
		return browser.ScreenshotArtifact{}, browser.ErrWorkerUnavailable
	}
	owner, mediaOwner, err := browserScreenshotOwners(
		ctx, source.workspace, capture.SessionID, request.RequestID,
	)
	if err != nil {
		return browser.ScreenshotArtifact{}, browser.ErrDenied
	}
	digest := sha256.Sum256(capture.Data)
	expiresAt := time.Now().Add(source.screenshotRetention).Unix()
	spec := nodes.TransferArtifactSpec{
		TransferID: request.RequestID, Direction: nodes.TransferDirectionDownload,
		Target: capture.Target, ProfileRevision: capture.PolicyRevision,
		SourceKind: browserScreenshotSourceKind, SourceScope: capture.TabID,
		SourceID: capture.SnapshotID, SourceRevision: capture.SnapshotGeneration,
		Filename: browserScreenshotFilename, ContentType: capture.ContentType,
		DeclaredSize: int64(len(capture.Data)), SHA256: hex.EncodeToString(digest[:]),
		ExpiresAt: expiresAt,
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(
		nodes.GatewayTransferSpoolPath(source.workspace),
	)
	if err != nil {
		return browser.ScreenshotArtifact{}, browser.ErrWorkerUnavailable
	}
	var writer *nodes.TransferArtifactWriter
	record, found, err := spool.LookupTransfer(owner, request.RequestID)
	if err != nil {
		return browser.ScreenshotArtifact{}, err
	}
	created := false
	if found {
		if !sameBrowserScreenshotSpec(record.Spec, spec) ||
			record.State != nodes.TransferArtifactCommitted {
			return browser.ScreenshotArtifact{}, nodes.ErrTransferArtifactConflict
		}
	} else {
		writer, record, created, err = spool.Begin(owner, spec)
		if err != nil && !(created && writer != nil && fileutil.IsCommittedWriteError(err)) {
			if writer != nil {
				_ = writer.Abort()
			}
			return browser.ScreenshotArtifact{}, fmt.Errorf("retain browser screenshot: %w", err)
		}
	}
	if created {
		if writer == nil {
			return browser.ScreenshotArtifact{}, browser.ErrWorkerUnavailable
		}
		for sequence, offset := uint64(1), 0; offset < len(capture.Data); sequence++ {
			end := offset + nodes.MaxTransferArtifactChunkBytes
			if end > len(capture.Data) {
				end = len(capture.Data)
			}
			if err = writer.WriteChunk(sequence, capture.Data[offset:end]); err != nil {
				_ = writer.Abort()
				return browser.ScreenshotArtifact{}, err
			}
			offset = end
		}
		record, err = writer.Commit()
		if err != nil && !(record.State == nodes.TransferArtifactCommitted &&
			fileutil.IsCommittedWriteError(err)) {
			_ = writer.Abort()
			return browser.ScreenshotArtifact{}, err
		}
	}
	mediaRef, err := source.registerBrowserScreenshot(
		ctx, spool, owner, record, source.services.MediaStore, mediaOwner, source.workspace,
	)
	if err != nil {
		return browser.ScreenshotArtifact{}, err
	}
	return browserScreenshotArtifact(record, mediaRef), nil
}

func sameBrowserScreenshotSpec(existing, requested nodes.TransferArtifactSpec) bool {
	return existing.TransferID == requested.TransferID &&
		existing.Direction == requested.Direction &&
		existing.Target == requested.Target &&
		existing.ProfileRevision == requested.ProfileRevision &&
		existing.SourceKind == requested.SourceKind &&
		existing.SourceScope == requested.SourceScope &&
		existing.SourceID == requested.SourceID &&
		existing.SourceRevision == requested.SourceRevision &&
		existing.Filename == requested.Filename &&
		existing.ContentType == requested.ContentType &&
		existing.DeclaredSize == requested.DeclaredSize &&
		existing.SHA256 == requested.SHA256
}

func (source *gatewayBrowserToolSource) registerBrowserScreenshot(
	ctx context.Context,
	spool *nodes.GatewayTransferSpool,
	owner nodes.TransferArtifactOwner,
	artifact nodes.TransferArtifactRecord,
	store media.MediaStore,
	mediaOwner media.MediaOwner,
	workspace string,
) (string, error) {
	idempotentStore, ok := store.(idempotentNodeTransferMediaStore)
	if !ok {
		return "", errors.New("persistent idempotent media store is required")
	}
	file, retained, err := spool.ResolveOwned(owner, artifact.Ref)
	if err != nil {
		return "", err
	}
	defer file.Close()
	deliveryKey := nodeFileDeliveryKey(owner, retained)
	copyDelivery := copyNodeTransferDeliveryTracked
	if source != nil && source.screenshotCopy != nil {
		copyDelivery = source.screenshotCopy
	}
	localPath, created, err := copyDelivery(
		ctx, file, retained, workspace, deliveryKey+".data",
	)
	if err != nil {
		if created {
			err = errors.Join(err, removeNodeTransferDelivery(workspace, deliveryKey+".data"))
		}
		return "", err
	}
	mediaRef, err := idempotentStore.StoreIdempotentOwned(
		localPath,
		media.MediaMeta{
			Filename: browserScreenshotFilename, ContentType: "image/png",
			Source: "tool:browser_observe", CleanupPolicy: media.CleanupPolicyDeleteOnCleanup,
		},
		owner.SessionID,
		deliveryKey,
		mediaOwner,
	)
	if err != nil {
		if created && !fileutil.IsCommittedWriteError(err) {
			err = errors.Join(err, removeNodeTransferDelivery(workspace, deliveryKey+".data"))
		}
		return "", err
	}
	return mediaRef, nil
}

func browserScreenshotArtifact(
	record nodes.TransferArtifactRecord,
	mediaRef string,
) browser.ScreenshotArtifact {
	deliveryState := browser.ScreenshotDeliveryPending
	if record.DeliveryAt != 0 {
		deliveryState = browser.ScreenshotDeliveryAlreadyClaimed
	}
	return browser.ScreenshotArtifact{
		Ref: record.Ref, Kind: "screenshot", ContentType: record.Spec.ContentType,
		Filename: record.Spec.Filename, Size: record.Spec.DeclaredSize,
		SHA256: record.Spec.SHA256, ExpiresAt: record.Spec.ExpiresAt,
		SessionID: record.Owner.SessionID, TabID: record.Spec.SourceScope,
		SnapshotID:         record.Spec.SourceID,
		SnapshotGeneration: record.Spec.SourceRevision,
		DeliveryState:      deliveryState,
		MediaRef:           mediaRef,
	}
}

func browserScreenshotOwners(
	ctx context.Context,
	workspace string,
	sessionID string,
	requestID string,
) (nodes.TransferArtifactOwner, media.MediaOwner, error) {
	mediaOwner, err := browserScreenshotMediaOwner(ctx, workspace)
	if err != nil {
		return nodes.TransferArtifactOwner{}, media.MediaOwner{}, err
	}
	owner := nodes.TransferArtifactOwner{
		WorkspaceID: mediaOwner.WorkspaceID,
		AgentID:     mediaOwner.AgentID,
		ActorID:     mediaOwner.ActorID,
		RouteID:     mediaOwner.RouteID,
		SessionID:   sessionID,
		ToolCallID:  requestID,
	}
	if err = owner.Validate(); err != nil {
		return nodes.TransferArtifactOwner{}, media.MediaOwner{}, err
	}
	return owner, mediaOwner, nil
}

func browserScreenshotMediaOwner(ctx context.Context, workspace string) (media.MediaOwner, error) {
	actorID := strings.TrimSpace(tools.ToolActorID(ctx))
	if actorID == "" {
		actorID = strings.TrimSpace(tools.ToolSenderID(ctx))
	}
	routeSession := strings.TrimSpace(tools.ToolRouteSessionKey(ctx))
	if routeSession == "" {
		routeSession = strings.TrimSpace(tools.ToolSessionKey(ctx))
	}
	return media.NewMediaOwner(
		workspace, tools.ToolAgentID(ctx), actorID, routeSession,
		tools.ToolChannel(ctx), tools.ToolChatID(ctx), tools.ToolTopicID(ctx),
	)
}
