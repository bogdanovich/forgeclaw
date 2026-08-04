package gateway

import (
	"bufio"
	"context"
	"errors"
	"io"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

const browserDownloadSourceKind = "browser_download"

func (source *gatewayBrowserToolSource) resolveBrowserUpload(
	ctx context.Context,
	request browser.PrepareActionRequest,
) (browser.UploadBinding, error) {
	if source == nil || source.services == nil || source.services.NodeAdmission == nil || source.workspace == "" {
		return browser.UploadBinding{}, browser.ErrWorkerUnavailable
	}
	owner, err := tools.RoutedNodeFileArtifactOwner(ctx, request.RequestID)
	if err != nil {
		return browser.UploadBinding{}, browser.ErrDenied
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(nodes.GatewayTransferSpoolPath(source.workspace))
	if err != nil {
		return browser.UploadBinding{}, browser.ErrWorkerUnavailable
	}
	file, record, err := spool.ResolveRoutedDownload(owner, request.Action.ArtifactRef)
	if err != nil {
		return browser.UploadBinding{}, browser.ErrDenied
	}
	defer file.Close()
	if record.Spec.DeclaredSize < 1 || record.Spec.DeclaredSize > int64(source.browserLimits().UploadBytes) ||
		verifyNodeTransferDelivery(file, record) != nil {
		return browser.UploadBinding{}, browser.ErrDenied
	}
	return browser.UploadBinding{
		Ref: request.Action.ArtifactRef, SHA256: record.Spec.SHA256, Size: record.Spec.DeclaredSize,
		Filename:    safeNodeTransferFilename(record.Spec.Filename, "artifact.bin"),
		ContentType: safeNodeTransferContentType(record.Spec.ContentType), Path: file.Name(),
	}, nil
}

func (source *gatewayBrowserToolSource) browserLimits() config.BrowserLimitsConfig {
	if source == nil || source.services == nil {
		return config.BrowserLimitsConfig{}.Effective()
	}
	return source.limits.Effective()
}

func (source *gatewayBrowserToolSource) retainBrowserDownload(
	ctx context.Context,
	prepared browser.PreparedAction,
	download browser.DriverDownload,
) (browser.DownloadArtifact, error) {
	if source == nil || source.services == nil || source.services.NodeAdmission == nil || source.workspace == "" ||
		download.Size < 1 || download.Size > int64(source.browserLimits().DownloadBytes) {
		return browser.DownloadArtifact{}, browser.ErrWorkerUnavailable
	}
	owner, mediaOwner, err := browserScreenshotOwners(ctx, source.workspace, prepared.SessionID, prepared.RequestID)
	if err != nil {
		return browser.DownloadArtifact{}, browser.ErrDenied
	}
	file, info, err := openNodeTransferMedia(download.Path)
	if err != nil {
		return browser.DownloadArtifact{}, browser.ErrDriverIncompatible
	}
	defer file.Close()
	if !info.Mode().IsRegular() || info.Size() != download.Size {
		return browser.DownloadArtifact{}, nodes.ErrTransferArtifactConflict
	}
	filename := safeNodeTransferFilename(download.Filename, download.Path)
	contentType := safeNodeTransferContentType(download.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	spec := nodes.TransferArtifactSpec{
		TransferID: prepared.RequestID, Direction: nodes.TransferDirectionDownload,
		Target: prepared.Target, ProfileRevision: prepared.PolicyRevision,
		SourceKind: browserDownloadSourceKind, SourceScope: prepared.TabID,
		SourceID: prepared.ID, SourceRevision: prepared.SnapshotGeneration,
		Filename: filename, ContentType: contentType, DeclaredSize: download.Size,
		SHA256: download.SHA256, ExpiresAt: time.Now().Add(source.screenshotRetention).Unix(),
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(nodes.GatewayTransferSpoolPath(source.workspace))
	if err != nil {
		return browser.DownloadArtifact{}, browser.ErrWorkerUnavailable
	}
	record, found, err := spool.LookupTransfer(owner, prepared.RequestID)
	if err != nil {
		return browser.DownloadArtifact{}, err
	}
	if !found {
		writer, begun, created, beginErr := spool.Begin(owner, spec)
		if beginErr != nil && !(created && writer != nil && fileutil.IsCommittedWriteError(beginErr)) {
			if writer != nil {
				_ = writer.Abort()
			}
			return browser.DownloadArtifact{}, beginErr
		}
		record = begun
		if created {
			if writer == nil {
				return browser.DownloadArtifact{}, browser.ErrWorkerUnavailable
			}
			if _, err = file.Seek(0, io.SeekStart); err != nil {
				_ = writer.Abort()
				return browser.DownloadArtifact{}, err
			}
			reader := bufio.NewReaderSize(file, nodes.MaxTransferArtifactChunkBytes)
			buffer := make([]byte, nodes.MaxTransferArtifactChunkBytes)
			for sequence := uint64(1); ; sequence++ {
				count, readErr := reader.Read(buffer)
				if count > 0 {
					if err = writer.WriteChunk(sequence, buffer[:count]); err != nil {
						_ = writer.Abort()
						return browser.DownloadArtifact{}, err
					}
				}
				if errors.Is(readErr, io.EOF) {
					break
				}
				if readErr != nil {
					_ = writer.Abort()
					return browser.DownloadArtifact{}, readErr
				}
			}
			record, err = writer.Commit()
			if err != nil && !(record.State == nodes.TransferArtifactCommitted && fileutil.IsCommittedWriteError(err)) {
				_ = writer.Abort()
				return browser.DownloadArtifact{}, err
			}
		}
	}
	if !validBrowserDownloadRecord(record) || record.Spec.SHA256 != download.SHA256 ||
		record.Spec.DeclaredSize != download.Size {
		return browser.DownloadArtifact{}, nodes.ErrTransferArtifactConflict
	}
	mediaRef := record.MediaRef
	if prepared.Action.Deliver && mediaRef == "" {
		mediaRef, err = source.registerBrowserDownload(ctx, spool, owner, record, mediaOwner)
		if err != nil {
			return browser.DownloadArtifact{}, err
		}
	}
	return browserDownloadArtifact(record, mediaRef, prepared.Action.Deliver), nil
}

func (source *gatewayBrowserToolSource) lookupBrowserDownload(
	ctx context.Context, _ browser.Owner, requestID, sessionID string, deliver bool,
) (browser.DownloadArtifact, bool, error) {
	owner, mediaOwner, err := browserScreenshotOwners(ctx, source.workspace, sessionID, requestID)
	if err != nil {
		return browser.DownloadArtifact{}, false, browser.ErrDenied
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(nodes.GatewayTransferSpoolPath(source.workspace))
	if err != nil {
		return browser.DownloadArtifact{}, false, err
	}
	record, found, err := spool.LookupTransfer(owner, requestID)
	if err != nil || !found || !validBrowserDownloadRecord(record) {
		return browser.DownloadArtifact{}, false, err
	}
	mediaRef := record.MediaRef
	if deliver {
		mediaRef, err = source.registerBrowserDownload(ctx, spool, owner, record, mediaOwner)
		if err != nil {
			return browser.DownloadArtifact{}, false, err
		}
	}
	return browserDownloadArtifact(record, mediaRef, deliver), true, nil
}

func validBrowserDownloadRecord(record nodes.TransferArtifactRecord) bool {
	return record.State == nodes.TransferArtifactCommitted &&
		record.Spec.Direction == nodes.TransferDirectionDownload &&
		record.Spec.SourceKind == browserDownloadSourceKind
}

func (source *gatewayBrowserToolSource) committedBrowserDownload(
	ctx context.Context,
	prepared browser.PreparedAction,
) (bool, error) {
	if source == nil || source.services == nil || source.services.NodeAdmission == nil || source.workspace == "" {
		return false, browser.ErrWorkerUnavailable
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(
		nodes.GatewayTransferSpoolPath(source.workspace),
	)
	if err != nil {
		return false, err
	}
	record, found, err := spool.LookupCommittedSource(browserDownloadSourceKind, prepared.ID)
	if err != nil || !found {
		return false, err
	}
	return validBrowserDownloadRecord(record) &&
		record.Spec.TransferID == prepared.RequestID &&
		record.Spec.Target == prepared.Target &&
		record.Spec.ProfileRevision == prepared.PolicyRevision &&
		record.Spec.SourceScope == prepared.TabID &&
		record.Spec.SourceRevision == prepared.SnapshotGeneration, nil
}

func (source *gatewayBrowserToolSource) registerBrowserDownload(
	ctx context.Context, spool *nodes.GatewayTransferSpool, owner nodes.TransferArtifactOwner,
	artifact nodes.TransferArtifactRecord, mediaOwner media.MediaOwner,
) (string, error) {
	idempotentStore, ok := source.services.MediaStore.(idempotentNodeTransferMediaStore)
	if !ok {
		return "", errors.New("persistent idempotent media store is required")
	}
	file, retained, err := spool.ResolveOwned(owner, artifact.Ref)
	if err != nil {
		return "", err
	}
	defer file.Close()
	deliveryKey := nodeFileDeliveryKey(owner, retained)
	localPath, _, err := copyNodeTransferDeliveryTracked(ctx, file, retained, source.workspace, deliveryKey+".data")
	if err != nil {
		return "", err
	}
	return idempotentStore.StoreIdempotentOwned(localPath, media.MediaMeta{
		Filename: artifact.Spec.Filename, ContentType: artifact.Spec.ContentType,
		Source: "tool:browser_act", CleanupPolicy: media.CleanupPolicyDeleteOnCleanup,
	}, owner.SessionID, deliveryKey, mediaOwner)
}

func browserDownloadArtifact(
	record nodes.TransferArtifactRecord,
	mediaRef string,
	deliver bool,
) browser.DownloadArtifact {
	state := browser.ScreenshotDeliveryPending
	if record.DeliveryAt != 0 {
		state = browser.ScreenshotDeliveryAlreadyClaimed
	}
	return browser.DownloadArtifact{
		Ref: record.Ref, Kind: "download", ContentType: record.Spec.ContentType,
		Filename: record.Spec.Filename, Size: record.Spec.DeclaredSize, SHA256: record.Spec.SHA256,
		ExpiresAt: record.Spec.ExpiresAt, SessionID: record.Owner.SessionID, TabID: record.Spec.SourceScope,
		Generation: record.Spec.SourceRevision, Deliver: deliver, DeliveryState: state, MediaRef: mediaRef,
		Recovery: &browser.ScreenshotRecovery{
			WorkspaceID: record.Owner.WorkspaceID, AgentID: record.Owner.AgentID, ActorID: record.Owner.ActorID,
			RouteID: record.Owner.RouteID, SessionID: record.Owner.SessionID, ToolCallID: record.Owner.ToolCallID,
		},
	}
}

func (source *gatewayBrowserToolSource) ClaimDownloadDelivery(
	_ context.Context, request browser.DownloadDeliveryRequest,
) error {
	if request.Recovery == nil {
		return browser.ErrDenied
	}
	owner, err := browserScreenshotRecoveryOwner(*request.Recovery, request.SessionID, request.RequestID)
	if err != nil {
		return err
	}
	spool, err := source.services.NodeAdmission.gatewayTransferSpool(nodes.GatewayTransferSpoolPath(source.workspace))
	if err != nil {
		return err
	}
	record, found, err := spool.LookupTransfer(owner, request.RequestID)
	if err != nil || !found || record.Ref != request.Ref || !validBrowserDownloadRecord(record) {
		return nodes.ErrTransferArtifactNotFound
	}
	claimedRecord, claimed, claimErr := spool.ClaimDelivery(
		owner, request.Ref, request.MediaRef, nodeFileDeliveryKey(owner, record),
	)
	if claimErr != nil && !(claimed && claimedRecord.DeliveryAt != 0 && fileutil.IsCommittedWriteError(claimErr)) {
		return claimErr
	}
	return nil
}

func recoverBrowserDownloadDelivery(
	runtime *nodeAdmissionRuntime, workspace string, recovery bus.OutboundRecovery,
) error {
	if runtime == nil || recovery.Kind != bus.OutboundRecoveryBrowserDownload {
		return errors.New("browser download recovery is unavailable")
	}
	owner := nodes.TransferArtifactOwner{
		WorkspaceID: recovery.WorkspaceID, AgentID: recovery.AgentID, ActorID: recovery.ActorID,
		RouteID: recovery.RouteID, SessionID: recovery.SessionID, ToolCallID: recovery.ToolCallID,
	}
	spool, err := runtime.gatewayTransferSpool(nodes.GatewayTransferSpoolPath(workspace))
	if err != nil {
		return err
	}
	record, found, err := spool.LookupTransfer(owner, owner.ToolCallID)
	if err != nil || !found || record.Ref != recovery.ArtifactRef || !validBrowserDownloadRecord(record) {
		return nodes.ErrTransferArtifactNotFound
	}
	claimedRecord, claimed, claimErr := spool.ClaimDelivery(
		owner, recovery.ArtifactRef, recovery.MediaRef, nodeFileDeliveryKey(owner, record),
	)
	if claimErr != nil && !(claimed && claimedRecord.DeliveryAt != 0 && fileutil.IsCommittedWriteError(claimErr)) {
		return claimErr
	}
	return nil
}
