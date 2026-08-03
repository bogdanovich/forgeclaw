package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

const browserScreenshotFilename = "browser-screenshot.png"

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
	mediaOwner, err := browserScreenshotMediaOwner(ctx, source.workspace)
	if err != nil {
		return browser.ScreenshotArtifact{}, browser.ErrDenied
	}
	owner := nodes.TransferArtifactOwner{
		WorkspaceID: mediaOwner.WorkspaceID,
		AgentID:     mediaOwner.AgentID,
		ActorID:     mediaOwner.ActorID,
		RouteID:     mediaOwner.RouteID,
		SessionID:   capture.SessionID,
		ToolCallID:  request.RequestID,
	}
	if err = owner.Validate(); err != nil {
		return browser.ScreenshotArtifact{}, browser.ErrDenied
	}
	digest := sha256.Sum256(capture.Data)
	expiresAt := time.Now().Add(source.screenshotRetention).Unix()
	spec := nodes.TransferArtifactSpec{
		TransferID: request.RequestID, Direction: nodes.TransferDirectionDownload,
		Target: capture.Target, ProfileRevision: capture.PolicyRevision,
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
		if err != nil && !errors.Is(err, context.Canceled) {
			return browser.ScreenshotArtifact{}, fmt.Errorf("retain browser screenshot: %w", err)
		}
		if err != nil {
			return browser.ScreenshotArtifact{}, err
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
		if err != nil {
			return browser.ScreenshotArtifact{}, err
		}
	}
	mediaRef, claimed, err := handoffBrowserScreenshot(
		ctx, spool, owner, record, source.services.MediaStore, mediaOwner, source.workspace,
	)
	if err != nil {
		return browser.ScreenshotArtifact{}, err
	}
	deliveryState := "already_claimed"
	if claimed {
		deliveryState = "claimed"
	}
	return browser.ScreenshotArtifact{
		Ref: record.Ref, Kind: "screenshot", ContentType: record.Spec.ContentType,
		Filename: record.Spec.Filename, Size: record.Spec.DeclaredSize,
		SHA256: record.Spec.SHA256, ExpiresAt: record.Spec.ExpiresAt,
		SessionID: capture.SessionID, TabID: capture.TabID,
		SnapshotID:         capture.SnapshotID,
		SnapshotGeneration: capture.SnapshotGeneration,
		DeliveryState:      deliveryState, MediaRef: mediaRef,
	}, nil
}

func sameBrowserScreenshotSpec(existing, requested nodes.TransferArtifactSpec) bool {
	return existing.TransferID == requested.TransferID &&
		existing.Direction == requested.Direction &&
		existing.Target == requested.Target &&
		existing.ProfileRevision == requested.ProfileRevision &&
		existing.Filename == requested.Filename &&
		existing.ContentType == requested.ContentType &&
		existing.DeclaredSize == requested.DeclaredSize &&
		existing.SHA256 == requested.SHA256
}

func handoffBrowserScreenshot(
	ctx context.Context,
	spool *nodes.GatewayTransferSpool,
	owner nodes.TransferArtifactOwner,
	artifact nodes.TransferArtifactRecord,
	store media.MediaStore,
	mediaOwner media.MediaOwner,
	workspace string,
) (string, bool, error) {
	idempotentStore, ok := store.(idempotentNodeTransferMediaStore)
	if !ok {
		return "", false, errors.New("persistent idempotent media store is required")
	}
	file, retained, err := spool.ResolveOwned(owner, artifact.Ref)
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	deliveryKey := nodeFileDeliveryKey(owner, retained)
	localPath, err := copyNodeTransferDelivery(
		ctx, file, retained, workspace, deliveryKey+".data",
	)
	if err != nil {
		return "", false, err
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
		return "", false, err
	}
	claimedRecord, claimed, err := spool.ClaimDelivery(owner, artifact.Ref, mediaRef, deliveryKey)
	if err != nil {
		return "", false, err
	}
	return claimedRecord.MediaRef, claimed, nil
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
