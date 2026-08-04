package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

func TestGatewayBrowserScreenshotUsesP2SpoolAndIdempotentMediaDelivery(t *testing.T) {
	workspace := t.TempDir()
	store, err := media.NewFileMediaStoreWithPersistentIndex(
		filepath.Join(workspace, "state", "media", "index.json"),
		media.MediaCleanerConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &nodeAdmissionRuntime{}
	t.Cleanup(func() {
		if runtime.transferSpool != nil {
			_ = runtime.transferSpool.Close()
		}
	})
	source := &gatewayBrowserToolSource{
		services:  &services{NodeAdmission: runtime, MediaStore: store},
		workspace: workspace, screenshotRetention: time.Hour,
	}
	data := append(append([]byte(nil), []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}...),
		[]byte("gateway fixture")...)
	request := browser.ScreenshotRequest{RequestID: "request_1"}
	capture := browser.ScreenshotCapture{
		SessionID: "session_1", Target: "gateway", Profile: "managed",
		PolicyRevision: "policy_1", TabID: "tab_primary", SnapshotID: "snapshot_1",
		SnapshotGeneration: 2, Data: data, ContentType: "image/png",
	}
	ctx := gatewayBrowserArtifactContext(workspace)
	artifact, err := source.retainScreenshot(ctx, request, capture)
	if err != nil || artifact.Ref == "" || artifact.MediaRef == "" ||
		artifact.DeliveryState != browser.ScreenshotDeliveryPending || artifact.Size != int64(len(data)) ||
		artifact.SnapshotID != "snapshot_1" || artifact.Truncated {
		t.Fatalf("retainScreenshot() = %+v, %v", artifact, err)
	}
	wantDigest := sha256.Sum256(data)
	if artifact.SHA256 != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("artifact digest = %q", artifact.SHA256)
	}
	path, meta, err := store.ResolveWithMeta(artifact.MediaRef)
	if err != nil || meta.ContentType != "image/png" || meta.Filename != browserScreenshotFilename {
		t.Fatalf("resolved media = %q, %+v, %v", path, meta, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(data) {
		t.Fatalf("media bytes = %q, %v", got, err)
	}
	if runtime.transferSpoolPath != filepath.Join(workspace, "state", "node_transfers") {
		t.Fatalf("transfer spool path = %q", runtime.transferSpoolPath)
	}

	owner := browser.Owner{ActorID: "actor", AgentID: "agent", SessionKey: "route", ExecutionID: "execution"}
	replay, found, err := source.LookupScreenshot(ctx, owner, request.RequestID, capture.SessionID)
	if err != nil || !found || replay.Ref != artifact.Ref || replay.MediaRef != artifact.MediaRef ||
		replay.DeliveryState != browser.ScreenshotDeliveryPending || replay.SnapshotID != artifact.SnapshotID {
		t.Fatalf("pending LookupScreenshot() = %+v, %t, %v", replay, found, err)
	}
	delivery := browser.ScreenshotDeliveryRequest{
		Owner: owner, RequestID: request.RequestID, SessionID: capture.SessionID,
		Ref: artifact.Ref, MediaRef: artifact.MediaRef, Recovery: artifact.Recovery,
	}
	claimCtx := tools.WithToolRouteSessionKey(ctx, "delivery-route-drift")
	if err = source.ClaimScreenshotDelivery(claimCtx, delivery); err != nil {
		t.Fatalf("ClaimScreenshotDelivery() error = %v", err)
	}
	replay, found, err = source.LookupScreenshot(ctx, owner, request.RequestID, capture.SessionID)
	if err != nil || !found || replay.Ref != artifact.Ref || replay.MediaRef != artifact.MediaRef ||
		replay.DeliveryState != browser.ScreenshotDeliveryAlreadyClaimed ||
		replay.SnapshotID != artifact.SnapshotID || replay.SnapshotGeneration != artifact.SnapshotGeneration {
		t.Fatalf("claimed LookupScreenshot() = %+v, %t, %v", replay, found, err)
	}
	if err = source.ClaimScreenshotDelivery(claimCtx, delivery); err != nil {
		t.Fatalf("idempotent ClaimScreenshotDelivery() error = %v", err)
	}
	wrongRecovery := *artifact.Recovery
	wrongRecovery.RouteID = "route_wrong"
	delivery.Recovery = &wrongRecovery
	if err = source.ClaimScreenshotDelivery(claimCtx, delivery); !errors.Is(
		err, nodes.ErrTransferArtifactNotFound,
	) {
		t.Fatalf("wrong recovery owner error = %v", err)
	}
	capture.Data = append(capture.Data, 0)
	if _, err = source.retainScreenshot(ctx, request, capture); err == nil {
		t.Fatal("conflicting replay unexpectedly succeeded")
	}
}

func TestGatewayBrowserDownloadRoundTripsIntoAuthorizedUpload(t *testing.T) {
	workspace := t.TempDir()
	store, err := media.NewFileMediaStoreWithPersistentIndex(
		filepath.Join(workspace, "state", "media", "index.json"), media.MediaCleanerConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &nodeAdmissionRuntime{}
	t.Cleanup(func() {
		if runtime.transferSpool != nil {
			_ = runtime.transferSpool.Close()
		}
	})
	source := &gatewayBrowserToolSource{
		services: &services{NodeAdmission: runtime, MediaStore: store}, workspace: workspace,
		screenshotRetention: time.Hour, limits: config.BrowserLimitsConfig{}.Effective(),
	}
	path := filepath.Join(t.TempDir(), "fixture.txt")
	data := []byte("browser transfer fixture")
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	ctx := gatewayBrowserArtifactContext(workspace)
	prepared := browser.PreparedAction{
		RequestID: "request_download", SessionID: "session_1", Target: "gateway",
		PolicyRevision: "policy_1", TabID: "tab_primary", SnapshotGeneration: 3,
		ID: "prepared_download", Action: browser.Action{Kind: browser.ActionDownload, Deliver: true},
	}
	artifact, err := source.retainBrowserDownload(ctx, prepared, browser.DriverDownload{
		Path: path, Filename: "fixture.txt", ContentType: "text/plain",
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data)),
	})
	if err != nil || artifact.Ref == "" || artifact.MediaRef == "" || artifact.Size != int64(len(data)) ||
		artifact.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("retainBrowserDownload() = %#v, %v", artifact, err)
	}
	binding, err := source.resolveBrowserUpload(ctx, browser.PrepareActionRequest{
		RequestID: "request_upload", SessionID: "session_1",
		Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
	})
	if err != nil || binding.Ref != artifact.Ref || binding.Size != int64(len(data)) ||
		binding.SHA256 != artifact.SHA256 || binding.Path == "" {
		t.Fatalf("resolveBrowserUpload() = %#v, %v", binding, err)
	}
	if _, err = source.resolveBrowserUpload(ctx, browser.PrepareActionRequest{
		RequestID: "request_wrong", SessionID: "session_2",
		Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
	}); !errors.Is(err, browser.ErrDenied) {
		t.Fatalf("cross-session upload error = %v", err)
	}
	if err = source.ClaimDownloadDelivery(ctx, browser.DownloadDeliveryRequest{
		Owner:     browser.Owner{ActorID: "actor", AgentID: "agent", SessionKey: "route", ExecutionID: "execution"},
		RequestID: prepared.RequestID, SessionID: prepared.SessionID,
		Ref: artifact.Ref, MediaRef: artifact.MediaRef, Recovery: artifact.Recovery,
	}); err != nil {
		t.Fatalf("ClaimDownloadDelivery() error = %v", err)
	}
}

func TestGatewayOutboundRecoveryUsesGatewayWorkspaceBeforePublication(t *testing.T) {
	workspace := t.TempDir()
	runtime := &nodeAdmissionRuntime{}
	t.Cleanup(func() {
		if runtime.transferSpool != nil {
			_ = runtime.transferSpool.Close()
		}
	})
	store, err := media.NewFileMediaStoreWithPersistentIndex(
		filepath.Join(workspace, "state", "media", "index.json"),
		media.MediaCleanerConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	source := &gatewayBrowserToolSource{
		services:  &services{NodeAdmission: runtime, MediaStore: store},
		workspace: workspace, screenshotRetention: time.Hour,
	}
	ctx := gatewayBrowserArtifactContext(workspace)
	request := browser.ScreenshotRequest{RequestID: "request_recovery"}
	artifact, err := source.retainScreenshot(ctx, request, browser.ScreenshotCapture{
		SessionID: "session_recovery", Target: "gateway", Profile: "managed",
		PolicyRevision: "policy_1", TabID: "tab_primary", SnapshotID: "snapshot_1",
		SnapshotGeneration: 1,
		Data: append(
			append([]byte(nil), []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}...),
			[]byte("recovery fixture")...,
		),
		ContentType: "image/png",
	})
	if err != nil || artifact.Recovery == nil ||
		artifact.DeliveryState != browser.ScreenshotDeliveryPending {
		t.Fatalf("retainScreenshot() = %+v, %v", artifact, err)
	}
	recovery := &bus.OutboundRecovery{
		Kind:        bus.OutboundRecoveryBrowserScreenshot,
		ArtifactRef: artifact.Ref, MediaRef: artifact.MediaRef,
		WorkspaceID: artifact.Recovery.WorkspaceID, AgentID: artifact.Recovery.AgentID,
		ActorID: artifact.Recovery.ActorID, RouteID: artifact.Recovery.RouteID,
		SessionID: artifact.Recovery.SessionID, ToolCallID: artifact.Recovery.ToolCallID,
	}
	first, err := outbox.OpenCoordinator(workspace)
	if err != nil {
		t.Fatal(err)
	}
	identity := outbox.Identity{
		SourceID: "spool-screenshot-recovery", Kind: outbox.KindMedia,
		Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1",
	}
	ownerWorkspace := filepath.Join(workspace, "agents", "browser")
	admission, err := first.AdmitMedia(ownerWorkspace, identity, bus.OutboundMediaMessage{
		Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1",
		Parts: []bus.MediaPart{{Type: "image", Ref: artifact.MediaRef}}, Recovery: recovery,
	})
	if err != nil || !admission.Dispatch {
		t.Fatalf("AdmitMedia() = %+v, %v", admission, err)
	}
	if admission.Intent.OwnerWorkspace != ownerWorkspace {
		t.Fatalf("OwnerWorkspace = %q, want %q", admission.Intent.OwnerWorkspace, ownerWorkspace)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := outbox.OpenCoordinator(workspace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	admissions, err := recovered.Recover()
	if err != nil || len(admissions) != 1 {
		t.Fatalf("Recover() = %+v, %v", admissions, err)
	}
	msgBus := bus.NewMessageBus()
	reconciler, err := startGatewayOutboundReconciler(
		ctx, recovered, msgBus, admissions, runtime, workspace,
	)
	if err != nil {
		t.Fatalf("startGatewayOutboundReconciler() error = %v", err)
	}
	t.Cleanup(reconciler.stop)
	owner := browser.Owner{ActorID: "actor", AgentID: "agent", SessionKey: "route", ExecutionID: "execution"}
	replayed, found, err := source.LookupScreenshot(ctx, owner, request.RequestID, "session_recovery")
	if err != nil || !found || replayed.DeliveryState != browser.ScreenshotDeliveryAlreadyClaimed {
		t.Fatalf("claimed recovered screenshot = %+v, %t, %v", replayed, found, err)
	}
	select {
	case message := <-msgBus.OutboundMediaChan():
		if message.DeliveryID != admission.Intent.ID || message.Recovery == nil ||
			message.Recovery.ArtifactRef != artifact.Ref {
			t.Fatalf("recovered outbound media = %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered screenshot was not published")
	}
}

func TestGatewayOutboundRecoveryReleasesAdmissionWhenScreenshotClaimFails(t *testing.T) {
	workspace := t.TempDir()
	first, err := outbox.OpenCoordinator(workspace)
	if err != nil {
		t.Fatal(err)
	}
	recovery := &bus.OutboundRecovery{
		Kind:        bus.OutboundRecoveryBrowserScreenshot,
		ArtifactRef: "transfer-artifact://missing",
		MediaRef:    "media://missing",
		WorkspaceID: "workspace",
		AgentID:     "agent",
		ActorID:     "actor",
		RouteID:     "route",
		SessionID:   "session",
		ToolCallID:  "tool-call",
	}
	admission, err := first.AdmitMedia(workspace, outbox.Identity{
		SourceID: "missing-screenshot-recovery", Kind: outbox.KindMedia,
		Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1",
	}, bus.OutboundMediaMessage{
		Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1",
		Parts: []bus.MediaPart{{Type: "image", Ref: recovery.MediaRef}}, Recovery: recovery,
	})
	if err != nil || !admission.Dispatch {
		t.Fatalf("AdmitMedia() = %+v, %v", admission, err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := outbox.OpenCoordinator(workspace)
	if err != nil {
		t.Fatal(err)
	}
	admissions, err := second.Recover()
	if err != nil || len(admissions) != 1 {
		t.Fatalf("Recover() = %+v, %v", admissions, err)
	}
	runtime := &nodeAdmissionRuntime{}
	msgBus := bus.NewMessageBus()
	if _, err = startGatewayOutboundReconciler(
		t.Context(), second, msgBus, admissions, runtime, workspace,
	); err == nil {
		t.Fatal("startGatewayOutboundReconciler() succeeded without the screenshot artifact")
	}
	msgBus.Close()
	if runtime.transferSpool != nil {
		_ = runtime.transferSpool.Close()
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}

	third, err := outbox.OpenCoordinator(workspace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = third.Close() })
	recovered, err := third.Recover()
	if err != nil {
		t.Fatalf("Recover() after prerequisite failure error = %v", err)
	}
	if len(recovered) != 1 || recovered[0].Intent.ID != admission.Intent.ID {
		t.Fatalf("Recover() after prerequisite failure = %#v", recovered)
	}
}

type failingBrowserScreenshotMediaStore struct {
	media.MediaStore
	err error
}

func (store failingBrowserScreenshotMediaStore) StoreIdempotentOwned(
	string,
	media.MediaMeta,
	string,
	string,
	media.MediaOwner,
) (string, error) {
	return "", store.err
}

func TestGatewayBrowserScreenshotRemovesUnregisteredDeliveryCopy(t *testing.T) {
	workspace := t.TempDir()
	runtime := &nodeAdmissionRuntime{}
	t.Cleanup(func() {
		if runtime.transferSpool != nil {
			_ = runtime.transferSpool.Close()
		}
	})
	storeErr := errors.New("definitive media registration failure")
	source := &gatewayBrowserToolSource{
		services: &services{
			NodeAdmission: runtime,
			MediaStore:    failingBrowserScreenshotMediaStore{err: storeErr},
		},
		workspace: workspace, screenshotRetention: time.Hour,
	}
	data := append(append([]byte(nil), []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}...),
		[]byte("cleanup fixture")...)
	_, err := source.retainScreenshot(
		gatewayBrowserArtifactContext(workspace),
		browser.ScreenshotRequest{RequestID: "request_cleanup"},
		browser.ScreenshotCapture{
			SessionID: "session_cleanup", Target: "gateway", Profile: "managed",
			PolicyRevision: "policy_1", TabID: "tab_primary", SnapshotID: "snapshot_1",
			SnapshotGeneration: 1, Data: data, ContentType: "image/png",
		},
	)
	if !errors.Is(err, storeErr) {
		t.Fatalf("retainScreenshot() error = %v", err)
	}
	entries, readErr := os.ReadDir(filepath.Join(workspace, "state", "media", "node-transfers"))
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("unregistered delivery files = %+v, %v", entries, readErr)
	}
}

func TestGatewayBrowserScreenshotRemovesCopyAfterPostRenameSyncWarning(t *testing.T) {
	workspace := t.TempDir()
	runtime := &nodeAdmissionRuntime{}
	t.Cleanup(func() {
		if runtime.transferSpool != nil {
			_ = runtime.transferSpool.Close()
		}
	})
	store, err := media.NewFileMediaStoreWithPersistentIndex(
		filepath.Join(workspace, "state", "media", "index.json"),
		media.MediaCleanerConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	syncErr := errors.New("delivery directory sync failed after rename")
	source := &gatewayBrowserToolSource{
		services:  &services{NodeAdmission: runtime, MediaStore: store},
		workspace: workspace, screenshotRetention: time.Hour,
		screenshotCopy: func(
			_ context.Context,
			_ *os.File,
			_ nodes.TransferArtifactRecord,
			copyWorkspace string,
			name string,
		) (string, bool, error) {
			directory := filepath.Join(copyWorkspace, "state", "media", "node-transfers")
			if mkdirErr := os.MkdirAll(directory, 0o700); mkdirErr != nil {
				return "", false, mkdirErr
			}
			path := filepath.Join(directory, name)
			if writeErr := os.WriteFile(path, []byte("renamed screenshot"), 0o600); writeErr != nil {
				return "", false, writeErr
			}
			return path, true, &fileutil.CommittedWriteError{Err: syncErr}
		},
	}
	data := append(append([]byte(nil), []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}...),
		[]byte("post-rename cleanup fixture")...)
	_, err = source.retainScreenshot(
		gatewayBrowserArtifactContext(workspace),
		browser.ScreenshotRequest{RequestID: "request_post_rename_cleanup"},
		browser.ScreenshotCapture{
			SessionID: "session_cleanup", Target: "gateway", Profile: "managed",
			PolicyRevision: "policy_1", TabID: "tab_primary", SnapshotID: "snapshot_1",
			SnapshotGeneration: 1, Data: data, ContentType: "image/png",
		},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("retainScreenshot() error = %v", err)
	}
	entries, readErr := os.ReadDir(filepath.Join(workspace, "state", "media", "node-transfers"))
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("post-rename warning files = %+v, %v", entries, readErr)
	}
}

func gatewayBrowserArtifactContext(workspace string) context.Context {
	ctx := tools.WithToolContext(context.Background(), "telegram", "chat-1")
	ctx = tools.WithToolInboundMetadata(ctx, bus.InboundContext{
		SenderID: "sender-1", ActorID: "actor-1",
	})
	ctx = tools.WithToolSessionContext(ctx, "browser", "history-1", nil)
	ctx = tools.WithToolRouteSessionKey(ctx, "route-1")
	ctx = tools.WithToolCallID(ctx, "call-1")
	return tools.WithToolExecutionIdentity(ctx, workspace, "execution-1")
}
