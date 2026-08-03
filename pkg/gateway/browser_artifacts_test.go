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
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
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
		Ref: artifact.Ref, MediaRef: artifact.MediaRef,
	}
	if err = source.ClaimScreenshotDelivery(ctx, delivery); err != nil {
		t.Fatalf("ClaimScreenshotDelivery() error = %v", err)
	}
	replay, found, err = source.LookupScreenshot(ctx, owner, request.RequestID, capture.SessionID)
	if err != nil || !found || replay.Ref != artifact.Ref || replay.MediaRef != artifact.MediaRef ||
		replay.DeliveryState != browser.ScreenshotDeliveryAlreadyClaimed ||
		replay.SnapshotID != artifact.SnapshotID || replay.SnapshotGeneration != artifact.SnapshotGeneration {
		t.Fatalf("claimed LookupScreenshot() = %+v, %t, %v", replay, found, err)
	}
	if err = source.ClaimScreenshotDelivery(ctx, delivery); err != nil {
		t.Fatalf("idempotent ClaimScreenshotDelivery() error = %v", err)
	}
	capture.Data = append(capture.Data, 0)
	if _, err = source.retainScreenshot(ctx, request, capture); err == nil {
		t.Fatal("conflicting replay unexpectedly succeeded")
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
