package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/media"
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
		artifact.DeliveryState != "claimed" || artifact.Size != int64(len(data)) ||
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

	duplicate, err := source.retainScreenshot(ctx, request, capture)
	if err != nil || duplicate.Ref != artifact.Ref || duplicate.MediaRef != artifact.MediaRef ||
		duplicate.DeliveryState != "already_claimed" {
		t.Fatalf("duplicate retainScreenshot() = %+v, %v", duplicate, err)
	}
	capture.Data = append(capture.Data, 0)
	if _, err = source.retainScreenshot(ctx, request, capture); err == nil {
		t.Fatal("conflicting replay unexpectedly succeeded")
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
