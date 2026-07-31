package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

func TestDecodeNodeFileTransferResponseRejectsUntrustedResultFields(t *testing.T) {
	validDigest := sha256.Sum256([]byte("artifact"))
	valid := tools.NodeFileTransferResult{
		State:       "streaming",
		Type:        "regular_file",
		Size:        10,
		Mode:        0o640,
		ModifiedAt:  1,
		SHA256:      hex.EncodeToString(validDigest[:]),
		Sequence:    1,
		Transferred: 5,
		Code:        "SOURCE_CHANGED",
	}
	tests := []struct {
		name   string
		mutate func(*tools.NodeFileTransferResult)
	}{
		{name: "unknown state", mutate: func(result *tools.NodeFileTransferResult) { result.State = "done" }},
		{
			name:   "gateway transfer id",
			mutate: func(result *tools.NodeFileTransferResult) { result.TransferID = "forged" },
		},
		{name: "gateway path", mutate: func(result *tools.NodeFileTransferResult) { result.Path = "/secret" }},
		{
			name:   "gateway artifact",
			mutate: func(result *tools.NodeFileTransferResult) { result.ArtifactRef = "media:forged" },
		},
		{name: "gateway filename", mutate: func(result *tools.NodeFileTransferResult) { result.Filename = "forged" }},
		{
			name:   "gateway content type",
			mutate: func(result *tools.NodeFileTransferResult) { result.ContentType = "text/plain" },
		},
		{
			name:   "gateway policy",
			mutate: func(result *tools.NodeFileTransferResult) { result.PolicyRevision = "forged" },
		},
		{
			name:   "gateway delivery",
			mutate: func(result *tools.NodeFileTransferResult) { result.DeliveryState = "delivered" },
		},
		{
			name:   "gateway recovery",
			mutate: func(result *tools.NodeFileTransferResult) { result.RecoveryAction = "retry" },
		},
		{name: "invalid type", mutate: func(result *tools.NodeFileTransferResult) { result.Type = "directory" }},
		{name: "oversized", mutate: func(result *tools.NodeFileTransferResult) {
			result.Size = uint64(nodes.MaxTransferArtifactBytes) + 1
		}},
		{name: "over transferred", mutate: func(result *tools.NodeFileTransferResult) { result.Transferred = 11 }},
		{name: "invalid mode", mutate: func(result *tools.NodeFileTransferResult) { result.Mode = 0o100640 }},
		{name: "negative modified time", mutate: func(result *tools.NodeFileTransferResult) { result.ModifiedAt = -1 }},
		{name: "short digest", mutate: func(result *tools.NodeFileTransferResult) { result.SHA256 = "00" }},
		{
			name:   "uppercase digest",
			mutate: func(result *tools.NodeFileTransferResult) { result.SHA256 = strings.ToUpper(result.SHA256) },
		},
		{name: "unsafe code", mutate: func(result *tools.NodeFileTransferResult) { result.Code = "source changed" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := valid
			test.mutate(&result)
			payload, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			_, err = decodeNodeFileTransferResponse(protocol.TransferFrame{
				Type:       protocol.TransferFrameStatus,
				TransferID: "transfer-1",
				Payload:    payload,
			})
			if !errors.Is(err, protocol.ErrInvalidTransferFrame) {
				t.Fatalf("decode error = %v, want invalid transfer frame", err)
			}
		})
	}

	payload, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNodeFileTransferResponse(protocol.TransferFrame{
		Type:       protocol.TransferFrameStatus,
		TransferID: "transfer-1",
		Payload:    payload,
	})
	if err != nil || decoded.TransferID != "transfer-1" {
		t.Fatalf("valid decode = (%#v, %v)", decoded, err)
	}
}

func TestNodeFileTransferSourceRejectsStaleRuntimeGeneration(t *testing.T) {
	runtime := &nodeAdmissionRuntime{
		registryPath: "/workspace/state/nodes.json",
		sessions:     nodews.NewSessionHub(),
		generation:   7,
		mounted:      true,
	}
	source := &nodeFileTransferSource{nodeInvocationSource: &nodeInvocationSource{
		nodeDiscoverySource: nodeDiscoverySource{
			runtime: runtime, registryPath: runtime.registryPath,
		},
		generation: runtime.generation,
	}}
	binding := tools.NodeFileTransferBinding{
		TransferID:     "transfer-1",
		Direction:      protocol.TransferDownload,
		PolicyRevision: "profile-v1",
		SHA256:         sha256.Sum256(nil),
	}
	if _, _, err := source.openTransfer(
		t.Context(), nodes.ID("node-1"), binding,
	); !errors.Is(err, nodews.ErrNodeDisconnected) {
		t.Fatalf("current generation error = %v, want disconnected", err)
	}
	runtime.registryMu.Lock()
	runtime.generation++
	runtime.registryMu.Unlock()
	if _, _, err := source.openTransfer(
		t.Context(), nodes.ID("node-1"), binding,
	); !errors.Is(err, errNodeDiscoveryAuthorityUnavailable) {
		t.Fatalf("stale generation error = %v", err)
	}
}

func TestNodeFileRecoverySourceOpensOnlyAnExistingSpoolWithoutGrant(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Nodes.Enabled = true
	runtime := &nodeAdmissionRuntime{
		registryPath: nodes.RegistryPath(cfg.WorkspacePath()),
		handler:      &fakeNodeAdmissionHandler{},
		generation:   1,
		mounted:      true,
	}
	if source, err := newNodeFileTransferRecoverySource(cfg, runtime); err != nil || source != nil {
		t.Fatalf("fresh recovery source = (%#v, %v)", source, err)
	}
	spoolPath := nodes.GatewayTransferSpoolPath(cfg.WorkspacePath())
	spool, err := nodes.NewGatewayTransferSpool(
		spoolPath,
		8,
		1024*1024,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	recovery, err := newNodeFileTransferRecoverySource(cfg, runtime)
	if err != nil || recovery == nil {
		t.Fatalf("existing recovery source = (%#v, %v)", recovery, err)
	}
	t.Cleanup(func() {
		if runtime.transferSpool != nil {
			_ = runtime.transferSpool.Close()
		}
	})
	if source, err := newNodeFileTransferSource(cfg, runtime); err != nil || source != nil {
		t.Fatalf("ungranted transfer source = (%#v, %v)", source, err)
	}
}

func TestNodeFileTransferSnapshotsOnlyRetainedMedia(t *testing.T) {
	workspace := t.TempDir()
	spool, err := nodes.NewGatewayTransferSpool(
		filepath.Join(workspace, "spool"),
		8,
		1024*1024,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	store, err := media.NewFileMediaStoreWithPersistentIndex(
		filepath.Join(workspace, "media", "index.json"),
		media.MediaCleanerConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(workspace, "inbound.bin")
	content := []byte{0, 1, 2, 3, 4, 0xff}
	if err := os.WriteFile(sourcePath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	mediaRef, err := store.Store(
		sourcePath,
		media.MediaMeta{
			Filename:      "photo.bin",
			ContentType:   "application/octet-stream",
			CleanupPolicy: media.CleanupPolicyForgetOnly,
		},
		"session-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	mediaOwner := testNodeTransferMediaOwner(t, "actor-1")
	if err := store.BindOwner(mediaRef, mediaOwner); err != nil {
		t.Fatal(err)
	}
	source := &nodeFileTransferSource{spool: spool, workspace: workspace}
	owner := testNodeTransferOwner()
	record, err := source.SnapshotUploadArtifact(
		t.Context(),
		owner,
		"transfer-1",
		"personal-vpn",
		"profile-v1",
		time.Now().Add(5*time.Minute).Unix(),
		1024,
		mediaRef,
		store,
		mediaOwner,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := sha256.Sum256(content)
	if record.State != nodes.TransferArtifactCommitted ||
		record.Spec.SHA256 != hex.EncodeToString(wantDigest[:]) ||
		record.Spec.DeclaredSize != int64(len(content)) ||
		record.Spec.Filename != "photo.bin" {
		t.Fatalf("snapshot record = %#v", record)
	}
	file, retained, err := spool.ResolveOwned(owner, record.Ref)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	if retained.Ref != record.Ref {
		t.Fatalf("retained ref = %q, want %q", retained.Ref, record.Ref)
	}
	if _, err := source.SnapshotUploadArtifact(
		t.Context(),
		owner,
		"transfer-other-actor",
		"personal-vpn",
		"profile-v1",
		time.Now().Add(5*time.Minute).Unix(),
		1024,
		mediaRef,
		store,
		testNodeTransferMediaOwner(t, "actor-2"),
	); !errors.Is(err, nodes.ErrTransferArtifactNotFound) {
		t.Fatalf("cross-actor media snapshot error = %v", err)
	}

	if _, err := source.SnapshotUploadArtifact(
		t.Context(),
		owner,
		"transfer-path",
		"personal-vpn",
		"profile-v1",
		time.Now().Add(5*time.Minute).Unix(),
		1024,
		sourcePath,
		store,
		mediaOwner,
	); !errors.Is(err, nodes.ErrTransferArtifactNotFound) {
		t.Fatalf("model path snapshot error = %v", err)
	}
	otherOwner := owner
	otherOwner.ActorID = "actor-2"
	if _, _, err := spool.ResolveOwned(otherOwner, record.Ref); !errors.Is(
		err,
		nodes.ErrTransferArtifactNotFound,
	) {
		t.Fatalf("cross-actor resolve error = %v", err)
	}
}

func TestNodeFileTransferHandoffClaimsOneRoutedDelivery(t *testing.T) {
	workspace := t.TempDir()
	spool, err := nodes.NewGatewayTransferSpool(
		filepath.Join(workspace, "spool"),
		8,
		1024*1024,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	store, err := media.NewFileMediaStoreWithPersistentIndex(
		filepath.Join(workspace, "state", "media", "index.json"),
		media.MediaCleanerConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	owner := testNodeTransferOwner()
	content := []byte("one routed artifact")
	digest := sha256.Sum256(content)
	writer, artifact, created, err := spool.Begin(owner, nodes.TransferArtifactSpec{
		TransferID:      "transfer-download-1",
		Direction:       nodes.TransferDirectionDownload,
		Target:          "personal-vpn",
		ProfileRevision: "profile-v1",
		Filename:        "result.txt",
		ContentType:     "text/plain",
		DeclaredSize:    int64(len(content)),
		SHA256:          hex.EncodeToString(digest[:]),
		ExpiresAt:       time.Now().Add(5 * time.Minute).Unix(),
	})
	if err != nil || !created {
		t.Fatalf("begin = (%v, %v)", created, err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	artifact, err = writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	source := &nodeFileTransferSource{spool: spool, workspace: workspace}
	mediaOwner := testNodeTransferMediaOwner(t, "actor-1")
	firstRef, firstClaim, err := source.HandoffDownloadedArtifact(
		t.Context(),
		owner,
		artifact.Ref,
		store,
		mediaOwner,
	)
	if err != nil || !firstClaim {
		t.Fatalf("first handoff = (%q, %v, %v)", firstRef, firstClaim, err)
	}
	secondRef, secondClaim, err := source.HandoffDownloadedArtifact(
		t.Context(),
		owner,
		artifact.Ref,
		store,
		mediaOwner,
	)
	if err != nil || secondClaim || secondRef != firstRef {
		t.Fatalf("second handoff = (%q, %v, %v)", secondRef, secondClaim, err)
	}
	resolved, meta, err := store.ResolveOwnedWithMeta(firstRef, mediaOwner)
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := os.ReadFile(resolved)
	if err != nil || string(delivered) != string(content) ||
		meta.Filename != "result.txt" ||
		meta.ContentType != "text/plain" {
		t.Fatalf("delivered = (%q, %#v, %v)", delivered, meta, err)
	}
	otherRoute := owner
	otherRoute.RouteID = "route-2"
	if _, _, err := source.HandoffDownloadedArtifact(
		context.Background(),
		otherRoute,
		artifact.Ref,
		store,
		testNodeTransferMediaOwner(t, "actor-2"),
	); !errors.Is(err, nodes.ErrTransferArtifactNotFound) {
		t.Fatalf("cross-route handoff error = %v", err)
	}
}

func TestCopyNodeTransferDeliveryRejectsSymlinkedWorkspaceAncestor(t *testing.T) {
	workspace := t.TempDir()
	escape := t.TempDir()
	if err := os.Symlink(escape, filepath.Join(workspace, "state")); err != nil {
		t.Fatal(err)
	}
	content := []byte("must remain in the workspace")
	sourcePath := filepath.Join(t.TempDir(), "source.data")
	if err := os.WriteFile(sourcePath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	digest := sha256.Sum256(content)
	artifact := nodes.TransferArtifactRecord{Spec: nodes.TransferArtifactSpec{
		DeclaredSize: int64(len(content)),
		SHA256:       hex.EncodeToString(digest[:]),
	}}
	if _, err := copyNodeTransferDelivery(
		t.Context(),
		source,
		artifact,
		workspace,
		"delivery.data",
	); err == nil {
		t.Fatal("delivery below a symlinked workspace ancestor succeeded")
	}
	if _, err := os.Stat(
		filepath.Join(escape, "media", "node-transfers", "delivery.data"),
	); !errors.Is(
		err,
		os.ErrNotExist,
	) {
		t.Fatalf("delivery escaped workspace: %v", err)
	}
}

func testNodeTransferMediaOwner(t *testing.T, actor string) media.MediaOwner {
	t.Helper()
	owner, err := media.NewMediaOwner(
		"/workspace/main",
		"main",
		actor,
		"telegram:chat-1",
		"telegram",
		"chat-1",
		"topic-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func testNodeTransferOwner() nodes.TransferArtifactOwner {
	return nodes.TransferArtifactOwner{
		WorkspaceID: "workspace-1",
		AgentID:     "main",
		ActorID:     "actor-1",
		RouteID:     "route-1",
		SessionID:   "session-1",
		ToolCallID:  "tool-call-1",
	}
}
