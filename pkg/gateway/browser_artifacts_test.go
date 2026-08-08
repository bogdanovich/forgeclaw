package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
	upload, err := source.resolveBrowserUpload(ctx, browser.PrepareActionRequest{
		RequestID: "request_upload", SessionID: capture.SessionID, TabID: capture.TabID,
		SnapshotID: capture.SnapshotID, SnapshotGeneration: capture.SnapshotGeneration,
		Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
	})
	if err != nil || upload.Ref != artifact.Ref || upload.SHA256 != artifact.SHA256 ||
		upload.Size != artifact.Size || upload.Filename != browserScreenshotFilename ||
		upload.ContentType != "image/png" {
		t.Fatalf("same-snapshot screenshot upload = %#v, %v", upload, err)
	}
	uploaded, err := os.ReadFile(upload.Path)
	if err != nil || !bytes.Equal(uploaded, data) {
		t.Fatalf("same-snapshot upload bytes = %d, %v", len(uploaded), err)
	}
	for name, request := range map[string]browser.PrepareActionRequest{
		"session": {
			RequestID: "wrong_session", SessionID: "session_2", TabID: capture.TabID,
			SnapshotID: capture.SnapshotID, SnapshotGeneration: capture.SnapshotGeneration,
			Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
		},
		"tab": {
			RequestID: "wrong_tab", SessionID: capture.SessionID, TabID: "tab_other",
			SnapshotID: capture.SnapshotID, SnapshotGeneration: capture.SnapshotGeneration,
			Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
		},
		"snapshot": {
			RequestID: "wrong_snapshot", SessionID: capture.SessionID, TabID: capture.TabID,
			SnapshotID: "snapshot_other", SnapshotGeneration: capture.SnapshotGeneration,
			Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
		},
		"generation": {
			RequestID: "wrong_generation", SessionID: capture.SessionID, TabID: capture.TabID,
			SnapshotID: capture.SnapshotID, SnapshotGeneration: capture.SnapshotGeneration + 1,
			Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
		},
	} {
		t.Run("upload rejects wrong "+name, func(t *testing.T) {
			if _, resolveErr := source.resolveBrowserUpload(ctx, request); !errors.Is(resolveErr, browser.ErrDenied) {
				t.Fatalf("resolveBrowserUpload() error = %v, want ErrDenied", resolveErr)
			}
		})
	}
	validUploadRequest := browser.PrepareActionRequest{
		RequestID: "wrong_authority", SessionID: capture.SessionID, TabID: capture.TabID,
		SnapshotID: capture.SnapshotID, SnapshotGeneration: capture.SnapshotGeneration,
		Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
	}
	wrongActor := tools.WithToolInboundMetadata(ctx, bus.InboundContext{
		SenderID: "sender-2", ActorID: "actor-2",
	})
	for name, wrongContext := range map[string]context.Context{
		"agent": tools.WithToolSessionContext(ctx, "main", "history-1", nil),
		"actor": wrongActor,
		"route": tools.WithToolRouteSessionKey(ctx, "route-2"),
	} {
		t.Run("upload rejects wrong "+name, func(t *testing.T) {
			if _, resolveErr := source.resolveBrowserUpload(wrongContext, validUploadRequest); !errors.Is(
				resolveErr, browser.ErrDenied,
			) {
				t.Fatalf("resolveBrowserUpload() error = %v, want ErrDenied", resolveErr)
			}
		})
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

func TestGatewayNodeDownloadResolvesThroughBrowserUploadWorker(t *testing.T) {
	workspace := t.TempDir()
	cfg := gatewayBrowserConfig(workspace)
	policyRevision, err := cfg.Tools.Browser.PolicyRevision()
	if err != nil {
		t.Fatal(err)
	}
	nodeRuntime := &nodeAdmissionRuntime{}
	t.Cleanup(func() {
		if nodeRuntime.transferSpool != nil {
			_ = nodeRuntime.transferSpool.Close()
		}
	})
	servicesOwner := &services{NodeAdmission: nodeRuntime}
	source := &gatewayBrowserToolSource{
		services: servicesOwner, policyRevision: policyRevision, workspace: workspace,
		limits: cfg.Tools.Browser.Limits.Effective(),
	}
	ctx := gatewayBrowserArtifactContext(workspace)
	nodeOwner, err := tools.RoutedNodeFileArtifactOwner(ctx, "node_download_call")
	if err != nil {
		t.Fatal(err)
	}
	spool, err := nodeRuntime.gatewayTransferSpool(nodes.GatewayTransferSpoolPath(workspace))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("node download to browser upload fixture")
	digest := sha256.Sum256(data)
	writer, artifact, created, err := spool.Begin(nodeOwner, nodes.TransferArtifactSpec{
		TransferID: "node_download", Direction: nodes.TransferDirectionDownload,
		Target: "personal-vpn", ProfileRevision: "files-v1", Filename: "fixture.txt",
		ContentType: "text/plain", DeclaredSize: int64(len(data)),
		SHA256: hex.EncodeToString(digest[:]), ExpiresAt: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil || !created || writer == nil {
		t.Fatalf("Begin() = %#v, %t, %v", artifact, created, err)
	}
	if err = writer.WriteChunk(1, data); err != nil {
		t.Fatal(err)
	}
	artifact, err = writer.Commit()
	if err != nil {
		t.Fatal(err)
	}

	worker := &gatewayUploadWorker{want: data}
	broker, err := browser.NewBroker(cfg, browser.NewMemoryStore(), &gatewayUploadFactory{worker: worker})
	if err != nil {
		t.Fatal(err)
	}
	servicesOwner.Browser = &browserRuntime{broker: broker, policyRevision: policyRevision}
	owner := browser.Owner{
		ActorID: "actor_1", AgentID: browser.OpaqueAgentID("browser"),
		SessionKey: "route_1", ExecutionID: "execution_1",
	}
	session, err := broker.Open(context.Background(), browser.OpenRequest{
		Owner: owner, Target: config.BrowserDefaultTarget, Profile: config.BrowserDefaultProfile,
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`\[ref=([^]]+)\]`).FindStringSubmatch(observation.Snapshot)
	if len(match) != 2 {
		t.Fatalf("observation snapshot = %q", observation.Snapshot)
	}
	preparation, err := source.PrepareAction(ctx, browser.PrepareActionRequest{
		Owner: owner, RequestID: "request_upload", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: browser.Action{Kind: browser.ActionUpload, Ref: match[1], ArtifactRef: artifact.Ref},
	})
	if err != nil || preparation.Action.ArtifactSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("PrepareAction() = %#v, %v", preparation, err)
	}
	invocation, err := source.ExecuteAction(ctx, owner, preparation.Action.ID, nil)
	if err != nil || invocation.State != browser.InvocationSucceeded || !filepath.IsAbs(worker.path) ||
		!bytes.Equal(worker.got, data) {
		t.Fatalf("ExecuteAction() = %#v, %v; upload path = %q, data = %q", invocation, err, worker.path, worker.got)
	}

	otherRoute := tools.WithToolRouteSessionKey(ctx, "route-2")
	if _, err = source.resolveBrowserUpload(otherRoute, browser.PrepareActionRequest{
		RequestID: "request_wrong_route", SessionID: session.ID,
		Action: browser.Action{Kind: browser.ActionUpload, ArtifactRef: artifact.Ref},
	}); !errors.Is(err, browser.ErrDenied) {
		t.Fatalf("cross-route upload error = %v", err)
	}
}

type gatewayUploadFactory struct{ worker *gatewayUploadWorker }

func (factory *gatewayUploadFactory) Open(
	context.Context, browser.WorkerOpenRequest,
) (browser.WorkerOpenResult, error) {
	return browser.WorkerOpenResult{Owner: factory.worker}, nil
}

type gatewayUploadWorker struct {
	want []byte
	got  []byte
	path string
}

func (*gatewayUploadWorker) Status(context.Context) (browser.WorkerStatus, error) {
	return browser.WorkerReady, nil
}
func (*gatewayUploadWorker) Close(context.Context) error { return nil }
func (*gatewayUploadWorker) Observe(context.Context) (browser.DriverObservation, error) {
	return browser.DriverObservation{
		URL: "https://example.com/upload", Origin: "https://example.com", Title: "Fixture",
		Snapshot: "- button \"Choose file\" [ref=e1]",
		Elements: []browser.DriverElement{{Target: "e1", Role: "button", Name: "Choose file"}},
	}, nil
}

func (*gatewayUploadWorker) Resolve(
	context.Context, string,
) (browser.DriverElement, string, error) {
	return browser.DriverElement{Target: "e1", Role: "button", Name: "Choose file"},
		"https://example.com", nil
}
func (*gatewayUploadWorker) Execute(context.Context, browser.DriverAction) error { return nil }
func (*gatewayUploadWorker) CatalogRevision() string                             { return strings.Repeat("c", 64) }
func (worker *gatewayUploadWorker) Upload(_ context.Context, action browser.DriverAction) error {
	if !filepath.IsAbs(action.Value) {
		return browser.ErrDriverIncompatible
	}
	data, err := os.ReadFile(action.Value)
	if err != nil || !bytes.Equal(data, worker.want) {
		return browser.ErrDenied
	}
	worker.path, worker.got = action.Value, data
	return nil
}

func (*gatewayUploadWorker) Download(
	context.Context, browser.DriverAction, int64,
) (browser.DriverDownload, error) {
	return browser.DriverDownload{}, browser.ErrDriverIncompatible
}

func TestGatewayBrowserDownloadRecoversAcrossActualBrokerRestart(t *testing.T) {
	workspace := t.TempDir()
	cfg := gatewayBrowserConfig(workspace)
	policyRevision, err := cfg.Tools.Browser.PolicyRevision()
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(workspace, "state", "browser", "browser.json")
	store, err := browser.NewFileStore(statePath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	worker := &gatewayArtifactRecoveryWorker{}
	broker, err := browser.NewBroker(cfg, store, &gatewayArtifactRecoveryFactory{worker: worker})
	if err != nil {
		t.Fatal(err)
	}
	owner := browser.Owner{
		ActorID: "actor_1", AgentID: browser.OpaqueAgentID("browser"),
		SessionKey: "route_1", ExecutionID: "execution_1",
	}
	session, err := broker.Open(context.Background(), browser.OpenRequest{
		Owner: owner, Target: config.BrowserDefaultTarget, Profile: config.BrowserDefaultProfile,
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := broker.Observe(context.Background(), owner, session.ID, session.TabID)
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`\[ref=([^]]+)\]`).FindStringSubmatch(observation.Snapshot)
	if len(match) != 2 {
		t.Fatalf("observation snapshot = %q", observation.Snapshot)
	}
	request := browser.PrepareActionRequest{
		Owner: owner, RequestID: "request_restart_download", SessionID: session.ID, TabID: session.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		Action: browser.Action{Kind: browser.ActionDownload, Ref: match[1]},
	}
	preparation, err := broker.PrepareAction(context.Background(), request)
	if err != nil || !preparation.RequiresApproval {
		t.Fatalf("PrepareAction() = %#v, %v", preparation, err)
	}
	invocations, err := store.ListInvocations(context.Background(), session.ID)
	if err != nil || len(invocations) != 1 {
		t.Fatalf("ListInvocations() = %#v, %v", invocations, err)
	}
	accepted := invocations[0]
	accepted.State = browser.InvocationAccepted
	accepted.Revision++
	accepted.AcceptedAt = accepted.CreatedAt + 1
	accepted.UpdatedAt = accepted.AcceptedAt
	if err = store.UpdateInvocation(context.Background(), accepted.Revision-1, accepted); err != nil {
		t.Fatal(err)
	}

	mediaStore, err := media.NewFileMediaStoreWithPersistentIndex(
		filepath.Join(workspace, "state", "media", "index.json"), media.MediaCleanerConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	nodeRuntime := &nodeAdmissionRuntime{}
	t.Cleanup(func() {
		if nodeRuntime.transferSpool != nil {
			_ = nodeRuntime.transferSpool.Close()
		}
	})
	servicesOwner := &services{NodeAdmission: nodeRuntime, MediaStore: mediaStore}
	source := &gatewayBrowserToolSource{
		services: servicesOwner, policyRevision: policyRevision, workspace: workspace,
		screenshotRetention: time.Hour, limits: cfg.Tools.Browser.Limits.Effective(),
	}
	payload := []byte("restart download fixture")
	path := filepath.Join(t.TempDir(), "restart.txt")
	if err = os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	ctx := gatewayBrowserArtifactContext(workspace)
	retained, err := source.retainBrowserDownload(ctx, preparation.Action, browser.DriverDownload{
		Path: path, Filename: "restart.txt", ContentType: "text/plain",
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	reopened, err := browser.NewFileStore(statePath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	recoveredBroker, err := browser.NewBroker(
		cfg, reopened, &gatewayArtifactRecoveryFactory{worker: &gatewayArtifactRecoveryWorker{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = recoveredBroker.Recover(ctx, source.committedBrowserDownload); err != nil {
		t.Fatal(err)
	}
	servicesOwner.Browser = &browserRuntime{broker: recoveredBroker, policyRevision: policyRevision}
	recoveredPreparation, err := source.PrepareAction(ctx, request)
	if err != nil || recoveredPreparation.Action.ID != preparation.Action.ID {
		t.Fatalf("PrepareAction() after restart = %#v, %v", recoveredPreparation, err)
	}
	result, err := source.ExecuteAction(ctx, owner, preparation.Action.ID, &preparation.Approval)
	if err != nil || result.State != browser.InvocationSucceeded || result.Download == nil ||
		result.Download.Ref != retained.Ref || worker.executeCalls != 0 {
		t.Fatalf("ExecuteAction() after restart = %#v, %v; driver calls = %d", result, err, worker.executeCalls)
	}
	storedSession, err := recoveredBroker.Status(context.Background(), owner, session.ID)
	if err != nil || storedSession.State != browser.SessionLost || storedSession.SnapshotID != "" {
		t.Fatalf("recovered browser session = %#v, %v", storedSession, err)
	}
}

type gatewayArtifactRecoveryFactory struct {
	worker *gatewayArtifactRecoveryWorker
}

func (factory *gatewayArtifactRecoveryFactory) Open(
	context.Context, browser.WorkerOpenRequest,
) (browser.WorkerOpenResult, error) {
	return browser.WorkerOpenResult{Owner: factory.worker}, nil
}

type gatewayArtifactRecoveryWorker struct{ executeCalls int }

func (*gatewayArtifactRecoveryWorker) Status(context.Context) (browser.WorkerStatus, error) {
	return browser.WorkerReady, nil
}
func (*gatewayArtifactRecoveryWorker) Close(context.Context) error { return nil }
func (*gatewayArtifactRecoveryWorker) Observe(context.Context) (browser.DriverObservation, error) {
	return browser.DriverObservation{
		URL: "https://example.com/download", Origin: "https://example.com", Title: "Fixture",
		Snapshot: "- link \"Download\" [ref=e1]",
		Elements: []browser.DriverElement{{Target: "e1", Role: "link", Name: "Download"}},
	}, nil
}

func (*gatewayArtifactRecoveryWorker) Resolve(
	context.Context, string,
) (browser.DriverElement, string, error) {
	return browser.DriverElement{Target: "e1", Role: "link", Name: "Download"}, "https://example.com", nil
}

func (worker *gatewayArtifactRecoveryWorker) Execute(context.Context, browser.DriverAction) error {
	worker.executeCalls++
	return nil
}
func (*gatewayArtifactRecoveryWorker) CatalogRevision() string { return strings.Repeat("c", 64) }

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

func TestGatewayOutboundRecoveryTerminalizesMissingScreenshot(t *testing.T) {
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
	reconciler, err := startGatewayOutboundReconciler(
		t.Context(), second, msgBus, admissions, runtime, workspace,
	)
	if err != nil {
		t.Fatalf("startGatewayOutboundReconciler() error = %v", err)
	}
	reconciler.stop()
	terminal, err := second.Get(admission.Intent.ID)
	if err != nil || terminal.Status != outbox.StatusAmbiguous ||
		terminal.LastError != missingRecoveredBrowserArtifactError {
		t.Fatalf("terminal intent = %+v, %v", terminal, err)
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
	if len(recovered) != 0 {
		t.Fatalf("Recover() after terminal prerequisite failure = %#v", recovered)
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
