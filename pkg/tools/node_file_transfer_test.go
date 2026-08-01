package tools

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type fakeNodeFileTransferSource struct {
	*fakeNodeInvocationSource
	dispatchResult NodeFileTransferResult
	dispatchErr    error
	dispatchMarked bool
	queryCalls     int
	cancelCalls    int
	snapshotCalls  int
	snapshotRef    string
	snapshotRecord nodes.TransferArtifactRecord
	inspectResult  NodeFileTransferResult
	inspectErr     error
	handoffRef     string
	handoffCalls   int
}

func (source *fakeNodeFileTransferSource) SnapshotUploadArtifact(
	_ context.Context,
	owner nodes.TransferArtifactOwner,
	transferID string,
	target string,
	profileRevision string,
	expiresAt int64,
	_ int64,
	ref string,
	_ media.MediaStore,
	_ media.MediaOwner,
) (nodes.TransferArtifactRecord, error) {
	source.snapshotCalls++
	source.snapshotRef = ref
	if source.snapshotRecord.Ref == "" {
		return nodes.TransferArtifactRecord{}, errors.New("unexpected upload snapshot")
	}
	record := source.snapshotRecord
	record.Owner = owner
	record.Spec.TransferID = transferID
	record.Spec.Target = target
	record.Spec.ProfileRevision = profileRevision
	record.Spec.ExpiresAt = expiresAt
	return record, nil
}

func (source *fakeNodeFileTransferSource) InspectFile(
	context.Context,
	nodes.ID,
	NodeFileTransferBinding,
) (NodeFileTransferResult, error) {
	if source.inspectErr != nil {
		return NodeFileTransferResult{}, source.inspectErr
	}
	if source.inspectResult.State == "" {
		return NodeFileTransferResult{}, errors.New("unexpected metadata probe")
	}
	return source.inspectResult, nil
}

func (source *fakeNodeFileTransferSource) DispatchFileTransfer(
	_ context.Context,
	owner nodes.GatewayInvocationOwner,
	record nodes.GatewayInvocationRecord,
) (NodeFileTransferResult, bool, error) {
	if source.dispatchMarked {
		return NodeFileTransferResult{}, true, nodes.ErrGatewayInvocationDispatched
	}
	_, transitioned, err := source.store.MarkDispatched(
		owner,
		record.Plan.InvocationID,
		record.ExpectedPlanHash,
	)
	if err != nil || !transitioned {
		return NodeFileTransferResult{}, transitioned, err
	}
	source.dispatchMarked = true
	source.dispatchCalls++
	return source.dispatchResult, true, source.dispatchErr
}

func (source *fakeNodeFileTransferSource) QueryFileTransfer(
	_ context.Context,
	principal nodes.GatewayInvocationPrincipal,
	record nodes.GatewayInvocationRecord,
) (NodeFileTransferResult, error) {
	source.queryCalls++
	retained, found, err := source.store.Lookup(principal, record.Plan.InvocationID)
	if err != nil || !found || retained.State != nodes.GatewayInvocationDispatched {
		return NodeFileTransferResult{}, nodes.ErrGatewayInvocationConflict
	}
	return source.dispatchResult, nil
}

func (source *fakeNodeFileTransferSource) CancelFileTransfer(
	_ context.Context,
	principal nodes.GatewayInvocationPrincipal,
	record nodes.GatewayInvocationRecord,
) (NodeFileTransferResult, bool, error) {
	source.cancelCalls++
	if _, found, err := source.store.Lookup(principal, record.Plan.InvocationID); err != nil || !found {
		return NodeFileTransferResult{}, false, nodes.ErrGatewayInvocationConflict
	}
	return NodeFileTransferResult{State: "canceled"}, true, nil
}

func (source *fakeNodeFileTransferSource) HandoffDownloadedArtifact(
	context.Context,
	nodes.TransferArtifactOwner,
	string,
	media.MediaStore,
	media.MediaOwner,
) (string, bool, error) {
	source.handoffCalls++
	if source.handoffRef == "" {
		return "", false, errors.New("unexpected artifact handoff")
	}
	return source.handoffRef, source.handoffCalls == 1, nil
}

func TestNodeFileInfoReusesExactApprovalAndQueriesWithoutReplay(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "required")
	tool := NewNodeFileInfoTool(nodeFileTransferTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "file-call-1")
	args := nodeFileInfoTestArgs(t, source, ctx)

	first, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	second, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	if first["plan_hash"] != second["plan_hash"] ||
		first["transfer_id"] != second["transfer_id"] ||
		source.prepareCalls != 1 {
		t.Fatalf("approval binding changed: first=%#v second=%#v prepares=%d",
			first, second, source.prepareCalls)
	}
	if result := tool.Execute(ctx, args); !strings.Contains(result.ForLLM, "APPROVAL_REQUIRED") {
		t.Fatalf("unapproved result = %s", result.ForLLM)
	}
	approved := tool.Execute(WithToolApprovalContinuation(ctx, true), args)
	payload := decodeNodeResult(t, approved)
	if payload["state"] != "committed" ||
		payload["path"] != "/srv/project/config.json" ||
		payload["policy_revision"] != "project-v1" ||
		source.dispatchCalls != 1 {
		t.Fatalf("approved result = %#v, dispatches=%d", payload, source.dispatchCalls)
	}
	repeated := tool.Execute(WithToolApprovalContinuation(ctx, true), args)
	repeatedPayload := decodeNodeResult(t, repeated)
	if repeatedPayload["state"] != "committed" ||
		source.dispatchCalls != 1 ||
		source.queryCalls != 1 {
		t.Fatalf("repeated result = %#v, dispatches=%d queries=%d",
			repeatedPayload, source.dispatchCalls, source.queryCalls)
	}
}

func TestNodeFileTransferEventsExposeLifecycleWithoutContentOrAuthority(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "none")
	tool := NewNodeFileInfoTool(nodeFileTransferTestConfig(), source)
	eventBus := &recordingNodeEventBus{}
	tool.SetEventPublisher(eventBus)
	ctx := nodeInvocationTestContext("actor-1", "file-event-call")
	args := nodeFileInfoTestArgs(t, source, ctx)

	result := tool.Execute(ctx, args)
	if result.IsError {
		t.Fatalf("nodes_file_info failed: %s", result.ForLLM)
	}
	events := eventBus.snapshot()
	want := []string{
		NodeInvocationObservationPrepared,
		NodeInvocationObservationDispatched,
		NodeInvocationObservationCompleted,
	}
	if len(events) != len(want) {
		t.Fatalf("file-transfer event count = %d, want %d: %#v", len(events), len(want), events)
	}
	for index, event := range events {
		if event.Kind != runtimeevents.KindNodeInvocationObserved {
			t.Fatalf("event[%d].Kind = %q", index, event.Kind)
		}
		payload, ok := event.Payload.(NodeInvocationEventPayload)
		if !ok || payload.Observation != want[index] ||
			payload.Command != "file.info.v1" || payload.Target != "build" ||
			payload.InvocationID == "" {
			t.Fatalf("event[%d] payload = %#v", index, event.Payload)
		}
		if event.Source.Name != "nodes_file_info" ||
			event.Scope.Workspace != "/workspace/main" ||
			event.Scope.TurnID != "execution-1" ||
			event.Scope.AgentID != "main" ||
			event.Scope.SessionKey != "route-session" ||
			event.Scope.Channel != "telegram" ||
			event.Scope.ChatID != "chat-1" ||
			event.Scope.SenderID != "actor-1" ||
			event.Correlation.RequestID != "file-event-call" {
			t.Fatalf("event[%d] scope = %#v correlation = %#v", index, event.Scope, event.Correlation)
		}
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"/srv/project/config.json",
		strings.Repeat("a", sha256.Size*2),
		"project-v1",
		"private-node-id",
		"plan_hash",
		"artifact_ref",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("file-transfer audit leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestNodeFileApprovalActionUsesExactRetainedPlan(t *testing.T) {
	digest := strings.Repeat("a", sha256.Size*2)
	common := map[string]any{
		"target":        "files",
		"profile":       "project",
		"profile_scope": "configured_regular_file_roots",
	}
	tests := []struct {
		name      string
		tool      string
		arguments map[string]any
		contains  []string
	}{
		{
			name: "metadata",
			tool: "nodes_file_info",
			arguments: mergeNodeFileApprovalArguments(common, map[string]any{
				"operation": "file.info.v1",
				"path":      "/srv/project/photo.png",
			}),
			contains: []string{"Inspect", "/srv/project/photo.png", "files", "project"},
		},
		{
			name: "upload replace",
			tool: "nodes_upload",
			arguments: mergeNodeFileApprovalArguments(common, map[string]any{
				"operation":   "file.upload.v1",
				"destination": "/srv/project/photo.png",
				"publication": "replace",
				"size":        float64(42),
				"sha256":      digest,
			}),
			contains: []string{"Upload 42 bytes", digest, "/srv/project/photo.png", "atomically replace"},
		},
		{
			name: "download and deliver",
			tool: "nodes_download",
			arguments: mergeNodeFileApprovalArguments(common, map[string]any{
				"operation": "file.download.v1",
				"source":    "/srv/project/photo.png",
				"size":      float64(42),
				"sha256":    digest,
				"deliver":   true,
			}),
			contains: []string{"Download 42 bytes", digest, "/srv/project/photo.png", "deliver once"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			action, handled, err := NodeFileApprovalAction(test.tool, test.arguments)
			if err != nil || !handled {
				t.Fatalf("NodeFileApprovalAction() = (%q, %v, %v)", action, handled, err)
			}
			for _, value := range test.contains {
				if !strings.Contains(action, value) {
					t.Fatalf("approval action %q omitted %q", action, value)
				}
			}
		})
	}
	if _, handled, err := NodeFileApprovalAction("nodes_upload", mergeNodeFileApprovalArguments(
		common,
		map[string]any{"operation": "file.upload.v1"},
	)); err == nil || !handled {
		t.Fatalf("incomplete file approval = (handled=%v, err=%v)", handled, err)
	}
	if action, handled, err := NodeFileApprovalAction("read_file", nil); err != nil || handled || action != "" {
		t.Fatalf("non-file approval = (%q, %v, %v)", action, handled, err)
	}
}

func mergeNodeFileApprovalArguments(base, extra map[string]any) map[string]any {
	result := make(map[string]any, len(base)+len(extra))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range extra {
		result[key] = value
	}
	return result
}

func TestNodeFileApprovalContinuationRejectsChangedInputAndActor(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "required")
	tool := NewNodeFileInfoTool(nodeFileTransferTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "file-call-1")
	args := nodeFileInfoTestArgs(t, source, ctx)
	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}

	changed := cloneStringAnyMap(args)
	changed["path"] = "/srv/project/other.json"
	result := tool.Execute(WithToolApprovalContinuation(ctx, true), changed)
	if !strings.Contains(result.ForLLM, "DISCOVERY_STALE") || source.dispatchCalls != 0 {
		t.Fatalf("changed continuation = %s, dispatches=%d", result.ForLLM, source.dispatchCalls)
	}

	otherActor := nodeInvocationTestContext("actor-2", "file-call-1")
	result = tool.Execute(WithToolApprovalContinuation(otherActor, true), args)
	if !strings.Contains(result.ForLLM, "DISCOVERY_STALE") || source.dispatchCalls != 0 {
		t.Fatalf("other actor continuation = %s, dispatches=%d", result.ForLLM, source.dispatchCalls)
	}

	otherRoute := WithToolInboundContext(ctx, "telegram", "chat-2", "", "")
	otherRoute = WithToolInboundMetadata(otherRoute, bus.InboundContext{
		Channel: "telegram", ChatID: "chat-2", SenderID: "actor-1", ActorID: "actor-1",
	})
	contexts := map[string]context.Context{
		"agent": WithToolSessionContext(
			ctx,
			"other-agent",
			"history-session",
			nil,
		),
		"routed session": WithToolRouteSessionKey(ctx, "other-route-session"),
		"route":          otherRoute,
		"workspace": WithToolExecutionIdentity(
			ctx,
			"/workspace/other",
			"execution-1",
		),
		"execution": WithToolExecutionIdentity(
			ctx,
			"/workspace/main",
			"execution-2",
		),
		"tool call": WithToolCallID(ctx, "file-call-2"),
	}
	for name, changedContext := range contexts {
		t.Run(name, func(t *testing.T) {
			changedResult := tool.Execute(
				WithToolApprovalContinuation(changedContext, true),
				args,
			)
			if !strings.Contains(changedResult.ForLLM, "DISCOVERY_STALE") ||
				source.dispatchCalls != 0 {
				t.Fatalf("continuation = %s, dispatches=%d",
					changedResult.ForLLM, source.dispatchCalls)
			}
		})
	}

	result = tool.Execute(WithToolApprovalBypass(ctx, true), args)
	if !strings.Contains(result.ForLLM, "APPROVAL_REQUIRED") || source.dispatchCalls != 0 {
		t.Fatalf("approval bypass = %s, dispatches=%d", result.ForLLM, source.dispatchCalls)
	}
}

func TestNodeUploadApprovalContinuationBindsOriginalMediaArtifact(t *testing.T) {
	source := newFakeNodeFileTransferSourceForDescriptor(
		t,
		nodeFileUploadTestDescriptor("required"),
	)
	source.snapshotRecord = nodes.TransferArtifactRecord{
		Ref: "transfer-artifact://retained-upload",
		Spec: nodes.TransferArtifactSpec{
			Direction:    nodes.TransferDirectionUpload,
			Filename:     "config.json",
			ContentType:  "application/json",
			DeclaredSize: 12,
			SHA256:       strings.Repeat("a", sha256.Size*2),
		},
	}
	tool := NewNodeUploadTool(nodeFileTransferTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "file-call-upload")
	args := nodeFileUploadTestArgs(t, source, ctx, "media://artifact-one")
	approval, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	if approval["artifact_ref"] != source.snapshotRecord.Ref ||
		source.snapshotRef != "media://artifact-one" || source.snapshotCalls != 1 {
		t.Fatalf("approval = %#v, source ref = %q, snapshots = %d",
			approval, source.snapshotRef, source.snapshotCalls)
	}

	changed := cloneStringAnyMap(args)
	changed["artifact_ref"] = "media://artifact-two"
	result := tool.Execute(WithToolApprovalContinuation(ctx, true), changed)
	if !strings.Contains(result.ForLLM, "DISCOVERY_STALE") ||
		source.snapshotCalls != 1 || source.dispatchCalls != 0 {
		t.Fatalf("changed artifact continuation = %s, snapshots=%d dispatches=%d",
			result.ForLLM, source.snapshotCalls, source.dispatchCalls)
	}
}

func TestGenericNodeInvokeRejectsFileTransferCommands(t *testing.T) {
	source := newFakeNodeFileTransferSourceForDescriptor(
		t,
		nodeFileUploadTestDescriptor("none"),
	)
	tool := NewNodeInvokeTool(nodeFileTransferTestConfig(), source)
	result := tool.Execute(
		nodeInvocationTestContext("actor-1", "generic-file-call"),
		map[string]any{
			"target":             "build",
			"command":            "file.upload.v1",
			"input":              map[string]any{},
			"discovery_revision": "opaque",
		},
	)
	if !strings.Contains(result.ForLLM, nodeDenialCommandUnavailable) ||
		source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf("generic file invocation = %s, prepares=%d dispatches=%d",
			result.ForLLM, source.prepareCalls, source.dispatchCalls)
	}
}

func TestNodeStatusAndCancelUseActorScopedFileTransferLifecycle(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "none")
	ctx := nodeInvocationTestContext("actor-1", "file-call-status")
	args := nodeFileInfoTestArgs(t, source, ctx)
	invoked := NewNodeFileInfoTool(nodeFileTransferTestConfig(), source).Execute(ctx, args)
	payload := decodeNodeResult(t, invoked)
	transferID, _ := payload["transfer_id"].(string)
	if transferID == "" {
		t.Fatalf("invocation result = %#v", payload)
	}

	status := NewNodeStatusTool(nodeFileTransferTestConfig(), source).Execute(
		ctx,
		map[string]any{"invocation_id": transferID},
	)
	statusPayload := decodeNodeResult(t, status)
	if statusPayload["state"] != "committed" || source.queryCalls != 1 {
		t.Fatalf("file status = %#v, queries=%d", statusPayload, source.queryCalls)
	}

	denied := NewNodeCancelTool(nodeFileTransferTestConfig(), source).Execute(
		nodeInvocationTestContext("actor-2", "other-cancel-call"),
		map[string]any{"invocation_id": transferID},
	)
	deniedPayload := decodeNodeResult(t, denied)
	if deniedPayload["status"] != "denied" || source.cancelCalls != 0 {
		t.Fatalf("cross-actor cancel = %#v, cancels=%d", deniedPayload, source.cancelCalls)
	}

	canceled := NewNodeCancelTool(nodeFileTransferTestConfig(), source).Execute(
		ctx,
		map[string]any{"invocation_id": transferID},
	)
	canceledPayload := decodeNodeResult(t, canceled)
	if canceledPayload["status"] != "canceled" || source.cancelCalls != 1 {
		t.Fatalf("file cancel = %#v, cancels=%d", canceledPayload, source.cancelCalls)
	}
}

func TestNodeStatusReportsDisconnectedFileTransferUnknownWithoutReplay(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "none")
	ctx := nodeInvocationTestContext("actor-1", "file-call-disconnected")
	args := nodeFileInfoTestArgs(t, source, ctx)
	payload := decodeNodeResult(
		t,
		NewNodeFileInfoTool(nodeFileTransferTestConfig(), source).Execute(ctx, args),
	)
	source.connected["private-node-id"] = false

	status := NewNodeStatusTool(nodeFileTransferTestConfig(), source).Execute(
		ctx,
		map[string]any{"invocation_id": payload["transfer_id"]},
	)
	statusPayload := decodeNodeResult(t, status)
	if statusPayload["state"] != "unknown" ||
		statusPayload["code"] != "NODE_UNAVAILABLE" ||
		source.queryCalls != 0 || source.dispatchCalls != 1 {
		t.Fatalf("disconnected status = %#v, queries=%d dispatches=%d",
			statusPayload, source.queryCalls, source.dispatchCalls)
	}
}

func TestNodeCancelReportsDisconnectedFileTransferUnknownWithoutRequest(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "none")
	ctx := nodeInvocationTestContext("actor-1", "file-call-disconnected-cancel")
	args := nodeFileInfoTestArgs(t, source, ctx)
	payload := decodeNodeResult(
		t,
		NewNodeFileInfoTool(nodeFileTransferTestConfig(), source).Execute(ctx, args),
	)
	source.connected["private-node-id"] = false

	canceled := NewNodeCancelTool(nodeFileTransferTestConfig(), source).Execute(
		ctx,
		map[string]any{"invocation_id": payload["transfer_id"]},
	)
	canceledPayload := decodeNodeResult(t, canceled)
	if canceledPayload["status"] != "unknown" ||
		canceledPayload["error_code"] != "NODE_UNAVAILABLE" ||
		!strings.Contains(fmt.Sprint(canceledPayload["recovery_action"]), "nodes_status") ||
		source.cancelCalls != 0 || source.dispatchCalls != 1 {
		t.Fatalf("disconnected cancel = %#v, cancels=%d dispatches=%d",
			canceledPayload, source.cancelCalls, source.dispatchCalls)
	}
}

func TestNodeDownloadDeliveryIsClaimedOnceWithoutCompletionReply(t *testing.T) {
	source := newFakeNodeFileTransferSourceForDescriptor(
		t,
		nodeFileDownloadTestDescriptor("none"),
	)
	digest := strings.Repeat("d", sha256.Size*2)
	source.inspectResult = NodeFileTransferResult{
		State:  "committed",
		Size:   19,
		SHA256: digest,
	}
	source.dispatchResult = NodeFileTransferResult{
		State:       "committed",
		Size:        19,
		SHA256:      digest,
		ArtifactRef: "transfer-artifact://downloaded-image",
		Filename:    "image.png",
		ContentType: "image/png",
	}
	source.handoffRef = "media://downloaded-image"
	tool := NewNodeDownloadTool(nodeFileTransferTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "file-call-download")
	args := nodeFileDownloadTestArgs(t, source, ctx, true)

	first := tool.Execute(ctx, args)
	if !first.ResponseHandled || len(first.Media) != 1 ||
		first.Media[0] != source.handoffRef || source.dispatchCalls != 1 ||
		source.handoffCalls != 1 {
		t.Fatalf("first delivery = %#v, dispatches=%d handoffs=%d",
			first, source.dispatchCalls, source.handoffCalls)
	}
	second := tool.Execute(ctx, args)
	if !second.ResponseHandled || len(second.Media) != 0 ||
		source.dispatchCalls != 1 || source.queryCalls != 1 ||
		source.handoffCalls != 2 ||
		!strings.Contains(second.ForLLM, "already_claimed") {
		t.Fatalf("duplicate delivery = %#v, dispatches=%d queries=%d handoffs=%d",
			second, source.dispatchCalls, source.queryCalls, source.handoffCalls)
	}
}

func TestNodeDownloadRetainOnlyKeepsExplicitFalseInPlan(t *testing.T) {
	source := newFakeNodeFileTransferSourceForDescriptor(
		t,
		nodeFileDownloadTestDescriptor("none"),
	)
	digest := strings.Repeat("d", sha256.Size*2)
	source.inspectResult = NodeFileTransferResult{
		State:  "committed",
		Size:   19,
		SHA256: digest,
	}
	source.dispatchResult = NodeFileTransferResult{
		State:       "committed",
		Size:        19,
		SHA256:      digest,
		ArtifactRef: "transfer-artifact://retained-image",
		Filename:    "image.png",
		ContentType: "image/png",
	}
	tool := NewNodeDownloadTool(nodeFileTransferTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "file-call-retain-only")
	args := nodeFileDownloadTestArgs(t, source, ctx, false)

	result := tool.Execute(ctx, args)
	payload := decodeNodeResult(t, result)
	if payload["state"] != "committed" ||
		payload["artifact_ref"] != "transfer-artifact://retained-image" ||
		source.dispatchCalls != 1 || source.handoffCalls != 0 {
		t.Fatalf("retain-only result = %#v, dispatches=%d handoffs=%d",
			payload, source.dispatchCalls, source.handoffCalls)
	}
}

func TestNodeFileApprovalPreparationReturnsOnlySafeDenial(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "required")
	tool := NewNodeFileInfoTool(nodeFileTransferTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "file-call-1")
	args := nodeFileInfoTestArgs(t, source, ctx)
	args["discovery_revision"] = "stale"

	_, err := tool.ApprovalArguments(ctx, args)
	if err == nil {
		t.Fatal("stale approval preparation unexpectedly succeeded")
	}
	result, safe := SafeApprovalDenialResult(err)
	if !safe ||
		!strings.Contains(result.ForLLM, "DISCOVERY_STALE") ||
		strings.Contains(result.ForLLM, "private-node-id") {
		t.Fatalf("safe denial = (%v, %s)", safe, result.ForLLM)
	}
}

func TestNodeFileToolsRequireExplicitAgentGrant(t *testing.T) {
	cfg := nodeFileTransferTestConfig()
	cfg.Agents.List = []config.AgentConfig{
		{ID: "main", Default: true},
		{ID: "inherited"},
		{
			ID: "explicit",
			TargetPolicy: &config.TargetPolicy{
				AllowedTargets: []string{"build"},
			},
		},
	}
	runtime := newNodeFileTransferToolRuntime(cfg, newFakeNodeFileTransferSource(t, "none"))
	if !runtime.enabledForAgent("main") ||
		runtime.enabledForAgent("inherited") ||
		!runtime.enabledForAgent("explicit") {
		t.Fatalf("agent grants: main=%v inherited=%v explicit=%v",
			runtime.enabledForAgent("main"),
			runtime.enabledForAgent("inherited"),
			runtime.enabledForAgent("explicit"))
	}
}

func TestNodeFileContextCancellationRequestsBoundCancel(t *testing.T) {
	source := newFakeNodeFileTransferSource(t, "none")
	source.dispatchErr = context.Canceled
	tool := NewNodeFileInfoTool(nodeFileTransferTestConfig(), source)
	ctx, cancel := context.WithCancel(nodeInvocationTestContext("actor-1", "file-call-1"))
	args := nodeFileInfoTestArgs(t, source, ctx)
	cancel()
	result := tool.Execute(ctx, args)
	if !strings.Contains(result.ForLLM, "TRANSFER_OUTCOME_UNKNOWN") ||
		source.dispatchCalls != 1 ||
		source.cancelCalls != 1 {
		t.Fatalf("canceled result = %s, dispatches=%d cancels=%d",
			result.ForLLM, source.dispatchCalls, source.cancelCalls)
	}
}

func newFakeNodeFileTransferSource(
	t *testing.T,
	metadataApproval string,
) *fakeNodeFileTransferSource {
	t.Helper()
	return newFakeNodeFileTransferSourceForDescriptor(
		t,
		nodeFileInfoTestDescriptor(metadataApproval),
	)
}

func newFakeNodeFileTransferSourceForDescriptor(
	t *testing.T,
	command nodes.CommandDescriptor,
) *fakeNodeFileTransferSource {
	t.Helper()
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	catalogHash := mustCatalogHash(t, catalog)
	snapshot := nodes.Snapshot{
		ID:             "private-node-id",
		State:          nodes.StateConnected,
		Catalog:        catalog,
		CatalogHash:    catalogHash,
		Executor:       "local",
		PolicyRevision: "node-policy-v1",
	}
	discovery := &fakeNodeDiscoverySource{
		byRef: map[string]nodes.Snapshot{"builder-node": snapshot},
		registrations: map[nodes.ID]nodes.Registration{
			snapshot.ID: {
				Snapshot:            snapshot,
				AllowedCommands:     []string{command.Name},
				ApprovedCatalogHash: catalogHash,
				ApprovedAt:          1,
			},
		},
		connected: map[nodes.ID]bool{snapshot.ID: true},
	}
	store, err := nodes.NewGatewayInvocationStore(
		filepath.Join(t.TempDir(), "node-file-invocations.json"),
		8,
		1024*1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	base := &fakeNodeInvocationSource{
		fakeNodeDiscoverySource: discovery,
		store:                   store,
	}
	return &fakeNodeFileTransferSource{
		fakeNodeInvocationSource: base,
		dispatchResult: NodeFileTransferResult{
			State:  "committed",
			Type:   "regular_file",
			Size:   12,
			SHA256: strings.Repeat("a", sha256.Size*2),
		},
	}
}

func nodeFileTransferTestConfig() *config.Config {
	cfg := nodeDiscoveryTestConfig()
	target := cfg.Execution.Targets["build"]
	target.FileProfile = "project"
	cfg.Execution.Targets["build"] = target
	return cfg
}

func nodeFileInfoTestArgs(
	t *testing.T,
	source NodeDiscoverySource,
	ctx context.Context,
) map[string]any {
	t.Helper()
	result := NewNodeDiscoveryTool(nodeFileTransferTestConfig(), source).Execute(
		ctx,
		map[string]any{
			"action":  "describe",
			"target":  "build",
			"command": "file.info.v1",
		},
	)
	payload := decodeNodeResult(t, result)
	return map[string]any{
		"target":             "build",
		"path":               "/srv/project/config.json",
		"discovery_revision": payload["discovery_revision"],
	}
}

func nodeFileUploadTestArgs(
	t *testing.T,
	source NodeDiscoverySource,
	ctx context.Context,
	artifactRef string,
) map[string]any {
	t.Helper()
	result := NewNodeDiscoveryTool(nodeFileTransferTestConfig(), source).Execute(
		ctx,
		map[string]any{
			"action":  "describe",
			"target":  "build",
			"command": "file.upload.v1",
		},
	)
	payload := decodeNodeResult(t, result)
	return map[string]any{
		"target":             "build",
		"artifact_ref":       artifactRef,
		"destination":        "/srv/project/config.json",
		"publication":        "replace",
		"discovery_revision": payload["discovery_revision"],
	}
}

func nodeFileDownloadTestArgs(
	t *testing.T,
	source NodeDiscoverySource,
	ctx context.Context,
	deliver bool,
) map[string]any {
	t.Helper()
	result := NewNodeDiscoveryTool(nodeFileTransferTestConfig(), source).Execute(
		ctx,
		map[string]any{
			"action":  "describe",
			"target":  "build",
			"command": "file.download.v1",
		},
	)
	payload := decodeNodeResult(t, result)
	return map[string]any{
		"target":             "build",
		"source":             "/srv/project/image.png",
		"deliver":            deliver,
		"discovery_revision": payload["discovery_revision"],
	}
}

func nodeFileInfoTestDescriptor(metadataApproval string) nodes.CommandDescriptor {
	profile := nodes.FileProfileDescriptor{
		Alias:         "project",
		Revision:      "project-v1",
		ReadableRoots: []string{"/srv/project"},
		MaxFileBytes:  1024 * 1024,
		Approval: nodes.FileProfileApproval{
			Metadata: metadataApproval,
			Read:     "required",
			Write:    "required",
		},
	}
	return nodes.CommandDescriptor{
		Name: "file.info.v1",
		InputSchema: json.RawMessage(
			`{"additionalProperties":false,"properties":{"path":{"type":"string"},` +
				`"route_id":{"type":"string"},"discovery_revision":{"type":"string"}},` +
				`"required":["path","route_id","discovery_revision"],"type":"object"}`,
		),
		OutputSchema:   json.RawMessage(`{"additionalProperties":true,"properties":{},"type":"object"}`),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
		ModelContract: &nodes.CommandModelContract{
			Availability:      nodes.ModelUnavailable,
			TimeoutSecondsMax: nodes.MaxInvocationTimeout,
			OutputBytesMax:    nodes.MaxInvocationOutput,
			ResultKind:        "json",
			AuthorityDigest:   strings.Repeat("b", sha256.Size*2),
			Constraints: nodes.CommandModelConstraints{
				ProfileAliases: []string{"project"},
			},
			Guidance: []string{},
			Examples: []json.RawMessage{},
		},
		FileProfiles: []nodes.FileProfileDescriptor{profile},
	}
}

func nodeFileUploadTestDescriptor(writeApproval string) nodes.CommandDescriptor {
	profile := nodes.FileProfileDescriptor{
		Alias:          "project",
		Revision:       "project-v1",
		WritableRoots:  []string{"/srv/project"},
		AllowCreate:    true,
		AllowOverwrite: true,
		MaxFileBytes:   1024 * 1024,
		Approval: nodes.FileProfileApproval{
			Metadata: "none",
			Read:     "required",
			Write:    writeApproval,
		},
	}
	return nodes.CommandDescriptor{
		Name: "file.upload.v1",
		InputSchema: json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"artifact_ref":{"type":"string"},"source_artifact_id":{"type":"string"},` +
				`"destination":{"type":"string"},"publication":{"type":"string"},` +
				`"size":{"type":"integer"},"sha256":{"type":"string"},` +
				`"filename":{"type":"string"},"content_type":{"type":"string"},` +
				`"route_id":{"type":"string"},"discovery_revision":{"type":"string"}},` +
				`"required":["artifact_ref","source_artifact_id","destination","publication",` +
				`"size","sha256","filename","route_id","discovery_revision"],"type":"object"}`,
		),
		OutputSchema:   json.RawMessage(`{"additionalProperties":true,"properties":{},"type":"object"}`),
		Risk:           nodes.RiskWrite,
		SupportsCancel: true,
		ModelContract: &nodes.CommandModelContract{
			Availability:      nodes.ModelUnavailable,
			TimeoutSecondsMax: nodes.MaxInvocationTimeout,
			OutputBytesMax:    nodes.MaxInvocationOutput,
			ResultKind:        "json",
			AuthorityDigest:   strings.Repeat("c", sha256.Size*2),
			Constraints: nodes.CommandModelConstraints{
				ProfileAliases: []string{"project"},
			},
			Guidance: []string{},
			Examples: []json.RawMessage{},
		},
		FileProfiles: []nodes.FileProfileDescriptor{profile},
	}
}

func nodeFileDownloadTestDescriptor(readApproval string) nodes.CommandDescriptor {
	profile := nodes.FileProfileDescriptor{
		Alias:         "project",
		Revision:      "project-v1",
		ReadableRoots: []string{"/srv/project"},
		MaxFileBytes:  1024 * 1024,
		Approval: nodes.FileProfileApproval{
			Metadata: "none",
			Read:     readApproval,
			Write:    "required",
		},
	}
	return nodes.CommandDescriptor{
		Name: "file.download.v1",
		InputSchema: json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"source":{"type":"string"},"deliver":{"type":"boolean"},` +
				`"size":{"type":"integer"},"sha256":{"type":"string"},` +
				`"filename":{"type":"string"},"content_type":{"type":"string"},` +
				`"channel":{"type":"string"},"chat_id":{"type":"string"},` +
				`"topic_id":{"type":"string"},"route_id":{"type":"string"},` +
				`"discovery_revision":{"type":"string"}},` +
				`"required":["source","deliver","size","sha256","filename","route_id",` +
				`"discovery_revision"],"type":"object"}`,
		),
		OutputSchema:   json.RawMessage(`{"additionalProperties":true,"properties":{},"type":"object"}`),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
		ModelContract: &nodes.CommandModelContract{
			Availability:      nodes.ModelUnavailable,
			TimeoutSecondsMax: nodes.MaxInvocationTimeout,
			OutputBytesMax:    nodes.MaxInvocationOutput,
			ResultKind:        "json",
			AuthorityDigest:   strings.Repeat("e", sha256.Size*2),
			Constraints: nodes.CommandModelConstraints{
				ProfileAliases: []string{"project"},
			},
			Guidance: []string{},
			Examples: []json.RawMessage{},
		},
		FileProfiles: []nodes.FileProfileDescriptor{profile},
	}
}

func cloneStringAnyMap(source map[string]any) map[string]any {
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}
