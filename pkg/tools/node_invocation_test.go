package tools

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
)

type recordingNodeEventBus struct {
	mu     sync.Mutex
	events []runtimeevents.Event
}

func (bus *recordingNodeEventBus) Publish(
	_ context.Context,
	event runtimeevents.Event,
) runtimeevents.PublishResult {
	return bus.record(event)
}

func (bus *recordingNodeEventBus) PublishNonBlocking(
	event runtimeevents.Event,
) runtimeevents.PublishResult {
	return bus.record(event)
}

func (bus *recordingNodeEventBus) record(
	event runtimeevents.Event,
) runtimeevents.PublishResult {
	bus.mu.Lock()
	bus.events = append(bus.events, event)
	bus.mu.Unlock()
	return runtimeevents.PublishResult{Matched: 1, Delivered: 1}
}

func (*recordingNodeEventBus) Channel() runtimeevents.EventChannel { return nil }
func (*recordingNodeEventBus) Close() error                        { return nil }
func (*recordingNodeEventBus) Stats() runtimeevents.Stats          { return runtimeevents.Stats{} }

func (bus *recordingNodeEventBus) snapshot() []runtimeevents.Event {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return append([]runtimeevents.Event(nil), bus.events...)
}

type fakeNodeInvocationSource struct {
	*fakeNodeDiscoverySource
	store                   *nodes.GatewayInvocationStore
	preDispatchErr          error
	dispatchErr             error
	queryErr                error
	queryErrors             []error
	cancelErr               error
	remote                  nodes.InvocationRecord
	lookupMiss              bool
	beforeAuthorityValidate func()
	prepareMu               sync.Mutex
	prepareCalls            int
	dispatchCalls           int
	queryCalls              int
	cancelCalls             int
}

type atomicPrepareNodeInvocationSource struct {
	*fakeNodeInvocationSource
}

func (source *atomicPrepareNodeInvocationSource) PrepareInvocation(
	nodeRef string,
	target string,
	toolCallID string,
	principal nodes.GatewayInvocationPrincipal,
	plan nodes.ExecutionPlan,
	descriptor nodes.CommandDescriptor,
	allowCreate bool,
	validate func(NodeDiscoveryRecord) error,
) (nodes.GatewayInvocationRecord, bool, error) {
	return source.fakeNodeInvocationSource.PrepareInvocation(
		nodeRef,
		target,
		toolCallID,
		principal,
		plan,
		descriptor,
		allowCreate,
		validate,
	)
}

func (source *fakeNodeInvocationSource) PrepareInvocation(
	nodeRef string,
	target string,
	toolCallID string,
	principal nodes.GatewayInvocationPrincipal,
	plan nodes.ExecutionPlan,
	descriptor nodes.CommandDescriptor,
	allowCreate bool,
	validate func(NodeDiscoveryRecord) error,
) (nodes.GatewayInvocationRecord, bool, error) {
	source.prepareMu.Lock()
	defer source.prepareMu.Unlock()
	if source.beforeAuthorityValidate != nil {
		hook := source.beforeAuthorityValidate
		source.beforeAuthorityValidate = nil
		hook()
	}
	current, found, err := source.Lookup(nodeRef)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, false, err
	}
	if !found {
		return nodes.GatewayInvocationRecord{}, false, errDiscoveryStale
	}
	if err := validate(current); err != nil {
		return nodes.GatewayInvocationRecord{}, false, err
	}
	if !source.lookupMiss {
		retained, retainedFound, lookupErr := source.store.ByToolCall(principal, toolCallID)
		if lookupErr != nil {
			return nodes.GatewayInvocationRecord{}, false, lookupErr
		}
		if retainedFound {
			return retained, false, nil
		}
	}
	if !allowCreate {
		return nodes.GatewayInvocationRecord{}, false, nodes.ErrGatewayInvocationNotFound
	}
	source.prepareCalls++
	return source.store.PrepareOwned(principal, target, toolCallID, plan, descriptor)
}

func (source *fakeNodeInvocationSource) LookupInvocationByToolCall(
	principal nodes.GatewayInvocationPrincipal,
	toolCallID string,
) (nodes.GatewayInvocationRecord, bool, error) {
	if source.lookupMiss {
		return nodes.GatewayInvocationRecord{}, false, nil
	}
	return source.store.ByToolCall(principal, toolCallID)
}

func (source *fakeNodeInvocationSource) LookupInvocation(
	principal nodes.GatewayInvocationPrincipal,
	invocationID string,
) (nodes.GatewayInvocationRecord, bool, error) {
	return source.store.Lookup(principal, invocationID)
}

func (source *fakeNodeInvocationSource) DispatchInvocation(
	_ context.Context,
	owner nodes.GatewayInvocationOwner,
	invocationID string,
	expectedPlanHash string,
) (json.RawMessage, bool, error) {
	if source.preDispatchErr != nil {
		return nil, false, source.preDispatchErr
	}
	principal := nodes.GatewayInvocationPrincipal{
		AgentID:     owner.AgentID,
		SessionID:   owner.SessionID,
		ActorID:     owner.ActorID,
		WorkspaceID: owner.WorkspaceID,
		ExecutionID: owner.ExecutionID,
	}
	record, found, err := source.store.Lookup(principal, invocationID)
	if err != nil || !found {
		return nil, false, nodes.ErrGatewayInvocationConflict
	}
	if record.State == nodes.GatewayInvocationDispatched {
		return nil, false, nodes.ErrGatewayInvocationDispatched
	}
	if _, transitioned, err := source.store.MarkDispatched(
		owner,
		invocationID,
		expectedPlanHash,
	); err != nil {
		return nil, false, err
	} else if !transitioned {
		return nil, false, nodes.ErrGatewayInvocationDispatched
	}
	source.dispatchCalls++
	if source.dispatchErr != nil {
		return nil, true, source.dispatchErr
	}
	return json.RawMessage(`{"stdout":"ok","exit_code":0}`), true, nil
}

func (source *fakeNodeInvocationSource) CancelInvocation(
	_ context.Context,
	principal nodes.GatewayInvocationPrincipal,
	target string,
	nodeID nodes.ID,
	invocationID string,
) (nodes.InvocationRecord, bool, error) {
	record, transitioned, err := source.store.RequestCancellation(principal, invocationID)
	if err != nil {
		return nodes.InvocationRecord{}, false, err
	}
	if record.Target != target || record.Plan.NodeID != nodeID {
		return nodes.InvocationRecord{}, transitioned, nodes.ErrGatewayInvocationConflict
	}
	if transitioned {
		source.cancelCalls++
	}
	if source.cancelErr != nil {
		return nodes.InvocationRecord{}, transitioned, source.cancelErr
	}
	return source.remote, transitioned, nil
}

func (source *fakeNodeInvocationSource) QueryInvocation(
	_ context.Context,
	principal nodes.GatewayInvocationPrincipal,
	target string,
	nodeID nodes.ID,
	invocationID string,
) (nodes.InvocationRecord, error) {
	source.queryCalls++
	if len(source.queryErrors) >= source.queryCalls && source.queryErrors[source.queryCalls-1] != nil {
		return nodes.InvocationRecord{}, source.queryErrors[source.queryCalls-1]
	}
	record, found, err := source.store.Lookup(principal, invocationID)
	if err != nil {
		return nodes.InvocationRecord{}, err
	}
	if !found || record.Target != target || record.Plan.NodeID != nodeID {
		return nodes.InvocationRecord{}, nodes.ErrGatewayInvocationConflict
	}
	if source.queryErr != nil {
		return nodes.InvocationRecord{}, source.queryErr
	}
	return source.remote, nil
}

func TestNodeInvokeToolReusesPreparedAuthorityAndDispatches(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	args := nodeInvocationTestArgs()

	first, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	second, err := tool.ApprovalArguments(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	if first["plan_hash"] != second["plan_hash"] ||
		first["invocation_id"] != second["invocation_id"] ||
		source.prepareCalls != 1 {
		t.Fatalf("approval binding changed = %#v, %#v", first, second)
	}
	result := tool.Execute(ctx, args)
	if result.IsError {
		t.Fatalf("nodes_invoke failed: %s", result.ForLLM)
	}
	payload := decodeNodeResult(t, result)
	if payload["state"] != string(nodes.InvocationSucceeded) ||
		payload["target"] != "build" ||
		payload["invocation_id"] != first["invocation_id"] {
		t.Fatalf("invoke result = %#v", payload)
	}
	if strings.Contains(result.ForLLM, "private-node-id") ||
		strings.Contains(result.ForLLM, "plan_hash") {
		t.Fatalf("invoke result leaked internal authority: %s", result.ForLLM)
	}
}

func TestNodeInvokeToolRequiresHumanApprovalContinuationForShellExec(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	command := shellNodeInvocationTestDescriptor()
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	catalogHash := mustCatalogHash(t, catalog)
	snapshot := source.byRef["builder-node"]
	snapshot.Catalog = catalog
	snapshot.CatalogHash = catalogHash
	source.byRef["builder-node"] = snapshot
	source.registrations[snapshot.ID] = nodes.Registration{
		Snapshot:            snapshot,
		AllowedCommands:     []string{command.Name},
		ApprovedCatalogHash: catalogHash,
		ApprovedAt:          1,
	}
	ctx := nodeInvocationTestContext("actor-1", "call-shell")
	discovery := NewNodeDiscoveryTool(nodeDiscoveryTestConfig(), source).Execute(
		ctx,
		map[string]any{
			"action": "describe", "target": "build", "command": command.Name,
		},
	)
	if discovery.IsError {
		t.Fatalf("shell discovery failed: %s", discovery.ForLLM)
	}
	revision := decodeNodeResult(t, discovery)["discovery_revision"]
	args := map[string]any{
		"target":  "build",
		"command": command.Name,
		"input": map[string]any{
			"profile": "owner", "script": "true", "cwd": "workspace",
			"env": map[string]any{}, "timeout_seconds": 5,
		},
		"discovery_revision": revision,
	}
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	oversizedInputs := []map[string]any{
		{
			"profile": "owner",
			"script":  strings.Repeat("界", nodes.MaxShellExecScriptBytes/3+1),
			"cwd":     "workspace", "env": map[string]any{}, "timeout_seconds": 5,
		},
		{
			"profile": "owner", "script": "true", "cwd": "workspace",
			"env": map[string]any{
				"LANG": strings.Repeat("x", nodes.MaxShellExecEnvironmentBytes/2),
				"TERM": strings.Repeat("y", nodes.MaxShellExecEnvironmentBytes/2),
			},
			"timeout_seconds": 5,
		},
	}
	for _, oversizedInput := range oversizedInputs {
		oversizedArgs := maps.Clone(args)
		oversizedArgs["input"] = oversizedInput
		result := tool.Execute(ctx, oversizedArgs)
		assertNodeDenialResult(
			t,
			result,
			nodeDenialConstraintViolation,
			nodeConstraintInputSize,
			nodeActionCorrectInput,
		)
	}
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"oversized shell input prepared or dispatched: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
	invalidArgs := maps.Clone(args)
	invalidInput := maps.Clone(args["input"].(map[string]any))
	invalidInput["profile"] = "invented"
	invalidArgs["input"] = invalidInput
	invalid := tool.Execute(ctx, invalidArgs)
	assertNodeDenialResult(
		t,
		invalid,
		nodeDenialConstraintViolation,
		nodeConstraintProfile,
		nodeActionCorrectInput,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"unknown shell profile prepared or dispatched: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
	rawAuthorityArgs := maps.Clone(args)
	rawAuthorityInput := maps.Clone(args["input"].(map[string]any))
	rawAuthorityInput["shell_path"] = "/bin/sh"
	rawAuthorityArgs["input"] = rawAuthorityInput
	rawAuthority := tool.Execute(ctx, rawAuthorityArgs)
	assertNodeDenialResult(
		t,
		rawAuthority,
		nodeDenialSchemaInvalid,
		nodeConstraintInputSchema,
		nodeActionCorrectInput,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"raw shell authority prepared or dispatched: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}
	result := tool.Execute(ctx, args)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialApprovalRequired,
		nodeConstraintApproval,
		nodeActionAskOperator,
	)
	if source.dispatchCalls != 0 {
		t.Fatalf("shell dispatched without human approval: %d", source.dispatchCalls)
	}
	result = tool.Execute(WithToolApprovalContinuation(ctx, true), args)
	if result.IsError || source.dispatchCalls != 1 {
		t.Fatalf("approved shell result = %s, dispatches = %d", result.ForLLM, source.dispatchCalls)
	}

	bypassSource := newFakeNodeInvocationSource(t)
	bypassSnapshot := bypassSource.byRef["builder-node"]
	bypassSnapshot.Catalog = catalog
	bypassSnapshot.CatalogHash = catalogHash
	bypassSource.byRef["builder-node"] = bypassSnapshot
	bypassSource.registrations[bypassSnapshot.ID] = nodes.Registration{
		Snapshot:            bypassSnapshot,
		AllowedCommands:     []string{command.Name},
		ApprovedCatalogHash: catalogHash,
		ApprovedAt:          1,
	}
	bypassTool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), bypassSource)
	bypassCtx := nodeInvocationTestContext("actor-1", "call-shell-bypass")
	result = bypassTool.Execute(WithToolApprovalBypass(bypassCtx, true), args)
	if result.IsError || bypassSource.prepareCalls != 1 || bypassSource.dispatchCalls != 1 {
		t.Fatalf(
			"allow-all shell result = %s, prepares = %d, dispatches = %d",
			result.ForLLM,
			bypassSource.prepareCalls,
			bypassSource.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolRejectsStaleDiscoveryBeforePreparation(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	args := nodeInvocationTestArgs()
	args["discovery_revision"] = "dr_v1_stale"

	result := tool.Execute(nodeInvocationTestContext("actor-1", "call-stale"), args)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialDiscoveryStale,
		nodeConstraintCommandPolicy,
		nodeActionRefreshDiscovery,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"stale invocation prepared or dispatched: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolMarksOnlyStructuredApprovalDenialAsModelSafe(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	args := nodeInvocationTestArgs()
	args["discovery_revision"] = "dr_v1_stale-secret-value"
	_, err := tool.ApprovalArguments(
		nodeInvocationTestContext("actor-1", "call-stale-approval"),
		args,
	)
	result, safe := SafeApprovalDenialResult(err)
	if !safe {
		t.Fatalf("approval denial was not marked model-safe: %v", err)
	}
	assertNodeDenialResult(
		t,
		result,
		nodeDenialDiscoveryStale,
		nodeConstraintCommandPolicy,
		nodeActionRefreshDiscovery,
	)
	if strings.Contains(result.ForLLM, "stale-secret-value") {
		t.Fatalf("approval denial leaked rejected revision: %s", result.ForLLM)
	}
	if _, safe := SafeApprovalDenialResult(errors.New("private approval failure")); safe {
		t.Fatal("ordinary approval error was marked model-safe")
	}
}

func TestNodeInvokeToolRejectsAliasReassignmentBeforePreparation(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	original := source.byRef["builder-node"]
	replacement := original
	replacement.ID = "replacement-private-node-id"
	registration := source.registrations[original.ID]
	registration.Snapshot = replacement
	source.byRef["builder-node"] = replacement
	source.registrations[replacement.ID] = registration
	source.connected[replacement.ID] = true

	result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(
		nodeInvocationTestContext("actor-1", "call-alias-move"),
		nodeInvocationTestArgs(),
	)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialDiscoveryStale,
		nodeConstraintCommandPolicy,
		nodeActionRefreshDiscovery,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"alias reassignment prepared or dispatched: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolRevalidatesAuthorityInsidePreparationLease(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.beforeAuthorityValidate = func() {
		snapshot := source.byRef["builder-node"]
		snapshot.PolicyRevision = "policy-raced"
		source.byRef["builder-node"] = snapshot
		registration := source.registrations[snapshot.ID]
		registration.Snapshot = snapshot
		source.registrations[snapshot.ID] = registration
	}

	result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(
		nodeInvocationTestContext("actor-1", "call-authority-race"),
		nodeInvocationTestArgs(),
	)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialDiscoveryStale,
		nodeConstraintCommandPolicy,
		nodeActionRefreshDiscovery,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"authority race mutated invocation: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolTargetGrantOrBindingChangeMakesDiscoveryStale(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.Config)
	}{
		{
			name: "target grant",
			mutate: func(cfg *config.Config) {
				cfg.Agents.Defaults.TargetPolicy.DefaultTarget = "cold"
				cfg.Agents.Defaults.TargetPolicy.AllowedTargets = []string{"cold"}
			},
		},
		{
			name: "target binding",
			mutate: func(cfg *config.Config) {
				binding := cfg.Execution.Targets["build"]
				binding.Node = "replacement-node"
				cfg.Execution.Targets["build"] = binding
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := newFakeNodeInvocationSource(t)
			cfg := nodeDiscoveryTestConfig()
			test.mutate(cfg)
			result := NewNodeInvokeTool(cfg, source).Execute(
				nodeInvocationTestContext("actor-1", "call-target-stale"),
				nodeInvocationTestArgs(),
			)
			assertNodeDenialResult(
				t,
				result,
				nodeDenialDiscoveryStale,
				nodeConstraintCommandPolicy,
				nodeActionRefreshDiscovery,
			)
			if source.prepareCalls != 0 || source.dispatchCalls != 0 {
				t.Fatalf(
					"changed target mutated invocation: prepare=%d dispatch=%d",
					source.prepareCalls,
					source.dispatchCalls,
				)
			}
		})
	}
}

func TestNodeInvokeToolReportsCatalogReapprovalRequirement(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	args := nodeInvocationTestArgs()
	snapshot := source.byRef["builder-node"]
	registration := source.registrations[snapshot.ID]
	registration.ApprovedCatalogHash = strings.Repeat("a", 64)
	source.registrations[snapshot.ID] = registration

	result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(
		nodeInvocationTestContext("actor-1", "call-reapproval-stale"),
		args,
	)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialReapprovalRequired,
		nodeConstraintCommandPolicy,
		nodeActionAskOperator,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"catalog reapproval prepared or dispatched: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolRejectsLocallyUnavailableCommandBeforePreparation(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	snapshot := source.byRef["builder-node"]
	command := snapshot.Catalog.Commands[0]
	command.ModelContract = &nodes.CommandModelContract{
		Availability:      nodes.ModelUnavailable,
		TimeoutSecondsMax: 30,
		OutputBytesMax:    4096,
		ResultKind:        "json",
		Guidance:          []string{},
		Examples:          []json.RawMessage{},
	}
	snapshot.Catalog = nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	snapshot.CatalogHash = mustCatalogHash(t, snapshot.Catalog)
	source.byRef["builder-node"] = snapshot
	registration := source.registrations[snapshot.ID]
	registration.Snapshot = snapshot
	registration.ApprovedCatalogHash = snapshot.CatalogHash
	source.registrations[snapshot.ID] = registration

	ctx := nodeInvocationTestContext("actor-1", "call-unavailable")
	discovered := decodeNodeResult(t, NewNodeDiscoveryTool(
		nodeDiscoveryTestConfig(),
		source,
	).Execute(ctx, map[string]any{
		"action":  "describe",
		"target":  "build",
		"command": command.Name,
	}))
	args := nodeInvocationTestArgs()
	args["discovery_revision"] = discovered["discovery_revision"]
	result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(ctx, args)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialCommandUnavailable,
		nodeConstraintCommandPolicy,
		nodeActionAskOperator,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"locally unavailable invocation mutated state: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolReturnsSafeConstraintDenials(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(map[string]any)
		constraint string
	}{
		{
			name: "executable alias",
			mutate: func(args map[string]any) {
				args["input"] = map[string]any{"argv": []any{"/secret/bin/git", "status"}}
			},
			constraint: nodeConstraintExecutable,
		},
		{
			name: "working scope",
			mutate: func(args map[string]any) {
				args["input"] = map[string]any{
					"argv": []any{"git", "status"},
					"cwd":  "/secret/worktree",
				}
			},
			constraint: nodeConstraintWorkingScope,
		},
		{
			name: "environment name",
			mutate: func(args map[string]any) {
				args["input"] = map[string]any{
					"argv": []any{"git", "status"},
					"env":  map[string]any{"SECRET_TOKEN": "do-not-return"},
				}
			},
			constraint: nodeConstraintEnvironment,
		},
		{
			name: "command timeout",
			mutate: func(args map[string]any) {
				args["input"] = map[string]any{
					"argv":            []any{"git", "status"},
					"timeout_seconds": 31,
				}
			},
			constraint: nodeConstraintTimeout,
		},
		{
			name: "tool timeout",
			mutate: func(args map[string]any) {
				args["timeout_seconds"] = 31
			},
			constraint: nodeConstraintTimeout,
		},
		{
			name: "output limit",
			mutate: func(args map[string]any) {
				args["output_limit_bytes"] = 4097
			},
			constraint: nodeConstraintOutputLimit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := newFakeNodeInvocationSource(t)
			args := nodeInvocationTestArgs()
			test.mutate(args)
			result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(
				nodeInvocationTestContext("actor-1", "call-constraint"),
				args,
			)
			assertNodeDenialResult(
				t,
				result,
				nodeDenialConstraintViolation,
				test.constraint,
				nodeActionCorrectInput,
			)
			for _, secret := range []string{
				"/secret/bin/git",
				"/secret/worktree",
				"SECRET_TOKEN",
				"do-not-return",
			} {
				if strings.Contains(result.ForLLM, secret) {
					t.Fatalf("denial leaked rejected value %q: %s", secret, result.ForLLM)
				}
			}
			if source.prepareCalls != 0 || source.dispatchCalls != 0 {
				t.Fatalf(
					"constraint denial mutated invocation: prepare=%d dispatch=%d",
					source.prepareCalls,
					source.dispatchCalls,
				)
			}
		})
	}
}

func TestNodeInvokeToolReturnsSafeSchemaDenial(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	args := nodeInvocationTestArgs()
	args["input"] = map[string]any{"argv": "secret malformed argv"}
	result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(
		nodeInvocationTestContext("actor-1", "call-schema"),
		args,
	)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialSchemaInvalid,
		nodeConstraintInputSchema,
		nodeActionCorrectInput,
	)
	if strings.Contains(result.ForLLM, "secret malformed argv") {
		t.Fatalf("schema denial leaked rejected input: %s", result.ForLLM)
	}
}

func TestNodeInvokeToolDeniesDisconnectedTargetAfterFreshDiscovery(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.connected["private-node-id"] = false
	ctx := nodeInvocationTestContext("actor-1", "call-disconnected")
	args := freshNodeInvocationArgs(t, source, ctx)
	result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(ctx, args)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialTargetUnavailable,
		nodeConstraintCommandPolicy,
		nodeActionRefreshDiscovery,
	)
	if source.prepareCalls != 0 || source.dispatchCalls != 0 {
		t.Fatalf(
			"disconnected target mutated invocation: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolDeniesPartiallyDescribedCommand(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	snapshot := source.byRef["builder-node"]
	command := snapshot.Catalog.Commands[0]
	command.ModelContract = nil
	snapshot.Catalog = nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	snapshot.CatalogHash = mustCatalogHash(t, snapshot.Catalog)
	source.byRef["builder-node"] = snapshot
	registration := source.registrations[snapshot.ID]
	registration.Snapshot = snapshot
	registration.ApprovedCatalogHash = snapshot.CatalogHash
	source.registrations[snapshot.ID] = registration

	ctx := nodeInvocationTestContext("actor-1", "call-partial")
	args := freshNodeInvocationArgs(t, source, ctx)
	result := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(ctx, args)
	assertNodeDenialResult(
		t,
		result,
		nodeDenialDiscoveryIncomplete,
		nodeConstraintInputSchema,
		nodeActionRefreshDiscovery,
	)
}

func TestNodeInvokeToolApprovalResumeCannotRefreshRetainedAuthority(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-resume-stale")
	args := nodeInvocationTestArgs()
	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}

	snapshot := source.byRef["builder-node"]
	snapshot.PolicyRevision = "policy-2"
	source.byRef["builder-node"] = snapshot
	registration := source.registrations[snapshot.ID]
	registration.Snapshot = snapshot
	source.registrations[snapshot.ID] = registration

	if _, err := tool.ApprovalArguments(
		WithToolApprovalContinuation(ctx, true),
		args,
	); !errors.Is(err, errDiscoveryStale) {
		t.Fatalf("approval resume with stale discovery error = %v", err)
	}
	if source.prepareCalls != 1 || source.dispatchCalls != 0 {
		t.Fatalf(
			"stale resume mutated invocation: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}

	discovery := NewNodeDiscoveryTool(nodeDiscoveryTestConfig(), source)
	discovered := decodeNodeResult(t, discovery.Execute(ctx, map[string]any{
		"action":  "describe",
		"target":  "build",
		"command": "system.exec.v1",
	}))
	freshArgs := nodeInvocationTestArgs()
	freshArgs["discovery_revision"] = discovered["discovery_revision"]
	if _, err := tool.ApprovalArguments(
		WithToolApprovalContinuation(ctx, true),
		freshArgs,
	); !errors.Is(err, errDiscoveryStale) {
		t.Fatalf("approval resume replaced retained authority: %v", err)
	}
	if source.prepareCalls != 1 || source.dispatchCalls != 0 {
		t.Fatalf(
			"fresh resume replaced invocation: prepare=%d dispatch=%d",
			source.prepareCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeInvokeToolNamespacesProviderCallByExecutionAndWorkspace(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	args := nodeInvocationTestArgs()
	firstCtx := nodeInvocationTestContext("actor-1", "reused-call")
	first, err := tool.ApprovalArguments(firstCtx, args)
	if err != nil {
		t.Fatal(err)
	}

	nextExecutionCtx := WithToolExecutionIdentity(
		nodeInvocationTestContext("actor-1", "reused-call"),
		"/workspace/main",
		"execution-2",
	)
	nextExecution, err := tool.ApprovalArguments(nextExecutionCtx, args)
	if err != nil {
		t.Fatal(err)
	}
	otherWorkspaceCtx := WithToolExecutionIdentity(
		nodeInvocationTestContext("actor-1", "reused-call"),
		"/workspace/other",
		"execution-1",
	)
	otherWorkspace, err := tool.ApprovalArguments(otherWorkspaceCtx, args)
	if err != nil {
		t.Fatal(err)
	}
	if first["invocation_id"] == nextExecution["invocation_id"] ||
		first["invocation_id"] == otherWorkspace["invocation_id"] ||
		nextExecution["invocation_id"] == otherWorkspace["invocation_id"] {
		t.Fatalf(
			"execution namespaces collided: first=%v next=%v workspace=%v",
			first["invocation_id"],
			nextExecution["invocation_id"],
			otherWorkspace["invocation_id"],
		)
	}
}

func TestNodeInvokeToolApprovalResumeRetainsOriginExecutionIdentity(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	first, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs())
	if err != nil {
		t.Fatal(err)
	}
	resumedCtx := WithToolApprovalContinuation(
		WithToolExecutionIdentity(ctx, "/workspace/main", "execution-1"),
		true,
	)
	resumed, err := tool.ApprovalArguments(resumedCtx, nodeInvocationTestArgs())
	if err != nil {
		t.Fatal(err)
	}
	if resumed["invocation_id"] != first["invocation_id"] || source.prepareCalls != 1 {
		t.Fatalf("approval resume changed authority: first=%#v resumed=%#v", first, resumed)
	}
}

func TestNodeInvokeToolRejectsChangedArgumentsAfterPreparation(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	if _, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs()); err != nil {
		t.Fatal(err)
	}
	changed := nodeInvocationTestArgs()
	changed["input"] = map[string]any{"argv": []any{"git", "diff"}}
	if _, err := tool.ApprovalArguments(ctx, changed); !errors.Is(err, errDiscoveryStale) {
		t.Fatalf("changed approval error = %v", err)
	}
}

func TestNodeInvokeToolDoesNotReplaceExpiredAuthorityOnApprovalResume(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	if _, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs()); err != nil {
		t.Fatal(err)
	}
	source.lookupMiss = true
	ctx = WithToolApprovalContinuation(ctx, true)
	if _, err := tool.ApprovalArguments(
		ctx,
		nodeInvocationTestArgs(),
	); !errors.Is(err, errDiscoveryStale) {
		t.Fatalf("approval resume error = %v", err)
	}
	if source.prepareCalls != 1 {
		t.Fatalf("approval resume minted new authority; prepare calls = %d", source.prepareCalls)
	}
}

func TestNodeInvokeToolReportsPostDispatchUncertaintyWithoutReplay(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.dispatchErr = errors.New("transport closed")
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	result := tool.Execute(
		nodeInvocationTestContext("actor-1", "call-1"),
		nodeInvocationTestArgs(),
	)
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_UNCERTAIN") ||
		!strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("uncertain dispatch = %#v", result)
	}
}

func TestNodeInvocationEventsUseProvenStatesAndRedactPayloads(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	eventBus := &recordingNodeEventBus{}
	tool.SetEventPublisher(eventBus)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	args := nodeInvocationTestArgs()
	args["input"] = map[string]any{
		"argv": []any{"git", "status", "super-secret-command-input"},
	}

	if _, err := tool.ApprovalArguments(ctx, args); err != nil {
		t.Fatal(err)
	}
	if result := tool.Execute(ctx, args); result.IsError {
		t.Fatalf("nodes_invoke failed: %s", result.ForLLM)
	}

	events := eventBus.snapshot()
	wantObservations := []string{
		NodeInvocationObservationPrepared,
		NodeInvocationObservationDispatched,
		NodeInvocationObservationCompleted,
	}
	if len(events) != len(wantObservations) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(wantObservations), events)
	}
	for index, event := range events {
		if event.Kind != runtimeevents.KindNodeInvocationObserved {
			t.Fatalf("event[%d].Kind = %q, want %q", index, event.Kind, runtimeevents.KindNodeInvocationObserved)
		}
		payload, ok := event.Payload.(NodeInvocationEventPayload)
		if !ok {
			t.Fatalf("event[%d].Payload = %T", index, event.Payload)
		}
		if payload.Observation != wantObservations[index] {
			t.Fatalf(
				"event[%d].Observation = %q, want %q",
				index,
				payload.Observation,
				wantObservations[index],
			)
		}
		if payload.Target != "build" || payload.Command != "system.exec.v1" ||
			payload.InvocationID == "" {
			t.Fatalf("event[%d] payload = %#v", index, payload)
		}
		if event.Scope.Workspace != "/workspace/main" ||
			event.Scope.TurnID != "execution-1" ||
			event.Scope.AgentID != "main" ||
			event.Scope.SessionKey != "route-session" ||
			event.Scope.Channel != "telegram" ||
			event.Scope.ChatID != "chat-1" ||
			event.Scope.SenderID != "actor-1" ||
			event.Correlation.RequestID != "call-1" {
			t.Fatalf("event[%d] scope = %#v correlation = %#v", index, event.Scope, event.Correlation)
		}
		wantGatewayState := nodes.GatewayInvocationPrepared
		if index > 0 {
			wantGatewayState = nodes.GatewayInvocationDispatched
		}
		if payload.GatewayState != wantGatewayState {
			t.Fatalf(
				"event[%d] gateway state = %q, want %q",
				index,
				payload.GatewayState,
				wantGatewayState,
			)
		}
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"super-secret-command-input",
		"private-node-id",
		"plan_hash",
		"policy_revision",
		`\"stdout\"`,
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("audit events leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestNodeInvocationEventsReportUncertainThenObservedFailure(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.dispatchErr = errors.New("sensitive transport endpoint disconnected")
	eventBus := &recordingNodeEventBus{}
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	invoke.SetEventPublisher(eventBus)
	ctx := nodeInvocationTestContext("actor-1", "call-1")

	result := invoke.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError {
		t.Fatalf("nodes_invoke = %#v, want uncertain error", result)
	}
	invocationID := invocationIDFromError(t, result)
	record := mustFakeGatewayInvocation(t, source, ctx, invocationID)
	source.remote = failedRemoteInvocation(record)

	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	status.SetEventPublisher(eventBus)
	statusResult := status.Execute(ctx, map[string]any{"invocation_id": invocationID})
	if statusResult.IsError {
		t.Fatalf("nodes_status failed: %#v", statusResult)
	}
	repeatedStatusResult := status.Execute(ctx, map[string]any{"invocation_id": invocationID})
	if repeatedStatusResult.IsError {
		t.Fatalf("repeated nodes_status failed: %#v", repeatedStatusResult)
	}

	events := eventBus.snapshot()
	wantObservations := []string{
		NodeInvocationObservationPrepared,
		NodeInvocationObservationDispatched,
		NodeInvocationObservationUncertain,
		NodeInvocationObservationStatus,
		NodeInvocationObservationStatus,
	}
	if len(events) != len(wantObservations) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(wantObservations), events)
	}
	for index, observation := range wantObservations {
		if events[index].Kind != runtimeevents.KindNodeInvocationObserved {
			t.Fatalf(
				"event[%d].Kind = %q, want %q",
				index,
				events[index].Kind,
				runtimeevents.KindNodeInvocationObserved,
			)
		}
		payload := events[index].Payload.(NodeInvocationEventPayload)
		if payload.Observation != observation {
			t.Fatalf("event[%d].Observation = %q, want %q", index, payload.Observation, observation)
		}
	}
	uncertain := events[2].Payload.(NodeInvocationEventPayload)
	if uncertain.State != string(nodes.InvocationUnknown) ||
		uncertain.ErrorCode != "DISPATCH_UNCERTAIN" ||
		events[2].Severity != runtimeevents.SeverityWarn {
		t.Fatalf("uncertain event = %#v", events[2])
	}
	for _, index := range []int{3, 4} {
		observed := events[index].Payload.(NodeInvocationEventPayload)
		if observed.State != string(nodes.InvocationFailed) ||
			observed.ErrorCode != "REMOTE_FAILED" {
			t.Fatalf("status observed event[%d] = %#v", index, events[index])
		}
	}
	for _, event := range events[3:] {
		payload := event.Payload.(NodeInvocationEventPayload)
		if payload.Observation == NodeInvocationObservationCompleted {
			t.Fatalf("nodes_status emitted completion observation: %#v", event)
		}
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "sensitive transport endpoint") ||
		strings.Contains(string(encoded), "remote failure detail") {
		t.Fatalf("audit events leaked errors: %s", encoded)
	}
}

func TestNodeInvocationPreparedEventEmittedOnceForConcurrentToolCall(t *testing.T) {
	base := newFakeNodeInvocationSource(t)
	base.lookupMiss = true
	source := &atomicPrepareNodeInvocationSource{fakeNodeInvocationSource: base}
	eventBus := &recordingNodeEventBus{}
	tools := []*NodeInvokeTool{
		NewNodeInvokeTool(nodeDiscoveryTestConfig(), source),
		NewNodeInvokeTool(nodeDiscoveryTestConfig(), source),
	}
	for _, tool := range tools {
		tool.SetEventPublisher(eventBus)
	}
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	start := make(chan struct{})
	results := make(chan error, len(tools))
	for _, tool := range tools {
		go func() {
			<-start
			_, err := tool.ApprovalArguments(ctx, nodeInvocationTestArgs())
			results <- err
		}()
	}
	close(start)
	successes := 0
	for range tools {
		err := <-results
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, nodes.ErrGatewayInvocationConflict) {
			t.Fatalf("concurrent prepare failed: %v", err)
		}
	}
	if successes == 0 {
		t.Fatal("concurrent prepare did not create canonical authority")
	}

	events := eventBus.snapshot()
	if len(events) != 1 || events[0].Kind != runtimeevents.KindNodeInvocationObserved {
		t.Fatalf("prepared observations = %#v, want one creation observation", events)
	}
	payload := events[0].Payload.(NodeInvocationEventPayload)
	if payload.Observation != NodeInvocationObservationPrepared {
		t.Fatalf("observation = %q, want %q", payload.Observation, NodeInvocationObservationPrepared)
	}
}

func TestNodeInvokeToolTreatsAlreadyDispatchedAsUncertain(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	if result := tool.Execute(ctx, nodeInvocationTestArgs()); result.IsError {
		t.Fatalf("first invoke = %#v", result)
	}
	result := tool.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_UNCERTAIN") ||
		!strings.Contains(result.ForLLM, "nodes_status") ||
		source.dispatchCalls != 1 {
		t.Fatalf("repeated invoke = %#v, dispatch calls = %d", result, source.dispatchCalls)
	}
}

func TestNodeInvokeToolDistinguishesPreDispatchDenial(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	source.preDispatchErr = errors.New("durable authority unavailable")
	tool := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	result := tool.Execute(ctx, nodeInvocationTestArgs())
	if !result.IsError ||
		!strings.Contains(result.ForLLM, "DISPATCH_DENIED") ||
		strings.Contains(result.ForLLM, "nodes_status") {
		t.Fatalf("pre-dispatch denial = %#v", result)
	}
	record := mustFakeGatewayInvocation(t, source, ctx, invocationIDFromError(t, result))
	if record.State != nodes.GatewayInvocationPrepared {
		t.Fatalf("pre-dispatch state = %q", record.State)
	}
}

func TestNodeStatusToolIsActorScopedAndRecoversResult(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		invoke.Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	record := mustFakeGatewayInvocation(t, source, ctx, invocationID)
	source.remote = successfulRemoteInvocation(record)

	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	payload := decodeNodeResult(
		t,
		status.Execute(ctx, map[string]any{"invocation_id": invocationID}),
	)
	if payload["state"] != string(nodes.InvocationSucceeded) {
		t.Fatalf("status = %#v", payload)
	}
	denied := status.Execute(
		nodeInvocationTestContext("actor-2", "status-call"),
		map[string]any{"invocation_id": invocationID},
	)
	if !denied.IsError || !strings.Contains(denied.ForLLM, "not found in this scope") {
		t.Fatalf("cross-actor status = %#v", denied)
	}
}

func TestNodeStatusToolReportsDisconnectedDispatchedInvocationAsUnknown(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		invoke.Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	source.connected = map[nodes.ID]bool{}

	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	payload := decodeNodeResult(
		t,
		status.Execute(ctx, map[string]any{"invocation_id": invocationID}),
	)
	if payload["state"] != string(nodes.InvocationUnknown) ||
		payload["error_code"] != "NODE_UNAVAILABLE" ||
		payload["node_available"] != false ||
		source.queryCalls != 0 {
		t.Fatalf("offline status = %#v, query calls = %d", payload, source.queryCalls)
	}
}

func TestNodeStatusToolReturnsPreparedStateWithoutQuery(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	invoke := NewNodeInvokeTool(nodeDiscoveryTestConfig(), source)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	approval, err := invoke.ApprovalArguments(ctx, nodeInvocationTestArgs())
	if err != nil {
		t.Fatal(err)
	}
	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	payload := decodeNodeResult(t, status.Execute(ctx, map[string]any{
		"invocation_id": approval["invocation_id"],
	}))
	if payload["state"] != string(nodes.GatewayInvocationPrepared) || source.queryCalls != 0 {
		t.Fatalf("prepared status = %#v, query calls = %d", payload, source.queryCalls)
	}
}

func TestNodeStatusToolRecoversAfterBoundedTransientQueriesWithoutRedispatch(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	record := mustFakeGatewayInvocation(t, source, ctx, invocationID)
	source.remote = successfulRemoteInvocation(record)
	source.queryErrors = []error{
		nodes.NewInvocationQueryError(nodes.InvocationQueryTransportUnavailable, errors.New("private endpoint")),
		nodes.NewInvocationQueryError(nodes.InvocationQueryNotFound, nil),
	}

	payload := decodeNodeResult(
		t,
		NewNodeStatusTool(nodeDiscoveryTestConfig(), source).Execute(
			ctx,
			map[string]any{"invocation_id": invocationID},
		),
	)
	if payload["state"] != string(nodes.InvocationSucceeded) ||
		payload["status_attempts"] != float64(defaultNodeStatusAttempts) ||
		source.queryCalls != defaultNodeStatusAttempts ||
		source.dispatchCalls != 1 {
		t.Fatalf(
			"recovered status = %#v; query calls = %d; dispatch calls = %d",
			payload,
			source.queryCalls,
			source.dispatchCalls,
		)
	}
}

func TestNodeStatusToolPreservesSafeFailureClassificationAfterBoundedPolling(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	privateCause := errors.New("dial secret.internal.example:1234")
	source.queryErr = nodes.NewInvocationQueryError(nodes.InvocationQueryNodeUnavailable, privateCause)
	eventBus := &recordingNodeEventBus{}
	status := NewNodeStatusTool(nodeDiscoveryTestConfig(), source)
	status.SetEventPublisher(eventBus)

	payload := decodeNodeResult(t, status.Execute(ctx, map[string]any{"invocation_id": invocationID}))
	if payload["state"] != string(nodes.InvocationUnknown) ||
		payload["error_code"] != nodes.InvocationQueryNodeUnavailable ||
		payload["status_attempts"] != float64(defaultNodeStatusAttempts) ||
		source.queryCalls != defaultNodeStatusAttempts ||
		source.dispatchCalls != 1 {
		t.Fatalf(
			"uncertain status = %#v; query calls = %d; dispatch calls = %d",
			payload,
			source.queryCalls,
			source.dispatchCalls,
		)
	}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	encodedEvents, err := json.Marshal(eventBus.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedPayload), "secret.internal") ||
		strings.Contains(string(encodedEvents), "secret.internal") {
		t.Fatalf("status diagnostics leaked transport details: payload=%s events=%s", encodedPayload, encodedEvents)
	}
}

func TestNodeStatusToolDoesNotRetryLedgerFailure(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	source.queryErr = nodes.NewInvocationQueryError(nodes.InvocationQueryLedgerUnavailable, nil)

	payload := decodeNodeResult(
		t,
		NewNodeStatusTool(nodeDiscoveryTestConfig(), source).Execute(
			ctx,
			map[string]any{"invocation_id": invocationID},
		),
	)
	if payload["error_code"] != nodes.InvocationQueryLedgerUnavailable ||
		payload["status_attempts"] != float64(1) || source.queryCalls != 1 {
		t.Fatalf("ledger status = %#v; query calls = %d", payload, source.queryCalls)
	}
}

func TestNodeCancelToolIsIdempotentAndRequiresExactExecutionScope(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	invocationID := decodeNodeResult(
		t,
		NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(ctx, nodeInvocationTestArgs()),
	)["invocation_id"].(string)
	record := mustFakeGatewayInvocation(t, source, ctx, invocationID)
	now := time.Now().UnixNano()
	source.remote = nodes.InvocationRecord{
		InvocationID: record.Plan.InvocationID, IdempotencyKey: record.Plan.IdempotencyKey,
		PlanHash: record.ExpectedPlanHash, NodeID: record.Plan.NodeID,
		CatalogHash: record.Plan.CatalogHash, Command: record.Plan.Command,
		Risk: record.Plan.Risk, State: nodes.InvocationRunning,
		AcceptedAt: now, UpdatedAt: now, ExpiresAt: record.Plan.ExpiresAt,
		Cancellation: &nodes.InvocationCancellation{RequestedAt: now},
	}
	cancel := NewNodeCancelTool(nodeDiscoveryTestConfig(), source)
	args := map[string]any{"invocation_id": invocationID}
	first := decodeNodeResult(t, cancel.Execute(ctx, args))
	repeated := decodeNodeResult(t, cancel.Execute(ctx, args))
	if first["status"] != "cancel_requested" ||
		repeated["status"] != "cancel_requested" ||
		source.cancelCalls != 1 {
		t.Fatalf("cancellation results = %#v, %#v; calls = %d", first, repeated, source.cancelCalls)
	}

	otherExecution := WithToolExecutionIdentity(ctx, "/workspace/main", "execution-2")
	denied := decodeNodeResult(t, cancel.Execute(otherExecution, args))
	if denied["status"] != "denied" || source.cancelCalls != 1 {
		t.Fatalf("cross-execution cancellation = %#v; calls = %d", denied, source.cancelCalls)
	}
	otherWorkspace := WithToolExecutionIdentity(ctx, "/workspace/other", "execution-1")
	denied = decodeNodeResult(t, cancel.Execute(otherWorkspace, args))
	if denied["status"] != "denied" || source.cancelCalls != 1 {
		t.Fatalf("cross-workspace cancellation = %#v; calls = %d", denied, source.cancelCalls)
	}
}

func TestNodeCancelToolDistinguishesConfirmedAndTerminalOutcomes(t *testing.T) {
	for _, test := range []struct {
		name   string
		remote func(nodes.GatewayInvocationRecord) nodes.InvocationRecord
		want   string
	}{
		{
			name: "confirmed cancellation",
			remote: func(record nodes.GatewayInvocationRecord) nodes.InvocationRecord {
				now := time.Now().UnixNano()
				return nodes.InvocationRecord{
					InvocationID:   record.Plan.InvocationID,
					IdempotencyKey: record.Plan.IdempotencyKey,
					PlanHash:       record.ExpectedPlanHash, NodeID: record.Plan.NodeID,
					CatalogHash: record.Plan.CatalogHash, Command: record.Plan.Command,
					Risk: record.Plan.Risk, State: nodes.InvocationCanceled,
					AcceptedAt: now, UpdatedAt: now, CompletedAt: now,
					ExpiresAt: record.Plan.ExpiresAt,
					Failure:   &nodes.InvocationFailure{Code: "CANCELED", Message: "canceled"},
					Cancellation: &nodes.InvocationCancellation{
						RequestedAt: now, TerminationConfirmed: true,
					},
				}
			},
			want: "canceled",
		},
		{name: "completion won", remote: successfulRemoteInvocation, want: "already_terminal"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := newFakeNodeInvocationSource(t)
			ctx := nodeInvocationTestContext("actor-1", "call-1")
			invocationID := decodeNodeResult(
				t,
				NewNodeInvokeTool(nodeDiscoveryTestConfig(), source).Execute(
					ctx,
					nodeInvocationTestArgs(),
				),
			)["invocation_id"].(string)
			record := mustFakeGatewayInvocation(t, source, ctx, invocationID)
			source.remote = test.remote(record)
			payload := decodeNodeResult(
				t,
				NewNodeCancelTool(nodeDiscoveryTestConfig(), source).Execute(
					ctx,
					map[string]any{"invocation_id": invocationID},
				),
			)
			if payload["status"] != test.want {
				t.Fatalf("cancellation = %#v, want %q", payload, test.want)
			}
		})
	}
}

func TestNodeCancelToolPersistsOfflineIntentWithoutReplay(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	ctx := nodeInvocationTestContext("actor-1", "call-1")
	approval, err := NewNodeInvokeTool(
		nodeDiscoveryTestConfig(),
		source,
	).ApprovalArguments(ctx, nodeInvocationTestArgs())
	if err != nil {
		t.Fatal(err)
	}
	cancel := NewNodeCancelTool(nodeDiscoveryTestConfig(), source)
	args := map[string]any{"invocation_id": approval["invocation_id"]}
	prepared := decodeNodeResult(t, cancel.Execute(ctx, args))
	if prepared["status"] != "already_terminal" || source.cancelCalls != 0 {
		t.Fatalf("prepared cancellation = %#v; calls = %d", prepared, source.cancelCalls)
	}

	record := mustFakeGatewayInvocation(t, source, ctx, approval["invocation_id"].(string))
	owner := nodes.GatewayInvocationOwner{
		Target: record.Target, AgentID: record.Plan.AgentID, SessionID: record.Plan.SessionID,
		ActorID: record.Plan.ActorID, ToolCallID: record.ToolCallID,
		WorkspaceID: record.WorkspaceID, ExecutionID: record.ExecutionID,
	}
	if _, _, err := source.store.MarkDispatched(
		owner,
		record.Plan.InvocationID,
		record.ExpectedPlanHash,
	); err != nil {
		t.Fatal(err)
	}
	source.connected = map[nodes.ID]bool{}
	source.cancelErr = errors.New("node unavailable")
	offline := decodeNodeResult(t, cancel.Execute(ctx, args))
	retained := mustFakeGatewayInvocation(t, source, ctx, record.Plan.InvocationID)
	if offline["status"] != "unknown" || source.cancelCalls != 1 ||
		retained.Cancellation == nil {
		t.Fatalf("offline cancellation = %#v; calls = %d", offline, source.cancelCalls)
	}
	source.connected = map[nodes.ID]bool{record.Plan.NodeID: true}
	source.cancelErr = nil
	now := time.Now().UnixNano()
	source.remote = nodes.InvocationRecord{
		InvocationID: record.Plan.InvocationID, IdempotencyKey: record.Plan.IdempotencyKey,
		PlanHash: record.ExpectedPlanHash, NodeID: record.Plan.NodeID,
		CatalogHash: record.Plan.CatalogHash, Command: record.Plan.Command,
		Risk: record.Plan.Risk, State: nodes.InvocationRunning,
		AcceptedAt: now, UpdatedAt: now, ExpiresAt: record.Plan.ExpiresAt,
	}
	repeated := decodeNodeResult(t, cancel.Execute(ctx, args))
	if repeated["status"] != "unknown" || source.cancelCalls != 1 {
		t.Fatalf("repeated cancellation = %#v; calls = %d", repeated, source.cancelCalls)
	}
}

func TestNodeInvocationToolRuntimeSemantics(t *testing.T) {
	source := newFakeNodeInvocationSource(t)
	if got := NewNodeInvokeTool(nil, source).ToolLoopSemantics(); got != loopguard.SemanticsMutating {
		t.Fatalf("invoke semantics = %q", got)
	}
	if got := NewNodeStatusTool(nil, source).ToolLoopSemantics(); got !=
		loopguard.SemanticsReadOnlyIdempotent {
		t.Fatalf("status semantics = %q", got)
	}
	if got := NewNodeCancelTool(nil, source).ToolLoopSemantics(); got != loopguard.SemanticsMutating {
		t.Fatalf("cancel semantics = %q", got)
	}
}

func newFakeNodeInvocationSource(t *testing.T) *fakeNodeInvocationSource {
	t.Helper()
	command := nodeInvocationTestDescriptor()
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	catalogHash := mustCatalogHash(t, catalog)
	snapshot := nodes.Snapshot{
		ID:             "private-node-id",
		State:          nodes.StateConnected,
		Catalog:        catalog,
		CatalogHash:    catalogHash,
		Executor:       "local",
		PolicyRevision: "policy-1",
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
		filepath.Join(t.TempDir(), "node_invocations.json"),
		8,
		1024*1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeNodeInvocationSource{
		fakeNodeDiscoverySource: discovery,
		store:                   store,
	}
}

func nodeInvocationTestContext(actorID string, toolCallID string) context.Context {
	ctx := WithToolSessionContext(context.Background(), "main", "history-session", nil)
	ctx = WithToolRouteSessionKey(ctx, "route-session")
	ctx = WithToolExecutionIdentity(ctx, "/workspace/main", "execution-1")
	ctx = WithToolInboundContext(ctx, "telegram", "chat-1", "", "")
	ctx = WithToolInboundMetadata(ctx, bus.InboundContext{
		Channel: "telegram", ChatID: "chat-1", SenderID: actorID, ActorID: actorID,
	})
	return WithToolCallID(ctx, toolCallID)
}

func nodeInvocationTestArgs() map[string]any {
	command := nodeInvocationTestDescriptor()
	catalog := nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{command}}
	catalogHash, err := catalog.Hash()
	if err != nil {
		panic(err)
	}
	snapshot := nodes.Snapshot{
		ID:             "private-node-id",
		State:          nodes.StateConnected,
		Catalog:        catalog,
		CatalogHash:    catalogHash,
		Executor:       "local",
		PolicyRevision: "policy-1",
	}
	registration := nodes.Registration{
		Snapshot:            snapshot,
		AllowedCommands:     []string{command.Name},
		ApprovedCatalogHash: catalogHash,
		ApprovedAt:          1,
	}
	revision, err := newNodeTargetAccess(
		nodeDiscoveryTestConfig(),
		nil,
	).discoveryRevision("main", "build", command.Name, snapshot, registration, command, true)
	if err != nil {
		panic(err)
	}
	return map[string]any{
		"target":             "build",
		"command":            "system.exec.v1",
		"input":              map[string]any{"argv": []any{"git", "status"}},
		"discovery_revision": revision,
	}
}

func freshNodeInvocationArgs(
	t *testing.T,
	source NodeDiscoverySource,
	ctx context.Context,
) map[string]any {
	t.Helper()
	result := NewNodeDiscoveryTool(nodeDiscoveryTestConfig(), source).Execute(
		ctx,
		map[string]any{
			"action":  "describe",
			"target":  "build",
			"command": "system.exec.v1",
		},
	)
	if result.IsError {
		t.Fatalf("fresh node discovery failed: %s", result.ForLLM)
	}
	discovered := decodeNodeResult(t, result)
	return map[string]any{
		"target":             "build",
		"command":            "system.exec.v1",
		"input":              map[string]any{"argv": []any{"git", "status"}},
		"discovery_revision": discovered["discovery_revision"],
	}
}

func assertNodeDenialResult(
	t *testing.T,
	result *ToolResult,
	code string,
	constraint string,
	action string,
) {
	t.Helper()
	if result == nil || !result.IsError {
		t.Fatalf("node denial = %#v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.ForLLM), &payload); err != nil {
		t.Fatalf("decode node denial %q: %v", result.ForLLM, err)
	}
	if len(payload) != 4 ||
		payload["status"] != "denied" ||
		payload["code"] != code ||
		payload["constraint"] != constraint ||
		payload["action"] != action {
		t.Fatalf("node denial = %#v", payload)
	}
}

func nodeInvocationTestDescriptor() nodes.CommandDescriptor {
	command := testNodeCommand("system.exec.v1", nodes.RiskWrite, false, true)
	command.InputSchema = json.RawMessage(
		`{"type":"object","properties":{"argv":{"type":"array","items":{"type":"string"}}},"required":["argv"],"additionalProperties":false}`,
	)
	command.ModelContract = &nodes.CommandModelContract{
		Availability:      nodes.ModelAvailable,
		TimeoutSecondsMax: 30,
		OutputBytesMax:    4096,
		ResultKind:        "json",
		Constraints: nodes.CommandModelConstraints{
			ExecutableAliases: []string{"git"},
		},
		Guidance: []string{},
		Examples: []json.RawMessage{},
	}
	return command
}

func shellNodeInvocationTestDescriptor() nodes.CommandDescriptor {
	command := testNodeCommand("shell.exec.v1", nodes.RiskPrivileged, false, true)
	command.InputSchema = json.RawMessage(
		`{"type":"object","required":["profile","script","cwd","env","timeout_seconds"],"properties":{"profile":{"type":"string"},"script":{"type":"string"},"cwd":{"type":"string"},"env":{"type":"object"},"timeout_seconds":{"type":"integer"}},"additionalProperties":false}`,
	)
	command.ModelContract = &nodes.CommandModelContract{
		Availability:      nodes.ModelAvailable,
		TimeoutSecondsMax: 30,
		OutputBytesMax:    4096,
		ResultKind:        "json",
		AuthorityDigest:   strings.Repeat("b", 64),
		ApprovalMode:      "each_command",
		Constraints: nodes.CommandModelConstraints{
			ProfileAliases: []string{"owner"},
			WorkingScopes:  []string{"workspace"},
		},
		Guidance: []string{},
		Examples: []json.RawMessage{},
	}
	return command
}

func mustFakeGatewayInvocation(
	t *testing.T,
	source *fakeNodeInvocationSource,
	ctx context.Context,
	invocationID string,
) nodes.GatewayInvocationRecord {
	t.Helper()
	principal, err := nodeInvocationIdentityWithoutCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := source.store.Lookup(principal, invocationID)
	if err != nil || !found {
		t.Fatalf("gateway invocation = (%#v, %v, %v)", record, found, err)
	}
	return record
}

func invocationIDFromError(t *testing.T, result *ToolResult) string {
	t.Helper()
	var payload struct {
		Invocation nodeInvokeResult `json:"invocation"`
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &payload); err != nil {
		t.Fatalf("decode invocation error %q: %v", result.ForLLM, err)
	}
	return payload.Invocation.InvocationID
}

func successfulRemoteInvocation(
	gateway nodes.GatewayInvocationRecord,
) nodes.InvocationRecord {
	now := time.Now().UnixNano()
	return nodes.InvocationRecord{
		InvocationID: gateway.Plan.InvocationID, IdempotencyKey: gateway.Plan.IdempotencyKey,
		PlanHash: gateway.ExpectedPlanHash, NodeID: gateway.Plan.NodeID,
		CatalogHash: gateway.Plan.CatalogHash, Command: gateway.Plan.Command,
		Risk: gateway.Plan.Risk, State: nodes.InvocationSucceeded,
		AcceptedAt: now, UpdatedAt: now, CompletedAt: now, ExpiresAt: gateway.Plan.ExpiresAt,
		Result: json.RawMessage(`{"stdout":"ok","exit_code":0}`),
	}
}

func failedRemoteInvocation(
	gateway nodes.GatewayInvocationRecord,
) nodes.InvocationRecord {
	now := time.Now().UnixNano()
	return nodes.InvocationRecord{
		InvocationID: gateway.Plan.InvocationID, IdempotencyKey: gateway.Plan.IdempotencyKey,
		PlanHash: gateway.ExpectedPlanHash, NodeID: gateway.Plan.NodeID,
		CatalogHash: gateway.Plan.CatalogHash, Command: gateway.Plan.Command,
		Risk: nodes.RiskWrite, State: nodes.InvocationFailed,
		AcceptedAt: now, UpdatedAt: now, CompletedAt: now, ExpiresAt: gateway.Plan.ExpiresAt,
		Failure: &nodes.InvocationFailure{
			Code: "REMOTE_FAILED", Message: "remote failure detail",
		},
	}
}
