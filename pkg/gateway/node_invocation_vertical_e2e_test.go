//go:build linux || darwin

package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/agent"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/testharness/llmscenario"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

func TestNodeInvocationVerticalSliceWithApprovalAndRealCompanion(t *testing.T) {
	workspace := t.TempDir()
	commandDir := t.TempDir()
	executable, err := exec.LookPath("echo")
	if err != nil {
		t.Skip("echo executable is unavailable")
	}

	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	cfg.Agents.Defaults.ModelName = "node-e2e-model"
	cfg.Agents.Defaults.ResponseFooter.Enabled = false
	cfg.Nodes.Enabled = true
	cfg.Execution.Targets = map[string]config.ExecutionTarget{
		"build": {Type: "node", Node: "build-node", Executor: companion.LocalExecutor},
	}
	cfg.Agents.Defaults.TargetPolicy = &config.TargetPolicy{
		DefaultTarget: "build",
		AllowedTargets: []string{
			"build",
		},
	}
	if err := cfg.ValidateExecutionTargets(); err != nil {
		t.Fatal(err)
	}

	registry, admission, runtimeState := newVerticalSliceNodeRuntime(t, workspace)
	server := httptest.NewTLSServer(admission)
	defer server.Close()
	defer closeVerticalSliceAdmission(t, admission)

	tempDir := t.TempDir()
	binaryPath := buildVerticalSliceCompanion(t, tempDir)
	fingerprint := sha256.Sum256(server.Certificate().Raw)
	companionConfig := companion.Config{
		GatewayURL: strings.Replace(server.URL, "https://", "wss://", 1) +
			companion.GatewayPath,
		StateDir: filepath.Join(tempDir, "state"),
		TLS: companion.TLSConfig{
			CertificateSHA256: hex.EncodeToString(fingerprint[:]),
		},
		Reconnect: companion.ReconnectConfig{
			MinDelaySeconds:     1,
			MaxDelaySeconds:     1,
			PendingDelaySeconds: 1,
		},
		Policy: nodes.LocalCommandPolicy{
			Revision:          "vertical-e2e-policy",
			AllowedCommands:   []string{"system.exec.v1"},
			MaximumRisk:       nodes.RiskWrite,
			MaxTimeoutSeconds: 5,
			MaxOutputBytes:    4096,
		},
		SystemExec: &companion.SystemExecPolicy{
			WorkingRoots: []string{commandDir},
			Executables:  []string{executable},
			Discovery: &companion.SystemExecDiscovery{
				ExecutableAliases:   map[string]string{"diagnostic": executable},
				WorkingScopeAliases: map[string]string{"workspace": commandDir},
			},
		},
	}
	configPath := filepath.Join(tempDir, "config.json")
	writeVerticalSliceConfig(t, configPath, companionConfig)
	process := startVerticalSliceCompanion(t, binaryPath, configPath)
	defer func() {
		process.stop(t)
	}()

	pending := waitForVerticalSliceNodeState(t, registry, nodes.StatePendingPairing)
	if _, err := registry.Approve(pending.ID, nodes.PairingApproval{
		Aliases:         []nodes.Alias{"build-node"},
		AllowedCommands: []string{"system.exec.v1"},
		At:              time.Now().Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	connected := waitForVerticalSliceNodeState(t, registry, nodes.StateConnected)
	provider := newNodeP0EvidenceProvider(
		commandDir,
		executable,
		"build",
		"system.exec.v1",
		"diagnostic",
		"workspace",
		"4096",
	)

	msgBus := bus.NewMessageBus()
	eventBus := runtimeevents.NewBus()
	agentLoop := agent.NewAgentLoop(
		cfg,
		msgBus,
		provider,
		agent.WithIsolatedToolBootstrap(),
		agent.WithRuntimeEvents(eventBus),
	)
	defer agentLoop.Close()
	if err := setupNodeTools(cfg, agentLoop, runtimeState); err != nil {
		t.Fatal(err)
	}
	if err := agentLoop.MountHook(agent.NamedHook(
		"node-e2e-approval",
		nodeVerticalSliceApprovalHook{},
	)); err != nil {
		t.Fatal(err)
	}

	channel := newNodeVerticalSliceChannel()
	manager, err := channels.NewManager(cfg, msgBus, media.NewFileMediaStore())
	if err != nil {
		t.Fatal(err)
	}
	manager.RegisterChannel("telegram", channel)
	if err := manager.StartAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.StopAll(context.Background()); err != nil {
			t.Errorf("stop channel manager: %v", err)
		}
	}()
	agentLoop.SetChannelManager(manager)

	subscription, eventChannel, err := eventBus.Channel().
		KindPrefix("node.invocation.").
		SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
			Name:   "node-vertical-slice",
			Buffer: 8,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	waitingSubscription, waitingEvents, err := eventBus.Channel().
		OfKind(runtimeevents.KindAgentInteractionWaiting).
		SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
			Name:   "node-vertical-slice-approval-waiting",
			Buffer: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer waitingSubscription.Close()
	interactionEndSubscription, interactionEndEvents, err := eventBus.Channel().
		OfKind(runtimeevents.KindAgentInteractionEnd).
		SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
			Name:   "node-vertical-slice-interaction-end",
			Buffer: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer interactionEndSubscription.Close()

	const sessionKey = "node-e2e-session"
	response, err := agentLoop.ProcessDirectWithChannel(
		t.Context(),
		"Run the remote node smoke test",
		sessionKey,
		"telegram",
		"chat-e2e",
	)
	if err != nil {
		t.Fatal(err)
	}
	if response != "" {
		t.Fatalf("suspended approval response = %q, want empty", response)
	}
	approvalPrompt := channel.nextMessage(t)
	if !strings.Contains(approvalPrompt.Content, "nodes_invoke") ||
		!strings.Contains(approvalPrompt.Content, "Run an operator-approved command on target build") ||
		!strings.Contains(approvalPrompt.Content, "allow_once") ||
		!strings.Contains(approvalPrompt.Content, "deny") ||
		strings.Contains(approvalPrompt.Content, "Approval needed") ||
		strings.Contains(approvalPrompt.Content, commandDir) ||
		strings.Contains(approvalPrompt.Content, "node-e2e-ok") {
		t.Fatalf("approval prompt = %#v", approvalPrompt)
	}
	waitForVerticalSliceEvent(t, waitingEvents, runtimeevents.KindAgentInteractionWaiting)

	runCtx, stopAgentLoop := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		runDone <- agentLoop.Run(runCtx)
	}()
	defer func() {
		stopAgentLoop()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("agent loop: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("agent loop did not stop")
		}
	}()
	if err := msgBus.PublishInbound(
		t.Context(),
		verticalSliceInboundMessage(
			sessionKey,
			"message-approval",
			"allow_once",
		),
	); err != nil {
		t.Fatal(err)
	}
	final := channel.nextMessage(t)
	if final.Content != "Remote command completed: node-e2e-ok" {
		t.Fatalf("final response = %#v", final)
	}

	events := collectVerticalSliceEvents(t, eventChannel, 4)
	wantObservations := map[string]int{
		tools.NodeInvocationObservationPrepared:   1,
		tools.NodeInvocationObservationDispatched: 1,
		tools.NodeInvocationObservationCompleted:  1,
		tools.NodeInvocationObservationStatus:     1,
	}
	gotObservations := make(map[string]int, len(wantObservations))
	for index, event := range events {
		if event.Kind != runtimeevents.KindNodeInvocationObserved {
			t.Fatalf(
				"event[%d].Kind = %q, want %q",
				index,
				event.Kind,
				runtimeevents.KindNodeInvocationObserved,
			)
		}
		payload, ok := event.Payload.(tools.NodeInvocationEventPayload)
		if !ok {
			t.Fatalf("event[%d].Payload = %T", index, event.Payload)
		}
		gotObservations[payload.Observation]++
	}
	if !reflect.DeepEqual(gotObservations, wantObservations) {
		t.Fatalf("observations = %#v, want %#v", gotObservations, wantObservations)
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		commandDir,
		executable,
		"node-e2e-ok",
		`\"stdout\"`,
		"plan_hash",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("vertical-slice audit leaked %q: %s", forbidden, encoded)
		}
	}
	waitForVerticalSliceEvent(
		t,
		interactionEndEvents,
		runtimeevents.KindAgentInteractionEnd,
	)

	staleResponse, err := agentLoop.ProcessDirectWithChannel(
		t.Context(),
		"Verify the remote outcome cannot be duplicated after a constraint change",
		"node-e2e-stale-session",
		"telegram",
		"chat-e2e",
	)
	if err != nil {
		t.Fatal(err)
	}
	if staleResponse != "" {
		t.Fatalf("stale suspended response = %q, want empty", staleResponse)
	}
	staleApprovalPrompt := channel.nextMessage(t)
	if !strings.Contains(staleApprovalPrompt.Content, "nodes_invoke") ||
		strings.Contains(staleApprovalPrompt.Content, commandDir) ||
		strings.Contains(staleApprovalPrompt.Content, "node-e2e-ok") {
		t.Fatalf("stale approval prompt = %#v", staleApprovalPrompt)
	}
	waitForVerticalSliceEvent(t, waitingEvents, runtimeevents.KindAgentInteractionWaiting)
	stalePrepared := waitForVerticalSliceEvent(
		t,
		eventChannel,
		runtimeevents.KindNodeInvocationObserved,
	)
	stalePreparedPayload, ok := stalePrepared.Payload.(tools.NodeInvocationEventPayload)
	if !ok || stalePreparedPayload.Observation != tools.NodeInvocationObservationPrepared {
		t.Fatalf("stale pre-change observation = %#v", stalePrepared)
	}

	process.stop(t)
	staleCompanionConfig := companionConfig
	staleCompanionConfig.Policy.Revision = "vertical-e2e-policy-stale"
	writeVerticalSliceConfig(t, configPath, staleCompanionConfig)
	process = startVerticalSliceCompanion(t, binaryPath, configPath)
	waitForVerticalSlicePolicyRevision(
		t,
		registry,
		staleCompanionConfig.Policy.Revision,
	)
	if err := msgBus.PublishInbound(
		t.Context(),
		verticalSliceInboundMessage(
			"node-e2e-stale-session",
			"message-stale-approval",
			"allow_once",
		),
	); err != nil {
		t.Fatal(err)
	}
	staleFinal := channel.nextMessage(t)
	if staleFinal.Content != "Stale discovery refreshed without dispatch." {
		t.Fatalf("stale final response = %#v", staleFinal)
	}
	waitForVerticalSliceEvent(
		t,
		interactionEndEvents,
		runtimeevents.KindAgentInteractionEnd,
	)
	assertNoVerticalSliceEvent(t, eventChannel)

	process.stop(t)
	writeVerticalSliceConfig(t, configPath, companionConfig)
	process = startVerticalSliceCompanion(t, binaryPath, configPath)
	waitForVerticalSlicePolicyRevision(t, registry, connected.PolicyRevision)
	recoveredResponse, err := agentLoop.ProcessDirectWithChannel(
		t.Context(),
		"Confirm remote capability discovery recovered",
		"node-e2e-recovered-session",
		"telegram",
		"chat-e2e",
	)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredResponse != "Remote discovery recovered." {
		t.Fatalf("recovered response = %q", recoveredResponse)
	}
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}
	if _, err := agentLoop.ProcessDirectWithChannel(
		t.Context(),
		"/clear",
		sessionKey,
		"telegram",
		"chat-e2e",
	); err != nil {
		t.Fatalf("clear vertical-slice context before shutdown: %v", err)
	}
}

type nodeP0EvidenceProvider struct {
	mu sync.Mutex

	step              int
	target            string
	command           string
	executableAlias   string
	workingScopeAlias string
	discoveryRevision string
	invocationID      string
	timeoutSeconds    int
	outputLimitBytes  int
	forbidden         []string
}

func newNodeP0EvidenceProvider(forbidden ...string) *nodeP0EvidenceProvider {
	return &nodeP0EvidenceProvider{forbidden: append([]string(nil), forbidden...)}
}

func (*nodeP0EvidenceProvider) GetDefaultModel() string {
	return "node-e2e-model"
}

func (provider *nodeP0EvidenceProvider) Chat(
	_ context.Context,
	messages []providers.Message,
	toolDefs []providers.ToolDefinition,
	_ string,
	_ map[string]any,
) (*providers.LLMResponse, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	call := llmscenario.ProviderCall{Messages: messages, Tools: toolDefs}
	step := provider.step
	provider.step++
	switch step {
	case 0:
		if err := requireNodeOutcomeOnlyPrompt(call, provider.forbidden); err != nil {
			return nil, err
		}
		if err := llmscenario.RequireToolDefinition("nodes")(call); err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will discover an available target.",
			llmscenario.ToolCall("call-node-list", "nodes", map[string]any{"action": "list"}),
		), nil
	case 1:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		target, err := nodeP0AvailableTarget(payload)
		if err != nil {
			return nil, err
		}
		provider.target = target
		return llmscenario.ToolCallResponse(
			"I will inspect the target's approved commands.",
			llmscenario.ToolCall("call-node-target", "nodes", map[string]any{
				"action": "describe",
				"target": target,
			}),
		), nil
	case 2:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		command, err := nodeP0AvailableCommand(payload)
		if err != nil {
			return nil, err
		}
		provider.command = command
		return llmscenario.ToolCallResponse(
			"I will inspect the bounded command contract.",
			llmscenario.ToolCall("call-node-command", "nodes", map[string]any{
				"action":  "describe",
				"target":  provider.target,
				"command": command,
			}),
		), nil
	case 3:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		invokeArgs, err := provider.nodeP0InvocationFromContract(payload)
		if err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will run the discovered bounded command.",
			llmscenario.ToolCall("call-node-e2e", "nodes_invoke", invokeArgs),
		), nil
	case 4:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		invocationID, ok := payload["invocation_id"].(string)
		if !ok || invocationID == "" || !nodeP0ResultContains(payload, "node-e2e-ok") {
			return nil, fmt.Errorf("invoke result is incomplete: %#v", payload)
		}
		provider.invocationID = invocationID
		return llmscenario.ToolCallResponse(
			"I will verify the retained result without replaying it.",
			llmscenario.ToolCall("call-node-status", "nodes_status", map[string]any{
				"invocation_id": invocationID,
			}),
		), nil
	case 5:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		if payload["invocation_id"] != provider.invocationID ||
			payload["state"] != string(nodes.InvocationSucceeded) ||
			!nodeP0ResultContains(payload, "node-e2e-ok") {
			return nil, fmt.Errorf("retained status is incomplete: %#v", payload)
		}
		return llmscenario.TextResponse("Remote command completed: node-e2e-ok"), nil
	case 6:
		if err := requireNodeOutcomeOnlyPrompt(call, provider.forbidden); err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will verify the prior discovery cannot replay the command.",
			llmscenario.ToolCall(
				"call-node-stale",
				"nodes_invoke",
				provider.nodeP0InvocationArgs(),
			),
		), nil
	case 7:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		if len(payload) != 4 ||
			payload["status"] != "denied" ||
			payload["code"] != "DISCOVERY_STALE" ||
			payload["constraint"] != "command_policy" ||
			payload["action"] != "refresh_discovery" {
			return nil, fmt.Errorf("stale denial is not model-safe: %#v", payload)
		}
		return llmscenario.ToolCallResponse(
			"I will refresh discovery after the stale denial.",
			llmscenario.ToolCall("call-node-stale-refresh", "nodes", map[string]any{
				"action":  "describe",
				"target":  provider.target,
				"command": provider.command,
			}),
		), nil
	case 8:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		command, ok := payload["command"].(map[string]any)
		revision, revisionOK := payload["discovery_revision"].(string)
		if !ok || command["availability"] != string(nodes.ModelAvailable) ||
			!revisionOK || revision == "" || revision == provider.discoveryRevision {
			return nil, fmt.Errorf("stale discovery did not refresh: %#v", payload)
		}
		return llmscenario.TextResponse("Stale discovery refreshed without dispatch."), nil
	case 9:
		if err := requireNodeOutcomeOnlyPrompt(call, provider.forbidden); err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will rediscover the restored command contract.",
			llmscenario.ToolCall("call-node-recovered", "nodes", map[string]any{
				"action":  "describe",
				"target":  provider.target,
				"command": provider.command,
			}),
		), nil
	case 10:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		command, ok := payload["command"].(map[string]any)
		if !ok || command["availability"] != string(nodes.ModelAvailable) ||
			payload["discovery_revision"] != provider.discoveryRevision {
			return nil, fmt.Errorf("restored discovery is incomplete: %#v", payload)
		}
		return llmscenario.TextResponse("Remote discovery recovered."), nil
	default:
		return nil, fmt.Errorf("unexpected node P0 evidence model call %d", step+1)
	}
}

func (provider *nodeP0EvidenceProvider) AssertExhausted() error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.step != 11 {
		return fmt.Errorf("node P0 evidence consumed %d model steps, want 11", provider.step)
	}
	return nil
}

func (provider *nodeP0EvidenceProvider) nodeP0InvocationFromContract(
	payload map[string]any,
) (map[string]any, error) {
	command, ok := payload["command"].(map[string]any)
	if !ok || command["name"] != provider.command ||
		command["availability"] != string(nodes.ModelAvailable) {
		return nil, fmt.Errorf("command contract is unavailable: %#v", payload)
	}
	revision, ok := payload["discovery_revision"].(string)
	if !ok || revision == "" {
		return nil, errors.New("command contract omitted discovery revision")
	}
	constraints, ok := command["constraints"].(map[string]any)
	if !ok {
		return nil, errors.New("command contract omitted constraints")
	}
	executable, err := nodeP0FirstString(constraints["executable_aliases"])
	if err != nil {
		return nil, err
	}
	workingScope, err := nodeP0FirstString(constraints["working_scopes"])
	if err != nil {
		return nil, err
	}
	execution, ok := command["execution"].(map[string]any)
	if !ok {
		return nil, errors.New("command contract omitted execution bounds")
	}
	timeout, ok := execution["timeout_seconds_max"].(float64)
	if !ok || timeout < 1 {
		return nil, errors.New("command contract omitted timeout ceiling")
	}
	outputLimit, ok := execution["output_bytes_max"].(float64)
	if !ok || outputLimit < 1 {
		return nil, errors.New("command contract omitted output ceiling")
	}
	inputSchema, ok := command["input_schema"].(map[string]any)
	if !ok {
		return nil, errors.New("command contract omitted input schema")
	}
	encodedSchema, err := json.Marshal(inputSchema)
	if err != nil {
		return nil, err
	}
	encodedExecutable, _ := json.Marshal(executable)
	encodedScope, _ := json.Marshal(workingScope)
	if !bytes.Contains(encodedSchema, encodedExecutable) ||
		!bytes.Contains(encodedSchema, encodedScope) {
		return nil, fmt.Errorf("command schema omitted aliases: %s", encodedSchema)
	}
	provider.executableAlias = executable
	provider.workingScopeAlias = workingScope
	provider.discoveryRevision = revision
	provider.timeoutSeconds = int(timeout)
	provider.outputLimitBytes = int(outputLimit)
	return provider.nodeP0InvocationArgs(), nil
}

func (provider *nodeP0EvidenceProvider) nodeP0InvocationArgs() map[string]any {
	return map[string]any{
		"target":             provider.target,
		"command":            provider.command,
		"discovery_revision": provider.discoveryRevision,
		"input": map[string]any{
			"argv":            []any{provider.executableAlias, "node-e2e-ok"},
			"cwd":             provider.workingScopeAlias,
			"timeout_seconds": provider.timeoutSeconds,
			"env":             map[string]any{},
		},
		"timeout_seconds":    provider.timeoutSeconds,
		"output_limit_bytes": provider.outputLimitBytes,
	}
}

func requireNodeOutcomeOnlyPrompt(
	call llmscenario.ProviderCall,
	forbidden []string,
) error {
	if len(call.Messages) == 0 {
		return errors.New("model received no outcome prompt")
	}
	last := call.Messages[len(call.Messages)-1]
	if last.Role != "user" {
		return fmt.Errorf("last message role = %q, want user", last.Role)
	}
	for _, value := range forbidden {
		if value != "" && strings.Contains(last.Content, value) {
			return fmt.Errorf("outcome prompt leaked operator policy value")
		}
	}
	return nil
}

func nodeP0LastToolPayload(call llmscenario.ProviderCall) (map[string]any, error) {
	if len(call.Messages) == 0 {
		return nil, errors.New("model received no tool result")
	}
	last := call.Messages[len(call.Messages)-1]
	if last.Role != "tool" {
		return nil, fmt.Errorf("last message role = %q, want tool", last.Role)
	}
	content := last.Content
	if start, end := strings.Index(content, "{"), strings.LastIndex(content, "}"); start >= 0 && end >= start {
		content = content[start : end+1]
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return nil, fmt.Errorf("decode tool result %q: %w", content, err)
	}
	return payload, nil
}

func nodeP0AvailableTarget(payload map[string]any) (string, error) {
	targets, ok := payload["targets"].([]any)
	if !ok {
		return "", errors.New("node list omitted targets")
	}
	for _, raw := range targets {
		target, ok := raw.(map[string]any)
		if !ok || target["available"] != true {
			continue
		}
		name, ok := target["target"].(string)
		if ok && name != "" {
			return name, nil
		}
	}
	return "", errors.New("node list contained no available target")
}

func nodeP0AvailableCommand(payload map[string]any) (string, error) {
	commands, ok := payload["commands"].([]any)
	if !ok {
		return "", errors.New("target description omitted commands")
	}
	for _, raw := range commands {
		command, ok := raw.(map[string]any)
		if !ok || command["availability"] != string(nodes.ModelAvailable) {
			continue
		}
		name, ok := command["name"].(string)
		if ok && name != "" {
			return name, nil
		}
	}
	return "", errors.New("target description contained no available command")
}

func nodeP0FirstString(raw any) (string, error) {
	values, ok := raw.([]any)
	if !ok || len(values) == 0 {
		return "", errors.New("model constraint omitted visible aliases")
	}
	value, ok := values[0].(string)
	if !ok || value == "" {
		return "", errors.New("model constraint alias is invalid")
	}
	return value, nil
}

func nodeP0ResultContains(payload map[string]any, want string) bool {
	result, exists := payload["result"]
	if !exists {
		return false
	}
	encoded, err := json.Marshal(result)
	return err == nil && bytes.Contains(encoded, []byte(want))
}

func assertNoVerticalSliceEvent(
	t *testing.T,
	eventChannel <-chan runtimeevents.Event,
) {
	t.Helper()
	select {
	case event := <-eventChannel:
		t.Fatalf("stale invocation emitted event: %#v", event)
	case <-time.After(500 * time.Millisecond):
	}
}

func waitForVerticalSliceEvent(
	t *testing.T,
	eventChannel <-chan runtimeevents.Event,
	want runtimeevents.Kind,
) runtimeevents.Event {
	t.Helper()
	select {
	case event := <-eventChannel:
		if event.Kind != want {
			t.Fatalf("event kind = %q, want %q", event.Kind, want)
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %q", want)
		return runtimeevents.Event{}
	}
}

func verticalSliceInboundMessage(
	sessionKey string,
	messageID string,
	content string,
) bus.InboundMessage {
	return bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:   "telegram",
			ChatID:    "chat-e2e",
			ChatType:  "direct",
			SenderID:  "cron",
			ActorID:   "cron",
			MessageID: messageID,
		},
		Content:    content,
		SessionKey: sessionKey,
	}
}

type nodeVerticalSliceApprovalHook struct{}

func (nodeVerticalSliceApprovalHook) ApproveTool(
	_ context.Context,
	request *agent.ToolApprovalRequest,
) (agent.ApprovalDecision, error) {
	if request.Tool != "nodes_invoke" {
		return agent.ApprovalDecision{Approved: true}, nil
	}
	return agent.ApprovalDecision{
		RequireHuman:  true,
		ActionSummary: "Run an operator-approved command on target build",
	}, nil
}

type nodeVerticalSliceChannel struct {
	messages chan bus.OutboundMessage
	media    chan bus.OutboundMediaMessage
	running  bool
}

func newNodeVerticalSliceChannel() *nodeVerticalSliceChannel {
	return &nodeVerticalSliceChannel{
		messages: make(chan bus.OutboundMessage, 8),
		media:    make(chan bus.OutboundMediaMessage, 8),
	}
}

func (*nodeVerticalSliceChannel) Name() string { return "telegram" }

func (channel *nodeVerticalSliceChannel) Start(context.Context) error {
	channel.running = true
	return nil
}

func (channel *nodeVerticalSliceChannel) Stop(context.Context) error {
	channel.running = false
	return nil
}

func (channel *nodeVerticalSliceChannel) Send(
	_ context.Context,
	message bus.OutboundMessage,
) ([]string, error) {
	channel.messages <- message
	return []string{"node-e2e-message"}, nil
}

func (channel *nodeVerticalSliceChannel) SendMedia(
	_ context.Context,
	message bus.OutboundMediaMessage,
) ([]string, error) {
	channel.media <- message
	return []string{"node-e2e-media"}, nil
}

func (channel *nodeVerticalSliceChannel) IsRunning() bool { return channel.running }
func (*nodeVerticalSliceChannel) IsAllowed(string) bool   { return true }
func (*nodeVerticalSliceChannel) IsAllowedSender(bus.SenderInfo) bool {
	return true
}
func (*nodeVerticalSliceChannel) ReasoningChannelID() string { return "" }

func (channel *nodeVerticalSliceChannel) nextMessage(t *testing.T) bus.OutboundMessage {
	t.Helper()
	select {
	case message := <-channel.messages:
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for channel message")
		return bus.OutboundMessage{}
	}
}

func (channel *nodeVerticalSliceChannel) nextMedia(t *testing.T) bus.OutboundMediaMessage {
	t.Helper()
	select {
	case message := <-channel.media:
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for channel media")
		return bus.OutboundMediaMessage{}
	}
}

func newVerticalSliceNodeRuntime(
	t *testing.T,
	workspace string,
) (*nodes.FileRegistry, *nodews.AdmissionHandler, *nodeAdmissionRuntime) {
	t.Helper()
	registryPath := nodes.RegistryPath(workspace)
	registry, err := nodes.NewFileRegistry(registryPath, 4)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sessions := nodews.NewSessionHub()
	admission, err := nodews.NewAdmissionHandler(authenticator, nodews.AdmissionConfig{
		Sessions: sessions,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry, admission, &nodeAdmissionRuntime{
		registry:     registry,
		registryPath: registryPath,
		handler:      admission,
		sessions:     sessions,
		generation:   1,
		mounted:      true,
	}
}

func closeVerticalSliceAdmission(t *testing.T, admission *nodews.AdmissionHandler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := admission.Close(ctx); err != nil {
		t.Errorf("close node admission: %v", err)
	}
}

func buildVerticalSliceCompanion(t *testing.T, outputDir string) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve gateway E2E test source")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	binaryPath := filepath.Join(outputDir, "mintclaw-node")
	command := exec.Command("go", "build", "-o", binaryPath, "./cmd/mintclaw-node")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build companion binary: %v\n%s", err, output)
	}
	return binaryPath
}

func writeVerticalSliceConfig(t *testing.T, path string, cfg companion.Config) {
	t.Helper()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

type verticalSliceCompanionProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	done    chan error
	once    sync.Once
}

func startVerticalSliceCompanion(
	t *testing.T,
	binaryPath string,
	configPath string,
) *verticalSliceCompanionProcess {
	t.Helper()
	process := &verticalSliceCompanionProcess{
		command: exec.Command(binaryPath, "run", "--config", configPath),
		done:    make(chan error, 1),
	}
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		process.done <- process.command.Wait()
	}()
	return process
}

func (process *verticalSliceCompanionProcess) stop(t *testing.T) {
	t.Helper()
	process.once.Do(func() {
		if err := process.command.Process.Signal(os.Interrupt); err != nil {
			t.Errorf("interrupt companion process: %v", err)
			_ = process.command.Process.Kill()
		}
		select {
		case err := <-process.done:
			if err != nil {
				t.Errorf("companion process exit: %v\n%s", err, process.output.String())
			}
		case <-time.After(3 * time.Second):
			_ = process.command.Process.Kill()
			err := <-process.done
			t.Errorf(
				"companion process did not stop after interrupt: %v\n%s",
				err,
				process.output.String(),
			)
		}
	})
}

func waitForVerticalSliceNodeState(
	t *testing.T,
	registry *nodes.FileRegistry,
	want nodes.State,
) nodes.Snapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snapshots, err := registry.List(nodes.Filter{States: []nodes.State{want}})
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) == 1 {
			return snapshots[0]
		}
		time.Sleep(25 * time.Millisecond)
	}
	snapshots, err := registry.List(nodes.Filter{})
	t.Fatalf("nodes = %s, error %v; want one %q node", formatVerticalSliceNodes(snapshots), err, want)
	return nodes.Snapshot{}
}

func waitForVerticalSlicePolicyRevision(
	t *testing.T,
	registry *nodes.FileRegistry,
	want string,
) nodes.Snapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, found, err := registry.Resolve("build-node")
		if err != nil {
			t.Fatal(err)
		}
		if found &&
			snapshot.State == nodes.StateConnected &&
			snapshot.PolicyRevision == want {
			return snapshot
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for authenticated policy revision %q", want)
	return nodes.Snapshot{}
}

func formatVerticalSliceNodes(snapshots []nodes.Snapshot) string {
	data, err := json.Marshal(snapshots)
	if err != nil {
		return fmt.Sprintf("%#v", snapshots)
	}
	return string(data)
}

func collectVerticalSliceEvents(
	t *testing.T,
	eventChannel <-chan runtimeevents.Event,
	count int,
) []runtimeevents.Event {
	t.Helper()
	events := make([]runtimeevents.Event, 0, count)
	deadline := time.After(5 * time.Second)
	for len(events) < count {
		select {
		case event := <-eventChannel:
			events = append(events, event)
		case <-deadline:
			t.Fatalf("received %d node events, want %d", len(events), count)
		}
	}
	return events
}
