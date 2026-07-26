//go:build linux || darwin

package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	"github.com/sipeed/picoclaw/pkg/agent"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/nodes/companion"
	nodews "github.com/sipeed/picoclaw/pkg/nodes/ws"
	"github.com/sipeed/picoclaw/pkg/testharness/llmscenario"
	"github.com/sipeed/picoclaw/pkg/tools"
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
		},
	}
	configPath := filepath.Join(tempDir, "config.json")
	writeVerticalSliceConfig(t, configPath, companionConfig)
	process := startVerticalSliceCompanion(t, binaryPath, configPath)
	defer process.stop(t)

	pending := waitForVerticalSliceNodeState(t, registry, nodes.StatePendingPairing)
	if _, err := registry.Approve(pending.ID, nodes.PairingApproval{
		Aliases:         []nodes.Alias{"build-node"},
		AllowedCommands: []string{"system.exec.v1"},
		At:              time.Now().Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	waitForVerticalSliceNodeState(t, registry, nodes.StateConnected)

	provider := llmscenario.NewScriptedProvider(
		"node-e2e-model",
		llmscenario.ProviderStep{
			Name:   "model requests remote execution",
			Assert: llmscenario.RequireToolDefinition("nodes_invoke"),
			Response: llmscenario.ToolCallResponse(
				"I will run the approved command.",
				llmscenario.ToolCall("call-node-e2e", "nodes_invoke", map[string]any{
					"target":  "build",
					"command": "system.exec.v1",
					"input": map[string]any{
						"argv":            []any{executable, "node-e2e-ok"},
						"cwd":             commandDir,
						"timeout_seconds": 5,
						"env":             map[string]any{},
					},
					"timeout_seconds":    5,
					"output_limit_bytes": 4096,
				}),
			),
		},
		llmscenario.ProviderStep{
			Name: "model receives companion result",
			Assert: llmscenario.RequireLastMessage(
				"tool",
				"node-e2e-ok",
			),
			Response: llmscenario.TextResponse("Remote command completed: node-e2e-ok"),
		},
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
	if !strings.Contains(approvalPrompt.Content, "Approval needed") ||
		!strings.Contains(approvalPrompt.Content, "nodes_invoke") ||
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
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}

	events := collectVerticalSliceEvents(t, eventChannel, 3)
	wantObservations := map[string]int{
		tools.NodeInvocationObservationPrepared:   1,
		tools.NodeInvocationObservationDispatched: 1,
		tools.NodeInvocationObservationCompleted:  1,
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
	running  bool
}

func newNodeVerticalSliceChannel() *nodeVerticalSliceChannel {
	return &nodeVerticalSliceChannel{messages: make(chan bus.OutboundMessage, 8)}
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
	binaryPath := filepath.Join(outputDir, "picoclaw-node")
	command := exec.Command("go", "build", "-o", binaryPath, "./cmd/picoclaw-node")
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
