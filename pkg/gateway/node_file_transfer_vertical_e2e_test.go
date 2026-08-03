//go:build (linux || darwin) && integration

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
	"path/filepath"
	"reflect"
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
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/testharness/llmscenario"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

func TestNodeFileTransferVerticalSliceWithApprovalAndDelivery(t *testing.T) {
	workspace := t.TempDir()
	projectRoot := filepath.Join(t.TempDir(), "project")
	if err := os.Mkdir(projectRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	projectRoot, err := filepath.EvalSymlinks(projectRoot)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = workspace
	cfg.Agents.Defaults.ModelName = "node-file-e2e-model"
	cfg.Agents.Defaults.ResponseFooter.Enabled = false
	cfg.Diagnostics.TraceCapture.Enabled = true
	cfg.Diagnostics.TraceCapture.ContentMode = "redacted_content"
	cfg.Nodes.Enabled = true
	cfg.Execution.Targets = map[string]config.ExecutionTarget{
		"files": {
			Type:        "node",
			Node:        "files-node",
			Executor:    companion.LocalExecutor,
			FileProfile: "project",
		},
	}
	cfg.Agents.Defaults.TargetPolicy = &config.TargetPolicy{
		DefaultTarget:  "files",
		AllowedTargets: []string{"files"},
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
		GatewayURL: strings.Replace(server.URL, "https://", "wss://", 1) + companion.GatewayPath,
		StateDir:   filepath.Join(tempDir, "state"),
		TLS: companion.TLSConfig{
			CertificateSHA256: hex.EncodeToString(fingerprint[:]),
		},
		Reconnect: companion.ReconnectConfig{
			MinDelaySeconds:     1,
			MaxDelaySeconds:     1,
			PendingDelaySeconds: 1,
		},
		Policy: nodes.LocalCommandPolicy{
			Revision:          "file-vertical-e2e-policy",
			AllowedCommands:   []string{"node.info.v1"},
			MaximumRisk:       nodes.RiskRead,
			MaxTimeoutSeconds: 5,
			MaxOutputBytes:    4096,
		},
		FilePolicies: companion.FilePolicies{
			"project": {
				Enabled:        true,
				Revision:       "project-files-v1",
				ReadableRoots:  []string{projectRoot},
				WritableRoots:  []string{projectRoot},
				AllowCreate:    true,
				AllowOverwrite: true,
				MaxFileBytes:   protocol.MaxTransferFileBytes,
				Approval: companion.FileApprovalPolicy{
					Metadata: companion.FileApprovalNone,
					Read:     companion.FileApprovalRequired,
					Write:    companion.FileApprovalRequired,
				},
			},
		},
	}
	configPath := filepath.Join(tempDir, "config.json")
	writeVerticalSliceConfig(t, configPath, companionConfig)
	process := startVerticalSliceCompanion(t, binaryPath, configPath)
	defer process.stop(t)

	pending := waitForVerticalSliceNodeState(t, registry, nodes.StatePendingPairing)
	if _, err := registry.Approve(pending.ID, nodes.PairingApproval{
		Aliases: []nodes.Alias{"files-node"},
		AllowedCommands: []string{
			"file.download.v1",
			"file.info.v1",
			"file.upload.v1",
		},
		At: time.Now().Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	connected := waitForVerticalSliceNodeState(t, registry, nodes.StateConnected)

	content := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0x00, 0xff, 0x41, 0x7f}, 257)...)
	digest := sha256.Sum256(content)
	sourcePath := filepath.Join(t.TempDir(), "source.png")
	if err := os.WriteFile(sourcePath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	mediaStore := media.NewFileMediaStore()
	artifactRef, err := mediaStore.Store(sourcePath, media.MediaMeta{
		Filename:      "source.png",
		ContentType:   "image/png",
		Source:        "test:node-file-e2e",
		CleanupPolicy: media.CleanupPolicyForgetOnly,
	}, "node-file-e2e-input")
	if err != nil {
		t.Fatal(err)
	}

	provider := newNodeFileEvidenceProvider(uint64(len(content)), hex.EncodeToString(digest[:]))
	msgBus := bus.NewMessageBus()
	eventBus := runtimeevents.NewBus()
	agentLoop := agent.NewAgentLoop(
		cfg,
		msgBus,
		provider,
		agent.WithIsolatedToolBootstrap(),
		agent.WithRuntimeEvents(eventBus),
	)
	agentClosed := false
	closeAgent := func() {
		if !agentClosed {
			agentLoop.Close()
			agentClosed = true
		}
	}
	defer closeAgent()
	if err := setupNodeTools(cfg, agentLoop, runtimeState); err != nil {
		t.Fatal(err)
	}
	agentLoop.SetMediaStore(mediaStore)
	if err := agentLoop.MountHook(agent.NamedHook(
		"node-file-e2e-approval",
		nodeFileVerticalSliceApprovalHook{},
	)); err != nil {
		t.Fatal(err)
	}

	channel := newNodeVerticalSliceChannel()
	manager, err := channels.NewManager(cfg, msgBus, mediaStore)
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

	eventSubscription, nodeEvents, err := eventBus.Channel().
		KindPrefix("node.invocation.").
		SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
			Name:   "node-file-vertical-slice",
			Buffer: 32,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer eventSubscription.Close()
	waitingSubscription, waitingEvents, err := eventBus.Channel().
		OfKind(runtimeevents.KindAgentInteractionWaiting).
		SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
			Name:   "node-file-vertical-slice-waiting",
			Buffer: 2,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer waitingSubscription.Close()
	interactionEndSubscription, interactionEndEvents, err := eventBus.Channel().
		OfKind(runtimeevents.KindAgentInteractionEnd).
		SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
			Name:   "node-file-vertical-slice-end",
			Buffer: 2,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer interactionEndSubscription.Close()

	runCtx, stopAgentLoop := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		runDone <- agentLoop.Run(runCtx)
	}()
	runStopped := false
	stopRunLoop := func() {
		if runStopped {
			return
		}
		stopAgentLoop()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("agent loop: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("agent loop did not stop")
		}
		runStopped = true
	}
	defer stopRunLoop()

	const sessionKey = "node-file-e2e-session"
	if err := msgBus.PublishInbound(t.Context(), bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:   "telegram",
			ChatID:    "chat-file-e2e",
			ChatType:  "direct",
			TopicID:   "topic-file-e2e",
			SenderID:  "operator-1",
			ActorID:   "operator-1",
			MessageID: "message-file-request",
		},
		Content:    "Upload the attached image to the authorized file target, verify it, and send it back.",
		Media:      []string{artifactRef},
		SessionKey: sessionKey,
	}); err != nil {
		t.Fatal(err)
	}

	uploadPrompt := channel.nextMessage(t)
	waitForVerticalSliceEvent(t, waitingEvents, runtimeevents.KindAgentInteractionWaiting)
	destination := provider.destinationPath()
	assertNodeFileApprovalPrompt(
		t,
		uploadPrompt,
		"nodes_upload",
		destination,
		hex.EncodeToString(digest[:]),
		uint64(len(content)),
	)
	for _, forbidden := range []string{artifactRef, sourcePath, string(content), string(connected.ID)} {
		if strings.Contains(uploadPrompt.Content, forbidden) {
			t.Fatalf("upload approval leaked %q: %q", forbidden, uploadPrompt.Content)
		}
	}
	if err := msgBus.PublishInbound(t.Context(), nodeFileApprovalAnswer(
		sessionKey,
		"message-upload-approval",
		uploadPrompt.Content,
	)); err != nil {
		t.Fatal(err)
	}

	downloadPrompt := channel.nextMessage(t)
	waitForVerticalSliceEvent(t, waitingEvents, runtimeevents.KindAgentInteractionWaiting)
	waitForVerticalSliceEvent(t, interactionEndEvents, runtimeevents.KindAgentInteractionEnd)
	assertNodeFileApprovalPrompt(
		t,
		downloadPrompt,
		"nodes_download",
		destination,
		hex.EncodeToString(digest[:]),
		uint64(len(content)),
	)
	if err := msgBus.PublishInbound(t.Context(), nodeFileApprovalAnswer(
		sessionKey,
		"message-download-approval",
		downloadPrompt.Content,
	)); err != nil {
		t.Fatal(err)
	}

	delivered := channel.nextMedia(t)
	waitForVerticalSliceEvent(t, interactionEndEvents, runtimeevents.KindAgentInteractionEnd)
	if delivered.Channel != "telegram" || delivered.ChatID != "chat-file-e2e" ||
		delivered.Context.TopicID != "topic-file-e2e" || delivered.SessionKey == "" ||
		len(delivered.Parts) != 1 {
		t.Fatalf("delivered media = %#v", delivered)
	}
	deliveredPath, err := mediaStore.Resolve(delivered.Parts[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	deliveredBytes, err := os.ReadFile(deliveredPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(deliveredBytes, content) {
		t.Fatal("delivered bytes differ from the uploaded binary")
	}
	remoteBytes, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remoteBytes, content) {
		t.Fatal("remote bytes differ from the uploaded binary")
	}
	if len(channel.messages) != 0 || len(channel.media) != 0 {
		var extraText bus.OutboundMessage
		if len(channel.messages) != 0 {
			extraText = <-channel.messages
		}
		t.Fatalf(
			"duplicate completion delivery: text=%#v remaining_text=%d media=%d",
			extraText,
			len(channel.messages),
			len(channel.media),
		)
	}
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}

	events := collectVerticalSliceEvents(t, nodeEvents, 9)
	wantObservations := map[string]int{
		tools.NodeInvocationObservationPrepared:   3,
		tools.NodeInvocationObservationDispatched: 3,
		tools.NodeInvocationObservationCompleted:  3,
	}
	gotObservations := make(map[string]int, len(wantObservations))
	gotTools := make(map[string]int, 3)
	for _, event := range events {
		payload, ok := event.Payload.(tools.NodeInvocationEventPayload)
		if event.Kind != runtimeevents.KindNodeInvocationObserved || !ok {
			t.Fatalf("node event = %#v", event)
		}
		gotObservations[payload.Observation]++
		gotTools[event.Source.Name]++
	}
	if !reflect.DeepEqual(gotObservations, wantObservations) ||
		!reflect.DeepEqual(gotTools, map[string]int{
			"nodes_file_info": 3,
			"nodes_upload":    3,
			"nodes_download":  3,
		}) {
		t.Fatalf("observations=%#v tools=%#v", gotObservations, gotTools)
	}
	assertNodeFileEvidenceRedacted(
		t,
		events,
		content,
		destination,
		hex.EncodeToString(digest[:]),
		artifactRef,
		sourcePath,
		string(connected.ID),
	)

	stopRunLoop()
	closeAgent()
	tracePaths := waitForNodeFileTraces(t, workspace)
	traces := make([]json.RawMessage, 0, len(tracePaths))
	for _, tracePath := range tracePaths {
		traceBytes, err := os.ReadFile(tracePath)
		if err != nil {
			t.Fatal(err)
		}
		traces = append(traces, json.RawMessage(traceBytes))
	}
	assertNodeFileEvidenceRedacted(
		t,
		traces,
		content,
		destination,
		hex.EncodeToString(digest[:]),
		artifactRef,
		sourcePath,
		string(connected.ID),
	)
}

type nodeFileEvidenceProvider struct {
	mu sync.Mutex

	step             int
	target           string
	artifactRef      string
	destination      string
	uploadRevision   string
	infoRevision     string
	downloadRevision string
	expectedSize     uint64
	expectedDigest   string
}

func newNodeFileEvidenceProvider(size uint64, digest string) *nodeFileEvidenceProvider {
	return &nodeFileEvidenceProvider{expectedSize: size, expectedDigest: digest}
}

func (*nodeFileEvidenceProvider) GetDefaultModel() string { return "node-file-e2e-model" }

func (provider *nodeFileEvidenceProvider) Chat(
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
		for _, toolName := range []string{
			"nodes",
			"nodes_file_info",
			"nodes_upload",
			"nodes_download",
		} {
			if err := llmscenario.RequireToolDefinition(toolName)(call); err != nil {
				return nil, err
			}
		}
		ref, err := nodeFileVisibleAttachment(messages)
		if err != nil {
			return nil, err
		}
		provider.artifactRef = ref
		return llmscenario.ToolCallResponse(
			"I will discover an authorized file target.",
			llmscenario.ToolCall("call-file-list", "nodes", map[string]any{"action": "list"}),
		), nil
	case 1:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		provider.target, err = nodeP0AvailableTarget(payload)
		if err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will inspect the target's visible capabilities.",
			llmscenario.ToolCall("call-file-target", "nodes", map[string]any{
				"action": "describe", "target": provider.target,
			}),
		), nil
	case 2:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		if err := requireNodeFileCommands(payload); err != nil {
			return nil, err
		}
		return provider.describe("upload", "file.upload.v1"), nil
	case 3:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		provider.uploadRevision, provider.destination, err = nodeFileUploadContract(payload)
		if err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will upload the visible attachment using the discovered profile.",
			llmscenario.ToolCall("call-file-upload", "nodes_upload", map[string]any{
				"target":             provider.target,
				"artifact_ref":       provider.artifactRef,
				"destination":        provider.destination,
				"publication":        "create",
				"discovery_revision": provider.uploadRevision,
			}),
		), nil
	case 4:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		if payload["state"] != "published" || payload["path"] != provider.destination {
			return nil, fmt.Errorf("upload result is incomplete: %#v", payload)
		}
		return provider.describe("metadata", "file.info.v1"), nil
	case 5:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		provider.infoRevision, err = nodeFileContractRevision(payload, "file.info.v1")
		if err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will verify bounded regular-file metadata.",
			llmscenario.ToolCall("call-file-info", "nodes_file_info", map[string]any{
				"target":             provider.target,
				"path":               provider.destination,
				"discovery_revision": provider.infoRevision,
			}),
		), nil
	case 6:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		if payload["state"] != "committed" || payload["type"] != "regular_file" ||
			payload["size"] != float64(provider.expectedSize) ||
			payload["sha256"] != provider.expectedDigest {
			return nil, fmt.Errorf("file metadata is incomplete: %#v", payload)
		}
		return provider.describe("download", "file.download.v1"), nil
	case 7:
		payload, err := nodeP0LastToolPayload(call)
		if err != nil {
			return nil, err
		}
		provider.downloadRevision, err = nodeFileContractRevision(payload, "file.download.v1")
		if err != nil {
			return nil, err
		}
		return llmscenario.ToolCallResponse(
			"I will download and deliver the verified file.",
			llmscenario.ToolCall("call-file-download", "nodes_download", map[string]any{
				"target":             provider.target,
				"source":             provider.destination,
				"deliver":            true,
				"discovery_revision": provider.downloadRevision,
			}),
		), nil
	default:
		return nil, fmt.Errorf("unexpected node file evidence model call %d", step+1)
	}
}

func (provider *nodeFileEvidenceProvider) describe(label, command string) *providers.LLMResponse {
	return llmscenario.ToolCallResponse(
		"I will inspect the "+label+" contract.",
		llmscenario.ToolCall("call-file-describe-"+label, "nodes", map[string]any{
			"action": "describe", "target": provider.target, "command": command,
		}),
	)
}

func (provider *nodeFileEvidenceProvider) destinationPath() string {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.destination
}

func (provider *nodeFileEvidenceProvider) AssertExhausted() error {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.step != 8 {
		return fmt.Errorf("node file evidence consumed %d model steps, want 8", provider.step)
	}
	return nil
}

type nodeFileVerticalSliceApprovalHook struct{}

func (nodeFileVerticalSliceApprovalHook) ApproveTool(
	_ context.Context,
	request *agent.ToolApprovalRequest,
) (agent.ApprovalDecision, error) {
	switch request.Tool {
	case "nodes_upload", "nodes_download":
		return agent.ApprovalDecision{
			RequireHuman:  true,
			ActionSummary: "Confirm the exact retained file-transfer plan",
		}, nil
	default:
		return agent.ApprovalDecision{Approved: true}, nil
	}
}

func nodeFileVisibleAttachment(messages []providers.Message) (string, error) {
	for index := len(messages) - 1; index >= 0; index-- {
		for _, attachment := range messages[index].Attachments {
			if strings.HasPrefix(attachment.Ref, "media://") {
				return attachment.Ref, nil
			}
		}
		for _, ref := range messages[index].Media {
			if strings.HasPrefix(ref, "media://") {
				return ref, nil
			}
		}
	}
	return "", errors.New("model did not receive the visible attachment reference")
}

func requireNodeFileCommands(payload map[string]any) error {
	commands, ok := payload["commands"].([]any)
	if !ok {
		return errors.New("target description omitted commands")
	}
	found := make(map[string]bool, 3)
	for _, raw := range commands {
		command, ok := raw.(map[string]any)
		if !ok || command["availability"] != string(nodes.ModelAvailable) {
			continue
		}
		name, _ := command["name"].(string)
		found[name] = true
	}
	for _, name := range []string{"file.info.v1", "file.upload.v1", "file.download.v1"} {
		if !found[name] {
			return fmt.Errorf("target did not expose %s", name)
		}
	}
	return nil
}

func nodeFileUploadContract(payload map[string]any) (string, string, error) {
	revision, err := nodeFileContractRevision(payload, "file.upload.v1")
	if err != nil {
		return "", "", err
	}
	command, _ := payload["command"].(map[string]any)
	fileContract, ok := command["file"].(map[string]any)
	if !ok || fileContract["allow_create"] != true {
		return "", "", errors.New("upload contract does not allow create")
	}
	root, err := nodeP0FirstString(fileContract["writable_roots"])
	if err != nil {
		return "", "", err
	}
	return revision, filepath.Join(root, "roundtrip.png"), nil
}

func nodeFileContractRevision(payload map[string]any, wantCommand string) (string, error) {
	command, ok := payload["command"].(map[string]any)
	if !ok || command["name"] != wantCommand ||
		command["availability"] != string(nodes.ModelAvailable) {
		return "", fmt.Errorf("file command contract is unavailable: %#v", payload)
	}
	revision, ok := payload["discovery_revision"].(string)
	if !ok || revision == "" {
		return "", errors.New("file command contract omitted discovery revision")
	}
	return revision, nil
}

func nodeFileApprovalAnswer(sessionKey, messageID, prompt string) bus.InboundMessage {
	const prefix = "`/answer "
	start := strings.Index(prompt, prefix)
	answer := "allow_once"
	if start >= 0 {
		shortID := strings.Fields(prompt[start+len(prefix):])
		if len(shortID) > 0 {
			answer = "/answer " + strings.Trim(shortID[0], "`") + " allow_once"
		}
	}
	return bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:   "telegram",
			ChatID:    "chat-file-e2e",
			ChatType:  "direct",
			TopicID:   "topic-file-e2e",
			SenderID:  "operator-1",
			ActorID:   "operator-1",
			MessageID: messageID,
		},
		Content:    answer,
		SessionKey: sessionKey,
	}
}

func assertNodeFileApprovalPrompt(
	t *testing.T,
	message bus.OutboundMessage,
	toolName string,
	path string,
	digest string,
	size uint64,
) {
	t.Helper()
	for _, value := range []string{
		toolName,
		path,
		digest,
		fmt.Sprintf("%d bytes", size),
		"target files",
		"profile project",
		"allow_once",
		"deny",
	} {
		if !strings.Contains(message.Content, value) {
			t.Fatalf("approval prompt omitted %q: %q", value, message.Content)
		}
	}
}

func assertNodeFileEvidenceRedacted(
	t *testing.T,
	value any,
	content []byte,
	forbidden ...string,
) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	forbidden = append(forbidden, string(content), hex.EncodeToString(content))
	for _, secret := range forbidden {
		if secret != "" && bytes.Contains(encoded, []byte(secret)) {
			t.Fatalf("node file evidence leaked %q: %s", secret, encoded)
		}
	}
}

func waitForNodeFileTraces(t *testing.T, workspace string) []string {
	t.Helper()
	pattern := filepath.Join(workspace, "state", "diagnostics", "traces", "*.json")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(pattern)
		if len(matches) > 0 {
			return matches
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for diagnostic traces at %s", pattern)
	return nil
}
