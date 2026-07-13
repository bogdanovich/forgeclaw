// Package evalscenario runs deterministic fixtures through ForgeClaw's real
// inbound agent path inside a private, disposable workspace.
package evalscenario

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/agent"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evalreplay"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/testharness/llmscenario"
	"github.com/sipeed/picoclaw/pkg/tools"
)

const defaultScenarioTimeout = 10 * time.Second

var safeScenarioID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Scenario struct {
	ID            string
	Source        string
	Prompt        string
	Instructions  string
	SessionKey    string
	Channel       string
	ChatID        string
	Model         string
	ProviderSteps []ProviderStep
	Tools         []StubTool
	Skills        []Skill
	ActiveSkills  []string
	ContextWindow int
	MaxTokens     int
	MaxToolTurns  int
	Timeout       time.Duration
}

// Skill is an isolated skill file available only inside one scenario run.
type Skill struct {
	Name    string
	Content string
}

type ProviderStep struct {
	Name      string
	Content   string
	ToolCalls []ToolCall
	Error     string
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments map[string]any
}

type StubTool struct {
	Name        string
	Description string
	Parameters  map[string]any
	Result      string
	IsError     bool
}

type Observation struct {
	Response        string
	ProviderCalls   int
	ToolCalls       map[string]int
	ToolInvocations map[string][]ToolInvocation
	Trace           evaltrace.Trace
	Replay          evalreplay.Result
}

type ToolInvocation struct {
	Arguments map[string]any
	Channel   string
	ChatID    string
}

func Run(ctx context.Context, scenario Scenario) (Observation, error) {
	if err := validateScenario(scenario, true); err != nil {
		return Observation{}, err
	}
	provider := scriptedProvider(scenario)
	return run(ctx, scenario, provider, func() int { return len(provider.Calls()) }, provider.AssertExhausted)
}

// RunWithProvider executes a scenario with a caller-supplied provider. It is
// intended for explicit operator-run baseline/candidate trials; normal tests
// should use Run and scripted provider steps.
func RunWithProvider(
	ctx context.Context,
	scenario Scenario,
	provider providers.LLMProvider,
) (Observation, error) {
	if provider == nil {
		return Observation{}, errors.New("scenario provider is required")
	}
	if len(scenario.ProviderSteps) != 0 {
		return Observation{}, errors.New("provider steps cannot be used with a supplied provider")
	}
	if err := validateScenario(scenario, false); err != nil {
		return Observation{}, err
	}
	recorded := &recordingProvider{provider: provider}
	return run(ctx, scenario, recorded, recorded.CallCount, nil)
}

func run(
	ctx context.Context,
	scenario Scenario,
	provider providers.LLMProvider,
	providerCallCount func() int,
	assertProvider func() error,
) (Observation, error) {
	timeout := scenario.Timeout
	if timeout <= 0 {
		timeout = defaultScenarioTimeout
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	workspace, workspaceErr := os.MkdirTemp("", "forgeclaw-eval-scenario-")
	if workspaceErr != nil {
		return Observation{}, fmt.Errorf("create isolated scenario workspace: %w", workspaceErr)
	}
	defer os.RemoveAll(workspace)
	if err := writeScenarioWorkspace(workspace, scenario); err != nil {
		return Observation{}, err
	}

	messageBus := bus.NewMessageBus()
	cfg := scenarioConfig(workspace, scenario)
	loop := agent.NewAgentLoop(
		cfg,
		messageBus,
		provider,
		agent.WithIsolatedToolBootstrap(),
		agent.WithIsolatedSkillBootstrap(),
	)
	messageBus.SetEventPublisher(loop.RuntimeEventBus())

	stubs, registerErr := registerScenarioTools(loop, scenario.Tools)
	if registerErr != nil {
		return Observation{}, registerErr
	}
	if err := verifyToolBoundary(loop, scenario.Tools); err != nil {
		return Observation{}, err
	}

	loopErr := make(chan error, 1)
	go func() { loopErr <- loop.Run(runCtx) }()
	defer func() {
		cancel()
		loop.Stop()
		messageBus.Close()
		select {
		case <-loopErr:
		case <-time.After(time.Second):
		}
		loop.Close()
	}()
	inbound := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel: scenarioChannel(scenario), ChatID: scenarioChatID(scenario),
			ChatType: "direct", SenderID: "eval-fixture", MessageID: "fixture-message-1",
		},
		Content: scenario.Prompt, SessionKey: scenarioSessionKey(scenario),
	}
	if err := messageBus.PublishInbound(runCtx, inbound); err != nil {
		return Observation{}, fmt.Errorf("publish scenario input: %w", err)
	}

	var outbound bus.OutboundMessage
	select {
	case outbound = <-messageBus.OutboundChan():
	case err := <-loopErr:
		return Observation{}, fmt.Errorf("agent loop stopped before output: %w", err)
	case <-runCtx.Done():
		return Observation{}, fmt.Errorf("wait for scenario output: %w", runCtx.Err())
	}
	publishDeliveryOutcome(loop.RuntimeEventBus(), outbound)

	trace, traceErr := waitForTrace(runCtx, workspace)
	if traceErr != nil {
		return Observation{}, traceErr
	}
	trace, normalizeErr := normalizeFixtureTrace(trace, scenario)
	if normalizeErr != nil {
		return Observation{}, normalizeErr
	}
	replayed, replayErr := evalreplay.Replay(trace)
	if replayErr != nil {
		return Observation{}, fmt.Errorf("replay captured scenario trace: %w", replayErr)
	}
	if assertProvider != nil {
		if err := assertProvider(); err != nil {
			return Observation{}, err
		}
	}

	toolCalls := make(map[string]int, len(stubs))
	toolInvocations := make(map[string][]ToolInvocation, len(stubs))
	for name, stub := range stubs {
		calls := stub.Calls()
		toolCalls[name] = len(calls)
		invocations := make([]ToolInvocation, 0, len(calls))
		for _, call := range calls {
			invocations = append(invocations, ToolInvocation{
				Arguments: call.Args,
				Channel:   call.Channel,
				ChatID:    call.ChatID,
			})
		}
		toolInvocations[name] = invocations
	}
	return Observation{
		Response: outbound.Content, ProviderCalls: providerCallCount(),
		ToolCalls: toolCalls, ToolInvocations: toolInvocations,
		Trace: trace, Replay: replayed,
	}, nil
}

func validateScenario(scenario Scenario, requireProviderSteps bool) error {
	if !safeScenarioID.MatchString(scenario.ID) {
		return errors.New("scenario id must be a path-safe identifier")
	}
	if strings.TrimSpace(scenario.Source) == "" {
		return errors.New("scenario source is required")
	}
	if strings.TrimSpace(scenario.Prompt) == "" {
		return errors.New("scenario prompt is required")
	}
	if scenario.ContextWindow < 0 || scenario.MaxTokens < 0 || scenario.MaxToolTurns < 0 {
		return errors.New("scenario model and tool limits cannot be negative")
	}
	if requireProviderSteps && len(scenario.ProviderSteps) == 0 {
		return errors.New("at least one provider step is required")
	}
	seen := make(map[string]struct{}, len(scenario.Tools))
	for _, stub := range scenario.Tools {
		name := strings.TrimSpace(stub.Name)
		if name == "" {
			return errors.New("stub tool name is required")
		}
		if !safeScenarioID.MatchString(name) {
			return fmt.Errorf("stub tool name %q is not safe for an allowlist", name)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate stub tool %q", name)
		}
		seen[name] = struct{}{}
	}
	skills := make(map[string]struct{}, len(scenario.Skills))
	for _, skill := range scenario.Skills {
		name := strings.TrimSpace(skill.Name)
		if !safeScenarioID.MatchString(name) {
			return fmt.Errorf("skill name %q is not a path-safe identifier", name)
		}
		if strings.TrimSpace(skill.Content) == "" {
			return fmt.Errorf("skill %q content is required", name)
		}
		if _, exists := skills[name]; exists {
			return fmt.Errorf("duplicate skill %q", name)
		}
		skills[name] = struct{}{}
	}
	active := make(map[string]struct{}, len(scenario.ActiveSkills))
	for _, rawName := range scenario.ActiveSkills {
		name := strings.TrimSpace(rawName)
		if _, exists := skills[name]; !exists {
			return fmt.Errorf("active skill %q is not present in the isolated scenario", name)
		}
		if _, exists := active[name]; exists {
			return fmt.Errorf("duplicate active skill %q", name)
		}
		active[name] = struct{}{}
	}
	return nil
}

func scriptedProvider(scenario Scenario) *llmscenario.ScriptedProvider {
	steps := make([]llmscenario.ProviderStep, 0, len(scenario.ProviderSteps))
	for _, step := range scenario.ProviderSteps {
		providerStep := llmscenario.ProviderStep{Name: step.Name}
		if step.Error != "" {
			providerStep.Err = errors.New(step.Error)
		} else if len(step.ToolCalls) > 0 {
			calls := make([]providers.ToolCall, 0, len(step.ToolCalls))
			for _, call := range step.ToolCalls {
				calls = append(calls, llmscenario.ToolCall(call.ID, call.Name, call.Arguments))
			}
			providerStep.Response = llmscenario.ToolCallResponse(step.Content, calls...)
		} else {
			providerStep.Response = llmscenario.TextResponse(step.Content)
		}
		steps = append(steps, providerStep)
	}
	return llmscenario.NewScriptedProvider(scenarioModel(scenario), steps...)
}

func scenarioConfig(workspace string, scenario Scenario) *config.Config {
	cfg := &config.Config{
		Agents: config.AgentsConfig{Defaults: config.AgentDefaults{
			Workspace: workspace, ModelName: scenarioModel(scenario),
			ContextWindow: scenarioContextWindow(scenario), MaxTokens: scenarioMaxTokens(scenario),
			MaxToolIterations: scenarioMaxToolTurns(scenario),
		}},
		Evaluation: config.EvaluationConfig{TraceCapture: config.EvaluationTraceCaptureConfig{
			Enabled: true, ContentMode: "metadata_only",
		}},
		Tools: config.ToolsConfig{FilterSensitiveData: true, FilterMinLength: 8},
	}
	_ = cfg.SensitiveDataReplacer()
	return cfg
}

func scenarioContextWindow(scenario Scenario) int {
	if scenario.ContextWindow > 0 {
		return scenario.ContextWindow
	}
	return 16_384
}

func scenarioMaxTokens(scenario Scenario) int {
	if scenario.MaxTokens > 0 {
		return scenario.MaxTokens
	}
	return 4096
}

func scenarioMaxToolTurns(scenario Scenario) int {
	if scenario.MaxToolTurns > 0 {
		return scenario.MaxToolTurns
	}
	return 10
}

func writeScenarioWorkspace(workspace string, scenario Scenario) error {
	names := make([]string, 0, len(scenario.Tools))
	for _, stub := range scenario.Tools {
		names = append(names, strings.TrimSpace(stub.Name))
	}
	sort.Strings(names)
	var builder strings.Builder
	builder.WriteString("---\ntools:\n")
	for _, name := range names {
		builder.WriteString("  - ")
		builder.WriteString(name)
		builder.WriteByte('\n')
	}
	if len(names) == 0 {
		builder.WriteString("  []\n")
	}
	if len(scenario.ActiveSkills) > 0 {
		builder.WriteString("skills:\n")
		for _, name := range scenario.ActiveSkills {
			builder.WriteString("  - ")
			builder.WriteString(strings.TrimSpace(name))
			builder.WriteByte('\n')
		}
	}
	builder.WriteString("mcpServers: []\n---\n# Evaluation fixture\n")
	if instructions := strings.TrimSpace(scenario.Instructions); instructions != "" {
		builder.WriteString("\n")
		builder.WriteString(instructions)
		builder.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(workspace, "AGENT.md"), []byte(builder.String()), 0o600); err != nil {
		return fmt.Errorf("write isolated tool policy: %w", err)
	}
	for _, skill := range scenario.Skills {
		dir := filepath.Join(workspace, "skills", strings.TrimSpace(skill.Name))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create isolated skill directory: %w", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(skill.Content), 0o600); err != nil {
			return fmt.Errorf("write isolated skill %q: %w", skill.Name, err)
		}
	}
	return nil
}

type recordingProvider struct {
	provider providers.LLMProvider
	mu       sync.Mutex
	calls    int
}

func (p *recordingProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	toolDefs []providers.ToolDefinition,
	model string,
	options map[string]any,
) (*providers.LLMResponse, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return p.provider.Chat(ctx, messages, toolDefs, model, options)
}

func (p *recordingProvider) GetDefaultModel() string {
	return p.provider.GetDefaultModel()
}

func (p *recordingProvider) CallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func registerScenarioTools(
	loop *agent.AgentLoop,
	specs []StubTool,
) (map[string]*llmscenario.StubTool, error) {
	stubs := make(map[string]*llmscenario.StubTool, len(specs))
	for _, spec := range specs {
		result := tools.NewToolResult(spec.Result)
		if spec.IsError {
			result = tools.ErrorResult(spec.Result)
		}
		stub := llmscenario.NewStubTool(spec.Name, result)
		if description := strings.TrimSpace(spec.Description); description != "" {
			stub.DescriptionValue = description
		}
		if spec.Parameters != nil {
			stub.ParametersValue = spec.Parameters
		}
		loop.RegisterTool(stub)
		stubs[spec.Name] = stub
	}
	return stubs, nil
}

func verifyToolBoundary(loop *agent.AgentLoop, specs []StubTool) error {
	instance := loop.GetRegistry().GetDefaultAgent()
	if instance == nil || instance.Tools == nil {
		return errors.New("scenario agent has no tool registry")
	}
	want := make([]string, 0, len(specs))
	for _, spec := range specs {
		want = append(want, spec.Name)
	}
	sort.Strings(want)
	got := instance.Tools.List()
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		return fmt.Errorf("scenario tool boundary violated: got %v, want %v", got, want)
	}
	return nil
}

func publishDeliveryOutcome(eventBus runtimeevents.Bus, outbound bus.OutboundMessage) {
	eventBus.PublishNonBlocking(runtimeevents.Event{
		Kind:     runtimeevents.KindChannelMessageOutboundSent,
		Source:   runtimeevents.Source{Component: "evalscenario", Name: outbound.Channel},
		Scope:    runtimeevents.Scope{Channel: outbound.Channel, ChatID: outbound.ChatID},
		Severity: runtimeevents.SeverityInfo,
		Payload:  channels.ChannelOutboundPayload{ContentLen: len([]rune(outbound.Content))},
	})
}

func waitForTrace(ctx context.Context, workspace string) (evaltrace.Trace, error) {
	root := filepath.Join(workspace, "state", "evaluation", "traces")
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		entries, err := os.ReadDir(root)
		if err == nil {
			for _, entry := range entries {
				if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
					id := strings.TrimSuffix(entry.Name(), ".json")
					return (evaltrace.Store{Root: root}).Load(id)
				}
			}
		} else if !os.IsNotExist(err) {
			return evaltrace.Trace{}, fmt.Errorf("read scenario trace store: %w", err)
		}
		select {
		case <-ctx.Done():
			return evaltrace.Trace{}, fmt.Errorf("wait for scenario trace: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func normalizeFixtureTrace(trace evaltrace.Trace, scenario Scenario) (evaltrace.Trace, error) {
	trace.TraceID = scenario.ID
	trace.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	trace.Source = evaltrace.Source{FixtureID: scenario.ID, FixtureSource: scenario.Source}
	trace.Policy = evaltrace.CapturePolicy{ContentMode: evaltrace.ContentFixture}
	return evaltrace.Finalize(trace)
}

func scenarioModel(scenario Scenario) string {
	if model := strings.TrimSpace(scenario.Model); model != "" {
		return model
	}
	return "eval-scenario-model"
}

func scenarioSessionKey(scenario Scenario) string {
	if key := strings.TrimSpace(scenario.SessionKey); key != "" {
		return key
	}
	return "eval:" + scenario.ID
}

func scenarioChannel(scenario Scenario) string {
	if channel := strings.TrimSpace(scenario.Channel); channel != "" {
		return channel
	}
	return "eval"
}

func scenarioChatID(scenario Scenario) string {
	if chatID := strings.TrimSpace(scenario.ChatID); chatID != "" {
		return chatID
	}
	return "fixture"
}
