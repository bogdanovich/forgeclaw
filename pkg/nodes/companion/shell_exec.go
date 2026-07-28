package companion

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	MaxShellBrokerProfiles = 1
)

var ErrShellBrokerCancellationConfirmed = errors.New("shell broker confirmed process-domain termination")

var ErrShellBrokerOutcomeUnknown = errors.New("shell broker execution outcome is unknown")

// ShellBroker is the narrow companion-side client contract. Execute may return
// ErrShellBrokerCancellationConfirmed only after the broker-owned process
// domain is empty.
type ShellBroker interface {
	Execute(context.Context, ShellBrokerRequest) (ShellBrokerResult, error)
}

type ShellBrokerSnapshot struct {
	Revision string               `json:"revision"`
	Profiles []ShellBrokerProfile `json:"profiles"`
}

type ShellBrokerProfile struct {
	Alias                   string   `json:"alias"`
	Revision                string   `json:"revision"`
	WorkingScopes           []string `json:"working_scopes"`
	EnvironmentNames        []string `json:"environment_names"`
	TimeoutSecondsMax       int      `json:"timeout_seconds_max"`
	OutputBytesMax          int      `json:"output_bytes_max"`
	ConcurrentCommands      int      `json:"concurrent_commands"`
	ConcurrentTerminals     int      `json:"concurrent_terminals"`
	TerminalIdleSeconds     int      `json:"terminal_idle_seconds"`
	TerminalLifetimeSeconds int      `json:"terminal_lifetime_seconds"`
	TerminalBufferBytes     int      `json:"terminal_buffer_bytes"`
}

type ShellBrokerRequest struct {
	InvocationID    string            `json:"invocation_id"`
	PlanHash        string            `json:"plan_hash"`
	Profile         string            `json:"profile"`
	ProfileRevision string            `json:"profile_revision"`
	Script          string            `json:"script"`
	WorkingScope    string            `json:"working_scope"`
	Environment     map[string]string `json:"environment"`
	TimeoutSeconds  int               `json:"timeout_seconds"`
	OutputBytesMax  int               `json:"output_bytes_max"`
}

type ShellBrokerResult struct {
	ExitCode    int    `json:"exit_code"`
	Stdout      string `json:"stdout"`
	Stderr      string `json:"stderr"`
	Signal      string `json:"signal"`
	Truncated   bool   `json:"truncated"`
	StartedAt   int64  `json:"started_at"`
	CompletedAt int64  `json:"completed_at"`
}

type shellExecInput struct {
	Profile        string            `json:"profile"`
	Script         string            `json:"script"`
	CWD            string            `json:"cwd"`
	Env            map[string]string `json:"env"`
	TimeoutSeconds int               `json:"timeout_seconds"`
}

type shellExecRuntime struct {
	handler *shellExecHandler
}

type shellExecHandler struct {
	snapshot ShellBrokerSnapshot
	profile  ShellBrokerProfile
	broker   ShellBroker
	contract *nodes.CommandModelContract
}

func newShellExecRuntime(snapshot ShellBrokerSnapshot, broker ShellBroker) (*shellExecRuntime, error) {
	if broker == nil {
		return nil, errors.New("shell broker is required")
	}
	normalized, err := normalizeShellBrokerSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	return &shellExecRuntime{
		handler: &shellExecHandler{
			snapshot: normalized,
			profile:  normalized.Profiles[0],
			broker:   broker,
		},
	}, nil
}

func normalizeShellBrokerSnapshot(snapshot ShellBrokerSnapshot) (ShellBrokerSnapshot, error) {
	snapshot.Revision = strings.TrimSpace(snapshot.Revision)
	if !validShellBrokerRevision(snapshot.Revision) {
		return ShellBrokerSnapshot{}, errors.New("shell broker revision is invalid")
	}
	if len(snapshot.Profiles) != MaxShellBrokerProfiles {
		return ShellBrokerSnapshot{}, errors.New("shell broker must expose exactly one P1 profile")
	}
	profile := snapshot.Profiles[0]
	profile.Alias = strings.TrimSpace(profile.Alias)
	profile.Revision = strings.TrimSpace(profile.Revision)
	if profile.ConcurrentTerminals == 0 {
		profile.ConcurrentTerminals = 1
	}
	if profile.TerminalIdleSeconds == 0 {
		profile.TerminalIdleSeconds = DefaultTerminalIdleSeconds
	}
	if profile.TerminalLifetimeSeconds == 0 {
		profile.TerminalLifetimeSeconds = MaxTerminalLifetimeSeconds
	}
	if profile.TerminalBufferBytes == 0 {
		profile.TerminalBufferBytes = DefaultTerminalBufferBytes
	}
	if profile.Alias == "" || !validShellBrokerRevision(profile.Revision) ||
		profile.TimeoutSecondsMax <= 0 ||
		profile.TimeoutSecondsMax > nodes.MaxInvocationTimeout ||
		profile.OutputBytesMax <= 0 ||
		profile.OutputBytesMax > 128*1024 ||
		profile.ConcurrentCommands <= 0 ||
		profile.ConcurrentCommands > 8 ||
		profile.ConcurrentTerminals <= 0 ||
		profile.ConcurrentTerminals > maxConcurrentTerminals ||
		profile.TerminalIdleSeconds <= 0 ||
		profile.TerminalIdleSeconds > MaxTerminalIdleSeconds ||
		profile.TerminalLifetimeSeconds < profile.TerminalIdleSeconds ||
		profile.TerminalLifetimeSeconds > MaxTerminalLifetimeSeconds ||
		profile.TerminalBufferBytes <= 0 ||
		profile.TerminalBufferBytes > MaxTerminalBufferBytes {
		return ShellBrokerSnapshot{}, errors.New("shell broker profile is invalid")
	}
	profile.WorkingScopes = append([]string(nil), profile.WorkingScopes...)
	profile.EnvironmentNames = append([]string(nil), profile.EnvironmentNames...)
	slices.Sort(profile.WorkingScopes)
	slices.Sort(profile.EnvironmentNames)
	contract := nodes.CommandModelContract{
		Availability:      nodes.ModelAvailable,
		TimeoutSecondsMax: profile.TimeoutSecondsMax,
		OutputBytesMax:    profile.OutputBytesMax,
		ResultKind:        "json",
		AuthorityDigest:   strings.Repeat("0", 64),
		ApprovalMode:      "each_command",
		Constraints: nodes.CommandModelConstraints{
			ProfileAliases:   []string{profile.Alias},
			WorkingScopes:    profile.WorkingScopes,
			EnvironmentNames: profile.EnvironmentNames,
		},
		Guidance: []string{},
		Examples: []json.RawMessage{},
	}
	if _, err := nodes.ShellExecModelInputSchema(contract); err != nil {
		return ShellBrokerSnapshot{}, fmt.Errorf("validate shell broker projection: %w", err)
	}
	snapshot.Profiles = []ShellBrokerProfile{profile}
	return snapshot, nil
}

func validShellBrokerRevision(revision string) bool {
	return nodes.ExecutionProfile{
		Executor:       "shell-broker",
		PolicyRevision: revision,
	}.Validate() == nil
}

func (handler *shellExecHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name: "shell.exec.v1",
		InputSchema: json.RawMessage(
			`{"type":"object","required":["profile","script","cwd","env","timeout_seconds"],"properties":{"profile":{"type":"string"},"script":{"type":"string","minLength":1,"maxLength":65536},"cwd":{"type":"string"},"env":{"type":"object","maxProperties":64,"additionalProperties":{"type":"string","maxLength":8192}},"timeout_seconds":{"type":"integer","minimum":1,"maximum":3600}},"additionalProperties":false}`,
		),
		OutputSchema: json.RawMessage(
			`{"type":"object","required":["exit_code","stdout","stderr","signal","truncated","started_at","completed_at"],"properties":{"exit_code":{"type":"integer"},"stdout":{"type":"string"},"stderr":{"type":"string"},"signal":{"type":"string","maxLength":32},"truncated":{"type":"boolean"},"started_at":{"type":"integer"},"completed_at":{"type":"integer"}},"additionalProperties":false}`,
		),
		Risk:           nodes.RiskPrivileged,
		SupportsCancel: true,
		ModelContract:  cloneModelContract(handler.contract),
	}
}

func (handler *shellExecHandler) modelContract(
	policy nodes.LocalCommandPolicy,
) (*nodes.CommandModelContract, error) {
	availability := nodes.ModelAvailable
	if !slices.Contains(policy.AllowedCommands, "shell.exec.v1") ||
		modelRiskRank(policy.MaximumRisk) < modelRiskRank(nodes.RiskPrivileged) {
		availability = nodes.ModelUnavailable
	}
	timeoutMax := min(handler.profile.TimeoutSecondsMax, policy.MaxTimeoutSeconds)
	outputMax := min(handler.profile.OutputBytesMax, policy.MaxOutputBytes)
	authorityDigest, err := shellBrokerAuthorityDigest(handler.snapshot, handler.profile)
	if err != nil {
		return nil, err
	}
	contract := &nodes.CommandModelContract{
		Availability:      availability,
		TimeoutSecondsMax: timeoutMax,
		OutputBytesMax:    outputMax,
		ResultKind:        "json",
		AuthorityDigest:   authorityDigest,
		ApprovalMode:      "each_command",
		Constraints: nodes.CommandModelConstraints{
			ProfileAliases:   []string{handler.profile.Alias},
			WorkingScopes:    append([]string(nil), handler.profile.WorkingScopes...),
			EnvironmentNames: append([]string(nil), handler.profile.EnvironmentNames...),
		},
		Guidance: []string{},
		Examples: []json.RawMessage{},
	}
	schema, err := nodes.ShellExecModelInputSchema(*contract)
	if err != nil {
		return nil, err
	}
	if err := contract.Validate(schema); err != nil {
		return nil, err
	}
	return contract, nil
}

func shellBrokerAuthorityDigest(
	snapshot ShellBrokerSnapshot,
	profile ShellBrokerProfile,
) (string, error) {
	authority, err := json.Marshal(struct {
		Revision string             `json:"revision"`
		Profile  ShellBrokerProfile `json:"profile"`
	}{Revision: snapshot.Revision, Profile: profile})
	if err != nil {
		return "", fmt.Errorf("encode shell broker authority: %w", err)
	}
	digest := sha256.Sum256(authority)
	return fmt.Sprintf("%x", digest[:]), nil
}

func (handler *shellExecHandler) authorize(plan nodes.ExecutionPlan) error {
	_, err := handler.prepare(plan)
	return err
}

func (handler *shellExecHandler) execute(
	ctx context.Context,
	invocation commandInvocation,
) (any, error) {
	request, err := handler.prepare(invocation.Plan)
	if err != nil {
		return nil, newCommandFailure("COMMAND_DENIED", "shell.exec input denied", err)
	}
	result, err := handler.broker.Execute(ctx, request)
	if err != nil {
		if errors.Is(err, ErrShellBrokerCancellationConfirmed) {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, newCommandFailure("TIMEOUT", "shell.exec timed out", err)
			}
			return nil, fmt.Errorf("%w: %v", errCommandCancellationConfirmed, err)
		}
		if errors.Is(err, ErrShellBrokerOutcomeUnknown) {
			return nil, fmt.Errorf("%w: %v", ErrInvocationOutcomeUnknown, err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, newCommandFailure("TIMEOUT", "shell.exec timed out", err)
		}
		return nil, newCommandFailure("BROKER_FAILED", "shell.exec broker failed", err)
	}
	if result.StartedAt <= 0 || result.CompletedAt < result.StartedAt {
		return nil, newCommandFailure(
			"INVALID_BROKER_RESULT",
			"shell.exec broker returned invalid timing",
			errors.New("invalid broker timing"),
		)
	}
	return result, nil
}

func (handler *shellExecHandler) prepare(plan nodes.ExecutionPlan) (ShellBrokerRequest, error) {
	var input shellExecInput
	if err := decodeStrictJSON(plan.Input, &input); err != nil {
		return ShellBrokerRequest{}, errors.New("invalid shell.exec input")
	}
	if input.Profile != handler.profile.Alias ||
		input.TimeoutSeconds <= 0 ||
		input.TimeoutSeconds > handler.profile.TimeoutSecondsMax ||
		input.TimeoutSeconds > plan.TimeoutSeconds {
		return ShellBrokerRequest{}, errors.New("shell.exec profile or timeout is invalid")
	}
	modelInput := map[string]any{
		"profile": input.Profile, "script": input.Script, "cwd": input.CWD,
		"env": stringMapToAny(input.Env), "timeout_seconds": input.TimeoutSeconds,
	}
	contract, err := handler.modelContract(nodes.LocalCommandPolicy{
		Revision:          "shell-authorize",
		AllowedCommands:   []string{"shell.exec.v1"},
		MaximumRisk:       nodes.RiskPrivileged,
		MaxTimeoutSeconds: handler.profile.TimeoutSecondsMax,
		MaxOutputBytes:    handler.profile.OutputBytesMax,
	})
	if err != nil {
		return ShellBrokerRequest{}, err
	}
	if err := nodes.ValidateShellExecModelInput(modelInput); err != nil {
		return ShellBrokerRequest{}, err
	}
	if err := nodes.ValidateShellExecModelInputSchema(*contract, modelInput); err != nil {
		return ShellBrokerRequest{}, err
	}
	return ShellBrokerRequest{
		InvocationID:    plan.InvocationID,
		PlanHash:        plan.PlanHash,
		Profile:         input.Profile,
		ProfileRevision: handler.profile.Revision,
		Script:          input.Script,
		WorkingScope:    input.CWD,
		Environment:     input.Env,
		TimeoutSeconds:  input.TimeoutSeconds,
		OutputBytesMax:  min(plan.OutputLimitBytes, handler.profile.OutputBytesMax),
	}, nil
}

func stringMapToAny(values map[string]string) map[string]any {
	result := make(map[string]any, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}
