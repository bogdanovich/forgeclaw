package nodes

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/sipeed/picoclaw/pkg/nodes/internal/jsonstrict"
)

const (
	MaxInvocationInputBytes = 512 * 1024
	MaxInvocationTimeout    = 60 * 60
	MaxInvocationOutput     = 16 * 1024 * 1024
	MaxPolicyRevisionLength = 128
	MaxExecutionPlanTTL     = 5 * time.Minute
	MaxExecutionPlanSkew    = 30 * time.Second
)

var (
	ErrInvalidInvocation = errors.New("invalid node invocation")
	ErrCommandDenied     = errors.New("node command denied")
)

// InvocationRequest is the transport-neutral command request prepared by the
// gateway. It contains no connection details or shell-specific authority.
type InvocationRequest struct {
	InvocationID     string          `json:"invocation_id"`
	IdempotencyKey   string          `json:"idempotency_key"`
	NodeID           ID              `json:"node_id"`
	Command          string          `json:"command"`
	Input            json.RawMessage `json:"input"`
	AgentID          string          `json:"agent_id"`
	SessionID        string          `json:"session_id"`
	ActorID          string          `json:"actor_id"`
	TimeoutSeconds   int             `json:"timeout_seconds"`
	OutputLimitBytes int             `json:"output_limit_bytes"`
}

func (request InvocationRequest) Validate() error {
	if !validInvocationIdentifier(request.InvocationID) ||
		!validInvocationIdentifier(request.IdempotencyKey) ||
		!validInvocationIdentifier(request.AgentID) ||
		!validInvocationIdentifier(request.SessionID) ||
		!validInvocationIdentifier(request.ActorID) {
		return fmt.Errorf("%w: malformed identity field", ErrInvalidInvocation)
	}
	if err := request.NodeID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInvocation, err)
	}
	if len(request.Command) == 0 || len(request.Command) > MaxCommandNameLen ||
		!commandPattern.MatchString(request.Command) {
		return fmt.Errorf("%w: malformed command", ErrInvalidInvocation)
	}
	if request.TimeoutSeconds <= 0 || request.TimeoutSeconds > MaxInvocationTimeout {
		return fmt.Errorf("%w: timeout is outside bounds", ErrInvalidInvocation)
	}
	if request.OutputLimitBytes <= 0 || request.OutputLimitBytes > MaxInvocationOutput {
		return fmt.Errorf("%w: output limit is outside bounds", ErrInvalidInvocation)
	}
	if _, err := canonicalInvocationInput(request.Input); err != nil {
		return err
	}
	return nil
}

// ExecutionPlan is the canonical authority reviewed before dispatch. PlanHash
// is a binding digest, not proof of origin; approval and ledger records retain
// the expected digest independently and compare it before dispatch.
type ExecutionPlan struct {
	InvocationRequest
	Risk           Risk   `json:"risk"`
	Executor       string `json:"executor"`
	PolicyRevision string `json:"policy_revision"`
	PreparedAt     int64  `json:"prepared_at"`
	ExpiresAt      int64  `json:"expires_at"`
	PlanHash       string `json:"plan_hash"`
}

func PrepareExecutionPlan(
	request InvocationRequest,
	descriptor CommandDescriptor,
	executor string,
	policyRevision string,
	preparedAt time.Time,
	ttl time.Duration,
) (ExecutionPlan, error) {
	if err := request.Validate(); err != nil {
		return ExecutionPlan{}, err
	}
	if err := descriptor.Validate(); err != nil {
		return ExecutionPlan{}, err
	}
	if descriptor.Name != request.Command {
		return ExecutionPlan{}, fmt.Errorf("%w: descriptor does not match command", ErrInvalidInvocation)
	}
	if !validInvocationIdentifier(executor) || len(policyRevision) == 0 ||
		len(policyRevision) > MaxPolicyRevisionLength || !idPattern.MatchString(policyRevision) {
		return ExecutionPlan{}, fmt.Errorf("%w: malformed execution policy", ErrInvalidInvocation)
	}
	if preparedAt.Unix() <= 0 || ttl < time.Second || ttl > MaxExecutionPlanTTL {
		return ExecutionPlan{}, fmt.Errorf("%w: plan lifetime is outside bounds", ErrInvalidInvocation)
	}
	input, value, err := canonicalInvocationInputValue(request.Input)
	if err != nil {
		return ExecutionPlan{}, err
	}
	if validationErr := validateInvocationInput(descriptor.InputSchema, value); validationErr != nil {
		return ExecutionPlan{}, validationErr
	}
	request.Input = input
	plan := ExecutionPlan{
		InvocationRequest: request,
		Risk:              descriptor.Risk,
		Executor:          executor,
		PolicyRevision:    policyRevision,
		PreparedAt:        preparedAt.Unix(),
		ExpiresAt:         preparedAt.Add(ttl).Unix(),
	}
	hash, err := plan.computeHash()
	if err != nil {
		return ExecutionPlan{}, err
	}
	plan.PlanHash = hash
	return plan, nil
}

func (plan ExecutionPlan) Validate() error {
	if err := plan.InvocationRequest.Validate(); err != nil {
		return err
	}
	if !plan.Risk.Valid() || !validInvocationIdentifier(plan.Executor) ||
		len(plan.PolicyRevision) == 0 || len(plan.PolicyRevision) > MaxPolicyRevisionLength ||
		!idPattern.MatchString(plan.PolicyRevision) {
		return fmt.Errorf("%w: malformed execution policy", ErrInvalidInvocation)
	}
	if plan.PreparedAt <= 0 || plan.ExpiresAt <= plan.PreparedAt ||
		plan.ExpiresAt-plan.PreparedAt > int64(MaxExecutionPlanTTL/time.Second) {
		return fmt.Errorf("%w: plan lifetime is outside bounds", ErrInvalidInvocation)
	}
	wantHash, err := plan.computeHash()
	if err != nil {
		return err
	}
	if plan.PlanHash != wantHash {
		return fmt.Errorf("%w: plan hash mismatch", ErrInvalidInvocation)
	}
	return nil
}

// ValidateAgainstHash verifies both plan self-consistency and the binding to a
// digest retained outside the mutable plan, such as an approval record.
func (plan ExecutionPlan) ValidateAgainstHash(expected string) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(expected)
	if err != nil || len(decoded) != sha256.Size ||
		subtle.ConstantTimeCompare([]byte(plan.PlanHash), []byte(expected)) != 1 {
		return fmt.Errorf("%w: plan does not match retained hash", ErrCommandDenied)
	}
	return nil
}

func (plan ExecutionPlan) computeHash() (string, error) {
	plan.PlanHash = ""
	data, err := json.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("%w: encode plan: %v", ErrInvalidInvocation, err)
	}
	canonical, err := jsonstrict.Canonical(data)
	if err != nil {
		return "", fmt.Errorf("%w: canonicalize plan: %v", ErrInvalidInvocation, err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// ApprovedCommand applies the durable pairing command surface. It does not
// replace agent, approval, or node-local policy checks.
func (registration Registration) ApprovedCommand(name string) (CommandDescriptor, error) {
	if registration.Snapshot.State != StateConnected {
		return CommandDescriptor{}, fmt.Errorf("%w: node is not connected", ErrCommandDenied)
	}
	descriptor, advertised := registration.Snapshot.Catalog.command(name)
	if !advertised {
		return CommandDescriptor{}, fmt.Errorf("%w: command is not advertised", ErrCommandDenied)
	}
	if !slices.Contains(registration.AllowedCommands, name) {
		return CommandDescriptor{}, fmt.Errorf("%w: command is not approved", ErrCommandDenied)
	}
	return descriptor, nil
}

// LocalCommandPolicy is the companion-owned maximum authority. Empty command
// lists deny all commands, including commands approved by the gateway.
type LocalCommandPolicy struct {
	Revision          string   `json:"revision"`
	AllowedCommands   []string `json:"allowed_commands"`
	MaximumRisk       Risk     `json:"maximum_risk"`
	MaxTimeoutSeconds int      `json:"max_timeout_seconds"`
	MaxOutputBytes    int      `json:"max_output_bytes"`
}

func (policy LocalCommandPolicy) Validate() error {
	if len(policy.Revision) == 0 || len(policy.Revision) > MaxPolicyRevisionLength ||
		!idPattern.MatchString(policy.Revision) || !policy.MaximumRisk.Valid() {
		return fmt.Errorf("%w: malformed local policy", ErrCommandDenied)
	}
	if policy.MaxTimeoutSeconds <= 0 || policy.MaxTimeoutSeconds > MaxInvocationTimeout ||
		policy.MaxOutputBytes <= 0 || policy.MaxOutputBytes > MaxInvocationOutput {
		return fmt.Errorf("%w: local policy limits are outside bounds", ErrCommandDenied)
	}
	seen := make(map[string]struct{}, len(policy.AllowedCommands))
	for _, command := range policy.AllowedCommands {
		if !commandPattern.MatchString(command) {
			return fmt.Errorf("%w: malformed allowed command", ErrCommandDenied)
		}
		if _, exists := seen[command]; exists {
			return fmt.Errorf("%w: duplicate allowed command", ErrCommandDenied)
		}
		seen[command] = struct{}{}
	}
	return nil
}

func (policy LocalCommandPolicy) Authorize(
	plan ExecutionPlan,
	descriptor CommandDescriptor,
	receivingNodeID ID,
	actualExecutor string,
	now time.Time,
) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if err := plan.Validate(); err != nil {
		return err
	}
	if err := descriptor.Validate(); err != nil {
		return err
	}
	if err := receivingNodeID.Validate(); err != nil || !validInvocationIdentifier(actualExecutor) ||
		plan.NodeID != receivingNodeID || plan.Executor != actualExecutor {
		return fmt.Errorf("%w: plan target does not match local runtime", ErrCommandDenied)
	}
	if descriptor.Name != plan.Command || descriptor.Risk != plan.Risk ||
		plan.PolicyRevision != policy.Revision {
		return fmt.Errorf("%w: plan does not match current policy or descriptor", ErrCommandDenied)
	}
	nowUnix := now.Unix()
	if nowUnix <= 0 ||
		(plan.PreparedAt > nowUnix && plan.PreparedAt-nowUnix > int64(MaxExecutionPlanSkew/time.Second)) ||
		nowUnix >= plan.ExpiresAt {
		return fmt.Errorf("%w: plan is not currently valid", ErrCommandDenied)
	}
	_, input, err := canonicalInvocationInputValue(plan.Input)
	if err != nil {
		return err
	}
	if err := validateInvocationInput(descriptor.InputSchema, input); err != nil {
		return err
	}
	if !slices.Contains(policy.AllowedCommands, plan.Command) ||
		riskRank(plan.Risk) > riskRank(policy.MaximumRisk) ||
		plan.TimeoutSeconds > policy.MaxTimeoutSeconds ||
		plan.OutputLimitBytes > policy.MaxOutputBytes {
		return fmt.Errorf("%w: plan exceeds local policy", ErrCommandDenied)
	}
	return nil
}

func (catalog CapabilityCatalog) command(name string) (CommandDescriptor, bool) {
	for _, descriptor := range catalog.Commands {
		if descriptor.Name == name {
			return descriptor, true
		}
	}
	return CommandDescriptor{}, false
}

func canonicalInvocationInput(raw json.RawMessage) (json.RawMessage, error) {
	canonical, _, err := canonicalInvocationInputValue(raw)
	return canonical, err
}

func canonicalInvocationInputValue(raw json.RawMessage) (json.RawMessage, map[string]any, error) {
	if len(raw) == 0 || len(raw) > MaxInvocationInputBytes {
		return nil, nil, fmt.Errorf("%w: input is outside bounds", ErrInvalidInvocation)
	}
	value, err := jsonstrict.Decode(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: invalid input: %v", ErrInvalidInvocation, err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, nil, fmt.Errorf("%w: input must be an object", ErrInvalidInvocation)
	}
	canonical, err := jsonstrict.Canonical(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: canonicalize input: %v", ErrInvalidInvocation, err)
	}
	// jsonstrict preserves numeric precision with json.Number, while the schema
	// validator classifies concrete Go numeric types. Decode the already strict,
	// canonical object once more only for schema validation.
	var schemaValue map[string]any
	if err := json.Unmarshal(canonical, &schemaValue); err != nil {
		return nil, nil, fmt.Errorf("%w: decode canonical input: %v", ErrInvalidInvocation, err)
	}
	return json.RawMessage(canonical), schemaValue, nil
}

func validateInvocationInput(rawSchema json.RawMessage, input map[string]any) error {
	var schema jsonschema.Schema
	if err := json.Unmarshal(rawSchema, &schema); err != nil {
		return fmt.Errorf("%w: decode input schema: %v", ErrInvalidInvocation, err)
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		return fmt.Errorf("%w: resolve input schema: %v", ErrInvalidInvocation, err)
	}
	if err := resolved.Validate(input); err != nil {
		return fmt.Errorf("%w: input violates command schema: %v", ErrInvalidInvocation, err)
	}
	return nil
}

func validInvocationIdentifier(value string) bool {
	return len(value) > 0 && len(value) <= MaxIDLength && idPattern.MatchString(value)
}

func riskRank(risk Risk) int {
	switch risk {
	case RiskRead:
		return 1
	case RiskWrite:
		return 2
	case RiskPrivileged:
		return 3
	default:
		return 4
	}
}
