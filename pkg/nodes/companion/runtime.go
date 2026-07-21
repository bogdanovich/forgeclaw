package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

const LocalExecutor = "local"

var ErrCommandUnavailable = errors.New("node command is unavailable")

var ErrInvocationOutcomeUnknown = errors.New("node invocation outcome is unknown")

type recordedInvocationError struct {
	failure nodes.InvocationFailure
}

func (err *recordedInvocationError) Error() string {
	return err.failure.Message
}

type commandHandler interface {
	descriptor() nodes.CommandDescriptor
	execute(context.Context, json.RawMessage) (any, error)
}

// Runtime is the instance-scoped capability boundary. It owns no gateway
// connection and can therefore be reused by a future multi-binding supervisor.
type Runtime struct {
	nodeID   nodes.ID
	policy   nodes.LocalCommandPolicy
	catalog  nodes.CapabilityCatalog
	handlers map[string]commandHandler
	ledger   *InvocationLedger
}

func NewRuntime(
	nodeID nodes.ID,
	version string,
	policy nodes.LocalCommandPolicy,
	ledger *InvocationLedger,
) (*Runtime, error) {
	handlers := []commandHandler{
		nodeInfoHandler{nodeID: nodeID, version: version},
		systemWhichHandler{},
	}
	catalog := nodes.CapabilityCatalog{Commands: make([]nodes.CommandDescriptor, 0, len(handlers))}
	byName := make(map[string]commandHandler, len(handlers))
	for _, handler := range handlers {
		descriptor := handler.descriptor()
		catalog.Commands = append(catalog.Commands, descriptor)
		byName[descriptor.Name] = handler
	}
	if err := nodeID.Validate(); err != nil {
		return nil, err
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	if ledger == nil {
		return nil, errors.New("node invocation ledger is required")
	}
	return &Runtime{
		nodeID:   nodeID,
		policy:   policy,
		catalog:  catalog,
		handlers: byName,
		ledger:   ledger,
	}, nil
}

func (runtime *Runtime) Catalog() nodes.CapabilityCatalog {
	return cloneCatalog(runtime.catalog)
}

func (runtime *Runtime) Invoke(
	ctx context.Context,
	plan nodes.ExecutionPlan,
) (json.RawMessage, error) {
	if runtime == nil {
		return nil, ErrCommandUnavailable
	}
	record, existing, existingErr := runtime.ledger.Existing(plan)
	if existingErr != nil {
		return nil, existingErr
	}
	if existing {
		if record.State == nodes.InvocationAccepted {
			if err := runtime.policy.Authorize(
				plan,
				runtime.catalog,
				runtime.nodeID,
				LocalExecutor,
				time.Now(),
			); err != nil {
				return nil, err
			}
			return runtime.executeAccepted(ctx, plan)
		}
		if err := runtime.policy.AuthorizeReplay(
			plan,
			runtime.catalog,
			runtime.nodeID,
			LocalExecutor,
		); err != nil {
			return nil, err
		}
		return invocationRecordResult(record)
	}
	if err := runtime.policy.Authorize(plan, runtime.catalog, runtime.nodeID, LocalExecutor, time.Now()); err != nil {
		return nil, err
	}
	handler := runtime.handlers[plan.Command]
	if handler == nil {
		return nil, ErrCommandUnavailable
	}
	record, existing, acceptErr := runtime.ledger.Accept(plan)
	if acceptErr != nil {
		return nil, acceptErr
	}
	if existing {
		if record.State == nodes.InvocationAccepted {
			return runtime.executeAccepted(ctx, plan)
		}
		return invocationRecordResult(record)
	}
	return runtime.executeAccepted(ctx, plan)
}

func (runtime *Runtime) executeAccepted(
	ctx context.Context,
	plan nodes.ExecutionPlan,
) (json.RawMessage, error) {
	handler := runtime.handlers[plan.Command]
	if handler == nil {
		return nil, ErrCommandUnavailable
	}
	if _, err := runtime.ledger.MarkRunning(plan.InvocationID); err != nil {
		record, found, lookupErr := runtime.ledger.Lookup(plan.InvocationID)
		if lookupErr == nil && found && record.State.Terminal() {
			return invocationRecordResult(record)
		}
		return nil, fmt.Errorf("%w: persist running state: %v", ErrInvocationOutcomeUnknown, err)
	}
	deadline := time.Now().Add(time.Duration(plan.TimeoutSeconds) * time.Second)
	if expires := time.Unix(plan.ExpiresAt, 0); expires.Before(deadline) {
		deadline = expires
	}
	invokeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	result, executeErr := handler.execute(invokeCtx, plan.Input)
	if executeErr != nil {
		return nil, runtime.completeInvocationFailure(plan.InvocationID, nodes.InvocationFailure{
			Code:    "EXECUTION_FAILED",
			Message: "node command failed",
		}, executeErr)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		encodeErr := fmt.Errorf("encode command output: %w", err)
		return nil, runtime.completeInvocationFailure(plan.InvocationID, nodes.InvocationFailure{
			Code:    "INVALID_OUTPUT",
			Message: "node command returned invalid output",
		}, encodeErr)
	}
	raw, err = nodes.ValidateInvocationOutput(handler.descriptor(), raw, plan.OutputLimitBytes)
	if err != nil {
		return nil, runtime.completeInvocationFailure(plan.InvocationID, nodes.InvocationFailure{
			Code:    "INVALID_OUTPUT",
			Message: "node command returned invalid output",
		}, err)
	}
	if _, err := runtime.ledger.CompleteSuccess(plan.InvocationID, raw); err != nil {
		return nil, fmt.Errorf("%w: persist successful result: %v", ErrInvocationOutcomeUnknown, err)
	}
	return raw, nil
}

func (runtime *Runtime) completeInvocationFailure(
	invocationID string,
	failure nodes.InvocationFailure,
	cause error,
) error {
	if _, err := runtime.ledger.CompleteFailure(invocationID, failure); err != nil {
		return fmt.Errorf("%w: persist failed result: %v", ErrInvocationOutcomeUnknown, err)
	}
	return cause
}

func (runtime *Runtime) Invocation(
	invocationID string,
) (nodes.InvocationRecord, bool, error) {
	if runtime == nil || runtime.ledger == nil {
		return nodes.InvocationRecord{}, false, nil
	}
	return runtime.ledger.Lookup(invocationID)
}

func invocationRecordResult(record nodes.InvocationRecord) (json.RawMessage, error) {
	switch record.State {
	case nodes.InvocationSucceeded:
		return append(json.RawMessage(nil), record.Result...), nil
	case nodes.InvocationFailed, nodes.InvocationCanceled:
		if record.Failure == nil {
			return nil, ErrInvocationOutcomeUnknown
		}
		return nil, &recordedInvocationError{failure: *record.Failure}
	default:
		return nil, ErrInvocationOutcomeUnknown
	}
}

type nodeInfoHandler struct {
	nodeID  nodes.ID
	version string
}

func (handler nodeInfoHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:        "node.info.v1",
		InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(
			`{"type":"object","required":["node_id","platform","architecture","version"],"properties":{"node_id":{"type":"string"},"platform":{"type":"string"},"architecture":{"type":"string"},"version":{"type":"string"}},"additionalProperties":false}`,
		),
		Risk: nodes.RiskRead,
	}
}

func (handler nodeInfoHandler) execute(context.Context, json.RawMessage) (any, error) {
	return struct {
		NodeID       nodes.ID `json:"node_id"`
		Platform     string   `json:"platform"`
		Architecture string   `json:"architecture"`
		Version      string   `json:"version"`
	}{handler.nodeID, runtime.GOOS, runtime.GOARCH, handler.version}, nil
}

type systemWhichHandler struct{}

func (systemWhichHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name: "system.which.v1",
		InputSchema: json.RawMessage(
			`{"type":"object","required":["name"],"properties":{"name":{"type":"string","pattern":"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$"}},"additionalProperties":false}`,
		),
		OutputSchema: json.RawMessage(
			`{"type":"object","required":["found","path"],"properties":{"found":{"type":"boolean"},"path":{"type":"string"}},"additionalProperties":false}`,
		),
		Risk: nodes.RiskRead,
	}
}

func (systemWhichHandler) execute(ctx context.Context, raw json.RawMessage) (any, error) {
	var input struct {
		Name string `json:"name"`
	}
	if err := decodeStrictJSON(raw, &input); err != nil {
		return nil, fmt.Errorf("decode system.which input: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := exec.LookPath(input.Name)
	if err != nil {
		path = ""
	}
	return struct {
		Found bool   `json:"found"`
		Path  string `json:"path"`
	}{err == nil, path}, nil
}
