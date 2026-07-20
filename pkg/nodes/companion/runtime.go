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
}

func NewRuntime(
	nodeID nodes.ID,
	version string,
	policy nodes.LocalCommandPolicy,
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
	return &Runtime{nodeID: nodeID, policy: policy, catalog: catalog, handlers: byName}, nil
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
	if err := runtime.policy.Authorize(plan, runtime.catalog, runtime.nodeID, LocalExecutor, time.Now()); err != nil {
		return nil, err
	}
	handler := runtime.handlers[plan.Command]
	if handler == nil {
		return nil, ErrCommandUnavailable
	}
	deadline := time.Now().Add(time.Duration(plan.TimeoutSeconds) * time.Second)
	if expires := time.Unix(plan.ExpiresAt, 0); expires.Before(deadline) {
		deadline = expires
	}
	invokeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	result, err := handler.execute(invokeCtx, plan.Input)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("encode command output: %w", err)
	}
	return nodes.ValidateInvocationOutput(handler.descriptor(), raw, plan.OutputLimitBytes)
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
