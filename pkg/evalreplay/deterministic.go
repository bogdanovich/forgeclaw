package evalreplay

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type VirtualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start.UTC()}
}

func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *VirtualClock) Advance(duration time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
	return c.now
}

type IDSource interface {
	Next(prefix string) string
}

type SequentialIDSource struct {
	mu   sync.Mutex
	next uint64
}

func (s *SequentialIDSource) Next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	if prefix == "" {
		prefix = "id"
	}
	return fmt.Sprintf("%s-%06d", prefix, s.next)
}

type SideEffect string

const (
	SideEffectNetwork            SideEffect = "network"
	SideEffectMCP                SideEffect = "mcp"
	SideEffectShell              SideEffect = "shell"
	SideEffectFilesystemMutation SideEffect = "filesystem_mutation"
	SideEffectSubprocess         SideEffect = "subprocess"
	SideEffectGateway            SideEffect = "gateway"
)

type SideEffectPolicy struct{}

func (SideEffectPolicy) Authorize(effect SideEffect) error {
	return fmt.Errorf("replay side effect %q is structurally denied", effect)
}

type SafeTool func(arguments map[string]any) (map[string]any, error)

type ToolCatalog struct {
	tools map[string]SafeTool
}

type IsolatedState struct {
	Sessions map[string]json.RawMessage `json:"sessions,omitempty"`
	Tasks    map[string]json.RawMessage `json:"tasks,omitempty"`
}

type CheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]IsolatedState
}

func NewCheckpointStore() *CheckpointStore {
	return &CheckpointStore{checkpoints: make(map[string]IsolatedState)}
}

func (s *CheckpointStore) Save(id string, state IsolatedState) error {
	if id == "" {
		return fmt.Errorf("checkpoint id is required")
	}
	cloned, err := cloneState(state)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.checkpoints[id] = cloned
	s.mu.Unlock()
	return nil
}

func (s *CheckpointStore) Restore(id string) (IsolatedState, error) {
	s.mu.Lock()
	state, ok := s.checkpoints[id]
	s.mu.Unlock()
	if !ok {
		return IsolatedState{}, fmt.Errorf("checkpoint %q does not exist", id)
	}
	return cloneState(state)
}

func NewToolCatalog(tools map[string]SafeTool) ToolCatalog {
	copyTools := make(map[string]SafeTool, len(tools))
	for name, tool := range tools {
		if name != "" && tool != nil {
			copyTools[name] = tool
		}
	}
	return ToolCatalog{tools: copyTools}
}

func (c ToolCatalog) Execute(name string, arguments map[string]any) (map[string]any, error) {
	tool := c.tools[name]
	if tool == nil {
		return nil, fmt.Errorf("replay tool %q is not registered", name)
	}
	return tool(cloneMap(arguments))
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = cloneValue(value)
	}
	return output
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		result := make([]any, len(typed))
		for i := range typed {
			result[i] = cloneValue(typed[i])
		}
		return result
	default:
		return typed
	}
}

func cloneState(state IsolatedState) (IsolatedState, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return IsolatedState{}, fmt.Errorf("encode isolated state: %w", err)
	}
	var cloned IsolatedState
	if err := json.Unmarshal(data, &cloned); err != nil {
		return IsolatedState{}, fmt.Errorf("decode isolated state: %w", err)
	}
	return cloned, nil
}
