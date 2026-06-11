package commands

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/config"
)

type MCPServerInfo struct {
	Name      string
	Enabled   bool
	Deferred  bool
	Connected bool
	ToolCount int
}

type MCPToolParameterInfo struct {
	Name        string
	Type        string
	Description string
	Required    bool
}

type MCPToolInfo struct {
	Name        string
	Description string
	Parameters  []MCPToolParameterInfo
}

type ConfiguredModelInfo struct {
	Name    string
	Current bool
	Targets []ConfiguredModelTarget
}

type ConfiguredModelTarget struct {
	Provider  string
	Model     string
	Workspace string
	Count     int
}

// ContextStats describes current session context window usage.
type ContextStats struct {
	ContextManager            string
	TotalTokens               int // model context window
	CompressAtTokens          int // hard budget compression threshold
	SummarizeAtTokens         int // soft summarization trigger
	SummarizeMessageThreshold int
	StoredUsedTokens          int
	StoredHistoryTokens       int
	StoredUsedPercent         int // 0-100 against compressAt
	StoredMessageCount        int
	AssembledUsedTokens       int
	AssembledHistoryTokens    int
	AssembledUsedPercent      int // 0-100 against compressAt
	AssembledMessageCount     int
}

// StopResult describes the outcome of a stop request for the current session.
type StopResult struct {
	Stopped  bool
	TaskName string
}

// Runtime provides runtime dependencies to command handlers. It is constructed
// per-request by the agent loop so that per-request state (like session scope)
// can coexist with long-lived callbacks (like GetModelInfo).
type Runtime struct {
	Config             *config.Config
	GetModelInfo       func() (name, provider string)
	ListModels         func() []ConfiguredModelInfo
	AskSideQuestion    func(ctx context.Context, question string) (string, error)
	ListAgentIDs       func() []string
	ListDefinitions    func() []Definition
	ListSkillNames     func() []string
	ListMCPServers     func(ctx context.Context) []MCPServerInfo
	ListMCPTools       func(ctx context.Context, serverName string) ([]MCPToolInfo, error)
	GetEnabledChannels func() []string
	GetActiveTurn      func() any // Returning any to avoid circular dependency with agent package
	GetContextStats    func() *ContextStats
	SwitchModel        func(value string) (oldModel string, err error)
	SwitchChannel      func(value string) error
	ResetSession       func(clearOverride bool) (sessionKey string, err error)
	SetToolFeedback    func(mode string) (enabled bool, source string, err error)
	GetToolFeedback    func() (enabled bool, source string)
	ClearHistory       func() error
	ReloadConfig       func() error
	StopActiveTurn     func() (StopResult, error)
}
