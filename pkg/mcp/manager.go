package mcp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/logger"
)

// headerTransport is an http.RoundTripper that adds custom headers to requests
type headerTransport struct {
	base    http.RoundTripper
	headers map[string]string
}

func expandHomeCommandPath(command string) string {
	if command == "" || command[0] != '~' {
		return command
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return command
	}
	if command == "~" {
		return home
	}
	if strings.HasPrefix(command, "~/") || strings.HasPrefix(command, "~\\") {
		return filepath.Join(home, command[2:])
	}
	return command
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	req = req.Clone(req.Context())

	// Add custom headers
	for key, value := range t.headers {
		req.Header.Set(key, value)
	}

	// Use the base transport
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req)
}

// loadEnvFile loads environment variables from a file in .env format
// Each line should be in the format: KEY=value
// Lines starting with # are comments
// Empty lines are ignored
func loadEnvFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open env file: %w", err)
	}
	defer file.Close()

	envVars := make(map[string]string)
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse KEY=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid format at line %d: %s", lineNum, line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if key == "" {
			return nil, fmt.Errorf("invalid format at line %d: empty key", lineNum)
		}

		// Remove surrounding quotes if present
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') ||
				(value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}

		envVars[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading env file: %w", err)
	}

	return envVars, nil
}

type commandEnvironmentValue struct {
	name  string
	value string
}

// IsValidEnvironmentName reports whether name can be represented without
// changing its identity in an os/exec environment entry.
func IsValidEnvironmentName(name string) bool {
	return name != "" && !strings.ContainsAny(name, "=\x00")
}

func mergeCommandEnvironment(
	parent []string,
	fileValues map[string]string,
	explicitValues map[string]string,
	caseInsensitive bool,
) ([]string, error) {
	merged := make(map[string]commandEnvironmentValue)
	normalize := func(name string) string {
		if caseInsensitive {
			return strings.ToLower(name)
		}
		return name
	}
	set := func(name, value string) {
		merged[normalize(name)] = commandEnvironmentValue{name: name, value: value}
	}
	for _, entry := range parent {
		if index := strings.Index(entry, "="); index > 0 {
			set(entry[:index], entry[index+1:])
		}
	}
	setMap := func(values map[string]string) error {
		names := make([]string, 0, len(values))
		for name := range values {
			if !IsValidEnvironmentName(name) {
				return fmt.Errorf("invalid environment variable name %q", name)
			}
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			set(name, values[name])
		}
		return nil
	}
	if err := setMap(fileValues); err != nil {
		return nil, err
	}
	if err := setMap(explicitValues); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	environment := make([]string, 0, len(keys))
	for _, key := range keys {
		value := merged[key]
		environment = append(environment, value.name+"="+value.value)
	}
	return environment, nil
}

// ServerConnection represents a connection to an MCP server
type ServerConnection struct {
	Name          string
	Config        config.MCPServerConfig
	Client        *mcp.Client
	Session       *mcp.ClientSession
	Tools         []*mcp.Tool
	reconnectMu   sync.Mutex
	leaseGroup    *exclusiveLeaseGroup
	cleanup       io.Closer
	cleanupFailed bool
	// recoveryRequired is set after a session-loss reconnect fails. New calls
	// must recover this known-stale connection before dispatching a tool.
	recoveryRequired atomic.Bool
}

// Manager manages multiple MCP server connections
type Manager struct {
	servers        map[string]*ServerConnection
	pendingCleanup []*ServerConnection
	runtimeEvents  runtimeevents.Bus
	mu             sync.RWMutex
	closed         atomic.Bool    // changed from bool to atomic.Bool to avoid TOCTOU race
	wg             sync.WaitGroup // tracks in-flight CallTool calls
}

type exclusiveLeaseGroup struct {
	mu      sync.Mutex
	lease   *exclusiveServerLease
	members map[*ServerConnection]struct{}
}

var connectServerFunc = connectServer

// ManagerOption configures an MCP manager.
type ManagerOption func(*Manager)

// CallOutcomeUncertainError reports that an MCP tool call lost its server
// session after dispatch may have begun. The call was not replayed, so callers
// must inspect external state before deciding whether to issue a new action.
type CallOutcomeUncertainError struct {
	Server      string
	Tool        string
	Reconnected bool
}

func (e *CallOutcomeUncertainError) Error() string {
	message := "MCP tool outcome is uncertain after server session loss; do not retry automatically"
	if e != nil && e.Reconnected {
		return message + "; server reconnected for future calls"
	}
	return message + "; server reconnect failed"
}

// WithRuntimeEvents injects the runtime event bus used for MCP observations.
func WithRuntimeEvents(eventBus runtimeevents.Bus) ManagerOption {
	return func(m *Manager) {
		m.runtimeEvents = eventBus
	}
}

// ServerEventPayload describes MCP server connection events.
type ServerEventPayload struct {
	Server    string `json:"server"`
	Type      string `json:"type,omitempty"`
	URL       string `json:"url,omitempty"`
	Command   string `json:"command,omitempty"`
	Tool      string `json:"tool,omitempty"`
	ToolCount int    `json:"tool_count,omitempty"`
	Error     string `json:"error,omitempty"`
}

// NewManager creates a new MCP manager
func NewManager(opts ...ManagerOption) *Manager {
	m := &Manager{
		servers: make(map[string]*ServerConnection),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

// LoadFromConfig loads MCP servers from configuration
func (m *Manager) LoadFromConfig(ctx context.Context, cfg *config.Config) error {
	return m.LoadFromMCPConfig(ctx, cfg.Tools.MCP, cfg.WorkspacePath())
}

// LoadFromMCPConfig loads MCP servers from MCP configuration and workspace path.
// This is the minimal dependency version that doesn't require the full Config object.
func (m *Manager) LoadFromMCPConfig(
	ctx context.Context,
	mcpCfg config.MCPConfig,
	workspacePath string,
) error {
	if !mcpCfg.Enabled {
		logger.InfoCF("mcp", "MCP integration is disabled", nil)
		return nil
	}

	if len(mcpCfg.Servers) == 0 {
		logger.InfoCF("mcp", "No MCP servers configured", nil)
		return nil
	}

	logger.InfoCF("mcp", "Initializing MCP servers",
		map[string]any{
			"count": len(mcpCfg.Servers),
		})

	var wg sync.WaitGroup
	errs := make(chan error, len(mcpCfg.Servers))
	enabledCount := 0

	for name, serverCfg := range mcpCfg.Servers {
		if !serverCfg.Enabled {
			logger.DebugCF("mcp", "Skipping disabled server",
				map[string]any{
					"server": name,
				})
			continue
		}

		enabledCount++
		wg.Add(1)
		go func(name string, serverCfg config.MCPServerConfig, workspace string) {
			defer wg.Done()

			// Resolve relative envFile paths relative to workspace
			if serverCfg.EnvFile != "" && !filepath.IsAbs(serverCfg.EnvFile) {
				if workspace == "" {
					err := fmt.Errorf(
						"workspace path is empty while resolving relative envFile %q for server %s",
						serverCfg.EnvFile,
						name,
					)
					logger.ErrorCF("mcp", "Invalid MCP server configuration",
						map[string]any{
							"server":   name,
							"env_file": serverCfg.EnvFile,
							"error":    err.Error(),
						})
					errs <- err
					return
				}
				serverCfg.EnvFile = filepath.Join(workspace, serverCfg.EnvFile)
			}

			if err := m.ConnectServer(ctx, name, serverCfg); err != nil {
				logger.ErrorCF("mcp", "Failed to connect to MCP server",
					map[string]any{
						"server": name,
						"error":  err.Error(),
					})
				errs <- fmt.Errorf("failed to connect to server %s: %w", name, err)
			}
		}(name, serverCfg, workspacePath)
	}

	wg.Wait()
	close(errs)

	// Collect errors
	var allErrors []error
	for err := range errs {
		allErrors = append(allErrors, err)
	}

	connectedCount := len(m.GetServers())

	// If all enabled servers failed to connect, return aggregated error
	if enabledCount > 0 && connectedCount == 0 {
		logger.ErrorCF("mcp", "All MCP servers failed to connect",
			map[string]any{
				"failed": len(allErrors),
				"total":  enabledCount,
			})
		return errors.Join(allErrors...)
	}

	if len(allErrors) > 0 {
		logger.WarnCF("mcp", "Some MCP servers failed to connect",
			map[string]any{
				"failed":    len(allErrors),
				"connected": connectedCount,
				"total":     enabledCount,
			})
		// Don't fail completely if some servers successfully connected
	}

	logger.InfoCF("mcp", "MCP server initialization complete",
		map[string]any{
			"connected": connectedCount,
			"total":     enabledCount,
		})

	return nil
}

// ConnectServer connects to a single MCP server
func (m *Manager) ConnectServer(
	ctx context.Context,
	name string,
	cfg config.MCPServerConfig,
) error {
	if err := config.ValidateMCPSessionLossReplay(cfg); err != nil {
		m.publishServerEvent(runtimeevents.KindMCPServerFailed, name, cfg, 0, err)
		return err
	}
	if err := config.ValidateMCPExclusiveLockFile(cfg); err != nil {
		m.publishServerEvent(runtimeevents.KindMCPServerFailed, name, cfg, 0, err)
		return err
	}
	if err := m.beginLifecycleOperation(); err != nil {
		m.publishServerEvent(runtimeevents.KindMCPServerFailed, name, cfg, 0, err)
		return err
	}
	defer m.wg.Done()
	cfg.SessionLossReplay = config.EffectiveMCPSessionLossReplay(cfg)

	m.publishServerEvent(runtimeevents.KindMCPServerConnecting, name, cfg, 0, nil)
	var lease *exclusiveServerLease
	if cfg.ExclusiveLockFile != "" {
		var err error
		lease, err = acquireExclusiveServerLease(name, cfg.ExclusiveLockFile)
		if err != nil {
			m.publishServerEvent(runtimeevents.KindMCPServerFailed, name, cfg, 0, err)
			return err
		}
	}
	conn, err := connectServerFunc(ctx, name, cfg)
	if conn != nil {
		if conn.cleanup != nil || lease != nil {
			conn.attachExclusiveLease(lease)
		} else {
			lease.release()
		}
	} else {
		lease.release()
	}
	if err != nil {
		cleanupErr := m.rejectConnection(conn)
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("connection cleanup failed: %w", cleanupErr))
		}
		m.publishServerEvent(runtimeevents.KindMCPServerFailed, name, cfg, 0, err)
		return err
	}

	m.mu.Lock()
	if m.closed.Load() {
		m.mu.Unlock()
		closedErr := fmt.Errorf("manager is closed")
		cleanupErr := m.rejectConnection(conn)
		if cleanupErr != nil {
			closedErr = errors.Join(closedErr, fmt.Errorf("connection cleanup failed: %w", cleanupErr))
		}
		m.publishServerEvent(runtimeevents.KindMCPServerFailed, name, cfg, 0, closedErr)
		return closedErr
	}

	m.servers[name] = conn
	m.mu.Unlock()
	for _, tool := range conn.Tools {
		toolName := ""
		if tool != nil {
			toolName = tool.Name
		}
		m.publishToolDiscovered(name, cfg, toolName)
	}
	m.publishServerEvent(runtimeevents.KindMCPServerConnected, name, cfg, len(conn.Tools), nil)
	return nil
}

func (m *Manager) beginLifecycleOperation() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}
	m.wg.Add(1)
	return nil
}

func (m *Manager) rejectConnection(conn *ServerConnection) error {
	if conn == nil {
		return nil
	}
	err := conn.close()
	if err == nil || conn.cleanup == nil {
		conn.releaseExclusiveLease()
		return err
	}
	m.mu.Lock()
	m.pendingCleanup = append(m.pendingCleanup, conn)
	m.mu.Unlock()
	return err
}

func connectServer(
	ctx context.Context,
	name string,
	cfg config.MCPServerConfig,
) (*ServerConnection, error) {
	logger.InfoCF("mcp", "Connecting to MCP server",
		map[string]any{
			"server":     name,
			"command":    cfg.Command,
			"args_count": len(cfg.Args),
		})

	// Create client
	client := mcp.NewClient(&mcp.Implementation{
		Name:    "mintclaw",
		Version: "1.0.0",
	}, nil)

	// Create transport based on configuration
	// Auto-detect transport type if not explicitly specified
	var transport mcp.Transport
	var commandTransport *isolatedCommandTransport
	transportType := config.EffectiveMCPTransportType(cfg)
	if transportType == "" {
		return nil, fmt.Errorf("either URL or command must be provided")
	}

	switch transportType {
	case "sse", "http":
		if cfg.URL == "" {
			return nil, fmt.Errorf("URL is required for SSE/HTTP transport")
		}

		// Configure DisableStandaloneSSE based on transport type.
		// - "http": Streamable HTTP request-response mode. Disable the standalone
		//   SSE stream to avoid compatibility issues with servers that don't
		//   support the optional GET listener.
		// - "sse": Bidirectional mode. Enable the standalone SSE stream to receive
		//   server-initiated notifications (e.g., ToolListChangedNotification).
		// - Empty or auto-detected: Defaults to "sse" behavior (standalone SSE enabled).
		disableStandaloneSSE := transportType == "http"

		logger.DebugCF("mcp", "Using SSE/HTTP transport",
			map[string]any{
				"server":               name,
				"url":                  cfg.URL,
				"disableStandaloneSSE": disableStandaloneSSE,
			})

		sseTransport := &mcp.StreamableClientTransport{
			Endpoint:             cfg.URL,
			DisableStandaloneSSE: disableStandaloneSSE,
		}

		// Add custom headers if provided
		if len(cfg.Headers) > 0 {
			// Create a custom HTTP client with header-injecting transport
			sseTransport.HTTPClient = &http.Client{
				Transport: &headerTransport{
					base:    http.DefaultTransport,
					headers: cfg.Headers,
				},
			}
			logger.DebugCF("mcp", "Added custom HTTP headers",
				map[string]any{
					"server":       name,
					"header_count": len(cfg.Headers),
				})
		}

		transport = sseTransport
	case "stdio":
		if cfg.Command == "" {
			return nil, fmt.Errorf("command is required for stdio transport")
		}
		logger.DebugCF("mcp", "Using stdio transport",
			map[string]any{
				"server":  name,
				"command": cfg.Command,
			})
		// Create command with context. Internal callers may impose an operating-
		// system write ceiling on the complete stdio process tree.
		cmd, commandErr := stdioCommand(
			ctx,
			expandHomeCommandPath(cfg.Command),
			cfg.Args,
			cfg.StdioFileSizeLimitBytes,
		)
		if commandErr != nil {
			return nil, fmt.Errorf("invalid MCP stdio process limit: %w", commandErr)
		}

		var envVars map[string]string
		if cfg.EnvFile != "" {
			var loadErr error
			envVars, loadErr = loadEnvFile(cfg.EnvFile)
			if loadErr != nil {
				return nil, fmt.Errorf("failed to load env file %s: %w", cfg.EnvFile, loadErr)
			}
			logger.DebugCF("mcp", "Loaded environment variables from file",
				map[string]any{
					"server":    name,
					"envFile":   cfg.EnvFile,
					"var_count": len(envVars),
				})
		}

		environment, environmentErr := mergeCommandEnvironment(
			cmd.Environ(), envVars, cfg.Env, runtime.GOOS == "windows",
		)
		if environmentErr != nil {
			return nil, fmt.Errorf("invalid MCP server environment: %w", environmentErr)
		}
		cmd.Env = environment
		commandTransport = &isolatedCommandTransport{ServerName: name, Command: cmd}
		transport = commandTransport
	default:
		return nil, fmt.Errorf(
			"unsupported transport type: %s (supported: stdio, sse, http, streamable-http)",
			transportType,
		)
	}

	// Connect to server
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		if commandTransport != nil && commandTransport.cleanup != nil {
			return &ServerConnection{
				Name: name, Config: cfg, Client: client,
				cleanup: commandTransport.cleanup, cleanupFailed: true,
			}, fmt.Errorf("failed to connect: %w", err)
		}
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	conn := &ServerConnection{
		Name: name, Config: cfg, Client: client, Session: session,
	}
	if commandTransport != nil {
		conn.cleanup = commandTransport.cleanup
	}

	// Get server info
	initResult := session.InitializeResult()
	logger.InfoCF("mcp", "Connected to MCP server",
		map[string]any{
			"server":        name,
			"serverName":    initResult.ServerInfo.Name,
			"serverVersion": initResult.ServerInfo.Version,
			"protocol":      initResult.ProtocolVersion,
		})

	// List available tools if supported
	tools, err := listServerTools(ctx, name, session, initResult)
	if err != nil {
		return conn, err
	}
	conn.Tools = tools
	return conn, nil
}

// GetServers returns all connected servers
func (m *Manager) GetServers() map[string]*ServerConnection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*ServerConnection, len(m.servers))
	for k, v := range m.servers {
		result[k] = v
	}
	return result
}

// GetServer returns a specific server connection
func (m *Manager) GetServer(name string) (*ServerConnection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conn, ok := m.servers[name]
	return conn, ok
}

// CallTool calls a tool on a specific server
func (m *Manager) CallTool(
	ctx context.Context,
	serverName, toolName string,
	arguments map[string]any,
) (*mcp.CallToolResult, error) {
	// Check if closed before acquiring lock (fast path)
	if m.closed.Load() {
		return nil, fmt.Errorf("manager is closed")
	}

	m.mu.RLock()
	// Double-check after acquiring lock to prevent TOCTOU race
	if m.closed.Load() {
		m.mu.RUnlock()
		return nil, fmt.Errorf("manager is closed")
	}
	conn, ok := m.servers[serverName]
	if ok {
		m.wg.Add(1) // Add to WaitGroup while holding the lock
	}
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("server %s not found", serverName)
	}
	defer m.wg.Done()
	if conn.recoveryRequired.Load() {
		reconnectedConn, err := m.reconnectServer(ctx, serverName, conn)
		if err != nil {
			return nil, fmt.Errorf("failed to recover MCP server before tool call: %w", err)
		}
		conn = reconnectedConn
	}

	params := &mcp.CallToolParams{
		Name:      toolName,
		Arguments: arguments,
	}

	result, err := conn.Session.CallTool(ctx, params)
	if err != nil {
		if shouldReconnectCallError(err) {
			logger.WarnCF("mcp", "MCP server session was lost during tool call, recovering session",
				map[string]any{
					"server": serverName,
					"tool":   toolName,
					"reason": "session_lost",
				})

			reconnectedConn, reconnectErr := m.reconnectServer(ctx, serverName, conn)
			if config.EffectiveMCPSessionLossReplay(conn.Config) == config.MCPSessionLossReplayNever {
				if reconnectErr != nil {
					logger.WarnCF("mcp", "MCP server reconnect failed after uncertain tool call",
						map[string]any{
							"server": serverName,
							"tool":   toolName,
							"reason": "reconnect_failed",
						})
				}
				return nil, &CallOutcomeUncertainError{
					Server:      serverName,
					Tool:        toolName,
					Reconnected: reconnectErr == nil,
				}
			}
			if reconnectErr != nil {
				return nil, fmt.Errorf("failed to recover lost MCP session: %w", reconnectErr)
			}

			result, err = reconnectedConn.Session.CallTool(ctx, params)
			if err == nil {
				return result, nil
			}
		}

		return nil, fmt.Errorf("failed to call tool: %w", err)
	}

	return result, nil
}

func listServerTools(
	ctx context.Context,
	name string,
	session *mcp.ClientSession,
	initResult *mcp.InitializeResult,
) ([]*mcp.Tool, error) {
	var tools []*mcp.Tool
	if initResult.Capabilities.Tools == nil {
		return tools, nil
	}

	for tool, err := range session.Tools(ctx, nil) {
		if err != nil {
			logger.WarnCF("mcp", "Error listing tool",
				map[string]any{
					"server": name,
					"error":  err.Error(),
				})
			continue
		}
		tools = append(tools, tool)
	}

	logger.InfoCF("mcp", "Listed tools from MCP server",
		map[string]any{
			"server":    name,
			"toolCount": len(tools),
		})

	return tools, nil
}

func shouldReconnectCallError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, mcp.ErrSessionMissing) {
		return true
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, mcp.ErrSessionMissing.Error()) {
		return true
	}

	// Stdio MCP servers can disappear after a config reload or an external
	// process failure. The SDK wraps the underlying pipe error in a
	// "client is closing" error rather than ErrSessionMissing, so recognize
	// the concrete closed-transport forms too and reconnect once.
	return strings.Contains(message, "client is closing") ||
		strings.Contains(message, "connection closed") ||
		strings.Contains(message, "file already closed") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "unexpected eof")
}

func (m *Manager) reconnectServer(
	ctx context.Context,
	serverName string,
	staleConn *ServerConnection,
) (*ServerConnection, error) {
	if staleConn == nil {
		return nil, fmt.Errorf("server %s not found", serverName)
	}

	staleConn.reconnectMu.Lock()
	defer staleConn.reconnectMu.Unlock()

	if m.closed.Load() {
		return nil, fmt.Errorf("manager is closed")
	}

	m.mu.RLock()
	currentConn, ok := m.servers[serverName]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("server %s not found", serverName)
	}
	if currentConn != staleConn {
		return currentConn, nil
	}
	staleConn.recoveryRequired.Store(true)

	// A replacement must not start while the stale process tree may still own
	// the exclusive resource. Keep the stale connection's lease-group
	// membership as a sentinel across replacement startup, and release that
	// sentinel only after the fresh connection is admitted.
	if err := staleConn.close(); err != nil {
		return nil, fmt.Errorf("close stale server before reconnect: %w", err)
	}
	if staleConn.leaseGroup.hasOtherMembers(staleConn) {
		return nil, fmt.Errorf("prior reconnect cleanup is still pending")
	}

	freshConn, err := connectServerFunc(ctx, serverName, staleConn.Config)
	if freshConn != nil {
		freshConn.attachExclusiveLeaseGroup(staleConn.leaseGroup)
	}
	if err != nil {
		if cleanupErr := m.rejectConnection(freshConn); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("fresh connection cleanup failed: %w", cleanupErr))
		}
		return nil, err
	}

	m.mu.Lock()
	if m.closed.Load() {
		m.mu.Unlock()
		err = m.rejectConnection(freshConn)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("manager is closed"),
				fmt.Errorf("fresh connection cleanup failed: %w", err),
			)
		}
		return nil, fmt.Errorf("manager is closed")
	}

	currentConn, ok = m.servers[serverName]
	if !ok {
		m.mu.Unlock()
		err = m.rejectConnection(freshConn)
		staleConn.releaseExclusiveLease()
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("server %s not found", serverName),
				fmt.Errorf("fresh connection cleanup failed: %w", err),
			)
		}
		return nil, fmt.Errorf("server %s not found", serverName)
	}

	if currentConn == staleConn {
		m.servers[serverName] = freshConn
		m.mu.Unlock()
		staleConn.releaseExclusiveLease()
		return freshConn, nil
	}

	m.mu.Unlock()
	if cleanupErr := m.rejectConnection(freshConn); cleanupErr != nil {
		staleConn.releaseExclusiveLease()
		return nil, fmt.Errorf("superseded fresh connection cleanup failed: %w", cleanupErr)
	}
	staleConn.releaseExclusiveLease()
	return currentConn, nil
}

// Close closes all server connections
func (m *Manager) Close() error {
	// Closing prevents new calls immediately, but failed server cleanup remains
	// retryable and retains its exclusive lease. A second Close is therefore a
	// real cleanup retry rather than an unconditional no-op.
	m.mu.Lock()
	m.closed.Store(true)
	m.mu.Unlock()

	// Wait for all in-flight CallTool calls to finish before closing sessions
	// After closed=true is set, no new CallTool can start (they check closed first)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	logger.InfoCF("mcp", "Closing all MCP server connections",
		map[string]any{
			"count": len(m.servers) + len(m.pendingCleanup),
		})

	var errs []error
	remaining := make(map[string]*ServerConnection)
	for name, conn := range m.servers {
		if err := conn.close(); err != nil {
			logger.ErrorCF("mcp", "Failed to close server connection",
				map[string]any{
					"server": name,
					"error":  err.Error(),
				})
			errs = append(errs, fmt.Errorf("server %s: %w", name, err))
			remaining[name] = conn
			continue
		}
		conn.releaseExclusiveLease()
	}

	m.servers = remaining
	remainingPending := make([]*ServerConnection, 0, len(m.pendingCleanup))
	for _, conn := range m.pendingCleanup {
		if err := conn.close(); err != nil {
			name := conn.Name
			logger.ErrorCF("mcp", "Failed to close rejected server connection",
				map[string]any{"server": name, "error": err.Error()})
			errs = append(errs, fmt.Errorf("rejected server %s: %w", name, err))
			remainingPending = append(remainingPending, conn)
			continue
		}
		conn.releaseExclusiveLease()
	}
	m.pendingCleanup = remainingPending

	if len(errs) > 0 {
		return fmt.Errorf("failed to close %d server(s): %w", len(errs), errors.Join(errs...))
	}

	return nil
}

func (c *ServerConnection) close() error {
	if c.cleanupFailed && c.cleanup != nil {
		err := c.cleanup.Close()
		c.cleanupFailed = err != nil
		return err
	}
	if c.Session == nil {
		if c.cleanup == nil {
			return nil
		}
		err := c.cleanup.Close()
		c.cleanupFailed = err != nil
		return err
	}
	err := c.Session.Close()
	if err != nil && c.cleanup != nil {
		c.cleanupFailed = true
	}
	return err
}

func (c *ServerConnection) releaseExclusiveLease() {
	if c == nil || c.leaseGroup == nil {
		return
	}
	group := c.leaseGroup
	c.leaseGroup = nil
	group.remove(c)
}

func (c *ServerConnection) attachExclusiveLease(lease *exclusiveServerLease) {
	if c == nil {
		return
	}
	c.attachExclusiveLeaseGroup(&exclusiveLeaseGroup{
		lease: lease, members: make(map[*ServerConnection]struct{}),
	})
}

func (c *ServerConnection) attachExclusiveLeaseGroup(group *exclusiveLeaseGroup) {
	if c == nil || group == nil || c.leaseGroup == group {
		return
	}
	group.mu.Lock()
	group.members[c] = struct{}{}
	group.mu.Unlock()
	c.leaseGroup = group
}

func (g *exclusiveLeaseGroup) remove(conn *ServerConnection) {
	if g == nil {
		return
	}
	g.mu.Lock()
	delete(g.members, conn)
	empty := len(g.members) == 0
	lease := g.lease
	if empty {
		g.lease = nil
	}
	g.mu.Unlock()
	if empty {
		lease.release()
	}
}

func (g *exclusiveLeaseGroup) hasOtherMembers(conn *ServerConnection) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	count := len(g.members)
	if _, ok := g.members[conn]; ok {
		count--
	}
	return count > 0
}

// GetAllTools returns all tools from all connected servers
func (m *Manager) GetAllTools() map[string][]*mcp.Tool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]*mcp.Tool)
	for name, conn := range m.servers {
		if len(conn.Tools) > 0 {
			result[name] = conn.Tools
		}
	}
	return result
}
