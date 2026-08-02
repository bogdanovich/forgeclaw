package browser

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/bogdanovich/mintclaw/pkg/config"
	localmcp "github.com/bogdanovich/mintclaw/pkg/mcp"
)

const playwrightPrivateServerName = "browser_driver"

var playwrightTargetPattern = regexp.MustCompile(`^e[1-9][0-9]{0,9}$`)

type DriverActionKind string

const (
	DriverNavigate DriverActionKind = "navigate"
	DriverClick    DriverActionKind = "click"
	DriverFill     DriverActionKind = "fill"
)

type DriverAction struct {
	Kind    DriverActionKind
	URL     string
	Target  string
	Element string
	Value   string
}

type DriverObservation struct {
	URL      string
	Origin   string
	Title    string
	Snapshot string
}

type playwrightMCPClient interface {
	Connect(context.Context, string, config.MCPServerConfig) ([]*sdkmcp.Tool, error)
	Ping(context.Context) error
	CallTool(context.Context, string, map[string]any) (*sdkmcp.CallToolResult, error)
	Close() error
}

type managerPlaywrightClient struct {
	manager    *localmcp.Manager
	connection *localmcp.ServerConnection
}

func newManagerPlaywrightClient() playwrightMCPClient {
	return &managerPlaywrightClient{manager: localmcp.NewManager()}
}

func (client *managerPlaywrightClient) Connect(
	ctx context.Context,
	server string,
	cfg config.MCPServerConfig,
) ([]*sdkmcp.Tool, error) {
	if err := client.manager.ConnectServer(ctx, server, cfg); err != nil {
		return nil, err
	}
	connection, ok := client.manager.GetServer(server)
	if !ok || connection == nil {
		return nil, errors.New("connected browser driver is unavailable")
	}
	client.connection = connection
	return append([]*sdkmcp.Tool(nil), connection.Tools...), nil
}

func (client *managerPlaywrightClient) Ping(ctx context.Context) error {
	if client.connection == nil || client.connection.Session == nil {
		return errors.New("browser driver session is unavailable")
	}
	return client.connection.Session.Ping(ctx, nil)
}

func (client *managerPlaywrightClient) CallTool(
	ctx context.Context,
	tool string,
	arguments map[string]any,
) (*sdkmcp.CallToolResult, error) {
	if client.connection == nil || client.connection.Session == nil {
		return nil, errors.New("browser driver session is unavailable")
	}
	// Deliberately bypass Manager.CallTool: the generic manager reconnects a
	// lost session for future calls even when replay is disabled. A browser
	// worker must instead become lost without starting a replacement driver.
	return client.connection.Session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: tool, Arguments: arguments,
	})
}

func (client *managerPlaywrightClient) Close() error {
	return client.manager.Close()
}

type PlaywrightWorkerFactory struct {
	target        string
	profile       string
	serverConfig  config.MCPServerConfig
	clientFactory func() playwrightMCPClient
}

func NewPlaywrightWorkerFactory(rootConfig *config.Config) (*PlaywrightWorkerFactory, error) {
	if rootConfig == nil {
		return nil, errors.New("Playwright worker factory requires a root config")
	}
	if err := rootConfig.ValidateBrowserConfig(); err != nil {
		return nil, err
	}
	if !rootConfig.Tools.Browser.Enabled {
		return nil, ErrDenied
	}
	target, ok := rootConfig.Tools.Browser.Targets[config.BrowserDefaultTarget]
	if !ok || !target.Enabled || target.Driver != config.BrowserDriverPlaywrightMCP {
		return nil, ErrDenied
	}
	profile, ok := target.Profiles[config.BrowserDefaultProfile]
	if !ok || !profile.Enabled || !profile.DryRun {
		return nil, ErrDenied
	}
	server, ok := rootConfig.Tools.MCP.Servers[target.DriverServer]
	if !ok {
		return nil, ErrDenied
	}
	return &PlaywrightWorkerFactory{
		target: config.BrowserDefaultTarget, profile: config.BrowserDefaultProfile,
		serverConfig: cloneMCPServerConfig(server), clientFactory: newManagerPlaywrightClient,
	}, nil
}

func (factory *PlaywrightWorkerFactory) Open(
	ctx context.Context,
	request WorkerOpenRequest,
) (Worker, error) {
	if factory == nil || factory.clientFactory == nil || request.Target != factory.target ||
		request.Profile != factory.profile || !request.DryRun || !validIdentifier(request.SessionID) {
		return nil, ErrDenied
	}
	client := factory.clientFactory()
	if client == nil {
		return nil, ErrWorkerUnavailable
	}
	lifetimeCtx, cancelLifetime := context.WithCancel(context.WithoutCancel(ctx))
	stopStartupCancellation := context.AfterFunc(ctx, cancelLifetime)
	catalog, err := client.Connect(
		lifetimeCtx,
		playwrightPrivateServerName,
		cloneMCPServerConfig(factory.serverConfig),
	)
	startupActive := stopStartupCancellation()
	if err != nil {
		cancelLifetime()
		_ = client.Close()
		return nil, ErrWorkerUnavailable
	}
	if !startupActive {
		cancelLifetime()
		_ = client.Close()
		return nil, ErrWorkerUnavailable
	}
	catalogRevision, err := validatePlaywrightCatalog(catalog)
	if err != nil {
		cancelLifetime()
		_ = client.Close()
		return nil, ErrDriverIncompatible
	}
	return &playwrightWorker{
		client: client, limits: request.Limits.Effective(), catalogRevision: catalogRevision,
		cancelLifetime: cancelLifetime,
	}, nil
}

type playwrightWorker struct {
	client          playwrightMCPClient
	limits          config.BrowserLimitsConfig
	catalogRevision string
	cancelLifetime  context.CancelFunc

	mu          sync.Mutex
	lost        bool
	closed      bool
	closeFailed bool
}

func (worker *playwrightWorker) Status(ctx context.Context) (WorkerStatus, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed || worker.lost {
		return WorkerLost, nil
	}
	if err := worker.client.Ping(ctx); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		worker.lost = true
		return WorkerLost, nil
	}
	return WorkerReady, nil
}

func (worker *playwrightWorker) Observe(ctx context.Context) (DriverObservation, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed || worker.lost {
		return DriverObservation{}, ErrWorkerUnavailable
	}
	result, err := worker.call(ctx, "browser_snapshot", map[string]any{"boxes": false})
	if err != nil {
		return DriverObservation{}, err
	}
	text, err := boundedPlaywrightText(result, worker.limits.ToolResultBytes)
	if err != nil {
		return DriverObservation{}, err
	}
	return parsePlaywrightObservation(text, worker.limits.SnapshotBytes)
}

func (worker *playwrightWorker) Execute(ctx context.Context, action DriverAction) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed || worker.lost {
		return ErrWorkerUnavailable
	}
	tool, arguments, err := mapPlaywrightAction(action, worker.limits)
	if err != nil {
		return err
	}
	result, err := worker.call(ctx, tool, arguments)
	if err != nil {
		return err
	}
	_, err = boundedPlaywrightText(result, worker.limits.ToolResultBytes)
	return err
}

func (worker *playwrightWorker) Close(ctx context.Context) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed {
		if worker.closeFailed {
			return ErrWorkerUnavailable
		}
		return nil
	}
	// browser_close is best effort. Closing the private manager is the actual
	// process and exclusive-lease boundary, and it must happen even if the page
	// or MCP session has already disappeared.
	if !worker.lost {
		_, _ = worker.client.CallTool(ctx, "browser_close", map[string]any{})
	}
	err := worker.client.Close()
	if worker.cancelLifetime != nil {
		worker.cancelLifetime()
	}
	worker.closed = true
	if err != nil {
		worker.closeFailed = true
		return ErrWorkerUnavailable
	}
	return nil
}

func (worker *playwrightWorker) call(
	ctx context.Context,
	tool string,
	arguments map[string]any,
) (*sdkmcp.CallToolResult, error) {
	result, err := worker.client.CallTool(ctx, tool, arguments)
	if err != nil {
		worker.lost = true
		return nil, ErrWorkerUnavailable
	}
	if result == nil {
		worker.lost = true
		return nil, ErrWorkerUnavailable
	}
	if result.IsError {
		return nil, ErrDriverRejected
	}
	return result, nil
}

func mapPlaywrightAction(
	action DriverAction,
	limits config.BrowserLimitsConfig,
) (string, map[string]any, error) {
	switch action.Kind {
	case DriverNavigate:
		normalized, err := normalizeDriverNavigationURL(action.URL)
		if err != nil || action.Target != "" || action.Element != "" || action.Value != "" {
			return "", nil, fmt.Errorf("%w: malformed navigate action", ErrInvalid)
		}
		return "browser_navigate", map[string]any{"url": normalized}, nil
	case DriverClick:
		if !playwrightTargetPattern.MatchString(action.Target) || action.URL != "" ||
			action.Value != "" || len(action.Element) > 512 {
			return "", nil, fmt.Errorf("%w: malformed click action", ErrInvalid)
		}
		arguments := map[string]any{
			"target": action.Target, "doubleClick": false, "button": "left",
		}
		if action.Element != "" {
			arguments["element"] = action.Element
		}
		return "browser_click", arguments, nil
	case DriverFill:
		if !playwrightTargetPattern.MatchString(action.Target) || action.URL != "" ||
			len(action.Element) > 512 || len(action.Value) > limits.TextInputBytes {
			return "", nil, fmt.Errorf("%w: malformed fill action", ErrInvalid)
		}
		arguments := map[string]any{
			"target": action.Target, "text": action.Value, "submit": false, "slowly": false,
		}
		if action.Element != "" {
			arguments["element"] = action.Element
		}
		return "browser_type", arguments, nil
	default:
		return "", nil, fmt.Errorf("%w: unsupported driver action", ErrInvalid)
	}
}

func normalizeDriverNavigationURL(raw string) (string, error) {
	if raw == "" || len(raw) > 2048 || strings.TrimSpace(raw) != raw {
		return "", ErrInvalid
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" {
		return "", ErrInvalid
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", ErrInvalid
	}
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String(), nil
}

func boundedPlaywrightText(result *sdkmcp.CallToolResult, maximum int) (string, error) {
	if result == nil || maximum <= 0 {
		return "", ErrDriverIncompatible
	}
	var builder strings.Builder
	for _, content := range result.Content {
		text, ok := content.(*sdkmcp.TextContent)
		if !ok || text == nil {
			return "", ErrDriverIncompatible
		}
		if builder.Len() != 0 {
			builder.WriteByte('\n')
		}
		if builder.Len()+len(text.Text) > maximum {
			return "", ErrDriverIncompatible
		}
		builder.WriteString(text.Text)
	}
	if builder.Len() == 0 {
		return "", ErrDriverIncompatible
	}
	return builder.String(), nil
}

func parsePlaywrightObservation(text string, maximumSnapshotBytes int) (DriverObservation, error) {
	pageURL := extractPlaywrightLine(text, "- Page URL: ")
	title := extractPlaywrightLine(text, "- Page Title: ")
	marker := "### Snapshot\n```yaml\n"
	start := strings.Index(text, marker)
	if pageURL == "" || start < 0 {
		return DriverObservation{}, ErrDriverIncompatible
	}
	start += len(marker)
	end := strings.Index(text[start:], "\n```")
	if end < 0 {
		return DriverObservation{}, ErrDriverIncompatible
	}
	snapshot := text[start : start+end]
	if snapshot == "" || len(snapshot) > maximumSnapshotBytes || len(title) > 1024 {
		return DriverObservation{}, ErrDriverIncompatible
	}
	safeURL, origin, err := sanitizeObservedURL(pageURL)
	if err != nil {
		return DriverObservation{}, ErrDriverIncompatible
	}
	return DriverObservation{URL: safeURL, Origin: origin, Title: title, Snapshot: snapshot}, nil
}

func extractPlaywrightLine(text, prefix string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

func sanitizeObservedURL(raw string) (string, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil {
		return "", "", ErrInvalid
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", "", ErrInvalid
	}
	hostname := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	if (parsed.Scheme == "http" && port == "80") || (parsed.Scheme == "https" && port == "443") {
		port = ""
	}
	host := hostname
	if port != "" {
		host = net.JoinHostPort(hostname, port)
	} else if strings.Contains(hostname, ":") {
		host = "[" + hostname + "]"
	}
	parsed.Host = host
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	origin := parsed.Scheme + "://" + host
	return parsed.String(), origin, nil
}

type expectedPlaywrightTool struct {
	required   []string
	properties map[string]string
}

var requiredPlaywrightTools = map[string]expectedPlaywrightTool{
	"browser_snapshot": {
		properties: map[string]string{"boxes": "boolean"},
	},
	"browser_navigate": {
		required: []string{"url"}, properties: map[string]string{"url": "string"},
	},
	"browser_click": {
		required: []string{"target"}, properties: map[string]string{
			"target": "string", "element": "string", "doubleClick": "boolean", "button": "string",
		},
	},
	"browser_type": {
		required: []string{"target", "text"}, properties: map[string]string{
			"target": "string", "element": "string", "text": "string",
			"submit": "boolean", "slowly": "boolean",
		},
	},
	"browser_close": {},
}

func validatePlaywrightCatalog(tools []*sdkmcp.Tool) (string, error) {
	available := make(map[string]*sdkmcp.Tool, len(tools))
	for _, tool := range tools {
		if tool != nil {
			available[tool.Name] = tool
		}
	}
	names := make([]string, 0, len(requiredPlaywrightTools))
	for name, expected := range requiredPlaywrightTools {
		tool := available[name]
		if tool == nil || validatePlaywrightToolSchema(tool.InputSchema, expected) != nil {
			return "", ErrDriverIncompatible
		}
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		encoded, err := json.Marshal(available[name].InputSchema)
		if err != nil {
			return "", ErrDriverIncompatible
		}
		_, _ = hash.Write([]byte(name))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(encoded)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validatePlaywrightToolSchema(schema any, expected expectedPlaywrightTool) error {
	encoded, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	var object map[string]any
	if err = json.Unmarshal(encoded, &object); err != nil || object["type"] != "object" ||
		object["additionalProperties"] != false {
		return ErrDriverIncompatible
	}
	properties, ok := object["properties"].(map[string]any)
	if !ok {
		return ErrDriverIncompatible
	}
	for name, expectedType := range expected.properties {
		property, ok := properties[name].(map[string]any)
		if !ok || property["type"] != expectedType {
			return ErrDriverIncompatible
		}
	}
	required := make(map[string]struct{})
	if rawRequired, exists := object["required"]; exists {
		values, ok := rawRequired.([]any)
		if !ok {
			return ErrDriverIncompatible
		}
		for _, value := range values {
			name, ok := value.(string)
			if !ok {
				return ErrDriverIncompatible
			}
			required[name] = struct{}{}
		}
	}
	if len(required) != len(expected.required) {
		return ErrDriverIncompatible
	}
	for _, name := range expected.required {
		if _, ok := required[name]; !ok {
			return ErrDriverIncompatible
		}
	}
	return nil
}

func cloneMCPServerConfig(source config.MCPServerConfig) config.MCPServerConfig {
	cloned := source
	cloned.Args = append([]string(nil), source.Args...)
	cloned.VisibleTools = append([]string(nil), source.VisibleTools...)
	cloned.Env = cloneStringMap(source.Env)
	cloned.Headers = cloneStringMap(source.Headers)
	return cloned
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}
