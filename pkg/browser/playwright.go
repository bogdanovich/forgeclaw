package browser

import (
	"bytes"
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

var (
	playwrightTargetPattern     = regexp.MustCompile(`^e[1-9][0-9]{0,9}$`)
	playwrightSnapshotRefToken  = regexp.MustCompile(`\[ref=`)
	playwrightSnapshotTargetRef = regexp.MustCompile(`\[ref=(e[1-9][0-9]{0,9})\]`)
	playwrightElementPattern    = regexp.MustCompile(
		`(?m)^\s*-\s+([A-Za-z][A-Za-z0-9_-]*)(?:\s+"([^"]*)")?[^\n]*\[ref=(e[1-9][0-9]{0,9})\]`,
	)
)

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
	Elements []DriverElement
}

type DriverElement struct {
	Target string
	Role   string
	Name   string
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
) (WorkerOpenResult, error) {
	if factory == nil || factory.clientFactory == nil || request.Target != factory.target ||
		request.Profile != factory.profile || !request.DryRun || !validIdentifier(request.SessionID) {
		return WorkerOpenResult{}, ErrDenied
	}
	client := factory.clientFactory()
	if client == nil {
		return WorkerOpenResult{}, ErrWorkerUnavailable
	}
	lifetimeCtx, cancelLifetime := context.WithCancel(context.WithoutCancel(ctx))
	worker := &playwrightWorker{
		client: client, limits: request.Limits.Effective(), cancelLifetime: cancelLifetime,
	}
	stopStartupCancellation := context.AfterFunc(ctx, cancelLifetime)
	catalog, err := client.Connect(
		lifetimeCtx,
		playwrightPrivateServerName,
		cloneMCPServerConfig(factory.serverConfig),
	)
	startupActive := stopStartupCancellation()
	if err != nil {
		return failedPlaywrightOpen(worker, ErrWorkerUnavailable)
	}
	if !startupActive {
		return failedPlaywrightOpen(worker, ErrWorkerUnavailable)
	}
	catalogRevision, err := validatePlaywrightCatalog(catalog)
	if err != nil {
		return failedPlaywrightOpen(worker, ErrDriverIncompatible)
	}
	worker.catalogRevision = catalogRevision
	return WorkerOpenResult{Owner: worker}, nil
}

func failedPlaywrightOpen(worker *playwrightWorker, err error) (WorkerOpenResult, error) {
	if worker == nil {
		return WorkerOpenResult{}, err
	}
	worker.closing = true
	if worker.cancelLifetime != nil {
		worker.cancelLifetime()
	}
	return WorkerOpenResult{Owner: worker}, err
}

type playwrightWorker struct {
	client          playwrightMCPClient
	limits          config.BrowserLimitsConfig
	catalogRevision string
	cancelLifetime  context.CancelFunc

	mu      sync.Mutex
	lost    bool
	closing bool
	closed  bool
}

func (worker *playwrightWorker) Status(ctx context.Context) (WorkerStatus, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closing || worker.closed || worker.lost {
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
	if worker.closing || worker.closed || worker.lost {
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
	return parsePlaywrightObservation(
		text,
		worker.limits.SnapshotBytes,
		worker.limits.SnapshotRefs,
	)
}

func (worker *playwrightWorker) Resolve(ctx context.Context, target string) (DriverElement, string, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closing || worker.closed || worker.lost || !playwrightTargetPattern.MatchString(target) {
		return DriverElement{}, "", ErrWorkerUnavailable
	}
	result, err := worker.call(ctx, "browser_snapshot", map[string]any{"boxes": false, "target": target})
	if err != nil {
		return DriverElement{}, "", err
	}
	text, err := boundedPlaywrightText(result, worker.limits.ToolResultBytes)
	if err != nil {
		return DriverElement{}, "", err
	}
	observation, err := parsePlaywrightObservation(text, worker.limits.SnapshotBytes, worker.limits.SnapshotRefs)
	if err != nil {
		return DriverElement{}, "", err
	}
	for _, element := range observation.Elements {
		if element.Target == target {
			return element, observation.Origin, nil
		}
	}
	return DriverElement{}, "", ErrStale
}

func (worker *playwrightWorker) CatalogRevision() string {
	return worker.catalogRevision
}

func (worker *playwrightWorker) Execute(ctx context.Context, action DriverAction) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closing || worker.closed || worker.lost {
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
		return nil
	}
	if !worker.closing {
		worker.closing = true
		// browser_close is best effort and must not be replayed. Closing the
		// private manager is the retryable process and exclusive-lease boundary.
		if !worker.lost {
			_, _ = worker.client.CallTool(ctx, "browser_close", map[string]any{})
		}
		if worker.cancelLifetime != nil {
			worker.cancelLifetime()
		}
	}
	if err := worker.client.Close(); err != nil {
		return ErrWorkerUnavailable
	}
	worker.closed = true
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

func parsePlaywrightObservation(
	text string,
	maximumSnapshotBytes int,
	maximumSnapshotRefs int,
) (DriverObservation, error) {
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
	referenceTokens := playwrightSnapshotRefToken.FindAllStringIndex(snapshot, maximumSnapshotRefs+1)
	targetReferences := playwrightSnapshotTargetRef.FindAllStringSubmatch(snapshot, maximumSnapshotRefs+1)
	if snapshot == "" || len(snapshot) > maximumSnapshotBytes || len(title) > 1024 ||
		maximumSnapshotRefs <= 0 ||
		len(referenceTokens) > maximumSnapshotRefs || len(referenceTokens) != len(targetReferences) {
		return DriverObservation{}, ErrDriverIncompatible
	}
	safeURL, origin, err := sanitizeObservedURL(pageURL)
	if err != nil {
		return DriverObservation{}, ErrDriverIncompatible
	}
	elements := parsePlaywrightElements(snapshot)
	return DriverObservation{
		URL: safeURL, Origin: origin, Title: title, Snapshot: snapshot, Elements: elements,
	}, nil
}

func parsePlaywrightElements(snapshot string) []DriverElement {
	semantics := make(map[string]DriverElement)
	for _, match := range playwrightElementPattern.FindAllStringSubmatch(snapshot, -1) {
		semantics[match[3]] = DriverElement{
			Target: match[3], Role: strings.ToLower(match[1]), Name: match[2],
		}
	}
	seen := make(map[string]struct{})
	refs := playwrightSnapshotTargetRef.FindAllStringSubmatch(snapshot, -1)
	elements := make([]DriverElement, 0, len(refs))
	for _, match := range refs {
		target := match[1]
		if _, exists := seen[target]; exists {
			continue
		}
		seen[target] = struct{}{}
		element, ok := semantics[target]
		if !ok {
			element = DriverElement{Target: target, Role: "unknown"}
		}
		elements = append(elements, element)
	}
	return elements
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

var pinnedPlaywrightToolSchemas = map[string]json.RawMessage{
	"browser_close": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{},
		"type":"object"
	}`),
	"browser_navigate": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{"url":{"description":"The URL to navigate to","type":"string"}},
		"required":["url"],
		"type":"object"
	}`),
	"browser_snapshot": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{
			"boxes":{"description":"Include each element's bounding box as [box=x,y,width,height] in the snapshot. Coordinates are viewport-relative, in CSS pixels (Element.getBoundingClientRect)","type":"boolean"},
			"depth":{"description":"Limit the depth of the snapshot tree","type":"number"},
			"filename":{"description":"Save snapshot to markdown file instead of returning it in the response.","type":"string"},
			"target":{"description":"Exact target element reference from the page snapshot, or a unique element selector","type":"string"}
		},
		"type":"object"
	}`),
	"browser_click": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{
			"button":{"description":"Button to click, defaults to left","enum":["left","right","middle"],"type":"string"},
			"doubleClick":{"description":"Whether to perform a double click instead of a single click","type":"boolean"},
			"element":{"description":"Human-readable element description used to obtain permission to interact with the element","type":"string"},
			"modifiers":{"description":"Modifier keys to press","items":{"enum":["Alt","Control","ControlOrMeta","Meta","Shift"],"type":"string"},"type":"array"},
			"target":{"description":"Exact target element reference from the page snapshot, or a unique element selector","type":"string"}
		},
		"required":["target"],
		"type":"object"
	}`),
	"browser_type": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{
			"element":{"description":"Human-readable element description used to obtain permission to interact with the element","type":"string"},
			"slowly":{"description":"Whether to type one character at a time. Useful for triggering key handlers in the page. By default entire text is filled in at once.","type":"boolean"},
			"submit":{"description":"Whether to submit entered text (press Enter after)","type":"boolean"},
			"target":{"description":"Exact target element reference from the page snapshot, or a unique element selector","type":"string"},
			"text":{"description":"Text to type into the element","type":"string"}
		},
		"required":["target","text"],
		"type":"object"
	}`),
}

func validatePlaywrightCatalog(tools []*sdkmcp.Tool) (string, error) {
	available := make(map[string]*sdkmcp.Tool, len(tools))
	for _, tool := range tools {
		if tool != nil {
			available[tool.Name] = tool
		}
	}
	names := make([]string, 0, len(pinnedPlaywrightToolSchemas))
	for name, expected := range pinnedPlaywrightToolSchemas {
		tool := available[name]
		actualSchema, actualErr := canonicalPlaywrightSchema(toolSchema(tool))
		expectedSchema, expectedErr := canonicalPlaywrightSchema(expected)
		if tool == nil || actualErr != nil || expectedErr != nil || !bytes.Equal(actualSchema, expectedSchema) {
			return "", ErrDriverIncompatible
		}
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		encoded, err := canonicalPlaywrightSchema(available[name].InputSchema)
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

func toolSchema(tool *sdkmcp.Tool) any {
	if tool == nil {
		return nil
	}
	return tool.InputSchema
}

func canonicalPlaywrightSchema(schema any) ([]byte, error) {
	encoded, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err = json.Unmarshal(encoded, &normalized); err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
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
