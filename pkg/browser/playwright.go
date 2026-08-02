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

const opaqueSnapshotReferenceBytes = len("ref_") + 32

const playwrightTargetExpression = `(?:f[1-9][0-9]{0,9})?e[1-9][0-9]{0,9}`

var (
	playwrightTargetPattern     = regexp.MustCompile(`^` + playwrightTargetExpression + `$`)
	playwrightSnapshotRefToken  = regexp.MustCompile(`\[ref=`)
	playwrightSnapshotTargetRef = regexp.MustCompile(`\[ref=(` + playwrightTargetExpression + `)\]`)
	playwrightElementPattern    = regexp.MustCompile(
		`(?m)^\s*-\s+([A-Za-z][A-Za-z0-9_-]*)(?:\s+"([^"]*)")?[^\n]*\[ref=(` +
			playwrightTargetExpression + `)\]`,
	)
	playwrightDialogPattern = regexp.MustCompile(
		`^- \["(alert|beforeunload|confirm|prompt)" dialog with message "(.*)"\]: can be handled by browser_handle_dialog$`,
	)
	playwrightSnapshotLinkPattern = regexp.MustCompile(`^- \[Snapshot\]\(.+\)$`)
)

type DriverActionKind string

const (
	DriverNavigate DriverActionKind = "navigate"
	DriverClick    DriverActionKind = "click"
	DriverFill     DriverActionKind = "fill"
	DriverSelect   DriverActionKind = "select"
	DriverPress    DriverActionKind = "press"
	DriverScroll   DriverActionKind = "scroll"
	DriverDialog   DriverActionKind = "dialog"
)

type DriverAction struct {
	Kind           DriverActionKind
	URL            string
	Target         string
	Element        string
	Value          string
	Key            string
	Direction      string
	Amount         int
	Accept         bool
	PromptProvided bool
}

type DriverObservation struct {
	URL           string
	Origin        string
	Title         string
	Snapshot      string
	Elements      []DriverElement
	PendingDialog *DialogObservation
	Truncated     bool
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
	server, err := playwrightServerWithOriginPolicy(server, profile.AllowedOrigins)
	if err != nil {
		return nil, err
	}
	return &PlaywrightWorkerFactory{
		target: config.BrowserDefaultTarget, profile: config.BrowserDefaultProfile,
		serverConfig: server, clientFactory: newManagerPlaywrightClient,
	}, nil
}

func playwrightServerWithOriginPolicy(
	server config.MCPServerConfig,
	rawOrigins []string,
) (config.MCPServerConfig, error) {
	server = cloneMCPServerConfig(server)
	for _, argument := range server.Args {
		if argument == "--allowed-origins" || strings.HasPrefix(argument, "--allowed-origins=") ||
			argument == "--blocked-origins" || strings.HasPrefix(argument, "--blocked-origins=") ||
			argument == "--config" || strings.HasPrefix(argument, "--config=") ||
			argument == "--caps" || strings.HasPrefix(argument, "--caps=") {
			return config.MCPServerConfig{}, fmt.Errorf(
				"browser driver policy and capabilities must be managed, not %q",
				argument,
			)
		}
	}
	for _, variable := range []string{
		"PLAYWRIGHT_MCP_ALLOWED_ORIGINS",
		"PLAYWRIGHT_MCP_BLOCKED_ORIGINS",
		"PLAYWRIGHT_MCP_CONFIG",
	} {
		if _, exists := server.Env[variable]; exists {
			return config.MCPServerConfig{}, fmt.Errorf(
				"browser driver policy and capabilities must be managed, not %s",
				variable,
			)
		}
	}
	origins := make([]string, 0, len(rawOrigins))
	for _, rawOrigin := range rawOrigins {
		origin, err := config.NormalizeBrowserOrigin(rawOrigin)
		if err != nil {
			return config.MCPServerConfig{}, fmt.Errorf("normalize browser driver origin: %w", err)
		}
		origins = append(origins, origin)
	}
	if len(origins) == 0 {
		return config.MCPServerConfig{}, errors.New("browser driver requires allowed origins")
	}
	sort.Strings(origins)
	allowedOrigins := strings.Join(origins, ";")
	if server.Env == nil {
		server.Env = make(map[string]string)
	}
	// MCP process environment precedence is parent < env file < explicit Env.
	// Set every policy input explicitly so neither inherited state nor an env
	// file can independently merge a blocklist or config-file policy.
	server.Env["PLAYWRIGHT_MCP_ALLOWED_ORIGINS"] = allowedOrigins
	server.Env["PLAYWRIGHT_MCP_BLOCKED_ORIGINS"] = ""
	server.Env["PLAYWRIGHT_MCP_CONFIG"] = ""
	server.Args = append(server.Args, "--caps", "vision", "--allowed-origins", allowedOrigins)
	return server, nil
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

	mu              sync.Mutex
	lost            bool
	closing         bool
	closed          bool
	lastObservation DriverObservation
	pendingDialog   *DialogObservation
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
	if worker.pendingDialog != nil {
		return worker.pendingDialogObservationLocked()
	}
	text, err := worker.callAndConsume(
		ctx, "browser_snapshot", map[string]any{"boxes": false}, true,
	)
	if err != nil {
		if errors.Is(err, ErrDriverRejected) && worker.pendingDialog != nil {
			return worker.pendingDialogObservationLocked()
		}
		return DriverObservation{}, err
	}
	observation, err := parsePlaywrightObservation(
		text,
		worker.limits.SnapshotBytes,
		worker.limits.SnapshotRefs,
		worker.limits.ToolResultBytes,
	)
	if err != nil {
		return DriverObservation{}, err
	}
	worker.lastObservation = observation
	return observation, nil
}

func (worker *playwrightWorker) Resolve(ctx context.Context, target string) (DriverElement, string, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closing || worker.closed || worker.lost || worker.pendingDialog != nil ||
		!playwrightTargetPattern.MatchString(target) {
		return DriverElement{}, "", ErrWorkerUnavailable
	}
	text, err := worker.callAndConsume(
		ctx, "browser_snapshot", map[string]any{"boxes": false, "target": target}, true,
	)
	if err != nil {
		return DriverElement{}, "", err
	}
	observation, err := parsePlaywrightObservation(
		text,
		worker.limits.SnapshotBytes,
		worker.limits.SnapshotRefs,
		worker.limits.ToolResultBytes,
	)
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
	if worker.pendingDialog != nil && action.Kind != DriverDialog {
		return ErrDriverRejected
	}
	if worker.pendingDialog == nil && action.Kind == DriverDialog {
		return ErrStale
	}
	tool, arguments, err := mapPlaywrightAction(action, worker.limits)
	if err != nil {
		return err
	}
	_, err = worker.callAndConsume(
		ctx, tool, arguments, playwrightActionIncludesSnapshot(action.Kind),
	)
	return err
}

func (worker *playwrightWorker) pendingDialogObservationLocked() (DriverObservation, error) {
	if worker.pendingDialog == nil || worker.lastObservation.Origin == "" {
		worker.lost = true
		return DriverObservation{}, ErrDriverIncompatible
	}
	observation := worker.lastObservation
	observation.Snapshot = ""
	observation.Elements = nil
	observation.PendingDialog = cloneDialogObservation(worker.pendingDialog)
	return observation, nil
}

func playwrightActionIncludesSnapshot(kind DriverActionKind) bool {
	switch kind {
	case DriverNavigate, DriverClick, DriverFill, DriverSelect:
		return true
	default:
		return false
	}
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

func (worker *playwrightWorker) callAndConsume(
	ctx context.Context,
	tool string,
	arguments map[string]any,
	allowSnapshotTail bool,
) (string, error) {
	result, err := worker.client.CallTool(ctx, tool, arguments)
	if err != nil || result == nil {
		worker.lost = true
		return "", ErrWorkerUnavailable
	}
	driverErr := error(nil)
	if result.IsError {
		driverErr = ErrDriverRejected
	}
	text, err := boundedPlaywrightText(result, worker.limits.ToolResultBytes)
	if err != nil {
		worker.lost = true
		return "", errors.Join(driverErr, err)
	}
	dialog, err := parsePlaywrightPendingDialog(text, allowSnapshotTail)
	if err != nil {
		worker.lost = true
		return "", errors.Join(driverErr, err)
	}
	if result.IsError && tool == "browser_handle_dialog" && worker.pendingDialog != nil && dialog == nil {
		// A rejected handler call without modal metadata does not establish
		// whether the known dialog closed. Do not guess and then issue another
		// potentially blocked MCP call; retire this worker instead.
		worker.lost = true
		return "", errors.Join(driverErr, ErrWorkerUnavailable)
	}
	if dialog != nil && worker.lastObservation.Origin == "" {
		worker.lost = true
		return "", errors.Join(driverErr, ErrDriverIncompatible)
	}
	worker.pendingDialog = dialog
	return text, driverErr
}

func mapPlaywrightAction(
	action DriverAction,
	limits config.BrowserLimitsConfig,
) (string, map[string]any, error) {
	switch action.Kind {
	case DriverNavigate:
		normalized, err := normalizeDriverNavigationURL(action.URL)
		if err != nil || action.Target != "" || action.Element != "" || action.Value != "" || action.Accept ||
			action.PromptProvided ||
			action.Key != "" ||
			action.Direction != "" ||
			action.Amount != 0 {
			return "", nil, fmt.Errorf("%w: malformed navigate action", ErrInvalid)
		}
		return "browser_navigate", map[string]any{"url": normalized}, nil
	case DriverClick:
		if !playwrightTargetPattern.MatchString(action.Target) || action.URL != "" || action.Accept ||
			action.PromptProvided ||
			action.Value != "" ||
			action.Key != "" ||
			action.Direction != "" ||
			action.Amount != 0 ||
			len(action.Element) > MaxElementNameBytes {
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
		if !playwrightTargetPattern.MatchString(action.Target) || action.URL != "" || action.Accept ||
			action.PromptProvided ||
			len(action.Element) > MaxElementNameBytes ||
			len(action.Value) > limits.TextInputBytes ||
			action.Key != "" ||
			action.Direction != "" ||
			action.Amount != 0 {
			return "", nil, fmt.Errorf("%w: malformed fill action", ErrInvalid)
		}
		arguments := map[string]any{
			"target": action.Target, "text": action.Value, "submit": false, "slowly": false,
		}
		if action.Element != "" {
			arguments["element"] = action.Element
		}
		return "browser_type", arguments, nil
	case DriverSelect:
		if !playwrightTargetPattern.MatchString(action.Target) || action.URL != "" || action.Key != "" ||
			action.Accept ||
			action.PromptProvided ||
			action.Direction != "" ||
			action.Amount != 0 ||
			len(action.Element) > MaxElementNameBytes ||
			action.Value == "" ||
			len(action.Value) > limits.TextInputBytes {
			return "", nil, fmt.Errorf("%w: malformed select action", ErrInvalid)
		}
		arguments := map[string]any{"target": action.Target, "values": []string{action.Value}}
		if action.Element != "" {
			arguments["element"] = action.Element
		}
		return "browser_select_option", arguments, nil
	case DriverPress:
		if !validBrowserKey(action.Key) || action.URL != "" || action.Target != "" || action.Accept ||
			action.PromptProvided ||
			action.Element != "" ||
			action.Value != "" ||
			action.Direction != "" ||
			action.Amount != 0 {
			return "", nil, fmt.Errorf("%w: malformed press action", ErrInvalid)
		}
		return "browser_press_key", map[string]any{"key": action.Key}, nil
	case DriverScroll:
		if action.URL != "" || action.Target != "" || action.Element != "" || action.Value != "" || action.Accept ||
			action.PromptProvided ||
			action.Key != "" ||
			(action.Direction != "up" && action.Direction != "down") ||
			action.Amount < 1 ||
			action.Amount > MaxScrollAmount {
			return "", nil, fmt.Errorf("%w: malformed scroll action", ErrInvalid)
		}
		delta := action.Amount * 500
		if action.Direction == "up" {
			delta = -delta
		}
		return "browser_mouse_wheel", map[string]any{"deltaX": 0, "deltaY": delta}, nil
	case DriverDialog:
		if action.URL != "" || action.Target != "" || action.Element != "" || action.Key != "" ||
			action.Direction != "" || action.Amount != 0 || len(action.Value) > limits.TextInputBytes ||
			(!action.Accept && (action.Value != "" || action.PromptProvided)) ||
			(!action.PromptProvided && action.Value != "") {
			return "", nil, fmt.Errorf("%w: malformed dialog action", ErrInvalid)
		}
		arguments := map[string]any{"accept": action.Accept}
		if action.PromptProvided {
			arguments["promptText"] = action.Value
		}
		return "browser_handle_dialog", arguments, nil
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
	maximumToolResultBytes int,
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
	referenceTokens := playwrightSnapshotRefToken.FindAllStringIndex(snapshot, -1)
	targetReferences := playwrightSnapshotTargetRef.FindAllStringSubmatch(snapshot, -1)
	if pageURL == initialBlankOrigin {
		if maximumSnapshotBytes <= 0 || maximumSnapshotRefs <= 0 ||
			maximumToolResultBytes < config.BrowserToolResultEnvelopeBytes || title != "" || snapshot != "" ||
			len(referenceTokens) != 0 || len(targetReferences) != 0 {
			return DriverObservation{}, ErrDriverIncompatible
		}
		return DriverObservation{URL: initialBlankOrigin, Origin: initialBlankOrigin}, nil
	}
	if snapshot == "" || len(title) > 1024 || maximumSnapshotBytes <= 0 || maximumSnapshotRefs <= 0 ||
		maximumToolResultBytes < config.BrowserToolResultEnvelopeBytes ||
		len(referenceTokens) != len(targetReferences) {
		return DriverObservation{}, ErrDriverIncompatible
	}
	safeURL, origin, err := sanitizeObservedURL(pageURL)
	if err != nil {
		return DriverObservation{}, ErrDriverIncompatible
	}
	projected, truncated, err := projectPlaywrightSnapshot(
		snapshot,
		maximumSnapshotBytes,
		maximumSnapshotRefs,
		maximumToolResultBytes-config.BrowserToolResultEnvelopeBytes,
	)
	if err != nil {
		return DriverObservation{}, err
	}
	elements := parsePlaywrightElements(projected)
	return DriverObservation{
		URL: safeURL, Origin: origin, Title: title, Snapshot: projected, Elements: elements,
		Truncated: truncated,
	}, nil
}

func projectPlaywrightSnapshot(
	snapshot string,
	maximumBytes int,
	maximumRefs int,
	maximumEncodedBytes int,
) (string, bool, error) {
	if snapshot == "" || maximumBytes <= 0 || maximumRefs <= 0 || maximumEncodedBytes < 0 {
		return "", false, ErrDriverIncompatible
	}
	if visiblePlaywrightSnapshotBytes(snapshot) <= maximumBytes &&
		encodedVisiblePlaywrightSnapshotBytes(snapshot) <= maximumEncodedBytes &&
		len(playwrightSnapshotTargetRef.FindAllStringSubmatch(snapshot, -1)) <= maximumRefs {
		return snapshot, false, nil
	}
	var projected strings.Builder
	retainedRefs := 0
	projectedVisibleBytes := 0
	projectedEncodedBytes := 0
	for _, line := range strings.SplitAfter(snapshot, "\n") {
		lineTargets := playwrightSnapshotTargetRef.FindAllStringSubmatch(line, -1)
		lineVisibleBytes := visiblePlaywrightSnapshotBytes(line)
		lineEncodedBytes := encodedVisiblePlaywrightSnapshotBytes(line)
		if projectedVisibleBytes+lineVisibleBytes > maximumBytes ||
			projectedEncodedBytes+lineEncodedBytes > maximumEncodedBytes ||
			retainedRefs+len(lineTargets) > maximumRefs {
			break
		}
		projected.WriteString(line)
		retainedRefs += len(lineTargets)
		projectedVisibleBytes += lineVisibleBytes
		projectedEncodedBytes += lineEncodedBytes
	}
	result := strings.TrimSuffix(projected.String(), "\n")
	return result, true, nil
}

func visiblePlaywrightSnapshotBytes(snapshot string) int {
	visibleBytes := len(snapshot)
	for _, target := range playwrightSnapshotTargetRef.FindAllStringSubmatch(snapshot, -1) {
		visibleBytes += opaqueSnapshotReferenceBytes - len(target[1])
	}
	return visibleBytes
}

func encodedVisiblePlaywrightSnapshotBytes(snapshot string) int {
	visible := playwrightSnapshotTargetRef.ReplaceAllString(
		snapshot,
		"[ref=ref_00000000000000000000000000000000]",
	)
	return encodedJSONStringBytes(visible)
}

func encodedJSONStringBytes(value string) int {
	encoded, _ := json.Marshal(value)
	return len(encoded) - len(`""`)
}

func parsePlaywrightPendingDialog(
	text string,
	allowSnapshotTail bool,
) (*DialogObservation, error) {
	lines := strings.Split(text, "\n")
	modalHeader := -1
	inFence := false
	for index, line := range lines {
		if strings.HasPrefix(line, "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		if line == "### Modal state" {
			if modalHeader >= 0 {
				return nil, ErrDriverIncompatible
			}
			modalHeader = index
		}
	}
	if inFence {
		return nil, ErrDriverIncompatible
	}
	if modalHeader < 0 {
		return nil, nil
	}
	if modalHeader+1 >= len(lines) {
		return nil, ErrDriverIncompatible
	}
	match := playwrightDialogPattern.FindStringSubmatch(lines[modalHeader+1])
	if len(match) != 3 || !validDialogType(match[1]) || len(match[2]) > MaxDialogMessageBytes {
		return nil, ErrDriverIncompatible
	}
	tail := trimEmptyPlaywrightLines(lines[modalHeader+2:])
	if len(tail) != 0 {
		if !allowSnapshotTail || !validPlaywrightSnapshotTail(tail) {
			return nil, ErrDriverIncompatible
		}
	}
	return &DialogObservation{Type: match[1], Message: match[2]}, nil
}

func trimEmptyPlaywrightLines(lines []string) []string {
	for len(lines) != 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) != 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func validPlaywrightSnapshotTail(lines []string) bool {
	if len(lines) == 2 && lines[0] == "### Snapshot" &&
		playwrightSnapshotLinkPattern.MatchString(lines[1]) {
		return true
	}
	if len(lines) < 3 || lines[0] != "### Snapshot" || lines[1] != "```yaml" ||
		lines[len(lines)-1] != "```" {
		return false
	}
	for _, line := range lines[2 : len(lines)-1] {
		if line == "```" || strings.HasPrefix(line, "### ") {
			return false
		}
	}
	return true
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
	if len(raw) > MaxURLBytes {
		return "", "", ErrInvalid
	}
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
	safeURL := parsed.String()
	if len(safeURL) > MaxURLBytes || len(origin) > MaxURLBytes {
		return "", "", ErrInvalid
	}
	return safeURL, origin, nil
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
	"browser_select_option": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{
			"element":{"description":"Human-readable element description used to obtain permission to interact with the element","type":"string"},
			"target":{"description":"Exact target element reference from the page snapshot, or a unique element selector","type":"string"},
			"values":{"description":"Array of values to select in the dropdown. This can be a single value or multiple values.","items":{"type":"string"},"type":"array"}
		},
		"required":["target","values"],
		"type":"object"
	}`),
	"browser_press_key": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{"key":{"description":"Name of the key to press or a character to generate, such as ` + "`ArrowLeft` or `a`" + `","type":"string"}},
		"required":["key"],
		"type":"object"
	}`),
	"browser_mouse_wheel": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{
			"deltaX":{"default":0,"description":"X delta","type":"number"},
			"deltaY":{"default":0,"description":"Y delta","type":"number"}
		},
		"required":["deltaX","deltaY"],
		"type":"object"
	}`),
	"browser_handle_dialog": json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"additionalProperties":false,
		"properties":{
			"accept":{"description":"Whether to accept the dialog.","type":"boolean"},
			"promptText":{"description":"The text of the prompt in case of a prompt dialog.","type":"string"}
		},
		"required":["accept"],
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
