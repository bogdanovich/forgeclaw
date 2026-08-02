package browser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/bogdanovich/mintclaw/pkg/config"
	localmcp "github.com/bogdanovich/mintclaw/pkg/mcp"
)

type playwrightCall struct {
	tool      string
	arguments map[string]any
}

type fakePlaywrightClient struct {
	catalog     []*sdkmcp.Tool
	connectErr  error
	connectCtx  context.Context
	connectName string
	connectCfg  config.MCPServerConfig
	pingErr     error
	calls       []playwrightCall
	callErrors  map[string]error
	callResults map[string]*sdkmcp.CallToolResult
	closeErr    error
	closeCalls  int
}

func (client *fakePlaywrightClient) Connect(
	ctx context.Context,
	name string,
	cfg config.MCPServerConfig,
) ([]*sdkmcp.Tool, error) {
	client.connectCtx = ctx
	client.connectName = name
	client.connectCfg = cloneMCPServerConfig(cfg)
	return client.catalog, client.connectErr
}

func (client *fakePlaywrightClient) Ping(context.Context) error {
	return client.pingErr
}

func (client *fakePlaywrightClient) CallTool(
	_ context.Context,
	tool string,
	arguments map[string]any,
) (*sdkmcp.CallToolResult, error) {
	cloned := make(map[string]any, len(arguments))
	for key, value := range arguments {
		cloned[key] = value
	}
	client.calls = append(client.calls, playwrightCall{tool: tool, arguments: cloned})
	if err := client.callErrors[tool]; err != nil {
		return nil, err
	}
	if result := client.callResults[tool]; result != nil {
		return result, nil
	}
	return playwrightTextResult("ok"), nil
}

func (client *fakePlaywrightClient) Close() error {
	client.closeCalls++
	return client.closeErr
}

func TestPlaywrightWorkerFactoryOwnsPrivateClientAndMapsAdmittedCalls(t *testing.T) {
	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.AllowedOrigins = []string{"https://Example.COM:443/", "http://b.example:80"}
	target.Profiles[config.BrowserDefaultProfile] = profile
	root.Tools.Browser.Targets[config.BrowserDefaultTarget] = target
	factory, err := NewPlaywrightWorkerFactory(root)
	if err != nil {
		t.Fatalf("NewPlaywrightWorkerFactory() error = %v", err)
	}
	server := root.Tools.MCP.Servers["playwright"]
	server.Command = "changed-after-snapshot"
	root.Tools.MCP.Servers["playwright"] = server

	client := &fakePlaywrightClient{
		catalog: playwrightCatalogFixture(),
		callResults: map[string]*sdkmcp.CallToolResult{
			"browser_snapshot": playwrightTextResult(
				"### Page\n- Page URL: https://Example.COM:443/items?q=secret#private\n" +
					"- Page Title: Fixture\n### Snapshot\n```yaml\n" +
					"- textbox \"Name\" [ref=e3]\n```",
			),
		},
	}
	factory.clientFactory = func() playwrightMCPClient { return client }
	openCtx, cancelOpen := context.WithCancel(context.Background())
	opened, err := factory.Open(openCtx, WorkerOpenRequest{
		SessionID: "session_1", Target: "gateway", Profile: "managed", DryRun: true,
		Limits: config.BrowserLimitsConfig{},
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker, ok := opened.Owner.(*playwrightWorker)
	if !ok {
		t.Fatalf("Open() worker type = %T", opened.Owner)
	}
	if len(worker.catalogRevision) != 64 {
		t.Fatalf("catalog revision = %q", worker.catalogRevision)
	}
	cancelOpen()
	select {
	case <-client.connectCtx.Done():
		t.Fatal("worker lifetime remained attached to the completed open call")
	default:
	}
	if client.connectName != playwrightPrivateServerName || client.connectCfg.Command != "npx" ||
		client.connectCfg.Enabled || !reflect.DeepEqual(
		client.connectCfg.Args,
		[]string{"--caps", "vision", "--allowed-origins", "http://b.example;https://example.com"},
	) || client.connectCfg.Env["PLAYWRIGHT_MCP_ALLOWED_ORIGINS"] !=
		"http://b.example;https://example.com" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_BLOCKED_ORIGINS"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_CONFIG"] != "" {
		t.Fatalf("private connection = %q, %+v", client.connectName, client.connectCfg)
	}
	if status, statusErr := worker.Status(context.Background()); statusErr != nil || status != WorkerReady {
		t.Fatalf("Status() = %q, %v", status, statusErr)
	}
	observation, err := worker.Observe(context.Background())
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if observation.URL != "https://example.com/items" || observation.Origin != "https://example.com" ||
		observation.Title != "Fixture" || !strings.Contains(observation.Snapshot, "[ref=e3]") ||
		len(observation.Elements) != 1 ||
		observation.Elements[0] != (DriverElement{Target: "e3", Role: "textbox", Name: "Name"}) {
		t.Fatalf("Observe() = %+v", observation)
	}
	if err = worker.Execute(context.Background(), DriverAction{
		Kind: DriverNavigate, URL: "https://Example.COM/next?q=1",
	}); err != nil {
		t.Fatalf("Execute(navigate) error = %v", err)
	}
	if err = worker.Execute(context.Background(), DriverAction{
		Kind: DriverFill, Target: "e3", Element: "Name", Value: "Ada",
	}); err != nil {
		t.Fatalf("Execute(fill) error = %v", err)
	}
	if err = worker.Execute(context.Background(), DriverAction{
		Kind: DriverClick, Target: "e4", Element: "Save",
	}); err != nil {
		t.Fatalf("Execute(click) error = %v", err)
	}
	if err = worker.Execute(context.Background(), DriverAction{
		Kind: DriverSelect, Target: "e5", Element: "State", Value: "CA",
	}); err != nil {
		t.Fatalf("Execute(select) error = %v", err)
	}
	if err = worker.Execute(context.Background(), DriverAction{Kind: DriverPress, Key: "Tab"}); err != nil {
		t.Fatalf("Execute(press) error = %v", err)
	}
	if err = worker.Execute(context.Background(), DriverAction{
		Kind: DriverScroll, Direction: "down", Amount: 2,
	}); err != nil {
		t.Fatalf("Execute(scroll) error = %v", err)
	}
	if err = worker.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-client.connectCtx.Done():
	default:
		t.Fatal("Close() did not cancel the worker lifetime")
	}
	if err = worker.Close(context.Background()); err != nil || client.closeCalls != 1 {
		t.Fatalf("second Close() error = %v, client closes = %d", err, client.closeCalls)
	}

	wantTools := []string{
		"browser_snapshot", "browser_navigate", "browser_type", "browser_click",
		"browser_select_option", "browser_press_key", "browser_mouse_wheel", "browser_close",
	}
	if len(client.calls) != len(wantTools) {
		t.Fatalf("driver calls = %+v", client.calls)
	}
	for index, want := range wantTools {
		if client.calls[index].tool != want {
			t.Fatalf("driver call %d = %+v, want %q", index, client.calls[index], want)
		}
	}
	if got := client.calls[1].arguments["url"]; got != "https://example.com/next?q=1" {
		t.Fatalf("navigate URL = %#v", got)
	}
	fill := client.calls[2].arguments
	if fill["target"] != "e3" || fill["text"] != "Ada" || fill["submit"] != false ||
		fill["slowly"] != false {
		t.Fatalf("fill arguments = %+v", fill)
	}
	click := client.calls[3].arguments
	if click["target"] != "e4" || click["doubleClick"] != false || click["button"] != "left" {
		t.Fatalf("click arguments = %+v", click)
	}
	selectArgs := client.calls[4].arguments
	if selectArgs["target"] != "e5" || !reflect.DeepEqual(selectArgs["values"], []string{"CA"}) {
		t.Fatalf("select arguments = %+v", selectArgs)
	}
	if got := client.calls[5].arguments["key"]; got != "Tab" {
		t.Fatalf("press key = %#v", got)
	}
	if scroll := client.calls[6].arguments; scroll["deltaX"] != 0 || scroll["deltaY"] != 1000 {
		t.Fatalf("scroll arguments = %+v", scroll)
	}
}

func TestPlaywrightWorkerFactoryRejectsOperatorOriginControls(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{name: "allowed argument", args: []string{"--allowed-origins", "https://other.example"}},
		{name: "allowed equals argument", args: []string{"--allowed-origins=https://other.example"}},
		{name: "blocked argument", args: []string{"--blocked-origins=*&!https://example.com"}},
		{name: "config argument", args: []string{"--config", "browser-policy.json"}},
		{name: "config equals argument", args: []string{"--config=browser-policy.json"}},
		{name: "caps argument", args: []string{"--caps", "pdf"}},
		{name: "caps equals argument", args: []string{"--caps=pdf"}},
		{name: "allowed environment", env: map[string]string{"PLAYWRIGHT_MCP_ALLOWED_ORIGINS": "*"}},
		{name: "blocked environment", env: map[string]string{"PLAYWRIGHT_MCP_BLOCKED_ORIGINS": ""}},
		{name: "config environment", env: map[string]string{"PLAYWRIGHT_MCP_CONFIG": "browser-policy.json"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := admittedBrowserConfig()
			server := root.Tools.MCP.Servers["playwright"]
			server.Args = test.args
			server.Env = test.env
			root.Tools.MCP.Servers["playwright"] = server
			if _, err := NewPlaywrightWorkerFactory(root); err == nil ||
				!strings.Contains(err.Error(), "policy and capabilities must be managed") {
				t.Fatalf("NewPlaywrightWorkerFactory() error = %v", err)
			}
		})
	}
}

func TestPlaywrightWorkerFactoryRejectsIncompatibleCatalog(t *testing.T) {
	tests := []struct {
		name    string
		catalog []*sdkmcp.Tool
	}{
		{name: "missing tool", catalog: playwrightCatalogFixture()[:4]},
		{name: "extra property", catalog: playwrightCatalogWithMutation("extra_property")},
		{name: "changed constraint", catalog: playwrightCatalogWithMutation("changed_constraint")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory, err := NewPlaywrightWorkerFactory(admittedBrowserConfig())
			if err != nil {
				t.Fatalf("NewPlaywrightWorkerFactory() error = %v", err)
			}
			client := &fakePlaywrightClient{catalog: test.catalog}
			factory.clientFactory = func() playwrightMCPClient { return client }
			opened, openErr := factory.Open(context.Background(), WorkerOpenRequest{
				SessionID: "session_1", Target: "gateway", Profile: "managed", DryRun: true,
			})
			if !errors.Is(openErr, ErrDriverIncompatible) || opened.Owner == nil || client.closeCalls != 0 {
				t.Fatalf("Open() = %+v, %v; client closes = %d", opened, openErr, client.closeCalls)
			}
			if err = opened.Owner.Close(context.Background()); err != nil || client.closeCalls != 1 {
				t.Fatalf("cleanup Close() error = %v, client closes = %d", err, client.closeCalls)
			}
		})
	}
}

func TestPlaywrightWorkerFactoryReturnsRetryableCleanupOwnerAfterCatalogFailure(t *testing.T) {
	factory, err := NewPlaywrightWorkerFactory(admittedBrowserConfig())
	if err != nil {
		t.Fatalf("NewPlaywrightWorkerFactory() error = %v", err)
	}
	client := &fakePlaywrightClient{
		catalog: playwrightCatalogFixture()[:4], closeErr: errors.New("process tree still alive"),
	}
	factory.clientFactory = func() playwrightMCPClient { return client }
	opened, err := factory.Open(context.Background(), WorkerOpenRequest{
		SessionID: "session_1", Target: "gateway", Profile: "managed", DryRun: true,
	})
	if !errors.Is(err, ErrDriverIncompatible) || opened.Owner == nil {
		t.Fatalf("Open() = %+v, %v; want cleanup owner and incompatible error", opened, err)
	}
	if err = opened.Owner.Close(context.Background()); !errors.Is(err, ErrWorkerUnavailable) ||
		client.closeCalls != 1 {
		t.Fatalf("first cleanup Close() error = %v, client closes = %d", err, client.closeCalls)
	}
	client.closeErr = nil
	if err = opened.Owner.Close(context.Background()); err != nil || client.closeCalls != 2 {
		t.Fatalf("second cleanup Close() error = %v, client closes = %d", err, client.closeCalls)
	}
	if len(client.calls) != 0 {
		t.Fatalf("failed startup replayed browser calls: %+v", client.calls)
	}
}

func TestBrokerRetriesPlaywrightCleanupAfterCatalogFailure(t *testing.T) {
	root := admittedBrowserConfig()
	factory, err := NewPlaywrightWorkerFactory(root)
	if err != nil {
		t.Fatalf("NewPlaywrightWorkerFactory() error = %v", err)
	}
	client := &fakePlaywrightClient{
		catalog: playwrightCatalogFixture()[:4], closeErr: errors.New("process tree still alive"),
	}
	factory.clientFactory = func() playwrightMCPClient { return client }
	broker := newTestBroker(t, root, NewMemoryStore(), factory)
	owner := testOwner()

	session, err := broker.Open(context.Background(), OpenRequest{
		Owner: owner, Target: config.BrowserDefaultTarget, Profile: config.BrowserDefaultProfile,
	})
	if !errors.Is(err, ErrWorkerUnavailable) || session.State != SessionClosing ||
		client.closeCalls != 1 {
		t.Fatalf("Open() = %+v, %v; client closes = %d", session, err, client.closeCalls)
	}
	client.closeErr = nil
	lost, err := broker.Close(context.Background(), owner, session.ID)
	if err != nil || lost.State != SessionLost || lost.SafeFailure != "worker_unavailable" ||
		client.closeCalls != 2 {
		t.Fatalf("Close() retry = %+v, %v; client closes = %d", lost, err, client.closeCalls)
	}
	if len(client.calls) != 0 {
		t.Fatalf("failed startup admitted browser calls: %+v", client.calls)
	}
}

func TestPlaywrightWorkerRejectsSelectorsOversizedInputAndUnknownActions(t *testing.T) {
	client := &fakePlaywrightClient{catalog: playwrightCatalogFixture()}
	worker := &playwrightWorker{
		client: client, limits: config.BrowserLimitsConfig{}.Effective(),
	}
	tests := []DriverAction{
		{Kind: DriverClick, Target: ".submit"},
		{Kind: DriverFill, Target: "e1", Value: strings.Repeat("x", config.BrowserMaxTextInputBytes+1)},
		{Kind: "evaluate", Target: "e1"},
		{Kind: DriverNavigate, URL: "file:///private/data"},
		{Kind: DriverSelect, Target: "e1", Value: ""},
		{Kind: DriverPress, Key: "Control+L"},
		{Kind: DriverScroll, Direction: "down", Amount: MaxScrollAmount + 1},
		{Kind: DriverScroll, Direction: "left", Amount: 1},
	}
	for _, action := range tests {
		if err := worker.Execute(context.Background(), action); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Execute(%+v) error = %v, want ErrInvalid", action, err)
		}
	}
	if len(client.calls) != 0 {
		t.Fatalf("driver calls after rejected actions = %+v", client.calls)
	}
}

func TestPlaywrightWorkerDoesNotReplayUncertainCallAndBecomesLost(t *testing.T) {
	client := &fakePlaywrightClient{
		catalog: playwrightCatalogFixture(),
		callErrors: map[string]error{
			"browser_click": &localmcp.CallOutcomeUncertainError{
				Server: playwrightPrivateServerName, Tool: "browser_click", Reconnected: true,
			},
		},
	}
	worker := &playwrightWorker{
		client: client, limits: config.BrowserLimitsConfig{}.Effective(),
	}
	err := worker.Execute(context.Background(), DriverAction{Kind: DriverClick, Target: "e1"})
	if !errors.Is(err, ErrWorkerUnavailable) || len(client.calls) != 1 {
		t.Fatalf("Execute() error = %v, calls = %+v", err, client.calls)
	}
	status, statusErr := worker.Status(context.Background())
	if statusErr != nil || status != WorkerLost {
		t.Fatalf("Status() = %q, %v", status, statusErr)
	}
}

func TestPlaywrightWorkerCloseFailureRetriesManagerCleanup(t *testing.T) {
	client := &fakePlaywrightClient{closeErr: errors.New("secret process failure")}
	lifetimeCtx, cancelLifetime := context.WithCancel(context.Background())
	worker := &playwrightWorker{
		client: client, limits: config.BrowserLimitsConfig{}.Effective(),
		cancelLifetime: cancelLifetime,
	}
	if err := worker.Close(context.Background()); !errors.Is(err, ErrWorkerUnavailable) ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("Close() error = %v", err)
	}
	client.closeErr = nil
	if err := worker.Close(context.Background()); err != nil || client.closeCalls != 2 {
		t.Fatalf("second Close() error = %v, client closes = %d", err, client.closeCalls)
	}
	if err := worker.Close(context.Background()); err != nil || client.closeCalls != 2 {
		t.Fatalf("third Close() error = %v, client closes = %d", err, client.closeCalls)
	}
	if len(client.calls) != 1 || client.calls[0].tool != "browser_close" {
		t.Fatalf("browser close calls = %+v, want exactly one", client.calls)
	}
	select {
	case <-lifetimeCtx.Done():
	default:
		t.Fatal("failed Close() did not cancel the worker lifetime")
	}
}

func TestPlaywrightWorkerBoundsObservationAndRedactsDriverError(t *testing.T) {
	client := &fakePlaywrightClient{
		catalog: playwrightCatalogFixture(),
		callResults: map[string]*sdkmcp.CallToolResult{
			"browser_snapshot": playwrightTextResult(strings.Repeat("x", 33)),
		},
	}
	worker := &playwrightWorker{
		client: client,
		limits: config.BrowserLimitsConfig{ToolResultBytes: 32, SnapshotBytes: 16}.Effective(),
	}
	if _, err := worker.Observe(context.Background()); !errors.Is(err, ErrDriverIncompatible) {
		t.Fatalf("Observe() oversized error = %v", err)
	}
	client.callResults = nil
	client.callErrors = map[string]error{"browser_snapshot": errors.New("secret endpoint and profile path")}
	worker.lost = false
	if _, err := worker.Observe(context.Background()); !errors.Is(err, ErrWorkerUnavailable) ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("Observe() driver error = %v", err)
	}
}

func TestPlaywrightObservationEnforcesReferenceLimit(t *testing.T) {
	observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n- button [ref=e1]\n- textbox [ref=e2]\n```"
	if _, err := parsePlaywrightObservation(observation, 1024, 2); err != nil {
		t.Fatalf("parsePlaywrightObservation() boundary error = %v", err)
	}
	if _, err := parsePlaywrightObservation(observation, 1024, 1); !errors.Is(err, ErrDriverIncompatible) {
		t.Fatalf("parsePlaywrightObservation() over-limit error = %v", err)
	}
	malformed := strings.Replace(observation, "[ref=e1]", "[ref=selector]", 1)
	if _, err := parsePlaywrightObservation(malformed, 1024, 2); !errors.Is(err, ErrDriverIncompatible) {
		t.Fatalf("parsePlaywrightObservation() malformed ref error = %v", err)
	}
}

func TestPlaywrightWorkerRealBrowserFixture(t *testing.T) {
	if os.Getenv("MINTCLAW_BROWSER_REAL_DRIVER") != "1" {
		t.Skip("set MINTCLAW_BROWSER_REAL_DRIVER=1 to run the pinned Playwright MCP fixture")
	}
	fixture := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(writer, `<!doctype html><title>MintClaw Fixture</title>
<form onsubmit="event.preventDefault(); document.querySelector('output').textContent='Saved '+document.querySelector('input').value">
<label>Name <input aria-label="Name"></label>
<label>State <select aria-label="State"><option value="CA">California</option><option value="NY">New York</option></select></label>
<button type="submit">Save</button></form><output></output><div style="height:2000px"></div>`)
	}))
	defer fixture.Close()
	fixtureURL, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	fixtureURL.Host = "127.0.0.1.nip.io:" + fixtureURL.Port()
	fixtureOrigin := fixtureURL.String()

	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.AllowedOrigins = []string{fixtureOrigin}
	target.Profiles[config.BrowserDefaultProfile] = profile
	root.Tools.Browser.Targets[config.BrowserDefaultTarget] = target
	server := root.Tools.MCP.Servers["playwright"]
	driverTemp := t.TempDir()
	server.ExclusiveLockFile = filepath.Join(driverTemp, "playwright.lock")
	server.Args = []string{
		"-y", "@playwright/mcp@0.0.78", "--headless", "--browser=chrome", "--isolated",
		"--output-mode=stdout", "--output-dir=" + filepath.Join(driverTemp, "output"),
	}
	root.Tools.MCP.Servers["playwright"] = server
	factory, err := NewPlaywrightWorkerFactory(root)
	if err != nil {
		t.Fatalf("NewPlaywrightWorkerFactory() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	opened, err := factory.Open(ctx, WorkerOpenRequest{
		SessionID: "fixture_session", Target: "gateway", Profile: "managed", DryRun: true,
		Limits: config.BrowserLimitsConfig{},
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker := opened.Owner.(*playwrightWorker)
	t.Cleanup(func() { _ = worker.Close(context.Background()) })
	if err = worker.Execute(ctx, DriverAction{Kind: DriverNavigate, URL: fixtureOrigin}); err != nil {
		t.Fatalf("navigate error = %v", err)
	}
	observation, err := worker.Observe(ctx)
	if err != nil {
		t.Fatalf("first Observe() error = %v", err)
	}
	textbox := mustSnapshotRef(t, observation.Snapshot, `textbox "Name" \[ref=(e[0-9]+)\]`)
	if err = worker.Execute(ctx, DriverAction{
		Kind: DriverFill, Target: textbox, Element: "Name", Value: "Ada",
	}); err != nil {
		t.Fatalf("fill error = %v", err)
	}
	observation, err = worker.Observe(ctx)
	if err != nil || !strings.Contains(observation.Snapshot, "Ada") {
		t.Fatalf("Observe() after fill = %+v, %v", observation, err)
	}
	state := mustSnapshotRef(t, observation.Snapshot, `combobox "State" \[ref=(e[0-9]+)\]`)
	if err = worker.Execute(ctx, DriverAction{
		Kind: DriverSelect, Target: state, Element: "State", Value: "NY",
	}); err != nil {
		t.Fatalf("select error = %v", err)
	}
	if err = worker.Execute(ctx, DriverAction{Kind: DriverPress, Key: "Tab"}); err != nil {
		t.Fatalf("press error = %v", err)
	}
	if err = worker.Execute(ctx, DriverAction{
		Kind: DriverScroll, Direction: "down", Amount: 1,
	}); err != nil {
		t.Fatalf("scroll error = %v", err)
	}
	button := mustSnapshotRef(t, observation.Snapshot, `button "Save" \[ref=(e[0-9]+)\]`)
	if err = worker.Execute(ctx, DriverAction{
		Kind: DriverClick, Target: button, Element: "Save",
	}); err != nil {
		t.Fatalf("click error = %v", err)
	}
	observation, err = worker.Observe(ctx)
	if err != nil || !strings.Contains(observation.Snapshot, "Saved Ada") {
		t.Fatalf("Observe() after click = %+v, %v", observation, err)
	}
	if err = worker.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func mustSnapshotRef(t *testing.T, snapshot, pattern string) string {
	t.Helper()
	matches := regexp.MustCompile(pattern).FindStringSubmatch(snapshot)
	if len(matches) != 2 {
		t.Fatalf("snapshot does not match %q:\n%s", pattern, snapshot)
	}
	return matches[1]
}

func playwrightTextResult(text string) *sdkmcp.CallToolResult {
	return &sdkmcp.CallToolResult{Content: []sdkmcp.Content{&sdkmcp.TextContent{Text: text}}}
}

func playwrightCatalogFixture() []*sdkmcp.Tool {
	names := []string{
		"browser_close", "browser_navigate", "browser_snapshot", "browser_click", "browser_type",
		"browser_select_option", "browser_press_key", "browser_mouse_wheel",
	}
	catalog := make([]*sdkmcp.Tool, 0, len(names)+1)
	for _, name := range names {
		var schema map[string]any
		if err := json.Unmarshal(pinnedPlaywrightToolSchemas[name], &schema); err != nil {
			panic(err)
		}
		catalog = append(catalog, &sdkmcp.Tool{Name: name, InputSchema: schema})
	}
	catalog = append(catalog, &sdkmcp.Tool{
		Name: "browser_run_code_unsafe",
		InputSchema: map[string]any{
			"type": "object", "properties": map[string]any{"code": map[string]any{"type": "string"}},
		},
	})
	return catalog
}

func playwrightCatalogWithMutation(mutation string) []*sdkmcp.Tool {
	catalog := playwrightCatalogFixture()
	for _, tool := range catalog {
		if tool.Name != "browser_click" {
			continue
		}
		schema := tool.InputSchema.(map[string]any)
		properties := schema["properties"].(map[string]any)
		switch mutation {
		case "extra_property":
			properties["selector"] = map[string]any{"type": "string"}
		case "changed_constraint":
			button := properties["button"].(map[string]any)
			button["enum"] = append(button["enum"].([]any), "back")
		default:
			panic("unknown catalog mutation")
		}
	}
	return catalog
}
