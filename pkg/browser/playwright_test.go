package browser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
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
	args := client.connectCfg.Args
	if client.connectName != playwrightPrivateServerName || client.connectCfg.Command != "npx" ||
		client.connectCfg.Enabled || len(args) != 8 ||
		!reflect.DeepEqual(args[:4], []string{"--caps", "vision", "--proxy-server", args[3]}) ||
		!strings.HasPrefix(args[3], "http://127.0.0.1:") ||
		!reflect.DeepEqual(
			args[4:],
			[]string{"--proxy-bypass", "<-loopback>", "--allowed-origins", "http://b.example;https://example.com"},
		) || client.connectCfg.Env["PLAYWRIGHT_MCP_ALLOWED_ORIGINS"] !=
		"http://b.example;https://example.com" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_BLOCKED_ORIGINS"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_CAPS"] != "vision" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_CONFIG"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_PROXY_SERVER"] != args[3] ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_PROXY_BYPASS"] != "<-loopback>" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_CDP_ENDPOINT"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_ENDPOINT"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_EXTENSION"] != "" {
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

func TestPlaywrightWorkerFactoryConfiguresPublicWebWithoutDriverAllowlist(t *testing.T) {
	t.Setenv("PLAYWRIGHT_MCP_CDP_ENDPOINT", "http://127.0.0.1:9222")
	t.Setenv("PLAYWRIGHT_MCP_ENDPOINT", "ws://127.0.0.1:3000")
	t.Setenv("PLAYWRIGHT_MCP_EXTENSION", "true")
	t.Setenv("PLAYWRIGHT_MCP_PROXY_SERVER", "http://unmanaged-proxy.example")
	t.Setenv("PLAYWRIGHT_MCP_PROXY_BYPASS", "localhost")
	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkPublicWeb
	profile.AllowedOrigins = nil
	target.Profiles[config.BrowserDefaultProfile] = profile
	root.Tools.Browser.Targets[config.BrowserDefaultTarget] = target
	server := root.Tools.MCP.Servers["playwright"]
	server.EnvFile = "/operator/playwright.env"
	root.Tools.MCP.Servers["playwright"] = server
	factory, err := NewPlaywrightWorkerFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakePlaywrightClient{catalog: playwrightCatalogFixture()}
	factory.clientFactory = func() playwrightMCPClient { return client }
	opened, err := factory.Open(context.Background(), WorkerOpenRequest{
		SessionID: "session_public", Target: "gateway", Profile: "managed", DryRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := opened.Owner.(*playwrightWorker)
	if err = worker.networkProxy.Close(); err != nil {
		t.Fatal(err)
	}
	if status, statusErr := worker.Status(context.Background()); statusErr != nil || status != WorkerLost {
		t.Fatalf("Status() after proxy exit = %q, %v", status, statusErr)
	}
	if err = worker.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(client.connectCfg.Args, " "), "--allowed-origins") ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_ALLOWED_ORIGINS"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_CAPS"] != "vision" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_PROXY_SERVER"] != client.connectCfg.Args[3] ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_PROXY_BYPASS"] != "<-loopback>" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_CDP_ENDPOINT"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_ENDPOINT"] != "" ||
		client.connectCfg.Env["PLAYWRIGHT_MCP_EXTENSION"] != "" ||
		client.connectCfg.EnvFile != "/operator/playwright.env" ||
		!strings.Contains(strings.Join(client.connectCfg.Args, " "), "--proxy-bypass <-loopback>") {
		t.Fatalf("public-web driver config = %+v", client.connectCfg)
	}
}

func TestPlaywrightServerConfiguresAnyHTTPThroughManagedProxyOnly(t *testing.T) {
	root := admittedBrowserConfig()
	server := root.Tools.MCP.Servers["playwright"]
	profile := config.BrowserProfileConfig{
		Enabled: true, Mode: config.BrowserProfileManaged,
		NetworkMode: config.BrowserNetworkAnyHTTP, DryRun: true,
	}
	configured, err := playwrightServerWithNetworkPolicy(server, profile, "http://127.0.0.1:43210")
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(configured.Args, " ")
	if strings.Contains(args, "--allowed-origins") ||
		configured.Env["PLAYWRIGHT_MCP_ALLOWED_ORIGINS"] != "" {
		t.Fatalf(
			"any_http retained driver allowlist: args=%q env=%q",
			args,
			configured.Env["PLAYWRIGHT_MCP_ALLOWED_ORIGINS"],
		)
	}
	if configured.Env["PLAYWRIGHT_MCP_PROXY_SERVER"] != "http://127.0.0.1:43210" ||
		!strings.Contains(args, "--proxy-server http://127.0.0.1:43210") ||
		configured.Env["PLAYWRIGHT_MCP_PROXY_BYPASS"] != "<-loopback>" {
		t.Fatalf("any_http managed proxy config = args=%q env=%+v", args, configured.Env)
	}
}

func TestPlaywrightWorkerFactoryRejectsOperatorOriginControls(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{name: "allowed argument", args: []string{"--allowed-origins", "https://other.example"}},
		{name: "allowed equals argument", args: []string{"--allowed-origins=https://other.example"}},
		{name: "blocked argument", args: []string{"--blocked-origins=*&!https://example.com"}},
		{name: "config argument", args: []string{"--config", "browser-policy.json"}},
		{name: "config equals argument", args: []string{"--config=browser-policy.json"}},
		{name: "caps argument", args: []string{"--caps", "pdf"}},
		{name: "caps equals argument", args: []string{"--caps=pdf"}},
		{name: "proxy argument", args: []string{"--proxy-server", "http://proxy.example"}},
		{name: "proxy equals argument", args: []string{"--proxy-server=http://proxy.example"}},
		{name: "proxy bypass argument", args: []string{"--proxy-bypass", "localhost"}},
		{name: "proxy bypass equals argument", args: []string{"--proxy-bypass=localhost"}},
		{name: "CDP endpoint argument", args: []string{"--cdp-endpoint", "http://127.0.0.1:9222"}},
		{name: "CDP endpoint equals argument", args: []string{"--cdp-endpoint=http://127.0.0.1:9222"}},
		{name: "bound endpoint argument", args: []string{"--endpoint", "ws://127.0.0.1:3000"}},
		{name: "bound endpoint equals argument", args: []string{"--endpoint=ws://127.0.0.1:3000"}},
		{name: "extension argument", args: []string{"--extension"}},
		{name: "extension equals argument", args: []string{"--extension=chrome"}},
		{name: "allowed environment", env: map[string]string{"PLAYWRIGHT_MCP_ALLOWED_ORIGINS": "*"}},
		{name: "blocked environment", env: map[string]string{"PLAYWRIGHT_MCP_BLOCKED_ORIGINS": ""}},
		{name: "caps environment", env: map[string]string{"PLAYWRIGHT_MCP_CAPS": "pdf"}},
		{name: "config environment", env: map[string]string{"PLAYWRIGHT_MCP_CONFIG": "browser-policy.json"}},
		{name: "proxy environment", env: map[string]string{"PLAYWRIGHT_MCP_PROXY_SERVER": "http://proxy.example"}},
		{name: "proxy bypass environment", env: map[string]string{"PLAYWRIGHT_MCP_PROXY_BYPASS": "localhost"}},
		{
			name: "CDP endpoint environment",
			env:  map[string]string{"PLAYWRIGHT_MCP_CDP_ENDPOINT": "http://127.0.0.1:9222"},
		},
		{name: "bound endpoint environment", env: map[string]string{"PLAYWRIGHT_MCP_ENDPOINT": "ws://127.0.0.1:3000"}},
		{name: "extension environment", env: map[string]string{"PLAYWRIGHT_MCP_EXTENSION": "true"}},
		{
			name: "case-variant CDP endpoint environment",
			env:  map[string]string{"Playwright_Mcp_Cdp_Endpoint": "http://127.0.0.1:9222"},
		},
		{name: "case-variant extension environment", env: map[string]string{"playwright_mcp_extension": "true"}},
		{
			name: "malformed protected environment",
			env:  map[string]string{"PLAYWRIGHT_MCP_CONFIG=/tmp/evil": ""},
			want: "environment name",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := admittedBrowserConfig()
			server := root.Tools.MCP.Servers["playwright"]
			server.Args = test.args
			server.Env = test.env
			root.Tools.MCP.Servers["playwright"] = server
			want := test.want
			if want == "" {
				want = "policy and capabilities must be managed"
			}
			if _, err := NewPlaywrightWorkerFactory(root); err == nil || !strings.Contains(err.Error(), want) {
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
	if _, _, err := mapPlaywrightAction(
		DriverAction{Kind: DriverDialog, Value: "not-allowed-on-dismiss", PromptProvided: true}, worker.limits,
	); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mapPlaywrightAction(malformed dialog) error = %v, want ErrInvalid", err)
	}
	tool, arguments, err := mapPlaywrightAction(
		DriverAction{Kind: DriverClick, Target: "f1e2", Element: "Save"}, worker.limits,
	)
	if err != nil || tool != "browser_click" || arguments["target"] != "f1e2" {
		t.Fatalf("mapPlaywrightAction(frame-qualified target) = %q, %+v, %v", tool, arguments, err)
	}
	tool, arguments, err = mapPlaywrightAction(
		DriverAction{Kind: DriverDialog, Accept: true, PromptProvided: true}, worker.limits,
	)
	if err != nil || tool != "browser_handle_dialog" || arguments["promptText"] != "" {
		t.Fatalf("mapPlaywrightAction(empty prompt) = %q, %+v, %v", tool, arguments, err)
	}
}

func TestPlaywrightWorkerTracksAndHandlesPendingDialog(t *testing.T) {
	client := &fakePlaywrightClient{
		catalog: playwrightCatalogFixture(),
		callResults: map[string]*sdkmcp.CallToolResult{
			"browser_snapshot": playwrightTextResult(
				"### Page\n- Page URL: https://example.com/items\n- Page Title: Fixture\n" +
					"### Snapshot\n```yaml\n- button \"Delete\" [ref=e1]\n```",
			),
			"browser_click": playwrightTextResult(
				"### Modal state\n" +
					"- [\"prompt\" dialog with message \"Type DELETE\"]: can be handled by browser_handle_dialog",
			),
		},
	}
	worker := &playwrightWorker{
		client: client, limits: config.BrowserLimitsConfig{}.Effective(),
	}
	if _, err := worker.Observe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Execute(context.Background(), DriverAction{
		Kind: DriverClick, Target: "e1", Element: "Delete",
	}); err != nil {
		t.Fatalf("Execute(click) error = %v", err)
	}
	callCount := len(client.calls)
	observation, err := worker.Observe(context.Background())
	if err != nil || observation.Snapshot != "" || len(observation.Elements) != 0 ||
		observation.PendingDialog == nil ||
		*observation.PendingDialog != (DialogObservation{Type: "prompt", Message: "Type DELETE"}) {
		t.Fatalf("Observe(pending dialog) = %+v, %v", observation, err)
	}
	if len(client.calls) != callCount {
		t.Fatalf("pending dialog observation called blocked MCP tool: %+v", client.calls)
	}
	if err = worker.Execute(
		context.Background(), DriverAction{Kind: DriverScroll, Direction: "down", Amount: 1},
	); !errors.Is(err, ErrDriverRejected) {
		t.Fatalf("Execute(non-dialog while pending) error = %v, want ErrDriverRejected", err)
	}
	if err = worker.Execute(context.Background(), DriverAction{
		Kind: DriverDialog, Accept: true, Value: "DELETE", PromptProvided: true,
	}); err != nil {
		t.Fatalf("Execute(dialog) error = %v", err)
	}
	last := client.calls[len(client.calls)-1]
	if last.tool != "browser_handle_dialog" || last.arguments["accept"] != true ||
		last.arguments["promptText"] != "DELETE" {
		t.Fatalf("dialog call = %+v", last)
	}
	client.callResults["browser_handle_dialog"] = playwrightTextResult(
		"### Modal state\n" +
			"- [\"alert\" dialog with message \"Saved\"]: can be handled by browser_handle_dialog",
	)
	worker.pendingDialog = &DialogObservation{Type: "prompt", Message: "Type DELETE"}
	if err = worker.Execute(context.Background(), DriverAction{Kind: DriverDialog}); err != nil {
		t.Fatalf("Execute(chained dialog) error = %v", err)
	}
	observation, err = worker.Observe(context.Background())
	if err != nil || observation.PendingDialog == nil ||
		*observation.PendingDialog != (DialogObservation{Type: "alert", Message: "Saved"}) {
		t.Fatalf("Observe(successor dialog) = %+v, %v", observation, err)
	}
}

func TestPlaywrightWorkerPreservesConcurrentDialogFromErrorResult(t *testing.T) {
	client := &fakePlaywrightClient{
		catalog: playwrightCatalogFixture(),
		callResults: map[string]*sdkmcp.CallToolResult{
			"browser_snapshot": playwrightTextResult(
				"### Page\n- Page URL: https://example.com/items\n- Page Title: Fixture\n" +
					"### Snapshot\n```yaml\n- button \"Save\" [ref=e1]\n```",
			),
			"browser_click": {
				IsError: true,
				Content: []sdkmcp.Content{&sdkmcp.TextContent{Text: "### Error\n- blocked by dialog\n" +
					"### Modal state\n- [\"confirm\" dialog with message \"Continue?\"]: can be handled by browser_handle_dialog\n" +
					"### Snapshot\n```yaml\n\n```"}},
			},
		},
	}
	worker := &playwrightWorker{client: client, limits: config.BrowserLimitsConfig{}.Effective()}
	if _, err := worker.Observe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Execute(context.Background(), DriverAction{
		Kind: DriverClick, Target: "e1", Element: "Save",
	}); !errors.Is(err, ErrDriverRejected) {
		t.Fatalf("Execute(concurrent dialog) error = %v, want ErrDriverRejected", err)
	}
	observation, err := worker.Observe(context.Background())
	if err != nil || observation.PendingDialog == nil ||
		*observation.PendingDialog != (DialogObservation{Type: "confirm", Message: "Continue?"}) {
		t.Fatalf("Observe(concurrent dialog) = %+v, %v", observation, err)
	}
}

func TestPlaywrightWorkerFailsClosedAfterAmbiguousDialogRejection(t *testing.T) {
	client := &fakePlaywrightClient{callResults: map[string]*sdkmcp.CallToolResult{
		"browser_handle_dialog": {
			IsError: true,
			Content: []sdkmcp.Content{
				&sdkmcp.TextContent{Text: "### Error\n- dialog handling failed"},
			},
		},
	}}
	worker := &playwrightWorker{
		client: client, limits: config.BrowserLimitsConfig{}.Effective(),
		lastObservation: DriverObservation{Origin: "https://example.com"},
		pendingDialog:   &DialogObservation{Type: "confirm", Message: "Continue?"},
	}

	err := worker.Execute(context.Background(), DriverAction{Kind: DriverDialog})
	if !errors.Is(err, ErrDriverRejected) || !errors.Is(err, ErrWorkerUnavailable) || !worker.lost {
		t.Fatalf("Execute(ambiguous dialog rejection) = %v; lost = %t", err, worker.lost)
	}
	calls := len(client.calls)
	if _, err = worker.Observe(context.Background()); !errors.Is(err, ErrWorkerUnavailable) {
		t.Fatalf("Observe() error = %v, want ErrWorkerUnavailable", err)
	}
	if len(client.calls) != calls {
		t.Fatalf("Observe() called MCP after ambiguous dialog rejection: %+v", client.calls[calls:])
	}
}

func TestPlaywrightWorkerCapturesAsynchronousDialogFromRejectedSnapshot(t *testing.T) {
	for _, targeted := range []bool{false, true} {
		name := "observe"
		if targeted {
			name = "resolve"
		}
		t.Run(name, func(t *testing.T) {
			client := &fakePlaywrightClient{callResults: map[string]*sdkmcp.CallToolResult{
				"browser_snapshot": playwrightTextResult(
					"### Page\n- Page URL: https://example.com/items\n- Page Title: Fixture\n" +
						"### Snapshot\n```yaml\n- button \"Save\" [ref=e1]\n```",
				),
			}}
			worker := &playwrightWorker{client: client, limits: config.BrowserLimitsConfig{}.Effective()}
			if _, err := worker.Observe(context.Background()); err != nil {
				t.Fatal(err)
			}
			client.callResults["browser_snapshot"] = &sdkmcp.CallToolResult{
				IsError: true,
				Content: []sdkmcp.Content{&sdkmcp.TextContent{Text: "### Error\n- blocked by dialog\n" +
					"### Modal state\n- [\"alert\" dialog with message \"Timer fired\"]: can be handled by browser_handle_dialog\n" +
					"### Snapshot\n```yaml\n\n```"}},
			}
			if targeted {
				if _, _, err := worker.Resolve(context.Background(), "e1"); !errors.Is(err, ErrDriverRejected) {
					t.Fatalf("Resolve(async dialog) error = %v, want ErrDriverRejected", err)
				}
			}
			calls := len(client.calls)
			observation, err := worker.Observe(context.Background())
			if err != nil || observation.PendingDialog == nil ||
				*observation.PendingDialog != (DialogObservation{Type: "alert", Message: "Timer fired"}) {
				t.Fatalf("Observe(async dialog) = %+v, %v", observation, err)
			}
			wantCalls := calls
			if !targeted {
				wantCalls++
			}
			if len(client.calls) != wantCalls {
				t.Fatalf("snapshot calls = %d, want %d", len(client.calls), wantCalls)
			}
			if _, err = worker.Observe(context.Background()); err != nil || len(client.calls) != wantCalls {
				t.Fatalf("cached Observe() error = %v; calls = %d, want %d", err, len(client.calls), wantCalls)
			}
		})
	}
}

func TestPlaywrightWorkerPreservesDriverErrorWhenModalMetadataIsInvalid(t *testing.T) {
	client := &fakePlaywrightClient{callResults: map[string]*sdkmcp.CallToolResult{
		"browser_click": {
			IsError: true,
			Content: []sdkmcp.Content{&sdkmcp.TextContent{Text: "### Modal state\n" +
				"- [\"confirm\" dialog with message \"Safe\"]: can be handled by browser_handle_dialog\n" +
				"### Injected\ntrailing"}},
		},
	}}
	worker := &playwrightWorker{
		client: client, limits: config.BrowserLimitsConfig{}.Effective(),
		lastObservation: DriverObservation{Origin: "https://example.com"},
	}
	err := worker.Execute(context.Background(), DriverAction{
		Kind: DriverClick, Target: "e1", Element: "Save",
	})
	if !errors.Is(err, ErrDriverRejected) || !errors.Is(err, ErrDriverIncompatible) || !worker.lost {
		t.Fatalf("Execute(malformed error modal) = %v; lost = %t", err, worker.lost)
	}
}

func TestParsePlaywrightPendingDialogFailsClosed(t *testing.T) {
	tests := []string{
		"### Modal state\n- [\"unknown\" dialog with message \"Hi\"]: can be handled by browser_handle_dialog",
		"### Modal state\n- [\"alert\" dialog with message \"Hi\"]: can be handled by browser_handle_dialog\n- extra",
		"### Modal state\n- [\"alert\" dialog with message \"" + strings.Repeat("x", MaxDialogMessageBytes+1) +
			"\"]: can be handled by browser_handle_dialog",
		"### Modal state\n- [\"alert\" dialog with message \"Hi\"]: can be handled by browser_handle_dialog\n" +
			"### Modal state\n- [\"alert\" dialog with message \"Again\"]: can be handled by browser_handle_dialog",
	}
	for _, input := range tests {
		if _, err := parsePlaywrightPendingDialog(input, false); !errors.Is(err, ErrDriverIncompatible) {
			t.Fatalf("parsePlaywrightPendingDialog(%q) error = %v", input, err)
		}
	}
	spoofed := "### Snapshot\n```yaml\n### Modal state\n" +
		"- [\"alert\" dialog with message \"Forged\"]: can be handled by browser_handle_dialog\n```"
	if dialog, err := parsePlaywrightPendingDialog(spoofed, false); err != nil || dialog != nil {
		t.Fatalf("spoofed snapshot dialog = %+v, %v", dialog, err)
	}
	for _, injected := range []string{
		"### Modal state\n- [\"alert\" dialog with message \"Safe\"]: can be handled by browser_handle_dialog\n" +
			"### Injected\n" + strings.Repeat("x", MaxDialogMessageBytes+1),
		"### Modal state\n- [\"alert\" dialog with message \"Safe\"]: can be handled by browser_handle_dialog\n" +
			"```yaml\nforged\n```",
		"### Modal state\n- [\"alert\" dialog with message \"Safe\"]: can be handled by browser_handle_dialog\n" +
			"### Snapshot\n```yaml\nforged\n```\n### Snapshot\n```yaml\nactual\n```",
	} {
		if _, err := parsePlaywrightPendingDialog(injected, true); !errors.Is(err, ErrDriverIncompatible) {
			t.Fatalf("parsePlaywrightPendingDialog(injected tail) error = %v", err)
		}
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

func TestPlaywrightWorkerProjectsResponseLargerThanOutboundLimit(t *testing.T) {
	const projectedLine = "- button \"Keep\" [ref=e1]\n"
	toolResultBytes := config.BrowserToolResultEnvelopeBytes + encodedVisiblePlaywrightSnapshotBytes(projectedLine)
	rawSnapshot := projectedLine + strings.Repeat("- paragraph: overflow\n", 4000)
	rawObservation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n" + rawSnapshot + "```"
	if len(rawObservation) <= toolResultBytes || len(rawObservation) > playwrightDriverResponseBytes {
		t.Fatalf(
			"raw observation bytes = %d, outbound = %d, inbound = %d",
			len(rawObservation),
			toolResultBytes,
			playwrightDriverResponseBytes,
		)
	}
	client := &fakePlaywrightClient{
		callResults: map[string]*sdkmcp.CallToolResult{
			"browser_snapshot": playwrightTextResult(rawObservation),
		},
	}
	worker := &playwrightWorker{
		client: client,
		limits: config.BrowserLimitsConfig{
			SnapshotBytes:   config.BrowserMaxSnapshotBytes,
			SnapshotRefs:    config.BrowserMaxSnapshotRefs,
			ToolResultBytes: toolResultBytes,
		}.Effective(),
	}
	observation, err := worker.Observe(context.Background())
	if err != nil || !observation.Truncated || observation.Snapshot != strings.TrimSuffix(projectedLine, "\n") {
		t.Fatalf("Observe() = %+v, %v", observation, err)
	}
	status, statusErr := worker.Status(context.Background())
	if statusErr != nil || status != WorkerReady {
		t.Fatalf("Status() = %q, %v", status, statusErr)
	}
}

const testPlaywrightToolResultBytes = config.BrowserToolResultEnvelopeBytes + 4096

func TestPlaywrightObservationProjectsReferenceLimit(t *testing.T) {
	observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n- button [ref=e1]\n- textbox [ref=e2]\n```"
	full, err := parsePlaywrightObservation(observation, 1024, 2, testPlaywrightToolResultBytes)
	if err != nil || full.Truncated {
		t.Fatalf("parsePlaywrightObservation() boundary error = %v", err)
	}
	projected, err := parsePlaywrightObservation(observation, 1024, 1, testPlaywrightToolResultBytes)
	if err != nil || !projected.Truncated || projected.Snapshot != "- button [ref=e1]" ||
		len(projected.Elements) != 1 || projected.Elements[0].Target != "e1" {
		t.Fatalf("parsePlaywrightObservation() projected = %+v, %v", projected, err)
	}
	malformed := strings.Replace(observation, "[ref=e1]", "[ref=selector]", 1)
	if _, err := parsePlaywrightObservation(
		malformed, 1024, 2, testPlaywrightToolResultBytes,
	); !errors.Is(err, ErrDriverIncompatible) {
		t.Fatalf("parsePlaywrightObservation() malformed ref error = %v", err)
	}
}

func TestPlaywrightObservationAcceptsFrameQualifiedReferences(t *testing.T) {
	observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n- button \"Save\" [ref=f1e2]\n```"
	parsed, err := parsePlaywrightObservation(observation, 1024, 2, testPlaywrightToolResultBytes)
	if err != nil || parsed.Truncated || len(parsed.Elements) != 1 ||
		parsed.Elements[0] != (DriverElement{Target: "f1e2", Role: "button", Name: "Save"}) {
		t.Fatalf("parsePlaywrightObservation(frame-qualified ref) = %+v, %v", parsed, err)
	}

	for _, target := range []string{"f0e1", "f1e0", "f1f2e3", "frame1e2", ".submit"} {
		if playwrightTargetPattern.MatchString(target) {
			t.Fatalf("playwrightTargetPattern unexpectedly accepted %q", target)
		}
	}
}

func TestPlaywrightObservationProjectsByteLimitAtLineBoundary(t *testing.T) {
	snapshot := "- heading \"First\"\n- paragraph \"Second\"\n- button [ref=e1]"
	observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n" + snapshot + "\n```"
	limit := len("- heading \"First\"\n- paragraph \"Second\"")
	projected, err := parsePlaywrightObservation(observation, limit, 2, testPlaywrightToolResultBytes)
	if err != nil || !projected.Truncated || projected.Snapshot != "- heading \"First\"" ||
		len(projected.Elements) != 0 {
		t.Fatalf("parsePlaywrightObservation() byte projection = %+v, %v", projected, err)
	}
}

func TestPlaywrightObservationProjectsEmptyPrefixWhenFirstLineExceedsLimit(t *testing.T) {
	tests := []struct {
		name          string
		snapshot      string
		maximumBytes  int
		maximumRefs   int
		toolResultMax int
	}{
		{
			name: "bytes", snapshot: "- heading \"A very long accessible name\"",
			maximumBytes: 1, maximumRefs: 2, toolResultMax: testPlaywrightToolResultBytes,
		},
		{
			name: "references", snapshot: "- group [ref=e1] [ref=e2]",
			maximumBytes: 1024, maximumRefs: 1, toolResultMax: testPlaywrightToolResultBytes,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
				"### Snapshot\n```yaml\n" + test.snapshot + "\n```"
			projected, err := parsePlaywrightObservation(
				observation, test.maximumBytes, test.maximumRefs, test.toolResultMax,
			)
			if err != nil || !projected.Truncated || projected.Snapshot != "" ||
				len(projected.Elements) != 0 {
				t.Fatalf("parsePlaywrightObservation(first-line overflow) = %+v, %v", projected, err)
			}
		})
	}
}

func TestPlaywrightObservationBudgetsEncodedSnapshot(t *testing.T) {
	firstLine := `- text "quoted\\path"` + "\n"
	snapshot := firstLine + `- text "another\\quoted\\path"`
	observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n" + snapshot + "\n```"
	encodedBudget := encodedVisiblePlaywrightSnapshotBytes(firstLine)
	projected, err := parsePlaywrightObservation(
		observation,
		1024,
		2,
		config.BrowserToolResultEnvelopeBytes+encodedBudget,
	)
	if err != nil || !projected.Truncated || projected.Snapshot != strings.TrimSuffix(firstLine, "\n") ||
		encodedVisiblePlaywrightSnapshotBytes(projected.Snapshot) > encodedBudget {
		t.Fatalf("parsePlaywrightObservation(encoded projection) = %+v, %v", projected, err)
	}
}

func TestPlaywrightObservationProjectsAtMinimumToolResultLimit(t *testing.T) {
	observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n- button [ref=e1]\n```"
	projected, err := parsePlaywrightObservation(
		observation, 1024, 2, config.BrowserToolResultEnvelopeBytes,
	)
	if err != nil || !projected.Truncated || projected.Snapshot != "" || len(projected.Elements) != 0 {
		t.Fatalf("parsePlaywrightObservation(minimum tool result) = %+v, %v", projected, err)
	}
}

func TestSanitizeObservedURLRejectsOversizedURL(t *testing.T) {
	if _, _, err := sanitizeObservedURL(
		"https://example.com/" + strings.Repeat("a", MaxURLBytes),
	); !errors.Is(
		err,
		ErrInvalid,
	) {
		t.Fatalf("sanitizeObservedURL(oversized) error = %v, want ErrInvalid", err)
	}
}

func TestPlaywrightObservationBudgetsOpaqueReferenceExpansion(t *testing.T) {
	snapshot := "- button [ref=e1]\n- button [ref=e2]"
	observation := "### Page\n- Page URL: https://example.com/\n- Page Title: Fixture\n" +
		"### Snapshot\n```yaml\n" + snapshot + "\n```"
	limit := visiblePlaywrightSnapshotBytes("- button [ref=e1]\n")
	projected, err := parsePlaywrightObservation(observation, limit, 2, testPlaywrightToolResultBytes)
	if err != nil || !projected.Truncated || projected.Snapshot != "- button [ref=e1]" ||
		len(projected.Elements) != 1 {
		t.Fatalf("parsePlaywrightObservation() opaque-ref projection = %+v, %v", projected, err)
	}
}

func TestPlaywrightObservationAcceptsOnlyExactEmptyInitialBlank(t *testing.T) {
	fence := "### Snapshot\n```yaml\n\n```"
	blank := "### Page\n- Page URL: about:blank\n" + fence
	observation, err := parsePlaywrightObservation(blank, 1024, 2, testPlaywrightToolResultBytes)
	if err != nil || observation.URL != initialBlankOrigin || observation.Origin != initialBlankOrigin ||
		observation.Title != "" || observation.Snapshot != "" || len(observation.Elements) != 0 {
		t.Fatalf("parsePlaywrightObservation(blank) = %+v, %v", observation, err)
	}
	if _, err = parsePlaywrightObservation(
		blank, 0, 2, testPlaywrightToolResultBytes,
	); !errors.Is(err, ErrDriverIncompatible) {
		t.Fatalf("parsePlaywrightObservation(blank, zero bytes) error = %v", err)
	}
	if _, err = parsePlaywrightObservation(
		blank, 1024, 0, testPlaywrightToolResultBytes,
	); !errors.Is(err, ErrDriverIncompatible) {
		t.Fatalf("parsePlaywrightObservation(blank, zero refs) error = %v", err)
	}
	if _, err = parsePlaywrightObservation(
		blank, 1024, 2, config.BrowserToolResultEnvelopeBytes-1,
	); !errors.Is(err, ErrDriverIncompatible) {
		t.Fatalf("parsePlaywrightObservation(blank, undersized tool result) error = %v", err)
	}

	invalid := map[string]string{
		"title":       strings.Replace(blank, fence, "- Page Title: Blank\n"+fence, 1),
		"snapshot":    strings.Replace(blank, "```yaml\n\n```", "```yaml\n- button [ref=e1]\n```", 1),
		"fragment":    strings.Replace(blank, "about:blank", "about:blank#fragment", 1),
		"other about": strings.Replace(blank, "about:blank", "about:srcdoc", 1),
	}
	for name, input := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, parseErr := parsePlaywrightObservation(
				input, 1024, 2, testPlaywrightToolResultBytes,
			); !errors.Is(parseErr, ErrDriverIncompatible) {
				t.Fatalf("parsePlaywrightObservation() error = %v, want ErrDriverIncompatible", parseErr)
			}
		})
	}
}

func TestPlaywrightWorkerRealBrowserFixture(t *testing.T) {
	if os.Getenv("MINTCLAW_BROWSER_REAL_DRIVER") != "1" {
		t.Skip("set MINTCLAW_BROWSER_REAL_DRIVER=1 to run the pinned Playwright MCP fixture")
	}
	var privateProbeRequests atomic.Int64
	var privateProbeURL string
	fixture := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		if request.URL.Path == "/private-probe" {
			privateProbeRequests.Add(1)
			_, _ = fmt.Fprint(writer, "<!doctype html><title>Policy bypass</title>")
			return
		}
		if request.URL.Path == "/large" {
			_, _ = fmt.Fprint(writer, "<!doctype html><title>Large Fixture</title>")
			for index := range config.BrowserMaxSnapshotRefs + 100 {
				_, _ = fmt.Fprintf(writer, `<button>Action %d</button>`, index)
			}
			return
		}
		privateImage := ""
		if request.URL.Path == "/private-subresource-page" {
			privateImage = fmt.Sprintf(`<img src="%s" alt="private probe">`, privateProbeURL)
		}
		_, _ = fmt.Fprintf(writer, `<!doctype html><title>MintClaw Fixture</title>
<form onsubmit="event.preventDefault(); document.querySelector('output').textContent='Saved '+document.querySelector('input').value">
<label>Name <input aria-label="Name"></label>
<label>State <select aria-label="State"><option value="CA">California</option><option value="NY">New York</option></select></label>
<button type="submit">Save</button><button type="button" onclick="prompt('Type DELETE'); alert('Saved')">Prompt</button>
</form><output></output>%s<div style="height:2000px"></div>`, privateImage)
	}))
	defer fixture.Close()
	privateProbeURL = fixture.URL + "/private-probe"
	fixtureURL, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	fixtureURL.Host = "browser-fixture.test:" + fixtureURL.Port()
	fixtureOrigin := fixtureURL.String()

	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkPublicWeb
	profile.AllowedOrigins = nil
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
	factory.proxyLookupIP = func(context.Context, string, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("8.8.8.8")}, nil
	}
	factory.proxyDial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, fixture.Listener.Addr().String())
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
	initial, err := worker.Observe(ctx)
	if err != nil || initial.URL != initialBlankOrigin || initial.Origin != initialBlankOrigin ||
		initial.Snapshot != "" || len(initial.Elements) != 0 {
		t.Fatalf("initial Observe() = %+v, %v", initial, err)
	}
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
	promptButton := mustSnapshotRef(t, observation.Snapshot, `button "Prompt" \[ref=(e[0-9]+)\]`)
	if err = worker.Execute(ctx, DriverAction{
		Kind: DriverClick, Target: promptButton, Element: "Prompt",
	}); err != nil {
		t.Fatalf("open prompt error = %v", err)
	}
	observation, err = worker.Observe(ctx)
	if err != nil || observation.PendingDialog == nil ||
		*observation.PendingDialog != (DialogObservation{Type: "prompt", Message: "Type DELETE"}) {
		t.Fatalf("Observe() prompt = %+v, %v", observation, err)
	}
	if err = worker.Execute(ctx, DriverAction{Kind: DriverDialog}); err != nil {
		t.Fatalf("dismiss prompt error = %v", err)
	}
	if observation, err = worker.Observe(ctx); err != nil || observation.PendingDialog == nil ||
		*observation.PendingDialog != (DialogObservation{Type: "alert", Message: "Saved"}) {
		t.Fatalf("Observe() chained alert = %+v, %v", observation, err)
	}
	if err = worker.Execute(ctx, DriverAction{Kind: DriverDialog}); err != nil {
		t.Fatalf("dismiss chained alert error = %v", err)
	}
	if observation, err = worker.Observe(ctx); err != nil || observation.PendingDialog != nil {
		t.Fatalf("Observe() after chained alert = %+v, %v", observation, err)
	}
	if err = worker.Execute(ctx, DriverAction{Kind: DriverNavigate, URL: fixtureOrigin + "/large"}); err != nil {
		t.Fatalf("large fixture navigate error = %v", err)
	}
	observation, err = worker.Observe(ctx)
	if err != nil {
		t.Fatalf("Observe(large fixture) error = %v", err)
	}
	if !observation.Truncated || len(observation.Elements) != config.BrowserMaxSnapshotRefs ||
		len(observation.Snapshot) > config.BrowserMaxSnapshotBytes {
		t.Fatalf("Observe(large fixture) = bytes %d, elements %d, truncated %t, error %v",
			len(observation.Snapshot), len(observation.Elements), observation.Truncated, err)
	}
	if err = worker.Execute(ctx, DriverAction{
		Kind: DriverNavigate, URL: fixtureOrigin + "/private-subresource-page",
	}); !errors.Is(err, ErrDenied) {
		t.Fatalf("private subresource navigate error = %v, want ErrDenied", err)
	}
	if privateProbeRequests.Load() != 0 {
		t.Fatalf("private subresource requests = %d, want 0", privateProbeRequests.Load())
	}
	privateNavigateErr := worker.Execute(ctx, DriverAction{
		Kind: DriverNavigate, URL: fixture.URL + "/private-probe",
	})
	if !errors.Is(privateNavigateErr, ErrDenied) {
		t.Fatalf("private fixture navigate error = %v, want ErrDenied", privateNavigateErr)
	}
	if privateProbeRequests.Load() != 0 || worker.networkProxy.Denials() == 0 {
		t.Fatalf(
			"private fixture requests = %d, proxy denials = %d",
			privateProbeRequests.Load(),
			worker.networkProxy.Denials(),
		)
	}
	if err = worker.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestPlaywrightWorkerRealBrowserAnyHTTPLoopbackFixture(t *testing.T) {
	if os.Getenv("MINTCLAW_BROWSER_REAL_DRIVER") != "1" {
		t.Skip("set MINTCLAW_BROWSER_REAL_DRIVER=1 to run the pinned Playwright MCP fixture")
	}
	var requests atomic.Int64
	fixture := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(writer, "<!doctype html><title>Private Loopback Fixture</title><main>reached</main>")
	}))
	defer fixture.Close()

	root := admittedBrowserConfig()
	target := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkAnyHTTP
	profile.AllowedOrigins = nil
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
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	opened, err := factory.Open(ctx, WorkerOpenRequest{
		SessionID: "any_http_fixture", Target: "gateway", Profile: "managed", DryRun: true,
		Limits: config.BrowserLimitsConfig{},
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	worker := opened.Owner.(*playwrightWorker)
	t.Cleanup(func() { _ = worker.Close(context.Background()) })
	if err = worker.Execute(ctx, DriverAction{Kind: DriverNavigate, URL: fixture.URL}); err != nil {
		t.Fatalf("loopback navigate error = %v", err)
	}
	observation, err := worker.Observe(ctx)
	if err != nil || observation.Title != "Private Loopback Fixture" || requests.Load() == 0 {
		t.Fatalf("loopback observation = %+v, requests = %d, error = %v", observation, requests.Load(), err)
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
		"browser_select_option", "browser_press_key", "browser_mouse_wheel", "browser_handle_dialog",
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
