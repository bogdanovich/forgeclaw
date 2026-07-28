package companion

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestConfigNormalizesSecureEndpointAndPaths(t *testing.T) {
	baseDir := t.TempDir()
	cfg, err := (Config{
		GatewayURL: "wss://gateway.example",
		StateDir:   "state",
		TLS:        TLSConfig{CAFile: "gateway-ca.pem"},
	}).Normalize(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GatewayURL != "wss://gateway.example"+GatewayPath {
		t.Fatalf("GatewayURL = %q", cfg.GatewayURL)
	}
	if cfg.StateDir != filepath.Join(baseDir, "state") || cfg.TLS.CAFile != filepath.Join(baseDir, "gateway-ca.pem") {
		t.Fatalf("normalized paths = %q, %q", cfg.StateDir, cfg.TLS.CAFile)
	}
	if cfg.minReconnectDelay != DefaultMinReconnectDelay ||
		cfg.maxReconnectDelay != DefaultMaxReconnectDelay ||
		cfg.pendingRetryDelay != DefaultPendingRetryDelay {
		t.Fatalf(
			"normalized reconnect delays = %v, %v, %v",
			cfg.minReconnectDelay,
			cfg.maxReconnectDelay,
			cfg.pendingRetryDelay,
		)
	}
	if cfg.Policy.Revision != "default-deny" ||
		cfg.Policy.MaximumRisk != "read" ||
		len(cfg.Policy.AllowedCommands) != 0 {
		t.Fatalf("default policy = %+v", cfg.Policy)
	}
}

func TestConfigRejectsInvalidLocalPolicy(t *testing.T) {
	cfg := Config{
		GatewayURL: "wss://gateway.example",
		Policy: nodes.LocalCommandPolicy{
			Revision:          "policy-test",
			AllowedCommands:   []string{"system.exec.v1"},
			MaximumRisk:       nodes.RiskRead,
			MaxTimeoutSeconds: 30,
			MaxOutputBytes:    nodes.MaxInvocationOutput + 1,
		},
	}
	if _, err := cfg.Normalize(t.TempDir()); err == nil {
		t.Fatal("Normalize() accepted invalid local policy")
	}
}

func TestConfigKeepsOwnerShellAbsentAndDisabledByDefault(t *testing.T) {
	for _, ownerShell := range []*OwnerShellConfig{
		nil,
		{Enabled: false},
	} {
		cfg, err := (Config{
			GatewayURL: "wss://gateway.example",
			OwnerShell: ownerShell,
		}).Normalize(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if cfg.OwnerShell != nil {
			t.Fatalf("disabled owner shell survived normalization: %#v", cfg.OwnerShell)
		}
	}
}

func TestConfigRequiresExplicitOwnerShellBrokerSocket(t *testing.T) {
	baseDir := t.TempDir()
	cfg, err := (Config{
		GatewayURL: "wss://gateway.example",
		OwnerShell: &OwnerShellConfig{
			Enabled:      true,
			BrokerSocket: "authority.sock",
		},
	}).Normalize(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OwnerShell == nil ||
		cfg.OwnerShell.BrokerSocket != filepath.Join(baseDir, "authority.sock") {
		t.Fatalf("owner shell config = %#v", cfg.OwnerShell)
	}
	for _, ownerShell := range []*OwnerShellConfig{
		{Enabled: true},
		{Enabled: false, BrokerSocket: "/run/hidden.sock"},
	} {
		if _, err := (Config{
			GatewayURL: "wss://gateway.example",
			OwnerShell: ownerShell,
		}).Normalize(baseDir); err == nil {
			t.Fatalf("unsafe owner shell config accepted: %#v", ownerShell)
		}
	}
}

func TestConfigNormalizesSystemExecPolicy(t *testing.T) {
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := (Config{
		GatewayURL: "wss://gateway.example",
		SystemExec: &SystemExecPolicy{
			WorkingRoots: []string{root},
			Executables:  []string{executable},
			Environment:  []string{"HOME"},
		},
	}).Normalize(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SystemExec == nil || len(cfg.SystemExec.rootSet) != 1 ||
		len(cfg.SystemExec.executableSet) != 1 || len(cfg.SystemExec.environmentSet) != 1 {
		t.Fatalf("normalized system_exec policy = %+v", cfg.SystemExec)
	}
}

func TestConfigNormalizesSystemExecDiscoveryMetadata(t *testing.T) {
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := (Config{
		GatewayURL: "wss://gateway.example",
		Policy: nodes.LocalCommandPolicy{
			Revision:          "alias-policy",
			AllowedCommands:   []string{"system.exec.v1"},
			MaximumRisk:       nodes.RiskWrite,
			MaxTimeoutSeconds: 12,
			MaxOutputBytes:    4096,
		},
		SystemExec: &SystemExecPolicy{
			WorkingRoots: []string{root},
			Executables:  []string{executable},
			Environment:  []string{"HOME"},
			Discovery: &SystemExecDiscovery{
				ExecutableAliases:   map[string]string{"diagnostic": executable},
				WorkingScopeAliases: map[string]string{"workspace": root},
				EnvironmentNames:    []string{"HOME"},
				Guidance:            []string{"Use the bounded diagnostic alias."},
				Examples: []json.RawMessage{
					json.RawMessage(
						`{"timeout_seconds":5,"env":{},"cwd":"workspace","argv":["diagnostic","--version"]}`,
					),
				},
			},
		},
	}).Normalize(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	discovery := cfg.SystemExec.Discovery
	if discovery == nil ||
		discovery.ExecutableAliases["diagnostic"] != cfg.SystemExec.Executables[0] ||
		discovery.WorkingScopeAliases["workspace"] != cfg.SystemExec.WorkingRoots[0] ||
		len(discovery.Examples) != 1 ||
		string(discovery.Examples[0]) !=
			`{"argv":["diagnostic","--version"],"cwd":"workspace","env":{},"timeout_seconds":5}` {
		t.Fatalf("normalized discovery metadata = %#v", discovery)
	}
	contract, err := systemExecModelContract(*cfg.SystemExec, cfg.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if contract.Availability != nodes.ModelAvailable ||
		contract.AuthorityDigest == "" ||
		len(contract.Constraints.ExecutableAliases) != 1 ||
		len(contract.Constraints.WorkingScopes) != 1 {
		t.Fatalf("system.exec model contract = %#v", contract)
	}
}

func TestConfigRejectsSystemExecDiscoveryThatBroadensAuthority(t *testing.T) {
	root := t.TempDir()
	hiddenRoot := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	base := SystemExecPolicy{
		WorkingRoots: []string{root},
		Executables:  []string{executable},
		Environment:  []string{"HOME"},
	}
	tests := []struct {
		name      string
		discovery *SystemExecDiscovery
	}{
		{
			name: "hidden executable",
			discovery: &SystemExecDiscovery{
				ExecutableAliases: map[string]string{"diagnostic": filepath.Join(root, "missing")},
			},
		},
		{
			name: "hidden root",
			discovery: &SystemExecDiscovery{
				WorkingScopeAliases: map[string]string{"workspace": hiddenRoot},
			},
		},
		{
			name: "hidden environment",
			discovery: &SystemExecDiscovery{
				EnvironmentNames: []string{"SECRET_TOKEN"},
			},
		},
		{
			name: "cross-kind alias collision",
			discovery: &SystemExecDiscovery{
				ExecutableAliases:   map[string]string{"shared": executable},
				WorkingScopeAliases: map[string]string{"shared": root},
			},
		},
		{
			name: "hidden example value",
			discovery: &SystemExecDiscovery{
				ExecutableAliases:   map[string]string{"diagnostic": executable},
				WorkingScopeAliases: map[string]string{"workspace": root},
				Examples: []json.RawMessage{
					json.RawMessage(
						`{"argv":["diagnostic"],"cwd":"/hidden","timeout_seconds":5,"env":{}}`,
					),
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := base
			policy.Discovery = test.discovery
			cfg := Config{
				GatewayURL: "wss://gateway.example",
				Policy: nodes.LocalCommandPolicy{
					Revision:          "alias-policy",
					AllowedCommands:   []string{"system.exec.v1"},
					MaximumRisk:       nodes.RiskWrite,
					MaxTimeoutSeconds: 30,
					MaxOutputBytes:    4096,
				},
				SystemExec: &policy,
			}
			if _, err := cfg.Normalize(t.TempDir()); err == nil {
				t.Fatalf("Normalize() accepted authority-broadening metadata: %#v", test.discovery)
			}
		})
	}
}

func TestConfigRejectsUnsafeSystemExecPolicy(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	tests := []SystemExecPolicy{
		{Executables: []string{executable}},
		{WorkingRoots: []string{t.TempDir()}},
		{
			WorkingRoots: []string{t.TempDir()},
			Executables:  []string{executable},
			Environment:  []string{"INVALID=NAME"},
		},
	}
	for _, policy := range tests {
		cfg := Config{GatewayURL: "wss://gateway.example", SystemExec: &policy}
		if _, err := cfg.Normalize(t.TempDir()); err == nil {
			t.Fatalf("Normalize() accepted unsafe system_exec policy: %+v", policy)
		}
	}
}

func TestConfigRejectsUnsafePlaintextEndpoints(t *testing.T) {
	tests := []Config{
		{GatewayURL: "ws://gateway.example"},
		{GatewayURL: "ws://127.0.0.1:3210"},
		{GatewayURL: "ws://gateway.example", AllowLoopbackPlaintext: true},
	}
	for _, cfg := range tests {
		if _, err := cfg.Normalize(t.TempDir()); err == nil {
			t.Fatalf("Normalize(%q) accepted unsafe plaintext", cfg.GatewayURL)
		}
	}
	allowed := Config{GatewayURL: "ws://127.0.0.1:3210", AllowLoopbackPlaintext: true}
	if _, err := allowed.Normalize(t.TempDir()); err != nil {
		t.Fatalf("explicit loopback plaintext rejected: %v", err)
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{"gateway_url":"wss://gateway.example","unknown":true}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() accepted an unknown field")
	}
}

func TestConfigReconnectBounds(t *testing.T) {
	cfg := Config{
		GatewayURL: "wss://gateway.example",
		Reconnect: ReconnectConfig{
			MinDelaySeconds: 10,
			MaxDelaySeconds: 5,
		},
	}
	if _, err := cfg.Normalize(t.TempDir()); err == nil {
		t.Fatal("Normalize() accepted inverted reconnect bounds")
	}
	cfg.Reconnect.MaxDelaySeconds = int((24*time.Hour)/time.Second) + 1
	if _, err := cfg.Normalize(t.TempDir()); err == nil {
		t.Fatal("Normalize() accepted excessive reconnect delay")
	}
}
