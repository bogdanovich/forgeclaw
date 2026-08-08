package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestConfigNormalizesCompanionBrowserProfileWithoutProjectingHostDetails(t *testing.T) {
	requireBrowserProfileIdentitySupport(t)
	baseDir := t.TempDir()
	profile := companionBrowserProfileFixture(t, baseDir)
	originalAgents := append([]string(nil), profile.AllowedAgents...)
	originalActions := append([]string(nil), profile.AllowedActions...)
	cfg, err := (Config{
		GatewayURL: "wss://gateway.example",
		BrowserProfiles: map[string]BrowserProfilePolicy{
			"managed": profile,
		},
	}).Normalize(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	ready := cfg.BrowserProfiles["managed"]
	if !ready.Enabled || !filepath.IsAbs(ready.DriverExecutable) ||
		!filepath.IsAbs(ready.ProfileDirectory) || !filepath.IsAbs(ready.LockFile) ||
		strings.Join(ready.AllowedAgents, ",") != "browser,marketplace" ||
		strings.Join(ready.AllowedActions, ",") != "download,navigate" {
		t.Fatalf("normalized browser profile = %#v", ready)
	}
	if strings.Join(profile.AllowedAgents, ",") != strings.Join(originalAgents, ",") ||
		strings.Join(profile.AllowedActions, ",") != strings.Join(originalActions, ",") {
		t.Fatal("Normalize() mutated caller-owned browser profile slices")
	}
	descriptors, err := browserProfileDescriptors(cfg.BrowserProfiles)
	if err != nil || len(descriptors) != 1 {
		t.Fatalf("browserProfileDescriptors() = %#v, %v", descriptors, err)
	}
	encoded, err := json.Marshal(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{
		ready.DriverExecutable, ready.ProfileDirectory, ready.LockFile,
		ready.DriverExecutableSHA256, "driver_arguments", "allowed_agents", "allowed_actors",
	} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("browser descriptor leaked companion-local value %q", private)
		}
	}
	commands, err := nodes.BrowserCommandDescriptors(descriptors)
	if err != nil || len(commands) != 5 {
		t.Fatalf("BrowserCommandDescriptors() count = %d, error = %v", len(commands), err)
	}
	for _, command := range commands {
		if command.ModelContract != nil {
			t.Fatalf("internal command %q became model-visible", command.Name)
		}
	}
}

func TestConfigRejectsUnsafeCompanionBrowserProfiles(t *testing.T) {
	requireBrowserProfileIdentitySupport(t)
	tests := []struct {
		name   string
		mutate func(*BrowserProfilePolicy, string)
		want   string
	}{
		{
			name:   "non dry run",
			mutate: func(profile *BrowserProfilePolicy, _ string) { profile.DryRun = false },
			want:   "dry_run=true",
		},
		{
			name:   "attached mode",
			mutate: func(profile *BrowserProfilePolicy, _ string) { profile.Mode = "attached_user" },
			want:   "managed mode",
		},
		{
			name: "raw evaluation action",
			mutate: func(profile *BrowserProfilePolicy, _ string) {
				profile.AllowedActions = []string{"evaluate"}
			},
			want: "unsupported action",
		},
		{
			name:   "missing actor ceiling",
			mutate: func(profile *BrowserProfilePolicy, _ string) { profile.AllowedActors = nil },
			want:   "must be non-empty",
		},
		{
			name: "wrong executable digest",
			mutate: func(profile *BrowserProfilePolicy, _ string) {
				profile.DriverExecutableSHA256 = strings.Repeat("0", sha256.Size*2)
			},
			want: "does not match",
		},
		{
			name: "profile nested lock",
			mutate: func(profile *BrowserProfilePolicy, root string) {
				profile.LockFile = filepath.Join(root, "profile", "browser.lock")
			},
			want: "outside profile_directory",
		},
		{
			name: "private exact origin",
			mutate: func(profile *BrowserProfilePolicy, _ string) {
				profile.NetworkMode = nodes.BrowserNetworkExactOrigins
				profile.AllowedOrigins = []string{"http://127.0.0.1"}
			},
			want: "outside the public network policy",
		},
		{
			name: "negative limit",
			mutate: func(profile *BrowserProfilePolicy, _ string) {
				profile.Limits.DownloadBytes = -1
			},
			want: "malformed browser limits",
		},
		{
			name: "limit expansion",
			mutate: func(profile *BrowserProfilePolicy, _ string) {
				profile.Limits.DownloadBytes = nodes.MaxBrowserDownloadBytes + 1
			},
			want: "malformed browser limits",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseDir := t.TempDir()
			profile := companionBrowserProfileFixture(t, baseDir)
			test.mutate(&profile, baseDir)
			_, err := (Config{
				GatewayURL:      "wss://gateway.example",
				BrowserProfiles: map[string]BrowserProfilePolicy{"managed": profile},
			}).Normalize(baseDir)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Normalize() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestConfigRejectsPermissiveCompanionBrowserProfileDirectory(t *testing.T) {
	requireBrowserProfileIdentitySupport(t)
	baseDir := t.TempDir()
	profile := companionBrowserProfileFixture(t, baseDir)
	if err := os.Chmod(profile.ProfileDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	_, err := (Config{
		GatewayURL:      "wss://gateway.example",
		BrowserProfiles: map[string]BrowserProfilePolicy{"managed": profile},
	}).Normalize(baseDir)
	if err == nil || !strings.Contains(err.Error(), "must not grant group or world access") {
		t.Fatalf("Normalize() permissive directory error = %v", err)
	}
}

func TestConfigKeepsCompanionBrowserProfilesDisabledByDefault(t *testing.T) {
	cfg, err := (Config{GatewayURL: "wss://gateway.example"}).Normalize(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	descriptors, err := browserProfileDescriptors(cfg.BrowserProfiles)
	if err != nil || len(descriptors) != 0 {
		t.Fatalf("browserProfileDescriptors() = %#v, %v", descriptors, err)
	}
	if _, err = (Config{
		GatewayURL: "wss://gateway.example",
		BrowserProfiles: map[string]BrowserProfilePolicy{
			"managed": {Revision: "hidden-authority"},
		},
	}).Normalize(t.TempDir()); err == nil || !strings.Contains(err.Error(), "disabled browser profile") {
		t.Fatalf("disabled authority error = %v", err)
	}
}

func companionBrowserProfileFixture(t *testing.T, baseDir string) BrowserProfilePolicy {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	profileDir := filepath.Join(baseDir, "profile")
	if err = os.Mkdir(profileDir, 0o700); err != nil {
		t.Fatal(err)
	}
	return BrowserProfilePolicy{
		Enabled: true, Revision: "managed-v1",
		AllowedAgents:    []string{"marketplace", "browser"},
		AllowedActors:    []string{"telegram:owner"},
		Driver:           nodes.BrowserDriverPlaywrightMCP,
		DriverExecutable: executable, DriverExecutableSHA256: hex.EncodeToString(digest[:]),
		DriverArguments:  []string{"--isolated"},
		ProfileDirectory: profileDir, LockFile: filepath.Join(baseDir, "browser.lock"),
		Mode: nodes.BrowserProfileManaged, NetworkMode: nodes.BrowserNetworkAnyHTTP,
		DryRun: true, AllowedActions: []string{"navigate", "download"}, Headed: true,
	}
}

func requireBrowserProfileIdentitySupport(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("browser profile identity validation is admitted for Darwin and Linux companions")
	}
}
