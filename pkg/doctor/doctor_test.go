package doctor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

func writeConfig(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestRunStableOrderingAndJSONSchema(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, `{
  "version": 3,
  "gateway": {"host": "0.0.0.0"},
  "agents": {"defaults": {"workspace": "`+dir+`", "restrict_to_workspace": true, "max_tokens": 200, "context_window": 100, "summarize_token_percent": 75}},
  "model_list": [{"model_name": "a", "provider": "openai", "model": "a", "enabled": true, "fallbacks": ["missing"]}],
  "channel_list": {}
}`)

	report, err := Run(Options{ConfigPath: path})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.SchemaVersion != SchemaVersion {
		t.Fatalf("schema = %q", report.SchemaVersion)
	}
	if len(report.Findings) < 3 {
		t.Fatalf("findings = %d, want at least 3", len(report.Findings))
	}
	for idx := 1; idx < len(report.Findings); idx++ {
		prev := report.Findings[idx-1]
		next := report.Findings[idx]
		if severityRank(prev.Severity) > severityRank(next.Severity) {
			t.Fatalf("findings not severity ordered at %d: %s before %s", idx, prev.Severity, next.Severity)
		}
	}
	data, err := MarshalJSON(report)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json schema output invalid: %v", err)
	}
	if decoded["schema_version"] != SchemaVersion {
		t.Fatalf("schema_version = %v", decoded["schema_version"])
	}
}

func TestPlaintextCredentialFindingRedactsValue(t *testing.T) {
	dir := t.TempDir()
	secret := "sk-test-secret-value"
	path := writeConfig(t, dir, `{
  "version": 3,
  "agents": {"defaults": {"workspace": "`+dir+`", "restrict_to_workspace": true}},
  "model_list": [{"model_name": "a", "provider": "openai", "model": "a", "enabled": true, "api_keys": ["`+secret+`"]}],
  "channel_list": {}
}`)

	report, err := Run(Options{ConfigPath: path})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	data, err := MarshalJSON(report)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if strings.Contains(string(data), secret) {
		t.Fatalf("report leaked secret: %s", data)
	}
	if !strings.Contains(string(data), CheckPlaintextCredential) {
		t.Fatalf("report missing plaintext credential finding: %s", data)
	}
}

func TestReadOnlyDoesNotCreateBackupsOrSecurityFile(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, `{
  "version": 2,
  "agents": {"defaults": {"workspace": "`+dir+`", "restrict_to_workspace": true}},
  "model_list": [{"model_name": "a", "provider": "openai", "model": "a", "enabled": true}],
  "channels": {}
}`)

	if _, err := Run(Options{ConfigPath: path}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".bak") || entry.Name() == ".security.yml" {
			t.Fatalf("read-only doctor created %s", entry.Name())
		}
	}
}

func TestEncryptedCredentialsAreNotPlaintextFindings(t *testing.T) {
	findings := plaintextFromJSON([]byte(`{"model_list":[{"api_keys":["enc://ciphertext"]}]}`), "config.json")
	for _, finding := range findings {
		if finding.ID == CheckPlaintextCredential {
			t.Fatalf("unexpected plaintext finding for encrypted credential: %+v", finding)
		}
	}
}

func TestFileAndEnvCredentialReferencesAreNotPlaintextFindings(t *testing.T) {
	findings := plaintextFromJSON(
		[]byte(`{"token":"file:///tmp/token","password":"${PICOCLAW_PASSWORD}","auth_token":"env://TOKEN"}`),
		"config.json",
	)
	for _, finding := range findings {
		if finding.ID == CheckPlaintextCredential {
			t.Fatalf("unexpected plaintext finding for reference credential: %+v", finding)
		}
	}
}

func TestDefaultAgentFallbackReferencesAreAudited(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ModelList = config.SecureModelList{
		&config.ModelConfig{ModelName: "primary"},
	}
	cfg.Agents.Defaults.ModelName = "primary"
	cfg.Agents.Defaults.ModelFallbacks = []string{"missing"}

	findings := checkFallbacks(cfg)
	for _, finding := range findings {
		if finding.ID == CheckAgentFallbackMissing && len(finding.Evidence) == 1 &&
			finding.Evidence[0].Path == "agents.defaults.model_name" {
			return
		}
	}
	t.Fatalf("missing defaults fallback finding: %+v", findings)
}
