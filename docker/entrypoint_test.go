package docker_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEntrypointHonorsLocationOverrides(t *testing.T) {
	t.Run("custom home", func(t *testing.T) {
		testRoot, logPath, commandPath := prepareEntrypointTest(t)
		customHome := filepath.Join(testRoot, "custom-home")
		if err := os.MkdirAll(filepath.Join(customHome, "workspace"), 0o755); err != nil {
			t.Fatal(err)
		}
		pidPath := filepath.Join(customHome, ".mintclaw.pid")
		if err := os.WriteFile(pidPath, []byte("123\n"), 0o600); err != nil {
			t.Fatal(err)
		}

		output, err := commandPath(
			"MINTCLAW_HOME="+customHome,
			"MINTCLAW_TEST_LOG="+logPath,
		).CombinedOutput()
		if err != nil {
			t.Fatalf("entrypoint failed: %v\n%s", err, output)
		}
		if got := readLog(t, logPath); got != "gateway --debug\n" {
			t.Fatalf("command log = %q, want %q", got, "gateway --debug\n")
		}
		if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
			t.Fatalf("stale PID file was not removed: %v", err)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		testRoot, logPath, commandPath := prepareEntrypointTest(t)
		configPath := filepath.Join(testRoot, "config", "custom.json")
		if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}

		output, err := commandPath(
			"MINTCLAW_CONFIG="+configPath,
			"MINTCLAW_TEST_LOG="+logPath,
		).CombinedOutput()
		if err != nil {
			t.Fatalf("entrypoint failed: %v\n%s", err, output)
		}
		if got := readLog(t, logPath); got != "gateway --debug\n" {
			t.Fatalf("command log = %q, want %q", got, "gateway --debug\n")
		}
	})

	t.Run("first run reports custom config", func(t *testing.T) {
		testRoot, logPath, commandPath := prepareEntrypointTest(t)
		customHome := filepath.Join(testRoot, "empty-home")
		configPath := filepath.Join(testRoot, "config", "custom.json")

		output, err := commandPath(
			"MINTCLAW_HOME="+customHome,
			"MINTCLAW_CONFIG="+configPath,
			"MINTCLAW_TEST_LOG="+logPath,
		).CombinedOutput()
		if err != nil {
			t.Fatalf("entrypoint failed: %v\n%s", err, output)
		}
		if got := readLog(t, logPath); got != "onboard\n" {
			t.Fatalf("command log = %q, want %q", got, "onboard\n")
		}
		if !strings.Contains(string(output), "Edit "+configPath) {
			t.Fatalf("first-run output does not mention custom config path:\n%s", output)
		}
	})
}

func prepareEntrypointTest(t *testing.T) (string, string, func(...string) *exec.Cmd) {
	t.Helper()
	testRoot := t.TempDir()
	binDir := filepath.Join(testRoot, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeBinary := filepath.Join(binDir, "mintclaw")
	fakeSource := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"${MINTCLAW_TEST_LOG}\"\n"
	if err := os.WriteFile(fakeBinary, []byte(fakeSource), 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(testRoot, "commands.log")
	homePath := filepath.Join(testRoot, "user-home")
	if err := os.MkdirAll(homePath, 0o755); err != nil {
		t.Fatal(err)
	}

	commandPath := func(overrides ...string) *exec.Cmd {
		t.Helper()
		command := exec.Command("sh", "entrypoint.sh", "--debug")
		command.Env = append(
			filterEnvironment(os.Environ(), "HOME", "MINTCLAW_HOME", "MINTCLAW_CONFIG", "MINTCLAW_TEST_LOG", "PATH"),
			"HOME="+homePath,
			"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		)
		command.Env = append(command.Env, overrides...)
		return command
	}
	return testRoot, logPath, commandPath
}

func filterEnvironment(environment []string, keys ...string) []string {
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key] = struct{}{}
	}
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if _, found := blocked[key]; !found {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
