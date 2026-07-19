package interactions

import (
	"os"
	"strings"
	"testing"
)

func TestHashArgumentsCanonicalRestartStableAndSecretSafe(t *testing.T) {
	workspace := t.TempDir()
	first, err := HashArguments(workspace, map[string]any{
		"z": []any{map[string]any{"secret": "low-entropy-token", "n": 1}},
		"a": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashArguments(workspace, map[string]any{
		"a": true,
		"z": []any{map[string]any{"n": 1, "secret": "low-entropy-token"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first) != 64 || strings.Contains(first, "low-entropy-token") {
		t.Fatalf("argument hashes = %q, %q", first, second)
	}
	info, err := os.Stat(argumentHashKeyPath(workspace))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("argument hash key mode = %o", info.Mode().Perm())
	}
}

func TestHashArgumentsUsesWorkspaceScopedKey(t *testing.T) {
	args := map[string]any{"token": "same"}
	first, err := HashArguments(t.TempDir(), args)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashArguments(t.TempDir(), args)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("separate workspaces reused an approval hash key")
	}
}
