package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyAsyncSystemMessageHelpersAreAdapterOnly(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}

	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") || path == "async_completion_legacy.go" {
			continue
		}
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		if strings.Contains(string(content), "systemFollowUpAsyncCompletionRaw") {
			t.Fatalf(
				"%s calls legacy synthetic async system-message helper; new async producers must use AsyncCompletionInput/processAsyncCompletion directly",
				path,
			)
		}
	}
}
