package evolutioneval_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/evolutioneval"
)

func TestLoadManifestIsStrictAndBounded(t *testing.T) {
	manifest := testManifest()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "manifest.json")
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	loaded, err := evolutioneval.LoadManifest(path)
	if err != nil || loaded.SchemaVersion != evolutioneval.ManifestSchemaV1 {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}

	bad := strings.Replace(string(data), `"source":`, `"unknown":true,"source":`, 1)
	if writeErr := os.WriteFile(path, []byte(bad), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, err := evolutioneval.LoadManifest(path); err == nil {
		t.Fatal("expected unknown-field rejection")
	}
}
