package evalreplay

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDeterministicClockAndIDs(t *testing.T) {
	start := time.Date(2026, 7, 12, 20, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(start)
	if got := clock.Advance(5 * time.Second); !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("Advance() = %v", got)
	}
	ids := &SequentialIDSource{}
	if got := ids.Next("turn"); got != "turn-000001" {
		t.Fatalf("first ID = %q", got)
	}
	if got := ids.Next("turn"); got != "turn-000002" {
		t.Fatalf("second ID = %q", got)
	}
}

func TestSideEffectsAreStructurallyDenied(t *testing.T) {
	policy := SideEffectPolicy{}
	for _, effect := range []SideEffect{SideEffectNetwork, SideEffectMCP, SideEffectShell, SideEffectFilesystemMutation, SideEffectSubprocess, SideEffectGateway} {
		if err := policy.Authorize(effect); err == nil ||
			!strings.Contains(err.Error(), "structurally denied") {
			t.Fatalf("Authorize(%q) = %v", effect, err)
		}
	}
}

func TestToolCatalogExecutesOnlyRegisteredSafeStubs(t *testing.T) {
	catalog := NewToolCatalog(map[string]SafeTool{
		"fixture_lookup": func(arguments map[string]any) (map[string]any, error) {
			arguments["mutated"] = true
			return map[string]any{"status": "ok"}, nil
		},
	})
	arguments := map[string]any{"nested": map[string]any{"value": "original"}}
	result, err := catalog.Execute("fixture_lookup", arguments)
	if err != nil || result["status"] != "ok" {
		t.Fatalf("Execute() = %#v, %v", result, err)
	}
	if _, mutated := arguments["mutated"]; mutated {
		t.Fatal("safe stub mutated caller arguments")
	}
	if _, err := catalog.Execute("shell", nil); err == nil {
		t.Fatal("unknown production tool was not denied")
	}
}

func TestCheckpointStoreRestoresIsolatedCopies(t *testing.T) {
	store := NewCheckpointStore()
	state := IsolatedState{
		Tasks: map[string]json.RawMessage{"task-1": json.RawMessage(`{"status":"running"}`)},
	}
	if err := store.Save("restart-1", state); err != nil {
		t.Fatal(err)
	}
	state.Tasks["task-1"][2] = 'X'
	restored, err := store.Restore("restart-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(restored.Tasks["task-1"]) != `{"status":"running"}` {
		t.Fatalf("restored state = %s", restored.Tasks["task-1"])
	}
	restored.Tasks["task-1"][2] = 'Y'
	again, err := store.Restore("restart-1")
	if err != nil || string(again.Tasks["task-1"]) != `{"status":"running"}` {
		t.Fatalf("second restore = %s, %v", again.Tasks["task-1"], err)
	}
}
