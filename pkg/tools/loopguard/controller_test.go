package loopguard

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestHashArgumentsCanonicalAndSecretSafe(t *testing.T) {
	a := map[string]any{
		"z": []any{map[string]any{"beta": "☤", "a": 1}},
		"a": map[string]any{"token": "secret-token", "x": 2},
	}
	b := map[string]any{
		"a": map[string]any{"x": 2, "token": "secret-token"},
		"z": []any{map[string]any{"a": 1, "beta": "☤"}},
	}
	if HashArguments(a) != HashArguments(b) {
		t.Fatal("equivalent nested arguments produced different hashes")
	}
	decision := New(DefaultConfig()).Before("read_file", a, SemanticsReadOnlyIdempotent)
	data, err := json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-token") || strings.Contains(string(data), "☤") ||
		len(decision.ArgsHash) != 64 {
		t.Fatalf("decision exposed raw arguments or invalid hash: %s", data)
	}
}

func TestRepeatedExactFailureWarnsThenBlocks(t *testing.T) {
	config := DefaultConfig()
	config.HardStopsEnabled = true
	controller := New(config)
	args := map[string]any{"path": "missing"}

	if got := controller.After(Observation{Tool: "read_file", Args: args, Failed: true}); got.Action != ActionAllow {
		t.Fatalf("first action = %q", got.Action)
	}
	if got := controller.After(
		Observation{Tool: "read_file", Args: args, Failed: true},
	); got.Code != "repeated_exact_failure_warning" {
		t.Fatalf("second decision = %#v", got)
	}
	for i := 0; i < 3; i++ {
		controller.After(Observation{Tool: "read_file", Args: args, Failed: true})
	}
	if got := controller.Before(
		"read_file",
		args,
		SemanticsReadOnlyIdempotent,
	); got.Code != "repeated_exact_failure_block" ||
		got.AllowsExecution() {
		t.Fatalf("block decision = %#v", got)
	}
}

func TestSameToolFailureRequiresConsecutiveCalls(t *testing.T) {
	config := DefaultConfig()
	config.SameToolFailureWarn = 2
	controller := New(config)
	controller.After(Observation{Tool: "exec", Args: map[string]any{"command": "one"}, Failed: true})
	if got := controller.After(
		Observation{Tool: "exec", Args: map[string]any{"command": "two"}, Failed: true},
	); got.Code != "same_tool_failure_warning" {
		t.Fatalf("same-tool warning = %#v", got)
	}
	controller.After(Observation{Tool: "read_file", Args: map[string]any{"path": "x"}, Failed: true})
	if got := controller.After(
		Observation{Tool: "exec", Args: map[string]any{"command": "three"}, Failed: true},
	); got.Code == "same_tool_failure_warning" {
		t.Fatalf("intervening tool did not reset streak: %#v", got)
	}
}

func TestIdenticalReadOnlyResultWarnsAndChangedResultResets(t *testing.T) {
	controller := New(DefaultConfig())
	observation := Observation{
		Tool:       "read_file",
		Args:       map[string]any{"path": "x"},
		ResultText: "same",
		Semantics:  SemanticsReadOnlyIdempotent,
	}
	if got := controller.After(observation); got.Action != ActionAllow {
		t.Fatalf("first action = %q", got.Action)
	}
	if got := controller.After(observation); got.Code != "read_only_no_progress_warning" {
		t.Fatalf("second decision = %#v", got)
	}
	observation.ResultText = "changed"
	if got := controller.After(observation); got.Action != ActionAllow {
		t.Fatalf("changed result did not reset: %#v", got)
	}
}

func TestIdenticalReadOnlyResultBlocksWhenHardStopsEnabled(t *testing.T) {
	config := DefaultConfig()
	config.HardStopsEnabled = true
	config.NoProgressBlock = 2
	controller := New(config)
	observation := Observation{
		Tool:       "read_file",
		Args:       map[string]any{"path": "x"},
		ResultText: "same",
		Semantics:  SemanticsReadOnlyIdempotent,
	}
	controller.After(observation)
	controller.After(observation)
	if got := controller.Before(
		observation.Tool,
		observation.Args,
		observation.Semantics,
	); got.Code != "read_only_no_progress_block" ||
		got.AllowsExecution() {
		t.Fatalf("no-progress block = %#v", got)
	}
}

func TestSameToolFailureHardStopWithChangingArguments(t *testing.T) {
	config := DefaultConfig()
	config.HardStopsEnabled = true
	config.ExactFailureWarn = 99
	config.SameToolFailureWarn = 1
	config.SameToolFailureHalt = 2
	controller := New(config)
	controller.After(Observation{Tool: "exec", Args: map[string]any{"command": "one"}, Failed: true})
	got := controller.After(Observation{Tool: "exec", Args: map[string]any{"command": "two"}, Failed: true})
	if got.Code != "same_tool_failure_halt" || got.Action != ActionHalt {
		t.Fatalf("same-tool halt = %#v", got)
	}
	if next := controller.Before(
		"exec",
		map[string]any{"command": "three"},
		SemanticsUnknown,
	); next.Code != "same_tool_failure_block" {
		t.Fatalf("next same-tool call = %#v", next)
	}
}

func TestMutatingAndUnknownSuccessesNeverNoProgressWarn(t *testing.T) {
	controller := New(DefaultConfig())
	for _, semantics := range []Semantics{SemanticsMutating, SemanticsUnknown} {
		for i := 0; i < 10; i++ {
			got := controller.After(
				Observation{
					Tool:       "write_file",
					Args:       map[string]any{"path": "x"},
					ResultText: "same",
					Semantics:  semantics,
				},
			)
			if got.Action != ActionAllow {
				t.Fatalf("semantics %q action = %#v", semantics, got)
			}
		}
	}
}

func TestSuccessfulCallClearsFailureCounters(t *testing.T) {
	config := DefaultConfig()
	config.HardStopsEnabled = true
	config.ExactFailureBlock = 2
	controller := New(config)
	args := map[string]any{"path": "x"}
	controller.After(Observation{Tool: "read_file", Args: args, Failed: true})
	controller.After(
		Observation{
			Tool:       "read_file",
			Args:       args,
			Failed:     false,
			ResultText: "ok",
			Semantics:  SemanticsReadOnlyIdempotent,
		},
	)
	controller.After(Observation{Tool: "read_file", Args: args, Failed: true})
	if got := controller.Before("read_file", args, SemanticsReadOnlyIdempotent); !got.AllowsExecution() {
		t.Fatalf("success did not clear exact failure count: %#v", got)
	}
}

func TestStateIsBounded(t *testing.T) {
	config := DefaultConfig()
	config.MaxSignatures = 2
	controller := New(config)
	for i := 0; i < 10; i++ {
		controller.After(Observation{Tool: "read_file", Args: map[string]any{"index": i}, Failed: true})
	}
	if len(controller.tracked) > 2 || len(controller.exactFailures) > 2 || len(controller.order) > 2 {
		t.Fatalf(
			"state not bounded: tracked=%d failures=%d order=%d",
			len(controller.tracked),
			len(controller.exactFailures),
			len(controller.order),
		)
	}
}

func TestConfigNormalizationPreservesSwitchesAndOrdersThresholds(t *testing.T) {
	config := (Config{Enabled: false, WarningsEnabled: false, HardStopsEnabled: true}).Normalized()
	if config.Enabled || config.WarningsEnabled || !config.HardStopsEnabled {
		t.Fatalf("switches changed during normalization: %#v", config)
	}
	if config.ExactFailureBlock < config.ExactFailureWarn ||
		config.SameToolFailureHalt < config.SameToolFailureWarn ||
		config.NoProgressBlock < config.NoProgressWarn || config.MaxSignatures <= 0 {
		t.Fatalf("invalid normalized thresholds: %#v", config)
	}
}
