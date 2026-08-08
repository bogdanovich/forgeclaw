package nodes

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBrowserCommandDescriptorsAreTypedAndInternal(t *testing.T) {
	profile := browserProfileDescriptorFixture()
	descriptors, err := BrowserCommandDescriptors([]BrowserProfileDescriptor{profile})
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 5 {
		t.Fatalf("descriptor count = %d", len(descriptors))
	}
	for _, descriptor := range descriptors {
		if descriptor.ModelContract != nil {
			t.Fatalf("%s unexpectedly has a model contract", descriptor.Name)
		}
		if len(descriptor.BrowserProfiles) != 1 ||
			descriptor.BrowserProfiles[0].Alias != "managed" {
			t.Fatalf("%s browser profiles = %#v", descriptor.Name, descriptor.BrowserProfiles)
		}
		encoded, marshalErr := json.Marshal(descriptor)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		for _, secretField := range []string{"executable", "profile_directory", "lock_file", "endpoint"} {
			if strings.Contains(string(encoded), secretField) {
				t.Fatalf("%s descriptor leaked %q", descriptor.Name, secretField)
			}
		}
	}
	if descriptors[0].Name != BrowserCommandSessionOpen || descriptors[0].Risk != RiskWrite ||
		descriptors[1].Name != BrowserCommandSessionStatus || descriptors[1].Risk != RiskRead ||
		descriptors[2].Name != BrowserCommandObserve || descriptors[2].Risk != RiskRead ||
		descriptors[3].Name != BrowserCommandAct || descriptors[3].Risk != RiskWrite ||
		descriptors[4].Name != BrowserCommandSessionClose || descriptors[4].Risk != RiskWrite {
		t.Fatalf("descriptor order or risks = %#v", descriptors)
	}
}

func TestBrowserActSchemaBindsActionsToProfileRevision(t *testing.T) {
	profile := browserProfileDescriptorFixture()
	profile.Actions = []string{"navigate"}
	descriptors, err := BrowserCommandDescriptors([]BrowserProfileDescriptor{profile})
	if err != nil {
		t.Fatal(err)
	}
	act := descriptors[3]
	base := map[string]any{
		"session_id": "session_1", "tab_id": "tab_1", "snapshot_generation": 1,
		"action_invocation_id": "action_1", "effect": "navigation",
		"prepared_action_hash":    strings.Repeat("a", 64),
		"browser_policy_revision": strings.Repeat("b", 64),
		"profile_revision":        "managed-v1",
	}
	base["action"] = map[string]any{"kind": "navigate", "url": "https://example.com"}
	if err = validateInvocationInput(act.InputSchema, base); err != nil {
		t.Fatalf("navigate input rejected: %v", err)
	}
	base["action"] = map[string]any{"kind": "download", "ref": "ref_1"}
	if err = validateInvocationInput(act.InputSchema, base); err == nil {
		t.Fatal("act schema accepted an action absent from profile authority")
	}
	base["action"] = map[string]any{"kind": "navigate", "url": "https://example.com"}
	base["effect"] = "download"
	if err = validateInvocationInput(act.InputSchema, base); err == nil {
		t.Fatal("act schema accepted an effect that did not match the action")
	}
	base["effect"] = "navigation"
	base["method"] = "Runtime.evaluate"
	if err = validateInvocationInput(act.InputSchema, base); err == nil {
		t.Fatal("act schema accepted an extra raw driver field")
	}
}

func TestBrowserDescriptorRejectsProfileOrSchemaBroadening(t *testing.T) {
	profile := browserProfileDescriptorFixture()
	descriptors, err := BrowserCommandDescriptors([]BrowserProfileDescriptor{profile})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*CommandDescriptor)
	}{
		{
			name: "non dry run",
			mutate: func(descriptor *CommandDescriptor) {
				descriptor.BrowserProfiles[0].DryRun = false
			},
		},
		{
			name: "raw action",
			mutate: func(descriptor *CommandDescriptor) {
				descriptor.BrowserProfiles[0].Actions = []string{"evaluate"}
			},
		},
		{
			name: "model projection",
			mutate: func(descriptor *CommandDescriptor) {
				descriptor.ModelContract = &CommandModelContract{}
			},
		},
		{
			name: "schema replacement",
			mutate: func(descriptor *CommandDescriptor) {
				descriptor.InputSchema = json.RawMessage(`{"type":"object","additionalProperties":true}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			descriptor := descriptors[3]
			descriptor.BrowserProfiles = CloneBrowserProfileDescriptors(descriptor.BrowserProfiles)
			test.mutate(&descriptor)
			if err := descriptor.Validate(); err == nil {
				t.Fatal("Validate() accepted broadened browser descriptor")
			}
		})
	}
}

func browserProfileDescriptorFixture() BrowserProfileDescriptor {
	return BrowserProfileDescriptor{
		Alias: "managed", Revision: "managed-v1", Driver: "playwright_mcp",
		Mode: "managed", NetworkMode: "any_http", DryRun: true,
		Actions: []string{"download", "navigate"},
		Limits: BrowserLimits{
			Sessions: 1, Tabs: 1, SessionSeconds: 3600, IdleSeconds: 600,
			PreparedSeconds: 300, ActionSeconds: 60, SnapshotBytes: MaxBrowserSnapshotBytes,
			ScreenshotBytes: MaxBrowserScreenshotBytes,
			UploadBytes:     MaxBrowserUploadBytes, DownloadBytes: MaxBrowserDownloadBytes,
			SnapshotRefs: 500, TextInputBytes: MaxBrowserTextInputBytes,
			ToolResultBytes: MaxBrowserToolResultBytes, RetentionSecs: MaxBrowserRetentionSeconds,
		},
	}
}
