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

func TestBrowserActSchemaRequiresApprovalOnlyForDownloads(t *testing.T) {
	descriptors, err := BrowserCommandDescriptors([]BrowserProfileDescriptor{
		browserProfileDescriptorFixture(),
	})
	if err != nil {
		t.Fatal(err)
	}
	act := descriptors[3]
	input := browserActInputFixture()
	input["action"] = map[string]any{"kind": "navigate", "url": "https://example.com"}
	if err = validateInvocationInput(act.InputSchema, input); err != nil {
		t.Fatalf("unapproved navigation input rejected: %v", err)
	}

	input["action"] = map[string]any{"kind": "download", "ref": "ref_1"}
	input["effect"] = "download"
	if err = validateInvocationInput(act.InputSchema, input); err == nil {
		t.Fatal("download input without approval_digest was accepted")
	}
	input["approval_digest"] = strings.Repeat("c", 64)
	if err = validateInvocationInput(act.InputSchema, input); err != nil {
		t.Fatalf("approved download input rejected: %v", err)
	}
}

func TestBrowserSessionOpenSchemaBindsProfileLimits(t *testing.T) {
	profile := browserProfileDescriptorFixture()
	profile.Limits.DownloadBytes = 1024
	descriptors, err := BrowserCommandDescriptors([]BrowserProfileDescriptor{profile})
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{
		"session_id": "session_1", "profile": "managed", "profile_revision": "managed-v1",
		"browser_policy_revision": strings.Repeat("a", 64), "dry_run": true,
		"limits": browserLimitsValue(profile.Limits),
	}
	if err = validateInvocationInput(descriptors[0].InputSchema, input); err != nil {
		t.Fatalf("profile-bounded open input rejected: %v", err)
	}
	input["limits"].(map[string]any)["download_bytes"] = 1025
	if err = validateInvocationInput(descriptors[0].InputSchema, input); err == nil {
		t.Fatal("open input above the selected profile's download ceiling was accepted")
	}
}

func TestBrowserSemanticValidationUsesUTF8ByteCeilings(t *testing.T) {
	descriptors, err := BrowserCommandDescriptors([]BrowserProfileDescriptor{
		browserProfileDescriptorFixture(),
	})
	if err != nil {
		t.Fatal(err)
	}
	input := browserActInputFixture()
	input["action"] = map[string]any{
		"kind": "navigate", "url": strings.Repeat("🙂", MaxBrowserURLBytes/4),
	}
	if err = validateDescriptorInvocationInput(descriptors[3], input); err != nil {
		t.Fatalf("navigate input at UTF-8 byte ceiling rejected: %v", err)
	}
	input["action"].(map[string]any)["url"] = strings.Repeat("🙂", MaxBrowserURLBytes/4+1)
	if err = validateDescriptorInvocationInput(descriptors[3], input); err == nil {
		t.Fatal("navigate input above UTF-8 byte ceiling was accepted")
	}

	observation := map[string]any{
		"session_id": "session_1", "tab_id": "tab_1", "snapshot_generation": 1,
		"url": "", "origin": "", "snapshot": strings.Repeat("🙂", MaxBrowserSnapshotBytes/4),
		"elements": []any{}, "truncated": false,
	}
	assertBrowserOutputValid(t, descriptors[2], observation)
	observation["snapshot"] = strings.Repeat("🙂", MaxBrowserSnapshotBytes/4+1)
	assertBrowserOutputInvalid(t, descriptors[2], observation)
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

func TestBrowserArtifactSchemasUseCapabilitySpecificCeilings(t *testing.T) {
	descriptors, err := BrowserCommandDescriptors([]BrowserProfileDescriptor{
		browserProfileDescriptorFixture(),
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := func(size int) map[string]any {
		return map[string]any{
			"transfer_id": "transfer_1", "sha256": strings.Repeat("a", 64),
			"size": size, "content_type": "image/png",
		}
	}
	observation := map[string]any{
		"session_id": "session_1", "tab_id": "tab_1", "snapshot_generation": 1,
		"url": "", "origin": "", "snapshot": "", "elements": []any{}, "truncated": false,
		"screenshot": artifact(MaxBrowserScreenshotBytes),
	}
	assertBrowserOutputValid(t, descriptors[2], observation)
	observation["screenshot"] = artifact(MaxBrowserScreenshotBytes + 1)
	assertBrowserOutputInvalid(t, descriptors[2], observation)

	action := map[string]any{
		"action_invocation_id": "action_1", "state": "succeeded",
		"artifact": artifact(MaxBrowserDownloadBytes),
	}
	assertBrowserOutputValid(t, descriptors[3], action)
	action["artifact"] = artifact(MaxBrowserDownloadBytes + 1)
	assertBrowserOutputInvalid(t, descriptors[3], action)
}

func assertBrowserOutputValid(t *testing.T, descriptor CommandDescriptor, value map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateInvocationOutput(descriptor, encoded, MaxInvocationOutput); err != nil {
		t.Fatalf("ValidateInvocationOutput() error = %v", err)
	}
}

func assertBrowserOutputInvalid(t *testing.T, descriptor CommandDescriptor, value map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateInvocationOutput(descriptor, encoded, MaxInvocationOutput); err == nil {
		t.Fatal("ValidateInvocationOutput() accepted an oversized artifact")
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

func browserActInputFixture() map[string]any {
	return map[string]any{
		"session_id": "session_1", "tab_id": "tab_1", "snapshot_generation": 1,
		"action_invocation_id": "action_1", "effect": "navigation",
		"prepared_action_hash":    strings.Repeat("a", 64),
		"browser_policy_revision": strings.Repeat("b", 64),
		"profile_revision":        "managed-v1",
	}
}

func browserLimitsValue(limits BrowserLimits) map[string]any {
	return map[string]any{
		"sessions": limits.Sessions, "tabs": limits.Tabs,
		"session_seconds": limits.SessionSeconds, "idle_seconds": limits.IdleSeconds,
		"prepared_seconds": limits.PreparedSeconds, "action_seconds": limits.ActionSeconds,
		"snapshot_bytes": limits.SnapshotBytes, "screenshot_bytes": limits.ScreenshotBytes,
		"upload_bytes": limits.UploadBytes, "download_bytes": limits.DownloadBytes,
		"snapshot_refs": limits.SnapshotRefs, "text_input_bytes": limits.TextInputBytes,
		"tool_result_bytes": limits.ToolResultBytes, "retention_seconds": limits.RetentionSecs,
	}
}
