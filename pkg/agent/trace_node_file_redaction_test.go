package agent

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/diagnostictrace"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

func TestNodeFileDiagnosticTraceRetainsLifecycleWithoutSensitivePreviews(t *testing.T) {
	settings := traceCaptureSettings{contentMode: diagnostictrace.ContentRedacted}
	started := time.Now()
	trace := &activeTraceCapture{startedAt: started, turnID: "turn-file"}
	secretPath := "/private/operator/config.json"
	fullDigest := strings.Repeat("a", 64)

	call := traceNodeFileRecord(t, settings, trace, runtimeevents.Event{
		Kind: runtimeevents.KindAgentToolExecStart,
		Time: started.Add(time.Millisecond),
		Payload: ToolExecStartPayload{
			ToolCallID: "call-file",
			Tool:       "nodes_upload",
			Arguments: map[string]any{
				"destination":  secretPath,
				"sha256":       fullDigest,
				"artifact_ref": "media://private-source",
			},
		},
	})
	if call.ArgumentsPreview != "" || call.ArgsHash == "" {
		t.Fatalf("file call trace payload = %#v", call)
	}

	result := traceNodeFileRecord(t, settings, trace, runtimeevents.Event{
		Kind: runtimeevents.KindAgentToolExecEnd,
		Time: started.Add(2 * time.Millisecond),
		Payload: ToolExecEndPayload{
			ToolCallID:       "call-status",
			Tool:             "nodes_status",
			ResultHash:       "safe-result-hash",
			DiagnosticResult: `{"path":"` + secretPath + `","sha256":"` + fullDigest + `"}`,
		},
	})
	if result.ResultPreview != "" || result.ResultHash != "safe-result-hash" {
		t.Fatalf("file status trace payload = %#v", result)
	}

	observed := traceNodeFileRecord(t, settings, trace, runtimeevents.Event{
		Kind: runtimeevents.KindNodeInvocationObserved,
		Time: started.Add(3 * time.Millisecond),
		Source: runtimeevents.Source{
			Component: "nodes",
			Name:      "nodes_upload",
		},
		Correlation: runtimeevents.Correlation{RequestID: "call-file"},
		Payload: tools.NodeInvocationEventPayload{
			Observation:  tools.NodeInvocationObservationCompleted,
			InvocationID: "private-transfer-id",
			Target:       "personal-vpn",
			Command:      "file.upload.v1",
			Risk:         nodes.RiskWrite,
			GatewayState: nodes.GatewayInvocationDispatched,
			State:        "committed",
		},
	})
	if observed.Tool != "nodes_upload" || observed.Action != "completed" ||
		observed.Status != "committed" || observed.ResultHash == "" ||
		observed.ResultHash == "private-transfer-id" {
		t.Fatalf("file lifecycle trace payload = %#v", observed)
	}
	encoded, err := json.Marshal([]diagnostictrace.ToolPayload{call, result, observed})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		secretPath,
		fullDigest,
		"media://private-source",
		"private-transfer-id",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("file diagnostic trace leaked %q: %s", forbidden, encoded)
		}
	}
}

func traceNodeFileRecord(
	t *testing.T,
	settings traceCaptureSettings,
	trace *activeTraceCapture,
	event runtimeevents.Event,
) diagnostictrace.ToolPayload {
	t.Helper()
	record, _, ok := runtimeEventRecord(settings, trace, event)
	if !ok {
		t.Fatalf("event %q did not produce a diagnostic record", event.Kind)
	}
	var payload diagnostictrace.ToolPayload
	if err := json.Unmarshal(record.Data, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}
