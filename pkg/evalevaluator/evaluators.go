package evalevaluator

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

const evaluatorVersionV1 = "v1"

type (
	deliveryReliability struct{}
	duplicateResponse   struct{}
	steeringCorrectness struct{}
	restartRecovery     struct{}
	durableInteraction  struct{}
	compactionRetention struct{}
	toolLoopRecovery    struct{}
	providerFailover    struct{}
)

func (deliveryReliability) Name() string { return "delivery_reliability.v1" }
func (duplicateResponse) Name() string   { return "duplicate_response.v1" }
func (steeringCorrectness) Name() string { return "steering_correctness.v1" }
func (restartRecovery) Name() string     { return "restart_recovery.v1" }
func (durableInteraction) Name() string  { return "durable_interaction.v1" }
func (compactionRetention) Name() string { return "compaction_retention.v1" }
func (toolLoopRecovery) Name() string    { return "tool_loop_recovery.v1" }
func (providerFailover) Name() string    { return "provider_failover.v1" }

func (deliveryReliability) Version() string { return evaluatorVersionV1 }
func (duplicateResponse) Version() string   { return evaluatorVersionV1 }
func (steeringCorrectness) Version() string { return evaluatorVersionV1 }
func (restartRecovery) Version() string     { return evaluatorVersionV1 }
func (durableInteraction) Version() string  { return evaluatorVersionV1 }
func (compactionRetention) Version() string { return evaluatorVersionV1 }
func (toolLoopRecovery) Version() string    { return evaluatorVersionV1 }
func (providerFailover) Version() string    { return evaluatorVersionV1 }

func (deliveryReliability) Evaluate(input Input) Finding {
	decisions := recordsOfKind(input.Trace, evaltrace.RecordDeliveryDecision)
	if len(decisions) == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"a required delivery decision",
			"no delivery decision evidence",
			"capture delivery decisions",
		)
	}
	outcomes := recordsOfKind(input.Trace, evaltrace.RecordDeliveryOutcome)
	for _, decision := range decisions {
		var payload evaltrace.DeliveryPayload
		if err := decode(decision, &payload); err != nil {
			return malformed(decision, err)
		}
		if !payload.WillUser && !payload.WillParent {
			continue
		}
		matched, matchErr := matchingDeliveryOutcomes(decision, payload, outcomes)
		if matchErr != nil {
			return malformed(matchErr.record, matchErr.err)
		}
		if len(matched) != 1 {
			return finding(
				StatusFail,
				SeverityCritical,
				"exactly one terminal outcome for required delivery",
				fmt.Sprintf("found %d matching terminal outcomes", len(matched)),
				"repair delivery settlement and idempotency",
				append([]uint64{decision.Sequence}, sequences(matched)...)...)
		}
		var outcome evaltrace.DeliveryPayload
		if err := decode(matched[0], &outcome); err != nil {
			return malformed(matched[0], err)
		}
		if !terminalDeliveryStatus(outcome.Status) {
			return finding(
				StatusFail,
				SeverityCritical,
				"sent, delivered, queued, or explicit retryable failure",
				"non-terminal status "+outcome.Status,
				"record a valid terminal delivery state",
				matched[0].Sequence,
			)
		}
	}
	return finding(
		StatusPass,
		SeverityInfo,
		"every required delivery settles once",
		"all required deliveries have one terminal outcome",
		"",
	)
}

func (duplicateResponse) Evaluate(input Input) Finding {
	outcomes := recordsOfKind(input.Trace, evaltrace.RecordDeliveryOutcome)
	if len(outcomes) == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"terminal delivery evidence",
			"no delivery outcomes",
			"capture delivery outcomes",
		)
	}
	seen := make(map[string]uint64)
	for _, record := range outcomes {
		var payload evaltrace.DeliveryPayload
		if err := decode(record, &payload); err != nil {
			return malformed(record, err)
		}
		if !successfulDeliveryStatus(payload.Status) {
			continue
		}
		key := deliveryIdentity(record, payload)
		if prior, exists := seen[key]; exists {
			return finding(
				StatusFail,
				SeverityCritical,
				"one successful delivery per completion and target",
				"duplicate successful delivery",
				"deduplicate by completion/fingerprint before publishing",
				prior,
				record.Sequence,
			)
		}
		seen[key] = record.Sequence
	}
	return finding(StatusPass, SeverityInfo, "no duplicate successful responses", "delivery identities are unique", "")
}

func (steeringCorrectness) Evaluate(input Input) Finding {
	if input.Projection.Steering.Enqueued == 0 && input.Projection.Steering.Interrupts == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"steering or interrupt evidence",
			"no steering activity",
			"capture steering events",
		)
	}
	if input.Projection.Steering.Pending != 0 ||
		input.Projection.Steering.Injected != input.Projection.Steering.Enqueued {
		return finding(
			StatusFail,
			SeverityCritical,
			"every accepted steering input injected exactly once",
			fmt.Sprintf(
				"enqueued=%d injected=%d pending=%d",
				input.Projection.Steering.Enqueued,
				input.Projection.Steering.Injected,
				input.Projection.Steering.Pending,
			),
			"fix steering queue correlation and draining",
		)
	}
	for _, diagnostic := range input.Projection.Diagnostics {
		if strings.HasPrefix(diagnostic.Code, "steering_") {
			return finding(
				StatusFail,
				SeverityCritical,
				"valid steering transitions",
				diagnostic.Code,
				"repair steering ordering",
				diagnostic.Sequence,
			)
		}
	}
	return finding(
		StatusPass,
		SeverityInfo,
		"accepted steering injected once",
		"steering queue fully and uniquely injected",
		"",
	)
}

func (restartRecovery) Evaluate(input Input) Finding {
	if len(input.Projection.Restarts) == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"restart boundary evidence",
			"no restart boundary",
			"capture restart reconciliation",
		)
	}
	for _, task := range input.Projection.Tasks {
		if !task.Terminal {
			return finding(
				StatusFail,
				SeverityCritical,
				"active tasks reconciled to terminal state",
				"task "+task.TaskID+" remains "+task.Status,
				"mark stale work lost or resume it by policy",
			)
		}
	}
	for _, diagnostic := range input.Projection.Diagnostics {
		if strings.Contains(diagnostic.Code, "task_") || strings.Contains(diagnostic.Code, "delivery_") {
			return finding(
				StatusFail,
				SeverityCritical,
				"legal idempotent restart transitions",
				diagnostic.Code,
				"repair restart reconciliation",
				diagnostic.Sequence,
			)
		}
	}
	return finding(
		StatusPass,
		SeverityInfo,
		"restart leaves terminal consistent state",
		"all observed tasks are terminal without replay diagnostics",
		"",
	)
}

func (durableInteraction) Evaluate(input Input) Finding {
	if len(input.Projection.Interactions) == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"durable interaction transition evidence",
			"no interaction activity",
			"enable trace capture while reproducing the interaction",
		)
	}
	if input.Trace.Metadata.TraceKind != evaltrace.TraceKindInteraction {
		observed := "trace kind is " + string(input.Trace.Metadata.TraceKind)
		if input.Trace.Metadata.TraceKind == "" {
			observed = "trace kind is unspecified"
		}
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"a dedicated interaction lifecycle trace",
			observed,
			"evaluate the correlated dedicated interaction trace",
		)
	}
	if input.Trace.Truncation.Incomplete {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"complete durable interaction evidence",
			"trace is marked incomplete",
			"reproduce with sufficient trace and registry limits",
		)
	}
	for _, diagnostic := range input.Projection.Diagnostics {
		if strings.HasPrefix(diagnostic.Code, "interaction_") {
			return finding(
				StatusFail,
				SeverityCritical,
				"legal, monotonic, exactly-once interaction transitions",
				diagnostic.Code,
				"inspect the correlated interaction registry events",
				diagnostic.Sequence,
			)
		}
	}
	for _, interaction := range input.Projection.Interactions {
		if !interaction.Terminal {
			if input.Trace.Outcome == nil || !terminalInteractionOutcome(input.Trace.Outcome.Status) {
				return finding(
					StatusNotEvaluable,
					SeverityInfo,
					"a complete durable interaction lifecycle",
					"interaction "+interaction.InteractionID+" continues in its lifecycle trace",
					"evaluate the terminal interaction trace",
				)
			}
			return finding(
				StatusFail,
				SeverityCritical,
				"a terminal durable interaction",
				"interaction "+interaction.InteractionID+" remains "+interaction.Status,
				"run recovery and inspect the last interaction transition",
			)
		}
		if interaction.PromptSuccesses > 1 || interaction.FinalSuccesses > 1 {
			return finding(
				StatusFail,
				SeverityCritical,
				"at most one successful prompt and final delivery",
				fmt.Sprintf(
					"interaction %s prompt_successes=%d final_successes=%d",
					interaction.InteractionID,
					interaction.PromptSuccesses,
					interaction.FinalSuccesses,
				),
				"repair interaction delivery claiming and retry policy",
			)
		}
		if interaction.AnswerClaims > 1 {
			return finding(
				StatusFail,
				SeverityCritical,
				"at most one accepted answer",
				fmt.Sprintf("interaction %s answer_claims=%d", interaction.InteractionID, interaction.AnswerClaims),
				"repair answer correlation and atomic claiming",
			)
		}
		if interaction.Kind == "approval" {
			if interaction.Outcome == "allowed" && interaction.Status != "resolved" {
				continue
			}
			wantConsumptions := 0
			if interaction.Outcome == "allowed" && interaction.Status == "resolved" {
				wantConsumptions = 1
			}
			if interaction.ApprovalConsumptions != wantConsumptions {
				return finding(
					StatusFail,
					SeverityCritical,
					fmt.Sprintf("%d approval consumptions for outcome %s", wantConsumptions, interaction.Outcome),
					fmt.Sprintf(
						"interaction %s has %d consumptions",
						interaction.InteractionID,
						interaction.ApprovalConsumptions,
					),
					"repair allow-once consumption or fail the approval closed",
				)
			}
		} else if interaction.ApprovalConsumptions != 0 {
			return finding(
				StatusFail,
				SeverityCritical,
				"question interactions never consume approvals",
				"question interaction consumed an approval",
				"separate question and approval continuation paths",
			)
		}
	}
	return finding(
		StatusPass,
		SeverityInfo,
		"terminal exactly-once durable interactions",
		"all observed interactions satisfy lifecycle and approval invariants",
		"",
	)
}

func terminalInteractionOutcome(status string) bool {
	return status == "resolved" || status == "cancel"+"led" || status == "failed"
}

func (compactionRetention) Evaluate(input Input) Finding {
	if input.Projection.Context.Compactions == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"compaction and post-compaction snapshot",
			"no compaction evidence",
			"capture compaction boundaries",
		)
	}
	refs := input.Projection.Context.ProtectedFactRefs
	if input.Projection.Context.LastSnapshotHash == "" || !contains(refs, "tool_pairing_valid:true") {
		return finding(
			StatusFail,
			SeverityCritical,
			"post-compaction snapshot with valid tool pairing",
			fmt.Sprintf("snapshot=%t protected=%v", input.Projection.Context.LastSnapshotHash != "", refs),
			"retain protected tail, goal, steering, and tool pairs",
		)
	}
	return finding(
		StatusPass,
		SeverityInfo,
		"protected context survives compaction",
		"snapshot and tool pairing evidence are present",
		"",
	)
}

func (toolLoopRecovery) Evaluate(input Input) Finding {
	decisions := input.Projection.ToolLoop.Decisions
	if len(decisions) == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"tool-loop decision evidence",
			"no loop decisions",
			"enable loop detection capture",
		)
	}
	for _, decision := range decisions {
		if decision.Action != "block" && decision.Action != "halt" {
			continue
		}
		for _, tool := range input.Projection.Tools {
			if tool.Tool == decision.Tool && tool.Executed && !tool.Skipped {
				return finding(
					StatusFail,
					SeverityCritical,
					"blocked tool calls do not execute",
					"blocked tool "+decision.Tool+" executed",
					"apply the loop decision before tool execution",
					decision.Sequence,
				)
			}
		}
	}
	return finding(StatusPass, SeverityInfo, "tool-loop decisions match execution", "no blocked tool was executed", "")
}

func (providerFailover) Evaluate(input Input) Finding {
	attempts := recordsOfKind(input.Trace, evaltrace.RecordModelFallbackAttempt)
	if len(attempts) == 0 {
		return finding(
			StatusNotEvaluable,
			SeverityInfo,
			"provider fallback attempts",
			"no fallback evidence",
			"capture provider candidate attempts",
		)
	}
	succeeded := 0
	selectedAt := -1
	for i, record := range attempts {
		var payload evaltrace.ModelPayload
		if err := decode(record, &payload); err != nil {
			return malformed(record, err)
		}
		if payload.Status == "succeeded" {
			succeeded++
			selectedAt = i
		}
	}
	if succeeded != 1 || selectedAt != len(attempts)-1 {
		return finding(
			StatusFail,
			SeverityCritical,
			"exactly one final successful candidate",
			fmt.Sprintf("successes=%d selected_index=%d attempts=%d", succeeded, selectedAt, len(attempts)),
			"stop fallback after success and preserve policy order",
			sequences(attempts)...)
	}
	return finding(StatusPass, SeverityInfo, "fallback selects one final identity", "one final candidate succeeded", "")
}

func malformed(record evaltrace.Record, err error) Finding {
	return finding(
		StatusError,
		SeverityCritical,
		"typed evaluator evidence",
		err.Error(),
		"repair or migrate trace payload",
		record.Sequence,
	)
}

func decode(record evaltrace.Record, target any) error {
	if err := json.Unmarshal(record.Data, target); err != nil {
		return fmt.Errorf("record %d payload: %w", record.Sequence, err)
	}
	return nil
}

func recordsOfKind(trace evaltrace.Trace, kind evaltrace.RecordKind) []evaltrace.Record {
	result := make([]evaltrace.Record, 0)
	for _, record := range trace.Records {
		if record.Kind == kind {
			result = append(result, record)
		}
	}
	return result
}

type recordError struct {
	record evaltrace.Record
	err    error
}

func matchingDeliveryOutcomes(
	decision evaltrace.Record,
	payload evaltrace.DeliveryPayload,
	outcomes []evaltrace.Record,
) ([]evaltrace.Record, *recordError) {
	want := deliveryIdentity(decision, payload)
	result := make([]evaltrace.Record, 0, 1)
	for _, outcome := range outcomes {
		var candidate evaltrace.DeliveryPayload
		if err := decode(outcome, &candidate); err != nil {
			return nil, &recordError{record: outcome, err: err}
		}
		if deliveryIdentity(outcome, candidate) == want {
			result = append(result, outcome)
		}
	}
	return result, nil
}

func deliveryIdentity(record evaltrace.Record, payload evaltrace.DeliveryPayload) string {
	return record.Correlation.CompletionID + "\x00" + first(
		payload.TargetHash,
		record.Scope.TargetHash,
	)
}

func terminalDeliveryStatus(status string) bool {
	return successfulDeliveryStatus(status) || status == "retryable_failed" || status == "not_applicable"
}

func successfulDeliveryStatus(status string) bool {
	switch status {
	case "sent", "delivered", "session_queued", "queued":
		return true
	default:
		return false
	}
}

func sequences(records []evaltrace.Record) []uint64 {
	result := make([]uint64, 0, len(records))
	for _, record := range records {
		result = append(result, record.Sequence)
	}
	return result
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
