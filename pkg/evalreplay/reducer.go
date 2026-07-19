package evalreplay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func Replay(trace evaltrace.Trace) (Result, error) {
	if err := evaltrace.Validate(trace); err != nil {
		return Result{}, fmt.Errorf("validate trace: %w", err)
	}
	r := newReducer(trace)
	for _, record := range trace.Records {
		r.apply(record)
	}
	r.finish(trace)
	canonical, err := canonicalProjection(r.projection)
	if err != nil {
		return Result{}, err
	}
	return Result{Projection: r.projection, Canonical: canonical}, nil
}

func (r *reducer) apply(record evaltrace.Record) {
	r.projection.Processed++
	if record.Sequence <= r.lastSequence || record.OffsetNanos < r.lastOffset {
		r.diagnostic(
			record,
			"record_order_invalid",
			SeverityError,
			"record ordering is not monotonic",
		)
	}
	r.lastSequence, r.lastOffset = record.Sequence, record.OffsetNanos
	if record.Origin.ID != "" {
		key := record.Origin.Kind + "\x00" + record.Origin.ID
		if digest, exists := r.origins[key]; exists {
			code := "duplicate_origin"
			if digest != record.Digest {
				code = "conflicting_origin"
			}
			r.diagnostic(record, code, SeverityError, "origin was applied more than once")
			return
		}
		r.origins[key] = record.Digest
	}

	switch record.Kind {
	case evaltrace.RecordTurnStart:
		if r.turnStarted && !r.turnEnded {
			r.diagnostic(
				record,
				"turn_start_while_active",
				SeverityError,
				"turn started before prior turn ended",
			)
		}
		r.turnStarted, r.turnEnded = true, false
	case evaltrace.RecordTurnEnd, evaltrace.RecordFinalOutcome:
		if !r.turnStarted {
			r.diagnostic(
				record,
				"turn_end_without_start",
				SeverityError,
				"terminal turn record has no start",
			)
		}
		if r.turnEnded {
			r.diagnostic(
				record,
				"duplicate_turn_terminal",
				SeverityError,
				"turn has multiple terminal records",
			)
		}
		r.turnEnded = true
	case evaltrace.RecordTaskTransition:
		r.applyTask(record)
	case evaltrace.RecordInteractionTransition:
		r.applyInteraction(record)
	case evaltrace.RecordDeliveryDecision,
		evaltrace.RecordDeliveryAttempt,
		evaltrace.RecordDeliveryOutcome:
		r.applyDelivery(record)
	case evaltrace.RecordSteeringEnqueued,
		evaltrace.RecordSteeringInjected,
		evaltrace.RecordInterrupt:
		r.applySteering(record)
	case evaltrace.RecordContextCompaction,
		evaltrace.RecordContextReconciliation,
		evaltrace.RecordContextSnapshot:
		r.applyContext(record)
	case evaltrace.RecordModelRequest,
		evaltrace.RecordModelResponse,
		evaltrace.RecordModelRetry,
		evaltrace.RecordModelFallbackAttempt:
		r.applyProvider(record)
	case evaltrace.RecordToolCall,
		evaltrace.RecordToolResult,
		evaltrace.RecordToolSkipped,
		evaltrace.RecordToolLoopDecision,
		evaltrace.RecordToolSteeringDecision:
		r.applyTool(record)
	case evaltrace.RecordRestartBoundary, evaltrace.RecordInboundSpoolTransition:
		r.applyRestart(record)
	case evaltrace.RecordUserCorrection:
		r.applyCorrection(record)
	}
}

func (r *reducer) applyInteraction(record evaltrace.Record) {
	var payload evaltrace.InteractionPayload
	if !r.decode(record, &payload) {
		return
	}
	id := record.Correlation.InteractionID
	if id == "" {
		r.diagnostic(
			record,
			"interaction_id_missing",
			SeverityError,
			"interaction transition has no correlation",
		)
		return
	}
	current := r.projection.Interactions[id]
	previousOutcome := current.Outcome
	if current.Terminal {
		r.diagnostic(
			record,
			"interaction_event_after_terminal",
			SeverityError,
			"interaction changed after terminal state",
		)
	}
	if current.Status != "" && payload.From != current.Status {
		r.diagnostic(
			record,
			"interaction_from_status_mismatch",
			SeverityError,
			"interaction event source status does not match replay state",
		)
	}
	if current.Kind != "" && payload.Kind != current.Kind {
		r.diagnostic(
			record,
			"interaction_kind_changed",
			SeverityError,
			"interaction kind changed during its lifecycle",
		)
	}
	if current.TaskID != "" && record.Scope.TaskID != current.TaskID {
		r.diagnostic(
			record,
			"interaction_task_correlation_changed",
			SeverityError,
			"interaction task correlation changed",
		)
	}
	if current.ToolCallID != "" && record.Correlation.ToolCallID != current.ToolCallID {
		r.diagnostic(
			record,
			"interaction_tool_correlation_changed",
			SeverityError,
			"interaction tool-call correlation changed",
		)
	}
	if payload.Sequence <= 0 ||
		current.LastSequence > 0 && payload.Sequence <= current.LastSequence {
		r.diagnostic(
			record,
			"interaction_sequence_not_increasing",
			SeverityError,
			"interaction event sequence did not increase",
		)
	}
	if payload.Revision <= 0 ||
		current.LastRevision > 0 && payload.Revision <= current.LastRevision {
		r.diagnostic(
			record,
			"interaction_revision_not_increasing",
			SeverityError,
			"interaction revision did not increase",
		)
	}
	if payload.Status == "" {
		r.diagnostic(
			record,
			"interaction_status_missing",
			SeverityError,
			"interaction event has no resulting status",
		)
	}
	if current.Status != "" && payload.EventType != "interaction.answer_claimed" &&
		payload.EventType != "interaction.approval_expired" && payload.Outcome != previousOutcome {
		r.diagnostic(
			record,
			"interaction_outcome_changed",
			SeverityError,
			"interaction outcome changed outside an allowed transition",
		)
	}
	current.InteractionID = id
	current.Kind = firstNonEmpty(payload.Kind, current.Kind)
	current.Status = firstNonEmpty(payload.Status, current.Status)
	current.Outcome = firstNonEmpty(payload.Outcome, current.Outcome)
	current.TaskID = firstNonEmpty(record.Scope.TaskID, current.TaskID)
	current.ToolCallID = firstNonEmpty(record.Correlation.ToolCallID, current.ToolCallID)
	if payload.Revision > current.LastRevision {
		current.LastRevision = payload.Revision
	}
	if payload.Sequence > current.LastSequence {
		current.LastSequence = payload.Sequence
	}
	switch payload.EventType {
	case "interaction.created":
		current.Created++
		if current.Created > 1 || payload.From != "" || payload.Status != "created" ||
			payload.Outcome != "" || !validInteractionKind(payload.Kind) {
			r.diagnostic(
				record,
				"interaction_duplicate_or_invalid_create",
				SeverityError,
				"interaction create transition is invalid",
			)
		}
	case "interaction.delivery_attempted":
		current.PromptAttempts++
		if payload.Success != nil && *payload.Success {
			current.PromptSuccesses++
		}
		if !validInteractionDeliveryEvent(payload, "created") {
			r.invalidInteractionTransition(
				record,
				"interaction_prompt_delivery_invalid",
				"prompt delivery transition is invalid",
			)
		}
	case "interaction.waiting":
		if payload.From != "created" || payload.Status != "waiting" || current.PromptSuccesses != 1 {
			r.diagnostic(
				record,
				"interaction_waiting_transition_invalid",
				SeverityError,
				"interaction waiting transition is invalid",
			)
		}
	case "interaction.answer_claimed":
		current.AnswerClaims++
		if current.AnswerClaims > 1 {
			r.diagnostic(
				record,
				"interaction_duplicate_answer_claim",
				SeverityError,
				"interaction answer was claimed more than once",
			)
		}
		if !validInteractionAnswerClaim(payload) {
			r.diagnostic(
				record,
				"interaction_answer_transition_invalid",
				SeverityError,
				"interaction answer transition is invalid",
			)
		}
	case "interaction.resume_started":
		current.ResumeStarts++
		if payload.From != "answer_claimed" || payload.Status != "resuming" {
			r.diagnostic(
				record,
				"interaction_resume_transition_invalid",
				SeverityError,
				"interaction resume transition is invalid",
			)
		}
	case "interaction.approval_consumed":
		current.ApprovalConsumptions++
		if current.ApprovalConsumptions > 1 || current.Kind != "approval" ||
			previousOutcome != "allowed" || payload.From != "resuming" ||
			payload.Status != "resuming" || payload.Outcome != "allowed" ||
			payload.Code != "allow_once_consumed" {
			r.diagnostic(
				record,
				"interaction_approval_consumption_invalid",
				SeverityError,
				"approval consumption is invalid or repeated",
			)
		}
	case "interaction.approval_expired":
		if current.Kind != "approval" || payload.From != "resuming" ||
			payload.Status != "resuming" || previousOutcome != "allowed" ||
			payload.Outcome != "timed_out" || payload.Code != "timeout_at_approval_consumption" {
			r.diagnostic(
				record,
				"interaction_approval_expiry_invalid",
				SeverityError,
				"approval expiry transition is invalid",
			)
		}
	case "interaction.final_delivery_attempted":
		current.FinalAttempts++
		if payload.Success != nil && *payload.Success {
			current.FinalSuccesses++
		}
		if !validInteractionDeliveryEvent(payload, "resuming") {
			r.diagnostic(
				record,
				"interaction_final_delivery_invalid",
				SeverityError,
				"final delivery transition is invalid",
			)
		}
	case "interaction.canceling":
		if !validInteractionCancellationSource(payload.From) || payload.Status != "canceling" {
			r.invalidInteractionTransition(
				record,
				"interaction_canceling_transition_invalid",
				"interaction canceling transition is invalid",
			)
		}
	case "interaction.recovery_observed":
		if (payload.From != "answer_claimed" && payload.From != "resuming") ||
			payload.Status != payload.From || payload.Code != "resume_failed" {
			r.invalidInteractionTransition(
				record,
				"interaction_recovery_transition_invalid",
				"interaction recovery observation is invalid",
			)
		}
	case "interaction.resolved":
		if payload.From != "resuming" || payload.Status != "resolved" {
			r.invalidInteractionTerminalTransition(record)
		}
	case "interaction.cancelled":
		if (!validInteractionCancellationSource(payload.From) && payload.From != "canceling") ||
			payload.Status != "cancel"+"led" {
			r.invalidInteractionTerminalTransition(record)
		}
	case "interaction.failed":
		if !validInteractionTerminalSource(payload.From) || payload.Status != "failed" {
			r.invalidInteractionTerminalTransition(record)
		}
	default:
		r.diagnostic(
			record,
			"interaction_event_unknown",
			SeverityError,
			"interaction event type is unsupported",
		)
	}
	if terminalInteractionStatus(current.Status) {
		current.Terminal = true
	}
	r.projection.Interactions[id] = current
}

func (r *reducer) invalidInteractionTerminalTransition(record evaltrace.Record) {
	r.invalidInteractionTransition(
		record,
		"interaction_terminal_transition_invalid",
		"interaction terminal transition is invalid",
	)
}

func (r *reducer) invalidInteractionTransition(record evaltrace.Record, code, message string) {
	r.diagnostic(record, code, SeverityError, message)
}

func (r *reducer) applyTask(record evaltrace.Record) {
	var payload evaltrace.TaskPayload
	if !r.decode(record, &payload) {
		return
	}
	if record.Scope.TaskID == "" {
		r.diagnostic(
			record,
			"task_id_missing",
			SeverityError,
			"task transition has no task correlation",
		)
		return
	}
	current := r.projection.Tasks[record.Scope.TaskID]
	if payload.Sequence > 0 && current.LastSequence > 0 &&
		payload.Sequence <= current.LastSequence {
		r.diagnostic(
			record,
			"task_sequence_not_increasing",
			SeverityError,
			"task event sequence did not increase",
		)
	}
	if current.Terminal && payload.Status != "" && payload.Status != current.Status {
		r.diagnostic(
			record,
			"task_transition_after_terminal",
			SeverityError,
			"task status changed after terminal state",
		)
	}
	current.TaskID = record.Scope.TaskID
	if payload.Status != "" {
		current.Status = payload.Status
	}
	if payload.DeliveryStatus != "" {
		current.DeliveryStatus = payload.DeliveryStatus
	}
	if payload.Sequence > current.LastSequence {
		current.LastSequence = payload.Sequence
	}
	current.Terminal = terminalTaskStatus(current.Status)
	r.projection.Tasks[current.TaskID] = current
}

func (r *reducer) applyDelivery(record evaltrace.Record) {
	var payload evaltrace.DeliveryPayload
	if !r.decode(record, &payload) {
		return
	}
	key := deliveryKey(record, payload)
	current := r.projection.Deliveries[key]
	current.Key, current.Mode, current.TargetHash = key, firstNonEmpty(
		payload.Mode,
		current.Mode,
	), firstNonEmpty(
		payload.TargetHash,
		current.TargetHash,
	)
	current.CompletionID = firstNonEmpty(record.Correlation.CompletionID, current.CompletionID)
	switch record.Kind {
	case evaltrace.RecordDeliveryDecision:
		current.WillUser, current.WillParent = payload.WillUser, payload.WillParent
	case evaltrace.RecordDeliveryAttempt:
		if current.Terminal != "" {
			r.diagnostic(
				record,
				"delivery_attempt_after_terminal",
				SeverityError,
				"delivery attempted after terminal outcome",
			)
		}
		current.Attempts++
	case evaltrace.RecordDeliveryOutcome:
		if current.Terminal != "" {
			r.diagnostic(
				record,
				"duplicate_delivery_terminal",
				SeverityError,
				"delivery has multiple terminal outcomes",
			)
		}
		current.Terminal = payload.Status
	}
	r.projection.Deliveries[key] = current
}

func (r *reducer) applySteering(record evaltrace.Record) {
	var payload evaltrace.SteeringPayload
	if !r.decode(record, &payload) {
		return
	}
	key := payload.MessageHash
	if key == "" {
		key = record.Origin.ID
	}
	switch record.Kind {
	case evaltrace.RecordSteeringEnqueued:
		r.projection.Steering.Enqueued++
		r.projection.Steering.Pending++
		r.projection.Steering.Messages[key]++
	case evaltrace.RecordSteeringInjected:
		count := payload.Count
		if count <= 0 {
			count = 1
		}
		r.projection.Steering.Injected += count
		if count > r.projection.Steering.Pending {
			r.diagnostic(
				record,
				"steering_injected_without_enqueue",
				SeverityError,
				"steering injection count exceeds pending steering",
			)
		}
		r.projection.Steering.Pending -= count
		if r.projection.Steering.Pending < 0 {
			r.projection.Steering.Pending = 0
		}
	case evaltrace.RecordInterrupt:
		r.projection.Steering.Interrupts++
	}
}

func (r *reducer) applyContext(record evaltrace.Record) {
	var payload evaltrace.ContextPayload
	if !r.decode(record, &payload) {
		return
	}
	switch record.Kind {
	case evaltrace.RecordContextCompaction:
		r.projection.Context.Compactions++
		if payload.AfterMessages > payload.BeforeMessages && payload.BeforeMessages > 0 {
			r.diagnostic(
				record,
				"compaction_grew_context",
				SeverityWarn,
				"compaction increased message count",
			)
		}
	case evaltrace.RecordContextReconciliation:
		r.projection.Context.Reconciliations++
	case evaltrace.RecordContextSnapshot:
		r.projection.Context.LastSnapshotHash = payload.SnapshotHash
		r.projection.Context.AfterMessages = payload.AfterMessages
		r.projection.Context.ProtectedFactRefs = append([]string(nil), payload.ProtectedFactRefs...)
	}
}

func (r *reducer) applyProvider(record evaltrace.Record) {
	var payload evaltrace.ModelPayload
	if !r.decode(record, &payload) {
		return
	}
	switch record.Kind {
	case evaltrace.RecordModelRequest:
		r.projection.Providers.Requests++
	case evaltrace.RecordModelResponse:
		r.projection.Providers.Responses++
	case evaltrace.RecordModelRetry:
		r.projection.Providers.Retries++
	case evaltrace.RecordModelFallbackAttempt:
		r.projection.Providers.FallbackAttempts++
		identity := firstNonEmpty(payload.IdentityKey, payload.Provider+":"+payload.Model)
		r.projection.Providers.Attempted = append(r.projection.Providers.Attempted, identity)
		if payload.Status == "succeeded" {
			r.projection.Providers.SelectedIdentity = identity
		}
	}
}

func (r *reducer) applyTool(record evaltrace.Record) {
	var payload evaltrace.ToolPayload
	if !r.decode(record, &payload) {
		return
	}
	if record.Kind == evaltrace.RecordToolSteeringDecision {
		r.projection.Steering.ToolDecisions = append(
			r.projection.Steering.ToolDecisions,
			ToolSteeringDecision{
				Sequence: record.Sequence, Tool: payload.Tool, Action: payload.Action,
				Classification: payload.Classification, Cause: payload.Cause,
			},
		)
		return
	}
	if record.Kind == evaltrace.RecordToolLoopDecision {
		r.projection.ToolLoop.Decisions = append(
			r.projection.ToolLoop.Decisions,
			ToolLoopDecision{
				Sequence: record.Sequence, Tool: payload.Tool, Action: payload.Action,
				Code: payload.DecisionCode, Count: payload.Count, Threshold: payload.Threshold,
			},
		)
		return
	}
	callID := record.Correlation.ToolCallID
	if callID == "" {
		r.diagnostic(
			record,
			"tool_call_id_missing",
			SeverityError,
			"tool record has no call correlation",
		)
		return
	}
	current := r.projection.Tools[callID]
	current.ToolCallID = callID
	current.Tool = firstNonEmpty(payload.Tool, current.Tool)
	switch record.Kind {
	case evaltrace.RecordToolCall:
		if current.Called {
			r.diagnostic(record, "duplicate_tool_call", SeverityError, "tool call ID was reused")
		}
		current.Called = true
	case evaltrace.RecordToolResult:
		if !current.Called {
			r.diagnostic(
				record,
				"tool_result_without_call",
				SeverityError,
				"tool result has no matching call",
			)
		}
		if current.Result || current.Skipped {
			r.diagnostic(
				record,
				"duplicate_tool_resolution",
				SeverityError,
				"tool call resolved more than once",
			)
		}
		current.Result, current.Executed = true, payload.Executed
	case evaltrace.RecordToolSkipped:
		if current.Result || current.Skipped {
			r.diagnostic(
				record,
				"duplicate_tool_resolution",
				SeverityError,
				"tool call resolved more than once",
			)
		}
		current.Skipped = true
	}
	r.projection.Tools[callID] = current
}

func (r *reducer) applyRestart(record evaltrace.Record) {
	var payload evaltrace.RestartPayload
	if !r.decode(record, &payload) {
		return
	}
	r.projection.Restarts = append(
		r.projection.Restarts,
		RestartProjection{
			Sequence:  record.Sequence,
			Phase:     payload.Phase,
			Status:    payload.Status,
			StateHash: payload.StateHash,
		},
	)
}

func (r *reducer) applyCorrection(record evaltrace.Record) {
	var correction evaltrace.Correction
	if !r.decode(record, &correction) {
		return
	}
	r.appendCorrection(record.Sequence, correction, record)
}

func (r *reducer) appendCorrection(
	sequence uint64,
	correction evaltrace.Correction,
	record evaltrace.Record,
) {
	if correction.CorrectionID == "" {
		r.diagnostic(record, "correction_id_missing", SeverityError, "correction has no identifier")
		return
	}
	if _, exists := r.corrections[correction.CorrectionID]; exists {
		r.diagnostic(
			record,
			"duplicate_correction",
			SeverityError,
			"correction ID was applied more than once",
		)
		return
	}
	r.corrections[correction.CorrectionID] = struct{}{}
	r.projection.Corrections = append(r.projection.Corrections, CorrectionProjection{
		CorrectionID: correction.CorrectionID,
		Sequence:     sequence,
		RecordRefs:   append([]uint64(nil), correction.RecordRefs...),
		Category:     correction.Category,
	})
}

func (r *reducer) finish(trace evaltrace.Trace) {
	for _, correction := range trace.Corrections {
		r.appendCorrection(0, correction, evaltrace.Record{})
	}
	r.projection.Terminal = trace.Outcome != nil || r.turnEnded ||
		allTasksTerminal(r.projection.Tasks) || allInteractionsTerminal(r.projection.Interactions)
	if r.turnStarted && !r.turnEnded && trace.Outcome == nil {
		r.projection.Diagnostics = append(
			r.projection.Diagnostics,
			Diagnostic{
				Code:     "turn_not_terminal",
				Severity: SeverityError,
				Message:  "trace ended with an active turn",
			},
		)
	}
	for callID, tool := range r.projection.Tools {
		if tool.Called && !tool.Result && !tool.Skipped {
			r.projection.Diagnostics = append(
				r.projection.Diagnostics,
				Diagnostic{
					Code:     "tool_call_unresolved",
					Severity: SeverityError,
					Message:  "tool call " + callID + " has no result or skipped record",
				},
			)
		}
	}
	for id, interaction := range r.projection.Interactions {
		if interaction.Created != 1 {
			r.projection.Diagnostics = append(r.projection.Diagnostics, Diagnostic{
				Code: "interaction_create_missing", Severity: SeverityError,
				Message: "interaction " + id + " does not have exactly one create event",
			})
		}
	}
	sort.SliceStable(r.projection.Diagnostics, func(i, j int) bool {
		if r.projection.Diagnostics[i].Sequence == r.projection.Diagnostics[j].Sequence {
			return r.projection.Diagnostics[i].Code < r.projection.Diagnostics[j].Code
		}
		return r.projection.Diagnostics[i].Sequence < r.projection.Diagnostics[j].Sequence
	})
}

func (r *reducer) decode(record evaltrace.Record, target any) bool {
	if len(record.Data) == 0 {
		r.diagnostic(record, "payload_missing", SeverityError, "record requires a typed payload")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		r.diagnostic(
			record,
			"payload_invalid",
			SeverityError,
			"record payload does not match its contract",
		)
		return false
	}
	return true
}

func (r *reducer) diagnostic(
	record evaltrace.Record,
	code string,
	severity Severity,
	message string,
) {
	r.projection.Diagnostics = append(
		r.projection.Diagnostics,
		Diagnostic{
			Code:       code,
			Severity:   severity,
			Sequence:   record.Sequence,
			RecordKind: string(record.Kind),
			Message:    message,
		},
	)
}

func canonicalProjection(projection Projection) (json.RawMessage, error) {
	data, err := json.Marshal(projection)
	if err != nil {
		return nil, fmt.Errorf("encode replay projection: %w", err)
	}
	return json.RawMessage(data), nil
}

func deliveryKey(record evaltrace.Record, payload evaltrace.DeliveryPayload) string {
	parts := []string{
		record.Correlation.CompletionID,
		firstNonEmpty(payload.TargetHash, record.Scope.TargetHash),
		payload.Mode,
	}
	key := strings.Join(parts, ":")
	if strings.Trim(key, ":") == "" {
		return fmt.Sprintf("sequence:%d", record.Sequence)
	}
	return key
}

func terminalTaskStatus(status string) bool {
	switch status {
	case "succeeded",
		"failed",
		"timed_out",
		"cancel" + "led", // Persisted task status uses British spelling.
		"lost":
		return true
	default:
		return false
	}
}

func allTasksTerminal(tasks map[string]TaskProjection) bool {
	if len(tasks) == 0 {
		return false
	}
	for _, task := range tasks {
		if !task.Terminal {
			return false
		}
	}
	return true
}

func allInteractionsTerminal(interactions map[string]InteractionProjection) bool {
	if len(interactions) == 0 {
		return false
	}
	for _, interaction := range interactions {
		if !interaction.Terminal {
			return false
		}
	}
	return true
}

func terminalInteractionStatus(status string) bool {
	return status == "resolved" || status == "cancel"+"led" || status == "failed"
}

func validInteractionAnswerClaim(payload evaltrace.InteractionPayload) bool {
	if payload.Status != "answer_claimed" {
		return false
	}
	switch payload.Outcome {
	case "timed_out":
		return (payload.From == "created" || payload.From == "waiting") &&
			(payload.Code == "timeout" || payload.Code == "timeout_at_answer_claim")
	case "delivery_unknown":
		return payload.From == "created" && payload.Code == "prompt_delivery_ambiguous"
	case "answered":
		return payload.Kind == "question" && payload.From == "waiting" && payload.Code == ""
	case "allowed", "denied":
		return payload.Kind == "approval" && payload.From == "waiting" && payload.Code == ""
	default:
		return false
	}
}

func validInteractionKind(kind string) bool {
	return kind == "question" || kind == "approval"
}

func validInteractionDeliveryEvent(payload evaltrace.InteractionPayload, status string) bool {
	if payload.From != status || payload.Status != status {
		return false
	}
	switch payload.Code {
	case "":
		return payload.Success != nil
	case "delivery_started":
		return payload.Success == nil
	case "delivery_completed":
		return payload.Success != nil
	default:
		return false
	}
}

func validInteractionCancellationSource(status string) bool {
	switch status {
	case "created", "waiting", "answer_claimed", "resuming":
		return true
	default:
		return false
	}
}

func validInteractionTerminalSource(status string) bool {
	switch status {
	case "created", "waiting", "answer_claimed", "resuming", "canceling":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
