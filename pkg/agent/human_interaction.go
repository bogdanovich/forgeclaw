package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/logger"
	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

const (
	interactionMessageKind = "human_interaction"
	interactionIDMetadata  = "interaction_id"
	interactionShortIDMeta = "interaction_short_id"
)

type humanInteractionRuntime struct {
	al *AgentLoop
}

type InteractionEventPayload struct {
	InteractionID string                 `json:"interaction_id"`
	ShortID       string                 `json:"short_id,omitempty"`
	Kind          interactions.Kind      `json:"kind"`
	Event         interactions.EventType `json:"event"`
	Status        interactions.Status    `json:"status"`
	Outcome       interactions.Outcome   `json:"outcome,omitempty"`
	Revision      int64                  `json:"revision"`
	Code          string                 `json:"code,omitempty"`
	Success       *bool                  `json:"success,omitempty"`
}

func (al *AgentLoop) humanInteractionRuntime() *humanInteractionRuntime {
	if al == nil {
		return nil
	}
	return &humanInteractionRuntime{al: al}
}

func (al *AgentLoop) interactionRegistryForWorkspace(workspace string) *interactions.Registry {
	if al == nil {
		return nil
	}
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return nil
	}
	if existing, ok := al.interactionRegistries.Load(workspace); ok {
		registry, _ := existing.(*interactions.Registry)
		return registry
	}
	options := interactions.Options{}
	if cfg := al.GetConfig(); cfg != nil {
		options.TerminalRetention = cfg.Tools.RequestUserInput.Retention()
	}
	registry := interactions.NewRegistryWithOptions(
		interactions.WorkspaceStorePath(workspace),
		options,
	)
	actual, loaded := al.interactionRegistries.LoadOrStore(workspace, registry)
	stored, _ := actual.(*interactions.Registry)
	if stored == nil {
		stored = registry
	}
	if !loaded {
		stored.Subscribe(func(observation interactions.EventObservation) {
			al.observeInteractionEvent(workspace, observation)
		})
		if al.traceCapture != nil {
			al.traceCapture.attachInteractionRegistry(workspace, stored)
		}
		stats := stored.Stats()
		logger.InfoCF("agent", "Loaded human interaction registry", map[string]any{
			"workspace":       workspace,
			"records":         stats.RecordCount,
			"nonterminal":     stats.NonterminalCount,
			"retention_hours": int(stats.Retention / time.Hour),
			"load_error":      errString(stored.LastLoadError()),
		})
	}
	return stored
}

func (al *AgentLoop) observeInteractionEvent(
	workspace string,
	observation interactions.EventObservation,
) {
	if al == nil {
		return
	}
	al.projectInteractionTaskState(workspace, observation)
	kind := runtimeKindForInteractionEvent(observation.Event.Type)
	if kind == "" {
		return
	}
	record := observation.Record
	al.runtimeEventEmitter().emitEvent(kind, HookMeta{
		AgentID:    record.Route.AgentID,
		SessionKey: record.Route.SessionKey,
		TurnID:     record.Origin.TurnID,
		Source:     "interaction_registry",
	}, InteractionEventPayload{
		InteractionID: record.ID,
		ShortID:       record.ShortID,
		Kind:          record.Kind,
		Event:         observation.Event.Type,
		Status:        record.Status,
		Outcome:       record.Outcome,
		Revision:      record.Revision,
		Code:          observation.Event.Code,
		Success:       observation.Event.Success,
	})
}

func (al *AgentLoop) projectInteractionTaskState(
	workspace string,
	observation interactions.EventObservation,
) {
	record := observation.Record
	taskID := strings.TrimSpace(record.Origin.TaskID)
	if taskID == "" {
		return
	}
	registry := al.taskRegistryForWorkspace(workspace)
	if registry == nil {
		return
	}
	var err error
	switch observation.Event.Type {
	case interactions.EventCreated, interactions.EventWaiting:
		err = registry.MarkWaitingForInput(
			taskID,
			record.ID,
			record.ShortID,
			record.PromptSummary,
		)
	case interactions.EventAnswerClaimed, interactions.EventResumeStarted:
		err = registry.MarkInteractionRunning(taskID, record.ID)
	case interactions.EventResolved:
		switch record.Outcome {
		case interactions.OutcomeTimedOut:
			err = registry.FinishInteraction(
				taskID, record.ID, taskregistry.StatusTimedOut, "human input timed out",
			)
		case interactions.OutcomeCanceled:
			err = registry.FinishInteraction(
				taskID, record.ID, taskregistry.StatusCancelled, "human input was canceled",
			)
		}
	case interactions.EventCancelled:
		err = registry.FinishInteraction(
			taskID, record.ID, taskregistry.StatusCancelled, "human input was canceled",
		)
	case interactions.EventFailed:
		summary := strings.TrimSpace(record.FailureDetail)
		if summary == "" {
			summary = "human interaction failed"
		}
		err = registry.FinishInteraction(
			taskID, record.ID, taskregistry.StatusFailed, summary,
		)
	}
	if err != nil {
		logger.WarnCF("agent", "Failed to project human interaction task state", map[string]any{
			"workspace": workspace, "task_id": taskID,
			"interaction_id": record.ID, "event": observation.Event.Type,
			"error": err.Error(),
		})
	}
}

func runtimeKindForInteractionEvent(event interactions.EventType) runtimeevents.Kind {
	switch event {
	case interactions.EventCreated:
		return runtimeevents.KindAgentInteractionCreated
	case interactions.EventDeliveryAttempt, interactions.EventFinalDelivery:
		return runtimeevents.KindAgentInteractionDelivery
	case interactions.EventWaiting:
		return runtimeevents.KindAgentInteractionWaiting
	case interactions.EventAnswerClaimed:
		return runtimeevents.KindAgentInteractionAnswer
	case interactions.EventResumeStarted, interactions.EventApprovalConsumed,
		interactions.EventApprovalExpired,
		interactions.EventRecoveryObserved, interactions.EventCanceling:
		return runtimeevents.KindAgentInteractionResume
	case interactions.EventResolved, interactions.EventCancelled, interactions.EventFailed:
		return runtimeevents.KindAgentInteractionEnd
	default:
		return ""
	}
}

func (runtime *humanInteractionRuntime) SuspendToolCall(
	ctx context.Context,
	request ToolSuspensionRequest,
) (ToolSuspensionDisposition, error) {
	if runtime == nil || runtime.al == nil {
		return ToolSuspensionDisposition{}, interactions.ErrStoreUnavailable
	}
	registry := runtime.al.interactionRegistryForWorkspace(request.Workspace)
	if registry == nil {
		return ToolSuspensionDisposition{}, interactions.ErrStoreUnavailable
	}
	catalogLocked := false
	if runtime.al.interactionCatalog != nil {
		runtime.al.interactionCatalogMu.Lock()
		catalogLocked = true
		if err := runtime.al.interactionCatalog.Register(request.Workspace); err != nil {
			runtime.al.interactionCatalogMu.Unlock()
			return ToolSuspensionDisposition{}, fmt.Errorf(
				"register interaction workspace: %w",
				err,
			)
		}
	}
	var executionContext *bus.InboundContext
	approvalAction := ""
	if request.Prompt.Kind == interactions.KindApproval {
		executionContext = cloneInboundContext(request.ExecutionContext)
		approvalAction = request.ApprovalAction
	}
	record, err := registry.Create(interactions.CreateRequest{
		Kind:  request.Prompt.Kind,
		Route: request.Route,
		Origin: interactions.Origin{
			TurnID:                 request.Origin.TurnID,
			ToolCallID:             request.Origin.ToolCallID,
			ToolName:               request.Origin.ToolName,
			TaskID:                 request.Origin.TaskID,
			ContinuationSessionKey: request.Origin.ContinuationSessionKey,
			ArgumentHash:           request.Origin.ArgumentHash,
			ExecutionContext:       executionContext,
		},
		Questions:      request.Prompt.Questions,
		PromptSummary:  request.Prompt.PromptSummary,
		ApprovalAction: approvalAction,
		ExpiresAt:      time.Now().Add(request.Prompt.Timeout),
	})
	if catalogLocked {
		runtime.al.interactionCatalogMu.Unlock()
	}
	if err != nil {
		return ToolSuspensionDisposition{}, err
	}
	disposition := ToolSuspensionDisposition{InteractionID: record.ID, Durable: true}
	if runtime.al.channelManager == nil {
		deliveryErr := fmt.Errorf("channel manager unavailable")
		_, stateErr := registry.RecordDeliveryAttempt(
			record.ID,
			record.Revision,
			false,
			deliveryErr.Error(),
		)
		if stateErr != nil {
			return disposition, fmt.Errorf("record interaction delivery: %w", stateErr)
		}
		return disposition, deliveryErr
	}
	record, err = registry.BeginPromptDelivery(record.ID, record.Revision)
	if err != nil {
		return disposition, fmt.Errorf("begin interaction delivery: %w", err)
	}
	deliveryErr := runtime.publishPrompt(ctx, record)
	record, stateErr := registry.CompletePromptDelivery(
		record.ID,
		record.Revision,
		deliveryErr == nil,
		deliveryErr != nil && !channels.DeliveryDefinitelyNotSent(deliveryErr),
		errString(deliveryErr),
	)
	if stateErr != nil {
		return disposition, fmt.Errorf("record interaction delivery: %w", stateErr)
	}
	if deliveryErr != nil {
		return disposition, deliveryErr
	}
	if _, err := registry.MarkWaiting(record.ID, record.Revision); err != nil {
		return disposition, fmt.Errorf("mark interaction waiting: %w", err)
	}
	return disposition, nil
}

func (runtime *humanInteractionRuntime) ConsumeApproval(
	_ context.Context,
	request ToolApprovalConsumptionRequest,
) error {
	if runtime == nil || runtime.al == nil {
		return interactions.ErrStoreUnavailable
	}
	registry := runtime.al.interactionRegistryForWorkspace(request.Workspace)
	if registry == nil {
		return interactions.ErrStoreUnavailable
	}
	_, err := registry.ConsumeApproval(
		request.InteractionID,
		request.Revision,
		request.Origin.ToolCallID,
		request.Origin.ToolName,
		request.Origin.ArgumentHash,
	)
	return err
}

func (runtime *humanInteractionRuntime) publishPrompt(
	ctx context.Context,
	record interactions.Record,
) error {
	if runtime.al.channelManager == nil {
		return fmt.Errorf("channel manager unavailable")
	}
	content := renderInteractionPrompt(record)
	outboundContext := bus.InboundContext{
		Channel:   record.Route.Channel,
		Account:   record.Route.AccountID,
		ChatID:    record.Route.ChatID,
		ChatType:  record.Route.ChatType,
		TopicID:   record.Route.TopicID,
		SpaceID:   record.Route.SpaceID,
		SpaceType: record.Route.SpaceType,
		Raw: map[string]string{
			metadataKeyMessageKind: interactionMessageKind,
			interactionIDMetadata:  record.ID,
			interactionShortIDMeta: record.ShortID,
			"delivery_key":         interactionDeliveryKey(record.ID, "prompt"),
		},
	}
	return runtime.al.sendInteractionMessage(ctx, bus.OutboundMessage{
		Channel:    record.Route.Channel,
		ChatID:     record.Route.ChatID,
		Context:    outboundContext,
		AgentID:    record.Route.AgentID,
		SessionKey: record.Route.SessionKey,
		Content:    content,
	})
}

func (al *AgentLoop) sendInteractionMessage(ctx context.Context, msg bus.OutboundMessage) error {
	if al == nil || al.channelManager == nil {
		return fmt.Errorf("channel manager unavailable")
	}
	return al.channelManager.SendMessageDefiniteRetryOnly(ctx, msg)
}

func interactionDeliveryKey(interactionID, kind string) string {
	return "interaction:" + strings.TrimSpace(interactionID) + ":" + strings.TrimSpace(kind)
}

func renderInteractionPrompt(record interactions.Record) string {
	var builder strings.Builder
	if record.Kind == interactions.KindApproval {
		fmt.Fprintf(&builder, "Approval needed [%s]\n\n", record.ShortID)
		builder.WriteString("Requested action:\n")
		builder.WriteString(strings.TrimSpace(record.ApprovalAction))
		fmt.Fprintf(
			&builder,
			"\n\nReply `allow_once` to authorize this exact tool call once, or `deny`. You can also use `/answer %s <decision>`.",
			record.ShortID,
		)
		return builder.String()
	}
	fmt.Fprintf(&builder, "Input needed [%s]\n", record.ShortID)
	for index, question := range record.Questions {
		fmt.Fprintf(&builder, "\n%d. [%s] ", index+1, question.ID)
		if question.Header != "" {
			fmt.Fprintf(&builder, "%s: ", question.Header)
		}
		builder.WriteString(question.Question)
		for _, option := range question.Options {
			fmt.Fprintf(&builder, "\n   - %s: %s", option.Label, option.Description)
		}
	}
	if len(record.Questions) == 1 {
		fmt.Fprintf(&builder, "\n\nReply with your answer or `/answer %s <answer>`.", record.ShortID)
	} else {
		fmt.Fprintf(
			&builder,
			"\n\nReply with one `question_id: answer` line per question, or prefix it with `/answer %s`.",
			record.ShortID,
		)
	}
	return builder.String()
}
