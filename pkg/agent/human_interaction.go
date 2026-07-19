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
		stored.Subscribe(al.observeInteractionEvent)
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

func (al *AgentLoop) observeInteractionEvent(observation interactions.EventObservation) {
	if al == nil {
		return
	}
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
	case interactions.EventResumeStarted, interactions.EventRecoveryObserved, interactions.EventCanceling:
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
	record, err := registry.Create(interactions.CreateRequest{
		Kind:          request.Prompt.Kind,
		Route:         request.Route,
		Origin:        request.Origin,
		Questions:     request.Prompt.Questions,
		PromptSummary: request.Prompt.PromptSummary,
		ExpiresAt:     time.Now().Add(request.Prompt.Timeout),
	})
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
	return runtime.al.channelManager.SendMessage(ctx, bus.OutboundMessage{
		Channel:    record.Route.Channel,
		ChatID:     record.Route.ChatID,
		Context:    outboundContext,
		AgentID:    record.Route.AgentID,
		SessionKey: record.Route.SessionKey,
		Content:    content,
	})
}

func interactionDeliveryKey(interactionID, kind string) string {
	return "interaction:" + strings.TrimSpace(interactionID) + ":" + strings.TrimSpace(kind)
}

func renderInteractionPrompt(record interactions.Record) string {
	var builder strings.Builder
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
