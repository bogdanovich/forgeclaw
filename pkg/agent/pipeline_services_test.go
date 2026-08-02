package agent

import (
	"context"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

type immediateDeliveryFeedbackCheck struct {
	t          *testing.T
	dismissed  *bool
	wasInvoked bool
}

func (d *immediateDeliveryFeedbackCheck) applySyncToolResultDelivery(
	_ context.Context,
	_ *turnState,
	result *tools.ToolResult,
	_ string,
) ([]providers.Attachment, *tools.ToolResult) {
	d.wasInvoked = true
	if *d.dismissed {
		d.t.Fatal("interim delivery dismissed feedback for subsequent tools")
	}
	return nil, result
}

type immediateDeliveryFeedbackManager struct {
	dismissed bool
}

func (m *immediateDeliveryFeedbackManager) publishToolFeedbackForCall(
	context.Context,
	*turnState,
	*providers.LLMResponse,
	providers.ToolCall,
	string,
	map[string]any,
	[]providers.Message,
) {
}

func (m *immediateDeliveryFeedbackManager) dismissToolFeedbackForTurn(
	context.Context,
	*turnState,
) {
	m.dismissed = true
}

func (m *immediateDeliveryFeedbackManager) shouldPublishToolFeedback(*turnState) bool {
	return false
}

func TestPipelineInterimMessageDeliveryDoesNotDismissToolFeedback(t *testing.T) {
	feedback := &immediateDeliveryFeedbackManager{}
	delivery := &immediateDeliveryFeedbackCheck{t: t, dismissed: &feedback.dismissed}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{
		ToolFeedback:     feedback,
		SyncToolDelivery: delivery,
	}}
	result := tools.UserResult("checking services").WithImmediateDelivery()

	_, got := pipeline.applySyncToolResultDelivery(
		context.Background(),
		&turnState{channel: "telegram", chatID: "chat-1", opts: processOptions{InboundContext: &bus.InboundContext{}}},
		result,
		"message",
	)

	if got != result {
		t.Fatalf("result = %#v, want original result", got)
	}
	if !delivery.wasInvoked {
		t.Fatal("sync delivery was not invoked")
	}
}
