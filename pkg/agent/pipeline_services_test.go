package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

type immediateDeliveryOrderCheck struct {
	t          *testing.T
	dismissed  *bool
	wasInvoked bool
}

func (d *immediateDeliveryOrderCheck) applySyncToolResultDelivery(
	_ context.Context,
	_ *turnState,
	result *tools.ToolResult,
	_ string,
) ([]providers.Attachment, *tools.ToolResult) {
	d.wasInvoked = true
	if !*d.dismissed {
		d.t.Fatal("tool feedback was not dismissed before immediate delivery")
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

func TestPipelineImmediateDeliveryDismissesToolFeedbackFirst(t *testing.T) {
	feedback := &immediateDeliveryFeedbackManager{}
	delivery := &immediateDeliveryOrderCheck{t: t, dismissed: &feedback.dismissed}
	pipeline := &Pipeline{Interaction: PipelineInteractionServices{
		ToolFeedback:     feedback,
		SyncToolDelivery: delivery,
	}}
	result := tools.UserResult("restart scheduled").WithImmediateDelivery()

	_, got := pipeline.applySyncToolResultDelivery(
		context.Background(),
		&turnState{channel: "telegram", chatID: "chat-1", opts: processOptions{InboundContext: &bus.InboundContext{}}},
		result,
		"gateway_restart",
	)

	if got != result {
		t.Fatalf("result = %#v, want original result", got)
	}
	if !delivery.wasInvoked {
		t.Fatal("sync delivery was not invoked")
	}
}
