package agent

import (
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers"
)

type testPipelinePromptBuilder struct {
	called       bool
	activeSkills []string
	messages     []providers.Message
}

func (b *testPipelinePromptBuilder) buildTurnMessages(
	_ *turnState,
	_ []providers.Message,
	_ string,
	_ string,
	_ []string,
	activeSkills []string,
) []providers.Message {
	b.called = true
	b.activeSkills = append([]string(nil), activeSkills...)
	return append([]providers.Message(nil), b.messages...)
}

func TestPipelineBuildTurnMessages_UsesInjectedBuilder(t *testing.T) {
	builder := &testPipelinePromptBuilder{
		messages: []providers.Message{{
			Role:    "user",
			Content: "injected",
		}},
	}
	pipeline := &Pipeline{Config: PipelineConfigServices{PromptBuilder: builder}}

	got := pipeline.buildTurnMessages(nil, nil, "", "ignored", nil, []string{"skill-a"})
	if len(got) != 1 || got[0].Content != "injected" {
		t.Fatalf("buildTurnMessages() = %#v, want injected message", got)
	}
	if !builder.called {
		t.Fatal("injected prompt builder was not called")
	}
	if len(builder.activeSkills) != 1 || builder.activeSkills[0] != "skill-a" {
		t.Fatalf("activeSkills = %#v, want [skill-a]", builder.activeSkills)
	}
}

func TestPipelineBuildTurnMessages_FallsBackToConfigBuilder(t *testing.T) {
	pipeline := &Pipeline{}
	ts := &turnState{
		agent: &AgentInstance{
			ContextBuilder: NewContextBuilder(t.TempDir()),
		},
		userMessage: "hello from fallback",
	}

	got := pipeline.buildTurnMessages(ts, nil, "", ts.userMessage, nil, nil)
	if len(got) == 0 {
		t.Fatal("buildTurnMessages() returned no messages")
	}
	if !messagesContainContent(got, "hello from fallback") {
		t.Fatalf("buildTurnMessages() = %#v, want current message content", got)
	}
}

func messagesContainContent(messages []providers.Message, want string) bool {
	for _, msg := range messages {
		if strings.Contains(msg.Content, want) {
			return true
		}
		for _, block := range msg.SystemParts {
			if strings.Contains(block.Text, want) {
				return true
			}
		}
	}
	return false
}
