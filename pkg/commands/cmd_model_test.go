package commands

import (
	"context"
	"testing"
)

func TestModelCommand_SetAndClear(t *testing.T) {
	info := ModelSelectionInfo{
		EffectiveName:     "gpt-5.4",
		EffectiveProvider: "openai",
		WorkspaceName:     "gpt-5.4",
		WorkspaceProvider: "openai",
	}
	rt := &Runtime{
		GetModelSelection: func() ModelSelectionInfo {
			return info
		},
		SetSessionModel: func(value string) error {
			if value != "deepseek" {
				t.Fatalf("SetSessionModel value=%q, want deepseek", value)
			}
			info = ModelSelectionInfo{
				EffectiveName:      "deepseek",
				EffectiveProvider:  "openrouter",
				WorkspaceName:      "gpt-5.4",
				WorkspaceProvider:  "openai",
				SessionOverride:    "deepseek",
				HasSessionOverride: true,
			}
			return nil
		},
		ClearSessionModel: func() error {
			info = ModelSelectionInfo{
				EffectiveName:     "gpt-5.4",
				EffectiveProvider: "openai",
				WorkspaceName:     "gpt-5.4",
				WorkspaceProvider: "openai",
			}
			return nil
		},
	}
	ex := NewExecutor(NewRegistry(BuiltinDefinitions()), rt)

	var reply string
	res := ex.Execute(context.Background(), Request{
		Text: "/model deepseek",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if res.Outcome != OutcomeHandled {
		t.Fatalf("/model deepseek outcome=%v, want handled", res.Outcome)
	}
	if reply != "Set session model override.\nCurrent Model: deepseek (Provider: openrouter)\nSession Override: deepseek\nWorkspace Default: gpt-5.4 (Provider: openai)" {
		t.Fatalf("unexpected set reply: %q", reply)
	}

	reply = ""
	res = ex.Execute(context.Background(), Request{
		Text: "/model clear",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if res.Outcome != OutcomeHandled {
		t.Fatalf("/model clear outcome=%v, want handled", res.Outcome)
	}
	if reply != "Cleared session model override.\nCurrent Model: gpt-5.4 (Provider: openai)\nWorkspace Default: gpt-5.4 (Provider: openai)\nScope: workspace default" {
		t.Fatalf("unexpected clear reply: %q", reply)
	}
}
