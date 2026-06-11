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
		Text: "/model use deepseek",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if res.Outcome != OutcomeHandled {
		t.Fatalf("/model use deepseek outcome=%v, want handled", res.Outcome)
	}
	if reply != "Set session model override.\nCurrent Model: deepseek (Provider: openrouter)\nSession Override: deepseek\nWorkspace Default: gpt-5.4 (Provider: openai)\n\nUse:\n- /model list\n- /model use <name>\n- /model clear" {
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
	if reply != "Cleared session model override.\nCurrent Model: gpt-5.4 (Provider: openai)\nWorkspace Default: gpt-5.4 (Provider: openai)\nScope: workspace default\n\nUse:\n- /model list\n- /model use <name>\n- /model clear" {
		t.Fatalf("unexpected clear reply: %q", reply)
	}
}

func TestModelCommand_ShowListAndUsage(t *testing.T) {
	rt := &Runtime{
		GetModelSelection: func() ModelSelectionInfo {
			return ModelSelectionInfo{
				EffectiveName:     "gpt-5.4",
				EffectiveProvider: "openai",
				WorkspaceName:     "gpt-5.4",
				WorkspaceProvider: "openai",
			}
		},
		ListModels: func() []ConfiguredModelInfo {
			return []ConfiguredModelInfo{
				{
					Name:    "gpt-5.4",
					Current: true,
					Targets: []ConfiguredModelTarget{{Provider: "openai", Model: "openai/gpt-5.4"}},
				},
				{
					Name:    "kimi",
					Targets: []ConfiguredModelTarget{{Provider: "openrouter", Model: "moonshotai/kimi-k2.6"}},
				},
			}
		},
	}
	ex := NewExecutor(NewRegistry(BuiltinDefinitions()), rt)

	var reply string
	res := ex.Execute(context.Background(), Request{
		Text: "/model",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if res.Outcome != OutcomeHandled {
		t.Fatalf("/model outcome=%v, want handled", res.Outcome)
	}
	if reply != "Current Model: gpt-5.4 (Provider: openai)\nWorkspace Default: gpt-5.4 (Provider: openai)\nScope: workspace default\n\nUse:\n- /model list\n- /model use <name>\n- /model clear" {
		t.Fatalf("unexpected /model reply: %q", reply)
	}

	reply = ""
	res = ex.Execute(context.Background(), Request{
		Text: "/model list",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if res.Outcome != OutcomeHandled {
		t.Fatalf("/model list outcome=%v, want handled", res.Outcome)
	}
	if reply != "Available Models:\n- gpt-5.4 (current)\n  - openai/gpt-5.4 via openai\n- kimi\n  - moonshotai/kimi-k2.6 via openrouter\n\nUse /model use <name> for this conversation." {
		t.Fatalf("unexpected /model list reply: %q", reply)
	}

	reply = ""
	res = ex.Execute(context.Background(), Request{
		Text: "/model deepseek",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if res.Outcome != OutcomeHandled {
		t.Fatalf("/model deepseek outcome=%v, want handled", res.Outcome)
	}
	if reply != "Usage: /model [list|use <name>|clear|default]" {
		t.Fatalf("unexpected strict usage reply: %q", reply)
	}
}
