package commands

import (
	"context"
	"fmt"
	"strings"
)

func modelCommand() Definition {
	return Definition{
		Name:        "model",
		Description: "Show the current model, list choices, set a conversation override, or clear it",
		Usage:       "/model [list|use <name>|clear|default]",
		Handler: func(_ context.Context, req Request, rt *Runtime) error {
			if rt == nil || rt.GetModelSelection == nil {
				return req.Reply(unavailableMsg)
			}

			parts := strings.Fields(strings.TrimSpace(req.Text))
			if len(parts) <= 1 {
				return req.Reply(formatModelOverview(rt))
			}

			subcmd := strings.ToLower(strings.TrimSpace(parts[1]))
			switch subcmd {
			case "list":
				return replyWithModelList(req, rt)
			case "clear", "default":
				if rt.ClearSessionModel == nil {
					return req.Reply(unavailableMsg)
				}
				if err := rt.ClearSessionModel(); err != nil {
					return req.Reply(err.Error())
				}
				return req.Reply("Cleared session model override.\n" + formatModelOverview(rt))
			case "use":
				if len(parts) <= 2 {
					return req.Reply("Usage: /model use <name>")
				}
				return setSessionModel(req, rt, strings.TrimSpace(strings.Join(parts[2:], " ")))
			case "help":
				return req.Reply(formatModelHelp())
			default:
				return req.Reply("Usage: /model [list|use <name>|clear|default]")
			}
		},
	}
}

func setSessionModel(req Request, rt *Runtime, value string) error {
	if rt.SetSessionModel == nil {
		return req.Reply(unavailableMsg)
	}
	if err := rt.SetSessionModel(value); err != nil {
		return req.Reply(err.Error())
	}
	return req.Reply("Set session model override.\n" + formatModelOverview(rt))
}

func replyWithModelList(req Request, rt *Runtime) error {
	if rt == nil || rt.ListModels == nil {
		return req.Reply(unavailableMsg)
	}
	models := rt.ListModels()
	if len(models) == 0 {
		return req.Reply("No configured models")
	}
	return req.Reply(formatConfiguredModels(models))
}

func formatModelOverview(rt *Runtime) string {
	if rt == nil || rt.GetModelSelection == nil {
		return unavailableMsg
	}
	info := rt.GetModelSelection()
	lines := strings.Split(formatModelSelection(info), "\n")
	lines = append(
		lines,
		"",
		"Use:",
		"- /model list",
		"- /model use <name>",
		"- /model clear",
	)
	return strings.Join(lines, "\n")
}

func formatModelHelp() string {
	return strings.Join([]string{
		"/model",
		"/model list",
		"/model use <name>",
		"/model clear",
		"/model default",
	}, "\n")
}

func formatModelSelection(info ModelSelectionInfo) string {
	lines := []string{
		fmt.Sprintf("Current Model: %s (Provider: %s)", info.EffectiveName, info.EffectiveProvider),
	}
	if info.HasSessionOverride {
		lines = append(lines, fmt.Sprintf("Session Override: %s", info.SessionOverride))
	}
	if info.WorkspaceName != "" || info.WorkspaceProvider != "" {
		lines = append(
			lines,
			fmt.Sprintf("Workspace Default: %s (Provider: %s)", info.WorkspaceName, info.WorkspaceProvider),
		)
	}
	if !info.HasSessionOverride {
		lines = append(lines, "Scope: workspace default")
	}
	return strings.Join(lines, "\n")
}

func formatConfiguredModels(models []ConfiguredModelInfo) string {
	lines := make([]string, 0, len(models)+5)
	lines = append(lines, "Available Models:")
	for _, model := range models {
		label := fmt.Sprintf("- %s", model.Name)
		if model.Current {
			label += " (current)"
		}
		lines = append(lines, label)
		for _, target := range model.Targets {
			targetText := target.Model
			if target.Provider != "" {
				targetText = fmt.Sprintf("%s via %s", target.Model, target.Provider)
			}
			if target.Workspace != "" {
				targetText += fmt.Sprintf(" (workspace: %s)", target.Workspace)
			}
			if target.Count > 1 {
				targetText += fmt.Sprintf(" [x%d]", target.Count)
			}
			lines = append(lines, fmt.Sprintf("  - %s", targetText))
		}
	}
	lines = append(
		lines,
		"",
		"Use /model use <name> for this conversation.",
	)
	return strings.Join(lines, "\n")
}
