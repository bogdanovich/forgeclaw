package commands

import (
	"context"
	"fmt"
	"strings"
)

func modelCommand() Definition {
	return Definition{
		Name:        "model",
		Description: "Show or override the model for this conversation",
		Usage:       "/model [<name>|clear]",
		Handler: func(_ context.Context, req Request, rt *Runtime) error {
			if rt == nil || rt.GetModelSelection == nil {
				return req.Reply(unavailableMsg)
			}

			parts := strings.Fields(strings.TrimSpace(req.Text))
			arg := ""
			if len(parts) > 1 {
				arg = strings.TrimSpace(strings.Join(parts[1:], " "))
			}
			if arg == "" {
				return req.Reply(formatModelSelection(rt.GetModelSelection()))
			}

			if strings.EqualFold(arg, "clear") {
				if rt.ClearSessionModel == nil {
					return req.Reply(unavailableMsg)
				}
				if err := rt.ClearSessionModel(); err != nil {
					return req.Reply(err.Error())
				}
				return req.Reply("Cleared session model override.\n" + formatModelSelection(rt.GetModelSelection()))
			}

			if rt.SetSessionModel == nil {
				return req.Reply(unavailableMsg)
			}
			if err := rt.SetSessionModel(arg); err != nil {
				return req.Reply(err.Error())
			}
			return req.Reply("Set session model override.\n" + formatModelSelection(rt.GetModelSelection()))
		},
	}
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
