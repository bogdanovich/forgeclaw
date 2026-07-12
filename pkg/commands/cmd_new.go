package commands

import (
	"context"
	"fmt"
	"strings"
)

func newCommand() Definition {
	return Definition{
		Name:        "new",
		Description: "Start a fresh session and clear the current goal",
		Usage:       "/new",
		Handler: func(_ context.Context, req Request, rt *Runtime) error {
			if rt == nil || rt.StartFreshSession == nil {
				return req.Reply(unavailableMsg)
			}
			if strings.TrimSpace(nthToken(req.Text, 1)) != "" {
				return req.Reply("Usage: /new")
			}

			sessionKey, err := rt.StartFreshSession()
			if err != nil {
				return req.Reply("Failed to start fresh session: " + err.Error())
			}
			return req.Reply(fmt.Sprintf(
				"Started a fresh session and cleared the current goal. Previous history was preserved. New session key: %s",
				sessionKey,
			))
		},
	}
}
