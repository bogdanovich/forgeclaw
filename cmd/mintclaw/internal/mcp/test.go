package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

func newTestCommand() *cobra.Command {
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "test <name>",
		Short: "Test connectivity for a configured MCP server",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}

			name := args[0]
			server, exists := cfg.Tools.MCP.Servers[name]
			if !exists {
				return fmt.Errorf("MCP server %q not found", name)
			}
			exclusiveLock := "no"
			if server.ExclusiveLockFile != "" {
				exclusiveLock = "yes"
			}
			fmt.Fprintf(
				cmd.OutOrStdout(),
				"MCP server %q configuration: session replay=%s, exclusive lock=%s.\n",
				name,
				config.EffectiveMCPSessionLossReplay(server),
				exclusiveLock,
			)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			result, err := serverProbe(ctx, name, server, cfg.WorkspacePath())
			if err != nil {
				if exclusiveLeaseBusy(err) {
					return exclusiveLeaseBusyDiagnostic(name)
				}
				return fmt.Errorf("failed to reach MCP server %q: %w", name, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "✓ MCP server %q reachable (%d tools).\n", name, result.ToolCount)
			return nil
		},
	}

	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "Connection timeout")

	return cmd
}
