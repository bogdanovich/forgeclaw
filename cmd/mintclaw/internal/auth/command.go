package auth

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

func NewAuthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage authentication (login, logout, status)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(
		newLoginCommand(),
		newLogoutCommand(),
		newStatusCommand(),
		newModelsCommand(),
		newWeixinCommand(),
		newWeComCommand(),
	)

	return cmd
}

func explicitChannelAllowFrom(values []string, public bool) (config.FlexibleStringSlice, error) {
	if public {
		return config.FlexibleStringSlice{"*"}, nil
	}

	allowFrom := config.NormalizeAllowFrom(values)
	if len(allowFrom) == 0 {
		return nil, fmt.Errorf(
			"specify at least one --allow-from sender ID, or use --public for intentional public access",
		)
	}
	return allowFrom, nil
}
