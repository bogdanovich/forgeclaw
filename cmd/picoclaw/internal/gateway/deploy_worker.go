package gateway

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/sipeed/picoclaw/pkg/config"
	runtimegateway "github.com/sipeed/picoclaw/pkg/gateway"
)

func newDeployWorkerCommand() *cobra.Command {
	var command, group, workspace, service, target, encodedOrigin string
	var timeoutSeconds int

	cmd := &cobra.Command{
		Use:    "deploy-worker",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			originBytes, decodeErr := base64.RawURLEncoding.DecodeString(encodedOrigin)
			if decodeErr != nil {
				return fmt.Errorf("decode deploy origin: %w", decodeErr)
			}
			var origin runtimegateway.RestartOrigin
			if unmarshalErr := json.Unmarshal(originBytes, &origin); unmarshalErr != nil {
				return fmt.Errorf("decode deploy origin: %w", unmarshalErr)
			}
			runner, err := runtimegateway.NewDeployRunner(config.GatewayDeployConfig{
				Enabled:        true,
				Group:          group,
				Command:        command,
				DefaultTarget:  target,
				AllowedTargets: []string{target},
				TimeoutSeconds: timeoutSeconds,
			}, workspace, service)
			if err != nil {
				return err
			}
			_, _, err = runner.RunHandoffWorker(cmd.Context(), target, origin)
			return err
		},
	}
	cmd.Flags().StringVar(&command, "command", "", "Configured deploy command")
	cmd.Flags().StringVar(&group, "group", "", "Configured deploy group")
	cmd.Flags().StringVar(&workspace, "workspace", "", "PicoClaw workspace path")
	cmd.Flags().StringVar(&service, "service", "", "Gateway service name")
	cmd.Flags().StringVar(&target, "target", "", "Configured deploy target")
	cmd.Flags().IntVar(&timeoutSeconds, "timeout-seconds", 0, "Deploy timeout")
	cmd.Flags().StringVar(&encodedOrigin, "origin", "", "Base64-encoded deploy origin")
	_ = cmd.MarkFlagRequired("command")
	_ = cmd.MarkFlagRequired("group")
	_ = cmd.MarkFlagRequired("workspace")
	_ = cmd.MarkFlagRequired("target")
	_ = cmd.MarkFlagRequired("origin")
	return cmd
}
