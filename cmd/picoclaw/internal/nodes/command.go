package nodes

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/sipeed/picoclaw/cmd/picoclaw/internal"
	"github.com/sipeed/picoclaw/pkg/config"
	nodepkg "github.com/sipeed/picoclaw/pkg/nodes"
)

type commandDeps struct {
	configPath func() string
	now        func() time.Time
}

type registrationView struct {
	Node            nodepkg.Snapshot `json:"node"`
	PublicKeySHA256 string           `json:"public_key_sha256,omitempty"`
	RequestedRole   string           `json:"requested_role,omitempty"`
	RequestedAt     int64            `json:"requested_at,omitempty"`
	AllowedCommands []string         `json:"allowed_commands"`
	ApprovedAt      int64            `json:"approved_at,omitempty"`
	RevokedAt       int64            `json:"revoked_at,omitempty"`
}

func NewNodesCommand() *cobra.Command {
	return newNodesCommand(commandDeps{configPath: internal.GetConfigPath, now: time.Now})
}

func newNodesCommand(deps commandDeps) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "Manage paired node companions",
		Args:  cobra.NoArgs,
	}
	cmd.PersistentFlags().StringVar(
		&configPath,
		"config",
		"",
		"Path to config.json (default: active PicoClaw config)",
	)
	open := func() (*nodepkg.FileRegistry, error) {
		path := strings.TrimSpace(configPath)
		if path == "" {
			path = deps.configPath()
		}
		cfg, err := config.LoadConfig(path)
		if err != nil {
			return nil, fmt.Errorf("load config: %w", err)
		}
		workspace := strings.TrimSpace(cfg.WorkspacePath())
		if workspace == "" {
			return nil, errors.New("agent workspace is required for node administration")
		}
		registry, err := nodepkg.NewFileRegistry(
			nodepkg.RegistryPath(workspace),
			cfg.Nodes.MaxPendingPairings,
		)
		if err != nil {
			return nil, fmt.Errorf("open node registry: %w", err)
		}
		return registry, nil
	}

	cmd.AddCommand(
		newListCommand(open),
		newDescribeCommand(open),
		newApproveCommand(open, deps.now),
		newDenyCommand(open, deps.now),
		newRevokeCommand(open, deps.now),
	)
	return cmd
}

func newListCommand(open func() (*nodepkg.FileRegistry, error)) *cobra.Command {
	var stateValues []string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List node identities",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			registry, err := open()
			if err != nil {
				return err
			}
			states, err := parseStates(stateValues)
			if err != nil {
				return err
			}
			snapshots, err := registry.List(nodepkg.Filter{States: states})
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(cmd.OutOrStdout(), snapshots)
			}
			if len(snapshots) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No nodes found.")
				return nil
			}
			for _, snapshot := range snapshots {
				fmt.Fprintf(
					cmd.OutOrStdout(),
					"%s\t%s\t%s\n",
					snapshot.ID,
					snapshot.State,
					displayLabel(snapshot),
				)
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&stateValues, "state", nil, "Filter by node state (repeatable)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON output")
	return cmd
}

func newDescribeCommand(open func() (*nodepkg.FileRegistry, error)) *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "describe <id-or-alias>",
		Short: "Show durable registration details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			registry, err := open()
			if err != nil {
				return err
			}
			registration, err := resolveRegistration(registry, args[0])
			if err != nil {
				return err
			}
			return writeRegistration(cmd.OutOrStdout(), registration, jsonOutput)
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON output")
	return cmd
}

func newApproveCommand(
	open func() (*nodepkg.FileRegistry, error),
	now func() time.Time,
) *cobra.Command {
	var aliases []string
	var displayName string
	var allowedCommands []string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "approve <node-id>",
		Short: "Approve one pending node identity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			registry, err := open()
			if err != nil {
				return err
			}
			id := nodepkg.ID(strings.TrimSpace(args[0]))
			parsedAliases := make([]nodepkg.Alias, len(aliases))
			for index, alias := range aliases {
				parsedAliases[index] = nodepkg.Alias(strings.TrimSpace(alias))
			}
			registration, err := registry.Approve(id, nodepkg.PairingApproval{
				Aliases:         parsedAliases,
				DisplayName:     displayName,
				AllowedCommands: allowedCommands,
				At:              now().Unix(),
			})
			if err != nil {
				return err
			}
			return writeRegistration(cmd.OutOrStdout(), registration, jsonOutput)
		},
	}
	cmd.Flags().StringSliceVar(&aliases, "alias", nil, "Assign an operator alias (repeatable)")
	cmd.Flags().StringVar(&displayName, "display-name", "", "Assign an operator display name")
	cmd.Flags().StringSliceVar(
		&allowedCommands,
		"allow-command",
		nil,
		"Approve one advertised command (repeatable; default grants none)",
	)
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON output")
	return cmd
}

func newDenyCommand(
	open func() (*nodepkg.FileRegistry, error),
	now func() time.Time,
) *cobra.Command {
	var reason string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "deny <node-id>",
		Short: "Deny one pending node identity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			registry, err := open()
			if err != nil {
				return err
			}
			registration, err := registry.Deny(
				nodepkg.ID(strings.TrimSpace(args[0])),
				nodepkg.Revocation{Reason: reason, At: now().Unix()},
			)
			if err != nil {
				return err
			}
			return writeRegistration(cmd.OutOrStdout(), registration, jsonOutput)
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "pairing denied by operator", "Record the denial reason")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON output")
	return cmd
}

func newRevokeCommand(
	open func() (*nodepkg.FileRegistry, error),
	now func() time.Time,
) *cobra.Command {
	var reason string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "revoke <id-or-alias>",
		Short: "Revoke one paired node identity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			registry, err := open()
			if err != nil {
				return err
			}
			snapshot, exists, err := registry.Resolve(strings.TrimSpace(args[0]))
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("node %q not found", args[0])
			}
			registration, err := registry.Revoke(
				snapshot.ID,
				nodepkg.Revocation{Reason: reason, At: now().Unix()},
			)
			if err != nil {
				return err
			}
			return writeRegistration(cmd.OutOrStdout(), registration, jsonOutput)
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "pairing revoked by operator", "Record the revocation reason")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON output")
	return cmd
}

func parseStates(values []string) ([]nodepkg.State, error) {
	states := make([]nodepkg.State, len(values))
	for index, value := range values {
		state := nodepkg.State(strings.TrimSpace(value))
		if !state.Valid() {
			return nil, fmt.Errorf("unsupported node state %q", value)
		}
		states[index] = state
	}
	return states, nil
}

func resolveRegistration(
	registry *nodepkg.FileRegistry,
	ref string,
) (nodepkg.Registration, error) {
	snapshot, exists, err := registry.Resolve(strings.TrimSpace(ref))
	if err != nil {
		return nodepkg.Registration{}, err
	}
	if !exists {
		return nodepkg.Registration{}, fmt.Errorf("node %q not found", ref)
	}
	registration, exists, err := registry.Registration(snapshot.ID)
	if err != nil {
		return nodepkg.Registration{}, err
	}
	if !exists {
		return nodepkg.Registration{}, fmt.Errorf("node %q registration not found", ref)
	}
	return registration, nil
}

func writeRegistration(writer io.Writer, registration nodepkg.Registration, jsonOutput bool) error {
	view := registrationToView(registration)
	if jsonOutput {
		return writeJSON(writer, view)
	}
	fmt.Fprintf(writer, "ID: %s\n", view.Node.ID)
	fmt.Fprintf(writer, "State: %s\n", view.Node.State)
	fmt.Fprintf(writer, "Name: %s\n", displayLabel(view.Node))
	fmt.Fprintf(writer, "Public key SHA-256: %s\n", valueOrDash(view.PublicKeySHA256))
	fmt.Fprintf(writer, "Requested role: %s\n", valueOrDash(view.RequestedRole))
	fmt.Fprintf(writer, "Requested at: %s\n", formatTimestamp(view.RequestedAt))
	fmt.Fprintf(writer, "Approved at: %s\n", formatTimestamp(view.ApprovedAt))
	fmt.Fprintf(writer, "Revoked at: %s\n", formatTimestamp(view.RevokedAt))
	fmt.Fprintf(writer, "Allowed commands: %s\n", listOrNone(view.AllowedCommands))
	return nil
}

func registrationToView(registration nodepkg.Registration) registrationView {
	fingerprint := ""
	if len(registration.PublicKey) > 0 {
		sum := sha256.Sum256(registration.PublicKey)
		fingerprint = hex.EncodeToString(sum[:])
	}
	commands := append([]string(nil), registration.AllowedCommands...)
	if commands == nil {
		commands = make([]string, 0)
	}
	return registrationView{
		Node:            registration.Snapshot,
		PublicKeySHA256: fingerprint,
		RequestedRole:   registration.RequestedRole,
		RequestedAt:     registration.RequestedAt,
		AllowedCommands: commands,
		ApprovedAt:      registration.ApprovedAt,
		RevokedAt:       registration.RevokedAt,
	}
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func displayLabel(snapshot nodepkg.Snapshot) string {
	if snapshot.DisplayName != "" {
		return snapshot.DisplayName
	}
	if len(snapshot.Aliases) > 0 {
		values := make([]string, len(snapshot.Aliases))
		for index, alias := range snapshot.Aliases {
			values[index] = string(alias)
		}
		return strings.Join(values, ", ")
	}
	return "-"
}

func formatTimestamp(value int64) string {
	if value <= 0 {
		return "-"
	}
	return time.Unix(value, 0).UTC().Format(time.RFC3339)
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func listOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}
