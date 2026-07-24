package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/sipeed/picoclaw/pkg/nodes/companion"
)

const (
	defaultNodeInstance   = "default"
	defaultNodeConfigPath = "~/.picoclaw-node/config.json"
	serviceCommandTimeout = 30 * time.Second
)

var (
	nodeInstancePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
	serviceAccountPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$`)
	numericAccountPattern = regexp.MustCompile(`^[0-9]+$`)
)

type lifecycleRequest struct {
	Instance       string
	ConfigPath     string
	ExecutablePath string
	ServiceUser    string
	System         bool
}

type lifecycleStatus struct {
	Instance  string `json:"instance"`
	Manager   string `json:"manager"`
	Scope     string `json:"scope"`
	Service   string `json:"service"`
	UnitPath  string `json:"unit_path"`
	Installed bool   `json:"installed"`
	Active    bool   `json:"active"`
	State     string `json:"state"`
}

type serviceLifecycle interface {
	Install(context.Context, lifecycleRequest) (lifecycleStatus, error)
	Uninstall(context.Context, lifecycleRequest) (lifecycleStatus, error)
	Status(context.Context, lifecycleRequest) (lifecycleStatus, error)
}

func runServiceLifecycle(action string, args []string) error {
	if action != "install" && action != "uninstall" && action != "status" {
		return fmt.Errorf("unsupported service action %q", action)
	}
	flags := flag.NewFlagSet(action, flag.ContinueOnError)
	instance := flags.String("instance", defaultNodeInstance, "named companion service instance")
	system := flags.Bool("system", false, "manage a system service instead of the current user service")
	jsonOutput := flags.Bool("json", false, "emit stable JSON output")
	configPath := ""
	serviceUser := ""
	if action == "install" {
		flags.StringVar(&configPath, "config", defaultNodeConfigPath, "path to node configuration")
		flags.StringVar(&serviceUser, "service-user", "", "unprivileged account for a system service")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	configExplicit := false
	flags.Visit(func(visited *flag.Flag) {
		if visited.Name == "config" {
			configExplicit = true
		}
	})
	if flags.NArg() != 0 {
		return fmt.Errorf("%s accepts no positional arguments", action)
	}
	request := lifecycleRequest{
		Instance: strings.TrimSpace(*instance),
		System:   *system,
	}
	if !nodeInstancePattern.MatchString(request.Instance) {
		return fmt.Errorf("invalid service instance %q", request.Instance)
	}
	requestedUser := strings.TrimSpace(serviceUser)
	if !request.System && requestedUser != "" {
		return errors.New("--service-user requires --system")
	}
	if err := validatePlatformServiceAction(action); err != nil {
		return err
	}
	if action == "install" {
		resolved, err := resolveLifecycleConfigPath(configPath, request.System, configExplicit)
		if err != nil {
			return fmt.Errorf("resolve node config: %w", err)
		}
		if request.System {
			request.ServiceUser, err = resolveServiceAccount(
				requestedUser,
				user.Lookup,
				func(account *user.User) ([]string, error) { return account.GroupIds() },
			)
			if err != nil {
				return err
			}
		}
		if _, err = companion.LoadConfig(resolved); err != nil {
			return fmt.Errorf("validate node config: %w", err)
		}
		request.ConfigPath = resolved
		request.ExecutablePath, err = currentExecutablePath()
		if err != nil {
			return err
		}
	}
	lifecycle, err := newPlatformServiceLifecycle(request.System)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	var status lifecycleStatus
	switch action {
	case "install":
		status, err = lifecycle.Install(ctx, request)
	case "uninstall":
		status, err = lifecycle.Uninstall(ctx, request)
	case "status":
		status, err = lifecycle.Status(ctx, request)
	}
	if err != nil {
		return err
	}
	return writeLifecycleStatus(os.Stdout, status, *jsonOutput)
}

func resolveLifecycleConfigPath(value string, system, explicit bool) (string, error) {
	if system {
		if !explicit {
			return "", errors.New("system installation requires an explicit --config")
		}
		if !filepath.IsAbs(strings.TrimSpace(value)) {
			return "", errors.New("system installation requires an absolute --config")
		}
	}
	return resolveLifecyclePath(value)
}

func resolveServiceAccount(
	name string,
	lookup func(string) (*user.User, error),
	groupIDs func(*user.User) ([]string, error),
) (string, error) {
	if !serviceAccountPattern.MatchString(name) {
		return "", errors.New("system installation requires a valid --service-user")
	}
	account, err := lookup(name)
	if err != nil {
		return "", fmt.Errorf("resolve system service user %q: %w", name, err)
	}
	if account == nil || !serviceAccountPattern.MatchString(account.Username) ||
		numericAccountPattern.MatchString(account.Username) {
		return "", fmt.Errorf("system service user %q must resolve to an unprivileged account", name)
	}
	uid, err := strconv.ParseUint(account.Uid, 10, 32)
	if err != nil || uid == 0 {
		return "", fmt.Errorf("system service user %q must resolve to an unprivileged account", name)
	}
	primaryGID, err := strconv.ParseUint(account.Gid, 10, 32)
	if err != nil || primaryGID == 0 {
		return "", fmt.Errorf("system service user %q must not belong to the root group", name)
	}
	groups, err := groupIDs(account)
	if err != nil {
		return "", fmt.Errorf("resolve groups for system service user %q: %w", name, err)
	}
	for _, group := range groups {
		gid, parseErr := strconv.ParseUint(group, 10, 32)
		if parseErr != nil || gid == 0 {
			return "", fmt.Errorf("system service user %q must not belong to the root group", name)
		}
	}
	return account.Username, nil
}

func currentExecutablePath() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve picoclaw-node executable: %w", err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve picoclaw-node executable symlinks: %w", err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve picoclaw-node executable path: %w", err)
	}
	return filepath.Clean(path), nil
}

func resolveLifecyclePath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", errors.New("path is empty or contains control characters")
	}
	if value == "~" || strings.HasPrefix(value, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if value == "~" {
			value = home
		} else {
			value = filepath.Join(home, strings.TrimPrefix(value, "~/"))
		}
	}
	path, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return filepath.Clean(path), nil
}

func writeLifecycleStatus(writer io.Writer, status lifecycleStatus, jsonOutput bool) error {
	if jsonOutput {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(status)
	}
	fmt.Fprintf(writer, "Instance: %s\n", status.Instance)
	fmt.Fprintf(writer, "Manager: %s (%s)\n", status.Manager, status.Scope)
	fmt.Fprintf(writer, "Service: %s\n", status.Service)
	fmt.Fprintf(writer, "Unit: %s\n", status.UnitPath)
	fmt.Fprintf(writer, "Installed: %t\n", status.Installed)
	fmt.Fprintf(writer, "Active: %t\n", status.Active)
	fmt.Fprintf(writer, "State: %s\n", status.State)
	return nil
}
