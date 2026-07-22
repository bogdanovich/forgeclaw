package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	defaultNodeInstance   = "default"
	serviceCommandTimeout = 30 * time.Second
)

var nodeInstancePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

type lifecycleRequest struct {
	Instance string
	System   bool
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
	Status(context.Context, lifecycleRequest) (lifecycleStatus, error)
}

func runServiceLifecycle(action string, args []string) error {
	if action != "status" {
		return fmt.Errorf("unsupported service action %q", action)
	}
	flags := flag.NewFlagSet(action, flag.ContinueOnError)
	instance := flags.String("instance", defaultNodeInstance, "named companion service instance")
	system := flags.Bool("system", false, "manage a system service instead of the current user service")
	jsonOutput := flags.Bool("json", false, "emit stable JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
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
	lifecycle, err := newPlatformServiceLifecycle(request.System)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	status, err := lifecycle.Status(ctx, request)
	if err != nil {
		return err
	}
	return writeLifecycleStatus(os.Stdout, status, *jsonOutput)
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
