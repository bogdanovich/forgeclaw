//go:build linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
)

func main() {
	if err := execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mintclaw-node-file-helper:", err)
		os.Exit(1)
	}
}

func execute(args []string) error {
	if len(args) == 0 || args[0] != "run" {
		return errors.New("usage: mintclaw-node-file-helper run --config <path>")
	}
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := flags.String(
		"config",
		"/etc/mintclaw/node-file-helper.json",
		"path to root-owned file helper configuration",
	)
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("file helper run accepts no positional arguments")
	}
	config, err := companion.LoadFileHelperServiceConfig(*configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return companion.RunFileHelper(ctx, config)
}
