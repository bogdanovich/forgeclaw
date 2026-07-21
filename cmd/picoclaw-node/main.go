package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/sipeed/picoclaw/pkg/nodes/companion"
)

var version = "dev"

func main() {
	if err := execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "picoclaw-node:", err)
		os.Exit(1)
	}
}

func execute(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: picoclaw-node <run|install|status|version>")
	}
	switch args[0] {
	case "run":
		return run(args[1:])
	case "install", "status":
		return runServiceLifecycle(args[0], args[1:])
	case "version":
		fmt.Println(clientVersion())
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := flags.String("config", "~/.picoclaw-node/config.json", "path to node configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("run accepts no positional arguments")
	}
	cfg, err := companion.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	identity, err := companion.LoadOrCreateIdentity(cfg.StateDir)
	if err != nil {
		return err
	}
	ledger, err := companion.NewFileInvocationLedger(
		companion.InvocationLedgerPath(cfg.StateDir),
		companion.DefaultInvocationLedgerLimit,
		companion.DefaultInvocationLedgerBytes,
	)
	if err != nil {
		return err
	}
	defer ledger.Close()
	runtimeOptions := make([]companion.RuntimeOption, 0, 1)
	if cfg.SystemExec != nil {
		runtimeOptions = append(runtimeOptions, companion.WithSystemExec(*cfg.SystemExec))
	}
	commandRuntime, err := companion.NewRuntime(
		identity.ID,
		clientVersion(),
		cfg.Policy,
		ledger,
		runtimeOptions...,
	)
	if err != nil {
		return err
	}
	client, err := companion.NewClientWithRuntime(
		cfg,
		identity,
		clientVersion(),
		commandRuntime,
		slog.Default(),
	)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("starting node companion", "node_id", identity.ID, "gateway", cfg.GatewayURL)
	return client.Run(ctx)
}

func clientVersion() string {
	if version != "" && version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}
