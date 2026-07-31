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
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
)

var version = "dev"

func main() {
	if err := execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mintclaw-node:", err)
		os.Exit(1)
	}
}

func execute(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: mintclaw-node <run|install|uninstall|status|version>")
	}
	switch args[0] {
	case "run":
		return run(args[1:])
	case "install", "uninstall", "status":
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
	configPath := flags.String("config", "~/.mintclaw-node/config.json", "path to node configuration")
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
	if privilegeErr := validateFileHelperProcessIdentity(cfg); privilegeErr != nil {
		return privilegeErr
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
	runtimeOptions := make([]companion.RuntimeOption, 0, 3)
	fileCapabilities := make([]companion.FileTransferCapability, 0, 2)
	if companion.HasEnabledFilePolicy(cfg.FilePolicies) {
		transferLedger, transferLedgerErr := companion.NewFileTransferLedger(
			companion.FileTransferLedgerPath(cfg.StateDir),
			companion.DefaultFileTransferLedgerLimit,
			companion.DefaultFileTransferLedgerBytes,
		)
		if transferLedgerErr != nil {
			return transferLedgerErr
		}
		defer transferLedger.Close()
		fileTransfers, transferRuntimeErr := companion.NewFileTransferRuntime(
			cfg.FilePolicies,
			transferLedger,
		)
		if transferRuntimeErr != nil {
			return transferRuntimeErr
		}
		defer fileTransfers.Close()
		fileCapabilities = append(fileCapabilities, fileTransfers)
	}
	if cfg.FileHelper != nil {
		snapshotContext, cancelSnapshot := context.WithTimeout(context.Background(), 5*time.Second)
		fileHelper, helperErr := companion.NewFileHelperClient(
			snapshotContext,
			cfg.FileHelper.SocketPath,
		)
		cancelSnapshot()
		if helperErr != nil {
			return fmt.Errorf("load file helper snapshot: %w", helperErr)
		}
		defer fileHelper.Close()
		fileCapabilities = append(fileCapabilities, fileHelper)
	}
	var fileTransfers *companion.FileTransferRouter
	if len(fileCapabilities) > 0 {
		fileTransfers, err = companion.NewFileTransferRouter(fileCapabilities...)
		if err != nil {
			return err
		}
		runtimeOptions = append(runtimeOptions, companion.WithFileCapabilities(fileTransfers))
	}
	if cfg.SystemExec != nil {
		runtimeOptions = append(runtimeOptions, companion.WithSystemExec(*cfg.SystemExec))
	}
	if cfg.OwnerShell != nil && cfg.OwnerShell.Enabled {
		broker, brokerErr := companion.NewAuthorityBrokerClient(cfg.OwnerShell.BrokerSocket)
		if brokerErr != nil {
			return brokerErr
		}
		snapshotContext, cancelSnapshot := context.WithTimeout(context.Background(), 5*time.Second)
		snapshot, snapshotErr := broker.Snapshot(snapshotContext)
		cancelSnapshot()
		if snapshotErr != nil {
			return fmt.Errorf("load authority broker snapshot: %w", snapshotErr)
		}
		runtimeOptions = append(runtimeOptions, companion.WithShellBroker(snapshot, broker))
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
	var client *companion.Client
	if fileTransfers != nil {
		client, err = companion.NewClientWithRuntimeAndTransferHandler(
			cfg,
			identity,
			clientVersion(),
			commandRuntime,
			fileTransfers,
			slog.Default(),
		)
	} else {
		client, err = companion.NewClientWithRuntime(
			cfg,
			identity,
			clientVersion(),
			commandRuntime,
			slog.Default(),
		)
	}
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
