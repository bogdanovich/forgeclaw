//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/nodes/companion"
	nodews "github.com/sipeed/picoclaw/pkg/nodes/ws"
)

func TestCompanionProcessAuthenticatesAndInvokesOverWSS(t *testing.T) {
	registry, admission := newProcessTestGateway(t)
	server := httptest.NewTLSServer(admission)
	defer server.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := admission.Close(ctx); err != nil {
			t.Errorf("close admission: %v", err)
		}
	}()

	tempDir := t.TempDir()
	binaryPath := buildCompanionBinary(t, tempDir)
	policy := nodes.LocalCommandPolicy{
		Revision:          "e2e-policy",
		AllowedCommands:   []string{"node.info.v1"},
		MaximumRisk:       nodes.RiskRead,
		MaxTimeoutSeconds: 5,
		MaxOutputBytes:    4096,
	}
	fingerprint := sha256.Sum256(server.Certificate().Raw)
	config := companion.Config{
		GatewayURL: strings.Replace(server.URL, "https://", "wss://", 1) +
			companion.GatewayPath,
		StateDir: filepath.Join(tempDir, "state"),
		TLS: companion.TLSConfig{
			CertificateSHA256: hex.EncodeToString(fingerprint[:]),
		},
		Reconnect: companion.ReconnectConfig{
			MinDelaySeconds:     1,
			MaxDelaySeconds:     1,
			PendingDelaySeconds: 1,
		},
		Policy: policy,
	}
	configPath := filepath.Join(tempDir, "config.json")
	writeProcessTestConfig(t, configPath, config)

	process := startCompanionProcess(t, binaryPath, configPath)
	defer process.stop(t)

	pending := waitForOnlyNodeState(t, registry, nodes.StatePendingPairing)
	if _, err := registry.Approve(pending.ID, nodes.PairingApproval{
		AllowedCommands: []string{"node.info.v1"},
		At:              time.Now().Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	connected := waitForOnlyNodeState(t, registry, nodes.StateConnected)
	registration, exists, err := registry.Registration(connected.ID)
	if err != nil || !exists {
		t.Fatalf("Registration() = exists %v, error %v", exists, err)
	}
	if registration.Snapshot.Executor != companion.LocalExecutor ||
		registration.Snapshot.PolicyRevision != policy.Revision {
		t.Fatalf("authenticated execution profile = %#v", registration.Snapshot)
	}
	descriptor, err := registration.ApprovedCommand("node.info.v1")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     "inv_process_e2e",
		IdempotencyKey:   "idem_process_e2e",
		NodeID:           connected.ID,
		CatalogHash:      registration.Snapshot.CatalogHash,
		Command:          descriptor.Name,
		Input:            json.RawMessage(`{}`),
		AgentID:          "agent_e2e",
		SessionID:        "session_e2e",
		ActorID:          "actor_e2e",
		TimeoutSeconds:   5,
		OutputLimitBytes: 4096,
	},
		descriptor,
		registration.Snapshot.Executor,
		registration.Snapshot.PolicyRevision,
		time.Now(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	invokeCtx, cancelInvoke := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancelInvoke()
	output, _, err := admission.Invoke(invokeCtx, connected.ID, plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	var info struct {
		NodeID nodes.ID `json:"node_id"`
	}
	if err := json.Unmarshal(output, &info); err != nil {
		t.Fatal(err)
	}
	if info.NodeID != connected.ID {
		t.Fatalf("node.info node_id = %q; want %q", info.NodeID, connected.ID)
	}

	process.stop(t)
	waitForOnlyNodeState(t, registry, nodes.StateDisconnected)
}

func newProcessTestGateway(t *testing.T) (*nodes.FileRegistry, *nodews.AdmissionHandler) {
	t.Helper()
	registry, err := nodes.NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := nodews.NewAdmissionHandler(authenticator, nodews.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return registry, admission
}

func buildCompanionBinary(t *testing.T, outputDir string) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve e2e test source path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	binaryPath := filepath.Join(outputDir, "picoclaw-node")
	command := exec.Command("go", "build", "-o", binaryPath, "./cmd/picoclaw-node")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build companion binary: %v\n%s", err, output)
	}
	return binaryPath
}

func writeProcessTestConfig(t *testing.T, path string, config companion.Config) {
	t.Helper()
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

type companionProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	done    chan error
	once    sync.Once
}

func startCompanionProcess(t *testing.T, binaryPath, configPath string) *companionProcess {
	t.Helper()
	process := &companionProcess{
		command: exec.Command(binaryPath, "run", "--config", configPath),
		done:    make(chan error, 1),
	}
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		process.done <- process.command.Wait()
	}()
	return process
}

func (process *companionProcess) stop(t *testing.T) {
	t.Helper()
	process.once.Do(func() {
		if err := process.command.Process.Signal(os.Interrupt); err != nil {
			t.Errorf("interrupt companion process: %v", err)
			_ = process.command.Process.Kill()
		}
		select {
		case err := <-process.done:
			if err != nil {
				t.Errorf("companion process exit: %v\n%s", err, process.output.String())
			}
		case <-time.After(3 * time.Second):
			_ = process.command.Process.Kill()
			err := <-process.done
			t.Errorf("companion process did not stop after interrupt: %v\n%s", err, process.output.String())
		}
	})
}

func waitForOnlyNodeState(
	t *testing.T,
	registry *nodes.FileRegistry,
	want nodes.State,
) nodes.Snapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snapshots, err := registry.List(nodes.Filter{States: []nodes.State{want}})
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) == 1 {
			return snapshots[0]
		}
		time.Sleep(25 * time.Millisecond)
	}
	snapshots, err := registry.List(nodes.Filter{})
	t.Fatalf("nodes = %s, error %v; want exactly one %q node", formatSnapshots(snapshots), err, want)
	return nodes.Snapshot{}
}

func formatSnapshots(snapshots []nodes.Snapshot) string {
	data, err := json.Marshal(snapshots)
	if err != nil {
		return fmt.Sprintf("%#v", snapshots)
	}
	return string(data)
}
