//go:build (linux || darwin) && integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/companion"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
	nodews "github.com/bogdanovich/mintclaw/pkg/nodes/ws"
)

func TestCompanionProcessAuthenticatesAndInvokesOverWSS(t *testing.T) {
	registry, admission, _ := newProcessTestGateway(t)
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

func TestCompanionProcessTransfersFilesOverAuthenticatedWSS(t *testing.T) {
	registry, admission, sessions := newProcessTestGateway(t)
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
	projectRoot, err := filepath.EvalSymlinks(filepath.Join(tempDir, "project"))
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(filepath.Join(tempDir, "project"), 0o700); err != nil {
			t.Fatal(err)
		}
		projectRoot, err = filepath.EvalSymlinks(filepath.Join(tempDir, "project"))
	}
	if err != nil {
		t.Fatal(err)
	}
	binaryPath := buildCompanionBinary(t, tempDir)
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
		Policy: nodes.LocalCommandPolicy{
			Revision:          "e2e-policy",
			AllowedCommands:   []string{"node.info.v1"},
			MaximumRisk:       nodes.RiskRead,
			MaxTimeoutSeconds: 5,
			MaxOutputBytes:    4096,
		},
		FilePolicies: companion.FilePolicies{
			"project": {
				Enabled:        true,
				Revision:       "project-files-v1",
				ReadableRoots:  []string{projectRoot},
				WritableRoots:  []string{projectRoot},
				AllowCreate:    true,
				AllowOverwrite: true,
				MaxFileBytes:   protocol.MaxTransferFileBytes,
			},
		},
	}
	configPath := filepath.Join(tempDir, "config.json")
	writeProcessTestConfig(t, configPath, config)
	process := startCompanionProcess(t, binaryPath, configPath)
	defer process.stop(t)

	pending := waitForOnlyNodeState(t, registry, nodes.StatePendingPairing)
	if _, err := registry.Approve(pending.ID, nodes.PairingApproval{
		AllowedCommands: []string{
			"file.download.v1",
			"file.info.v1",
			"file.upload.v1",
			"node.info.v1",
		},
		At: time.Now().Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	connected := waitForOnlyNodeState(t, registry, nodes.StateConnected)

	uploadContent := bytes.Repeat([]byte("real process upload\n"), 20000)
	uploadDigest := sha256.Sum256(uploadContent)
	uploadBinding := nodews.TransferBinding{
		TransferID:     "process_upload",
		Direction:      protocol.TransferUpload,
		PolicyRevision: "project-files-v1",
		TotalSize:      uint64(len(uploadContent)),
		SHA256:         uploadDigest,
	}
	uploadCtx, cancelUpload := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancelUpload()
	upload, err := sessions.OpenTransfer(uploadCtx, connected.ID, uploadBinding)
	if err != nil {
		t.Fatal(err)
	}
	defer upload.Close()
	uploadPath := filepath.Join(projectRoot, "uploaded.txt")
	sendTransferPrepare(t, uploadCtx, upload, uploadBinding, map[string]any{
		"operation":   "upload",
		"path":        uploadPath,
		"publication": "create",
		"expires_at":  time.Now().Add(time.Minute).Unix(),
	})
	expectTransferType(t, uploadCtx, upload, protocol.TransferFrameAccept)
	sendGatewayUpload(t, uploadCtx, upload, uploadBinding, uploadContent)
	if err := upload.Send(uploadCtx, transferFrame(
		uploadBinding,
		protocol.TransferFrameCommit,
	)); err != nil {
		t.Fatal(err)
	}
	expectTransferType(t, uploadCtx, upload, protocol.TransferFrameCommitted)
	uploaded, err := os.ReadFile(uploadPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(uploaded, uploadContent) {
		t.Fatal("real-process upload published different bytes")
	}

	downloadContent := bytes.Repeat([]byte{0, 1, 2, 3, 255}, 70000)
	downloadPath := filepath.Join(projectRoot, "source.bin")
	if err := os.WriteFile(downloadPath, downloadContent, 0o600); err != nil {
		t.Fatal(err)
	}
	downloadDigest := sha256.Sum256(downloadContent)
	downloadBinding := nodews.TransferBinding{
		TransferID:     "process_download",
		Direction:      protocol.TransferDownload,
		PolicyRevision: "project-files-v1",
		TotalSize:      uint64(len(downloadContent)),
		SHA256:         downloadDigest,
	}
	downloadCtx, cancelDownload := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancelDownload()
	download, err := sessions.OpenTransfer(
		downloadCtx,
		connected.ID,
		downloadBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer download.Close()
	sendTransferPrepare(t, downloadCtx, download, downloadBinding, map[string]any{
		"operation":  "download",
		"path":       downloadPath,
		"expires_at": time.Now().Add(time.Minute).Unix(),
	})
	expectTransferType(t, downloadCtx, download, protocol.TransferFrameAccept)
	var downloaded bytes.Buffer
	for downloaded.Len() < len(downloadContent) {
		frame, err := download.Receive(downloadCtx)
		if err != nil {
			t.Fatal(err)
		}
		if frame.Type != protocol.TransferFrameChunk {
			t.Fatalf("download frame = %#v, want chunk", frame)
		}
		_, _ = downloaded.Write(frame.Payload)
		ack := transferFrame(downloadBinding, protocol.TransferFrameAck)
		ack.Sequence = frame.Sequence
		if err := download.Send(downloadCtx, ack); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(downloaded.Bytes(), downloadContent) {
		t.Fatal("real-process download returned different bytes")
	}
	received := expectTransferType(
		t,
		downloadCtx,
		download,
		protocol.TransferFrameStatus,
	)
	if !bytes.Contains(received.Payload, []byte(`"state":"received"`)) {
		t.Fatalf("download received status = %s", received.Payload)
	}
	if err := download.Send(downloadCtx, transferFrame(
		downloadBinding,
		protocol.TransferFrameCommit,
	)); err != nil {
		t.Fatal(err)
	}
	expectTransferType(t, downloadCtx, download, protocol.TransferFrameCommitted)
}

func newProcessTestGateway(
	t *testing.T,
) (*nodes.FileRegistry, *nodews.AdmissionHandler, *nodews.SessionHub) {
	t.Helper()
	registry, err := nodes.NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sessions := nodews.NewSessionHub()
	admission, err := nodews.NewAdmissionHandler(authenticator, nodews.AdmissionConfig{
		Sessions: sessions,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry, admission, sessions
}

func sendTransferPrepare(
	t *testing.T,
	ctx context.Context,
	stream *nodews.TransferStream,
	binding nodews.TransferBinding,
	metadata map[string]any,
) {
	t.Helper()
	payload, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	frame := transferFrame(binding, protocol.TransferFramePrepare)
	frame.Payload = payload
	if err := stream.Send(ctx, frame); err != nil {
		t.Fatal(err)
	}
}

func sendGatewayUpload(
	t *testing.T,
	ctx context.Context,
	stream *nodews.TransferStream,
	binding nodews.TransferBinding,
	content []byte,
) {
	t.Helper()
	var sequence uint64
	for len(content) > 0 {
		count := min(len(content), protocol.MaxTransferChunkBytes)
		sequence++
		frame := transferFrame(binding, protocol.TransferFrameChunk)
		frame.Sequence = sequence
		frame.Payload = append([]byte(nil), content[:count]...)
		if err := stream.Send(ctx, frame); err != nil {
			t.Fatal(err)
		}
		ack := expectTransferType(t, ctx, stream, protocol.TransferFrameAck)
		if ack.Sequence != sequence {
			t.Fatalf("upload acknowledgement sequence = %d, want %d", ack.Sequence, sequence)
		}
		content = content[count:]
	}
}

func expectTransferType(
	t *testing.T,
	ctx context.Context,
	stream *nodews.TransferStream,
	want protocol.TransferFrameType,
) protocol.TransferFrame {
	t.Helper()
	frame, err := stream.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != want {
		t.Fatalf("transfer frame = %#v, want type %d", frame, want)
	}
	return frame
}

func transferFrame(
	binding nodews.TransferBinding,
	frameType protocol.TransferFrameType,
) protocol.TransferFrame {
	return protocol.TransferFrame{
		Type:           frameType,
		Direction:      binding.Direction,
		TransferID:     binding.TransferID,
		PolicyRevision: binding.PolicyRevision,
		TotalSize:      binding.TotalSize,
		SHA256:         binding.SHA256,
	}
}

func buildCompanionBinary(t *testing.T, outputDir string) string {
	t.Helper()
	if binaryPath := os.Getenv("MINTCLAW_NODE_TEST_BINARY"); binaryPath != "" {
		if _, err := os.Stat(binaryPath); err != nil {
			t.Fatalf("stat shared companion binary: %v", err)
		}
		return binaryPath
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve e2e test source path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	binaryPath := filepath.Join(outputDir, "mintclaw-node")
	command := exec.Command("go", "build", "-o", binaryPath, "./cmd/mintclaw-node")
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
