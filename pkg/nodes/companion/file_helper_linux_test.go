//go:build linux

package companion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestFileHelperUnixMetadataReadCreateAndAtomicReplace(t *testing.T) {
	client, root, _ := startTestFileHelper(t, 0)
	metadataPath := filepath.Join(root, "metadata.conf")
	if err := os.WriteFile(metadataPath, []byte("metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	metadata := helperPrepareFrame(
		t,
		"helper_metadata",
		protocol.TransferDownload,
		emptyTransferDigest,
		0,
		fileTransferPrepare{
			Operation: fileOperationInfo,
			Path:      metadataPath,
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	if response := helperSendAndWait(t, client, metadata); response.Type != protocol.TransferFrameCommitted {
		t.Fatalf("metadata response = %#v", response)
	}

	createdPath := filepath.Join(root, "created.conf")
	created := []byte("created through helper")
	uploadThroughHelper(t, client, "helper_create", createdPath, filePublicationCreate, created)
	if data, err := os.ReadFile(createdPath); err != nil || !bytes.Equal(data, created) {
		t.Fatalf("created file = (%q, %v)", data, err)
	}
	replaced := []byte("atomically replaced through helper")
	uploadThroughHelper(t, client, "helper_replace", createdPath, filePublicationReplace, replaced)
	if data, err := os.ReadFile(createdPath); err != nil || !bytes.Equal(data, replaced) {
		t.Fatalf("replaced file = (%q, %v)", data, err)
	}

	downloaded := downloadThroughHelper(t, client, "helper_download", createdPath, replaced)
	if !bytes.Equal(downloaded, replaced) {
		t.Fatalf("downloaded file = %q, want %q", downloaded, replaced)
	}
}

func TestFileHelperExternalRootFixture(t *testing.T) {
	socketPath := os.Getenv("MINTCLAW_FILE_HELPER_E2E_SOCKET")
	root := os.Getenv("MINTCLAW_FILE_HELPER_E2E_ROOT")
	if socketPath == "" || root == "" {
		t.Skip("external root helper fixture is not configured")
	}
	if os.Geteuid() == 0 {
		t.Fatal("external root helper proof must keep the companion-side test unprivileged")
	}
	client, err := NewFileHelperClient(t.Context(), socketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	fixture := filepath.Join(root, "root-owned.conf")
	metadata := helperPrepareFrame(
		t,
		"root_helper_metadata",
		protocol.TransferDownload,
		emptyTransferDigest,
		0,
		fileTransferPrepare{
			Operation: fileOperationInfo,
			Path:      fixture,
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	if response := helperSendAndWait(t, client, metadata); response.Type != protocol.TransferFrameCommitted {
		t.Fatalf("root metadata response = %#v", response)
	}
	initial := []byte("root-old")
	if downloaded := downloadThroughHelper(
		t,
		client,
		"root_helper_read",
		fixture,
		initial,
	); !bytes.Equal(downloaded, initial) {
		t.Fatalf("root helper initial read = %q", downloaded)
	}
	replacement := []byte("root-new")
	uploadThroughHelper(
		t,
		client,
		"root_helper_replace",
		fixture,
		filePublicationReplace,
		replacement,
	)
	if downloaded := downloadThroughHelper(
		t,
		client,
		"root_helper_read_replaced",
		fixture,
		replacement,
	); !bytes.Equal(downloaded, replacement) {
		t.Fatalf("root helper replaced read = %q", downloaded)
	}
	created := []byte("root-created")
	createdPath := filepath.Join(root, "root-created.conf")
	uploadThroughHelper(
		t,
		client,
		"root_helper_create",
		createdPath,
		filePublicationCreate,
		created,
	)
	if downloaded := downloadThroughHelper(
		t,
		client,
		"root_helper_read_created",
		createdPath,
		created,
	); !bytes.Equal(downloaded, created) {
		t.Fatalf("root helper created read = %q", downloaded)
	}
}

func TestFileHelperUnixDeniesPeerProfileRevisionPathSizeModeExpiryAndDigest(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("peer-account test requires an unprivileged test process")
	}
	client, root, config := startTestFileHelper(t, 4)
	outside := filepath.Join(canonicalTempDir(t), "outside")
	digest := sha256.Sum256([]byte("12345"))
	tests := []struct {
		name  string
		frame protocol.TransferFrame
	}{
		{
			name: "path",
			frame: helperPrepareFrame(
				t,
				"helper_path_denied",
				protocol.TransferUpload,
				sha256.Sum256([]byte("ok")),
				2,
				fileTransferPrepare{
					Operation: fileOperationUpload, Path: outside,
					Publication: filePublicationCreate,
					ExpiresAt:   time.Now().Add(time.Minute).Unix(),
				},
			),
		},
		{
			name: "size",
			frame: helperPrepareFrame(
				t,
				"helper_size_denied",
				protocol.TransferUpload,
				digest,
				5,
				fileTransferPrepare{
					Operation:   fileOperationUpload,
					Path:        filepath.Join(root, "too-large"),
					Publication: filePublicationCreate,
					ExpiresAt:   time.Now().Add(time.Minute).Unix(),
				},
			),
		},
		{
			name: "mode",
			frame: helperPrepareFrame(
				t,
				"helper_mode_denied",
				protocol.TransferUpload,
				sha256.Sum256([]byte("ok")),
				2,
				fileTransferPrepare{
					Operation:   fileOperationUpload,
					Path:        filepath.Join(root, "bad-mode"),
					Publication: "append",
					ExpiresAt:   time.Now().Add(time.Minute).Unix(),
				},
			),
		},
		{
			name: "expiry",
			frame: helperPrepareFrame(
				t,
				"helper_expired",
				protocol.TransferUpload,
				sha256.Sum256([]byte("ok")),
				2,
				fileTransferPrepare{
					Operation:   fileOperationUpload,
					Path:        filepath.Join(root, "expired"),
					Publication: filePublicationCreate,
					ExpiresAt:   time.Now().Add(-time.Minute).Unix(),
				},
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := helperSendAndWait(t, client, test.frame)
			if response.Type != protocol.TransferFrameDeny {
				t.Fatalf("denial response = %#v", response)
			}
		})
	}

	wrongProfile := helperPrepareFrame(
		t,
		"helper_profile_denied",
		protocol.TransferDownload,
		emptyTransferDigest,
		0,
		fileTransferPrepare{
			Operation: fileOperationInfo,
			Path:      filepath.Join(root, "missing"),
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	if response := rawFileHelperRequest(
		t,
		config.SocketPath,
		"other-profile",
		wrongProfile,
	); response.Type != protocol.TransferFrameDeny {
		t.Fatalf("wrong profile response = %#v", response)
	}
	wrongRevision := wrongProfile
	wrongRevision.TransferID = "helper_revision_denied"
	wrongRevision.PolicyRevision = "other-v1"
	if response := rawFileHelperRequest(
		t,
		config.SocketPath,
		"server-admin",
		wrongRevision,
	); response.Type != protocol.TransferFrameDeny {
		t.Fatalf("wrong revision response = %#v", response)
	}

	goodDigest := sha256.Sum256([]byte("ok"))
	prepare := helperPrepareFrame(
		t,
		"helper_digest_denied",
		protocol.TransferUpload,
		goodDigest,
		2,
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        filepath.Join(root, "digest"),
			Publication: filePublicationCreate,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	connection := rawFileHelperConnection(t, config.SocketPath)
	writeRawFileHelperTransfer(t, connection, "server-admin", prepare)
	if response := readRawFileHelperTransfer(t, connection); response.Type != protocol.TransferFrameAccept {
		t.Fatalf("digest prepare response = %#v", response)
	}
	changed := transferFrameFromBinding(prepare, protocol.TransferFrameChunk)
	changed.Sequence = 1
	changed.Payload = []byte("ok")
	changed.SHA256 = sha256.Sum256([]byte("no"))
	writeRawFileHelperTransfer(t, connection, "server-admin", changed)
	message, err := readFileHelperMessage(connection)
	if err != nil || message.Kind != fileHelperErrorResponse {
		t.Fatalf("digest conflict response = (%#v, %v)", message, err)
	}
	_ = connection.Close()

	deniedConfig := config
	deniedConfig.AllowedUID++
	deniedConfig.SocketPath = filepath.Join(root, "denied-peer.sock")
	startTestFileHelperServer(t, deniedConfig)
	if _, err := newFileHelperClient(
		t.Context(),
		deniedConfig.SocketPath,
		uint32(os.Getuid()),
		uint32(os.Getgid()),
	); err == nil {
		t.Fatal("wrong companion peer account was accepted")
	}
}

func startTestFileHelper(
	t *testing.T,
	maxFileBytes int64,
) (*FileHelperClient, string, FileHelperServiceConfig) {
	t.Helper()
	if os.Getuid() == 0 {
		t.Skip("helper IPC test requires an unprivileged companion peer")
	}
	root := canonicalTempDir(t)
	policies := normalizedFilePoliciesForTest(t, "server-admin", "server-admin-v1", root)
	if maxFileBytes > 0 {
		profile := policies["server-admin"]
		profile.MaxFileBytes = maxFileBytes
		policies["server-admin"] = profile
	}
	config, err := NormalizeFileHelperServiceConfig(FileHelperServiceConfig{
		SocketPath: filepath.Join(root, "helper.sock"),
		StateDir:   filepath.Join(root, "state"),
		AllowedUID: uint32(os.Getuid()),
		AllowedGID: uint32(os.Getgid()),
		Profiles:   policies,
	}, root)
	if err != nil {
		t.Fatal(err)
	}
	startTestFileHelperServer(t, config)
	client, err := newFileHelperClient(
		t.Context(),
		config.SocketPath,
		uint32(os.Getuid()),
		uint32(os.Getgid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, root, config
}

func startTestFileHelperServer(t *testing.T, config FileHelperServiceConfig) {
	t.Helper()
	ledger := newMemoryFileTransferLedger()
	runtime, err := NewFileTransferRuntime(config.Profiles, ledger)
	if err != nil {
		t.Fatal(err)
	}
	server, err := newFileHelperServer(config, runtime)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: config.SocketPath, Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("file helper Serve() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("file helper server did not stop")
		}
		runtime.Close()
	})
}

func helperPrepareFrame(
	t *testing.T,
	transferID string,
	direction protocol.TransferDirection,
	digest [32]byte,
	size uint64,
	prepare fileTransferPrepare,
) protocol.TransferFrame {
	t.Helper()
	frame := testFilePrepareFrame(t, transferID, direction, digest, size, prepare)
	frame.PolicyRevision = "server-admin-v1"
	return frame
}

func helperSendAndWait(
	t *testing.T,
	client *FileHelperClient,
	frame protocol.TransferFrame,
) protocol.TransferFrame {
	t.Helper()
	responses := make(chan protocol.TransferFrame, 1)
	if err := client.HandleTransferFrame(
		t.Context(),
		frame,
		func(response protocol.TransferFrame) error {
			responses <- response
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	return waitFileHelperResponse(t, responses)
}

func waitFileHelperResponse(
	t *testing.T,
	responses <-chan protocol.TransferFrame,
) protocol.TransferFrame {
	t.Helper()
	select {
	case response := <-responses:
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("file helper response timed out")
		return protocol.TransferFrame{}
	}
}

func uploadThroughHelper(
	t *testing.T,
	client *FileHelperClient,
	transferID string,
	path string,
	publication string,
	content []byte,
) {
	t.Helper()
	digest := sha256.Sum256(content)
	prepare := helperPrepareFrame(
		t,
		transferID,
		protocol.TransferUpload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation: fileOperationUpload, Path: path,
			Publication: publication,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	if response := helperSendAndWait(t, client, prepare); response.Type != protocol.TransferFrameAccept {
		t.Fatalf("upload prepare response = %#v", response)
	}
	for index, chunk := range chunkBytes(content) {
		frame := transferFrameFromBinding(prepare, protocol.TransferFrameChunk)
		frame.Sequence = uint64(index + 1)
		frame.Payload = chunk
		if response := helperSendAndWait(t, client, frame); response.Type != protocol.TransferFrameAck {
			t.Fatalf("upload chunk response = %#v", response)
		}
	}
	commit := transferFrameFromBinding(prepare, protocol.TransferFrameCommit)
	if response := helperSendAndWait(t, client, commit); response.Type != protocol.TransferFrameCommitted {
		t.Fatalf("upload commit response = %#v", response)
	}
}

func downloadThroughHelper(
	t *testing.T,
	client *FileHelperClient,
	transferID string,
	path string,
	content []byte,
) []byte {
	t.Helper()
	digest := sha256.Sum256(content)
	prepare := helperPrepareFrame(
		t,
		transferID,
		protocol.TransferDownload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation: fileOperationDownload,
			Path:      path,
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	responses := make(chan protocol.TransferFrame, 8)
	callback := func(response protocol.TransferFrame) error {
		responses <- response
		return nil
	}
	if err := client.HandleTransferFrame(t.Context(), prepare, callback); err != nil {
		t.Fatal(err)
	}
	var downloaded []byte
	for {
		response := waitFileHelperResponse(t, responses)
		switch response.Type {
		case protocol.TransferFrameAccept:
		case protocol.TransferFrameChunk:
			downloaded = append(downloaded, response.Payload...)
			ack := transferFrameFromBinding(prepare, protocol.TransferFrameAck)
			ack.Sequence = response.Sequence
			if err := client.HandleTransferFrame(t.Context(), ack, callback); err != nil {
				t.Fatal(err)
			}
		case protocol.TransferFrameStatus:
			commit := transferFrameFromBinding(prepare, protocol.TransferFrameCommit)
			if err := client.HandleTransferFrame(t.Context(), commit, callback); err != nil {
				t.Fatal(err)
			}
			if committed := waitFileHelperResponse(t, responses); committed.Type != protocol.TransferFrameCommitted {
				t.Fatalf("download commit response = %#v", committed)
			}
			return downloaded
		default:
			t.Fatalf("download response = %#v", response)
		}
	}
}

func rawFileHelperRequest(
	t *testing.T,
	socketPath string,
	profileAlias string,
	frame protocol.TransferFrame,
) protocol.TransferFrame {
	t.Helper()
	connection := rawFileHelperConnection(t, socketPath)
	defer connection.Close()
	writeRawFileHelperTransfer(t, connection, profileAlias, frame)
	return readRawFileHelperTransfer(t, connection)
}

func rawFileHelperConnection(t *testing.T, socketPath string) *net.UnixConn {
	t.Helper()
	connection, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func writeRawFileHelperTransfer(
	t *testing.T,
	connection *net.UnixConn,
	profileAlias string,
	frame protocol.TransferFrame,
) {
	t.Helper()
	payload, err := encodeFileHelperTransferRequest(profileAlias, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileHelperMessage(connection, fileHelperMessage{
		Kind: fileHelperTransferRequest, Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func readRawFileHelperTransfer(
	t *testing.T,
	connection *net.UnixConn,
) protocol.TransferFrame {
	t.Helper()
	message, err := readFileHelperMessage(connection)
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != fileHelperTransferResponse {
		t.Fatalf("raw helper message = %#v", message)
	}
	frame, err := protocol.DecodeTransferFrame(message.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}
