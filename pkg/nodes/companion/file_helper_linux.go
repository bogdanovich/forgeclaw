//go:build linux

package companion

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

const (
	fileHelperHandshakeTimeout = 5 * time.Second
	fileHelperWriteTimeout     = 30 * time.Second
	fileHelperSocketBuffer     = protocol.MaxTransferChunkBytes + 4096
)

type FileHelperClient struct {
	socketPath        string
	expectedServerUID uint32
	expectedServerGID uint32
	descriptors       []nodes.CommandDescriptor
	serviceDigest     string
	aliasesByRevision map[string]string

	mu      sync.Mutex
	session *fileHelperClientSession
	closed  bool
}

type fileHelperTransferCallback struct {
	binding protocol.TransferFrame
	send    func(protocol.TransferFrame) error
}

type fileHelperClientSession struct {
	ctx        context.Context
	connection *net.UnixConn

	writeMu sync.Mutex
	mu      sync.Mutex
	closed  bool
	err     error
	close   sync.Once
	active  map[string]fileHelperTransferCallback
}

func NewFileHelperClient(
	ctx context.Context,
	socketPath string,
) (*FileHelperClient, error) {
	return newFileHelperClient(ctx, socketPath, 0, 0)
}

func newFileHelperClient(
	ctx context.Context,
	socketPath string,
	expectedServerUID uint32,
	expectedServerGID uint32,
) (*FileHelperClient, error) {
	socketPath = filepath.Clean(socketPath)
	if !validFileHelperServicePath(socketPath) || socketPath == string(os.PathSeparator) {
		return nil, errors.New("file helper socket path is invalid")
	}
	client := &FileHelperClient{
		socketPath:        socketPath,
		expectedServerUID: expectedServerUID,
		expectedServerGID: expectedServerGID,
	}
	descriptors, serviceDigest, err := client.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	client.descriptors = descriptors
	client.serviceDigest = serviceDigest
	client.aliasesByRevision = make(map[string]string)
	for _, profile := range descriptors[0].FileProfiles {
		client.aliasesByRevision[profile.Revision] = profile.Alias
	}
	return client, nil
}

func (client *FileHelperClient) Descriptors() []nodes.CommandDescriptor {
	if client == nil {
		return nil
	}
	return cloneFileCapabilityDescriptors(client.descriptors)
}

func (client *FileHelperClient) HandleTransferFrame(
	ctx context.Context,
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	if client == nil || send == nil {
		return errors.New("file helper client is unavailable")
	}
	if err := frame.Validate(); err != nil {
		return err
	}
	profileAlias, allowed := client.aliasesByRevision[frame.PolicyRevision]
	if !allowed {
		return errors.New("file helper policy revision is unavailable")
	}
	session, err := client.ensureSession(ctx)
	if err != nil {
		return err
	}
	return session.sendTransfer(client.serviceDigest, profileAlias, frame, send)
}

func (client *FileHelperClient) Close() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	client.closed = true
	session := client.session
	client.session = nil
	client.mu.Unlock()
	if session == nil {
		return nil
	}
	return session.finish(errors.New("file helper client closed"))
}

func (client *FileHelperClient) snapshot(
	ctx context.Context,
) ([]nodes.CommandDescriptor, string, error) {
	connection, err := client.dial(ctx)
	if err != nil {
		return nil, "", err
	}
	defer connection.Close()
	deadline := time.Now().Add(fileHelperHandshakeTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if deadlineErr := connection.SetDeadline(deadline); deadlineErr != nil {
		return nil, "", deadlineErr
	}
	if writeErr := writeFileHelperMessage(connection, fileHelperMessage{
		Kind: fileHelperSnapshotRequest,
	}); writeErr != nil {
		return nil, "", fmt.Errorf("request file helper snapshot: %w", writeErr)
	}
	message, err := readFileHelperMessage(connection)
	if err != nil {
		return nil, "", fmt.Errorf("read file helper snapshot: %w", err)
	}
	if message.Kind == fileHelperErrorResponse {
		return nil, "", errors.New("file helper denied snapshot")
	}
	if message.Kind != fileHelperSnapshotResponse {
		return nil, "", errors.New("file helper returned an unexpected snapshot response")
	}
	snapshot, err := decodeFileHelperSnapshot(message.Payload)
	if err != nil {
		return nil, "", err
	}
	descriptors, err := snapshot.descriptors()
	if err != nil {
		return nil, "", err
	}
	return cloneFileCapabilityDescriptors(descriptors), snapshot.ServiceDigest, nil
}

func (client *FileHelperClient) ensureSession(
	ctx context.Context,
) (*fileHelperClientSession, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil, errors.New("file helper client is closed")
	}
	if client.session != nil && client.session.available() && client.session.ctx.Err() == nil {
		return client.session, nil
	}
	connection, err := client.dial(ctx)
	if err != nil {
		return nil, err
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	session := &fileHelperClientSession{
		ctx: ctx, connection: connection,
		active: make(map[string]fileHelperTransferCallback),
	}
	client.session = session
	go session.readResponses()
	go func() {
		<-ctx.Done()
		_ = session.finish(ctx.Err())
	}()
	return session, nil
}

func (client *FileHelperClient) dial(ctx context.Context) (*net.UnixConn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", client.socketPath)
	if err != nil {
		return nil, fmt.Errorf("connect file helper: %w", err)
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return nil, errors.New("file helper connection is not Unix")
	}
	peer, err := authorityBrokerPeerCredentials(unixConnection)
	if err != nil ||
		peer.Uid != client.expectedServerUID ||
		peer.Gid != client.expectedServerGID {
		_ = connection.Close()
		return nil, errors.New("file helper server identity is invalid")
	}
	return unixConnection, nil
}

func (session *fileHelperClientSession) available() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return !session.closed && session.err == nil
}

func (session *fileHelperClientSession) sendTransfer(
	serviceDigest string,
	profileAlias string,
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	encoded, err := encodeFileHelperTransferRequest(serviceDigest, profileAlias, frame)
	if err != nil {
		return err
	}
	session.mu.Lock()
	if session.closed || session.err != nil {
		result := session.err
		if result == nil {
			result = errors.New("file helper session is closed")
		}
		session.mu.Unlock()
		return result
	}
	if existing, found := session.active[frame.TransferID]; found &&
		!existing.binding.SameBinding(frame) {
		session.mu.Unlock()
		return errors.New("file helper transfer binding changed")
	}
	session.active[frame.TransferID] = fileHelperTransferCallback{
		binding: frame,
		send:    send,
	}
	session.mu.Unlock()
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	if err := session.connection.SetWriteDeadline(time.Now().Add(fileHelperWriteTimeout)); err != nil {
		return err
	}
	if err := writeFileHelperMessage(session.connection, fileHelperMessage{
		Kind: fileHelperTransferRequest, Payload: encoded,
	}); err != nil {
		_ = session.finish(fmt.Errorf("write file helper transfer: %w", err))
		return fmt.Errorf("write file helper transfer: %w", err)
	}
	return session.connection.SetWriteDeadline(time.Time{})
}

func (session *fileHelperClientSession) readResponses() {
	for {
		message, err := readFileHelperMessage(session.connection)
		if err != nil {
			_ = session.finish(fmt.Errorf("read file helper response: %w", err))
			return
		}
		if message.Kind == fileHelperErrorResponse {
			_ = session.finish(errors.New("file helper rejected transfer request"))
			return
		}
		if message.Kind != fileHelperTransferResponse {
			_ = session.finish(errors.New("file helper returned an unexpected response"))
			return
		}
		frame, err := protocol.DecodeTransferFrame(message.Payload)
		if err != nil {
			_ = session.finish(err)
			return
		}
		session.mu.Lock()
		callback, found := session.active[frame.TransferID]
		if found && terminalFileHelperFrame(frame.Type) {
			delete(session.active, frame.TransferID)
		}
		session.mu.Unlock()
		if !found || !callback.binding.SameBinding(frame) {
			_ = session.finish(errors.New("file helper response binding is invalid"))
			return
		}
		if err := callback.send(frame); err != nil {
			_ = session.finish(fmt.Errorf("forward file helper response: %w", err))
			return
		}
	}
}

func terminalFileHelperFrame(frameType protocol.TransferFrameType) bool {
	switch frameType {
	case protocol.TransferFrameDeny,
		protocol.TransferFrameCommitted,
		protocol.TransferFrameFailure:
		return true
	default:
		return false
	}
}

func (session *fileHelperClientSession) finish(reason error) error {
	var closeErr error
	session.close.Do(func() {
		session.mu.Lock()
		session.closed = true
		if session.err == nil {
			session.err = reason
		}
		session.active = make(map[string]fileHelperTransferCallback)
		session.mu.Unlock()
		closeErr = session.connection.Close()
	})
	return closeErr
}

type fileHelperServer struct {
	config        FileHelperServiceConfig
	runtime       *FileTransferRuntime
	serviceDigest string
	snapshot      []byte
}

func newFileHelperServer(
	config FileHelperServiceConfig,
	runtime *FileTransferRuntime,
) (*fileHelperServer, error) {
	if !config.normalized || runtime == nil {
		return nil, errors.New("file helper server configuration is incomplete")
	}
	descriptors := runtime.Descriptors()
	if err := validateFileHelperDescriptors(descriptors); err != nil {
		return nil, err
	}
	serviceDigest, err := fileHelperServiceDigest(config)
	if err != nil {
		return nil, err
	}
	snapshot, err := encodeFileHelperSnapshot(descriptors, serviceDigest)
	if err != nil {
		return nil, err
	}
	return &fileHelperServer{
		config: config, runtime: runtime,
		serviceDigest: serviceDigest, snapshot: snapshot,
	}, nil
}

func (server *fileHelperServer) Serve(
	ctx context.Context,
	listener *net.UnixListener,
) error {
	if server == nil || listener == nil {
		return errors.New("file helper server is unavailable")
	}
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	var workers sync.WaitGroup
	defer workers.Wait()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept file helper connection: %w", err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer connection.Close()
			server.handleConnection(ctx, connection)
		}()
	}
}

func (server *fileHelperServer) handleConnection(
	serverContext context.Context,
	connection *net.UnixConn,
) {
	peer, err := authorityBrokerPeerCredentials(connection)
	if err != nil ||
		peer.Uid != server.config.AllowedUID ||
		peer.Gid != server.config.AllowedGID {
		return
	}
	if err := connection.SetWriteBuffer(fileHelperSocketBuffer); err != nil {
		return
	}
	connectionContext, cancel := context.WithCancel(serverContext)
	defer cancel()
	stopClose := context.AfterFunc(connectionContext, func() { _ = connection.Close() })
	defer stopClose()
	writer := &fileHelperConnectionWriter{connection: connection}
	for {
		message, readErr := readFileHelperMessage(connection)
		if readErr != nil {
			return
		}
		switch message.Kind {
		case fileHelperSnapshotRequest:
			_ = writer.write(fileHelperMessage{
				Kind: fileHelperSnapshotResponse, Payload: server.snapshot,
			})
			return
		case fileHelperTransferRequest:
			transfer, decodeErr := decodeFileHelperTransferRequest(message.Payload)
			if decodeErr != nil {
				_ = writer.writeError("INVALID_REQUEST")
				return
			}
			profile, found := server.config.Profiles[transfer.ProfileAlias]
			denialCode := "PROFILE_DENIED"
			if transfer.ServiceDigest != server.serviceDigest {
				denialCode = "AUTHORITY_STALE"
				found = false
			}
			if !found || !profile.Enabled || profile.Revision != transfer.Frame.PolicyRevision {
				_ = writer.writeTransfer(responseTransferFrame(
					transfer.Frame,
					protocol.TransferFrameDeny,
					mustFileTransferResult(fileTransferResult{
						State: FileTransferFailed,
						Code:  denialCode,
					}),
				))
				continue
			}
			if handleErr := server.runtime.HandleTransferFrame(
				connectionContext,
				transfer.Frame,
				writer.writeTransfer,
			); handleErr != nil {
				_ = writer.writeError("INVALID_REQUEST")
				return
			}
		default:
			_ = writer.writeError("INVALID_REQUEST")
			return
		}
	}
}

type fileHelperConnectionWriter struct {
	connection *net.UnixConn
	mu         sync.Mutex
}

func (writer *fileHelperConnectionWriter) writeTransfer(
	frame protocol.TransferFrame,
) error {
	payload, err := protocol.EncodeTransferFrame(frame)
	if err != nil {
		return err
	}
	return writer.write(fileHelperMessage{
		Kind: fileHelperTransferResponse, Payload: payload,
	})
}

func (writer *fileHelperConnectionWriter) writeError(code string) error {
	return writer.write(fileHelperMessage{
		Kind: fileHelperErrorResponse, Payload: []byte(code),
	})
}

func (writer *fileHelperConnectionWriter) write(message fileHelperMessage) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if err := writer.connection.SetWriteDeadline(time.Now().Add(fileHelperWriteTimeout)); err != nil {
		return err
	}
	if err := writeFileHelperMessage(writer.connection, message); err != nil {
		return err
	}
	return writer.connection.SetWriteDeadline(time.Time{})
}
