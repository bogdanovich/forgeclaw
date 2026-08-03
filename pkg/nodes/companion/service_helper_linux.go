//go:build linux

package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const serviceHelperHandshakeTimeout = 5 * time.Second

type ServiceHelperClient struct {
	socketPath        string
	expectedServerUID uint32
	expectedServerGID uint32
	descriptors       []nodes.CommandDescriptor
	descriptorByName  map[string]nodes.CommandDescriptor
	snapshotDigest    string
	profiles          map[string]string

	mu     sync.Mutex
	closed bool
}

type serviceHelperExchangeError struct {
	err       error
	uncertain bool
}

func (failure *serviceHelperExchangeError) Error() string { return failure.err.Error() }
func (failure *serviceHelperExchangeError) Unwrap() error { return failure.err }

func NewServiceHelperClient(ctx context.Context, socketPath string) (*ServiceHelperClient, error) {
	return newServiceHelperClient(ctx, socketPath, 0, 0)
}

func newServiceHelperClient(
	ctx context.Context,
	socketPath string,
	expectedServerUID uint32,
	expectedServerGID uint32,
) (*ServiceHelperClient, error) {
	socketPath = filepath.Clean(socketPath)
	if !validFileHelperServicePath(socketPath) || socketPath == string(os.PathSeparator) {
		return nil, errors.New("service helper socket path is invalid")
	}
	client := &ServiceHelperClient{
		socketPath:        socketPath,
		expectedServerUID: expectedServerUID,
		expectedServerGID: expectedServerGID,
	}
	snapshot, err := client.loadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	client.descriptors = cloneCatalog(nodes.CapabilityCatalog{Commands: snapshot.Descriptors}).Commands
	client.descriptorByName = make(map[string]nodes.CommandDescriptor, len(client.descriptors))
	client.profiles = make(map[string]string)
	for _, descriptor := range client.descriptors {
		client.descriptorByName[descriptor.Name] = descriptor
		profile := descriptor.ServiceProfiles[0]
		client.profiles[profile.Alias] = profile.Revision
	}
	client.snapshotDigest = snapshot.SnapshotDigest
	return client, nil
}

func (client *ServiceHelperClient) Descriptors() []nodes.CommandDescriptor {
	if client == nil {
		return nil
	}
	return cloneCatalog(nodes.CapabilityCatalog{Commands: client.descriptors}).Commands
}

func (client *ServiceHelperClient) Status(
	ctx context.Context,
	request ServiceStatusRequest,
) (ServiceStatus, error) {
	response, err := client.invoke(ctx, serviceHelperRequest{
		Kind:    serviceHelperRequestStatus,
		Command: "service.status.v1",
		Profile: request.Profile,
		Service: request.Service,
	})
	if err != nil {
		return ServiceStatus{}, err
	}
	if response.Status == nil || response.Status.Service != request.Service {
		return ServiceStatus{}, errors.New("service helper status binding is invalid")
	}
	if err := client.validateOutput("service.status.v1", response.Status); err != nil {
		return ServiceStatus{}, err
	}
	return *response.Status, nil
}

func (client *ServiceHelperClient) Logs(
	ctx context.Context,
	request ServiceLogRequest,
) (ServiceLogs, error) {
	limits, found := client.logLimits(request.Profile)
	if !found {
		return ServiceLogs{}, &ServiceManagerError{Code: "command_denied"}
	}
	if request.Entries == 0 {
		request.Entries = limits.EntriesMax
	}
	if request.SinceSeconds == 0 {
		request.SinceSeconds = limits.AgeSecondsMax
	}
	response, err := client.invoke(ctx, serviceHelperRequest{
		Kind:         serviceHelperRequestLogs,
		Command:      "service.logs.v1",
		Profile:      request.Profile,
		Service:      request.Service,
		Entries:      request.Entries,
		SinceSeconds: request.SinceSeconds,
	})
	if err != nil {
		return ServiceLogs{}, err
	}
	if response.Logs == nil || response.Logs.Service != request.Service {
		return ServiceLogs{}, errors.New("service helper logs binding is invalid")
	}
	if err := client.validateOutput("service.logs.v1", response.Logs); err != nil {
		return ServiceLogs{}, err
	}
	return *response.Logs, nil
}

func (client *ServiceHelperClient) logLimits(profileAlias string) (nodes.ServiceLogLimits, bool) {
	descriptor, found := client.descriptorByName["service.logs.v1"]
	if !found {
		return nodes.ServiceLogLimits{}, false
	}
	for _, profile := range descriptor.ServiceProfiles {
		if profile.Alias == profileAlias {
			return profile.LogLimits, true
		}
	}
	return nodes.ServiceLogLimits{}, false
}

func (client *ServiceHelperClient) Action(
	ctx context.Context,
	request ServiceActionRequest,
) (ServiceActionResult, error) {
	response, err := client.invoke(ctx, serviceHelperRequest{
		Kind:    serviceHelperRequestAction,
		Command: "service.action.v1",
		Profile: request.Profile,
		Service: request.Service,
		Action:  request.Action,
	})
	if err != nil {
		var managerErr *ServiceManagerError
		if errors.As(err, &managerErr) {
			return ServiceActionResult{}, err
		}
		var exchangeErr *serviceHelperExchangeError
		if !errors.As(err, &exchangeErr) || !exchangeErr.uncertain {
			return ServiceActionResult{}, err
		}
		return ServiceActionResult{
			Service: request.Service,
			Action:  request.Action,
			State:   "unknown",
			Code:    "helper_response_lost",
		}, nil
	}
	if response.Action == nil || response.Action.Service != request.Service ||
		response.Action.Action != request.Action {
		return uncertainServiceHelperAction(request, "helper_response_invalid"), nil
	}
	if err := client.validateOutput("service.action.v1", response.Action); err != nil {
		// A malformed terminal response cannot prove that the accepted helper
		// action did not execute, so it remains explicitly uncertain.
		//nolint:nilerr
		return uncertainServiceHelperAction(request, "helper_response_invalid"), nil
	}
	return *response.Action, nil
}

func uncertainServiceHelperAction(
	request ServiceActionRequest,
	code string,
) ServiceActionResult {
	return ServiceActionResult{
		Service: request.Service,
		Action:  request.Action,
		State:   "unknown",
		Code:    code,
	}
}

func (client *ServiceHelperClient) Close() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	client.closed = true
	client.mu.Unlock()
	return nil
}

func (client *ServiceHelperClient) loadSnapshot(ctx context.Context) (serviceHelperSnapshot, error) {
	requestID, err := randomRequestID()
	if err != nil {
		return serviceHelperSnapshot{}, err
	}
	deadline := time.Now().Add(serviceHelperHandshakeTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	request := serviceHelperRequest{
		Version:   ServiceHelperProtocolVersion,
		Kind:      serviceHelperRequestSnapshot,
		RequestID: requestID,
		Command:   serviceHelperRequestSnapshot,
		ExpiresAt: deadline.Unix(),
	}
	response, err := client.exchange(ctx, request, deadline)
	if err != nil {
		return serviceHelperSnapshot{}, fmt.Errorf("load service helper snapshot: %w", err)
	}
	if response.Kind != serviceHelperRequestSnapshot || response.Snapshot == nil {
		return serviceHelperSnapshot{}, errors.New("service helper returned an unexpected snapshot")
	}
	return *response.Snapshot, nil
}

func (client *ServiceHelperClient) invoke(
	ctx context.Context,
	request serviceHelperRequest,
) (serviceHelperResponse, error) {
	client.mu.Lock()
	closed := client.closed
	client.mu.Unlock()
	if closed {
		return serviceHelperResponse{}, errors.New("service helper client is closed")
	}
	revision, found := client.profiles[request.Profile]
	if !found {
		return serviceHelperResponse{}, &ServiceManagerError{Code: "command_denied"}
	}
	if _, found := client.descriptorByName[request.Command]; !found {
		return serviceHelperResponse{}, &ServiceManagerError{Code: "command_denied"}
	}
	if !client.allows(request) {
		return serviceHelperResponse{}, &ServiceManagerError{Code: "command_denied"}
	}
	if err := ctx.Err(); err != nil {
		return serviceHelperResponse{}, err
	}
	requestID, err := randomRequestID()
	if err != nil {
		return serviceHelperResponse{}, err
	}
	now := time.Now()
	deadline := serviceHelperDeadline(ctx, now)
	if !deadline.After(now) {
		return serviceHelperResponse{}, context.DeadlineExceeded
	}
	request.Version = ServiceHelperProtocolVersion
	request.RequestID = requestID
	request.Revision = revision
	request.SnapshotDigest = client.snapshotDigest
	request.ExpiresAt = deadline.Unix()
	response, err := client.exchangeWithCancel(ctx, request, deadline)
	if err != nil {
		return serviceHelperResponse{}, err
	}
	if response.RequestID != requestID {
		return serviceHelperResponse{}, serviceHelperResponseBindingError(
			request.Kind,
			errors.New("service helper response request binding changed"),
		)
	}
	if response.Kind == "error" {
		return serviceHelperResponse{}, &ServiceManagerError{Code: "helper_denied"}
	}
	if response.Kind != request.Kind {
		return serviceHelperResponse{}, serviceHelperResponseBindingError(
			request.Kind,
			errors.New("service helper response kind changed"),
		)
	}
	return response, nil
}

func serviceHelperResponseBindingError(kind string, err error) error {
	if kind == serviceHelperRequestAction {
		return &serviceHelperExchangeError{err: err, uncertain: true}
	}
	return err
}

func (client *ServiceHelperClient) allows(request serviceHelperRequest) bool {
	descriptor, found := client.descriptorByName[request.Command]
	if !found {
		return false
	}
	for _, profile := range descriptor.ServiceProfiles {
		if profile.Alias != request.Profile {
			continue
		}
		for _, service := range profile.Services {
			if service.Alias != request.Service {
				continue
			}
			switch request.Command {
			case "service.status.v1":
				return service.Status
			case "service.logs.v1":
				return service.Logs &&
					request.Entries <= profile.LogLimits.EntriesMax &&
					request.SinceSeconds <= profile.LogLimits.AgeSecondsMax
			case "service.action.v1":
				for _, action := range service.Actions {
					if action == request.Action {
						return true
					}
				}
			}
		}
	}
	return false
}

func (client *ServiceHelperClient) exchangeWithCancel(
	ctx context.Context,
	request serviceHelperRequest,
	deadline time.Time,
) (serviceHelperResponse, error) {
	connection, err := client.dial(ctx)
	if err != nil {
		return serviceHelperResponse{}, &serviceHelperExchangeError{err: err}
	}
	defer connection.Close()
	if deadlineErr := connection.SetDeadline(deadline); deadlineErr != nil {
		return serviceHelperResponse{}, &serviceHelperExchangeError{err: deadlineErr}
	}
	if writeErr := writeServiceHelperRequest(connection, request); writeErr != nil {
		return serviceHelperResponse{}, &serviceHelperExchangeError{err: writeErr, uncertain: true}
	}
	stopCancel := context.AfterFunc(ctx, func() {
		client.sendCancel(request.RequestID, request.ExpiresAt)
	})
	defer stopCancel()
	response, err := readServiceHelperResponse(connection)
	if err != nil {
		return serviceHelperResponse{}, &serviceHelperExchangeError{err: err, uncertain: true}
	}
	return response, nil
}

func (client *ServiceHelperClient) exchange(
	ctx context.Context,
	request serviceHelperRequest,
	deadline time.Time,
) (serviceHelperResponse, error) {
	connection, err := client.dial(ctx)
	if err != nil {
		return serviceHelperResponse{}, err
	}
	defer connection.Close()
	if deadlineErr := connection.SetDeadline(deadline); deadlineErr != nil {
		return serviceHelperResponse{}, deadlineErr
	}
	if writeErr := writeServiceHelperRequest(connection, request); writeErr != nil {
		return serviceHelperResponse{}, writeErr
	}
	return readServiceHelperResponse(connection)
}

func (client *ServiceHelperClient) sendCancel(targetRequestID string, expiresAt int64) {
	requestID, err := randomRequestID()
	if err != nil {
		return
	}
	deadline := time.Now().Add(serviceHelperHandshakeTimeout)
	request := serviceHelperRequest{
		Version:         ServiceHelperProtocolVersion,
		Kind:            serviceHelperRequestCancel,
		RequestID:       requestID,
		TargetRequestID: targetRequestID,
		Command:         serviceHelperRequestCancel,
		SnapshotDigest:  client.snapshotDigest,
		ExpiresAt:       expiresAt,
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_, _ = client.exchange(ctx, request, deadline)
}

func (client *ServiceHelperClient) dial(ctx context.Context) (*net.UnixConn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", client.socketPath)
	if err != nil {
		return nil, fmt.Errorf("connect service helper: %w", err)
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return nil, errors.New("service helper connection is not Unix")
	}
	peer, err := authorityBrokerPeerCredentials(unixConnection)
	if err != nil || peer.Uid != client.expectedServerUID || peer.Gid != client.expectedServerGID {
		_ = connection.Close()
		return nil, errors.New("service helper server identity is invalid")
	}
	return unixConnection, nil
}

func (client *ServiceHelperClient) validateOutput(command string, value any) error {
	descriptor, found := client.descriptorByName[command]
	if !found || descriptor.ModelContract == nil {
		return errors.New("service helper output descriptor is unavailable")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = nodes.ValidateInvocationOutput(
		descriptor,
		data,
		descriptor.ModelContract.OutputBytesMax,
	)
	return err
}

func serviceHelperDeadline(ctx context.Context, now time.Time) time.Time {
	deadline := now.Add(nodes.MaxExecutionPlanTTL)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	return deadline
}
