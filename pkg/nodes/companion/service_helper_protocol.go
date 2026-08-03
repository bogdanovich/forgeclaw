package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	serviceHelperRequestSnapshot = "snapshot"
	serviceHelperRequestStatus   = "status"
	serviceHelperRequestLogs     = "logs"
	serviceHelperRequestAction   = "action"
	serviceHelperRequestCancel   = "cancel"
)

var (
	serviceHelperRequestIDPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	serviceHelperActionCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

type serviceHelperSnapshot struct {
	Descriptors    []nodes.CommandDescriptor `json:"descriptors"`
	ServiceDigest  string                    `json:"service_digest"`
	SnapshotDigest string                    `json:"snapshot_digest"`
}

type serviceHelperRequest struct {
	Version         int                 `json:"version"`
	Kind            string              `json:"kind"`
	RequestID       string              `json:"request_id"`
	TargetRequestID string              `json:"target_request_id,omitempty"`
	Command         string              `json:"command"`
	Profile         string              `json:"profile,omitempty"`
	Revision        string              `json:"revision,omitempty"`
	Service         string              `json:"service,omitempty"`
	Action          nodes.ServiceAction `json:"action,omitempty"`
	Entries         int                 `json:"entries,omitempty"`
	SinceSeconds    int                 `json:"since_seconds,omitempty"`
	SnapshotDigest  string              `json:"snapshot_digest,omitempty"`
	ExpiresAt       int64               `json:"expires_at"`
}

type serviceHelperResponse struct {
	Version   int                    `json:"version"`
	Kind      string                 `json:"kind"`
	RequestID string                 `json:"request_id"`
	Code      string                 `json:"code,omitempty"`
	Snapshot  *serviceHelperSnapshot `json:"snapshot,omitempty"`
	Status    *ServiceStatus         `json:"status,omitempty"`
	Logs      *ServiceLogs           `json:"logs,omitempty"`
	Action    *ServiceActionResult   `json:"action,omitempty"`
}

func readServiceHelperRequest(reader io.Reader) (serviceHelperRequest, error) {
	var request serviceHelperRequest
	if err := readAuthorityBrokerFrame(reader, &request); err != nil {
		return serviceHelperRequest{}, err
	}
	if err := request.validate(); err != nil {
		return serviceHelperRequest{}, err
	}
	return request, nil
}

func writeServiceHelperRequest(writer io.Writer, request serviceHelperRequest) error {
	if err := request.validate(); err != nil {
		return err
	}
	return writeAuthorityBrokerFrame(writer, request)
}

func readServiceHelperResponse(reader io.Reader) (serviceHelperResponse, error) {
	var response serviceHelperResponse
	if err := readAuthorityBrokerFrame(reader, &response); err != nil {
		return serviceHelperResponse{}, err
	}
	if err := response.validate(); err != nil {
		return serviceHelperResponse{}, err
	}
	return response, nil
}

func writeServiceHelperResponse(writer io.Writer, response serviceHelperResponse) error {
	if err := response.validate(); err != nil {
		return err
	}
	return writeAuthorityBrokerFrame(writer, response)
}

func (request serviceHelperRequest) validate() error {
	if request.Version != ServiceHelperProtocolVersion ||
		!serviceHelperRequestIDPattern.MatchString(request.RequestID) ||
		request.ExpiresAt <= 0 {
		return errors.New("service helper request envelope is invalid")
	}
	switch request.Kind {
	case serviceHelperRequestSnapshot:
		if request.Command != serviceHelperRequestSnapshot || request.hasAuthorityFields() ||
			request.TargetRequestID != "" {
			return errors.New("service helper snapshot request is invalid")
		}
	case serviceHelperRequestStatus, serviceHelperRequestLogs, serviceHelperRequestAction:
		if !validFileHelperDigest(request.SnapshotDigest) ||
			(nodes.Alias(request.Profile)).Validate() != nil ||
			!validServicePolicyRevision(request.Revision) ||
			(nodes.Alias(request.Service)).Validate() != nil ||
			request.TargetRequestID != "" {
			return errors.New("service helper authority binding is invalid")
		}
		if err := request.validateCommand(); err != nil {
			return err
		}
	case serviceHelperRequestCancel:
		if request.Command != serviceHelperRequestCancel ||
			!serviceHelperRequestIDPattern.MatchString(request.TargetRequestID) ||
			!validFileHelperDigest(request.SnapshotDigest) ||
			request.Profile != "" || request.Revision != "" || request.Service != "" ||
			request.Action != "" || request.Entries != 0 || request.SinceSeconds != 0 {
			return errors.New("service helper cancel request is invalid")
		}
	default:
		return errors.New("service helper request kind is unsupported")
	}
	return nil
}

func (request serviceHelperRequest) validateCommand() error {
	switch request.Kind {
	case serviceHelperRequestStatus:
		if request.Command != "service.status.v1" || request.Action != "" ||
			request.Entries != 0 || request.SinceSeconds != 0 {
			return errors.New("service helper status request is invalid")
		}
	case serviceHelperRequestLogs:
		if request.Command != "service.logs.v1" || request.Action != "" ||
			request.Entries <= 0 || request.Entries > nodes.MaxServiceLogEntries ||
			request.SinceSeconds <= 0 || request.SinceSeconds > nodes.MaxServiceLogAge {
			return errors.New("service helper logs request is invalid")
		}
	case serviceHelperRequestAction:
		if request.Command != "service.action.v1" || !request.Action.Valid() ||
			request.Entries != 0 || request.SinceSeconds != 0 {
			return errors.New("service helper action request is invalid")
		}
	default:
		return errors.New("service helper command is unsupported")
	}
	return nil
}

func (request serviceHelperRequest) hasAuthorityFields() bool {
	return request.Profile != "" || request.Revision != "" || request.Service != "" ||
		request.Action != "" || request.Entries != 0 || request.SinceSeconds != 0 ||
		request.SnapshotDigest != ""
}

func (response serviceHelperResponse) validate() error {
	if response.Version != ServiceHelperProtocolVersion ||
		!serviceHelperRequestIDPattern.MatchString(response.RequestID) {
		return errors.New("service helper response envelope is invalid")
	}
	payloads := 0
	for _, present := range []bool{
		response.Snapshot != nil,
		response.Status != nil,
		response.Logs != nil,
		response.Action != nil,
	} {
		if present {
			payloads++
		}
	}
	if response.Kind == "error" {
		if payloads != 0 || !fileHelperCodePattern.MatchString(response.Code) {
			return errors.New("service helper error response is invalid")
		}
		return nil
	}
	if response.Code != "" {
		return errors.New("service helper success response carries an error")
	}
	switch response.Kind {
	case serviceHelperRequestSnapshot:
		if payloads != 1 || response.Snapshot == nil {
			return errors.New("service helper snapshot response is invalid")
		}
		return response.Snapshot.validate()
	case serviceHelperRequestStatus:
		if payloads != 1 || response.Status == nil {
			return errors.New("service helper status response is invalid")
		}
	case serviceHelperRequestLogs:
		if payloads != 1 || response.Logs == nil {
			return errors.New("service helper logs response is invalid")
		}
	case serviceHelperRequestAction:
		if payloads != 1 || response.Action == nil {
			return errors.New("service helper action response is invalid")
		}
		if response.Action.Code != "" && !serviceHelperActionCodePattern.MatchString(response.Action.Code) {
			return errors.New("service helper action response code is invalid")
		}
		return response.Action.validateTerminal()
	case serviceHelperRequestCancel:
		if payloads != 0 {
			return errors.New("service helper cancel response is invalid")
		}
	default:
		return errors.New("service helper response kind is unsupported")
	}
	return nil
}

func newServiceHelperSnapshot(config ServiceHelperServiceConfig) (serviceHelperSnapshot, error) {
	descriptors, err := config.Descriptors()
	if err != nil {
		return serviceHelperSnapshot{}, err
	}
	serviceDigest, err := serviceHelperServiceDigest(config)
	if err != nil {
		return serviceHelperSnapshot{}, err
	}
	binding, err := json.Marshal(struct {
		Descriptors   []nodes.CommandDescriptor `json:"descriptors"`
		ServiceDigest string                    `json:"service_digest"`
	}{Descriptors: descriptors, ServiceDigest: serviceDigest})
	if err != nil {
		return serviceHelperSnapshot{}, fmt.Errorf("encode service helper snapshot: %w", err)
	}
	digest := sha256.Sum256(binding)
	snapshot := serviceHelperSnapshot{
		Descriptors:    descriptors,
		ServiceDigest:  serviceDigest,
		SnapshotDigest: hex.EncodeToString(digest[:]),
	}
	if err := snapshot.validate(); err != nil {
		return serviceHelperSnapshot{}, err
	}
	return snapshot, nil
}

func (snapshot serviceHelperSnapshot) validate() error {
	if !validFileHelperDigest(snapshot.ServiceDigest) ||
		!validFileHelperDigest(snapshot.SnapshotDigest) ||
		validateServiceHelperDescriptors(snapshot.Descriptors) != nil {
		return errors.New("service helper snapshot is invalid")
	}
	binding, err := json.Marshal(struct {
		Descriptors   []nodes.CommandDescriptor `json:"descriptors"`
		ServiceDigest string                    `json:"service_digest"`
	}{Descriptors: snapshot.Descriptors, ServiceDigest: snapshot.ServiceDigest})
	if err != nil {
		return errors.New("service helper snapshot is invalid")
	}
	digest := sha256.Sum256(binding)
	if hex.EncodeToString(digest[:]) != snapshot.SnapshotDigest {
		return errors.New("service helper snapshot digest is invalid")
	}
	return nil
}
