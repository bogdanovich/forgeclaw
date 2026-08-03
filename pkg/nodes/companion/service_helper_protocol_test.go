package companion

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestServiceHelperSnapshotBindsAndRedactsAuthority(t *testing.T) {
	config := serviceHelperConfigFixture(t)
	snapshot, err := newServiceHelperSnapshot(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Descriptors) != 3 || snapshot.SnapshotDigest == snapshot.ServiceDigest {
		t.Fatalf("service helper snapshot = %#v", snapshot)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, hidden := range []string{
		"wg-quick@wg0.service",
		config.SystemctlPath,
		config.JournalctlPath,
		config.CompanionCgroup,
	} {
		if strings.Contains(string(data), hidden) {
			t.Fatalf("snapshot leaked hidden authority %q: %s", hidden, data)
		}
	}
	snapshot.Descriptors[0].ServiceProfiles[0].Services[0].Alias = "mutated"
	if snapshot.validate() == nil {
		t.Fatal("tampered service helper snapshot remained valid")
	}
}

func TestServiceHelperProtocolRoundTrip(t *testing.T) {
	request := serviceHelperRequest{
		Version:        ServiceHelperProtocolVersion,
		Kind:           serviceHelperRequestAction,
		RequestID:      "request_1",
		Command:        "service.action.v1",
		Profile:        "server-services",
		Revision:       "server-services-v1",
		Service:        "vpn",
		Action:         nodes.ServiceActionRestart,
		SnapshotDigest: strings.Repeat("a", 64),
		ExpiresAt:      time.Now().Add(time.Minute).Unix(),
	}
	var encoded bytes.Buffer
	if err := writeServiceHelperRequest(&encoded, request); err != nil {
		t.Fatal(err)
	}
	decoded, err := readServiceHelperRequest(&encoded)
	if err != nil || decoded != request {
		t.Fatalf("request round trip = %#v, error %v", decoded, err)
	}
	response := serviceHelperResponse{
		Version:   ServiceHelperProtocolVersion,
		Kind:      serviceHelperRequestAction,
		RequestID: request.RequestID,
		Action: &ServiceActionResult{
			Service: "vpn", Action: nodes.ServiceActionRestart,
			State: "unknown", AcceptedAt: 1, Code: "verification_unavailable",
		},
	}
	if writeErr := writeServiceHelperResponse(&encoded, response); writeErr != nil {
		t.Fatal(writeErr)
	}
	decodedResponse, err := readServiceHelperResponse(&encoded)
	if err != nil || decodedResponse.Action == nil || decodedResponse.Action.State != "unknown" {
		t.Fatalf("response round trip = %#v, error %v", decodedResponse, err)
	}
}

func TestServiceHelperProtocolRequiresStateDependentActionProof(t *testing.T) {
	active := ServiceStatus{
		Service: "vpn", LoadState: "loaded", ActiveState: "active",
		Substate: "running", Enabled: "enabled", ObservedAt: 1,
	}
	valid := ServiceActionResult{
		Service: "vpn", Action: nodes.ServiceActionRestart,
		State: "completed", AcceptedAt: 1, Status: &active,
	}
	tests := []struct {
		name   string
		mutate func(*ServiceActionResult)
	}{
		{name: "missing status", mutate: func(value *ServiceActionResult) { value.Status = nil }},
		{name: "wrong service", mutate: func(value *ServiceActionResult) { value.Status.Service = "other" }},
		{name: "unproven action", mutate: func(value *ServiceActionResult) {
			value.Status.ActiveState = "inactive"
		}},
		{name: "unknown before acceptance", mutate: func(value *ServiceActionResult) {
			value.State = "unknown"
			value.Status = nil
			value.AcceptedAt = 0
			value.Code = "verification_unavailable"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := valid
			status := active
			result.Status = &status
			test.mutate(&result)
			response := serviceHelperResponse{
				Version: ServiceHelperProtocolVersion, Kind: serviceHelperRequestAction,
				RequestID: "request_1", Action: &result,
			}
			if response.validate() == nil {
				t.Fatalf("invalid action terminal accepted: %#v", result)
			}
		})
	}
	response := serviceHelperResponse{
		Version: ServiceHelperProtocolVersion, Kind: serviceHelperRequestAction,
		RequestID: "request_1", Action: &valid,
	}
	if err := response.validate(); err != nil {
		t.Fatalf("valid completed action rejected: %v", err)
	}
}

func TestServiceHelperProtocolRejectsMalformedAuthority(t *testing.T) {
	valid := serviceHelperRequest{
		Version:        ServiceHelperProtocolVersion,
		Kind:           serviceHelperRequestLogs,
		RequestID:      "request_1",
		Command:        "service.logs.v1",
		Profile:        "server-services",
		Revision:       "server-services-v1",
		Service:        "vpn",
		Entries:        1,
		SinceSeconds:   1,
		SnapshotDigest: strings.Repeat("a", 64),
		ExpiresAt:      1,
	}
	tests := []struct {
		name   string
		mutate func(*serviceHelperRequest)
	}{
		{name: "wrong command", mutate: func(value *serviceHelperRequest) { value.Command = "system.exec.v1" }},
		{name: "raw service", mutate: func(value *serviceHelperRequest) { value.Service = "../vpn.service" }},
		{name: "unbounded entries", mutate: func(value *serviceHelperRequest) { value.Entries = 0 }},
		{name: "stale digest shape", mutate: func(value *serviceHelperRequest) { value.SnapshotDigest = "bad" }},
		{name: "action on logs", mutate: func(value *serviceHelperRequest) { value.Action = nodes.ServiceActionStop }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if request.validate() == nil {
				t.Fatalf("malformed request accepted: %#v", request)
			}
		})
	}
}

func TestServiceHelperProtocolRejectsDuplicateAndOversizedJSON(t *testing.T) {
	duplicate := []byte(`{"version":1,"version":1}`)
	var framed bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(duplicate)))
	framed.Write(header[:])
	framed.Write(duplicate)
	if _, err := readServiceHelperRequest(&framed); err == nil {
		t.Fatal("duplicate service helper request key accepted")
	}
	binary.BigEndian.PutUint32(header[:], MaxServiceHelperMessageBytes+1)
	if _, err := readServiceHelperRequest(bytes.NewReader(header[:])); err == nil {
		t.Fatal("oversized service helper request accepted")
	}
}

func TestExistingPrivilegedProtocolsRejectServiceHelperFrames(t *testing.T) {
	request := serviceHelperRequest{
		Version:        ServiceHelperProtocolVersion,
		Kind:           serviceHelperRequestAction,
		RequestID:      "request_1",
		Command:        "service.action.v1",
		Profile:        "server-services",
		Revision:       "server-services-v1",
		Service:        "vpn",
		Action:         nodes.ServiceActionRestart,
		SnapshotDigest: strings.Repeat("a", 64),
		ExpiresAt:      1,
	}
	var encoded bytes.Buffer
	if err := writeServiceHelperRequest(&encoded, request); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	if _, err := readFileHelperMessage(bytes.NewReader(data)); err == nil {
		t.Fatal("file helper accepted a service helper frame")
	}
	var brokerRequest authorityBrokerRequestFrame
	if err := readAuthorityBrokerFrame(bytes.NewReader(data), &brokerRequest); err == nil {
		t.Fatal("owner-shell broker accepted a service helper frame")
	}
}

func serviceHelperConfigFixture(t *testing.T) ServiceHelperServiceConfig {
	t.Helper()
	config, err := NormalizeServiceHelperServiceConfig(ServiceHelperServiceConfig{
		SocketPath:      "/run/mintclaw/node-service-helper.sock",
		AllowedUID:      1000,
		AllowedGID:      1000,
		CompanionCgroup: "/system.slice/mintclaw-node.service",
		SystemctlPath:   "/usr/bin/systemctl",
		JournalctlPath:  "/usr/bin/journalctl",
		Profiles:        ServicePolicies{"server-services": servicePolicyFixture()},
	}, "/")
	if err != nil {
		t.Fatal(err)
	}
	config.systemctlIdentity = strings.Repeat("a", 64)
	config.journalIdentity = strings.Repeat("b", 64)
	return config
}
