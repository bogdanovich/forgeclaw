package companion

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestNormalizeServicePoliciesDefaultsAndBoundsAuthority(t *testing.T) {
	policies, err := normalizeServicePolicies(ServicePolicies{
		"server-services": {
			Enabled:  true,
			Revision: "server-services-v1",
			Manager:  "systemd-system",
			Services: map[string]ServicePolicyEntry{
				"vpn": {
					Unit:                "wg-quick@wg0.service",
					Description:         "Managed VPN service",
					Status:              true,
					Logs:                true,
					Actions:             []nodes.ServiceAction{nodes.ServiceActionRestart},
					ExpectedActiveState: "active",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := policies["server-services"]
	if profile.LogLimits.EntriesMax != nodes.MaxServiceLogEntries ||
		profile.LogLimits.BytesMax != nodes.MaxServiceLogBytes ||
		profile.LogLimits.AgeSecondsMax != nodes.MaxServiceLogAge {
		t.Fatalf("normalized profile = %#v", profile)
	}
}

func TestNormalizeServicePoliciesRejectsUnsafeAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ServicePolicyProfile)
		want   string
	}{
		{
			name: "unsupported manager",
			mutate: func(profile *ServicePolicyProfile) {
				profile.Manager = "launchd-system"
			},
			want: "manager must be systemd-system",
		},
		{
			name: "unit glob",
			mutate: func(profile *ServicePolicyProfile) {
				service := profile.Services["vpn"]
				service.Unit = "wg-quick@*.service"
				profile.Services["vpn"] = service
			},
			want: "exact instantiated .service",
		},
		{
			name: "template unit",
			mutate: func(profile *ServicePolicyProfile) {
				service := profile.Services["vpn"]
				service.Unit = "wg-quick@.service"
				profile.Services["vpn"] = service
			},
			want: "exact instantiated .service",
		},
		{
			name: "restart without expectation",
			mutate: func(profile *ServicePolicyProfile) {
				service := profile.Services["vpn"]
				service.ExpectedActiveState = ""
				profile.Services["vpn"] = service
			},
			want: "require expected_active_state active",
		},
		{
			name: "unbounded logs",
			mutate: func(profile *ServicePolicyProfile) {
				profile.LogLimits = nodes.ServiceLogLimits{
					EntriesMax:    nodes.MaxServiceLogEntries + 1,
					BytesMax:      1,
					AgeSecondsMax: 1,
				}
			},
			want: "malformed service log limits",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := servicePolicyFixture()
			test.mutate(&profile)
			_, err := normalizeServicePolicies(ServicePolicies{"server-services": profile})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("normalizeServicePolicies() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestServiceCapabilitiesRequireEnforcementAndUseExactActionPairs(t *testing.T) {
	profile := servicePolicyFixture()
	profile.Services["app"] = ServicePolicyEntry{
		Unit:                "mintclaw-canary.service",
		Status:              true,
		Actions:             []nodes.ServiceAction{nodes.ServiceActionStart},
		ExpectedActiveState: "active",
	}
	policies, err := normalizeServicePolicies(ServicePolicies{"server-services": profile})
	if err != nil {
		t.Fatal(err)
	}
	descriptors, err := serviceCapabilityDescriptors(policies, serviceEnforcement{}, "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 0 {
		t.Fatalf("descriptors without enforcement = %#v", descriptors)
	}
	descriptors, err = serviceCapabilityDescriptors(
		policies,
		serviceEnforcement{status: true},
		"darwin",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 0 {
		t.Fatalf("Darwin advertised systemd descriptors: %#v", descriptors)
	}
	descriptors, err = serviceCapabilityDescriptors(
		policies,
		serviceEnforcement{actions: true},
		"linux",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 1 || descriptors[0].Name != "service.action.v1" {
		t.Fatalf("descriptors = %#v", descriptors)
	}
	descriptor := descriptors[0]
	data, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "wg-quick@wg0.service") ||
		strings.Contains(string(data), "mintclaw-canary.service") {
		t.Fatalf("descriptor leaked raw unit: %s", data)
	}
	prepareServicePlan(t, descriptor, `{"service":"vpn","action":"restart"}`, true)
	prepareServicePlan(t, descriptor, `{"service":"app","action":"start"}`, true)
	prepareServicePlan(t, descriptor, `{"service":"vpn","action":"start"}`, false)
	prepareServicePlan(t, descriptor, `{"service":"app","action":"restart"}`, false)
}

func TestCloneCatalogIsolatesNestedServiceAuthority(t *testing.T) {
	policies, err := normalizeServicePolicies(ServicePolicies{
		"server-services": servicePolicyFixture(),
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptors, err := serviceCapabilityDescriptors(
		policies,
		serviceEnforcement{actions: true},
		"linux",
	)
	if err != nil {
		t.Fatal(err)
	}
	original := nodes.CapabilityCatalog{Commands: descriptors}
	cloned := cloneCatalog(original)
	cloned.Commands[0].ServiceProfiles[0].Services[0].Alias = "database"
	cloned.Commands[0].ServiceProfiles[0].Services[0].Actions[0] = nodes.ServiceActionStop

	service := original.Commands[0].ServiceProfiles[0].Services[0]
	if service.Alias != "vpn" || service.Actions[0] != nodes.ServiceActionRestart {
		t.Fatalf("clone mutated retained runtime authority: %#v", service)
	}
}

func servicePolicyFixture() ServicePolicyProfile {
	return ServicePolicyProfile{
		Enabled:  true,
		Revision: "server-services-v1",
		Manager:  "systemd-system",
		Services: map[string]ServicePolicyEntry{
			"vpn": {
				Unit:                "wg-quick@wg0.service",
				Description:         "Managed VPN service",
				Status:              true,
				Logs:                true,
				Actions:             []nodes.ServiceAction{nodes.ServiceActionRestart},
				ExpectedActiveState: "active",
			},
		},
	}
}

func prepareServicePlan(
	t *testing.T,
	descriptor nodes.CommandDescriptor,
	input string,
	wantOK bool,
) {
	t.Helper()
	_, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     "inv_test",
		IdempotencyKey:   "idem_test",
		NodeID:           nodes.ID("node_test"),
		CatalogHash:      strings.Repeat("a", 64),
		Command:          descriptor.Name,
		Input:            json.RawMessage(input),
		AgentID:          "main",
		SessionID:        "session",
		ActorID:          "actor",
		TimeoutSeconds:   30,
		OutputLimitBytes: 4096,
	}, descriptor, "local", "policy-v1", time.Unix(1, 0), time.Minute)
	if (err == nil) != wantOK {
		t.Fatalf("PrepareExecutionPlan(%s) error = %v, want OK %v", input, err, wantOK)
	}
}
