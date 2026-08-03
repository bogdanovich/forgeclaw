package companion

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestNormalizeServiceHelperConfigFailsClosed(t *testing.T) {
	base := ServiceHelperServiceConfig{
		SocketPath:      "/run/mintclaw/node-service-helper.sock",
		AllowedUID:      1000,
		AllowedGID:      1000,
		CompanionCgroup: "/system.slice/mintclaw-node.service",
		SystemctlPath:   "/usr/bin/systemctl",
		JournalctlPath:  "/usr/bin/journalctl",
		Profiles:        ServicePolicies{"server-services": servicePolicyFixture()},
	}
	tests := []struct {
		name   string
		mutate func(*ServiceHelperServiceConfig)
		want   string
	}{
		{
			name:   "root peer",
			mutate: func(value *ServiceHelperServiceConfig) { value.AllowedUID = 0 },
			want:   "unprivileged",
		},
		{
			name:   "root cgroup",
			mutate: func(value *ServiceHelperServiceConfig) { value.CompanionCgroup = "/" },
			want:   "cgroup",
		},
		{
			name:   "relative manager",
			mutate: func(value *ServiceHelperServiceConfig) { value.SystemctlPath = "systemctl" },
			want:   "systemctl",
		},
		{name: "multiple profiles", mutate: func(value *ServiceHelperServiceConfig) {
			other := servicePolicyFixture()
			other.Revision = "other-v1"
			value.Profiles["other"] = other
		}, want: "exactly one"},
		{name: "no action", mutate: func(value *ServiceHelperServiceConfig) {
			profile := value.Profiles["server-services"]
			service := profile.Services["vpn"]
			service.Actions = nil
			service.ExpectedActiveState = ""
			profile.Services["vpn"] = service
			value.Profiles["server-services"] = profile
		}, want: "grants no action"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := cloneServiceHelperConfig(base)
			test.mutate(&config)
			_, err := NormalizeServiceHelperServiceConfig(config, "/")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NormalizeServiceHelperServiceConfig() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestServiceHelperDescriptorsBindHelperAuthority(t *testing.T) {
	config := serviceHelperConfigFixture(t)
	descriptors, err := config.Descriptors()
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 3 {
		t.Fatalf("service helper descriptors = %#v", descriptors)
	}
	for _, descriptor := range descriptors {
		if descriptor.Name == "service.action.v1" {
			if descriptor.Risk != nodes.RiskPrivileged ||
				descriptor.ModelContract.ApprovalMode != "each_command" {
				t.Fatalf("service action contract = %#v", descriptor)
			}
		}
	}
	data, err := json.Marshal(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "wg-quick@wg0.service") ||
		strings.Contains(string(data), config.SystemctlPath) {
		t.Fatalf("service helper descriptors leaked hidden authority: %s", data)
	}
	changed := cloneServiceHelperConfig(config)
	changed.SystemctlPath = "/bin/systemctl"
	changedDigest, err := serviceHelperServiceDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	originalDigest, err := serviceHelperServiceDigest(config)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == originalDigest {
		t.Fatal("service helper executable identity did not change authority digest")
	}
}

func cloneServiceHelperConfig(config ServiceHelperServiceConfig) ServiceHelperServiceConfig {
	clone := config
	clone.Profiles = make(ServicePolicies, len(config.Profiles))
	for alias, profile := range config.Profiles {
		profile.Services = make(map[string]ServicePolicyEntry, len(profile.Services))
		for serviceAlias, service := range config.Profiles[alias].Services {
			service.Actions = append([]nodes.ServiceAction(nil), service.Actions...)
			profile.Services[serviceAlias] = service
		}
		clone.Profiles[alias] = profile
	}
	return clone
}
