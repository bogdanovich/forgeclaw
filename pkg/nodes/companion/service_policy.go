package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const MaxServicePolicyProfiles = nodes.MaxServiceProfiles

const MaxSystemdUnitNameBytes = 256

var systemdServiceUnitPattern = regexp.MustCompile(
	`^[A-Za-z0-9][A-Za-z0-9_.:@-]*\.service$`,
)

var servicePolicyRevisionPattern = regexp.MustCompile(
	`^[A-Za-z0-9][A-Za-z0-9._:-]*$`,
)

type ServicePolicyEntry struct {
	Unit                string                `json:"unit"`
	Description         string                `json:"description,omitempty"`
	Status              bool                  `json:"status,omitempty"`
	Logs                bool                  `json:"logs,omitempty"`
	Actions             []nodes.ServiceAction `json:"actions,omitempty"`
	ExpectedActiveState string                `json:"expected_active_state,omitempty"`
}

type ServicePolicyProfile struct {
	Enabled         bool                          `json:"enabled"`
	Revision        string                        `json:"revision"`
	Manager         string                        `json:"manager"`
	Services        map[string]ServicePolicyEntry `json:"services,omitempty"`
	LogLimits       nodes.ServiceLogLimits        `json:"log_limits,omitempty"`
	normalizedAlias string
}

type ServicePolicies map[string]ServicePolicyProfile

type serviceEnforcement struct {
	status  bool
	logs    bool
	actions bool
}

func (enforcement serviceEnforcement) allows(command string) bool {
	switch command {
	case "service.status.v1":
		return enforcement.status
	case "service.logs.v1":
		return enforcement.logs
	case "service.action.v1":
		return enforcement.actions
	default:
		return false
	}
}

func (enforcement serviceEnforcement) empty() bool {
	return !enforcement.status && !enforcement.logs && !enforcement.actions
}

func normalizeServicePolicies(policies ServicePolicies) (ServicePolicies, error) {
	if policies == nil {
		return nil, nil
	}
	if len(policies) == 0 || len(policies) > MaxServicePolicyProfiles {
		return nil, fmt.Errorf(
			"node_service_policies must contain between 1 and %d profiles",
			MaxServicePolicyProfiles,
		)
	}
	normalized := make(ServicePolicies, len(policies))
	aliases := make(map[string]string, len(policies))
	revisions := make(map[string]string, len(policies))
	for rawAlias, rawProfile := range policies {
		alias := strings.TrimSpace(rawAlias)
		if alias != rawAlias {
			return nil, errors.New("service policy alias must not contain surrounding whitespace")
		}
		if err := (nodes.Alias(alias)).Validate(); err != nil {
			return nil, fmt.Errorf("validate service policy alias: %w", err)
		}
		folded := strings.ToLower(alias)
		if prior, duplicate := aliases[folded]; duplicate {
			return nil, fmt.Errorf("service policy aliases %q and %q collide", prior, alias)
		}
		profile, err := normalizeServicePolicyProfile(alias, rawProfile)
		if err != nil {
			return nil, fmt.Errorf("validate service policy %q: %w", alias, err)
		}
		if prior, duplicate := revisions[profile.Revision]; duplicate {
			return nil, fmt.Errorf("service policies %q and %q use the same revision", prior, alias)
		}
		aliases[folded] = alias
		revisions[profile.Revision] = alias
		normalized[alias] = profile
	}
	return normalized, nil
}

func normalizeServicePolicyProfile(alias string, profile ServicePolicyProfile) (ServicePolicyProfile, error) {
	if !validServicePolicyRevision(profile.Revision) {
		return ServicePolicyProfile{}, errors.New("revision is required and bounded")
	}
	if profile.Manager != "systemd-system" {
		return ServicePolicyProfile{}, errors.New("manager must be systemd-system")
	}
	if len(profile.Services) == 0 || len(profile.Services) > nodes.MaxServicesPerProfile {
		return ServicePolicyProfile{}, fmt.Errorf(
			"services must contain between 1 and %d aliases",
			nodes.MaxServicesPerProfile,
		)
	}
	if profile.LogLimits == (nodes.ServiceLogLimits{}) {
		profile.LogLimits = nodes.ServiceLogLimits{
			EntriesMax:    nodes.MaxServiceLogEntries,
			BytesMax:      nodes.MaxServiceLogBytes,
			AgeSecondsMax: nodes.MaxServiceLogAge,
		}
	}
	if err := profile.LogLimits.Validate(); err != nil {
		return ServicePolicyProfile{}, err
	}
	services := make(map[string]ServicePolicyEntry, len(profile.Services))
	serviceAliases := make(map[string]string, len(profile.Services))
	for rawServiceAlias, rawService := range profile.Services {
		serviceAlias := strings.TrimSpace(rawServiceAlias)
		if serviceAlias != rawServiceAlias {
			return ServicePolicyProfile{}, errors.New("service alias must not contain surrounding whitespace")
		}
		if err := (nodes.Alias(serviceAlias)).Validate(); err != nil {
			return ServicePolicyProfile{}, fmt.Errorf("validate service alias: %w", err)
		}
		folded := strings.ToLower(serviceAlias)
		if prior, duplicate := serviceAliases[folded]; duplicate {
			return ServicePolicyProfile{}, fmt.Errorf("service aliases %q and %q collide", prior, serviceAlias)
		}
		service, err := normalizeServicePolicyEntry(rawService)
		if err != nil {
			return ServicePolicyProfile{}, fmt.Errorf("validate service %q: %w", serviceAlias, err)
		}
		serviceAliases[folded] = serviceAlias
		services[serviceAlias] = service
	}
	profile.Services = services
	profile.normalizedAlias = alias
	return profile, nil
}

func normalizeServicePolicyEntry(service ServicePolicyEntry) (ServicePolicyEntry, error) {
	if len(service.Unit) == 0 || len(service.Unit) > MaxSystemdUnitNameBytes ||
		!systemdServiceUnitPattern.MatchString(service.Unit) ||
		strings.ContainsAny(service.Unit, "/*?[]{}\\") ||
		strings.Contains(service.Unit, "@.service") {
		return ServicePolicyEntry{}, errors.New("unit must be one exact instantiated .service unit")
	}
	if len(service.Description) > nodes.MaxServiceDescription ||
		service.Description != strings.TrimSpace(service.Description) ||
		!utf8.ValidString(service.Description) ||
		strings.IndexFunc(service.Description, unicode.IsControl) >= 0 {
		return ServicePolicyEntry{}, errors.New("description is malformed or exceeds its limit")
	}
	if !service.Status && !service.Logs && len(service.Actions) == 0 {
		return ServicePolicyEntry{}, errors.New("service grants no operation")
	}
	if len(service.Actions) > 6 {
		return ServicePolicyEntry{}, errors.New("service grants too many actions")
	}
	actions := append([]nodes.ServiceAction(nil), service.Actions...)
	sort.Slice(actions, func(i, j int) bool { return actions[i] < actions[j] })
	for index, action := range actions {
		if !action.Valid() || (index > 0 && actions[index-1] == action) {
			return ServicePolicyEntry{}, errors.New("actions must be unique supported operations")
		}
	}
	service.Actions = actions
	if service.ExpectedActiveState != "" && service.ExpectedActiveState != "active" {
		return ServicePolicyEntry{}, errors.New("expected_active_state must be active when configured")
	}
	for _, action := range actions {
		if (action == nodes.ServiceActionStart || action == nodes.ServiceActionRestart ||
			action == nodes.ServiceActionReload) && service.ExpectedActiveState != "active" {
			return ServicePolicyEntry{}, errors.New(
				"start, restart, and reload require expected_active_state active",
			)
		}
	}
	return service, nil
}

func validServicePolicyRevision(revision string) bool {
	return revision != "" && len(revision) <= nodes.MaxPolicyRevisionLength &&
		revision == strings.TrimSpace(revision) &&
		servicePolicyRevisionPattern.MatchString(revision)
}

func hasEnabledServicePolicy(policies ServicePolicies) bool {
	for _, profile := range policies {
		if profile.Enabled {
			return true
		}
	}
	return false
}

// serviceCapabilityDescriptors returns only commands backed by an enforcement
// source. Configured authority alone never becomes an advertised capability.
func serviceCapabilityDescriptors(
	policies ServicePolicies,
	enforcement serviceEnforcement,
	platform string,
) ([]nodes.CommandDescriptor, error) {
	profiles := make([]ServicePolicyProfile, 0, len(policies))
	for _, profile := range policies {
		if profile.Enabled {
			profiles = append(profiles, profile)
		}
	}
	if platform != "linux" || len(profiles) == 0 || enforcement.empty() {
		return []nodes.CommandDescriptor{}, nil
	}
	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].normalizedAlias < profiles[j].normalizedAlias
	})
	authority, err := json.Marshal(profiles)
	if err != nil {
		return nil, fmt.Errorf("encode service capability authority: %w", err)
	}
	digest := sha256.Sum256(authority)
	descriptors := make([]nodes.CommandDescriptor, 0, 3)
	for _, command := range []string{"service.status.v1", "service.logs.v1", "service.action.v1"} {
		if !enforcement.allows(command) {
			continue
		}
		projected := projectServiceProfiles(profiles, command)
		if len(projected) == 0 {
			continue
		}
		descriptor := serviceCapabilityDescriptor(command, hex.EncodeToString(digest[:]), projected)
		if err := descriptor.Validate(); err != nil {
			return nil, err
		}
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, nil
}

func projectServiceProfiles(
	profiles []ServicePolicyProfile,
	command string,
) []nodes.ServiceProfileDescriptor {
	result := make([]nodes.ServiceProfileDescriptor, 0, len(profiles))
	for _, profile := range profiles {
		services := make([]nodes.ServiceDescriptor, 0, len(profile.Services))
		aliases := make([]string, 0, len(profile.Services))
		for alias := range profile.Services {
			aliases = append(aliases, alias)
		}
		sort.Strings(aliases)
		for _, alias := range aliases {
			service := profile.Services[alias]
			projected := nodes.ServiceDescriptor{Alias: alias, Description: service.Description}
			switch command {
			case "service.status.v1":
				projected.Status = service.Status
			case "service.logs.v1":
				projected.Logs = service.Logs
			case "service.action.v1":
				projected.Actions = append([]nodes.ServiceAction(nil), service.Actions...)
			}
			if projected.Status || projected.Logs || len(projected.Actions) > 0 {
				services = append(services, projected)
			}
		}
		if len(services) > 0 {
			result = append(result, nodes.ServiceProfileDescriptor{
				Alias:          profile.normalizedAlias,
				Revision:       profile.Revision,
				Manager:        "systemd",
				Services:       services,
				LogLimits:      profile.LogLimits,
				ActionApproval: "required",
			})
		}
	}
	return result
}

func serviceCapabilityDescriptor(
	name string,
	authorityDigest string,
	profiles []nodes.ServiceProfileDescriptor,
) nodes.CommandDescriptor {
	risk := nodes.RiskRead
	approval := ""
	outputBytesMax := 64 * 1024
	if name == "service.logs.v1" {
		outputBytesMax = 0
		for _, profile := range profiles {
			outputBytesMax = max(outputBytesMax, profile.LogLimits.BytesMax)
		}
	}
	if name == "service.action.v1" {
		risk = nodes.RiskPrivileged
		approval = "each_command"
	}
	return nodes.CommandDescriptor{
		Name:             name,
		InputSchema:      nodes.ServiceCommandInputSchema(name, profiles),
		OutputSchema:     nodes.ServiceCommandOutputSchema(name),
		Risk:             risk,
		SupportsProgress: true,
		SupportsCancel:   true,
		ModelContract: &nodes.CommandModelContract{
			Availability:      nodes.ModelUnavailable,
			TimeoutSecondsMax: nodes.MaxInvocationTimeout,
			OutputBytesMax:    outputBytesMax,
			ResultKind:        "json",
			AuthorityDigest:   authorityDigest,
			ApprovalMode:      approval,
			Guidance:          []string{},
			Examples:          []json.RawMessage{},
		},
		ServiceProfiles: nodes.CloneServiceProfileDescriptors(profiles),
	}
}
