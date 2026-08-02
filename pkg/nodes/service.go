package nodes

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	MaxServiceProfiles    = 32
	MaxServicesPerProfile = 64
	MaxServiceDescription = 256
	MaxServiceLogEntries  = 500
	MaxServiceLogBytes    = 256 * 1024
	MaxServiceLogAge      = 7 * 24 * 60 * 60
)

type ServiceAction string

const (
	ServiceActionStart   ServiceAction = "start"
	ServiceActionStop    ServiceAction = "stop"
	ServiceActionRestart ServiceAction = "restart"
	ServiceActionReload  ServiceAction = "reload"
	ServiceActionEnable  ServiceAction = "enable"
	ServiceActionDisable ServiceAction = "disable"
)

func (action ServiceAction) Valid() bool {
	switch action {
	case ServiceActionStart, ServiceActionStop, ServiceActionRestart,
		ServiceActionReload, ServiceActionEnable, ServiceActionDisable:
		return true
	default:
		return false
	}
}

type ServiceLogLimits struct {
	EntriesMax    int `json:"entries_max"`
	BytesMax      int `json:"bytes_max"`
	AgeSecondsMax int `json:"age_seconds_max"`
}

func (limits ServiceLogLimits) Validate() error {
	if limits.EntriesMax <= 0 || limits.EntriesMax > MaxServiceLogEntries ||
		limits.BytesMax <= 0 || limits.BytesMax > MaxServiceLogBytes ||
		limits.AgeSecondsMax <= 0 || limits.AgeSecondsMax > MaxServiceLogAge {
		return fmt.Errorf("%w: malformed service log limits", ErrInvalidCapability)
	}
	return nil
}

type ServiceDescriptor struct {
	Alias       string          `json:"alias"`
	Description string          `json:"description,omitempty"`
	Status      bool            `json:"status,omitempty"`
	Logs        bool            `json:"logs,omitempty"`
	Actions     []ServiceAction `json:"actions,omitempty"`
}

func (service ServiceDescriptor) Validate() error {
	if err := (Alias(service.Alias)).Validate(); err != nil ||
		len(service.Description) > MaxServiceDescription ||
		!utf8.ValidString(service.Description) ||
		strings.IndexFunc(service.Description, containsModelControlRune) >= 0 ||
		service.Description != strings.TrimSpace(service.Description) ||
		(!service.Status && !service.Logs && len(service.Actions) == 0) {
		return fmt.Errorf("%w: malformed service descriptor", ErrInvalidCapability)
	}
	if len(service.Actions) > 6 || !sort.SliceIsSorted(service.Actions, func(i, j int) bool {
		return service.Actions[i] < service.Actions[j]
	}) {
		return fmt.Errorf("%w: malformed service actions", ErrInvalidCapability)
	}
	seen := make(map[ServiceAction]struct{}, len(service.Actions))
	for _, action := range service.Actions {
		if !action.Valid() {
			return fmt.Errorf("%w: unsupported service action", ErrInvalidCapability)
		}
		if _, duplicate := seen[action]; duplicate {
			return fmt.Errorf("%w: duplicate service action", ErrInvalidCapability)
		}
		seen[action] = struct{}{}
	}
	return nil
}

// ServiceProfileDescriptor is the authenticated model-safe projection of a
// node-local system service profile. Raw unit names and manager connection
// details intentionally remain local to the companion and privileged helper.
type ServiceProfileDescriptor struct {
	Alias          string              `json:"alias"`
	Revision       string              `json:"revision"`
	Manager        string              `json:"manager"`
	Services       []ServiceDescriptor `json:"services"`
	LogLimits      ServiceLogLimits    `json:"log_limits"`
	ActionApproval string              `json:"action_approval"`
}

func CloneServiceProfileDescriptors(
	profiles []ServiceProfileDescriptor,
) []ServiceProfileDescriptor {
	result := make([]ServiceProfileDescriptor, len(profiles))
	for index, profile := range profiles {
		result[index] = profile
		result[index].Services = make([]ServiceDescriptor, len(profile.Services))
		for serviceIndex, service := range profile.Services {
			result[index].Services[serviceIndex] = service
			result[index].Services[serviceIndex].Actions = append(
				[]ServiceAction(nil),
				service.Actions...,
			)
		}
	}
	return result
}

func (profile ServiceProfileDescriptor) Validate() error {
	if err := (Alias(profile.Alias)).Validate(); err != nil ||
		!validInvocationIdentifier(profile.Revision) ||
		profile.Manager != "systemd" ||
		len(profile.Services) == 0 ||
		len(profile.Services) > MaxServicesPerProfile ||
		(profile.ActionApproval != "required" &&
			profile.ActionApproval != "operator_bypass_configured") {
		return fmt.Errorf("%w: malformed service profile descriptor", ErrInvalidCapability)
	}
	if err := profile.LogLimits.Validate(); err != nil {
		return err
	}
	prior := ""
	for _, service := range profile.Services {
		if err := service.Validate(); err != nil {
			return err
		}
		if prior != "" && service.Alias <= prior {
			return fmt.Errorf("%w: services are not sorted", ErrInvalidCapability)
		}
		prior = service.Alias
	}
	return nil
}

func IsServiceCommand(name string) bool {
	switch name {
	case "service.status.v1", "service.logs.v1", "service.action.v1":
		return true
	default:
		return false
	}
}

// ServiceCommandInputSchema derives the exact command schema from an already
// narrowed profile projection. Action authority is encoded as alias/action
// pairs rather than independent enums that could imply a forbidden pair.
func ServiceCommandInputSchema(
	command string,
	profiles []ServiceProfileDescriptor,
) json.RawMessage {
	aliasSet := make(map[string]struct{})
	actionsByAlias := make(map[string]map[ServiceAction]struct{})
	entriesMax := 0
	ageSecondsMax := 0
	for _, profile := range profiles {
		entriesMax = max(entriesMax, profile.LogLimits.EntriesMax)
		ageSecondsMax = max(ageSecondsMax, profile.LogLimits.AgeSecondsMax)
		for _, service := range profile.Services {
			aliasSet[service.Alias] = struct{}{}
			for _, action := range service.Actions {
				if actionsByAlias[service.Alias] == nil {
					actionsByAlias[service.Alias] = make(map[ServiceAction]struct{})
				}
				actionsByAlias[service.Alias][action] = struct{}{}
			}
		}
	}
	aliases := make([]string, 0, len(aliasSet))
	for alias := range aliasSet {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	actionBranches := make([]any, 0, len(actionsByAlias))
	for _, alias := range aliases {
		actionSet := actionsByAlias[alias]
		if len(actionSet) == 0 {
			continue
		}
		actions := make([]ServiceAction, 0, len(actionSet))
		for action := range actionSet {
			actions = append(actions, action)
		}
		sort.Slice(actions, func(i, j int) bool { return actions[i] < actions[j] })
		actionBranches = append(actionBranches, map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []string{"service", "action"},
			"properties": map[string]any{
				"service": map[string]any{"const": alias},
				"action":  map[string]any{"enum": actions},
			},
		})
	}
	var schema map[string]any
	switch command {
	case "service.status.v1":
		schema = serviceAliasInputSchema(aliases)
	case "service.logs.v1":
		if entriesMax <= 0 || entriesMax > MaxServiceLogEntries ||
			ageSecondsMax <= 0 || ageSecondsMax > MaxServiceLogAge {
			return json.RawMessage("false")
		}
		schema = serviceAliasInputSchema(aliases)
		schema["properties"].(map[string]any)["entries"] = map[string]any{
			"type": "integer", "minimum": 1, "maximum": entriesMax,
		}
		schema["properties"].(map[string]any)["since_seconds"] = map[string]any{
			"type": "integer", "minimum": 1, "maximum": ageSecondsMax,
		}
	case "service.action.v1":
		schema = map[string]any{"oneOf": actionBranches}
	default:
		return json.RawMessage("false")
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return json.RawMessage("false")
	}
	return data
}

func ServiceCommandOutputSchema(command string) json.RawMessage {
	var schema map[string]any
	switch command {
	case "service.status.v1":
		schema = serviceStatusSchema()
	case "service.logs.v1":
		schema = map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []string{"service", "records", "truncated"},
			"properties": map[string]any{
				"service":   aliasStringSchema(),
				"truncated": map[string]any{"type": "boolean"},
				"records": map[string]any{
					"type": "array", "maxItems": MaxServiceLogEntries,
					"items": map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"required":             []string{"timestamp", "message"},
						"properties": map[string]any{
							"timestamp": map[string]any{"type": "integer", "minimum": 1},
							"severity": map[string]any{
								"enum": []string{"debug", "info", "notice", "warning", "error", "critical"},
							},
							"message": map[string]any{"type": "string", "maxLength": 16384},
						},
					},
				},
			},
		}
	case "service.action.v1":
		schema = map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []string{"service", "action", "state"},
			"properties": map[string]any{
				"service": aliasStringSchema(),
				"action": map[string]any{
					"enum": []ServiceAction{
						ServiceActionStart, ServiceActionStop, ServiceActionRestart,
						ServiceActionReload, ServiceActionEnable, ServiceActionDisable,
					},
				},
				"state": map[string]any{
					"enum": []string{"completed", "failed", "canceled", "unknown"},
				},
				"accepted_at": map[string]any{"type": "integer", "minimum": 1},
				"status":      serviceStatusSchema(),
				"code":        map[string]any{"type": "string", "maxLength": 64},
			},
		}
	default:
		return json.RawMessage("false")
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return json.RawMessage("false")
	}
	return data
}

func serviceStatusSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required": []string{
			"service", "load_state", "active_state", "substate", "enabled", "observed_at",
		},
		"properties": map[string]any{
			"service": aliasStringSchema(),
			"load_state": map[string]any{
				"enum": []string{"loaded", "not_found", "masked", "error", "unknown"},
			},
			"active_state": map[string]any{
				"enum": []string{"active", "inactive", "activating", "deactivating", "failed", "unknown"},
			},
			"substate": map[string]any{
				"enum": []string{"running", "exited", "dead", "failed", "start", "stop", "reload", "unknown"},
			},
			"enabled": map[string]any{
				"enum": []string{"enabled", "disabled", "static", "masked", "unknown"},
			},
			"observed_at": map[string]any{"type": "integer", "minimum": 1},
			"code":        map[string]any{"type": "string", "maxLength": 64},
		},
	}
}

func aliasStringSchema() map[string]any {
	return map[string]any{"type": "string", "minLength": 1, "maxLength": MaxAliasLength}
}

func serviceAliasInputSchema(aliases []string) map[string]any {
	values := make([]any, len(aliases))
	for index, alias := range aliases {
		values[index] = alias
	}
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"service"},
		"properties": map[string]any{
			"service": map[string]any{"type": "string", "enum": values},
		},
	}
}
