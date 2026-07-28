package nodes

import (
	"encoding/json"
	"fmt"
)

const (
	MaxShellExecScriptBytes      = 64 * 1024
	MaxShellExecEnvironmentBytes = 8 * 1024
)

// ShellExecModelInputSchema returns the model-visible shell.exec.v1 schema.
// Only operator-authored aliases and permitted environment names are
// projected. Shell paths, identities, environment values, and raw working
// paths remain node-local authority.
func ShellExecModelInputSchema(
	contract CommandModelContract,
) (json.RawMessage, error) {
	if err := validateModelConstraintNames(contract.Constraints); err != nil {
		return nil, err
	}
	if contract.TimeoutSecondsMax <= 0 ||
		contract.TimeoutSecondsMax > MaxInvocationTimeout {
		return nil, fmt.Errorf("%w: invalid shell.exec model timeout", ErrInvalidCapability)
	}
	profileSchema := enumStringSchema(contract.Constraints.ProfileAliases)
	workingScopeSchema := enumStringSchema(contract.Constraints.WorkingScopes)
	environmentSchema := map[string]any{
		"type":                 "object",
		"maxProperties":        len(contract.Constraints.EnvironmentNames),
		"additionalProperties": false,
	}
	if len(contract.Constraints.EnvironmentNames) > 0 {
		environmentSchema["propertyNames"] = map[string]any{
			"enum": contract.Constraints.EnvironmentNames,
		}
		environmentSchema["additionalProperties"] = map[string]any{
			"type":      "string",
			"maxLength": 8192,
		}
	}
	schema := map[string]any{
		"type":     "object",
		"required": []string{"profile", "script", "cwd", "env", "timeout_seconds"},
		"properties": map[string]any{
			"profile": profileSchema,
			"script": map[string]any{
				"type":      "string",
				"minLength": 1,
				"maxLength": MaxShellExecScriptBytes,
			},
			"cwd": workingScopeSchema,
			"env": environmentSchema,
			"timeout_seconds": map[string]any{
				"type":    "integer",
				"minimum": 1,
				"maximum": contract.TimeoutSecondsMax,
			},
		},
		"additionalProperties": false,
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: encode shell.exec model schema", ErrInvalidCapability)
	}
	if err := validateObjectSchema("shell.exec model input", data); err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

// ValidateShellExecModelInput enforces byte ceilings that JSON Schema
// maxLength cannot express because maxLength counts Unicode code points.
func ValidateShellExecModelInput(input map[string]any) error {
	script, ok := input["script"].(string)
	if !ok || len(script) == 0 || len(script) > MaxShellExecScriptBytes {
		return fmt.Errorf("%w: shell.exec script exceeds byte limits", ErrInvalidCapability)
	}
	environment, ok := input["env"].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: shell.exec environment is invalid", ErrInvalidCapability)
	}
	totalBytes := 0
	for name, raw := range environment {
		value, valid := raw.(string)
		if !valid {
			return fmt.Errorf("%w: shell.exec environment is invalid", ErrInvalidCapability)
		}
		totalBytes += len(name) + len(value) + 1
		if totalBytes > MaxShellExecEnvironmentBytes {
			return fmt.Errorf("%w: shell.exec environment exceeds byte limits", ErrInvalidCapability)
		}
	}
	return nil
}

func enumStringSchema(values []string) any {
	if len(values) == 0 {
		return false
	}
	return map[string]any{"type": "string", "enum": values}
}
