package nodes

import (
	"encoding/json"
	"fmt"
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
				"maxLength": 64 * 1024,
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

func enumStringSchema(values []string) any {
	if len(values) == 0 {
		return false
	}
	return map[string]any{"type": "string", "enum": values}
}
