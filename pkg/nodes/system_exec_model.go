package nodes

import (
	"encoding/json"
	"fmt"
)

// SystemExecModelInputSchema returns the model-visible system.exec.v1 schema.
// It names only operator-authored aliases and visible environment names; raw
// enforcement paths remain accepted by the companion but are never projected.
func SystemExecModelInputSchema(
	contract CommandModelContract,
) (json.RawMessage, error) {
	if err := validateModelConstraintNames(contract.Constraints); err != nil {
		return nil, err
	}
	if contract.TimeoutSecondsMax <= 0 ||
		contract.TimeoutSecondsMax > MaxInvocationTimeout {
		return nil, fmt.Errorf("%w: invalid system.exec model timeout", ErrInvalidCapability)
	}
	executableSchema := any(false)
	if len(contract.Constraints.ExecutableAliases) > 0 {
		executableSchema = map[string]any{
			"type": "string",
			"enum": contract.Constraints.ExecutableAliases,
		}
	}
	workingScopeSchema := any(false)
	if len(contract.Constraints.WorkingScopes) > 0 {
		workingScopeSchema = map[string]any{
			"type": "string",
			"enum": contract.Constraints.WorkingScopes,
		}
	}
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
			"maxLength": 16384,
		}
	}
	schema := map[string]any{
		"type":     "object",
		"required": []string{"argv", "cwd", "timeout_seconds", "env"},
		"properties": map[string]any{
			"argv": map[string]any{
				"type":        "array",
				"minItems":    1,
				"maxItems":    128,
				"prefixItems": []any{executableSchema},
				"items": map[string]any{
					"type":      "string",
					"minLength": 1,
					"maxLength": 4096,
				},
			},
			"cwd": workingScopeSchema,
			"timeout_seconds": map[string]any{
				"type":    "integer",
				"minimum": 1,
				"maximum": contract.TimeoutSecondsMax,
			},
			"env": environmentSchema,
		},
		"additionalProperties": false,
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: encode system.exec model schema", ErrInvalidCapability)
	}
	if err := validateObjectSchema("system.exec model input", data); err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}
