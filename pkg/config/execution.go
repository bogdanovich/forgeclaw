package config

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const MaxExecutionTargets = 128

var (
	executionTargetNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	nodeReferencePattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// ExecutionConfig defines operator-owned target names. Models select only a
// target name and never supply transport connection details.
type ExecutionConfig struct {
	Targets map[string]ExecutionTarget `json:"targets,omitempty"`
}

// ExecutionTarget binds one operator-owned name to a transport-specific
// destination. Only first-party node targets and the local node executor are
// available in the MVP.
type ExecutionTarget struct {
	Type           string `json:"type"`
	Node           string `json:"node"`
	Executor       string `json:"executor,omitempty"`
	FileProfile    string `json:"file_profile,omitempty"`
	ServiceProfile string `json:"service_profile,omitempty"`
}

// TargetPolicy bounds the named execution targets visible to one agent.
type TargetPolicy struct {
	DefaultTarget  string   `json:"default_target,omitempty"`
	AllowedTargets []string `json:"allowed_targets,omitempty"`
}

// ValidateExecutionTargets validates static target definitions and per-agent
// references. Live node aliases are resolved separately because a configured
// target may legitimately be offline during config loading.
func (c *Config) ValidateExecutionTargets() error {
	if c == nil {
		return errors.New("config is required")
	}
	if len(c.Execution.Targets) > MaxExecutionTargets {
		return fmt.Errorf("execution.targets exceeds the %d target limit", MaxExecutionTargets)
	}
	for name, target := range c.Execution.Targets {
		if !validExecutionTargetName(name) {
			return fmt.Errorf("execution target %q has an invalid name", name)
		}
		if target.Type != "node" {
			return fmt.Errorf("execution target %q has unsupported type %q", name, target.Type)
		}
		if !validNodeReference(target.Node) {
			return fmt.Errorf("execution target %q has an invalid node reference", name)
		}
		if target.Executor != "" && target.Executor != "local" {
			return fmt.Errorf("execution target %q has unsupported executor %q", name, target.Executor)
		}
		if target.FileProfile != "" && !validNodeFileProfile(target.FileProfile) {
			return fmt.Errorf("execution target %q has an invalid file profile", name)
		}
		if target.ServiceProfile != "" && !validNodeFileProfile(target.ServiceProfile) {
			return fmt.Errorf("execution target %q has an invalid service profile", name)
		}
	}
	if err := validateTargetPolicy(
		"agents.defaults.target_policy",
		c.Agents.Defaults.TargetPolicy,
		c.Execution.Targets,
	); err != nil {
		return err
	}
	for index := range c.Agents.List {
		label := fmt.Sprintf("agents.list[%d].target_policy", index)
		if id := strings.TrimSpace(c.Agents.List[index].ID); id != "" {
			label = fmt.Sprintf("agent %q target_policy", id)
		}
		if err := validateTargetPolicy(label, c.Agents.List[index].TargetPolicy, c.Execution.Targets); err != nil {
			return err
		}
	}
	return nil
}

func validateTargetPolicy(
	label string,
	policy *TargetPolicy,
	targets map[string]ExecutionTarget,
) error {
	if policy == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(policy.AllowedTargets))
	for _, name := range policy.AllowedTargets {
		if !validExecutionTargetName(name) {
			return fmt.Errorf("%s contains invalid target %q", label, name)
		}
		if _, exists := targets[name]; !exists {
			return fmt.Errorf("%s references unknown target %q", label, name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("%s contains duplicate target %q", label, name)
		}
		seen[name] = struct{}{}
	}
	if policy.DefaultTarget == "" {
		return nil
	}
	if !validExecutionTargetName(policy.DefaultTarget) {
		return fmt.Errorf("%s has invalid default target %q", label, policy.DefaultTarget)
	}
	if _, allowed := seen[policy.DefaultTarget]; !allowed {
		return fmt.Errorf("%s default target %q is not allowed", label, policy.DefaultTarget)
	}
	return nil
}

func validExecutionTargetName(value string) bool {
	return value == strings.TrimSpace(value) && executionTargetNamePattern.MatchString(value)
}

func validNodeReference(value string) bool {
	return value == strings.TrimSpace(value) && nodeReferencePattern.MatchString(value)
}

func validNodeFileProfile(value string) bool {
	return value == strings.TrimSpace(value) && executionTargetNamePattern.MatchString(value)
}
