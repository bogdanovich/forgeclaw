package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	ownerShellExecutorCompanion = "companion_user"
	ownerShellExecutorBroker    = "authority_broker"
	ownerShellNetworkInherit    = "inherit"
	maxOwnerShellProfiles       = 32
	maxOwnerShellScriptBytes    = 64 * 1024
	maxOwnerShellEnvBytes       = 8 * 1024
	maxOwnerShellOutputBytes    = 128 * 1024
	maxOwnerShellTimeout        = 3600
	maxOwnerShellConcurrent     = 8
)

type OwnerShellPolicy struct {
	Enabled  bool                         `json:"enabled"`
	Revision string                       `json:"revision,omitempty"`
	Profiles map[string]OwnerShellProfile `json:"profiles,omitempty"`
}

type OwnerShellProfile struct {
	Executor                  string               `json:"executor"`
	Label                     string               `json:"label,omitempty"`
	Shell                     OwnerShellExecutable `json:"shell"`
	Identity                  *OwnerShellIdentity  `json:"identity,omitempty"`
	WorkingRoots              []string             `json:"working_roots"`
	InitialDirectory          string               `json:"initial_directory"`
	WorkingScopeAliases       map[string]string    `json:"working_scope_aliases"`
	FixedEnvironment          map[string]string    `json:"fixed_environment"`
	PermittedEnvironmentNames []string             `json:"permitted_environment_names"`
	Network                   string               `json:"network"`
	Approval                  OwnerShellApproval   `json:"approval"`
	Limits                    OwnerShellLimits     `json:"limits"`
	rootSet                   map[string]struct{}
	environmentSet            map[string]string
}

type OwnerShellExecutable struct {
	Path  string `json:"path"`
	Login bool   `json:"login"`
}

type OwnerShellIdentity struct {
	UID                 int   `json:"uid"`
	GID                 int   `json:"gid"`
	SupplementaryGroups []int `json:"supplementary_groups"`
}

type OwnerShellApproval struct {
	ShellExec    string `json:"shell_exec"`
	TerminalOpen string `json:"terminal_open"`
}

type OwnerShellLimits struct {
	CommandBytes            int `json:"command_bytes"`
	TimeoutSeconds          int `json:"timeout_seconds"`
	OutputBytes             int `json:"output_bytes"`
	ConcurrentCommands      int `json:"concurrent_commands"`
	ConcurrentTerminals     int `json:"concurrent_terminals"`
	TerminalIdleSeconds     int `json:"terminal_idle_seconds"`
	TerminalLifetimeSeconds int `json:"terminal_lifetime_seconds"`
	TerminalBufferBytes     int `json:"terminal_buffer_bytes"`
}

func normalizeOwnerShellPolicy(policy OwnerShellPolicy, baseDir string) (OwnerShellPolicy, error) {
	normalized := OwnerShellPolicy{
		Enabled:  policy.Enabled,
		Revision: strings.TrimSpace(policy.Revision),
		Profiles: make(map[string]OwnerShellProfile, len(policy.Profiles)),
	}
	if policy.Enabled && runtime.GOOS == "windows" {
		return OwnerShellPolicy{}, errors.New("owner_shell is supported only on Unix hosts")
	}
	if len(policy.Profiles) > maxOwnerShellProfiles {
		return OwnerShellPolicy{}, errors.New("owner_shell contains too many profiles")
	}
	if !policy.Enabled {
		if normalized.Revision == "" && len(policy.Profiles) == 0 {
			return normalized, nil
		}
	}
	if err := nodes.ID(normalized.Revision).Validate(); err != nil {
		return OwnerShellPolicy{}, errors.New("owner_shell revision is invalid")
	}
	if policy.Enabled && len(policy.Profiles) == 0 {
		return OwnerShellPolicy{}, errors.New("enabled owner_shell requires at least one profile")
	}
	seenAliases := make(map[string]struct{}, len(policy.Profiles))
	for alias, profile := range policy.Profiles {
		if err := nodes.Alias(alias).Validate(); err != nil {
			return OwnerShellPolicy{}, fmt.Errorf("owner_shell profile alias %q is invalid", alias)
		}
		key := strings.ToLower(alias)
		if _, exists := seenAliases[key]; exists {
			return OwnerShellPolicy{}, fmt.Errorf("owner_shell profile alias %q collides by case", alias)
		}
		seenAliases[key] = struct{}{}
		ready, err := normalizeOwnerShellProfile(alias, profile, baseDir)
		if err != nil {
			return OwnerShellPolicy{}, err
		}
		normalized.Profiles[alias] = ready
	}
	return normalized, nil
}

func normalizeOwnerShellProfile(
	alias string,
	profile OwnerShellProfile,
	baseDir string,
) (OwnerShellProfile, error) {
	if profile.Executor == ownerShellExecutorBroker {
		return OwnerShellProfile{}, fmt.Errorf(
			"owner_shell profile %q requires the not-yet-configured authority broker",
			alias,
		)
	}
	if profile.Executor != ownerShellExecutorCompanion || profile.Identity != nil {
		return OwnerShellProfile{}, fmt.Errorf(
			"owner_shell profile %q must use the companion service identity",
			alias,
		)
	}
	if profile.Label == "" {
		profile.Label = alias
	}
	if len(profile.Label) > nodes.MaxAliasLength ||
		profile.Label != strings.TrimSpace(profile.Label) ||
		strings.IndexFunc(profile.Label, func(value rune) bool { return value < 0x20 || value == 0x7f }) >= 0 {
		return OwnerShellProfile{}, fmt.Errorf("owner_shell profile %q label is invalid", alias)
	}
	shellPath, err := resolveConfiguredSystemExecExecutable(baseDir, profile.Shell.Path)
	if err != nil {
		return OwnerShellProfile{}, fmt.Errorf("resolve owner_shell profile %q shell: %w", alias, err)
	}
	if len(profile.WorkingRoots) == 0 || len(profile.WorkingRoots) > maxSystemExecRoots {
		return OwnerShellProfile{}, fmt.Errorf("owner_shell profile %q requires working roots", alias)
	}
	ready := profile
	ready.Shell.Path = shellPath
	ready.WorkingRoots = make([]string, 0, len(profile.WorkingRoots))
	ready.rootSet = make(map[string]struct{}, len(profile.WorkingRoots))
	for _, configured := range profile.WorkingRoots {
		root, resolveErr := resolveExistingSystemExecPath(baseDir, configured)
		if resolveErr != nil {
			return OwnerShellProfile{}, fmt.Errorf("resolve owner_shell profile %q working root: %w", alias, resolveErr)
		}
		info, statErr := os.Stat(root)
		if statErr != nil || !info.IsDir() {
			return OwnerShellProfile{}, fmt.Errorf("owner_shell profile %q working root is not a directory", alias)
		}
		key := systemExecPathKey(root)
		if _, duplicate := ready.rootSet[key]; duplicate {
			return OwnerShellProfile{}, fmt.Errorf("owner_shell profile %q has a duplicate working root", alias)
		}
		ready.rootSet[key] = struct{}{}
		ready.WorkingRoots = append(ready.WorkingRoots, root)
	}
	slices.Sort(ready.WorkingRoots)
	initial, err := resolveOwnerShellDirectory(ready, baseDir, profile.InitialDirectory)
	if err != nil {
		return OwnerShellProfile{}, fmt.Errorf("owner_shell profile %q initial directory: %w", alias, err)
	}
	ready.InitialDirectory = initial
	ready.WorkingScopeAliases, err = normalizeOwnerShellScopes(alias, profile.WorkingScopeAliases, baseDir, ready)
	if err != nil {
		return OwnerShellProfile{}, err
	}
	ready.FixedEnvironment, ready.PermittedEnvironmentNames, ready.environmentSet, err = normalizeOwnerShellEnvironment(
		alias,
		profile.FixedEnvironment,
		profile.PermittedEnvironmentNames,
	)
	if err != nil {
		return OwnerShellProfile{}, err
	}
	if profile.Network != ownerShellNetworkInherit {
		return OwnerShellProfile{}, fmt.Errorf("owner_shell profile %q network must be inherit", alias)
	}
	if profile.Approval.ShellExec != "each_command" ||
		profile.Approval.TerminalOpen != "session_start" {
		return OwnerShellProfile{}, fmt.Errorf("owner_shell profile %q approval modes are invalid", alias)
	}
	ready.Limits, err = normalizeOwnerShellLimits(alias, profile.Limits)
	if err != nil {
		return OwnerShellProfile{}, err
	}
	return ready, nil
}

func normalizeOwnerShellScopes(
	profileAlias string,
	scopes map[string]string,
	baseDir string,
	profile OwnerShellProfile,
) (map[string]string, error) {
	if len(scopes) == 0 || len(scopes) > maxSystemExecScopeAliases {
		return nil, fmt.Errorf("owner_shell profile %q requires visible working scopes", profileAlias)
	}
	normalized := make(map[string]string, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for alias, configured := range scopes {
		if err := nodes.Alias(alias).Validate(); err != nil {
			return nil, fmt.Errorf("owner_shell profile %q working scope %q is invalid", profileAlias, alias)
		}
		key := strings.ToLower(alias)
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("owner_shell profile %q working scope %q collides by case", profileAlias, alias)
		}
		seen[key] = struct{}{}
		path, err := resolveOwnerShellDirectory(profile, baseDir, configured)
		if err != nil {
			return nil, fmt.Errorf("owner_shell profile %q working scope %q: %w", profileAlias, alias, err)
		}
		normalized[alias] = path
	}
	return normalized, nil
}

func resolveOwnerShellDirectory(
	profile OwnerShellProfile,
	baseDir string,
	value string,
) (string, error) {
	path, err := resolveExistingSystemExecPath(baseDir, value)
	if err != nil {
		return "", errors.New("directory is unavailable")
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return "", errors.New("directory is not a directory")
	}
	for _, root := range profile.WorkingRoots {
		if systemExecPathWithin(root, path) {
			return path, nil
		}
	}
	return "", errors.New("directory is outside allowed roots")
}

func normalizeOwnerShellEnvironment(
	profileAlias string,
	fixed map[string]string,
	permitted []string,
) (map[string]string, []string, map[string]string, error) {
	if fixed == nil {
		fixed = map[string]string{}
	}
	if permitted == nil {
		permitted = []string{}
	}
	if len(fixed)+len(permitted) > maxSystemExecEnvNames {
		return nil, nil, nil, fmt.Errorf("owner_shell profile %q environment contains too many names", profileAlias)
	}
	normalizedFixed := make(map[string]string, len(fixed))
	seen := make(map[string]struct{}, len(fixed)+len(permitted))
	totalBytes := 0
	for name, value := range fixed {
		if !systemExecEnvNamePattern.MatchString(name) || strings.ContainsRune(value, 0) {
			return nil, nil, nil, fmt.Errorf("owner_shell profile %q fixed environment is invalid", profileAlias)
		}
		key := systemExecEnvKey(name)
		if _, duplicate := seen[key]; duplicate {
			return nil, nil, nil, fmt.Errorf("owner_shell profile %q environment name is duplicated", profileAlias)
		}
		seen[key] = struct{}{}
		totalBytes += len(name) + len(value) + 1
		normalizedFixed[name] = value
	}
	normalizedPermitted := make([]string, 0, len(permitted))
	environmentSet := make(map[string]string, len(permitted))
	for _, name := range permitted {
		if !systemExecEnvNamePattern.MatchString(name) {
			return nil, nil, nil, fmt.Errorf("owner_shell profile %q permitted environment is invalid", profileAlias)
		}
		key := systemExecEnvKey(name)
		if _, duplicate := seen[key]; duplicate {
			return nil, nil, nil, fmt.Errorf("owner_shell profile %q environment name is duplicated", profileAlias)
		}
		seen[key] = struct{}{}
		environmentSet[key] = name
		normalizedPermitted = append(normalizedPermitted, name)
	}
	if totalBytes > maxOwnerShellEnvBytes {
		return nil, nil, nil, fmt.Errorf("owner_shell profile %q fixed environment exceeds limits", profileAlias)
	}
	slices.Sort(normalizedPermitted)
	return normalizedFixed, normalizedPermitted, environmentSet, nil
}

func normalizeOwnerShellLimits(alias string, limits OwnerShellLimits) (OwnerShellLimits, error) {
	if limits.CommandBytes == 0 {
		limits.CommandBytes = maxOwnerShellScriptBytes
	}
	if limits.TimeoutSeconds == 0 {
		limits.TimeoutSeconds = 900
	}
	if limits.OutputBytes == 0 {
		limits.OutputBytes = maxOwnerShellOutputBytes
	}
	if limits.ConcurrentCommands == 0 {
		limits.ConcurrentCommands = 2
	}
	if limits.ConcurrentTerminals == 0 {
		limits.ConcurrentTerminals = 1
	}
	if limits.TerminalIdleSeconds == 0 {
		limits.TerminalIdleSeconds = 900
	}
	if limits.TerminalLifetimeSeconds == 0 {
		limits.TerminalLifetimeSeconds = 28800
	}
	if limits.TerminalBufferBytes == 0 {
		limits.TerminalBufferBytes = 1024 * 1024
	}
	if limits.CommandBytes < 1 || limits.CommandBytes > maxOwnerShellScriptBytes ||
		limits.TimeoutSeconds < 1 || limits.TimeoutSeconds > maxOwnerShellTimeout ||
		limits.OutputBytes < 1 || limits.OutputBytes > maxOwnerShellOutputBytes ||
		limits.ConcurrentCommands < 1 || limits.ConcurrentCommands > maxOwnerShellConcurrent ||
		limits.ConcurrentTerminals < 1 || limits.ConcurrentTerminals > 2 ||
		limits.TerminalIdleSeconds < 1 || limits.TerminalIdleSeconds > 3600 ||
		limits.TerminalLifetimeSeconds < 1 || limits.TerminalLifetimeSeconds > 28800 ||
		limits.TerminalBufferBytes < 1 || limits.TerminalBufferBytes > 1024*1024 {
		return OwnerShellLimits{}, fmt.Errorf("owner_shell profile %q limits are invalid", alias)
	}
	return limits, nil
}

func cloneReadyOwnerShellPolicy(policy OwnerShellPolicy) (OwnerShellPolicy, error) {
	if !policy.Enabled || len(policy.Profiles) == 0 {
		return OwnerShellPolicy{}, errors.New("owner_shell policy is not enabled")
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return OwnerShellPolicy{}, err
	}
	var cloned OwnerShellPolicy
	if err := json.Unmarshal(data, &cloned); err != nil {
		return OwnerShellPolicy{}, err
	}
	return normalizeOwnerShellPolicy(cloned, "")
}

func ownerShellAuthorityDigest(policy OwnerShellPolicy) string {
	data, _ := json.Marshal(policy)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ownerShellModelContract(
	policy OwnerShellPolicy,
	local nodes.LocalCommandPolicy,
) (*nodes.CommandModelContract, error) {
	profiles := make([]string, 0, len(policy.Profiles))
	scopeSet := make(map[string]struct{})
	environmentSet := make(map[string]struct{})
	timeoutMax := local.MaxTimeoutSeconds
	outputMax := local.MaxOutputBytes
	for alias, profile := range policy.Profiles {
		profiles = append(profiles, alias)
		timeoutMax = min(timeoutMax, profile.Limits.TimeoutSeconds)
		outputMax = min(outputMax, profile.Limits.OutputBytes)
		for scope := range profile.WorkingScopeAliases {
			scopeSet[scope] = struct{}{}
		}
		for _, name := range profile.PermittedEnvironmentNames {
			environmentSet[name] = struct{}{}
		}
	}
	slices.Sort(profiles)
	scopes := sortedOwnerShellSet(scopeSet)
	environment := sortedOwnerShellSet(environmentSet)
	availability := nodes.ModelAvailable
	if !policy.Enabled ||
		!slices.Contains(local.AllowedCommands, "shell.exec.v1") ||
		modelRiskRank(nodes.RiskPrivileged) > modelRiskRank(local.MaximumRisk) {
		availability = nodes.ModelUnavailable
	}
	contract := &nodes.CommandModelContract{
		Availability:      availability,
		TimeoutSecondsMax: timeoutMax,
		OutputBytesMax:    outputMax,
		ResultKind:        "json",
		AuthorityDigest:   ownerShellAuthorityDigest(policy),
		ApprovalMode:      "each_command",
		Constraints: nodes.CommandModelConstraints{
			ProfileAliases:   profiles,
			WorkingScopes:    scopes,
			EnvironmentNames: environment,
		},
		Guidance: []string{
			"Use only an operator-owned profile and working-scope alias returned by discovery.",
			"Each shell command requires the configured trusted approval flow.",
		},
		Examples: []json.RawMessage{},
	}
	schema, err := nodes.ShellExecModelInputSchema(*contract)
	if err != nil {
		return nil, err
	}
	if err := contract.Validate(schema); err != nil {
		return nil, err
	}
	return contract, nil
}

func sortedOwnerShellSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}

func (profile OwnerShellProfile) resolveWorkingScope(alias string) (string, error) {
	path, ok := profile.WorkingScopeAliases[alias]
	if !ok {
		return "", errors.New("shell.exec working scope is not allowed")
	}
	resolved, err := resolveExistingSystemExecPath("", path)
	if err != nil {
		return "", errors.New("shell.exec working scope is unavailable")
	}
	for _, root := range profile.WorkingRoots {
		if systemExecPathWithin(root, resolved) {
			return resolved, nil
		}
	}
	return "", errors.New("shell.exec working scope escaped its configured root")
}
