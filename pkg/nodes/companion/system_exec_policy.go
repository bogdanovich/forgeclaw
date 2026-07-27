package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

const (
	maxSystemExecRoots        = 32
	maxSystemExecExecutables  = 128
	maxSystemExecEnvNames     = 64
	maxSystemExecExecAliases  = 64
	maxSystemExecScopeAliases = 32
)

var systemExecEnvNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

var forbiddenSystemExecShells = map[string]struct{}{
	"bash":       {},
	"cmd":        {},
	"csh":        {},
	"dash":       {},
	"fish":       {},
	"ksh":        {},
	"powershell": {},
	"pwsh":       {},
	"sh":         {},
	"tcsh":       {},
	"zsh":        {},
}

// SystemExecPolicy is the companion-owned authority for direct argv
// execution. Normalization resolves roots and executables to real host paths.
type SystemExecPolicy struct {
	WorkingRoots []string             `json:"working_roots"`
	Executables  []string             `json:"executables"`
	Environment  []string             `json:"environment,omitempty"`
	Discovery    *SystemExecDiscovery `json:"discovery,omitempty"`

	rootSet             map[string]struct{}
	executableSet       map[string]struct{}
	environmentSet      map[string]string
	executableAliases   map[string]string
	workingScopeAliases map[string]string
}

// SystemExecDiscovery is operator-authored, non-authoritative model metadata.
// Alias destinations must already exist in the enforcement policy.
type SystemExecDiscovery struct {
	ExecutableAliases   map[string]string `json:"executable_aliases,omitempty"`
	WorkingScopeAliases map[string]string `json:"working_scope_aliases,omitempty"`
	EnvironmentNames    []string          `json:"environment_names,omitempty"`
	Guidance            []string          `json:"guidance,omitempty"`
	Examples            []json.RawMessage `json:"examples,omitempty"`
}

func cloneReadySystemExecPolicy(policy SystemExecPolicy) (SystemExecPolicy, error) {
	if len(policy.WorkingRoots) == 0 || len(policy.rootSet) != len(policy.WorkingRoots) ||
		len(policy.Executables) == 0 || len(policy.executableSet) != len(policy.Executables) ||
		len(policy.environmentSet) != len(policy.Environment) ||
		policy.executableAliases == nil ||
		policy.workingScopeAliases == nil {
		return SystemExecPolicy{}, errors.New("system_exec policy is not normalized")
	}
	cloned := SystemExecPolicy{
		WorkingRoots:        append([]string(nil), policy.WorkingRoots...),
		Executables:         append([]string(nil), policy.Executables...),
		Environment:         append([]string(nil), policy.Environment...),
		Discovery:           cloneSystemExecDiscovery(policy.Discovery),
		rootSet:             make(map[string]struct{}, len(policy.rootSet)),
		executableSet:       make(map[string]struct{}, len(policy.executableSet)),
		environmentSet:      make(map[string]string, len(policy.environmentSet)),
		executableAliases:   make(map[string]string, len(policy.executableAliases)),
		workingScopeAliases: make(map[string]string, len(policy.workingScopeAliases)),
	}
	for key := range policy.rootSet {
		cloned.rootSet[key] = struct{}{}
	}
	for key := range policy.executableSet {
		cloned.executableSet[key] = struct{}{}
	}
	for key, name := range policy.environmentSet {
		cloned.environmentSet[key] = name
	}
	for alias, destination := range policy.executableAliases {
		cloned.executableAliases[alias] = destination
	}
	for alias, destination := range policy.workingScopeAliases {
		cloned.workingScopeAliases[alias] = destination
	}
	return cloned, nil
}

func normalizeSystemExecPolicy(
	policy SystemExecPolicy,
	baseDir string,
) (SystemExecPolicy, error) {
	if len(policy.WorkingRoots) == 0 || len(policy.WorkingRoots) > maxSystemExecRoots {
		return SystemExecPolicy{}, errors.New("system_exec working_roots must contain between 1 and 32 paths")
	}
	if len(policy.Executables) == 0 || len(policy.Executables) > maxSystemExecExecutables {
		return SystemExecPolicy{}, errors.New("system_exec executables must contain between 1 and 128 entries")
	}
	if len(policy.Environment) > maxSystemExecEnvNames {
		return SystemExecPolicy{}, errors.New("system_exec environment contains too many names")
	}

	normalized := SystemExecPolicy{
		WorkingRoots:        make([]string, 0, len(policy.WorkingRoots)),
		Executables:         make([]string, 0, len(policy.Executables)),
		Environment:         make([]string, 0, len(policy.Environment)),
		rootSet:             make(map[string]struct{}, len(policy.WorkingRoots)),
		executableSet:       make(map[string]struct{}, len(policy.Executables)),
		environmentSet:      make(map[string]string, len(policy.Environment)),
		executableAliases:   make(map[string]string),
		workingScopeAliases: make(map[string]string),
	}
	for _, configuredRoot := range policy.WorkingRoots {
		root, err := resolveExistingSystemExecPath(baseDir, configuredRoot)
		if err != nil {
			return SystemExecPolicy{}, fmt.Errorf("resolve system_exec working root: %w", err)
		}
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			return SystemExecPolicy{}, fmt.Errorf("system_exec working root %q is not a directory", root)
		}
		key := systemExecPathKey(root)
		if _, duplicate := normalized.rootSet[key]; duplicate {
			return SystemExecPolicy{}, fmt.Errorf("duplicate system_exec working root %q", root)
		}
		normalized.rootSet[key] = struct{}{}
		normalized.WorkingRoots = append(normalized.WorkingRoots, root)
	}
	for _, configuredExecutable := range policy.Executables {
		path, err := resolveConfiguredSystemExecExecutable(baseDir, configuredExecutable)
		if err != nil {
			return SystemExecPolicy{}, err
		}
		if isForbiddenSystemExecShell(path) {
			return SystemExecPolicy{}, fmt.Errorf("system_exec shell executable %q is not supported", path)
		}
		key := systemExecPathKey(path)
		if _, duplicate := normalized.executableSet[key]; duplicate {
			return SystemExecPolicy{}, fmt.Errorf("duplicate system_exec executable %q", path)
		}
		normalized.executableSet[key] = struct{}{}
		normalized.Executables = append(normalized.Executables, path)
	}
	for _, name := range policy.Environment {
		name = strings.TrimSpace(name)
		if !systemExecEnvNamePattern.MatchString(name) {
			return SystemExecPolicy{}, fmt.Errorf("invalid system_exec environment name %q", name)
		}
		key := systemExecEnvKey(name)
		if _, duplicate := normalized.environmentSet[key]; duplicate {
			return SystemExecPolicy{}, fmt.Errorf("duplicate system_exec environment name %q", name)
		}
		normalized.environmentSet[key] = name
		normalized.Environment = append(normalized.Environment, name)
	}
	slices.Sort(normalized.WorkingRoots)
	slices.Sort(normalized.Executables)
	slices.Sort(normalized.Environment)
	discovery, err := normalizeSystemExecDiscovery(policy.Discovery, baseDir, &normalized)
	if err != nil {
		return SystemExecPolicy{}, err
	}
	normalized.Discovery = discovery
	return normalized, nil
}

func normalizeSystemExecDiscovery(
	discovery *SystemExecDiscovery,
	baseDir string,
	policy *SystemExecPolicy,
) (*SystemExecDiscovery, error) {
	if discovery == nil {
		return nil, nil
	}
	if len(discovery.ExecutableAliases) > maxSystemExecExecAliases {
		return nil, errors.New("system_exec discovery contains too many executable aliases")
	}
	if len(discovery.WorkingScopeAliases) > maxSystemExecScopeAliases {
		return nil, errors.New("system_exec discovery contains too many working-scope aliases")
	}
	if len(discovery.EnvironmentNames) > maxSystemExecEnvNames {
		return nil, errors.New("system_exec discovery contains too many environment names")
	}
	normalized := &SystemExecDiscovery{
		ExecutableAliases:   make(map[string]string, len(discovery.ExecutableAliases)),
		WorkingScopeAliases: make(map[string]string, len(discovery.WorkingScopeAliases)),
		EnvironmentNames:    make([]string, 0, len(discovery.EnvironmentNames)),
		Guidance:            append([]string{}, discovery.Guidance...),
		Examples:            make([]json.RawMessage, 0, len(discovery.Examples)),
	}
	seenAliases := make(map[string]struct{}, len(discovery.ExecutableAliases)+len(discovery.WorkingScopeAliases))
	for alias, configured := range discovery.ExecutableAliases {
		if err := validateSystemExecDiscoveryAlias(alias, seenAliases); err != nil {
			return nil, err
		}
		path, err := resolveConfiguredSystemExecExecutable(baseDir, configured)
		if err != nil {
			return nil, fmt.Errorf("resolve system_exec discovery executable alias %q: %w", alias, err)
		}
		if _, allowed := policy.executableSet[systemExecPathKey(path)]; !allowed {
			return nil, fmt.Errorf(
				"system_exec discovery executable alias %q is outside executables",
				alias,
			)
		}
		normalized.ExecutableAliases[alias] = path
		policy.executableAliases[alias] = path
	}
	for alias, configured := range discovery.WorkingScopeAliases {
		if err := validateSystemExecDiscoveryAlias(alias, seenAliases); err != nil {
			return nil, err
		}
		root, err := resolveExistingSystemExecPath(baseDir, configured)
		if err != nil {
			return nil, fmt.Errorf("resolve system_exec discovery working-scope alias %q: %w", alias, err)
		}
		if _, allowed := policy.rootSet[systemExecPathKey(root)]; !allowed {
			return nil, fmt.Errorf(
				"system_exec discovery working-scope alias %q is not an allowed root",
				alias,
			)
		}
		normalized.WorkingScopeAliases[alias] = root
		policy.workingScopeAliases[alias] = root
	}
	seenEnvironment := make(map[string]struct{}, len(discovery.EnvironmentNames))
	for _, configured := range discovery.EnvironmentNames {
		name := strings.TrimSpace(configured)
		canonical, allowed := policy.environmentSet[systemExecEnvKey(name)]
		if !allowed {
			return nil, fmt.Errorf(
				"system_exec discovery environment name %q is not allowed",
				name,
			)
		}
		key := systemExecEnvKey(canonical)
		if _, duplicate := seenEnvironment[key]; duplicate {
			return nil, fmt.Errorf(
				"duplicate system_exec discovery environment name %q",
				name,
			)
		}
		seenEnvironment[key] = struct{}{}
		normalized.EnvironmentNames = append(normalized.EnvironmentNames, canonical)
	}
	slices.Sort(normalized.EnvironmentNames)
	for _, example := range discovery.Examples {
		if len(example) == 0 || len(example) > nodes.MaxModelExampleBytes {
			return nil, errors.New("system_exec discovery example exceeds size limit")
		}
		canonical, err := jsonstrict.Canonical(example)
		if err != nil {
			return nil, errors.New("system_exec discovery example is invalid JSON")
		}
		normalized.Examples = append(
			normalized.Examples,
			append(json.RawMessage(nil), canonical...),
		)
	}
	slices.SortFunc(normalized.Examples, func(left, right json.RawMessage) int {
		return strings.Compare(string(left), string(right))
	})
	return normalized, nil
}

func validateSystemExecDiscoveryAlias(
	alias string,
	seen map[string]struct{},
) error {
	if err := nodes.Alias(alias).Validate(); err != nil {
		return fmt.Errorf("invalid system_exec discovery alias %q", alias)
	}
	key := strings.ToLower(alias)
	if _, duplicate := seen[key]; duplicate {
		return fmt.Errorf("duplicate system_exec discovery alias %q", alias)
	}
	seen[key] = struct{}{}
	return nil
}

func systemExecModelContract(
	policy SystemExecPolicy,
	local nodes.LocalCommandPolicy,
) (*nodes.CommandModelContract, error) {
	executableAliases := sortedSystemExecMapKeys(policy.executableAliases)
	workingScopes := sortedSystemExecMapKeys(policy.workingScopeAliases)
	environmentNames := []string{}
	guidance := []string{}
	examples := []json.RawMessage{}
	if policy.Discovery != nil {
		environmentNames = append(environmentNames, policy.Discovery.EnvironmentNames...)
		guidance = append(guidance, policy.Discovery.Guidance...)
		for _, example := range policy.Discovery.Examples {
			examples = append(examples, append(json.RawMessage(nil), example...))
		}
	}
	availability := nodes.ModelAvailable
	if !slices.Contains(local.AllowedCommands, "system.exec.v1") ||
		modelRiskRank(nodes.RiskWrite) > modelRiskRank(local.MaximumRisk) {
		availability = nodes.ModelUnavailable
	} else if len(executableAliases) == 0 || len(workingScopes) == 0 {
		availability = nodes.ModelPartiallyDescribed
	}
	contract := &nodes.CommandModelContract{
		Availability:      availability,
		TimeoutSecondsMax: local.MaxTimeoutSeconds,
		OutputBytesMax:    local.MaxOutputBytes,
		ResultKind:        "json",
		AuthorityDigest:   systemExecDiscoveryAuthorityDigest(policy),
		Constraints: nodes.CommandModelConstraints{
			ExecutableAliases: executableAliases,
			WorkingScopes:     workingScopes,
			EnvironmentNames:  environmentNames,
		},
		Guidance: guidance,
		Examples: examples,
	}
	schema, err := nodes.SystemExecModelInputSchema(*contract)
	if err != nil {
		return nil, err
	}
	if err := contract.Validate(schema); err != nil {
		return nil, err
	}
	handler := newSystemExecHandler(policy)
	for _, example := range examples {
		if _, err := handler.prepare(example, local.MaxTimeoutSeconds); err != nil {
			return nil, errors.New("system_exec discovery example violates command policy")
		}
	}
	return contract, nil
}

func systemExecDiscoveryAuthorityDigest(policy SystemExecPolicy) string {
	if policy.Discovery == nil {
		return ""
	}
	type binding struct {
		Alias       string `json:"alias"`
		Destination string `json:"destination"`
	}
	payload := struct {
		Executables []binding `json:"executables"`
		Scopes      []binding `json:"scopes"`
		Environment []string  `json:"environment"`
	}{
		Environment: append([]string(nil), policy.Discovery.EnvironmentNames...),
	}
	for _, alias := range sortedSystemExecMapKeys(policy.executableAliases) {
		payload.Executables = append(payload.Executables, binding{
			Alias: alias, Destination: policy.executableAliases[alias],
		})
	}
	for _, alias := range sortedSystemExecMapKeys(policy.workingScopeAliases) {
		payload.Scopes = append(payload.Scopes, binding{
			Alias: alias, Destination: policy.workingScopeAliases[alias],
		})
	}
	data, _ := json.Marshal(payload)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func sortedSystemExecMapKeys(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

func cloneSystemExecDiscovery(discovery *SystemExecDiscovery) *SystemExecDiscovery {
	if discovery == nil {
		return nil
	}
	cloned := &SystemExecDiscovery{
		ExecutableAliases:   make(map[string]string, len(discovery.ExecutableAliases)),
		WorkingScopeAliases: make(map[string]string, len(discovery.WorkingScopeAliases)),
		EnvironmentNames:    append([]string(nil), discovery.EnvironmentNames...),
		Guidance:            append([]string(nil), discovery.Guidance...),
		Examples:            make([]json.RawMessage, 0, len(discovery.Examples)),
	}
	for alias, destination := range discovery.ExecutableAliases {
		cloned.ExecutableAliases[alias] = destination
	}
	for alias, destination := range discovery.WorkingScopeAliases {
		cloned.WorkingScopeAliases[alias] = destination
	}
	for _, example := range discovery.Examples {
		cloned.Examples = append(cloned.Examples, append(json.RawMessage(nil), example...))
	}
	return cloned
}

func resolveConfiguredSystemExecExecutable(baseDir, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("system_exec executable is empty")
	}
	var path string
	var err error
	if filepath.IsAbs(value) || strings.ContainsAny(value, `/\\`) {
		path, err = resolveConfigPath(baseDir, value)
	} else {
		path, err = exec.LookPath(value)
	}
	if err != nil {
		return "", fmt.Errorf("resolve system_exec executable %q: %w", value, err)
	}
	path, err = resolveExistingSystemExecPath("", path)
	if err != nil {
		return "", fmt.Errorf("resolve system_exec executable %q: %w", value, err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("system_exec executable %q is not a regular file", path)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("system_exec executable %q is not executable", path)
	}
	return path, nil
}

func resolveExistingSystemExecPath(baseDir, value string) (string, error) {
	path, err := resolveConfigPath(baseDir, value)
	if err != nil {
		return "", err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(path), nil
}

func (policy SystemExecPolicy) resolveExecutable(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsRune(value, 0) {
		return "", errors.New("system.exec argv[0] is invalid")
	}
	if path, aliased := policy.executableAliases[value]; aliased {
		return path, nil
	}
	var path string
	var err error
	if filepath.IsAbs(value) {
		path = value
	} else if strings.ContainsAny(value, `/\\`) {
		return "", errors.New("system.exec relative executable paths are not allowed")
	} else {
		path, err = exec.LookPath(value)
		if err != nil {
			return "", errors.New("system.exec executable is unavailable")
		}
	}
	path, err = resolveExistingSystemExecPath("", path)
	if err != nil {
		return "", errors.New("system.exec executable is unavailable")
	}
	if _, allowed := policy.executableSet[systemExecPathKey(path)]; !allowed {
		return "", errors.New("system.exec executable is not allowed")
	}
	if isForbiddenSystemExecShell(path) {
		return "", errors.New("system.exec shell execution is not allowed")
	}
	return path, nil
}

func (policy SystemExecPolicy) resolveWorkingDirectory(value string) (string, error) {
	if path, aliased := policy.workingScopeAliases[value]; aliased {
		return path, nil
	}
	if !filepath.IsAbs(value) || strings.ContainsRune(value, 0) {
		return "", errors.New("system.exec cwd must be an absolute path")
	}
	path, err := resolveExistingSystemExecPath("", value)
	if err != nil {
		return "", errors.New("system.exec cwd is unavailable")
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return "", errors.New("system.exec cwd is not a directory")
	}
	for _, root := range policy.WorkingRoots {
		if systemExecPathWithin(root, path) {
			return path, nil
		}
	}
	return "", errors.New("system.exec cwd is outside allowed roots")
}

func systemExecPathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func systemExecPathKey(path string) string {
	path = filepath.Clean(path)
	if runtime.GOOS == "windows" {
		return strings.ToLower(path)
	}
	return path
}

func systemExecEnvKey(name string) string {
	if runtime.GOOS == "windows" {
		return strings.ToUpper(name)
	}
	return name
}

func isForbiddenSystemExecShell(path string) bool {
	name := strings.ToLower(filepath.Base(path))
	name = strings.TrimSuffix(name, ".exe")
	_, forbidden := forbiddenSystemExecShells[name]
	return forbidden
}
