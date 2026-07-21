package companion

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
)

const (
	maxSystemExecRoots       = 32
	maxSystemExecExecutables = 128
	maxSystemExecEnvNames    = 64
)

var systemExecEnvNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

var forbiddenSystemExecShells = map[string]struct{}{
	"bash":       {},
	"cmd":        {},
	"cmd.exe":    {},
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
	WorkingRoots []string `json:"working_roots"`
	Executables  []string `json:"executables"`
	Environment  []string `json:"environment,omitempty"`

	rootSet        map[string]struct{}
	executableSet  map[string]struct{}
	environmentSet map[string]string
}

func cloneReadySystemExecPolicy(policy SystemExecPolicy) (SystemExecPolicy, error) {
	if len(policy.WorkingRoots) == 0 || len(policy.rootSet) != len(policy.WorkingRoots) ||
		len(policy.Executables) == 0 || len(policy.executableSet) != len(policy.Executables) ||
		len(policy.environmentSet) != len(policy.Environment) {
		return SystemExecPolicy{}, errors.New("system_exec policy is not normalized")
	}
	cloned := SystemExecPolicy{
		WorkingRoots:   append([]string(nil), policy.WorkingRoots...),
		Executables:    append([]string(nil), policy.Executables...),
		Environment:    append([]string(nil), policy.Environment...),
		rootSet:        make(map[string]struct{}, len(policy.rootSet)),
		executableSet:  make(map[string]struct{}, len(policy.executableSet)),
		environmentSet: make(map[string]string, len(policy.environmentSet)),
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
		WorkingRoots:   make([]string, 0, len(policy.WorkingRoots)),
		Executables:    make([]string, 0, len(policy.Executables)),
		Environment:    make([]string, 0, len(policy.Environment)),
		rootSet:        make(map[string]struct{}, len(policy.WorkingRoots)),
		executableSet:  make(map[string]struct{}, len(policy.Executables)),
		environmentSet: make(map[string]string, len(policy.Environment)),
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
	return normalized, nil
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
	_, forbidden := forbiddenSystemExecShells[strings.ToLower(filepath.Base(path))]
	return forbidden
}
