package companion

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	MaxAuthorityBrokerGroups          = 64
	MaxAuthorityBrokerWorkingScopes   = 32
	MaxAuthorityBrokerEnvironment     = 64
	MaxAuthorityBrokerFixedEnvBytes   = 64 * 1024
	MaxAuthorityBrokerPathBytes       = 4096
	DefaultAuthorityBrokerSocket      = "/run/mintclaw/node-authority-broker.sock"
	AuthorityBrokerProtocolVersion    = 1
	maxAuthorityBrokerConcurrentCalls = 8
)

var (
	authorityBrokerEnvironmentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	authorityBrokerDigestPattern      = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

type AuthorityBrokerConfig struct {
	SocketPath        string                            `json:"socket_path"`
	AllowedUID        uint32                            `json:"allowed_uid"`
	AllowedGID        uint32                            `json:"allowed_gid"`
	CompanionCgroup   string                            `json:"companion_cgroup"`
	Revision          string                            `json:"revision"`
	Profiles          map[string]AuthorityBrokerProfile `json:"profiles"`
	normalizedProfile map[string]normalizedAuthorityBrokerProfile
}

type AuthorityBrokerProfile struct {
	Revision                  string            `json:"revision"`
	ShellPath                 string            `json:"shell_path"`
	Login                     bool              `json:"login"`
	UID                       uint32            `json:"uid"`
	GID                       uint32            `json:"gid"`
	SupplementaryGroups       []uint32          `json:"supplementary_groups"`
	WorkingScopes             map[string]string `json:"working_scopes"`
	FixedEnvironment          map[string]string `json:"fixed_environment"`
	PermittedEnvironmentNames []string          `json:"permitted_environment_names"`
	Network                   string            `json:"network"`
	TimeoutSecondsMax         int               `json:"timeout_seconds_max"`
	OutputBytesMax            int               `json:"output_bytes_max"`
	ConcurrentCommands        int               `json:"concurrent_commands"`
}

type normalizedAuthorityBrokerProfile struct {
	AuthorityBrokerProfile
	alias            string
	environmentNames map[string]struct{}
}

type preparedAuthorityBrokerExecution struct {
	profile          normalizedAuthorityBrokerProfile
	shellPath        string
	shellArguments   []string
	workingDirectory string
	environment      []string
}

func NormalizeAuthorityBrokerConfig(
	config AuthorityBrokerConfig,
	baseDir string,
) (AuthorityBrokerConfig, error) {
	config.SocketPath = strings.TrimSpace(config.SocketPath)
	if config.SocketPath == "" {
		config.SocketPath = DefaultAuthorityBrokerSocket
	}
	socketPath, err := resolveAuthorityBrokerPath(baseDir, config.SocketPath, false)
	if err != nil || socketPath == string(filepath.Separator) {
		return AuthorityBrokerConfig{}, errors.New("authority broker socket path is invalid")
	}
	config.SocketPath = socketPath
	config.Revision = strings.TrimSpace(config.Revision)
	if !validShellBrokerRevision(config.Revision) {
		return AuthorityBrokerConfig{}, errors.New("authority broker revision is invalid")
	}
	if config.AllowedUID == 0 || config.AllowedGID == 0 {
		return AuthorityBrokerConfig{}, errors.New("authority broker companion peer must be unprivileged")
	}
	config.CompanionCgroup = strings.TrimSpace(config.CompanionCgroup)
	if config.CompanionCgroup == "" ||
		len(config.CompanionCgroup) > MaxAuthorityBrokerPathBytes ||
		!strings.HasPrefix(config.CompanionCgroup, "/") ||
		config.CompanionCgroup == "/" ||
		path.Clean(config.CompanionCgroup) != config.CompanionCgroup {
		return AuthorityBrokerConfig{}, errors.New("authority broker companion cgroup is invalid")
	}
	if len(config.Profiles) != MaxShellBrokerProfiles {
		return AuthorityBrokerConfig{}, errors.New("authority broker must configure exactly one P1 profile")
	}
	normalized := AuthorityBrokerConfig{
		SocketPath:        config.SocketPath,
		AllowedUID:        config.AllowedUID,
		AllowedGID:        config.AllowedGID,
		CompanionCgroup:   config.CompanionCgroup,
		Revision:          config.Revision,
		Profiles:          make(map[string]AuthorityBrokerProfile, len(config.Profiles)),
		normalizedProfile: make(map[string]normalizedAuthorityBrokerProfile, len(config.Profiles)),
	}
	for alias, profile := range config.Profiles {
		ready, normalizeErr := normalizeAuthorityBrokerProfile(alias, profile, baseDir)
		if normalizeErr != nil {
			return AuthorityBrokerConfig{}, normalizeErr
		}
		normalized.Profiles[ready.alias] = ready.AuthorityBrokerProfile
		normalized.normalizedProfile[ready.alias] = ready
	}
	return normalized, nil
}

func normalizeAuthorityBrokerProfile(
	alias string,
	profile AuthorityBrokerProfile,
	baseDir string,
) (normalizedAuthorityBrokerProfile, error) {
	alias = strings.TrimSpace(alias)
	profile.Revision = strings.TrimSpace(profile.Revision)
	if !validShellBrokerRevision(profile.Revision) {
		return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
			"authority broker profile %q revision is invalid",
			alias,
		)
	}
	shellPath, err := resolveAuthorityBrokerPath(baseDir, profile.ShellPath, true)
	if err != nil {
		return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
			"resolve authority broker profile %q shell: %w",
			alias,
			err,
		)
	}
	info, err := os.Stat(shellPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
			"authority broker profile %q shell is not executable",
			alias,
		)
	}
	if len(profile.SupplementaryGroups) > MaxAuthorityBrokerGroups ||
		len(profile.WorkingScopes) == 0 ||
		len(profile.WorkingScopes) > MaxAuthorityBrokerWorkingScopes ||
		len(profile.FixedEnvironment) > MaxAuthorityBrokerEnvironment ||
		len(profile.PermittedEnvironmentNames) > MaxAuthorityBrokerEnvironment ||
		profile.Network != "inherit" ||
		profile.TimeoutSecondsMax <= 0 ||
		profile.TimeoutSecondsMax > nodes.MaxInvocationTimeout ||
		profile.OutputBytesMax <= 0 ||
		profile.OutputBytesMax > 128*1024 ||
		profile.ConcurrentCommands <= 0 ||
		profile.ConcurrentCommands > maxAuthorityBrokerConcurrentCalls {
		return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
			"authority broker profile %q limits are invalid",
			alias,
		)
	}
	profile.ShellPath = shellPath
	profile.SupplementaryGroups = append([]uint32(nil), profile.SupplementaryGroups...)
	slices.Sort(profile.SupplementaryGroups)
	for index := 1; index < len(profile.SupplementaryGroups); index++ {
		if profile.SupplementaryGroups[index] == profile.SupplementaryGroups[index-1] {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q has duplicate groups",
				alias,
			)
		}
	}
	workingScopes := make(map[string]string, len(profile.WorkingScopes))
	for scope, configuredPath := range profile.WorkingScopes {
		scope = strings.TrimSpace(scope)
		path, pathErr := resolveAuthorityBrokerPath(baseDir, configuredPath, true)
		if pathErr != nil {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"resolve authority broker profile %q working scope: %w",
				alias,
				pathErr,
			)
		}
		info, statErr := os.Stat(path)
		if statErr != nil || !info.IsDir() {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q working scope is not a directory",
				alias,
			)
		}
		if _, duplicate := workingScopes[scope]; duplicate {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q has duplicate working scopes",
				alias,
			)
		}
		workingScopes[scope] = path
	}
	profile.WorkingScopes = workingScopes
	fixedEnvironment := make(map[string]string, len(profile.FixedEnvironment))
	totalEnvironmentBytes := 0
	for name, value := range profile.FixedEnvironment {
		if !authorityBrokerEnvironmentPattern.MatchString(name) ||
			strings.ContainsRune(value, 0) {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q fixed environment is invalid",
				alias,
			)
		}
		totalEnvironmentBytes += len(name) + len(value) + 1
		if totalEnvironmentBytes > MaxAuthorityBrokerFixedEnvBytes {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q fixed environment is too large",
				alias,
			)
		}
		fixedEnvironment[name] = value
	}
	profile.FixedEnvironment = fixedEnvironment
	permitted := append([]string(nil), profile.PermittedEnvironmentNames...)
	slices.Sort(permitted)
	environmentNames := make(map[string]struct{}, len(permitted))
	for _, name := range permitted {
		if !authorityBrokerEnvironmentPattern.MatchString(name) {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q permitted environment is invalid",
				alias,
			)
		}
		if _, fixed := fixedEnvironment[name]; fixed {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q environment is both fixed and supplied",
				alias,
			)
		}
		if _, duplicate := environmentNames[name]; duplicate {
			return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
				"authority broker profile %q has duplicate environment names",
				alias,
			)
		}
		environmentNames[name] = struct{}{}
	}
	profile.PermittedEnvironmentNames = permitted
	projection := ShellBrokerSnapshot{
		Revision: "projection-validation",
		Profiles: []ShellBrokerProfile{
			{
				Alias: alias, Revision: profile.Revision,
				WorkingScopes:      sortedAuthorityBrokerMapKeys(workingScopes),
				EnvironmentNames:   permitted,
				TimeoutSecondsMax:  profile.TimeoutSecondsMax,
				OutputBytesMax:     profile.OutputBytesMax,
				ConcurrentCommands: profile.ConcurrentCommands,
			},
		},
	}
	if _, err := normalizeShellBrokerSnapshot(projection); err != nil {
		return normalizedAuthorityBrokerProfile{}, fmt.Errorf(
			"authority broker profile %q safe projection is invalid: %w",
			alias,
			err,
		)
	}
	return normalizedAuthorityBrokerProfile{
		AuthorityBrokerProfile: profile,
		alias:                  alias,
		environmentNames:       environmentNames,
	}, nil
}

func (config AuthorityBrokerConfig) Snapshot() (ShellBrokerSnapshot, error) {
	if len(config.normalizedProfile) != MaxShellBrokerProfiles {
		return ShellBrokerSnapshot{}, errors.New("authority broker config is not normalized")
	}
	profiles := make([]ShellBrokerProfile, 0, len(config.normalizedProfile))
	for alias, profile := range config.normalizedProfile {
		profiles = append(profiles, ShellBrokerProfile{
			Alias: alias, Revision: profile.Revision,
			WorkingScopes:      sortedAuthorityBrokerMapKeys(profile.WorkingScopes),
			EnvironmentNames:   append([]string(nil), profile.PermittedEnvironmentNames...),
			TimeoutSecondsMax:  profile.TimeoutSecondsMax,
			OutputBytesMax:     profile.OutputBytesMax,
			ConcurrentCommands: profile.ConcurrentCommands,
		})
	}
	slices.SortFunc(profiles, func(left, right ShellBrokerProfile) int {
		return strings.Compare(left.Alias, right.Alias)
	})
	return normalizeShellBrokerSnapshot(ShellBrokerSnapshot{
		Revision: config.Revision,
		Profiles: profiles,
	})
}

func (config AuthorityBrokerConfig) prepareExecution(
	request ShellBrokerRequest,
) (preparedAuthorityBrokerExecution, error) {
	profile, ok := config.normalizedProfile[request.Profile]
	invocationValid := nodes.InvocationCancelRequest{
		InvocationID: request.InvocationID,
	}.Validate() == nil
	if !ok ||
		request.ProfileRevision != profile.Revision ||
		!invocationValid ||
		!authorityBrokerDigestPattern.MatchString(request.PlanHash) ||
		request.TimeoutSeconds <= 0 ||
		request.TimeoutSeconds > profile.TimeoutSecondsMax ||
		request.OutputBytesMax <= 0 ||
		request.OutputBytesMax > profile.OutputBytesMax {
		return preparedAuthorityBrokerExecution{}, errors.New("authority broker request is invalid")
	}
	if len(request.Script) == 0 ||
		len(request.Script) > nodes.MaxShellExecScriptBytes ||
		strings.ContainsRune(request.Script, 0) {
		return preparedAuthorityBrokerExecution{}, errors.New("authority broker script is invalid")
	}
	workingDirectory, ok := profile.WorkingScopes[request.WorkingScope]
	if !ok {
		return preparedAuthorityBrokerExecution{}, errors.New("authority broker working scope is invalid")
	}
	environment := make(map[string]string, len(profile.FixedEnvironment)+len(request.Environment))
	for name, value := range profile.FixedEnvironment {
		environment[name] = value
	}
	suppliedBytes := 0
	for name, value := range request.Environment {
		if _, allowed := profile.environmentNames[name]; !allowed ||
			strings.ContainsRune(value, 0) {
			return preparedAuthorityBrokerExecution{}, errors.New(
				"authority broker supplied environment is invalid",
			)
		}
		suppliedBytes += len(name) + len(value) + 1
		if suppliedBytes > nodes.MaxShellExecEnvironmentBytes {
			return preparedAuthorityBrokerExecution{}, errors.New(
				"authority broker supplied environment is too large",
			)
		}
		environment[name] = value
	}
	names := sortedAuthorityBrokerMapKeys(environment)
	encodedEnvironment := make([]string, 0, len(names))
	for _, name := range names {
		encodedEnvironment = append(encodedEnvironment, name+"="+environment[name])
	}
	shellArguments := []string{"-c", request.Script}
	if profile.Login {
		shellArguments = []string{"-l", "-c", request.Script}
	}
	return preparedAuthorityBrokerExecution{
		profile:          profile,
		shellPath:        profile.ShellPath,
		shellArguments:   shellArguments,
		workingDirectory: workingDirectory,
		environment:      encodedEnvironment,
	}, nil
}

func resolveAuthorityBrokerPath(baseDir, value string, requireExisting bool) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > MaxAuthorityBrokerPathBytes {
		return "", errors.New("path is empty or too large")
	}
	if !filepath.IsAbs(value) {
		value = filepath.Join(baseDir, value)
	}
	value = filepath.Clean(value)
	if !filepath.IsAbs(value) {
		return "", errors.New("path must be absolute")
	}
	if !requireExisting {
		return value, nil
	}
	resolved, err := filepath.EvalSymlinks(value)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func sortedAuthorityBrokerMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
