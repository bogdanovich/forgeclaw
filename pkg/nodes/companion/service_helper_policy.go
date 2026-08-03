package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	ServiceHelperProtocolVersion = 1
	MaxServiceHelperConfigBytes  = 1024 * 1024
	MaxServiceHelperMessageBytes = MaxAuthorityBrokerFrameBytes
)

type ServiceHelperServiceConfig struct {
	SocketPath      string          `json:"socket_path"`
	AllowedUID      uint32          `json:"allowed_uid"`
	AllowedGID      uint32          `json:"allowed_gid"`
	CompanionCgroup string          `json:"companion_cgroup"`
	SystemctlPath   string          `json:"systemctl_path"`
	JournalctlPath  string          `json:"journalctl_path,omitempty"`
	Profiles        ServicePolicies `json:"node_service_policies"`

	normalized        bool
	systemctlIdentity string
	journalIdentity   string
}

func NormalizeServiceHelperServiceConfig(
	config ServiceHelperServiceConfig,
	baseDir string,
) (ServiceHelperServiceConfig, error) {
	var err error
	config.SocketPath, err = normalizeServiceHelperPath(baseDir, config.SocketPath, "socket")
	if err != nil || config.SocketPath == string(filepath.Separator) {
		return ServiceHelperServiceConfig{}, errors.New("service helper socket path is invalid")
	}
	if config.AllowedUID == 0 || config.AllowedGID == 0 {
		return ServiceHelperServiceConfig{}, errors.New("service helper companion peer must be unprivileged")
	}
	config.CompanionCgroup = strings.TrimSpace(config.CompanionCgroup)
	if config.CompanionCgroup == "" ||
		len(config.CompanionCgroup) > MaxAuthorityBrokerPathBytes ||
		!strings.HasPrefix(config.CompanionCgroup, "/") ||
		config.CompanionCgroup == "/" ||
		path.Clean(config.CompanionCgroup) != config.CompanionCgroup {
		return ServiceHelperServiceConfig{}, errors.New("service helper companion cgroup is invalid")
	}
	config.Profiles, err = normalizeServicePolicies(config.Profiles)
	if err != nil {
		return ServiceHelperServiceConfig{}, fmt.Errorf("validate service helper policies: %w", err)
	}
	if len(config.Profiles) != 1 || !hasEnabledServicePolicy(config.Profiles) {
		return ServiceHelperServiceConfig{}, errors.New("service helper requires exactly one enabled profile")
	}
	if !serviceActionRequired(config.Profiles) {
		return ServiceHelperServiceConfig{}, errors.New("service helper profile grants no action")
	}
	config.SystemctlPath, err = normalizeServiceHelperExecutablePath(
		baseDir,
		config.SystemctlPath,
		"systemctl",
	)
	if err != nil {
		return ServiceHelperServiceConfig{}, err
	}
	_, needLogs := serviceReadRequirements(config.Profiles)
	if needLogs {
		config.JournalctlPath, err = normalizeServiceHelperExecutablePath(
			baseDir,
			config.JournalctlPath,
			"journalctl",
		)
		if err != nil {
			return ServiceHelperServiceConfig{}, err
		}
	} else if strings.TrimSpace(config.JournalctlPath) != "" {
		return ServiceHelperServiceConfig{}, errors.New("service helper journalctl is configured without log authority")
	}
	config.normalized = true
	return config, nil
}

func normalizeServiceHelperPath(baseDir, value, label string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("service helper %s path is required", label)
	}
	resolved, err := resolveConfigPath(baseDir, value)
	if err != nil || !validFileHelperServicePath(resolved) ||
		resolved == string(filepath.Separator) {
		return "", fmt.Errorf("service helper %s path is invalid", label)
	}
	return resolved, nil
}

func normalizeServiceHelperExecutablePath(baseDir, value, label string) (string, error) {
	if !filepath.IsAbs(strings.TrimSpace(value)) {
		return "", fmt.Errorf("service helper %s path must be absolute", label)
	}
	return normalizeServiceHelperPath(baseDir, value, label)
}

func (config ServiceHelperServiceConfig) Descriptors() ([]nodes.CommandDescriptor, error) {
	if !config.normalized {
		return nil, errors.New("service helper configuration is not normalized")
	}
	if !validFileHelperDigest(config.systemctlIdentity) ||
		(config.JournalctlPath != "" && !validFileHelperDigest(config.journalIdentity)) ||
		(config.JournalctlPath == "" && config.journalIdentity != "") {
		return nil, errors.New("service helper executables are not pinned")
	}
	digest, err := serviceHelperServiceDigest(config)
	if err != nil {
		return nil, err
	}
	descriptors, err := serviceCapabilityDescriptors(
		config.Profiles,
		serviceEnforcement{status: true, logs: true, actions: true},
		"linux",
	)
	if err != nil {
		return nil, err
	}
	for index := range descriptors {
		if descriptors[index].ModelContract == nil {
			return nil, errors.New("service helper descriptor lacks model contract")
		}
		combined, combineErr := combineServiceHelperAuthority(
			descriptors[index].ModelContract.AuthorityDigest,
			digest,
		)
		if combineErr != nil {
			return nil, combineErr
		}
		descriptors[index].ModelContract.AuthorityDigest = combined
	}
	if err := validateServiceHelperDescriptors(descriptors); err != nil {
		return nil, err
	}
	return cloneCatalog(nodes.CapabilityCatalog{Commands: descriptors}).Commands, nil
}

func serviceHelperServiceDigest(config ServiceHelperServiceConfig) (string, error) {
	if !config.normalized {
		return "", errors.New("service helper configuration is not normalized")
	}
	binding := struct {
		Version         int             `json:"version"`
		SocketPath      string          `json:"socket_path"`
		AllowedUID      uint32          `json:"allowed_uid"`
		AllowedGID      uint32          `json:"allowed_gid"`
		CompanionCgroup string          `json:"companion_cgroup"`
		SystemctlPath   string          `json:"systemctl_path"`
		JournalctlPath  string          `json:"journalctl_path,omitempty"`
		SystemctlID     string          `json:"systemctl_identity"`
		JournalctlID    string          `json:"journalctl_identity,omitempty"`
		Profiles        ServicePolicies `json:"node_service_policies"`
	}{
		Version:         ServiceHelperProtocolVersion,
		SocketPath:      config.SocketPath,
		AllowedUID:      config.AllowedUID,
		AllowedGID:      config.AllowedGID,
		CompanionCgroup: config.CompanionCgroup,
		SystemctlPath:   config.SystemctlPath,
		JournalctlPath:  config.JournalctlPath,
		SystemctlID:     config.systemctlIdentity,
		JournalctlID:    config.journalIdentity,
		Profiles:        config.Profiles,
	}
	data, err := json.Marshal(binding)
	if err != nil {
		return "", fmt.Errorf("encode service helper authority: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func combineServiceHelperAuthority(policyDigest, serviceDigest string) (string, error) {
	if !validFileHelperDigest(policyDigest) || !validFileHelperDigest(serviceDigest) {
		return "", errors.New("service helper authority digest is invalid")
	}
	data, err := json.Marshal([]string{policyDigest, serviceDigest})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validateServiceHelperDescriptors(descriptors []nodes.CommandDescriptor) error {
	if len(descriptors) == 0 || len(descriptors) > 3 {
		return errors.New("service helper snapshot has invalid capability count")
	}
	identity := ""
	seen := make(map[string]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("validate service helper descriptor: %w", err)
		}
		if !nodes.IsServiceCommand(descriptor.Name) {
			return errors.New("service helper snapshot contains a non-service capability")
		}
		if _, duplicate := seen[descriptor.Name]; duplicate {
			return errors.New("service helper snapshot contains a duplicate capability")
		}
		seen[descriptor.Name] = struct{}{}
		if len(descriptor.ServiceProfiles) != 1 || descriptor.ModelContract == nil ||
			!validFileHelperDigest(descriptor.ModelContract.AuthorityDigest) {
			return errors.New("service helper capability lacks one bound profile")
		}
		profile := descriptor.ServiceProfiles[0]
		current := profile.Alias + "\x00" + profile.Revision
		if identity == "" {
			identity = current
		} else if current != identity {
			return errors.New("service helper capabilities disagree on profile identity")
		}
	}
	if _, actions := seen["service.action.v1"]; !actions {
		return errors.New("service helper snapshot lacks action capability")
	}
	return nil
}
