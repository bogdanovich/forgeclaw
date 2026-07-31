package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

const (
	MaxFilePolicyProfiles = 32
	MaxFilePolicyRoots    = 32
	MaxFilePathBytes      = 4096
)

var filePolicyRevisionPattern = regexp.MustCompile(
	`^[A-Za-z0-9][A-Za-z0-9._:-]*$`,
)

type FileApprovalRequirement string

const (
	FileApprovalNone     FileApprovalRequirement = "none"
	FileApprovalRequired FileApprovalRequirement = "required"
)

type FileApprovalPolicy struct {
	Metadata FileApprovalRequirement `json:"metadata,omitempty"`
	Read     FileApprovalRequirement `json:"read,omitempty"`
	Write    FileApprovalRequirement `json:"write,omitempty"`
}

type FilePolicyProfile struct {
	Enabled         bool               `json:"enabled"`
	Revision        string             `json:"revision"`
	ReadableRoots   []string           `json:"readable_roots,omitempty"`
	WritableRoots   []string           `json:"writable_roots,omitempty"`
	AllowCreate     bool               `json:"allow_create,omitempty"`
	AllowOverwrite  bool               `json:"allow_overwrite,omitempty"`
	FollowSymlinks  bool               `json:"follow_symlinks,omitempty"`
	CrossMounts     bool               `json:"cross_mounts,omitempty"`
	MaxFileBytes    int64              `json:"max_file_bytes,omitempty"`
	Approval        FileApprovalPolicy `json:"approval,omitempty"`
	normalizedAlias string
}

type FilePolicies map[string]FilePolicyProfile

func normalizeFilePolicies(
	policies FilePolicies,
	baseDir string,
) (FilePolicies, error) {
	if policies == nil {
		return nil, nil
	}
	if len(policies) == 0 || len(policies) > MaxFilePolicyProfiles {
		return nil, errors.New("node_file_policies must contain between 1 and 32 profiles")
	}
	normalized := make(FilePolicies, len(policies))
	aliases := make([]string, 0, len(policies))
	revisions := make(map[string]string, len(policies))
	for rawAlias, rawProfile := range policies {
		alias := strings.TrimSpace(rawAlias)
		if alias != rawAlias {
			return nil, errors.New("file policy alias must not contain surrounding whitespace")
		}
		if err := (nodes.Alias(alias)).Validate(); err != nil {
			return nil, fmt.Errorf("validate file policy alias: %w", err)
		}
		profile, err := normalizeFilePolicyProfile(alias, rawProfile, baseDir)
		if err != nil {
			return nil, fmt.Errorf("validate file policy %q: %w", alias, err)
		}
		if prior, duplicate := revisions[profile.Revision]; duplicate {
			return nil, fmt.Errorf(
				"file policies %q and %q use the same revision",
				prior,
				alias,
			)
		}
		revisions[profile.Revision] = alias
		normalized[alias] = profile
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	if len(aliases) != len(normalized) {
		return nil, errors.New("duplicate file policy alias")
	}
	return normalized, nil
}

func normalizeFilePolicyProfile(
	alias string,
	profile FilePolicyProfile,
	baseDir string,
) (FilePolicyProfile, error) {
	if profile.Revision == "" ||
		len(profile.Revision) > nodes.MaxPolicyRevisionLength ||
		!filePolicyRevisionPattern.MatchString(profile.Revision) {
		return FilePolicyProfile{}, errors.New("revision is required and bounded")
	}
	if profile.FollowSymlinks {
		return FilePolicyProfile{}, errors.New("follow_symlinks is not supported")
	}
	if profile.MaxFileBytes == 0 {
		profile.MaxFileBytes = protocol.MaxTransferFileBytes
	}
	if profile.MaxFileBytes < 0 ||
		profile.MaxFileBytes > protocol.MaxTransferFileBytes {
		return FilePolicyProfile{}, errors.New("max_file_bytes exceeds the transfer ceiling")
	}
	if err := normalizeFileApprovalPolicy(&profile.Approval); err != nil {
		return FilePolicyProfile{}, err
	}
	readable, err := normalizeFileRoots(profile.ReadableRoots, baseDir)
	if err != nil {
		return FilePolicyProfile{}, fmt.Errorf("readable_roots: %w", err)
	}
	writable, err := normalizeFileRoots(profile.WritableRoots, baseDir)
	if err != nil {
		return FilePolicyProfile{}, fmt.Errorf("writable_roots: %w", err)
	}
	if profile.Enabled && len(readable) == 0 && len(writable) == 0 {
		return FilePolicyProfile{}, errors.New("enabled profile requires at least one root")
	}
	if !profile.Enabled &&
		(len(readable) > 0 ||
			len(writable) > 0 ||
			profile.AllowCreate ||
			profile.AllowOverwrite) {
		return FilePolicyProfile{}, errors.New("disabled profile cannot retain file authority")
	}
	if len(writable) == 0 && (profile.AllowCreate || profile.AllowOverwrite) {
		return FilePolicyProfile{}, errors.New("publication modes require a writable root")
	}
	profile.ReadableRoots = readable
	profile.WritableRoots = writable
	profile.normalizedAlias = alias
	return profile, nil
}

func HasEnabledFilePolicy(policies FilePolicies) bool {
	for _, profile := range policies {
		if profile.Enabled {
			return true
		}
	}
	return false
}

func normalizeFileApprovalPolicy(policy *FileApprovalPolicy) error {
	if policy.Metadata == "" {
		policy.Metadata = FileApprovalNone
	}
	if policy.Read == "" {
		policy.Read = FileApprovalNone
	}
	if policy.Write == "" {
		policy.Write = FileApprovalNone
	}
	for _, requirement := range []FileApprovalRequirement{
		policy.Metadata,
		policy.Read,
		policy.Write,
	} {
		if requirement != FileApprovalNone &&
			requirement != FileApprovalRequired {
			return errors.New("approval requirements must be none or required")
		}
	}
	return nil
}

func normalizeFileRoots(roots []string, baseDir string) ([]string, error) {
	if roots == nil {
		return nil, nil
	}
	if len(roots) == 0 || len(roots) > MaxFilePolicyRoots {
		return nil, errors.New("roots must contain between 1 and 32 paths")
	}
	normalized := make([]string, 0, len(roots))
	seen := make(map[string]struct{}, len(roots))
	for _, raw := range roots {
		if raw == "" || strings.TrimSpace(raw) != raw || strings.ContainsRune(raw, 0) {
			return nil, errors.New("root is empty or malformed")
		}
		if !filepath.IsAbs(raw) {
			return nil, errors.New("root must be absolute")
		}
		root, err := resolveConfigPath(baseDir, raw)
		if err != nil {
			return nil, err
		}
		root, err = filepath.EvalSymlinks(root)
		if err != nil {
			return nil, errors.New("root must resolve to an existing directory")
		}
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			return nil, errors.New("root must resolve to an existing directory")
		}
		if !filepath.IsAbs(root) || len(root) > MaxFilePathBytes ||
			!utf8.ValidString(root) ||
			strings.IndexFunc(root, unicode.IsControl) >= 0 {
			return nil, errors.New("root must be a bounded absolute UTF-8 path")
		}
		if _, duplicate := seen[root]; duplicate {
			return nil, errors.New("duplicate file root")
		}
		seen[root] = struct{}{}
		normalized = append(normalized, root)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func fileCapabilityDescriptors(policies FilePolicies) ([]nodes.CommandDescriptor, error) {
	enabled := make([]FilePolicyProfile, 0, len(policies))
	for _, profile := range policies {
		if profile.Enabled {
			enabled = append(enabled, profile)
		}
	}
	if len(enabled) == 0 {
		return nil, nil
	}
	sort.Slice(enabled, func(left, right int) bool {
		return enabled[left].normalizedAlias < enabled[right].normalizedAlias
	})
	authorityData, err := json.Marshal(enabled)
	if err != nil {
		return nil, fmt.Errorf("encode file capability authority: %w", err)
	}
	authoritySum := sha256.Sum256(authorityData)
	authorityDigest := hex.EncodeToString(authoritySum[:])
	profileAliases := make([]string, 0, len(enabled))
	for _, profile := range enabled {
		profileAliases = append(profileAliases, profile.normalizedAlias)
	}
	contract := &nodes.CommandModelContract{
		Availability:      nodes.ModelUnavailable,
		TimeoutSecondsMax: nodes.MaxInvocationTimeout,
		OutputBytesMax:    nodes.MaxInvocationOutput,
		ResultKind:        "json",
		AuthorityDigest:   authorityDigest,
		Constraints: nodes.CommandModelConstraints{
			ProfileAliases: profileAliases,
		},
		Guidance: []string{},
		Examples: []json.RawMessage{},
	}
	descriptors := []nodes.CommandDescriptor{
		fileCapabilityDescriptor("file.info.v1", nodes.RiskRead, contract),
		fileCapabilityDescriptor("file.download.v1", nodes.RiskRead, contract),
		fileCapabilityDescriptor("file.upload.v1", nodes.RiskWrite, contract),
	}
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return nil, err
		}
	}
	return descriptors, nil
}

func fileCapabilityDescriptor(
	name string,
	risk nodes.Risk,
	contract *nodes.CommandModelContract,
) nodes.CommandDescriptor {
	input := json.RawMessage(
		`{"additionalProperties":false,"properties":{},"type":"object"}`,
	)
	output := json.RawMessage(
		`{"additionalProperties":true,"properties":{},"type":"object"}`,
	)
	clonedContract := *contract
	clonedContract.Constraints.ProfileAliases = append(
		[]string(nil),
		contract.Constraints.ProfileAliases...,
	)
	return nodes.CommandDescriptor{
		Name:             name,
		InputSchema:      input,
		OutputSchema:     output,
		Risk:             risk,
		SupportsProgress: true,
		SupportsCancel:   true,
		ModelContract:    &clonedContract,
	}
}
