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
	foldedAliases := make(map[string]string, len(policies))
	revisions := make(map[string]string, len(policies))
	for rawAlias, rawProfile := range policies {
		alias := strings.TrimSpace(rawAlias)
		if alias != rawAlias {
			return nil, errors.New("file policy alias must not contain surrounding whitespace")
		}
		if err := (nodes.Alias(alias)).Validate(); err != nil {
			return nil, fmt.Errorf("validate file policy alias: %w", err)
		}
		foldedAlias := strings.ToLower(alias)
		if prior, duplicate := foldedAliases[foldedAlias]; duplicate {
			return nil, fmt.Errorf(
				"file policy aliases %q and %q collide",
				prior,
				alias,
			)
		}
		foldedAliases[foldedAlias] = alias
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
	profileDescriptors := make([]nodes.FileProfileDescriptor, 0, len(enabled))
	for _, profile := range enabled {
		profileAliases = append(profileAliases, profile.normalizedAlias)
		profileDescriptors = append(profileDescriptors, nodes.FileProfileDescriptor{
			Alias:          profile.normalizedAlias,
			Revision:       profile.Revision,
			ReadableRoots:  append([]string(nil), profile.ReadableRoots...),
			WritableRoots:  append([]string(nil), profile.WritableRoots...),
			AllowCreate:    profile.AllowCreate,
			AllowOverwrite: profile.AllowOverwrite,
			MaxFileBytes:   profile.MaxFileBytes,
			Approval: nodes.FileProfileApproval{
				Metadata: string(profile.Approval.Metadata),
				Read:     string(profile.Approval.Read),
				Write:    string(profile.Approval.Write),
			},
		})
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
		fileCapabilityDescriptor("file.info.v1", nodes.RiskRead, contract, profileDescriptors),
		fileCapabilityDescriptor("file.download.v1", nodes.RiskRead, contract, profileDescriptors),
		fileCapabilityDescriptor("file.upload.v1", nodes.RiskWrite, contract, profileDescriptors),
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
	profiles []nodes.FileProfileDescriptor,
) nodes.CommandDescriptor {
	input := fileTransferPlanInputSchema(name)
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
		FileProfiles:     cloneFileProfileDescriptors(profiles),
	}
}

func fileTransferPlanInputSchema(name string) json.RawMessage {
	const commonProperties = `"route_id":{"maxLength":128,"minLength":1,"type":"string"},` +
		`"discovery_revision":{"maxLength":128,"minLength":1,"type":"string"}`
	switch name {
	case "file.info.v1":
		return json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"path":{"maxLength":4096,"minLength":1,"type":"string"},` +
				commonProperties +
				`},"required":["path","route_id","discovery_revision"],"type":"object"}`,
		)
	case "file.upload.v1":
		return json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"artifact_ref":{"maxLength":256,"minLength":1,"type":"string"},` +
				`"source_artifact_id":{"maxLength":128,"minLength":1,"type":"string"},` +
				`"destination":{"maxLength":4096,"minLength":1,"type":"string"},` +
				`"publication":{"enum":["create","replace"],"type":"string"},` +
				`"size":{"maximum":1073741824,"minimum":0,"type":"integer"},` +
				`"sha256":{"maxLength":64,"minLength":64,"type":"string"},` +
				`"filename":{"maxLength":255,"minLength":1,"type":"string"},` +
				`"content_type":{"maxLength":255,"type":"string"},` +
				commonProperties +
				`},"required":["artifact_ref","source_artifact_id","destination","publication",` +
				`"size","sha256",` +
				`"filename","route_id","discovery_revision"],"type":"object"}`,
		)
	case "file.download.v1":
		return json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"source":{"maxLength":4096,"minLength":1,"type":"string"},` +
				`"deliver":{"type":"boolean"},` +
				`"size":{"maximum":1073741824,"minimum":0,"type":"integer"},` +
				`"sha256":{"maxLength":64,"minLength":64,"type":"string"},` +
				`"filename":{"maxLength":255,"minLength":1,"type":"string"},` +
				`"content_type":{"maxLength":255,"type":"string"},` +
				`"channel":{"maxLength":64,"type":"string"},` +
				`"chat_id":{"maxLength":512,"type":"string"},` +
				`"topic_id":{"maxLength":512,"type":"string"},` +
				commonProperties +
				`},"required":["source","deliver","size","sha256","filename","route_id",` +
				`"discovery_revision"],"type":"object"}`,
		)
	default:
		return json.RawMessage("false")
	}
}

func cloneFileProfileDescriptors(
	profiles []nodes.FileProfileDescriptor,
) []nodes.FileProfileDescriptor {
	result := make([]nodes.FileProfileDescriptor, len(profiles))
	for index, profile := range profiles {
		result[index] = profile
		result[index].ReadableRoots = append([]string(nil), profile.ReadableRoots...)
		result[index].WritableRoots = append([]string(nil), profile.WritableRoots...)
	}
	return result
}
