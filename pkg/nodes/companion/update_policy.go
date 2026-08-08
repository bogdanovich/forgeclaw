package companion

import (
	"errors"
	"fmt"
	"net/url"
	pathpkg "path"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	nodeupdate "github.com/bogdanovich/mintclaw/pkg/nodes/update"
)

const MaxUpdateSources = 8

var updateReleaseTagPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type UpdateSourceConfig struct {
	BaseURL   string `json:"base_url"`
	PublicKey string `json:"public_key"`
	Revoked   bool   `json:"revoked,omitempty"`

	trustedKey nodeupdate.TrustedKey
}

type UpdateReleaseConfig struct {
	Tag         string `json:"tag"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type UpdatePolicyProfile struct {
	Enabled        bool                           `json:"enabled"`
	Revision       string                         `json:"revision"`
	Source         string                         `json:"source"`
	Channel        nodeupdate.Channel             `json:"channel"`
	AllowDowngrade bool                           `json:"allow_downgrade,omitempty"`
	Approval       string                         `json:"approval,omitempty"`
	Releases       map[string]UpdateReleaseConfig `json:"releases,omitempty"`

	normalizedAlias string
}

type (
	UpdateSources  map[string]UpdateSourceConfig
	UpdatePolicies map[string]UpdatePolicyProfile
)

func normalizeUpdateConfiguration(
	sources UpdateSources,
	policies UpdatePolicies,
) (UpdateSources, UpdatePolicies, error) {
	normalizedSources, err := normalizeUpdateSources(sources)
	if err != nil {
		return nil, nil, err
	}
	normalizedPolicies, err := normalizeUpdatePolicies(policies, normalizedSources)
	if err != nil {
		return nil, nil, err
	}
	if len(normalizedSources) > 0 && len(normalizedPolicies) == 0 {
		return nil, nil, errors.New("node update sources require at least one deny-by-default policy")
	}
	if _, err = updateProfileDescriptors(normalizedPolicies); err != nil {
		return nil, nil, fmt.Errorf("project node update policy: %w", err)
	}
	return normalizedSources, normalizedPolicies, nil
}

func normalizeUpdateSources(sources UpdateSources) (UpdateSources, error) {
	if sources == nil {
		return nil, nil
	}
	if len(sources) == 0 || len(sources) > MaxUpdateSources {
		return nil, fmt.Errorf("node_update_sources must contain between 1 and %d sources", MaxUpdateSources)
	}
	normalized := make(UpdateSources, len(sources))
	folded := make(map[string]string, len(sources))
	for rawAlias, source := range sources {
		alias, err := normalizeUpdateAlias(rawAlias, folded, "source")
		if err != nil {
			return nil, err
		}
		parsed, err := url.Parse(source.BaseURL)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
			parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" {
			return nil, fmt.Errorf("node update source %q requires an absolute HTTPS base_url", alias)
		}
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
		if parsed.Path == "" || parsed.Path == "/" || pathpkg.Clean(parsed.Path) != parsed.Path {
			return nil, fmt.Errorf("node update source %q requires a non-root base_url", alias)
		}
		trustedKey, err := nodeupdate.ParsePublicKey(source.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("node update source %q public key: %w", alias, err)
		}
		source.BaseURL = parsed.String()
		source.trustedKey = trustedKey
		normalized[alias] = source
	}
	return normalized, nil
}

func normalizeUpdatePolicies(policies UpdatePolicies, sources UpdateSources) (UpdatePolicies, error) {
	if policies == nil {
		return nil, nil
	}
	if len(policies) == 0 || len(policies) > nodes.MaxUpdateProfiles {
		return nil, fmt.Errorf(
			"node_update_policies must contain between 1 and %d profiles",
			nodes.MaxUpdateProfiles,
		)
	}
	normalized := make(UpdatePolicies, len(policies))
	folded := make(map[string]string, len(policies))
	revisions := make(map[string]string, len(policies))
	for rawAlias, profile := range policies {
		alias, err := normalizeUpdateAlias(rawAlias, folded, "policy")
		if err != nil {
			return nil, err
		}
		profile, err = normalizeUpdatePolicyProfile(alias, profile, sources)
		if err != nil {
			return nil, fmt.Errorf("validate node update policy %q: %w", alias, err)
		}
		if prior, duplicate := revisions[profile.Revision]; duplicate {
			return nil, fmt.Errorf("node update policies %q and %q use the same revision", prior, alias)
		}
		revisions[profile.Revision] = alias
		normalized[alias] = profile
	}
	return normalized, nil
}

func normalizeUpdatePolicyProfile(
	alias string,
	profile UpdatePolicyProfile,
	sources UpdateSources,
) (UpdatePolicyProfile, error) {
	if !validInvocationPolicyRevision(profile.Revision) {
		return UpdatePolicyProfile{}, errors.New("revision is required and bounded")
	}
	if _, found := sources[profile.Source]; !found || profile.Source != strings.TrimSpace(profile.Source) {
		return UpdatePolicyProfile{}, errors.New("source must reference one configured trust source")
	}
	if profile.Enabled && sources[profile.Source].Revoked {
		return UpdatePolicyProfile{}, errors.New("enabled policy cannot use a revoked trust source")
	}
	if profile.Channel != nodeupdate.ChannelStable && profile.Channel != nodeupdate.ChannelNightly {
		return UpdatePolicyProfile{}, errors.New("channel must be stable or nightly")
	}
	if profile.Approval == "" {
		profile.Approval = "required"
	}
	if profile.Approval != "required" {
		return UpdatePolicyProfile{}, errors.New("approval must be required")
	}
	if !profile.Enabled {
		if len(profile.Releases) > 0 || profile.AllowDowngrade {
			return UpdatePolicyProfile{}, errors.New("disabled policy cannot retain release authority")
		}
		profile.Releases = nil
		profile.normalizedAlias = alias
		return profile, nil
	}
	if len(profile.Releases) == 0 || len(profile.Releases) > nodes.MaxUpdateReleases {
		return UpdatePolicyProfile{}, fmt.Errorf(
			"enabled policy must contain between 1 and %d releases",
			nodes.MaxUpdateReleases,
		)
	}
	releases := make(map[string]UpdateReleaseConfig, len(profile.Releases))
	folded := make(map[string]string, len(profile.Releases))
	versions := make(map[string]string, len(profile.Releases))
	for rawReleaseAlias, release := range profile.Releases {
		releaseAlias, err := normalizeUpdateAlias(rawReleaseAlias, folded, "release")
		if err != nil {
			return UpdatePolicyProfile{}, err
		}
		descriptor := nodes.UpdateReleaseDescriptor{
			Alias: releaseAlias, Version: release.Version, Description: release.Description,
		}
		if err = descriptor.Validate(string(profile.Channel)); err != nil {
			return UpdatePolicyProfile{}, err
		}
		if !updateReleaseTagPattern.MatchString(release.Tag) || release.Tag != strings.TrimSpace(release.Tag) {
			return UpdatePolicyProfile{}, errors.New("release tag is malformed")
		}
		if prior, duplicate := versions[release.Version]; duplicate {
			return UpdatePolicyProfile{}, fmt.Errorf(
				"update releases %q and %q select the same version",
				prior,
				releaseAlias,
			)
		}
		versions[release.Version] = releaseAlias
		releases[releaseAlias] = release
	}
	profile.Releases = releases
	profile.normalizedAlias = alias
	return profile, nil
}

func updateProfileDescriptors(policies UpdatePolicies) ([]nodes.UpdateProfileDescriptor, error) {
	profiles := make([]nodes.UpdateProfileDescriptor, 0, len(policies))
	for _, policy := range policies {
		if !policy.Enabled {
			continue
		}
		releases := make([]nodes.UpdateReleaseDescriptor, 0, len(policy.Releases))
		for alias, release := range policy.Releases {
			releases = append(releases, nodes.UpdateReleaseDescriptor{
				Alias: alias, Version: release.Version, Description: release.Description,
			})
		}
		sort.Slice(releases, func(left, right int) bool { return releases[left].Alias < releases[right].Alias })
		profiles = append(profiles, nodes.UpdateProfileDescriptor{
			Alias:     policy.normalizedAlias,
			Revision:  policy.Revision,
			Channel:   string(policy.Channel),
			Approval:  policy.Approval,
			Releases:  releases,
			Downgrade: policy.AllowDowngrade,
		})
	}
	sort.Slice(profiles, func(left, right int) bool { return profiles[left].Alias < profiles[right].Alias })
	for _, profile := range profiles {
		if err := profile.Validate(); err != nil {
			return nil, err
		}
	}
	return profiles, nil
}

func normalizeUpdateAlias(raw string, folded map[string]string, kind string) (string, error) {
	alias := strings.TrimSpace(raw)
	if alias != raw {
		return "", fmt.Errorf("node update %s alias must not contain surrounding whitespace", kind)
	}
	if err := (nodes.Alias(alias)).Validate(); err != nil {
		return "", fmt.Errorf("validate node update %s alias: %w", kind, err)
	}
	foldedAlias := strings.ToLower(alias)
	if prior, duplicate := folded[foldedAlias]; duplicate {
		return "", fmt.Errorf("node update %s aliases %q and %q collide", kind, prior, alias)
	}
	folded[foldedAlias] = alias
	return alias, nil
}

func validInvocationPolicyRevision(value string) bool {
	return len(value) > 0 && len(value) <= nodes.MaxPolicyRevisionLength &&
		value == strings.TrimSpace(value) && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0 && updateReleaseTagPattern.MatchString(value)
}
