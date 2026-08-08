package nodes

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var updateVersionPattern = regexp.MustCompile(
	`^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)` +
		`(?:-[0-9A-Za-z][0-9A-Za-z-]*(?:\.[0-9A-Za-z][0-9A-Za-z-]*)*)?$`,
)

const (
	MaxUpdateProfiles        = 16
	MaxUpdateReleases        = 16
	MaxUpdateVersionBytes    = 64
	MaxUpdateDescriptionSize = 256
)

type UpdateReleaseDescriptor struct {
	Alias       string `json:"alias"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type UpdateProfileDescriptor struct {
	Alias     string                    `json:"alias"`
	Revision  string                    `json:"revision"`
	Channel   string                    `json:"channel"`
	Approval  string                    `json:"approval"`
	Releases  []UpdateReleaseDescriptor `json:"releases"`
	Downgrade bool                      `json:"downgrade,omitempty"`
}

func (profile UpdateProfileDescriptor) Validate() error {
	if err := (Alias(profile.Alias)).Validate(); err != nil ||
		!validInvocationIdentifier(profile.Revision) ||
		(profile.Channel != "stable" && profile.Channel != "nightly") ||
		profile.Approval != "required" ||
		len(profile.Releases) == 0 || len(profile.Releases) > MaxUpdateReleases {
		return fmt.Errorf("%w: malformed update profile descriptor", ErrInvalidCapability)
	}
	priorAlias := ""
	versions := make(map[string]struct{}, len(profile.Releases))
	for _, release := range profile.Releases {
		if err := release.Validate(profile.Channel); err != nil {
			return err
		}
		if priorAlias != "" && release.Alias <= priorAlias {
			return fmt.Errorf("%w: update releases are not sorted", ErrInvalidCapability)
		}
		if _, duplicate := versions[release.Version]; duplicate {
			return fmt.Errorf("%w: duplicate update version", ErrInvalidCapability)
		}
		versions[release.Version] = struct{}{}
		priorAlias = release.Alias
	}
	return nil
}

func (release UpdateReleaseDescriptor) Validate(channel string) error {
	if err := (Alias(release.Alias)).Validate(); err != nil ||
		len(release.Version) == 0 || len(release.Version) > MaxUpdateVersionBytes ||
		!validUpdateVersion(release.Version) ||
		len(release.Description) > MaxUpdateDescriptionSize ||
		release.Description != strings.TrimSpace(release.Description) ||
		containsModelControl(release.Description) {
		return fmt.Errorf("%w: malformed update release descriptor", ErrInvalidCapability)
	}
	prerelease := strings.Contains(release.Version, "-")
	if (channel == "stable" && prerelease) || (channel == "nightly" && !prerelease) {
		return fmt.Errorf("%w: update release does not match channel", ErrInvalidCapability)
	}
	return nil
}

func NodeUpdateInputSchema(profiles []UpdateProfileDescriptor) json.RawMessage {
	releases := make(map[string]struct{})
	for _, profile := range profiles {
		for _, release := range profile.Releases {
			releases[release.Alias] = struct{}{}
		}
	}
	aliases := make([]string, 0, len(releases))
	for alias := range releases {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	schema, _ := json.Marshal(map[string]any{
		"type":     "object",
		"required": []string{"release"},
		"properties": map[string]any{
			"release": map[string]any{"type": "string", "enum": aliases},
		},
		"additionalProperties": false,
	})
	return schema
}

func validUpdateVersion(value string) bool {
	return updateVersionPattern.MatchString(value)
}

func CloneUpdateProfileDescriptors(profiles []UpdateProfileDescriptor) []UpdateProfileDescriptor {
	cloned := make([]UpdateProfileDescriptor, len(profiles))
	for index, profile := range profiles {
		cloned[index] = profile
		cloned[index].Releases = append([]UpdateReleaseDescriptor(nil), profile.Releases...)
	}
	return cloned
}
