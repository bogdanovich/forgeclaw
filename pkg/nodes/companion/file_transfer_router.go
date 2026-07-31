package companion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

type FileTransferCapability interface {
	TransferFrameHandler
	Descriptors() []nodes.CommandDescriptor
}

type FileTransferRouter struct {
	descriptors []nodes.CommandDescriptor
	byRevision  map[string]FileTransferCapability
}

func NewFileTransferRouter(
	sources ...FileTransferCapability,
) (*FileTransferRouter, error) {
	if len(sources) == 0 {
		return nil, errors.New("file transfer router requires a capability source")
	}
	router := &FileTransferRouter{
		byRevision: make(map[string]FileTransferCapability),
	}
	descriptorSets := make([][]nodes.CommandDescriptor, 0, len(sources))
	aliases := make(map[string]string)
	for _, source := range sources {
		if source == nil {
			return nil, errors.New("file transfer router source is unavailable")
		}
		descriptors := source.Descriptors()
		if err := validateFileCapabilitySet(descriptors); err != nil {
			return nil, err
		}
		for _, profile := range descriptors[0].FileProfiles {
			foldedAlias := strings.ToLower(profile.Alias)
			if prior, duplicate := aliases[foldedAlias]; duplicate {
				return nil, fmt.Errorf(
					"file transfer profiles %q and %q collide",
					prior,
					profile.Alias,
				)
			}
			if router.byRevision[profile.Revision] != nil {
				return nil, errors.New("file transfer profile revision is duplicated")
			}
			aliases[foldedAlias] = profile.Alias
			router.byRevision[profile.Revision] = source
		}
		descriptorSets = append(descriptorSets, descriptors)
	}
	if len(descriptorSets) == 1 {
		router.descriptors = cloneFileCapabilityDescriptors(descriptorSets[0])
		return router, nil
	}
	descriptors, err := mergeFileCapabilityDescriptors(descriptorSets)
	if err != nil {
		return nil, err
	}
	router.descriptors = descriptors
	return router, nil
}

func (router *FileTransferRouter) Descriptors() []nodes.CommandDescriptor {
	if router == nil {
		return nil
	}
	return cloneFileCapabilityDescriptors(router.descriptors)
}

func (router *FileTransferRouter) HandleTransferFrame(
	ctx context.Context,
	frame protocol.TransferFrame,
	send func(protocol.TransferFrame) error,
) error {
	if router == nil || send == nil {
		return errors.New("file transfer router is unavailable")
	}
	if err := frame.Validate(); err != nil {
		return err
	}
	source := router.byRevision[frame.PolicyRevision]
	if source == nil {
		return send(responseTransferFrame(
			frame,
			protocol.TransferFrameDeny,
			mustFileTransferResult(fileTransferResult{
				State: FileTransferFailed,
				Code:  "PROFILE_DENIED",
			}),
		))
	}
	return source.HandleTransferFrame(ctx, frame, send)
}

func validateFileCapabilitySet(descriptors []nodes.CommandDescriptor) error {
	if len(descriptors) != 3 {
		return errors.New("file capability source must contain three descriptors")
	}
	byName := make(map[string]nodes.CommandDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return err
		}
		byName[descriptor.Name] = descriptor
	}
	for _, name := range []string{"file.info.v1", "file.download.v1", "file.upload.v1"} {
		if _, found := byName[name]; !found {
			return fmt.Errorf("file capability source lacks %q", name)
		}
	}
	profiles := byName["file.info.v1"].FileProfiles
	for _, name := range []string{"file.download.v1", "file.upload.v1"} {
		if !reflect.DeepEqual(profiles, byName[name].FileProfiles) {
			return errors.New("file capability descriptors disagree on profiles")
		}
	}
	return nil
}

func mergeFileCapabilityDescriptors(
	sets [][]nodes.CommandDescriptor,
) ([]nodes.CommandDescriptor, error) {
	bySet := make([]map[string]nodes.CommandDescriptor, len(sets))
	profiles := make([]nodes.FileProfileDescriptor, 0)
	authorityDigests := make([]string, 0, len(sets))
	for index, set := range sets {
		bySet[index] = make(map[string]nodes.CommandDescriptor, len(set))
		for _, descriptor := range set {
			bySet[index][descriptor.Name] = descriptor
		}
		profiles = append(profiles, set[0].FileProfiles...)
		if set[0].ModelContract == nil || set[0].ModelContract.AuthorityDigest == "" {
			return nil, errors.New("file capability source lacks authority binding")
		}
		authorityDigests = append(authorityDigests, set[0].ModelContract.AuthorityDigest)
	}
	sort.Slice(profiles, func(left, right int) bool {
		return profiles[left].Alias < profiles[right].Alias
	})
	sort.Strings(authorityDigests)
	authorityData, err := json.Marshal(authorityDigests)
	if err != nil {
		return nil, err
	}
	authoritySum := sha256.Sum256(authorityData)
	authorityDigest := hex.EncodeToString(authoritySum[:])
	aliases := make([]string, len(profiles))
	for index, profile := range profiles {
		aliases[index] = profile.Alias
	}
	result := make([]nodes.CommandDescriptor, 0, 3)
	for _, name := range []string{"file.info.v1", "file.download.v1", "file.upload.v1"} {
		base := bySet[0][name]
		for index := 1; index < len(bySet); index++ {
			if !sameFileDescriptorShape(base, bySet[index][name]) {
				return nil, fmt.Errorf("file capability %q shapes disagree", name)
			}
		}
		base.FileProfiles = cloneFileProfileDescriptors(profiles)
		base.ModelContract = cloneModelContract(base.ModelContract)
		base.ModelContract.AuthorityDigest = authorityDigest
		base.ModelContract.Constraints.ProfileAliases = append([]string(nil), aliases...)
		if err := base.Validate(); err != nil {
			return nil, err
		}
		result = append(result, base)
	}
	return result, nil
}

func sameFileDescriptorShape(left, right nodes.CommandDescriptor) bool {
	if left.Name != right.Name ||
		!bytes.Equal(left.InputSchema, right.InputSchema) ||
		!bytes.Equal(left.OutputSchema, right.OutputSchema) ||
		left.Risk != right.Risk ||
		left.SupportsProgress != right.SupportsProgress ||
		left.SupportsCancel != right.SupportsCancel {
		return false
	}
	leftContract := cloneModelContract(left.ModelContract)
	rightContract := cloneModelContract(right.ModelContract)
	if leftContract == nil || rightContract == nil {
		return leftContract == nil && rightContract == nil
	}
	leftContract.AuthorityDigest = ""
	rightContract.AuthorityDigest = ""
	leftContract.Constraints.ProfileAliases = nil
	rightContract.Constraints.ProfileAliases = nil
	return reflect.DeepEqual(leftContract, rightContract)
}

func cloneFileCapabilityDescriptors(
	descriptors []nodes.CommandDescriptor,
) []nodes.CommandDescriptor {
	result := make([]nodes.CommandDescriptor, len(descriptors))
	for index, descriptor := range descriptors {
		result[index] = descriptor
		result[index].InputSchema = append(json.RawMessage(nil), descriptor.InputSchema...)
		result[index].OutputSchema = append(json.RawMessage(nil), descriptor.OutputSchema...)
		result[index].ModelContract = cloneModelContract(descriptor.ModelContract)
		result[index].FileProfiles = cloneFileProfileDescriptors(descriptor.FileProfiles)
	}
	return result
}
