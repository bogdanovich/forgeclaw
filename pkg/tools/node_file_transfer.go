package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
	"github.com/bogdanovich/mintclaw/pkg/routing"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
)

const nodeFileTransferTTL = nodes.MaxExecutionPlanTTL

type NodeFileTransferSource interface {
	NodeInvocationSource
	SnapshotUploadArtifact(
		context.Context,
		nodes.TransferArtifactOwner,
		string,
		string,
		string,
		int64,
		int64,
		string,
		media.MediaStore,
		media.MediaOwner,
	) (nodes.TransferArtifactRecord, error)
	InspectFile(
		context.Context,
		nodes.ID,
		NodeFileTransferBinding,
	) (NodeFileTransferResult, error)
	DispatchFileTransfer(
		context.Context,
		nodes.GatewayInvocationOwner,
		nodes.GatewayInvocationRecord,
	) (NodeFileTransferResult, bool, error)
	QueryFileTransfer(
		context.Context,
		nodes.GatewayInvocationPrincipal,
		nodes.GatewayInvocationRecord,
	) (NodeFileTransferResult, error)
	CancelFileTransfer(
		context.Context,
		nodes.GatewayInvocationPrincipal,
		nodes.GatewayInvocationRecord,
	) (NodeFileTransferResult, bool, error)
	HandoffDownloadedArtifact(
		context.Context,
		nodes.TransferArtifactOwner,
		string,
		media.MediaStore,
		media.MediaOwner,
	) (string, bool, error)
}

type NodeFileTransferBinding struct {
	TransferID     string
	Direction      protocol.TransferDirection
	ProfileAlias   string
	PolicyRevision string
	Path           string
	Publication    string
	TotalSize      uint64
	SHA256         [sha256.Size]byte
	ExpiresAt      int64
	Filename       string
	ContentType    string
}

type NodeFileTransferResult struct {
	TransferID     string `json:"transfer_id"`
	State          string `json:"state"`
	Path           string `json:"path,omitempty"`
	Type           string `json:"type,omitempty"`
	Size           uint64 `json:"size,omitempty"`
	Mode           uint32 `json:"mode,omitempty"`
	ModifiedAt     int64  `json:"modified_at,omitempty"`
	SHA256         string `json:"sha256,omitempty"`
	Sequence       uint64 `json:"sequence,omitempty"`
	Transferred    uint64 `json:"transferred,omitempty"`
	Code           string `json:"code,omitempty"`
	ArtifactRef    string `json:"artifact_ref,omitempty"`
	Filename       string `json:"filename,omitempty"`
	ContentType    string `json:"content_type,omitempty"`
	PolicyRevision string `json:"policy_revision,omitempty"`
	DeliveryState  string `json:"delivery_state,omitempty"`
	RecoveryAction string `json:"recovery_action,omitempty"`
}

type nodeFileTransferPlanInput struct {
	Path              string  `json:"path,omitempty"`
	Destination       string  `json:"destination,omitempty"`
	Source            string  `json:"source,omitempty"`
	Publication       string  `json:"publication,omitempty"`
	ArtifactRef       string  `json:"artifact_ref,omitempty"`
	SourceArtifactID  string  `json:"source_artifact_id,omitempty"`
	Size              float64 `json:"size,omitempty"`
	SHA256            string  `json:"sha256,omitempty"`
	Filename          string  `json:"filename,omitempty"`
	ContentType       string  `json:"content_type,omitempty"`
	Deliver           *bool   `json:"deliver,omitempty"`
	Channel           string  `json:"channel,omitempty"`
	ChatID            string  `json:"chat_id,omitempty"`
	TopicID           string  `json:"topic_id,omitempty"`
	RouteID           string  `json:"route_id"`
	DiscoveryRevision string  `json:"discovery_revision"`
}

type NodeFileInfoTool struct {
	runtime *nodeFileTransferToolRuntime
}

type NodeUploadTool struct {
	runtime    *nodeFileTransferToolRuntime
	mediaStore media.MediaStore
}

type NodeDownloadTool struct {
	runtime    *nodeFileTransferToolRuntime
	mediaStore media.MediaStore
}

type nodeFileTransferToolRuntime struct {
	access          *nodeTargetAccess
	source          NodeFileTransferSource
	permittedAgents map[string]struct{}
	runtimeEvents   runtimeevents.Bus
}

type preparedNodeFileTransfer struct {
	record  nodes.GatewayInvocationRecord
	profile nodes.FileProfileDescriptor
	owner   nodes.TransferArtifactOwner
}

type nodeFileSafeDenialError struct {
	code  string
	cause error
}

func (denial *nodeFileSafeDenialError) Error() string {
	return "node file transfer denied"
}

func (denial *nodeFileSafeDenialError) Unwrap() error {
	return denial.cause
}

func (denial *nodeFileSafeDenialError) SafeApprovalDenialResult() *ToolResult {
	return nodeFileErrorResult(map[string]any{
		"state": "denied",
		"code":  denial.code,
	})
}

func NewNodeFileInfoTool(cfg *config.Config, source NodeFileTransferSource) *NodeFileInfoTool {
	return &NodeFileInfoTool{runtime: newNodeFileTransferToolRuntime(cfg, source)}
}

func NewNodeUploadTool(cfg *config.Config, source NodeFileTransferSource) *NodeUploadTool {
	return &NodeUploadTool{runtime: newNodeFileTransferToolRuntime(cfg, source)}
}

func NewNodeDownloadTool(cfg *config.Config, source NodeFileTransferSource) *NodeDownloadTool {
	return &NodeDownloadTool{runtime: newNodeFileTransferToolRuntime(cfg, source)}
}

// SetEventPublisher injects the runtime event bus used for file-transfer observations.
func (tool *NodeFileInfoTool) SetEventPublisher(eventBus runtimeevents.Bus) {
	if tool != nil && tool.runtime != nil {
		tool.runtime.runtimeEvents = eventBus
	}
}

// SetEventPublisher injects the runtime event bus used for file-transfer observations.
func (tool *NodeUploadTool) SetEventPublisher(eventBus runtimeevents.Bus) {
	if tool != nil && tool.runtime != nil {
		tool.runtime.runtimeEvents = eventBus
	}
}

// SetEventPublisher injects the runtime event bus used for file-transfer observations.
func (tool *NodeDownloadTool) SetEventPublisher(eventBus runtimeevents.Bus) {
	if tool != nil && tool.runtime != nil {
		tool.runtime.runtimeEvents = eventBus
	}
}

func newNodeFileTransferToolRuntime(
	cfg *config.Config,
	source NodeFileTransferSource,
) *nodeFileTransferToolRuntime {
	return &nodeFileTransferToolRuntime{
		access:          newNodeTargetAccess(cfg, source),
		source:          source,
		permittedAgents: configuredNodeFileAgents(cfg),
	}
}

func configuredNodeFileAgents(cfg *config.Config) map[string]struct{} {
	permitted := make(map[string]struct{})
	if cfg == nil {
		return permitted
	}
	defaultAgentID := "main"
	if len(cfg.Agents.List) > 0 {
		defaultAgentID = routing.NormalizeAgentID(cfg.Agents.List[0].ID)
		for _, agentCfg := range cfg.Agents.List {
			if routing.NormalizeAgentID(agentCfg.ID) == "main" {
				defaultAgentID = "main"
				break
			}
		}
		if defaultAgentID != "main" {
			for _, agentCfg := range cfg.Agents.List {
				if !agentCfg.Default {
					continue
				}
				defaultAgentID = routing.NormalizeAgentID(agentCfg.ID)
				break
			}
		}
	}
	if targetPolicyHasFileGrant(cfg.Agents.Defaults.TargetPolicy, cfg.Execution.Targets) {
		permitted[defaultAgentID] = struct{}{}
	}
	for _, agentCfg := range cfg.Agents.List {
		if agentCfg.TargetPolicy != nil &&
			targetPolicyHasFileGrant(agentCfg.TargetPolicy, cfg.Execution.Targets) {
			permitted[routing.NormalizeAgentID(agentCfg.ID)] = struct{}{}
		}
	}
	return permitted
}

func targetPolicyHasFileGrant(
	policy *config.TargetPolicy,
	targets map[string]config.ExecutionTarget,
) bool {
	if policy == nil {
		return false
	}
	for _, target := range policy.AllowedTargets {
		if strings.TrimSpace(targets[target].FileProfile) != "" {
			return true
		}
	}
	return false
}

func (*NodeFileInfoTool) Name() string { return "nodes_file_info" }

func (*NodeFileInfoTool) Description() string {
	return "Inspect bounded metadata for one regular file on an authorized node target. " +
		"This tool never lists directories or follows symlinks."
}

func (*NodeFileInfoTool) Parameters() map[string]any {
	return nodeFileInfoParameters()
}

func (*NodeUploadTool) Name() string { return "nodes_upload" }

func (*NodeUploadTool) Description() string {
	return "Upload one retained gateway artifact to one regular-file destination on an authorized node target."
}

func (*NodeUploadTool) Parameters() map[string]any {
	return nodeUploadParameters()
}

func (*NodeDownloadTool) Name() string { return "nodes_download" }

func (*NodeDownloadTool) Description() string {
	return "Download one regular file from an authorized node target into the bounded gateway spool, " +
		"optionally delivering it once to the originating routed conversation."
}

func (*NodeDownloadTool) Parameters() map[string]any {
	return nodeDownloadParameters()
}

func nodeFileInfoParameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"target": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 64,
			},
			"path": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 4096,
			},
			"discovery_revision": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 128,
			},
		},
		"required":             []string{"target", "path", "discovery_revision"},
		"additionalProperties": false,
	}
}

func nodeUploadParameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"target": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 64,
			},
			"artifact_ref": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 256,
			},
			"destination": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 4096,
			},
			"publication": map[string]any{
				"type": "string",
				"enum": []string{"create", "replace"},
			},
			"discovery_revision": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 128,
			},
		},
		"required": []string{
			"target",
			"artifact_ref",
			"destination",
			"publication",
			"discovery_revision",
		},
		"additionalProperties": false,
	}
}

func nodeDownloadParameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"target": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 64,
			},
			"source": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 4096,
			},
			"deliver": map[string]any{"type": "boolean"},
			"discovery_revision": map[string]any{
				"type": "string", "minLength": 1, "maxLength": 128,
			},
		},
		"required":             []string{"target", "source", "deliver", "discovery_revision"},
		"additionalProperties": false,
	}
}

func (tool *NodeFileInfoTool) ApprovalArguments(
	ctx context.Context,
	args map[string]any,
) (map[string]any, error) {
	prepared, err := tool.runtime.prepare(ctx, "file.info.v1", args, nil)
	if err != nil {
		return nil, safeNodeFileApprovalError(err)
	}
	return nodeFileApprovalArguments(prepared), nil
}

func (tool *NodeUploadTool) ApprovalArguments(
	ctx context.Context,
	args map[string]any,
) (map[string]any, error) {
	prepared, err := tool.runtime.prepare(ctx, "file.upload.v1", args, tool.mediaStore)
	if err != nil {
		return nil, safeNodeFileApprovalError(err)
	}
	return nodeFileApprovalArguments(prepared), nil
}

func (tool *NodeDownloadTool) ApprovalArguments(
	ctx context.Context,
	args map[string]any,
) (map[string]any, error) {
	prepared, err := tool.runtime.prepare(ctx, "file.download.v1", args, tool.mediaStore)
	if err != nil {
		return nil, safeNodeFileApprovalError(err)
	}
	return nodeFileApprovalArguments(prepared), nil
}

func safeNodeFileApprovalError(err error) error {
	code := "FILE_TRANSFER_DENIED"
	if errors.Is(err, errDiscoveryStale) ||
		errors.Is(err, nodes.ErrGatewayInvocationConflict) {
		code = "DISCOVERY_STALE"
	}
	return &nodeFileSafeDenialError{code: code, cause: err}
}

func nodeFileApprovalArguments(prepared preparedNodeFileTransfer) map[string]any {
	var input nodeFileTransferPlanInput
	_ = json.Unmarshal(prepared.record.Plan.Input, &input)
	result := map[string]any{
		"target":        prepared.record.Target,
		"transfer_id":   prepared.record.Plan.InvocationID,
		"operation":     prepared.record.Plan.Command,
		"profile":       prepared.profile.Alias,
		"profile_scope": nodeFileProfileBlastRadius(prepared.profile),
		"expires_at":    prepared.record.Plan.ExpiresAt,
		"plan_hash":     prepared.record.ExpectedPlanHash,
	}
	switch prepared.record.Plan.Command {
	case "file.info.v1":
		result["path"] = input.Path
	case "file.upload.v1":
		result["destination"] = input.Destination
		result["publication"] = input.Publication
		result["size"] = input.Size
		result["sha256"] = input.SHA256
		result["artifact_ref"] = input.ArtifactRef
	case "file.download.v1":
		result["source"] = input.Source
		result["size"] = input.Size
		result["sha256"] = input.SHA256
		result["deliver"] = input.Deliver != nil && *input.Deliver
	}
	return result
}

// NodeFileApprovalAction renders the exact operator-visible action from the
// retained file-transfer plan. The boolean is false for non-file tools.
func NodeFileApprovalAction(toolName string, arguments map[string]any) (string, bool, error) {
	if !isNodeFileToolName(toolName) {
		return "", false, nil
	}
	operation, ok := arguments["operation"].(string)
	if !ok || operation == "" {
		return "", true, errors.New("file approval operation is unavailable")
	}
	target, targetOK := arguments["target"].(string)
	profile, profileOK := arguments["profile"].(string)
	blastRadius, blastRadiusOK := arguments["profile_scope"].(string)
	if !targetOK || target == "" || !profileOK || profile == "" ||
		!blastRadiusOK || blastRadius == "" {
		return "", true, errors.New("file approval authority is unavailable")
	}
	switch toolName {
	case "nodes_file_info":
		path, pathOK := arguments["path"].(string)
		if operation != "file.info.v1" || !pathOK || path == "" {
			return "", true, errors.New("file metadata approval is incomplete")
		}
		return fmt.Sprintf(
			"Inspect regular-file metadata at %s on target %s using profile %s (%s)",
			path,
			target,
			profile,
			blastRadius,
		), true, nil
	case "nodes_upload":
		destination, destinationOK := arguments["destination"].(string)
		publication, publicationOK := arguments["publication"].(string)
		size, sizeOK := exactNodeFileApprovalSize(arguments["size"])
		digest, digestOK := arguments["sha256"].(string)
		if operation != "file.upload.v1" || !destinationOK || destination == "" ||
			!publicationOK || (publication != "create" && publication != "replace") ||
			!sizeOK || !digestOK || !validNodeFileApprovalDigest(digest) {
			return "", true, errors.New("file upload approval is incomplete")
		}
		consequence := "create only; fail if the destination exists"
		if publication == "replace" {
			consequence = "atomically replace the existing regular file"
		}
		return fmt.Sprintf(
			"Upload %d bytes with SHA-256 %s to %s on target %s using profile %s (%s); %s",
			size,
			digest,
			destination,
			target,
			profile,
			blastRadius,
			consequence,
		), true, nil
	case "nodes_download":
		source, sourceOK := arguments["source"].(string)
		size, sizeOK := exactNodeFileApprovalSize(arguments["size"])
		digest, digestOK := arguments["sha256"].(string)
		deliver, deliverOK := arguments["deliver"].(bool)
		if operation != "file.download.v1" || !sourceOK || source == "" ||
			!sizeOK || !digestOK || !validNodeFileApprovalDigest(digest) || !deliverOK {
			return "", true, errors.New("file download approval is incomplete")
		}
		delivery := "retain the bounded gateway artifact without chat delivery"
		if deliver {
			delivery = "deliver once to the originating authorized conversation"
		}
		return fmt.Sprintf(
			"Download %d bytes with SHA-256 %s from %s on target %s using profile %s (%s); %s",
			size,
			digest,
			source,
			target,
			profile,
			blastRadius,
			delivery,
		), true, nil
	default:
		return "", false, nil
	}
}

func isNodeFileToolName(toolName string) bool {
	switch toolName {
	case "nodes_file_info", "nodes_upload", "nodes_download":
		return true
	default:
		return false
	}
}

// ToolLogArguments returns bounded log fields without retaining file paths,
// artifact references, transfer identities, or discovery authority.
func ToolLogArguments(toolName string, arguments map[string]any) map[string]any {
	if isNodeFileToolName(toolName) || toolName == "nodes_status" || toolName == "nodes_cancel" {
		return map[string]any{
			"redacted":       true,
			"argument_count": len(arguments),
		}
	}
	return arguments
}

func exactNodeFileApprovalSize(value any) (uint64, bool) {
	switch size := value.(type) {
	case float64:
		if size < 0 || size != float64(uint64(size)) {
			return 0, false
		}
		return uint64(size), true
	case uint64:
		return size, true
	case int:
		if size < 0 {
			return 0, false
		}
		return uint64(size), true
	default:
		return 0, false
	}
}

func validNodeFileApprovalDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func nodeFileProfileBlastRadius(profile nodes.FileProfileDescriptor) string {
	if nodeFileContainsString(profile.ReadableRoots, "/") ||
		nodeFileContainsString(profile.WritableRoots, "/") {
		return "filesystem_root"
	}
	return "configured_regular_file_roots"
}

func (tool *NodeFileInfoTool) Execute(
	ctx context.Context,
	args map[string]any,
) *ToolResult {
	prepared, err := tool.runtime.prepare(ctx, "file.info.v1", args, nil)
	if err != nil {
		return nodeFileDenied(err)
	}
	if approvalRequired(prepared.profile.Approval.Metadata) &&
		!ToolApprovalContinuation(ctx) {
		return nodeFileApprovalRequired()
	}
	return tool.runtime.execute(ctx, prepared, nil)
}

func (tool *NodeUploadTool) Execute(
	ctx context.Context,
	args map[string]any,
) *ToolResult {
	prepared, err := tool.runtime.prepare(ctx, "file.upload.v1", args, tool.mediaStore)
	if err != nil {
		return nodeFileDenied(err)
	}
	if approvalRequired(prepared.profile.Approval.Write) &&
		!ToolApprovalContinuation(ctx) {
		return nodeFileApprovalRequired()
	}
	return tool.runtime.execute(ctx, prepared, tool.mediaStore)
}

func (tool *NodeDownloadTool) Execute(
	ctx context.Context,
	args map[string]any,
) *ToolResult {
	prepared, err := tool.runtime.prepare(ctx, "file.download.v1", args, tool.mediaStore)
	if err != nil {
		return nodeFileDenied(err)
	}
	if approvalRequired(prepared.profile.Approval.Read) &&
		!ToolApprovalContinuation(ctx) {
		return nodeFileApprovalRequired()
	}
	return tool.runtime.execute(ctx, prepared, tool.mediaStore)
}

func approvalRequired(value string) bool {
	return value == "required"
}

func nodeFileApprovalRequired() *ToolResult {
	return nodeFileErrorResult(map[string]any{
		"state": "denied",
		"code":  "APPROVAL_REQUIRED",
	})
}

func nodeFileDenied(err error) *ToolResult {
	code := "FILE_TRANSFER_DENIED"
	if errors.Is(err, errDiscoveryStale) ||
		errors.Is(err, nodes.ErrGatewayInvocationConflict) {
		code = "DISCOVERY_STALE"
	}
	return nodeFileErrorResult(map[string]any{
		"state": "denied",
		"code":  code,
	})
}

func nodeFileErrorResult(value any) *ToolResult {
	result := nodeJSONResult(value)
	result.IsError = true
	return result
}

func (runtime *nodeFileTransferToolRuntime) prepare(
	ctx context.Context,
	command string,
	args map[string]any,
	store media.MediaStore,
) (preparedNodeFileTransfer, error) {
	if runtime == nil || runtime.source == nil || runtime.access == nil {
		return preparedNodeFileTransfer{}, errors.New("node file transfer runtime is unavailable")
	}
	principal, executionCallID, err := nodeInvocationIdentity(ctx)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	storedToolCallID := stableNodeInvocationID("file_call", executionCallID)
	existing, found, err := runtime.source.LookupInvocationByToolCall(
		principal,
		storedToolCallID,
	)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	resolved, descriptor, profile, revision, err := runtime.resolveAuthority(
		ctx,
		command,
		args,
	)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	owner, err := nodeFileArtifactOwner(ctx, principal, storedToolCallID)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	mediaOwner, err := nodeFileMediaOwner(ctx)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	if found {
		if retainedErr := validateRetainedFileTransfer(
			existing,
			resolved,
			descriptor,
			profile,
			revision,
			args,
			owner,
			mediaOwner,
		); retainedErr != nil {
			return preparedNodeFileTransfer{}, retainedErr
		}
		return preparedNodeFileTransfer{
			record: existing, profile: profile, owner: owner,
		}, nil
	}
	if ToolApprovalContinuation(ctx) {
		return preparedNodeFileTransfer{}, errDiscoveryStale
	}

	transferID := stableNodeInvocationID(
		"file",
		principal.AgentID,
		principal.SessionID,
		principal.ActorID,
		executionCallID,
	)
	now := time.Now()
	expiresAt := now.Add(nodeFileTransferTTL).Unix()
	input, err := runtime.preparePlanInput(
		ctx,
		command,
		args,
		resolved,
		profile,
		revision,
		owner,
		transferID,
		expiresAt,
		store,
		mediaOwner,
	)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	request := nodes.InvocationRequest{
		InvocationID:     transferID,
		IdempotencyKey:   stableNodeInvocationID("file_idem", transferID),
		NodeID:           resolved.snapshot.ID,
		CatalogHash:      resolved.snapshot.CatalogHash,
		Command:          command,
		Input:            inputJSON,
		AgentID:          principal.AgentID,
		SessionID:        principal.SessionID,
		ActorID:          principal.ActorID,
		TimeoutSeconds:   int(nodeFileTransferTTL / time.Second),
		OutputLimitBytes: nodes.MaxInvocationOutput,
	}
	plan, err := nodes.PrepareExecutionPlan(
		request,
		descriptor,
		resolved.snapshot.Executor,
		profile.Revision,
		now,
		nodeFileTransferTTL,
	)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	record, created, err := runtime.source.PrepareInvocation(
		resolved.binding.Node,
		resolved.name,
		storedToolCallID,
		principal,
		plan,
		descriptor,
		true,
		func(current NodeDiscoveryRecord) error {
			return runtime.validateCurrentAuthority(
				ToolAgentID(ctx),
				resolved.name,
				command,
				revision,
				profile,
				current,
			)
		},
	)
	if err != nil {
		return preparedNodeFileTransfer{}, err
	}
	if created {
		runtime.publishFileTransferEvent(
			ctx,
			NodeInvocationObservationPrepared,
			record,
			string(nodes.GatewayInvocationPrepared),
			"",
		)
	}
	return preparedNodeFileTransfer{
		record: record, profile: profile, owner: owner,
	}, nil
}

func (runtime *nodeFileTransferToolRuntime) resolveAuthority(
	ctx context.Context,
	command string,
	args map[string]any,
) (
	resolvedNodeTarget,
	nodes.CommandDescriptor,
	nodes.FileProfileDescriptor,
	string,
	error,
) {
	target, targetErr := exactNodeFileArgument(args, "target")
	if targetErr != nil {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{},
			nodes.FileProfileDescriptor{}, "", errDiscoveryStale
	}
	resolved, err := (&nodeInvocationToolRuntime{access: runtime.access}).resolveTarget(
		ToolAgentID(ctx),
		target,
		true,
	)
	if err != nil || resolved.registration == nil || resolved.requiresReapproval {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{},
			nodes.FileProfileDescriptor{}, "", errDiscoveryStale
	}
	descriptor, found := nodeCatalogDescriptor(resolved.snapshot.Catalog, command)
	if !found {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{},
			nodes.FileProfileDescriptor{}, "", errDiscoveryStale
	}
	descriptor, found = projectFileDescriptorForTarget(
		descriptor,
		resolved.binding.FileProfile,
	)
	if !found || len(descriptor.FileProfiles) != 1 ||
		descriptor.ModelContract == nil ||
		descriptor.ModelContract.Availability != nodes.ModelAvailable {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{},
			nodes.FileProfileDescriptor{}, "", errDiscoveryStale
	}
	if _, approvalErr := resolved.registration.ApprovedCommand(command); approvalErr != nil {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{},
			nodes.FileProfileDescriptor{}, "", errDiscoveryStale
	}
	revision, err := runtime.access.discoveryRevision(
		ToolAgentID(ctx),
		resolved.name,
		command,
		resolved.snapshot,
		*resolved.registration,
		descriptor,
		resolved.available,
	)
	requestedRevision, revisionErr := exactNodeFileArgument(args, "discovery_revision")
	if err != nil || revisionErr != nil || revision != requestedRevision {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{},
			nodes.FileProfileDescriptor{}, "", errDiscoveryStale
	}
	return resolved, descriptor, descriptor.FileProfiles[0], revision, nil
}

func (runtime *nodeFileTransferToolRuntime) validateCurrentAuthority(
	agentID string,
	target string,
	command string,
	revision string,
	profile nodes.FileProfileDescriptor,
	current NodeDiscoveryRecord,
) error {
	if current.Registration == nil || !current.Connected {
		return errDiscoveryStale
	}
	descriptor, found := nodeCatalogDescriptor(current.Snapshot.Catalog, command)
	if !found {
		return errDiscoveryStale
	}
	binding := runtime.access.targets[target]
	descriptor, found = projectFileDescriptorForTarget(descriptor, binding.FileProfile)
	if !found || len(descriptor.FileProfiles) != 1 ||
		!reflect.DeepEqual(descriptor.FileProfiles[0], profile) {
		return errDiscoveryStale
	}
	currentRevision, err := runtime.access.discoveryRevision(
		agentID,
		target,
		command,
		current.Snapshot,
		*current.Registration,
		descriptor,
		current.Connected,
	)
	if err != nil || currentRevision != revision {
		return errDiscoveryStale
	}
	_, err = current.Registration.ApprovedCommand(command)
	return err
}

func (runtime *nodeFileTransferToolRuntime) preparePlanInput(
	ctx context.Context,
	command string,
	args map[string]any,
	resolved resolvedNodeTarget,
	profile nodes.FileProfileDescriptor,
	revision string,
	owner nodes.TransferArtifactOwner,
	transferID string,
	expiresAt int64,
	store media.MediaStore,
	mediaOwner media.MediaOwner,
) (nodeFileTransferPlanInput, error) {
	input := nodeFileTransferPlanInput{
		RouteID:           owner.RouteID,
		DiscoveryRevision: revision,
	}
	var err error
	switch command {
	case "file.info.v1":
		input.Path, err = exactNodeFileArgument(args, "path")
		if err != nil {
			return nodeFileTransferPlanInput{}, errors.New("path is required")
		}
	case "file.upload.v1":
		sourceRef, sourceErr := exactNodeFileArgument(args, "artifact_ref")
		input.Destination, err = exactNodeFileArgument(args, "destination")
		var publicationErr error
		input.Publication, publicationErr = exactNodeFileArgument(args, "publication")
		if sourceErr != nil || err != nil || publicationErr != nil ||
			(input.Publication != "create" && input.Publication != "replace") {
			return nodeFileTransferPlanInput{}, errors.New("invalid upload destination")
		}
		input.SourceArtifactID = nodeFileSourceArtifactID(mediaOwner, sourceRef)
		artifact, snapshotErr := runtime.source.SnapshotUploadArtifact(
			ctx,
			owner,
			transferID,
			resolved.name,
			profile.Revision,
			expiresAt,
			profile.MaxFileBytes,
			sourceRef,
			store,
			mediaOwner,
		)
		if snapshotErr != nil {
			return nodeFileTransferPlanInput{}, snapshotErr
		}
		input.ArtifactRef = artifact.Ref
		input.Size = float64(artifact.Spec.DeclaredSize)
		input.SHA256 = artifact.Spec.SHA256
		input.Filename = artifact.Spec.Filename
		input.ContentType = artifact.Spec.ContentType
	case "file.download.v1":
		input.Source, err = exactNodeFileArgument(args, "source")
		deliver, _ := args["deliver"].(bool)
		input.Deliver = &deliver
		if err != nil {
			return nodeFileTransferPlanInput{}, errors.New("source is required")
		}
		infoID := stableNodeInvocationID("file_info", transferID)
		info, err := runtime.source.InspectFile(
			ctx,
			resolved.snapshot.ID,
			NodeFileTransferBinding{
				TransferID:     infoID,
				Direction:      protocol.TransferDownload,
				ProfileAlias:   profile.Alias,
				PolicyRevision: profile.Revision,
				Path:           input.Source,
				TotalSize:      0,
				SHA256:         sha256.Sum256(nil),
				ExpiresAt:      expiresAt,
			},
		)
		if err != nil || info.State != "committed" ||
			info.Size > uint64(profile.MaxFileBytes) {
			return nodeFileTransferPlanInput{}, errors.New("download source is unavailable")
		}
		digest, err := decodeNodeFileDigest(info.SHA256)
		if err != nil {
			return nodeFileTransferPlanInput{}, err
		}
		input.Size = float64(info.Size)
		input.SHA256 = hex.EncodeToString(digest[:])
		input.Filename = safeNodeDownloadFilename(input.Source)
		input.ContentType = ""
		if *input.Deliver {
			input.Channel = ToolChannel(ctx)
			input.ChatID = ToolChatID(ctx)
			input.TopicID = ToolTopicID(ctx)
			if input.Channel == "" || input.ChatID == "" {
				return nodeFileTransferPlanInput{}, errors.New("delivery route is unavailable")
			}
		}
	default:
		return nodeFileTransferPlanInput{}, errors.New("unsupported file operation")
	}
	return input, nil
}

func (runtime *nodeFileTransferToolRuntime) execute(
	ctx context.Context,
	prepared preparedNodeFileTransfer,
	store media.MediaStore,
) *ToolResult {
	record := prepared.record
	if record.State == nodes.GatewayInvocationDispatched {
		result, err := runtime.source.QueryFileTransfer(
			ctx,
			nodeFilePrincipal(record),
			record,
		)
		if err != nil {
			runtime.publishFileTransferEvent(
				ctx,
				NodeInvocationObservationUncertain,
				record,
				"unknown",
				"STATUS_UNAVAILABLE",
			)
			return nodeFileUnknown(record.Plan.InvocationID)
		}
		runtime.publishFileTransferEvent(
			ctx,
			NodeInvocationObservationStatus,
			record,
			result.State,
			result.Code,
		)
		return runtime.result(ctx, prepared, result, store)
	}
	owner := nodes.GatewayInvocationOwner{
		Target:      record.Target,
		AgentID:     record.Plan.AgentID,
		SessionID:   record.Plan.SessionID,
		ActorID:     record.Plan.ActorID,
		ToolCallID:  record.ToolCallID,
		WorkspaceID: record.WorkspaceID,
		ExecutionID: record.ExecutionID,
	}
	result, dispatched, err := runtime.source.DispatchFileTransfer(ctx, owner, record)
	if err != nil {
		if dispatched {
			runtime.publishFileTransferEvent(
				ctx,
				NodeInvocationObservationDispatched,
				record,
				string(nodes.GatewayInvocationDispatched),
				"",
			)
			runtime.publishFileTransferEvent(
				ctx,
				NodeInvocationObservationUncertain,
				record,
				"unknown",
				"TRANSFER_OUTCOME_UNKNOWN",
			)
			if ctx.Err() != nil {
				runtime.cancelAfterContext(record)
			}
			return nodeFileUnknown(record.Plan.InvocationID)
		}
		return nodeFileDenied(err)
	}
	runtime.publishFileTransferEvent(
		ctx,
		NodeInvocationObservationDispatched,
		record,
		string(nodes.GatewayInvocationDispatched),
		"",
	)
	runtime.publishFileTransferEvent(
		ctx,
		NodeInvocationObservationCompleted,
		record,
		result.State,
		result.Code,
	)
	return runtime.result(ctx, prepared, result, store)
}

func (runtime *nodeFileTransferToolRuntime) publishFileTransferEvent(
	ctx context.Context,
	observation string,
	record nodes.GatewayInvocationRecord,
	state string,
	errorCode string,
) {
	if runtime == nil {
		return
	}
	publishNodeInvocationEvent(
		runtime.runtimeEvents,
		ctx,
		observation,
		nodeFileToolName(record.Plan.Command),
		record,
		state,
		errorCode,
	)
}

func nodeFileToolName(command string) string {
	switch command {
	case "file.info.v1":
		return "nodes_file_info"
	case "file.upload.v1":
		return "nodes_upload"
	case "file.download.v1":
		return "nodes_download"
	default:
		return "nodes_file_transfer"
	}
}

func (runtime *nodeFileTransferToolRuntime) cancelAfterContext(
	record nodes.GatewayInvocationRecord,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, _ = runtime.source.CancelFileTransfer(
		ctx,
		nodeFilePrincipal(record),
		record,
	)
}

func (runtime *nodeFileTransferToolRuntime) result(
	ctx context.Context,
	prepared preparedNodeFileTransfer,
	result NodeFileTransferResult,
	store media.MediaStore,
) *ToolResult {
	result.TransferID = prepared.record.Plan.InvocationID
	result.PolicyRevision = prepared.record.Plan.PolicyRevision
	var retainedInput nodeFileTransferPlanInput
	if json.Unmarshal(prepared.record.Plan.Input, &retainedInput) == nil {
		switch prepared.record.Plan.Command {
		case "file.info.v1":
			result.Path = retainedInput.Path
		case "file.upload.v1":
			result.Path = retainedInput.Destination
		case "file.download.v1":
			result.Path = retainedInput.Source
		}
	}
	if prepared.record.Plan.Command != "file.download.v1" {
		return nodeJSONResult(result)
	}
	input := retainedInput
	if input.Source == "" {
		return nodeFileUnknown(prepared.record.Plan.InvocationID)
	}
	if input.Deliver == nil || !*input.Deliver || result.ArtifactRef == "" {
		return nodeJSONResult(result)
	}
	mediaOwner, err := nodeFileMediaOwner(ctx)
	if err != nil {
		result.DeliveryState = "unknown"
		return nodeJSONResult(result)
	}
	mediaRef, claimed, err := runtime.source.HandoffDownloadedArtifact(
		ctx,
		prepared.owner,
		result.ArtifactRef,
		store,
		mediaOwner,
	)
	if err != nil {
		result.DeliveryState = "unknown"
		return nodeJSONResult(result)
	}
	if !claimed {
		result.DeliveryState = "already_claimed"
		return nodeJSONResult(result).WithResponseHandled()
	}
	result.DeliveryState = "claimed"
	data, _ := json.Marshal(result)
	return MediaResult(string(data), []string{mediaRef}).WithResponseHandled()
}

func nodeFileUnknown(transferID string) *ToolResult {
	return nodeFileErrorResult(NodeFileTransferResult{
		TransferID: transferID,
		State:      "unknown",
		Code:       "TRANSFER_OUTCOME_UNKNOWN",
		RecoveryAction: "Repeat the same status-bearing tool call; " +
			"do not start another transfer automatically.",
	})
}

func validateRetainedFileTransfer(
	record nodes.GatewayInvocationRecord,
	resolved resolvedNodeTarget,
	descriptor nodes.CommandDescriptor,
	profile nodes.FileProfileDescriptor,
	revision string,
	args map[string]any,
	owner nodes.TransferArtifactOwner,
	mediaOwner media.MediaOwner,
) error {
	if record.Target != resolved.name || record.Plan.NodeID != resolved.snapshot.ID {
		return fmt.Errorf("%w: retained target authority changed", errDiscoveryStale)
	}
	if record.Plan.CatalogHash != resolved.snapshot.CatalogHash ||
		record.Descriptor.Name != descriptor.Name ||
		record.Plan.DescriptorHash != descriptorHashOrEmpty(descriptor) {
		return fmt.Errorf("%w: retained catalog authority changed", errDiscoveryStale)
	}
	if record.Plan.PolicyRevision != profile.Revision {
		return fmt.Errorf("%w: retained file profile changed", errDiscoveryStale)
	}
	if record.Plan.ExpiresAt <= time.Now().Unix() {
		return fmt.Errorf("%w: retained file transfer expired", errDiscoveryStale)
	}
	var input nodeFileTransferPlanInput
	if err := json.Unmarshal(record.Plan.Input, &input); err != nil ||
		input.RouteID != owner.RouteID ||
		input.DiscoveryRevision != revision {
		return fmt.Errorf("%w: retained route authority changed", errDiscoveryStale)
	}
	switch record.Plan.Command {
	case "file.info.v1":
		path, err := exactNodeFileArgument(args, "path")
		if err != nil || input.Path != path {
			return fmt.Errorf("%w: retained metadata input changed", errDiscoveryStale)
		}
	case "file.upload.v1":
		destination, destinationErr := exactNodeFileArgument(args, "destination")
		publication, publicationErr := exactNodeFileArgument(args, "publication")
		sourceRef, sourceErr := exactNodeFileArgument(args, "artifact_ref")
		if destinationErr != nil || publicationErr != nil || sourceErr != nil ||
			input.Destination != destination || input.Publication != publication ||
			input.SourceArtifactID != nodeFileSourceArtifactID(
				mediaOwner,
				sourceRef,
			) {
			return fmt.Errorf("%w: retained upload input changed", errDiscoveryStale)
		}
	case "file.download.v1":
		deliver, _ := args["deliver"].(bool)
		source, err := exactNodeFileArgument(args, "source")
		if err != nil || input.Source != source ||
			input.Deliver == nil || *input.Deliver != deliver {
			return fmt.Errorf("%w: retained download input changed", errDiscoveryStale)
		}
	default:
		return fmt.Errorf("%w: retained file command changed", errDiscoveryStale)
	}
	return record.Plan.ValidateAgainstHash(record.ExpectedPlanHash)
}

func descriptorHashOrEmpty(descriptor nodes.CommandDescriptor) string {
	hash, _ := descriptor.Hash()
	return hash
}

func nodeFileArtifactOwner(
	ctx context.Context,
	principal nodes.GatewayInvocationPrincipal,
	toolCallID string,
) (nodes.TransferArtifactOwner, error) {
	routeSession := strings.TrimSpace(ToolRouteSessionKey(ctx))
	if routeSession == "" {
		routeSession = strings.TrimSpace(ToolSessionKey(ctx))
	}
	routeID := stableNodeInvocationID(
		"file_route",
		ToolChannel(ctx),
		ToolChatID(ctx),
		ToolTopicID(ctx),
		routeSession,
	)
	owner := nodes.TransferArtifactOwner{
		WorkspaceID: principal.WorkspaceID,
		AgentID:     principal.AgentID,
		ActorID:     principal.ActorID,
		RouteID:     routeID,
		SessionID:   principal.SessionID,
		ToolCallID:  toolCallID,
	}
	return owner, owner.Validate()
}

func nodeFileMediaOwner(ctx context.Context) (media.MediaOwner, error) {
	actorID := strings.TrimSpace(ToolActorID(ctx))
	if actorID == "" {
		actorID = strings.TrimSpace(ToolSenderID(ctx))
	}
	if actorID == "" {
		actorID = strings.TrimSpace(ToolAgentID(ctx))
	}
	routeSession := strings.TrimSpace(ToolRouteSessionKey(ctx))
	if routeSession == "" {
		routeSession = strings.TrimSpace(ToolSessionKey(ctx))
	}
	return media.NewMediaOwner(
		ToolWorkspace(ctx),
		ToolAgentID(ctx),
		actorID,
		routeSession,
		ToolChannel(ctx),
		ToolChatID(ctx),
		ToolTopicID(ctx),
	)
}

func nodeFileSourceArtifactID(owner media.MediaOwner, ref string) string {
	return stableNodeInvocationID(
		"file_source",
		owner.WorkspaceID,
		owner.AgentID,
		owner.ActorID,
		owner.RouteID,
		owner.SessionID,
		ref,
	)
}

func exactNodeFileArgument(args map[string]any, name string) (string, error) {
	value, ok := args[name].(string)
	if !ok || value == "" || value != strings.TrimSpace(value) {
		return "", errors.New("invalid node file argument")
	}
	return value, nil
}

func nodeFilePrincipal(record nodes.GatewayInvocationRecord) nodes.GatewayInvocationPrincipal {
	return nodes.GatewayInvocationPrincipal{
		AgentID:     record.Plan.AgentID,
		SessionID:   record.Plan.SessionID,
		ActorID:     record.Plan.ActorID,
		WorkspaceID: record.WorkspaceID,
		ExecutionID: record.ExecutionID,
	}
}

func nodeFileContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func decodeNodeFileDigest(value string) ([sha256.Size]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, errors.New("invalid file digest")
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return digest, nil
}

func safeNodeDownloadFilename(path string) string {
	name := filepath.Base(path)
	if name == "." || name == string(filepath.Separator) ||
		name == "" || len(name) > 255 ||
		strings.ContainsAny(name, `/\`) {
		return "download.bin"
	}
	return name
}

func (tool *NodeUploadTool) SetMediaStore(store media.MediaStore) {
	tool.mediaStore = store
}

func (tool *NodeDownloadTool) SetMediaStore(store media.MediaStore) {
	tool.mediaStore = store
}

func (tool *NodeFileInfoTool) ToolEnabledForAgent(agentID string) bool {
	return tool.runtime.enabledForAgent(agentID)
}

func (tool *NodeUploadTool) ToolEnabledForAgent(agentID string) bool {
	return tool.runtime.enabledForAgent(agentID)
}

func (tool *NodeDownloadTool) ToolEnabledForAgent(agentID string) bool {
	return tool.runtime.enabledForAgent(agentID)
}

func (runtime *nodeFileTransferToolRuntime) enabledForAgent(agentID string) bool {
	if runtime == nil || runtime.access == nil {
		return false
	}
	if _, permitted := runtime.permittedAgents[routing.NormalizeAgentID(agentID)]; !permitted {
		return false
	}
	targets, _ := runtime.access.visibleTargets(agentID)
	for _, target := range targets {
		if runtime.access.targets[target].FileProfile != "" {
			return true
		}
	}
	return false
}

func (*NodeFileInfoTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (*NodeUploadTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsMutating
}

func (*NodeDownloadTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsMutating
}

func (*NodeFileInfoTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyReadOnly
}

func (*NodeUploadTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (*NodeDownloadTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}
