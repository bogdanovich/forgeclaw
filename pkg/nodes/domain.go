package nodes

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

const (
	ProtocolV1            = 1
	MaxIDLength           = 128
	MaxAliasLength        = 64
	MaxCommandNameLen     = 128
	MaxSchemaBytes        = 64 * 1024
	MaxCatalogCommands    = 128
	MaxCatalogBytes       = 512 * 1024
	MaxModelContractBytes = 32 * 1024
	MaxModelGuidanceBytes = 2 * 1024
	MaxModelExamples      = 4
	MaxModelExampleBytes  = 8 * 1024
)

var (
	ErrInvalidNode       = errors.New("invalid node")
	ErrInvalidCapability = errors.New("invalid node capability")

	idPattern         = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	aliasPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	capabilityPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	commandPattern    = regexp.MustCompile(`^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)*\.v[1-9][0-9]*$`)
)

type ID string

func (id ID) Validate() error {
	value := string(id)
	if len(value) == 0 || len(value) > MaxIDLength || !idPattern.MatchString(value) {
		return fmt.Errorf("%w: malformed id", ErrInvalidNode)
	}
	return nil
}

type Alias string

func (alias Alias) Validate() error {
	value := string(alias)
	if len(value) == 0 || len(value) > MaxAliasLength || !aliasPattern.MatchString(value) {
		return fmt.Errorf("%w: malformed alias", ErrInvalidNode)
	}
	return nil
}

type State string

const (
	StatePendingPairing State = "pending_pairing"
	StateConnected      State = "connected"
	StateDisconnected   State = "disconnected"
	StateRevoked        State = "revoked"
	StateIncompatible   State = "incompatible"
	StateDegraded       State = "degraded"
)

func (state State) Valid() bool {
	switch state {
	case StatePendingPairing, StateConnected, StateDisconnected, StateRevoked,
		StateIncompatible, StateDegraded:
		return true
	default:
		return false
	}
}

type Risk string

const (
	RiskRead       Risk = "read"
	RiskWrite      Risk = "write"
	RiskPrivileged Risk = "privileged"
)

func (risk Risk) Valid() bool {
	return risk == RiskRead || risk == RiskWrite || risk == RiskPrivileged
}

type ModelAvailability string

const (
	ModelAvailable          ModelAvailability = "available"
	ModelPartiallyDescribed ModelAvailability = "partially_described"
	ModelUnavailable        ModelAvailability = "unavailable"
)

func (availability ModelAvailability) Valid() bool {
	switch availability {
	case ModelAvailable, ModelPartiallyDescribed, ModelUnavailable:
		return true
	default:
		return false
	}
}

type CommandModelConstraints struct {
	ExecutableAliases []string `json:"executable_aliases,omitempty"`
	ProfileAliases    []string `json:"profile_aliases,omitempty"`
	WorkingScopes     []string `json:"working_scopes,omitempty"`
	EnvironmentNames  []string `json:"environment_names,omitempty"`
}

type CommandModelContract struct {
	Availability      ModelAvailability       `json:"availability"`
	TimeoutSecondsMax int                     `json:"timeout_seconds_max"`
	OutputBytesMax    int                     `json:"output_bytes_max"`
	ResultKind        string                  `json:"result_kind"`
	AuthorityDigest   string                  `json:"authority_digest,omitempty"`
	ApprovalMode      string                  `json:"approval_mode,omitempty"`
	Constraints       CommandModelConstraints `json:"constraints"`
	Guidance          []string                `json:"guidance"`
	Examples          []json.RawMessage       `json:"examples"`
}

func (contract CommandModelContract) Validate(inputSchema json.RawMessage) error {
	if !contract.Availability.Valid() ||
		contract.TimeoutSecondsMax <= 0 ||
		contract.TimeoutSecondsMax > MaxInvocationTimeout ||
		contract.OutputBytesMax <= 0 ||
		contract.OutputBytesMax > MaxInvocationOutput ||
		contract.ResultKind != "json" ||
		(contract.AuthorityDigest != "" && !validSHA256Digest(contract.AuthorityDigest)) ||
		(contract.ApprovalMode != "" &&
			contract.ApprovalMode != "each_command" &&
			contract.ApprovalMode != "session_start") ||
		contract.Guidance == nil ||
		contract.Examples == nil {
		return fmt.Errorf("%w: malformed model contract", ErrInvalidCapability)
	}
	if err := validateModelConstraintNames(contract.Constraints); err != nil {
		return err
	}
	guidanceBytes := 0
	for _, statement := range contract.Guidance {
		guidanceBytes += len(statement)
		if statement == "" || statement != strings.TrimSpace(statement) ||
			!utf8.ValidString(statement) || containsModelControl(statement) ||
			guidanceBytes > MaxModelGuidanceBytes {
			return fmt.Errorf("%w: malformed model guidance", ErrInvalidCapability)
		}
	}
	if len(contract.Examples) > MaxModelExamples {
		return fmt.Errorf("%w: too many model examples", ErrInvalidCapability)
	}
	for _, example := range contract.Examples {
		if len(example) == 0 || len(example) > MaxModelExampleBytes {
			return fmt.Errorf("%w: model example exceeds size limit", ErrInvalidCapability)
		}
		value, err := jsonstrict.Decode(example)
		if err != nil {
			return fmt.Errorf("%w: invalid model example", ErrInvalidCapability)
		}
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: model example must be an object", ErrInvalidCapability)
		}
		if err := validateInvocationInput(inputSchema, object); err != nil {
			return fmt.Errorf("%w: model example violates input schema", ErrInvalidCapability)
		}
	}
	data, err := json.Marshal(contract)
	if err != nil || len(data) > MaxModelContractBytes {
		return fmt.Errorf("%w: model contract exceeds size limit", ErrInvalidCapability)
	}
	return nil
}

func validateModelConstraintNames(constraints CommandModelConstraints) error {
	groups := [][]string{
		constraints.ExecutableAliases,
		constraints.ProfileAliases,
		constraints.WorkingScopes,
		constraints.EnvironmentNames,
	}
	limits := []int{64, 32, 32, 64}
	for index, values := range groups {
		if len(values) > limits[index] {
			return fmt.Errorf("%w: too many model constraint names", ErrInvalidCapability)
		}
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if len(value) == 0 || len(value) > MaxAliasLength ||
				value != strings.TrimSpace(value) || !idPattern.MatchString(value) {
				return fmt.Errorf("%w: malformed model constraint name", ErrInvalidCapability)
			}
			if _, duplicate := seen[value]; duplicate {
				return fmt.Errorf("%w: duplicate model constraint name", ErrInvalidCapability)
			}
			seen[value] = struct{}{}
		}
		if !sort.StringsAreSorted(values) {
			return fmt.Errorf("%w: model constraint names are not sorted", ErrInvalidCapability)
		}
	}
	return nil
}

func containsModelControl(value string) bool {
	return strings.IndexFunc(value, func(character rune) bool {
		return character < 0x20 || character == 0x7f
	}) >= 0
}

type CommandDescriptor struct {
	Name             string                `json:"name"`
	InputSchema      json.RawMessage       `json:"input_schema"`
	OutputSchema     json.RawMessage       `json:"output_schema"`
	Risk             Risk                  `json:"risk"`
	SupportsProgress bool                  `json:"supports_progress,omitempty"`
	SupportsCancel   bool                  `json:"supports_cancel,omitempty"`
	ModelContract    *CommandModelContract `json:"model_contract,omitempty"`
}

func (descriptor CommandDescriptor) Validate() error {
	if len(descriptor.Name) == 0 || len(descriptor.Name) > MaxCommandNameLen ||
		!commandPattern.MatchString(descriptor.Name) {
		return fmt.Errorf("%w: malformed command name", ErrInvalidCapability)
	}
	if !descriptor.Risk.Valid() {
		return fmt.Errorf("%w: unsupported risk %q", ErrInvalidCapability, descriptor.Risk)
	}
	if err := validateObjectSchema("input", descriptor.InputSchema); err != nil {
		return err
	}
	if err := validateObjectSchema("output", descriptor.OutputSchema); err != nil {
		return err
	}
	if descriptor.Name == "shell.exec.v1" &&
		(descriptor.Risk != RiskPrivileged ||
			descriptor.ModelContract == nil ||
			descriptor.ModelContract.ApprovalMode != "each_command") {
		return fmt.Errorf("%w: shell.exec requires privileged per-command approval", ErrInvalidCapability)
	}
	if descriptor.ModelContract != nil {
		if err := descriptor.ModelContract.Validate(descriptor.InputSchema); err != nil {
			return err
		}
		switch descriptor.Name {
		case "system.exec.v1":
			modelSchema, err := SystemExecModelInputSchema(*descriptor.ModelContract)
			if err != nil {
				return err
			}
			if err := descriptor.ModelContract.Validate(modelSchema); err != nil {
				return err
			}
		case "shell.exec.v1":
			modelSchema, err := ShellExecModelInputSchema(*descriptor.ModelContract)
			if err != nil {
				return err
			}
			if err := descriptor.ModelContract.Validate(modelSchema); err != nil {
				return err
			}
			for _, example := range descriptor.ModelContract.Examples {
				value, err := jsonstrict.Decode(example)
				if err != nil {
					return fmt.Errorf("%w: invalid shell.exec example", ErrInvalidCapability)
				}
				input, ok := value.(map[string]any)
				if !ok {
					return fmt.Errorf("%w: shell.exec example must be an object", ErrInvalidCapability)
				}
				if err := ValidateShellExecModelInput(input); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (descriptor CommandDescriptor) Capability() string {
	prefix, _, _ := strings.Cut(descriptor.Name, ".")
	if !capabilityPattern.MatchString(prefix) {
		return ""
	}
	return prefix
}

// Hash returns the canonical identity of one command contract.
func (descriptor CommandDescriptor) Hash() (string, error) {
	return (CapabilityCatalog{Commands: []CommandDescriptor{descriptor}}).Hash()
}

type CapabilityCatalog struct {
	Commands []CommandDescriptor `json:"commands"`
}

func (catalog CapabilityCatalog) Validate() error {
	if len(catalog.Commands) > MaxCatalogCommands {
		return fmt.Errorf("%w: catalog contains too many commands", ErrInvalidCapability)
	}
	seen := make(map[string]struct{}, len(catalog.Commands))
	totalBytes := 0
	for _, descriptor := range catalog.Commands {
		totalBytes += len(descriptor.Name) + len(descriptor.InputSchema) + len(descriptor.OutputSchema)
		if descriptor.ModelContract != nil {
			modelContract, err := json.Marshal(descriptor.ModelContract)
			if err != nil {
				return fmt.Errorf("%w: encode model contract", ErrInvalidCapability)
			}
			totalBytes += len(modelContract)
		}
		if totalBytes > MaxCatalogBytes {
			return fmt.Errorf("%w: catalog exceeds size limit", ErrInvalidCapability)
		}
		if err := descriptor.Validate(); err != nil {
			return err
		}
		if _, exists := seen[descriptor.Name]; exists {
			return fmt.Errorf("%w: duplicate command %q", ErrInvalidCapability, descriptor.Name)
		}
		seen[descriptor.Name] = struct{}{}
	}
	return nil
}

// Hash returns a stable digest regardless of descriptor or schema key order.
func (catalog CapabilityCatalog) Hash() (string, error) {
	if err := catalog.Validate(); err != nil {
		return "", err
	}
	commands := append([]CommandDescriptor(nil), catalog.Commands...)
	if commands == nil {
		commands = make([]CommandDescriptor, 0)
	}
	sort.Slice(commands, func(i, j int) bool { return commands[i].Name < commands[j].Name })
	for i := range commands {
		var err error
		commands[i].InputSchema, err = canonicalJSON(commands[i].InputSchema)
		if err != nil {
			return "", err
		}
		commands[i].OutputSchema, err = canonicalJSON(commands[i].OutputSchema)
		if err != nil {
			return "", err
		}
		if commands[i].ModelContract != nil {
			contract := cloneCommandModelContract(*commands[i].ModelContract)
			for exampleIndex := range contract.Examples {
				contract.Examples[exampleIndex], err = canonicalJSON(contract.Examples[exampleIndex])
				if err != nil {
					return "", err
				}
			}
			commands[i].ModelContract = &contract
		}
	}
	data, err := json.Marshal(CapabilityCatalog{Commands: commands})
	if err != nil {
		return "", fmt.Errorf("marshal capability catalog: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func cloneCommandModelContract(contract CommandModelContract) CommandModelContract {
	contract.Constraints.ExecutableAliases = append(
		[]string(nil),
		contract.Constraints.ExecutableAliases...,
	)
	contract.Constraints.ProfileAliases = append(
		[]string(nil),
		contract.Constraints.ProfileAliases...,
	)
	contract.Constraints.WorkingScopes = append([]string(nil), contract.Constraints.WorkingScopes...)
	contract.Constraints.EnvironmentNames = append(
		[]string(nil),
		contract.Constraints.EnvironmentNames...,
	)
	contract.Guidance = append([]string(nil), contract.Guidance...)
	contract.Examples = append([]json.RawMessage(nil), contract.Examples...)
	for index := range contract.Examples {
		contract.Examples[index] = bytes.Clone(contract.Examples[index])
	}
	if contract.Guidance == nil {
		contract.Guidance = []string{}
	}
	if contract.Examples == nil {
		contract.Examples = []json.RawMessage{}
	}
	return contract
}

type Snapshot struct {
	ID               ID                `json:"id"`
	Aliases          []Alias           `json:"aliases,omitempty"`
	DisplayName      string            `json:"display_name,omitempty"`
	State            State             `json:"state"`
	ProtocolVersion  int               `json:"protocol_version,omitempty"`
	Platform         string            `json:"platform,omitempty"`
	Architecture     string            `json:"architecture,omitempty"`
	SoftwareVersion  string            `json:"software_version,omitempty"`
	CatalogHash      string            `json:"catalog_hash,omitempty"`
	Catalog          CapabilityCatalog `json:"catalog,omitempty"`
	Executor         string            `json:"executor,omitempty"`
	PolicyRevision   string            `json:"policy_revision,omitempty"`
	LastSeenAt       int64             `json:"last_seen_at,omitempty"`
	DisconnectReason string            `json:"disconnect_reason,omitempty"`
}

func (snapshot Snapshot) Validate() error {
	if err := snapshot.ID.Validate(); err != nil {
		return err
	}
	if !snapshot.State.Valid() {
		return fmt.Errorf("%w: unsupported state %q", ErrInvalidNode, snapshot.State)
	}
	seen := make(map[Alias]struct{}, len(snapshot.Aliases))
	for _, alias := range snapshot.Aliases {
		if err := alias.Validate(); err != nil {
			return err
		}
		if _, exists := seen[alias]; exists {
			return fmt.Errorf("%w: duplicate alias %q", ErrInvalidNode, alias)
		}
		seen[alias] = struct{}{}
	}
	if snapshot.ProtocolVersion < 0 {
		return fmt.Errorf("%w: negative protocol version", ErrInvalidNode)
	}
	if err := snapshot.Catalog.Validate(); err != nil {
		return err
	}
	if err := (ExecutionProfile{
		Executor:       snapshot.Executor,
		PolicyRevision: snapshot.PolicyRevision,
	}).ValidateOptional(); err != nil {
		return err
	}
	if snapshot.CatalogHash == "" {
		return nil
	}
	if !validSHA256Digest(snapshot.CatalogHash) {
		return fmt.Errorf("%w: malformed catalog hash", ErrInvalidNode)
	}
	catalogHash, err := snapshot.Catalog.Hash()
	if err != nil {
		return err
	}
	if snapshot.CatalogHash != catalogHash {
		return fmt.Errorf("%w: catalog hash does not match catalog", ErrInvalidNode)
	}
	return nil
}

type Filter struct {
	States []State
	Alias  Alias
}

type Disconnect struct {
	Reason string
	At     int64
}

// PairingApproval is the operator-owned authority granted to a pending or
// already paired node.
// AllowedCommands must be a subset of the capability catalog presented during
// admission; an empty list grants no executable command surface.
type PairingApproval struct {
	Aliases         []Alias
	DisplayName     string
	AllowedCommands []string
	At              int64
}

// Revocation records an operator decision that prevents an identity from
// returning to pending admission on its next connection.
type Revocation struct {
	Reason string
	At     int64
}

// Registration is the durable operator view of a node identity. PublicKey is
// intentionally retained here so authentication can bind approval to the
// exact admitted device rather than to a mutable alias.
type Registration struct {
	Snapshot            Snapshot
	PublicKey           []byte
	RequestedRole       string
	RequestedAt         int64
	AllowedCommands     []string
	ApprovedCatalogHash string
	ApprovedAt          int64
	RevokedAt           int64
}

func validSHA256Digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

// Registry is the durable node-state boundary. Connection ownership remains
// in the gateway transport layer and is represented here only as snapshots.
type Registry interface {
	List(Filter) ([]Snapshot, error)
	Resolve(string) (Snapshot, bool, error)
	Upsert(Snapshot) error
	MarkDisconnected(ID, Disconnect) error
}

func validateObjectSchema(label string, raw json.RawMessage) error {
	if len(raw) == 0 || len(raw) > MaxSchemaBytes || !json.Valid(raw) {
		return fmt.Errorf("%w: invalid %s schema", ErrInvalidCapability, label)
	}
	value, err := jsonstrict.Decode(raw)
	if err != nil {
		return fmt.Errorf("%w: invalid %s schema: %v", ErrInvalidCapability, label, err)
	}
	if _, ok := value.(map[string]any); !ok {
		return fmt.Errorf("%w: %s schema must be an object", ErrInvalidCapability, label)
	}
	if err := validateJSONSchema(raw); err != nil {
		return fmt.Errorf("%w: invalid %s schema: %v", ErrInvalidCapability, label, err)
	}
	return nil
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	data, err := jsonstrict.Canonical(raw)
	if err != nil {
		return nil, fmt.Errorf("canonicalize json: %w", err)
	}
	return json.RawMessage(data), nil
}
