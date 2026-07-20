package nodes

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/nodes/internal/jsonstrict"
)

const (
	ProtocolV1         = 1
	MaxIDLength        = 128
	MaxAliasLength     = 64
	MaxCommandNameLen  = 128
	MaxSchemaBytes     = 64 * 1024
	MaxCatalogCommands = 128
	MaxCatalogBytes    = 512 * 1024
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

type CommandDescriptor struct {
	Name             string          `json:"name"`
	InputSchema      json.RawMessage `json:"input_schema"`
	OutputSchema     json.RawMessage `json:"output_schema"`
	Risk             Risk            `json:"risk"`
	SupportsProgress bool            `json:"supports_progress,omitempty"`
	SupportsCancel   bool            `json:"supports_cancel,omitempty"`
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
	return nil
}

func (descriptor CommandDescriptor) Capability() string {
	prefix, _, _ := strings.Cut(descriptor.Name, ".")
	if !capabilityPattern.MatchString(prefix) {
		return ""
	}
	return prefix
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
	}
	data, err := json.Marshal(CapabilityCatalog{Commands: commands})
	if err != nil {
		return "", fmt.Errorf("marshal capability catalog: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
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
	return err == nil && len(decoded) == sha256.Size
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
