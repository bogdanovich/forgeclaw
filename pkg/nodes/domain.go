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
)

const (
	ProtocolV1        = 1
	MaxIDLength       = 128
	MaxAliasLength    = 64
	MaxCommandNameLen = 128
	MaxSchemaBytes    = 64 * 1024
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
	Capability       string          `json:"capability"`
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
	if !capabilityPattern.MatchString(descriptor.Capability) {
		return fmt.Errorf("%w: malformed capability", ErrInvalidCapability)
	}
	if prefix, _, _ := strings.Cut(descriptor.Name, "."); prefix != descriptor.Capability {
		return fmt.Errorf("%w: command does not belong to capability", ErrInvalidCapability)
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

type CapabilityCatalog struct {
	Commands []CommandDescriptor `json:"commands"`
}

func (catalog CapabilityCatalog) Validate() error {
	seen := make(map[string]struct{}, len(catalog.Commands))
	for _, descriptor := range catalog.Commands {
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
	decodedHash, err := hex.DecodeString(snapshot.CatalogHash)
	if err != nil || len(decodedHash) != sha256.Size {
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
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return fmt.Errorf("%w: %s schema must be an object", ErrInvalidCapability, label)
	}
	return nil
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("canonicalize json: %w", err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("canonicalize json: %w", err)
	}
	return json.RawMessage(data), nil
}
