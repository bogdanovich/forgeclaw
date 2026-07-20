package nodes

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const registryFileVersion = 1

type registryRecord struct {
	Snapshot      Snapshot `json:"snapshot"`
	PublicKey     []byte   `json:"public_key,omitempty"`
	RequestedRole string   `json:"requested_role,omitempty"`
	RequestedAt   int64    `json:"requested_at,omitempty"`
}

type registryDocument struct {
	Version int                       `json:"version"`
	Records map[string]registryRecord `json:"records"`
}

type FileRegistry struct {
	path       string
	maxPending int

	mu      sync.RWMutex
	records map[string]registryRecord
}

func NewFileRegistry(path string, maxPending int) (*FileRegistry, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("node registry path is required")
	}
	if maxPending <= 0 {
		maxPending = DefaultMaxPendingPairings
	}
	registry := &FileRegistry{
		path:       path,
		maxPending: maxPending,
		records:    make(map[string]registryRecord),
	}
	if err := registry.load(); err != nil {
		return nil, err
	}
	return registry, nil
}

func (registry *FileRegistry) List(filter Filter) ([]Snapshot, error) {
	if filter.Alias != "" {
		if err := filter.Alias.Validate(); err != nil {
			return nil, err
		}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	states := make(map[State]struct{}, len(filter.States))
	for _, state := range filter.States {
		if !state.Valid() {
			return nil, fmt.Errorf("%w: unsupported filter state %q", ErrInvalidNode, state)
		}
		states[state] = struct{}{}
	}
	result := make([]Snapshot, 0, len(registry.records))
	for _, record := range registry.records {
		if len(states) > 0 {
			if _, included := states[record.Snapshot.State]; !included {
				continue
			}
		}
		if filter.Alias != "" && !snapshotHasAlias(record.Snapshot, filter.Alias) {
			continue
		}
		result = append(result, cloneSnapshot(record.Snapshot))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (registry *FileRegistry) Resolve(ref string) (Snapshot, bool, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return Snapshot{}, false, nil
	}
	if record, exists := registry.records[ref]; exists {
		return cloneSnapshot(record.Snapshot), true, nil
	}
	var found *Snapshot
	for _, record := range registry.records {
		for _, alias := range record.Snapshot.Aliases {
			if string(alias) != ref {
				continue
			}
			if found != nil {
				return Snapshot{}, false, fmt.Errorf("%w: ambiguous alias %q", ErrInvalidNode, ref)
			}
			snapshot := cloneSnapshot(record.Snapshot)
			found = &snapshot
		}
	}
	if found == nil {
		return Snapshot{}, false, nil
	}
	return *found, true, nil
}

func (registry *FileRegistry) Upsert(snapshot Snapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if snapshot.State == StatePendingPairing {
		return fmt.Errorf("%w: pending nodes require pairing identity", ErrInvalidNode)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record := registry.records[string(snapshot.ID)]
	record.Snapshot = cloneSnapshot(snapshot)
	return registry.commitRecordLocked(record)
}

func (registry *FileRegistry) MarkDisconnected(id ID, disconnect Disconnect) error {
	if err := id.Validate(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, exists := registry.records[string(id)]
	if !exists {
		return fmt.Errorf("%w: unknown node %q", ErrInvalidNode, id)
	}
	if record.Snapshot.State != StateConnected && record.Snapshot.State != StateDegraded {
		return fmt.Errorf("%w: cannot disconnect node in state %q", ErrInvalidNode, record.Snapshot.State)
	}
	record.Snapshot.State = StateDisconnected
	record.Snapshot.DisconnectReason = strings.TrimSpace(disconnect.Reason)
	if disconnect.At > 0 {
		record.Snapshot.LastSeenAt = disconnect.At
	}
	return registry.commitRecordLocked(record)
}

func (registry *FileRegistry) UpsertPending(pairing PendingPairing) error {
	if pairing.Node.State != StatePendingPairing {
		return fmt.Errorf("%w: pending pairing has state %q", ErrInvalidNode, pairing.Node.State)
	}
	if err := pairing.Node.Validate(); err != nil {
		return err
	}
	if len(pairing.PublicKey) != ed25519.PublicKeySize ||
		pairing.RequestedRole != "companion" || pairing.RequestedAt <= 0 {
		return fmt.Errorf("%w: malformed pending pairing", ErrInvalidNode)
	}
	derivedID, err := DeriveID(ed25519.PublicKey(pairing.PublicKey))
	if err != nil || derivedID != pairing.Node.ID {
		return fmt.Errorf("%w: pending node id does not match public key", ErrInvalidNode)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	existing, exists := registry.records[string(pairing.Node.ID)]
	if exists && len(existing.PublicKey) > 0 && !bytes.Equal(existing.PublicKey, pairing.PublicKey) {
		return fmt.Errorf("%w: node public key changed", ErrInvalidNode)
	}
	if !exists && registry.pendingCountLocked() >= registry.maxPending {
		return ErrAdmissionBusy
	}
	if exists && existing.Snapshot.State != StatePendingPairing {
		return fmt.Errorf("%w: node is already %s", ErrInvalidNode, existing.Snapshot.State)
	}
	if exists {
		pairing.RequestedAt = existing.RequestedAt
	}
	return registry.commitRecordLocked(registryRecord{
		Snapshot:      cloneSnapshot(pairing.Node),
		PublicKey:     append([]byte(nil), pairing.PublicKey...),
		RequestedRole: pairing.RequestedRole,
		RequestedAt:   pairing.RequestedAt,
	})
}

func (registry *FileRegistry) Pending(id ID) (PendingPairing, bool, error) {
	if err := id.Validate(); err != nil {
		return PendingPairing{}, false, err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	record, exists := registry.records[string(id)]
	if !exists || record.Snapshot.State != StatePendingPairing {
		return PendingPairing{}, false, nil
	}
	return PendingPairing{
		Node:          cloneSnapshot(record.Snapshot),
		PublicKey:     append([]byte(nil), record.PublicKey...),
		RequestedRole: record.RequestedRole,
		RequestedAt:   record.RequestedAt,
	}, true, nil
}

func (registry *FileRegistry) commitRecordLocked(record registryRecord) error {
	recordID := string(record.Snapshot.ID)
	next := make(map[string]registryRecord, len(registry.records)+1)
	for id, existing := range registry.records {
		next[id] = existing
	}
	next[recordID] = record
	if err := validateRegistryNamespace(next); err != nil {
		return err
	}
	if err := registry.save(next); err != nil {
		if fileutil.IsCommittedWriteError(err) {
			registry.records = next
		}
		return err
	}
	registry.records = next
	return nil
}

func (registry *FileRegistry) pendingCountLocked() int {
	count := 0
	for _, record := range registry.records {
		if record.Snapshot.State == StatePendingPairing {
			count++
		}
	}
	return count
}

func (registry *FileRegistry) save(records map[string]registryRecord) error {
	document := registryDocument{Version: registryFileVersion, Records: records}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encode node registry: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(registry.path), 0o700); err != nil {
		return fmt.Errorf("create node registry directory: %w", err)
	}
	if err := fileutil.WriteFileAtomic(registry.path, data, 0o600); err != nil {
		return fmt.Errorf("save node registry: %w", err)
	}
	return nil
}

func (registry *FileRegistry) load() error {
	data, err := os.ReadFile(registry.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read node registry: %w", err)
	}
	var document registryDocument
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("decode node registry: %w", err)
	}
	if document.Version != registryFileVersion {
		return fmt.Errorf("unsupported node registry version %d", document.Version)
	}
	if document.Records == nil {
		document.Records = make(map[string]registryRecord)
	}
	for id, record := range document.Records {
		if id != string(record.Snapshot.ID) {
			return fmt.Errorf("node registry key %q does not match record id %q", id, record.Snapshot.ID)
		}
		if err := record.Snapshot.Validate(); err != nil {
			return fmt.Errorf("validate node registry record %q: %w", id, err)
		}
		if record.Snapshot.State == StatePendingPairing &&
			(len(record.PublicKey) != ed25519.PublicKeySize ||
				record.RequestedRole != "companion" || record.RequestedAt <= 0) {
			return fmt.Errorf("validate pending node registry record %q: missing pairing identity", id)
		}
		if record.Snapshot.State == StatePendingPairing {
			derivedID, deriveErr := DeriveID(ed25519.PublicKey(record.PublicKey))
			if deriveErr != nil || derivedID != record.Snapshot.ID {
				return fmt.Errorf("validate pending node registry record %q: identity mismatch", id)
			}
		}
	}
	if err := validateRegistryNamespace(document.Records); err != nil {
		return fmt.Errorf("validate node registry namespace: %w", err)
	}
	registry.records = document.Records
	return nil
}

func validateRegistryNamespace(records map[string]registryRecord) error {
	aliases := make(map[Alias]string)
	for id, record := range records {
		for _, alias := range record.Snapshot.Aliases {
			if aliasOwner, exists := aliases[alias]; exists && aliasOwner != id {
				return fmt.Errorf("%w: alias %q belongs to both %q and %q", ErrInvalidNode, alias, aliasOwner, id)
			}
			if _, exists := records[string(alias)]; exists && string(alias) != id {
				return fmt.Errorf("%w: alias %q conflicts with a node id", ErrInvalidNode, alias)
			}
			aliases[alias] = id
		}
	}
	return nil
}

func snapshotHasAlias(snapshot Snapshot, alias Alias) bool {
	for _, candidate := range snapshot.Aliases {
		if candidate == alias {
			return true
		}
	}
	return false
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	result := snapshot
	result.Aliases = append([]Alias(nil), snapshot.Aliases...)
	result.Catalog.Commands = append([]CommandDescriptor(nil), snapshot.Catalog.Commands...)
	for index := range result.Catalog.Commands {
		result.Catalog.Commands[index].InputSchema = append(
			json.RawMessage(nil), snapshot.Catalog.Commands[index].InputSchema...,
		)
		result.Catalog.Commands[index].OutputSchema = append(
			json.RawMessage(nil), snapshot.Catalog.Commands[index].OutputSchema...,
		)
	}
	return result
}
