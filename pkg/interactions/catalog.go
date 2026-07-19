package interactions

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const (
	workspaceCatalogVersion   = "interaction-workspace/v1"
	workspaceCatalogMaxFiles  = 1024
	workspaceCatalogMaxBytes  = 4096
	workspaceCatalogDirectory = "interaction_workspaces"
)

type workspaceDescriptor struct {
	SchemaVersion string `json:"schema_version"`
	Workspace     string `json:"workspace"`
}

// WorkspaceCatalog records workspace-local interaction stores so restart
// recovery can find stores whose agents are no longer configured.
type WorkspaceCatalog struct {
	directory string
}

func NewWorkspaceCatalog(stateRoot string) *WorkspaceCatalog {
	stateRoot = strings.TrimSpace(stateRoot)
	if stateRoot == "" {
		return nil
	}
	return &WorkspaceCatalog{
		directory: filepath.Join(stateRoot, "state", workspaceCatalogDirectory),
	}
}

func (c *WorkspaceCatalog) Register(workspace string) error {
	if c == nil || c.directory == "" {
		return ErrStoreUnavailable
	}
	workspace = cleanCatalogWorkspace(workspace)
	if workspace == "" {
		return fmt.Errorf("%w: empty workspace", ErrInvalidInteraction)
	}
	descriptor := workspaceDescriptor{
		SchemaVersion: workspaceCatalogVersion,
		Workspace:     workspace,
	}
	data, err := json.Marshal(descriptor)
	if err != nil {
		return fmt.Errorf("marshal interaction workspace descriptor: %w", err)
	}
	return fileutil.WriteFileAtomic(c.descriptorPath(workspace), data, 0o600)
}

// List returns valid catalog entries and a joined diagnostic for malformed
// entries. Valid entries remain usable when another entry is damaged.
func (c *WorkspaceCatalog) List() ([]string, error) {
	if c == nil || c.directory == "" {
		return nil, ErrStoreUnavailable
	}
	entries, err := os.ReadDir(c.directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read interaction workspace catalog: %w", err)
	}
	if len(entries) > workspaceCatalogMaxFiles {
		return nil, fmt.Errorf(
			"interaction workspace catalog exceeds %d entries",
			workspaceCatalogMaxFiles,
		)
	}
	workspaces := make([]string, 0, len(entries))
	var diagnostics []error
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil || !info.Mode().IsRegular() || info.Size() > workspaceCatalogMaxBytes {
			diagnostics = append(diagnostics, fmt.Errorf("invalid catalog entry %q", entry.Name()))
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(c.directory, entry.Name()))
		if readErr != nil {
			diagnostics = append(diagnostics, fmt.Errorf("read catalog entry %q: %w", entry.Name(), readErr))
			continue
		}
		var descriptor workspaceDescriptor
		if jsonErr := json.Unmarshal(data, &descriptor); jsonErr != nil ||
			descriptor.SchemaVersion != workspaceCatalogVersion {
			diagnostics = append(diagnostics, fmt.Errorf("decode catalog entry %q", entry.Name()))
			continue
		}
		descriptor.Workspace = cleanCatalogWorkspace(descriptor.Workspace)
		if descriptor.Workspace == "" || entry.Name() != descriptorName(descriptor.Workspace) {
			diagnostics = append(diagnostics, fmt.Errorf("mismatched catalog entry %q", entry.Name()))
			continue
		}
		workspaces = append(workspaces, descriptor.Workspace)
	}
	sort.Strings(workspaces)
	return workspaces, errors.Join(diagnostics...)
}

func (c *WorkspaceCatalog) Remove(workspace string) error {
	if c == nil || c.directory == "" {
		return ErrStoreUnavailable
	}
	err := os.Remove(c.descriptorPath(cleanCatalogWorkspace(workspace)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (c *WorkspaceCatalog) descriptorPath(workspace string) string {
	return filepath.Join(c.directory, descriptorName(workspace))
}

func descriptorName(workspace string) string {
	sum := sha256.Sum256([]byte(workspace))
	return hex.EncodeToString(sum[:]) + ".json"
}

func cleanCatalogWorkspace(workspace string) string {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return ""
	}
	return filepath.Clean(workspace)
}
