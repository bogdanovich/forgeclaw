package media

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const mediaIndexVersion = 1

// persistentMediaEntry is the complete durable representation of a media ref.
// The local file is deliberately not copied: the store only records ownership
// and verifies the file before every resolution.
type persistentMediaEntry struct {
	Ref      string    `json:"ref"`
	Path     string    `json:"path"`
	Meta     MediaMeta `json:"meta"`
	Scope    string    `json:"scope"`
	StoredAt time.Time `json:"stored_at"`
}

type mediaIndexSnapshot struct {
	Version int                    `json:"version"`
	Entries []persistentMediaEntry `json:"entries"`
}

type mediaIndex struct {
	path string
}

func loadMediaIndex(path string) ([]persistentMediaEntry, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read media index: %w", err)
	}

	var snapshot mediaIndexSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("decode media index: %w", err)
	}
	if snapshot.Version != mediaIndexVersion {
		return nil, fmt.Errorf("unsupported media index version: %d", snapshot.Version)
	}

	seen := make(map[string]struct{}, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if entry.Ref == "" || entry.Path == "" || entry.StoredAt.IsZero() {
			return nil, fmt.Errorf("invalid media index entry")
		}
		if _, exists := seen[entry.Ref]; exists {
			return nil, fmt.Errorf("duplicate media ref in index: %s", entry.Ref)
		}
		seen[entry.Ref] = struct{}{}
	}
	return snapshot.Entries, nil
}

func (i mediaIndex) save(entries []persistentMediaEntry) error {
	sort.Slice(entries, func(a, b int) bool { return entries[a].Ref < entries[b].Ref })
	data, err := json.Marshal(mediaIndexSnapshot{
		Version: mediaIndexVersion,
		Entries: entries,
	})
	if err != nil {
		return fmt.Errorf("encode media index: %w", err)
	}
	if err := fileutil.WriteFileAtomic(i.path, data, 0o600); err != nil {
		return fmt.Errorf("write media index: %w", err)
	}
	return nil
}
