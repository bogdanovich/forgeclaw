package bus

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	inboundSpoolVersion = 1
	spoolPendingExt     = ".json"
	spoolProcessingExt  = ".processing"
	spoolFailedExt      = ".failed"
)

// InboundSpool stores normalized inbound messages durably before they are
// processed by the agent loop. It is intentionally channel-agnostic: raw
// platform updates are normalized first, then this spool preserves the common
// bus.InboundMessage contract.
type InboundSpool struct {
	dir string
}

type spooledInboundRecord struct {
	Version    int            `json:"version"`
	ID         string         `json:"id"`
	ReceivedAt time.Time      `json:"received_at"`
	Attempts   int            `json:"attempts,omitempty"`
	LastError  string         `json:"last_error,omitempty"`
	Message    InboundMessage `json:"message"`
}

func NewInboundSpool(dir string) (*InboundSpool, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("inbound spool dir is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create inbound spool dir: %w", err)
	}
	return &InboundSpool{dir: dir}, nil
}

func (s *InboundSpool) Dir() string {
	if s == nil {
		return ""
	}
	return s.dir
}

func (s *InboundSpool) Prepare(ctx context.Context, msg InboundMessage) (InboundMessage, error) {
	if s == nil {
		return msg, nil
	}
	if msg.SpoolID != "" {
		return msg, nil
	}
	if err := ctx.Err(); err != nil {
		return msg, err
	}

	id, err := newSpoolID()
	if err != nil {
		return msg, err
	}
	msg.SpoolID = id
	rec := spooledInboundRecord{
		Version:    inboundSpoolVersion,
		ID:         id,
		ReceivedAt: time.Now().UTC(),
		Message:    msg,
	}
	if err := s.writeRecord(s.pendingPath(id), rec); err != nil {
		return msg, err
	}
	if err := os.Rename(s.pendingPath(id), s.processingPath(id)); err != nil {
		return msg, fmt.Errorf("claim inbound spool entry %s: %w", id, err)
	}
	return msg, nil
}

func (s *InboundSpool) Pending(ctx context.Context, limit int) ([]InboundMessage, error) {
	if s == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read inbound spool dir: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, spoolPendingExt) || strings.HasSuffix(name, spoolProcessingExt) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}

	msgs := make([]InboundMessage, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return msgs, err
		}
		rec, err := s.readRecord(filepath.Join(s.dir, name))
		if err != nil {
			return msgs, err
		}
		msg := NormalizeInboundMessage(rec.Message)
		msg.SpoolID = rec.ID
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (s *InboundSpool) Ack(id string) error {
	if s == nil || id == "" {
		return nil
	}
	if err := removeIfExists(s.processingPath(id)); err != nil {
		return err
	}
	return removeIfExists(s.pendingPath(id))
}

func (s *InboundSpool) Release(id string, cause error) error {
	if s == nil || id == "" {
		return nil
	}
	rec, err := s.readRecord(s.processingPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	rec.Attempts++
	if cause != nil {
		rec.LastError = cause.Error()
	}
	if err := s.writeRecord(s.processingPath(id), rec); err != nil {
		return err
	}
	return os.Rename(s.processingPath(id), s.pendingPath(id))
}

func (s *InboundSpool) Fail(id string, cause error) error {
	if s == nil || id == "" {
		return nil
	}
	rec, err := s.readRecord(s.processingPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	rec.Attempts++
	if cause != nil {
		rec.LastError = cause.Error()
	}
	if err := s.writeRecord(s.processingPath(id), rec); err != nil {
		return err
	}
	return os.Rename(s.processingPath(id), s.failedPath(id))
}

func (s *InboundSpool) writeRecord(path string, rec spooledInboundRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal inbound spool entry %s: %w", rec.ID, err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write inbound spool entry %s: %w", rec.ID, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit inbound spool entry %s: %w", rec.ID, err)
	}
	return nil
}

func (s *InboundSpool) readRecord(path string) (spooledInboundRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return spooledInboundRecord{}, err
	}
	var rec spooledInboundRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return spooledInboundRecord{}, fmt.Errorf("decode inbound spool entry %s: %w", path, err)
	}
	if rec.ID == "" {
		rec.ID = spoolIDFromName(filepath.Base(path))
	}
	return rec, nil
}

func (s *InboundSpool) pendingPath(id string) string {
	return filepath.Join(s.dir, id+spoolPendingExt)
}

func (s *InboundSpool) processingPath(id string) string {
	return filepath.Join(s.dir, id+spoolProcessingExt)
}

func (s *InboundSpool) failedPath(id string) string {
	return filepath.Join(s.dir, id+spoolFailedExt)
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func newSpoolID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate inbound spool id: %w", err)
	}
	return fmt.Sprintf("%d-%s", time.Now().UTC().UnixNano(), hex.EncodeToString(b[:])), nil
}

func spoolIDFromName(name string) string {
	for _, suffix := range []string{spoolProcessingExt, spoolFailedExt, spoolPendingExt} {
		name = strings.TrimSuffix(name, suffix)
	}
	return name
}
