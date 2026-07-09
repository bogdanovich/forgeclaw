package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const restartSentinelFileName = "restart-sentinel.json"

const DefaultRestartPreflightTimeout = 2 * time.Second

type RestartOrigin struct {
	Channel    string `json:"channel,omitempty"`
	ChatID     string `json:"chat_id,omitempty"`
	SessionKey string `json:"session_key,omitempty"`
}

type RestartPreflight struct {
	CheckedAt              time.Time           `json:"checked_at"`
	ActiveTurnsUnavailable bool                `json:"active_turns_unavailable,omitempty"`
	ActiveTurns            int                 `json:"active_turns"`
	PendingInbound         int                 `json:"pending_inbound"`
	PendingInboundError    string              `json:"pending_inbound_error,omitempty"`
	PendingInboundTimedOut bool                `json:"pending_inbound_timed_out,omitempty"`
	OutboundDepth          int                 `json:"outbound_depth"`
	OutboundMediaDepth     int                 `json:"outbound_media_depth"`
	CronJobsUnavailable    bool                `json:"cron_jobs_unavailable,omitempty"`
	ActiveCronJobs         int                 `json:"active_cron_jobs"`
	BusStats               bus.MessageBusStats `json:"bus_stats"`
}

func (p RestartPreflight) HasActiveWork() bool {
	return p.ActiveTurns > 0 ||
		p.PendingInbound > 0 ||
		p.OutboundDepth > 0 ||
		p.OutboundMediaDepth > 0 ||
		p.ActiveCronJobs > 0
}

type RestartPreflightOptions struct {
	Now     func() time.Time
	Timeout time.Duration
}

type RestartPreflightSource interface {
	Stats() bus.MessageBusStats
	PendingInboundSpool(ctx context.Context) ([]bus.InboundMessage, error)
}

func CollectRestartPreflight(
	ctx context.Context,
	source RestartPreflightSource,
	opts RestartPreflightOptions,
) RestartPreflight {
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	result := RestartPreflight{
		CheckedAt:              now().UTC(),
		ActiveTurnsUnavailable: true,
		CronJobsUnavailable:    true,
	}
	if source == nil {
		return result
	}

	stats := source.Stats()
	result.BusStats = stats
	result.OutboundDepth = stats.Outbound.Depth
	result.OutboundMediaDepth = stats.OutboundMedia.Depth

	pending, err, timedOut := collectPendingInboundBounded(ctx, source, opts.Timeout)
	if timedOut {
		result.PendingInboundTimedOut = true
		result.PendingInboundError = "pending inbound spool preflight timed out"
		return result
	}
	if err != nil {
		result.PendingInboundError = err.Error()
		return result
	}
	result.PendingInbound = len(pending)
	return result
}

func collectPendingInboundBounded(
	ctx context.Context,
	source RestartPreflightSource,
	timeout time.Duration,
) ([]bus.InboundMessage, error, bool) {
	if timeout <= 0 {
		timeout = DefaultRestartPreflightTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		messages []bus.InboundMessage
		err      error
	}
	done := make(chan result, 1)
	go func() {
		messages, err := source.PendingInboundSpool(ctx)
		done <- result{messages: messages, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err(), true
	case result := <-done:
		return result.messages, result.err, false
	}
}

type RestartSentinel struct {
	Kind             string           `json:"kind"`
	Status           string           `json:"status"`
	RequestedService string           `json:"requested_service,omitempty"`
	Origin           RestartOrigin    `json:"origin,omitempty"`
	RequestedAt      time.Time        `json:"requested_at"`
	UpdatedAt        time.Time        `json:"updated_at"`
	Reason           string           `json:"reason,omitempty"`
	Preflight        RestartPreflight `json:"preflight"`
}

type RestartSentinelStore struct {
	dir string
}

func NewRestartSentinelStore(dir string) (*RestartSentinelStore, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("restart sentinel dir is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create restart sentinel dir: %w", err)
	}
	return &RestartSentinelStore{dir: dir}, nil
}

func (s *RestartSentinelStore) Write(sentinel RestartSentinel) error {
	if s == nil {
		return errors.New("restart sentinel store is nil")
	}
	if sentinel.Kind == "" {
		sentinel.Kind = "restart"
	}
	if sentinel.Kind != "restart" {
		return fmt.Errorf("unsupported restart sentinel kind %q", sentinel.Kind)
	}
	if sentinel.Status == "" {
		sentinel.Status = "pending"
	}
	if sentinel.RequestedAt.IsZero() {
		sentinel.RequestedAt = time.Now().UTC()
	}
	if sentinel.UpdatedAt.IsZero() {
		sentinel.UpdatedAt = sentinel.RequestedAt
	}
	data, err := json.MarshalIndent(sentinel, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal restart sentinel: %w", err)
	}
	if err := fileutil.WriteFileAtomic(s.path(), data, 0o600); err != nil {
		return fmt.Errorf("write restart sentinel: %w", err)
	}
	return nil
}

func (s *RestartSentinelStore) Read() (RestartSentinel, error) {
	if s == nil {
		return RestartSentinel{}, errors.New("restart sentinel store is nil")
	}
	data, err := os.ReadFile(s.path())
	if err != nil {
		return RestartSentinel{}, err
	}
	var sentinel RestartSentinel
	if err := json.Unmarshal(data, &sentinel); err != nil {
		return RestartSentinel{}, fmt.Errorf("decode restart sentinel: %w", err)
	}
	return sentinel, nil
}

func (s *RestartSentinelStore) Clear() error {
	if s == nil {
		return errors.New("restart sentinel store is nil")
	}
	if err := os.Remove(s.path()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *RestartSentinelStore) path() string {
	return filepath.Join(s.dir, restartSentinelFileName)
}
