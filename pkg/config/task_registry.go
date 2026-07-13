package config

import (
	"time"

	taskregistry "github.com/sipeed/picoclaw/pkg/tasks"
)

// TaskConfig bounds the durable async task registry stored in each
// workspace. Zero values preserve the registry's built-in safe defaults.
type TaskConfig struct {
	TerminalRetentionHours int `json:"terminal_retention_hours,omitempty" env:"PICOCLAW_TASK_REGISTRY_TERMINAL_RETENTION_HOURS"`
	MaxRecords             int `json:"max_records,omitempty"              env:"PICOCLAW_TASK_REGISTRY_MAX_RECORDS"`
	MaxEvents              int `json:"max_events,omitempty"               env:"PICOCLAW_TASK_REGISTRY_MAX_EVENTS"`
	MaxSnapshotBytes       int `json:"max_snapshot_bytes,omitempty"       env:"PICOCLAW_TASK_REGISTRY_MAX_SNAPSHOT_BYTES"`
}

func defaultTaskConfig() TaskConfig {
	return TaskConfig{
		TerminalRetentionHours: int(taskregistry.DefaultTerminalRetention / time.Hour),
		MaxRecords:             taskregistry.DefaultMaxRecords,
		MaxEvents:              taskregistry.DefaultMaxEvents,
		MaxSnapshotBytes:       taskregistry.DefaultMaxSnapshotBytes,
	}
}

// Options converts config into the task registry's runtime options.
func (c TaskConfig) Options() taskregistry.Options {
	opts := taskregistry.Options{
		MaxRecords:       c.MaxRecords,
		MaxEvents:        c.MaxEvents,
		MaxSnapshotBytes: c.MaxSnapshotBytes,
	}
	if c.TerminalRetentionHours > 0 {
		opts.TerminalRetention = time.Duration(c.TerminalRetentionHours) * time.Hour
	}
	return opts
}
