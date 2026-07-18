package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSessionLifecycleConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  SessionLifecycleConfig
		wantErr bool
	}{
		{name: "missing strategy", wantErr: true},
		{name: "never", config: SessionLifecycleConfig{Strategy: "never"}},
		{
			name: "calendar day",
			config: SessionLifecycleConfig{
				Strategy: "calendar",
				Period:   "day",
				Timezone: "America/Los_Angeles",
			},
		},
		{
			name:    "calendar missing timezone",
			config:  SessionLifecycleConfig{Strategy: "calendar", Period: "day"},
			wantErr: true,
		},
		{
			name: "calendar invalid period",
			config: SessionLifecycleConfig{
				Strategy: "calendar",
				Period:   "quarter",
				Timezone: "UTC",
			},
			wantErr: true,
		},
		{
			name:   "idle",
			config: SessionLifecycleConfig{Strategy: "idle", IdleTimeoutMinutes: 30},
		},
		{
			name:    "idle missing timeout",
			config:  SessionLifecycleConfig{Strategy: "idle"},
			wantErr: true,
		},
		{
			name:   "max age",
			config: SessionLifecycleConfig{Strategy: "max_age", MaxAgeMinutes: 60},
		},
		{
			name:    "unknown strategy",
			config:  SessionLifecycleConfig{Strategy: "daily"},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := (&test.config).Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestSessionLifecycleConfigJSONIsOptional(t *testing.T) {
	cfg := DefaultConfig()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(data), `"lifecycle"`) {
		t.Fatalf("default config unexpectedly contains lifecycle: %s", data)
	}

	cfg.Session.Lifecycle = &SessionLifecycleConfig{
		Strategy: "calendar",
		Period:   "day",
		Timezone: "UTC",
	}
	data, err = json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !strings.Contains(string(data), `"lifecycle":{"strategy":"calendar","period":"day","timezone":"UTC"}`) {
		t.Fatalf("config is missing lifecycle: %s", data)
	}
}
