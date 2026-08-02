package api

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

func TestValidateWecomAllowFrom(t *testing.T) {
	tests := []struct {
		name      string
		allowFrom config.FlexibleStringSlice
		wantErr   bool
	}{
		{name: "omitted denies binding", wantErr: true},
		{name: "blank denies binding", allowFrom: config.FlexibleStringSlice{"  "}, wantErr: true},
		{name: "trusted sender permits binding", allowFrom: config.FlexibleStringSlice{"owner-1"}},
		{name: "wildcard permits binding", allowFrom: config.FlexibleStringSlice{"*"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			cfg := config.DefaultConfig()
			wecom := cfg.Channels.Get(config.ChannelWeCom)
			wecom.AllowFrom = tt.allowFrom
			if err := config.SaveConfig(path, cfg); err != nil {
				t.Fatalf("SaveConfig() error = %v", err)
			}

			handler := &Handler{configPath: path}
			err := handler.validateWecomAllowFrom()
			if tt.wantErr && (err == nil || !strings.Contains(err.Error(), "allow_from")) {
				t.Fatalf("validateWecomAllowFrom() error = %v", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validateWecomAllowFrom() error = %v", err)
			}
		})
	}
}
