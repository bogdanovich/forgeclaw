package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateMCPExclusiveLockFile(t *testing.T) {
	parent := t.TempDir()
	validPath := filepath.Join(parent, "playwright.lock")
	notDirectory := filepath.Join(parent, "not-a-directory")
	if err := os.WriteFile(notDirectory, []byte("test"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	tests := []struct {
		name    string
		server  MCPServerConfig
		wantErr string
	}{
		{name: "omitted", server: MCPServerConfig{Command: "example"}},
		{
			name: "valid stdio lease",
			server: MCPServerConfig{
				Command:           "example",
				ExclusiveLockFile: validPath,
			},
		},
		{
			name: "remote transport",
			server: MCPServerConfig{
				Type:              "http",
				URL:               "https://example.invalid/mcp",
				ExclusiveLockFile: validPath,
			},
			wantErr: "only for stdio",
		},
		{
			name: "relative path",
			server: MCPServerConfig{
				Command:           "example",
				ExclusiveLockFile: "playwright.lock",
			},
			wantErr: "must be absolute",
		},
		{
			name: "unclean path",
			server: MCPServerConfig{
				Command: "example",
				ExclusiveLockFile: parent + string(os.PathSeparator) + "nested" +
					string(os.PathSeparator) + ".." + string(os.PathSeparator) + "playwright.lock",
			},
			wantErr: "must be clean",
		},
		{
			name: "missing parent",
			server: MCPServerConfig{
				Command:           "example",
				ExclusiveLockFile: filepath.Join(parent, "missing", "playwright.lock"),
			},
			wantErr: "parent is unavailable",
		},
		{
			name: "parent is a file",
			server: MCPServerConfig{
				Command:           "example",
				ExclusiveLockFile: filepath.Join(notDirectory, "playwright.lock"),
			},
			wantErr: "parent is not a directory",
		},
		{
			name: "oversized path",
			server: MCPServerConfig{
				Command:           "example",
				ExclusiveLockFile: string(os.PathSeparator) + strings.Repeat("a", maxMCPExclusiveLockFilePathBytes),
			},
			wantErr: "exceeds",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateMCPExclusiveLockFile(test.server)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateMCPExclusiveLockFile() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateMCPExclusiveLockFile() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}
