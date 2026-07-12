package doctor

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	doctorpkg "github.com/sipeed/picoclaw/pkg/doctor"
)

func TestNewDoctorCommand(t *testing.T) {
	cmd := NewDoctorCommand()
	if cmd.Use != "doctor" {
		t.Fatalf("Use = %q", cmd.Use)
	}
	if cmd.RunE == nil {
		t.Fatal("RunE is nil")
	}
	for _, name := range []string{"config", "json", "strict"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("missing flag %q", name)
		}
	}
}

func TestCommandReturnsExitErrorAfterRendering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	config := `{"version":3,"gateway":{"host":"0.0.0.0"},"channel_list":{},"model_list":[]}`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := NewDoctorCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--config", path, "--json"})
	err := cmd.Execute()
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != 2 {
		t.Fatalf("error = %v, want ExitError code 2", err)
	}
	if !strings.Contains(out.String(), `"schema_version": "doctor.v1"`) {
		t.Fatalf("missing JSON output: %s", out.String())
	}
}

func TestExitCode(t *testing.T) {
	tests := []struct {
		name   string
		report *doctorpkg.Report
		strict bool
		want   int
	}{
		{name: "nil", want: 1},
		{name: "clean", report: &doctorpkg.Report{}, want: 0},
		{name: "warning default ok", report: &doctorpkg.Report{Summary: doctorpkg.Summary{Warning: 1}}, want: 0},
		{
			name:   "warning strict fails",
			report: &doctorpkg.Report{Summary: doctorpkg.Summary{Warning: 1}},
			strict: true,
			want:   2,
		},
		{name: "fail fails", report: &doctorpkg.Report{Summary: doctorpkg.Summary{Fail: 1}}, want: 2},
		{name: "error fails", report: &doctorpkg.Report{Summary: doctorpkg.Summary{Error: 1}}, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExitCode(tt.report, tt.strict); got != tt.want {
				t.Fatalf("ExitCode = %d, want %d", got, tt.want)
			}
		})
	}
}
