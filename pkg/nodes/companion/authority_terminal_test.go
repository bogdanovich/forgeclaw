package companion

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestTerminalBrokerControlValidation(t *testing.T) {
	valid := TerminalBrokerControl{
		Version: AuthorityBrokerProtocolVersion, Sequence: 1,
		IdempotencyKey: "input-1",
		InputBase64:    base64.StdEncoding.EncodeToString([]byte("echo ready\n")),
	}
	if _, err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*TerminalBrokerControl)
	}{
		{name: "zero sequence", mutate: func(control *TerminalBrokerControl) {
			control.Sequence = 0
		}},
		{name: "invalid base64", mutate: func(control *TerminalBrokerControl) {
			control.InputBase64 = "!"
		}},
		{name: "oversized input", mutate: func(control *TerminalBrokerControl) {
			control.InputBase64 = base64.StdEncoding.EncodeToString(
				[]byte(strings.Repeat("x", MaxTerminalFrameBytes+1)),
			)
		}},
		{name: "multiple operations", mutate: func(control *TerminalBrokerControl) {
			control.Resize = &TerminalSize{
				Columns: DefaultTerminalColumns,
				Rows:    DefaultTerminalRows,
			}
		}},
		{name: "unsupported signal", mutate: func(control *TerminalBrokerControl) {
			control.InputBase64 = ""
			control.Signal = "KILL"
		}},
		{name: "out of bounds resize", mutate: func(control *TerminalBrokerControl) {
			control.InputBase64 = ""
			control.Resize = &TerminalSize{
				Columns: MinTerminalColumns - 1,
				Rows:    DefaultTerminalRows,
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			control := valid
			test.mutate(&control)
			if _, err := control.validate(); err == nil {
				t.Fatal("invalid terminal control was accepted")
			}
		})
	}
}

func TestTerminalBrokerEventValidationBoundsOutput(t *testing.T) {
	valid := TerminalBrokerEvent{
		Version: AuthorityBrokerProtocolVersion,
		Type:    TerminalEventOutput, TerminalID: "terminal_test",
		Cursor:     1,
		DataBase64: base64.StdEncoding.EncodeToString([]byte("x")),
	}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	valid.Cursor = 0
	if err := valid.validate(); err == nil {
		t.Fatal("output cursor before frame boundary was accepted")
	}
	valid.Cursor = MaxTerminalFrameBytes + 1
	valid.DataBase64 = base64.StdEncoding.EncodeToString(
		[]byte(strings.Repeat("x", MaxTerminalFrameBytes+1)),
	)
	if err := valid.validate(); err == nil {
		t.Fatal("oversized terminal output was accepted")
	}
}
