package companion

import (
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
)

const (
	MaxTerminalFrameBytes       = 32 * 1024
	MaxTerminalBufferBytes      = 1024 * 1024
	MaxTerminalIdleSeconds      = 3600
	MaxTerminalLifetimeSeconds  = 8 * 60 * 60
	DefaultTerminalIdleSeconds  = 900
	DefaultTerminalBufferBytes  = MaxTerminalBufferBytes
	DefaultTerminalColumns      = 80
	DefaultTerminalRows         = 24
	MinTerminalColumns          = 20
	MaxTerminalColumns          = 400
	MinTerminalRows             = 5
	MaxTerminalRows             = 200
	maxConcurrentTerminals      = 2
	terminalCloseEscalationCode = "KILL"
)

const (
	TerminalEventOpened  = "opened"
	TerminalEventOutput  = "output"
	TerminalEventAck     = "ack"
	TerminalEventClosed  = "closed"
	TerminalEventUnknown = "unknown"
	TerminalEventDenied  = "denied"
)

const (
	TerminalCloseNatural        = "exit"
	TerminalCloseRequested      = "close"
	TerminalCloseIdleTimeout    = "idle_timeout"
	TerminalCloseLifetime       = "lifetime_timeout"
	TerminalCloseDisconnected   = "disconnect"
	TerminalCloseOutputOverflow = "output_overflow"
)

var terminalIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

var ErrTerminalOutcomeUnknown = errors.New("terminal process-tree outcome is unknown")

type TerminalBrokerOpenRequest struct {
	OpenID          string            `json:"open_id"`
	PlanHash        string            `json:"plan_hash"`
	Profile         string            `json:"profile"`
	ProfileRevision string            `json:"profile_revision"`
	WorkingScope    string            `json:"working_scope"`
	Environment     map[string]string `json:"environment"`
	Columns         int               `json:"columns"`
	Rows            int               `json:"rows"`
	IdleSeconds     int               `json:"idle_seconds"`
	LifetimeSeconds int               `json:"lifetime_seconds"`
	BufferBytes     int               `json:"buffer_bytes"`
}

type TerminalSize struct {
	Columns int `json:"columns"`
	Rows    int `json:"rows"`
}

type TerminalBrokerControl struct {
	Version        int           `json:"version"`
	Sequence       uint64        `json:"sequence"`
	IdempotencyKey string        `json:"idempotency_key"`
	InputBase64    string        `json:"input_base64,omitempty"`
	Resize         *TerminalSize `json:"resize,omitempty"`
	Signal         string        `json:"signal,omitempty"`
	Close          bool          `json:"close,omitempty"`
}

type TerminalBrokerEvent struct {
	Version              int    `json:"version"`
	Type                 string `json:"type"`
	TerminalID           string `json:"terminal_id"`
	AcceptedSequence     uint64 `json:"accepted_sequence,omitempty"`
	Cursor               uint64 `json:"cursor,omitempty"`
	DataBase64           string `json:"data_base64,omitempty"`
	State                string `json:"state,omitempty"`
	Reason               string `json:"reason,omitempty"`
	ExitCode             int    `json:"exit_code,omitempty"`
	Signal               string `json:"signal,omitempty"`
	StartedAt            int64  `json:"started_at,omitempty"`
	CompletedAt          int64  `json:"completed_at,omitempty"`
	TerminationConfirmed bool   `json:"termination_confirmed,omitempty"`
}

func (request TerminalBrokerOpenRequest) validate() error {
	if !terminalIdentifierPattern.MatchString(strings.TrimSpace(request.OpenID)) ||
		!authorityBrokerDigestPattern.MatchString(strings.TrimSpace(request.PlanHash)) ||
		strings.TrimSpace(request.Profile) == "" ||
		!validShellBrokerRevision(strings.TrimSpace(request.ProfileRevision)) ||
		strings.TrimSpace(request.WorkingScope) == "" ||
		!validTerminalSize(request.Columns, request.Rows) ||
		request.IdleSeconds <= 0 ||
		request.IdleSeconds > MaxTerminalIdleSeconds ||
		request.LifetimeSeconds < request.IdleSeconds ||
		request.LifetimeSeconds > MaxTerminalLifetimeSeconds ||
		request.BufferBytes <= 0 ||
		request.BufferBytes > MaxTerminalBufferBytes {
		return errors.New("terminal open request is invalid")
	}
	return nil
}

func (control TerminalBrokerControl) validate() ([]byte, error) {
	if control.Version != AuthorityBrokerProtocolVersion ||
		control.Sequence == 0 ||
		!terminalIdentifierPattern.MatchString(strings.TrimSpace(control.IdempotencyKey)) {
		return nil, errors.New("terminal control identity is invalid")
	}
	operations := 0
	if control.InputBase64 != "" {
		operations++
	}
	if control.Resize != nil {
		operations++
	}
	if control.Signal != "" {
		operations++
	}
	if control.Close {
		operations++
	}
	if operations != 1 {
		return nil, errors.New("terminal control must contain exactly one operation")
	}
	if control.Resize != nil &&
		!validTerminalSize(control.Resize.Columns, control.Resize.Rows) {
		return nil, errors.New("terminal resize is out of bounds")
	}
	switch control.Signal {
	case "", "INT", "TERM", "HUP":
	default:
		return nil, errors.New("terminal signal is unsupported")
	}
	if control.InputBase64 == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.Strict().DecodeString(control.InputBase64)
	if err != nil || len(data) == 0 || len(data) > MaxTerminalFrameBytes {
		return nil, errors.New("terminal input frame is invalid")
	}
	return data, nil
}

func (event TerminalBrokerEvent) validate() error {
	if event.Version != AuthorityBrokerProtocolVersion ||
		!terminalIdentifierPattern.MatchString(event.TerminalID) {
		return errors.New("terminal event identity is invalid")
	}
	switch event.Type {
	case TerminalEventOpened:
		if event.State != "live" || event.StartedAt <= 0 {
			return errors.New("terminal opened event is invalid")
		}
	case TerminalEventOutput:
		data, err := base64.StdEncoding.Strict().DecodeString(event.DataBase64)
		if err != nil || len(data) == 0 || len(data) > MaxTerminalFrameBytes ||
			event.Cursor < uint64(len(data)) {
			return errors.New("terminal output event is invalid")
		}
	case TerminalEventAck:
		if event.AcceptedSequence == 0 || event.State != "live" {
			return errors.New("terminal acknowledgement is invalid")
		}
	case TerminalEventClosed:
		if event.State != "closed" || event.CompletedAt < event.StartedAt ||
			event.CompletedAt <= 0 || !event.TerminationConfirmed {
			return errors.New("terminal closed event is invalid")
		}
	case TerminalEventUnknown:
		if event.State != "unknown" || event.TerminationConfirmed {
			return errors.New("terminal unknown event is invalid")
		}
	case TerminalEventDenied:
	default:
		return errors.New("terminal event type is invalid")
	}
	return nil
}

func validTerminalSize(columns, rows int) bool {
	return columns >= MinTerminalColumns && columns <= MaxTerminalColumns &&
		rows >= MinTerminalRows && rows <= MaxTerminalRows
}
