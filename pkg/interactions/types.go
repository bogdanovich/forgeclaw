package interactions

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type Kind string

const (
	KindQuestion Kind = "question"
	KindApproval Kind = "approval"
)

type Status string

const (
	StatusCreated  Status = "created"
	StatusWaiting  Status = "waiting"
	StatusClaimed  Status = "answer_claimed"
	StatusResuming Status = "resuming"
	StatusResolved Status = "resolved"
	//nolint:misspell // External interaction status follows the existing task status spelling.
	StatusCancelled Status = "cancelled"
	StatusFailed    Status = "failed"
)

type Outcome string

const (
	OutcomeAnswered Outcome = "answered"
	OutcomeTimedOut Outcome = "timed_out"
	OutcomeAllowed  Outcome = "allowed"
	OutcomeDenied   Outcome = "denied"
)

type EventType string

const (
	EventCreated          EventType = "interaction.created"
	EventDeliveryAttempt  EventType = "interaction.delivery_attempted"
	EventWaiting          EventType = "interaction.waiting"
	EventAnswerClaimed    EventType = "interaction.answer_claimed"
	EventResumeStarted    EventType = "interaction.resume_started"
	EventResolved         EventType = "interaction.resolved"
	EventCancelled        EventType = "interaction.cancelled"
	EventFailed           EventType = "interaction.failed"
	EventRecoveryObserved EventType = "interaction.recovery_observed"
)

const (
	SnapshotSchemaVersion = "interaction_snapshot.v1"
	EventSchemaVersion    = "interaction_event.v1"
	DefaultRetention      = 7 * 24 * time.Hour
	DefaultMaxRecords     = 1000
	DefaultMaxEvents      = 5000
	DefaultMaxBytes       = 2 * 1024 * 1024

	MaxQuestions         = 3
	MaxOptions           = 3
	MaxQuestionIDLength  = 64
	MaxHeaderLength      = 12
	MaxQuestionLength    = 1000
	MaxOptionLabelLength = 64
	MaxDescriptionLength = 500
	MaxAnswerLength      = 16 * 1024
	MaxSummaryLength     = 1000
)

var questionIDPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

var (
	ErrStoreUnavailable   = errors.New("interaction store unavailable")
	ErrNotFound           = errors.New("interaction not found")
	ErrConflict           = errors.New("interaction revision conflict")
	ErrInvalidTransition  = errors.New("invalid interaction transition")
	ErrSessionHasActive   = errors.New("session already has an active interaction")
	ErrDuplicateAnswer    = errors.New("answer message already claimed")
	ErrAnswerTooLate      = errors.New("interaction is no longer waiting")
	ErrSnapshotOverBudget = errors.New("interaction snapshot exceeds size budget")
	ErrInvalidInteraction = errors.New("invalid interaction")
	ErrCapacityExceeded   = errors.New("interaction registry capacity exceeded")
)

type Option struct {
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type Question struct {
	ID          string   `json:"id"`
	Header      string   `json:"header,omitempty"`
	Question    string   `json:"question"`
	Options     []Option `json:"options,omitempty"`
	MultiSelect bool     `json:"multi_select,omitempty"`
}

// SuspensionRequest is the authority-free payload a tool or trusted policy
// returns when execution must pause for human input. The runtime supplies the
// route, sender, turn, and tool-call identity before creating a durable record.
type SuspensionRequest struct {
	Kind          Kind
	Questions     []Question
	PromptSummary string
	Timeout       time.Duration
}

type Route struct {
	AgentID         string `json:"agent_id"`
	SessionKey      string `json:"session_key"`
	RouteSessionKey string `json:"route_session_key,omitempty"`
	Channel         string `json:"channel"`
	AccountID       string `json:"account_id,omitempty"`
	ChatID          string `json:"chat_id"`
	TopicID         string `json:"topic_id,omitempty"`
	SenderID        string `json:"sender_id"`
}

type Origin struct {
	TurnID       string `json:"turn_id"`
	ToolCallID   string `json:"tool_call_id"`
	ToolName     string `json:"tool_name"`
	TaskID       string `json:"task_id,omitempty"`
	ArgumentHash string `json:"argument_hash,omitempty"`
}

type Answer struct {
	Text       string            `json:"text,omitempty"`
	Values     map[string]string `json:"values,omitempty"`
	MessageID  string            `json:"message_id,omitempty"`
	ReceivedAt int64             `json:"received_at"`
}

type Record struct {
	ID             string     `json:"id"`
	ShortID        string     `json:"short_id"`
	Kind           Kind       `json:"kind"`
	Status         Status     `json:"status"`
	Outcome        Outcome    `json:"outcome,omitempty"`
	Revision       int64      `json:"revision"`
	LastEventSeq   int64      `json:"last_event_sequence"`
	Route          Route      `json:"route"`
	Origin         Origin     `json:"origin"`
	Questions      []Question `json:"questions,omitempty"`
	PromptSummary  string     `json:"prompt_summary,omitempty"`
	Answer         *Answer    `json:"answer,omitempty"`
	CreatedAt      int64      `json:"created_at"`
	UpdatedAt      int64      `json:"updated_at"`
	ExpiresAt      int64      `json:"expires_at"`
	ResolvedAt     int64      `json:"resolved_at,omitempty"`
	CleanupAfter   int64      `json:"cleanup_after,omitempty"`
	DeliveryTries  int        `json:"delivery_tries,omitempty"`
	LastDeliveryAt int64      `json:"last_delivery_at,omitempty"`
	DeliveryError  string     `json:"delivery_error,omitempty"`
	ResumeTries    int        `json:"resume_tries,omitempty"`
	LastResumeAt   int64      `json:"last_resume_at,omitempty"`
	ResumeError    string     `json:"resume_error,omitempty"`
	FailureCode    string     `json:"failure_code,omitempty"`
	FailureDetail  string     `json:"failure_detail,omitempty"`
}

type Event struct {
	SchemaVersion string    `json:"schema_version"`
	EventID       string    `json:"event_id"`
	InteractionID string    `json:"interaction_id"`
	Type          EventType `json:"type"`
	From          Status    `json:"from,omitempty"`
	To            Status    `json:"to,omitempty"`
	Outcome       Outcome   `json:"outcome,omitempty"`
	Revision      int64     `json:"revision"`
	Sequence      int64     `json:"sequence"`
	EmittedAt     int64     `json:"emitted_at"`
	Code          string    `json:"code,omitempty"`
	Success       *bool     `json:"success,omitempty"`
}

type EventObservation struct {
	Event  Event
	Record Record
}

type CreateRequest struct {
	ID            string
	Kind          Kind
	Route         Route
	Origin        Origin
	Questions     []Question
	PromptSummary string
	ExpiresAt     time.Time
}

type Stats struct {
	RecordCount      int
	EventCount       int
	NonterminalCount int
	SnapshotBytes    int
	Retention        time.Duration
	MaxRecords       int
	MaxEvents        int
	MaxSnapshotBytes int
	OverBudget       bool
}

// Store is the durable boundary consumed by tools, routing, and recovery.
// Implementations must preserve revision-based compare-and-swap semantics.
type Store interface {
	Create(CreateRequest) (Record, error)
	Get(string) (Record, bool)
	ListNonterminal() []Record
	FindWaitingByRoute(Route) []Record
	Prune(time.Time) error
}

func (r Route) validate() error {
	if strings.TrimSpace(r.AgentID) == "" || strings.TrimSpace(r.SessionKey) == "" ||
		strings.TrimSpace(r.Channel) == "" || strings.TrimSpace(r.ChatID) == "" ||
		strings.TrimSpace(r.SenderID) == "" {
		return fmt.Errorf(
			"%w: route requires agent, session, channel, chat, and sender",
			ErrInvalidInteraction,
		)
	}
	return nil
}

func (o Origin) validate() error {
	if strings.TrimSpace(o.TurnID) == "" || strings.TrimSpace(o.ToolCallID) == "" ||
		strings.TrimSpace(o.ToolName) == "" {
		return fmt.Errorf("%w: origin requires turn, tool call, and tool", ErrInvalidInteraction)
	}
	return nil
}

func validateQuestions(kind Kind, questions []Question) error {
	if kind == KindQuestion && (len(questions) == 0 || len(questions) > MaxQuestions) {
		return fmt.Errorf(
			"%w: questions must contain 1 to %d entries",
			ErrInvalidInteraction,
			MaxQuestions,
		)
	}
	if kind == KindApproval && len(questions) != 0 {
		return fmt.Errorf("%w: approval prompts are policy-owned", ErrInvalidInteraction)
	}
	seen := make(map[string]struct{}, len(questions))
	for i, question := range questions {
		if !validBoundedString(question.ID, MaxQuestionIDLength) ||
			!questionIDPattern.MatchString(question.ID) {
			return fmt.Errorf("%w: question %d has invalid id", ErrInvalidInteraction, i)
		}
		if _, ok := seen[question.ID]; ok {
			return fmt.Errorf("%w: duplicate question id %q", ErrInvalidInteraction, question.ID)
		}
		seen[question.ID] = struct{}{}
		if !validBoundedString(question.Header, MaxHeaderLength) ||
			strings.TrimSpace(question.Question) == "" ||
			!validBoundedString(
				question.Question,
				MaxQuestionLength,
			) || len(question.Options) > MaxOptions || len(question.Options) == 1 {
			return fmt.Errorf("%w: question %q exceeds bounds", ErrInvalidInteraction, question.ID)
		}
		optionLabels := make(map[string]struct{}, len(question.Options))
		for _, option := range question.Options {
			if strings.TrimSpace(option.Label) == "" ||
				!validBoundedString(option.Label, MaxOptionLabelLength) ||
				!validBoundedString(option.Description, MaxDescriptionLength) {
				return fmt.Errorf(
					"%w: question %q has invalid option",
					ErrInvalidInteraction,
					question.ID,
				)
			}
			label := strings.ToLower(strings.TrimSpace(option.Label))
			if _, ok := optionLabels[label]; ok {
				return fmt.Errorf(
					"%w: question %q has duplicate option %q",
					ErrInvalidInteraction,
					question.ID,
					option.Label,
				)
			}
			optionLabels[label] = struct{}{}
		}
	}
	return nil
}

// ValidateSuspensionRequest validates model- or policy-produced prompt data.
// Trusted route and origin data are deliberately absent from this contract.
func ValidateSuspensionRequest(request SuspensionRequest) error {
	if request.Kind != KindQuestion && request.Kind != KindApproval {
		return fmt.Errorf("%w: unsupported suspension kind %q", ErrInvalidInteraction, request.Kind)
	}
	if err := validateQuestions(request.Kind, request.Questions); err != nil {
		return err
	}
	if !validBoundedString(request.PromptSummary, MaxSummaryLength) {
		return fmt.Errorf("%w: prompt summary exceeds bounds", ErrInvalidInteraction)
	}
	if request.Timeout < time.Minute || request.Timeout > 24*time.Hour {
		return fmt.Errorf("%w: timeout must be between 1 minute and 24 hours", ErrInvalidInteraction)
	}
	return nil
}

func validBoundedString(value string, maxRunes int) bool {
	return utf8.ValidString(value) && utf8.RuneCountInString(value) <= maxRunes
}

func isTerminal(status Status) bool {
	switch status {
	case StatusResolved, StatusCancelled, StatusFailed:
		return true
	default:
		return false
	}
}

func validTransition(from, to Status) bool {
	switch from {
	case StatusCreated:
		return to == StatusWaiting || to == StatusCancelled || to == StatusFailed
	case StatusWaiting:
		return to == StatusClaimed || to == StatusCancelled || to == StatusFailed
	case StatusClaimed:
		return to == StatusResuming || to == StatusCancelled || to == StatusFailed
	case StatusResuming:
		return to == StatusResolved || to == StatusCancelled || to == StatusFailed
	default:
		return false
	}
}
