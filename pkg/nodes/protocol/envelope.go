package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/sipeed/picoclaw/pkg/nodes/internal/jsonstrict"
)

const (
	VersionV1    = 1
	MaxFrameSize = 1024 * 1024
	MaxIDLength  = 128
)

var (
	ErrFrameTooLarge = errors.New("node protocol frame exceeds size limit")
	ErrInvalidFrame  = errors.New("invalid node protocol frame")

	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	methodPattern     = regexp.MustCompile(`^[a-z][a-z0-9_.-]*$`)
	errorCodePattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
)

type FrameType string

const (
	FrameRequest  FrameType = "request"
	FrameResponse FrameType = "response"
	FrameEvent    FrameType = "event"
)

type Error struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Details json.RawMessage `json:"details,omitempty"`
}

type Envelope struct {
	Type           FrameType       `json:"type"`
	ID             string          `json:"id,omitempty"`
	Method         string          `json:"method,omitempty"`
	Params         json.RawMessage `json:"params,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	OK             *bool           `json:"ok,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          *Error          `json:"error,omitempty"`
	Event          string          `json:"event,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

func Decode(data []byte) (Envelope, error) {
	if len(data) > MaxFrameSize {
		return Envelope{}, ErrFrameTooLarge
	}
	wireValue, err := jsonstrict.Decode(data)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidFrame, err)
	}
	if err := validateWireShape(wireValue); err != nil {
		return Envelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidFrame, err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Envelope{}, err
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func Encode(envelope Envelope) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode node protocol frame: %w", err)
	}
	if len(data) > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	return data, nil
}

func (envelope Envelope) Validate() error {
	switch envelope.Type {
	case FrameRequest:
		return envelope.validateRequest()
	case FrameResponse:
		return envelope.validateResponse()
	case FrameEvent:
		return envelope.validateEvent()
	default:
		return fmt.Errorf("%w: unsupported frame type %q", ErrInvalidFrame, envelope.Type)
	}
}

func (envelope Envelope) validateRequest() error {
	if !validIdentifier(envelope.ID) || !methodPattern.MatchString(envelope.Method) {
		return fmt.Errorf("%w: request requires a valid id and method", ErrInvalidFrame)
	}
	if !validOptionalIdentifier(envelope.IdempotencyKey) {
		return fmt.Errorf("%w: malformed idempotency key", ErrInvalidFrame)
	}
	if err := validateJSONObject("params", envelope.Params, true); err != nil {
		return err
	}
	if envelope.OK != nil || len(envelope.Result) != 0 || envelope.Error != nil ||
		envelope.Event != "" || len(envelope.Payload) != 0 {
		return fmt.Errorf("%w: request contains fields from another frame type", ErrInvalidFrame)
	}
	return nil
}

func (envelope Envelope) validateResponse() error {
	if !validIdentifier(envelope.ID) || envelope.OK == nil {
		return fmt.Errorf("%w: response requires a valid id and ok", ErrInvalidFrame)
	}
	if envelope.Method != "" || len(envelope.Params) != 0 || envelope.IdempotencyKey != "" ||
		envelope.Event != "" || len(envelope.Payload) != 0 {
		return fmt.Errorf("%w: response contains fields from another frame type", ErrInvalidFrame)
	}
	if *envelope.OK {
		if envelope.Error != nil {
			return fmt.Errorf("%w: successful response contains an error", ErrInvalidFrame)
		}
		return validateJSONValue("result", envelope.Result, false)
	}
	if len(envelope.Result) != 0 || envelope.Error == nil {
		return fmt.Errorf("%w: failed response requires only an error", ErrInvalidFrame)
	}
	return envelope.Error.Validate()
}

func (envelope Envelope) validateEvent() error {
	if !methodPattern.MatchString(envelope.Event) {
		return fmt.Errorf("%w: event requires a valid name", ErrInvalidFrame)
	}
	if err := validateJSONObject("payload", envelope.Payload, true); err != nil {
		return err
	}
	if envelope.ID != "" || envelope.Method != "" || len(envelope.Params) != 0 ||
		envelope.IdempotencyKey != "" || envelope.OK != nil || len(envelope.Result) != 0 ||
		envelope.Error != nil {
		return fmt.Errorf("%w: event contains fields from another frame type", ErrInvalidFrame)
	}
	return nil
}

func (protocolError Error) Validate() error {
	if !errorCodePattern.MatchString(protocolError.Code) || protocolError.Message == "" {
		return fmt.Errorf("%w: malformed protocol error", ErrInvalidFrame)
	}
	return validateJSONValue("error details", protocolError.Details, true)
}

func validIdentifier(value string) bool {
	return len(value) > 0 && len(value) <= MaxIDLength && identifierPattern.MatchString(value)
}

func validOptionalIdentifier(value string) bool {
	return value == "" || validIdentifier(value)
}

func validateWireShape(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: frame must be an object", ErrInvalidFrame)
	}
	frameType, ok := object["type"].(string)
	if !ok {
		return fmt.Errorf("%w: frame requires a string type", ErrInvalidFrame)
	}
	var required, foreign map[string]struct{}
	switch FrameType(frameType) {
	case FrameRequest:
		required = fieldSet("type", "id", "method", "params")
		foreign = fieldSet("ok", "result", "error", "event", "payload")
		if key, present := object["idempotency_key"]; present {
			text, isString := key.(string)
			if !isString || text == "" {
				return fmt.Errorf("%w: idempotency key must be a non-empty string", ErrInvalidFrame)
			}
		}
	case FrameResponse:
		required = fieldSet("type", "id", "ok")
		foreign = fieldSet("method", "params", "idempotency_key", "event", "payload")
		okValue, isBool := object["ok"].(bool)
		if !isBool {
			return fmt.Errorf("%w: response requires a boolean ok", ErrInvalidFrame)
		}
		_, hasResult := object["result"]
		_, hasError := object["error"]
		if okValue && (!hasResult || hasError) {
			return fmt.Errorf("%w: successful response requires only a result", ErrInvalidFrame)
		}
		if !okValue && (hasResult || !hasError) {
			return fmt.Errorf("%w: failed response requires only an error", ErrInvalidFrame)
		}
	case FrameEvent:
		required = fieldSet("type", "event", "payload")
		foreign = fieldSet("id", "method", "params", "idempotency_key", "ok", "result", "error")
	default:
		return fmt.Errorf("%w: unsupported frame type %q", ErrInvalidFrame, frameType)
	}
	for field := range foreign {
		if _, exists := object[field]; exists {
			return fmt.Errorf("%w: field %q is not valid for %s", ErrInvalidFrame, field, frameType)
		}
	}
	for field := range required {
		if _, exists := object[field]; !exists {
			return fmt.Errorf("%w: missing %s", ErrInvalidFrame, field)
		}
	}
	return nil
}

func fieldSet(fields ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		result[field] = struct{}{}
	}
	return result
}

func validateJSONObject(label string, raw json.RawMessage, required bool) error {
	if len(raw) == 0 {
		if !required {
			return nil
		}
		return fmt.Errorf("%w: missing %s", ErrInvalidFrame, label)
	}
	value, err := jsonstrict.Decode(raw)
	if err != nil {
		return fmt.Errorf("%w: malformed %s: %v", ErrInvalidFrame, label, err)
	}
	if _, ok := value.(map[string]any); !ok {
		return fmt.Errorf("%w: %s must be an object", ErrInvalidFrame, label)
	}
	return nil
}

func validateJSONValue(label string, raw json.RawMessage, optional bool) error {
	if len(raw) == 0 {
		if optional {
			return nil
		}
		return fmt.Errorf("%w: missing %s", ErrInvalidFrame, label)
	}
	if _, err := jsonstrict.Decode(raw); err != nil {
		return fmt.Errorf("%w: malformed %s: %v", ErrInvalidFrame, label, err)
	}
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data", ErrInvalidFrame)
	}
	return nil
}
