package protocol

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	ok := true
	tests := []Envelope{
		{Type: FrameRequest, ID: "req_1", Method: "node.invoke", Params: json.RawMessage(`{"command":"node.info.v1"}`)},
		{Type: FrameResponse, ID: "req_1", OK: &ok, Result: json.RawMessage(`{"healthy":true}`)},
		{Type: FrameEvent, Event: "node.invoke.progress", Payload: json.RawMessage(`{"percent":50}`)},
	}

	for _, envelope := range tests {
		data, err := Encode(envelope)
		if err != nil {
			t.Fatalf("Encode(%s) error = %v", envelope.Type, err)
		}
		decoded, err := Decode(data)
		if err != nil {
			t.Fatalf("Decode(%s) error = %v", envelope.Type, err)
		}
		if decoded.Type != envelope.Type {
			t.Fatalf("decoded type = %q, want %q", decoded.Type, envelope.Type)
		}
	}
}

func TestDecodeRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "unknown field", data: `{"type":"event","event":"node.heartbeat","payload":{},"extra":true}`},
		{name: "duplicate type", data: `{"type":"request","type":"event","event":"node.ready","payload":{}}`},
		{
			name: "duplicate nested member",
			data: `{"type":"event","event":"node.ready",` +
				`"payload":{"state":"one","state":"two"}}`,
		},
		{
			name: "mixed shape",
			data: `{"type":"request","id":"req_1","method":"node.info",` +
				`"params":{},"event":"node.ready"}`,
		},
		{name: "failed without error", data: `{"type":"response","id":"req_1","ok":false}`},
		{
			name: "success with error",
			data: `{"type":"response","id":"req_1","ok":true,"result":{},` +
				`"error":{"code":"FAILED","message":"no"}}`,
		},
		{name: "event array payload", data: `{"type":"event","event":"node.ready","payload":[]}`},
		{name: "explicit empty foreign field", data: `{"type":"event","event":"node.ready","payload":{},"id":""}`},
		{
			name: "empty idempotency key",
			data: `{"type":"request","id":"req_1","method":"node.info",` +
				`"params":{},"idempotency_key":""}`,
		},
		{name: "null error on success", data: `{"type":"response","id":"req_1","ok":true,"result":{},"error":null}`},
		{name: "trailing value", data: `{"type":"event","event":"node.ready","payload":{}} {}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Decode([]byte(test.data)); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("Decode() error = %v", err)
			}
		})
	}
}

func TestEncodeRejectsDuplicateRawMessageMembers(t *testing.T) {
	envelope := Envelope{
		Type:   FrameRequest,
		ID:     "req_1",
		Method: "node.invoke",
		Params: json.RawMessage(`{"command":"one","command":"two"}`),
	}
	if _, err := Encode(envelope); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("Encode() error = %v", err)
	}
}

func TestDecodeRejectsOversizedFrame(t *testing.T) {
	data := []byte(strings.Repeat("x", MaxFrameSize+1))
	if _, err := Decode(data); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Decode() error = %v", err)
	}
}

func TestFailedResponseErrorContract(t *testing.T) {
	ok := false
	envelope := Envelope{
		Type:  FrameResponse,
		ID:    "req_2",
		OK:    &ok,
		Error: &Error{Code: "POLICY_DENIED", Message: "not allowed"},
	}
	if _, err := Encode(envelope); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	envelope.Error.Code = "not-valid"
	if _, err := Encode(envelope); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("Encode() error = %v", err)
	}
}
