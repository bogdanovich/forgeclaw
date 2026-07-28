package companion

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestAuthorityBrokerProtocolRoundTrip(t *testing.T) {
	frame := authorityBrokerRequestFrame{
		Version: AuthorityBrokerProtocolVersion,
		Action:  authorityBrokerActionSnapshot,
	}
	var encoded bytes.Buffer
	if err := writeAuthorityBrokerFrame(&encoded, frame); err != nil {
		t.Fatal(err)
	}
	var decoded authorityBrokerRequestFrame
	if err := readAuthorityBrokerFrame(&encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Version != frame.Version || decoded.Action != frame.Action {
		t.Fatalf("decoded frame = %#v", decoded)
	}
}

func TestAuthorityBrokerProtocolRejectsInvalidFrames(t *testing.T) {
	var oversized bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxAuthorityBrokerFrameBytes+1)
	oversized.Write(header[:])
	if err := readAuthorityBrokerFrame(&oversized, new(authorityBrokerRequestFrame)); err == nil {
		t.Fatal("oversized frame accepted")
	}
	var unknown bytes.Buffer
	if err := writeAuthorityBrokerFrame(&unknown, map[string]any{
		"version": AuthorityBrokerProtocolVersion,
		"action":  authorityBrokerActionSnapshot,
		"secret":  "not allowed",
	}); err != nil {
		t.Fatal(err)
	}
	if err := readAuthorityBrokerFrame(&unknown, new(authorityBrokerRequestFrame)); err == nil {
		t.Fatal("unknown field accepted")
	}
	var duplicate bytes.Buffer
	duplicatePayload := []byte(`{"version":1,"action":"snapshot","action":"execute"}`)
	binary.BigEndian.PutUint32(header[:], uint32(len(duplicatePayload)))
	duplicate.Write(header[:])
	duplicate.Write(duplicatePayload)
	if err := readAuthorityBrokerFrame(&duplicate, new(authorityBrokerRequestFrame)); err == nil {
		t.Fatal("duplicate field accepted")
	}
	if err := writeAuthorityBrokerFrame(
		&bytes.Buffer{},
		strings.Repeat("x", MaxAuthorityBrokerFrameBytes),
	); err == nil {
		t.Fatal("oversized encoded value accepted")
	}
}

func TestAuthorityBrokerProtocolRejectsGeneralActions(t *testing.T) {
	for _, frame := range []authorityBrokerRequestFrame{
		{Version: 2, Action: authorityBrokerActionSnapshot},
		{Version: AuthorityBrokerProtocolVersion, Action: "file.read"},
		{Version: AuthorityBrokerProtocolVersion, Action: authorityBrokerActionExecute},
		{
			Version: AuthorityBrokerProtocolVersion,
			Action:  authorityBrokerActionSnapshot,
			Execute: &ShellBrokerRequest{},
		},
	} {
		if err := validateAuthorityBrokerRequestFrame(frame); err == nil {
			t.Fatalf("invalid frame accepted: %#v", frame)
		}
	}
}
