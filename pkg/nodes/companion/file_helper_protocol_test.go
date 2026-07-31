//go:build linux || darwin

package companion

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestFileHelperProtocolRoundTripsSnapshotAndBinaryTransfer(t *testing.T) {
	runtime, _, _ := newTestFileTransferRuntime(t)
	serviceDigest := strings.Repeat("a", sha256.Size*2)
	snapshotPayload, err := encodeFileHelperSnapshot(runtime.Descriptors(), serviceDigest)
	if err != nil {
		t.Fatal(err)
	}
	transfer := protocol.TransferFrame{
		Type:           protocol.TransferFrameChunk,
		Direction:      protocol.TransferUpload,
		TransferID:     "helper_protocol",
		PolicyRevision: "project-v1",
		Sequence:       1,
		TotalSize:      4,
		SHA256:         sha256.Sum256([]byte("data")),
		Payload:        []byte{0, 1, 2, 0},
	}
	transferPayload, err := protocol.EncodeTransferFrame(transfer)
	if err != nil {
		t.Fatal(err)
	}
	transferRequest, err := encodeFileHelperTransferRequest(serviceDigest, "project", transfer)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []fileHelperMessage{
		{Kind: fileHelperSnapshotRequest},
		{Kind: fileHelperSnapshotResponse, Payload: snapshotPayload},
		{Kind: fileHelperTransferRequest, Payload: transferRequest},
		{Kind: fileHelperTransferResponse, Payload: transferPayload},
		{Kind: fileHelperErrorResponse, Payload: []byte("INVALID_REQUEST")},
	} {
		var encoded bytes.Buffer
		if err := writeFileHelperMessage(&encoded, message); err != nil {
			t.Fatal(err)
		}
		decoded, err := readFileHelperMessage(&encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Kind != message.Kind || !bytes.Equal(decoded.Payload, message.Payload) {
			t.Fatalf("decoded message = %#v, want %#v", decoded, message)
		}
	}
}

func TestFileHelperProtocolRejectsMalformedAndOversizedRequests(t *testing.T) {
	tests := []fileHelperMessage{
		{Kind: fileHelperSnapshotRequest, Payload: []byte("unexpected")},
		{Kind: fileHelperTransferRequest, Payload: []byte("not-a-transfer-frame")},
		{Kind: fileHelperErrorResponse, Payload: []byte("unsafe error text")},
		{Kind: 99, Payload: nil},
	}
	for _, message := range tests {
		if err := writeFileHelperMessage(new(bytes.Buffer), message); err == nil {
			t.Fatalf("write accepted invalid message %#v", message)
		}
	}
	var header [fileHelperHeaderBytes]byte
	copy(header[:4], fileHelperMagic[:])
	header[4] = FileHelperProtocolVersion
	header[5] = byte(fileHelperTransferRequest)
	binary.BigEndian.PutUint32(header[8:12], MaxFileHelperMessageBytes+1)
	if _, err := readFileHelperMessage(bytes.NewReader(header[:])); err == nil {
		t.Fatal("oversized file helper request was accepted")
	}
	if _, err := encodeFileHelperSnapshot(nil, strings.Repeat("a", sha256.Size*2)); err == nil {
		t.Fatal("empty file helper snapshot was accepted")
	}
	if _, err := encodeFileHelperTransferRequest("stale", "project", protocol.TransferFrame{}); err == nil {
		t.Fatal("invalid service digest was accepted")
	}
	if _, err := decodeFileHelperSnapshot([]byte(
		`{"profile":{},"profile":{}}`,
	)); err == nil {
		t.Fatal("duplicate snapshot fields were accepted")
	}
	if err := writeFileHelperMessage(new(bytes.Buffer), fileHelperMessage{
		Kind:    fileHelperErrorResponse,
		Payload: []byte(strings.Repeat("A", 65)),
	}); err == nil {
		t.Fatal("oversized helper error code was accepted")
	}
}
