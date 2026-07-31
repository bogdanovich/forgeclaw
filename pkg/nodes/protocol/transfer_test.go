package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func TestTransferFrameRoundTrip(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("payload"))
	tests := []TransferFrame{
		{
			Type: TransferFramePrepare, Direction: TransferUpload,
			TransferID: "transfer_1", PolicyRevision: "policy-v1",
			TotalSize: 7, SHA256: digest, Payload: []byte(`{"path":"project/file"}`),
		},
		{
			Type: TransferFrameChunk, Direction: TransferDownload,
			TransferID: "transfer_2", PolicyRevision: "policy-v2",
			Sequence: 1, TotalSize: 7, SHA256: digest, Payload: []byte("payload"),
		},
		{
			Type: TransferFrameCommitted, Direction: TransferUpload,
			TransferID: "transfer_3", PolicyRevision: "policy-v3",
			TotalSize: 7, SHA256: digest, Payload: []byte(`{"state":"published"}`),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.TransferID, func(t *testing.T) {
			t.Parallel()
			data, err := EncodeTransferFrame(test)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeTransferFrame(data)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Type != test.Type ||
				decoded.Direction != test.Direction ||
				decoded.TransferID != test.TransferID ||
				decoded.PolicyRevision != test.PolicyRevision ||
				decoded.Sequence != test.Sequence ||
				decoded.TotalSize != test.TotalSize ||
				decoded.SHA256 != test.SHA256 ||
				string(decoded.Payload) != string(test.Payload) {
				t.Fatalf("decoded frame mismatch: %#v", decoded)
			}
		})
	}
}

func TestTransferFrameRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("payload"))
	valid := TransferFrame{
		Type: TransferFrameChunk, Direction: TransferUpload,
		TransferID: "transfer_1", PolicyRevision: "policy-v1",
		Sequence: 1, TotalSize: 7, SHA256: digest, Payload: []byte("payload"),
	}
	tests := map[string]func(*TransferFrame){
		"unknown type":       func(frame *TransferFrame) { frame.Type = 99 },
		"unknown direction":  func(frame *TransferFrame) { frame.Direction = 99 },
		"missing sequence":   func(frame *TransferFrame) { frame.Sequence = 0 },
		"empty chunk":        func(frame *TransferFrame) { frame.Payload = nil },
		"oversized chunk":    func(frame *TransferFrame) { frame.Payload = make([]byte, MaxTransferChunkBytes+1) },
		"oversized file":     func(frame *TransferFrame) { frame.TotalSize = MaxTransferFileBytes + 1 },
		"malformed identity": func(frame *TransferFrame) { frame.TransferID = "../escape" },
		"zero digest":        func(frame *TransferFrame) { frame.SHA256 = [32]byte{} },
	}
	for name, mutate := range tests {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			frame := valid
			mutate(&frame)
			if _, err := EncodeTransferFrame(frame); !errors.Is(err, ErrInvalidTransferFrame) {
				t.Fatalf("EncodeTransferFrame() error = %v", err)
			}
		})
	}
}

func TestDecodeTransferFrameRejectsLengthAndHeaderTampering(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("payload"))
	data, err := EncodeTransferFrame(TransferFrame{
		Type: TransferFrameChunk, Direction: TransferUpload,
		TransferID: "transfer_1", PolicyRevision: "policy-v1",
		Sequence: 1, TotalSize: 7, SHA256: digest, Payload: []byte("payload"),
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]byte{
		"truncated": data[:len(data)-1],
		"appended":  append(append([]byte(nil), data...), 0),
		"bad magic": append([]byte("FAIL"), data[4:]...),
		"bad version": func() []byte {
			changed := append([]byte(nil), data...)
			changed[4]++
			return changed
		}(),
		"reserved bit": func() []byte {
			changed := append([]byte(nil), data...)
			changed[7] = 1
			return changed
		}(),
		"oversized payload length": func() []byte {
			baseLength := transferHeaderBytes + len("transfer_1") + len("policy-v1")
			changed := make([]byte, baseLength+MaxTransferChunkBytes+1)
			copy(changed, data[:baseLength])
			binary.BigEndian.PutUint32(changed[28:32], MaxTransferChunkBytes+1)
			return changed
		}(),
	}
	for name, malformed := range tests {
		malformed := malformed
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeTransferFrame(malformed); !errors.Is(err, ErrInvalidTransferFrame) {
				t.Fatalf("DecodeTransferFrame() error = %v", err)
			}
		})
	}
}
