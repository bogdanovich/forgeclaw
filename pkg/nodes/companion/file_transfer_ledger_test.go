package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestFileTransferLedgerPersistsExactBindingAndState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transfers.json")
	clock := time.Unix(100, 0)
	ledger := newFileTransferLedger(
		path,
		DefaultFileTransferLedgerLimit,
		DefaultFileTransferLedgerBytes,
		func() time.Time { return clock },
	)
	record := testFileTransferRecord(clock)
	accepted, existing, err := ledger.Accept(record)
	if err != nil {
		t.Fatal(err)
	}
	if existing || accepted.State != FileTransferAccepted {
		t.Fatalf("Accept() = %#v, existing %v", accepted, existing)
	}
	clock = clock.Add(time.Second)
	streaming, err := ledger.Transition(record.TransferID, func(
		current *FileTransferRecord,
		_ time.Time,
	) error {
		current.State = FileTransferStreaming
		current.Sequence = 1
		current.ObservedBytes = current.TotalSize
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if streaming.State != FileTransferStreaming ||
		streaming.Sequence != 1 ||
		streaming.ObservedBytes != record.TotalSize {
		t.Fatalf("streaming record = %#v", streaming)
	}
	for _, update := range []func(*FileTransferRecord){
		func(current *FileTransferRecord) {
			current.State = FileTransferAccepted
		},
		func(current *FileTransferRecord) {
			current.Path += ".changed"
		},
		func(current *FileTransferRecord) {
			current.Sequence = 0
		},
	} {
		if _, err := ledger.Transition(record.TransferID, func(
			current *FileTransferRecord,
			_ time.Time,
		) error {
			update(current)
			return nil
		}); !errors.Is(err, ErrFileTransferConflict) {
			t.Fatalf("invalid transition error = %v", err)
		}
	}

	reloaded := newFileTransferLedger(
		path,
		DefaultFileTransferLedgerLimit,
		DefaultFileTransferLedgerBytes,
		func() time.Time { return clock },
	)
	if err := reloaded.load(); err != nil {
		t.Fatal(err)
	}
	got, found, err := reloaded.Lookup(record.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !sameFileTransferBinding(got, record) ||
		got.State != FileTransferStreaming ||
		got.Sequence != 1 {
		t.Fatalf("reloaded record = %#v, found %v", got, found)
	}
}

func TestFileTransferLedgerRejectsConflictingDuplicate(t *testing.T) {
	now := time.Unix(100, 0)
	ledger := newFileTransferLedger("", 8, 1024*1024, func() time.Time {
		return now
	})
	record := testFileTransferRecord(now)
	if _, _, err := ledger.Accept(record); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*FileTransferRecord){
		func(value *FileTransferRecord) { value.Path += ".other" },
		func(value *FileTransferRecord) { value.PolicyRevision = "project-v2" },
		func(value *FileTransferRecord) { value.Publication = filePublicationReplace },
		func(value *FileTransferRecord) { value.TotalSize++ },
		func(value *FileTransferRecord) {
			digest := sha256.Sum256([]byte("other"))
			value.SHA256 = hex.EncodeToString(digest[:])
		},
	} {
		changed := record
		mutate(&changed)
		if _, _, err := ledger.Accept(changed); !errors.Is(
			err,
			ErrFileTransferConflict,
		) {
			t.Fatalf("conflicting Accept() error = %v", err)
		}
	}
}

func TestFileTransferLedgerNeverPrunesLiveIdentity(t *testing.T) {
	now := time.Unix(100, 0)
	ledger := newFileTransferLedger("", 1, 1024*1024, func() time.Time {
		return now
	})
	first := testFileTransferRecord(now)
	if _, _, err := ledger.Accept(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.TransferID = "transfer_2"
	if _, _, err := ledger.Accept(second); !errors.Is(
		err,
		ErrFileTransferLedgerFull,
	) {
		t.Fatalf("second live Accept() error = %v", err)
	}
}

func TestFileTransferLedgerPrunesOnlyAfterBoundedRetention(t *testing.T) {
	clock := time.Unix(100, 0)
	ledger := newFileTransferLedger("", 2, 1024*1024, func() time.Time {
		return clock
	})
	first := testFileTransferRecord(clock)
	if _, _, err := ledger.Accept(first); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transition(first.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferCanceled
		record.FailureCode = "CANCELED"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(DefaultFileTransferRetention - time.Second)
	second := testFileTransferRecord(clock)
	second.TransferID = "transfer_2"
	if _, _, err := ledger.Accept(second); err != nil {
		t.Fatal(err)
	}
	if _, found, err := ledger.Lookup(first.TransferID); err != nil || !found {
		t.Fatalf("retained Lookup() = found %v, error %v", found, err)
	}
	if _, err := ledger.Transition(second.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferCanceled
		record.FailureCode = "CANCELED"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Second)
	third := testFileTransferRecord(clock)
	third.TransferID = "transfer_3"
	if _, _, err := ledger.Accept(third); err != nil {
		t.Fatal(err)
	}
	if _, found, err := ledger.Lookup(first.TransferID); err != nil || found {
		t.Fatalf("expired Lookup() = found %v, error %v", found, err)
	}
	if _, found, err := ledger.Lookup(second.TransferID); err != nil || !found {
		t.Fatalf("recent Lookup() = found %v, error %v", found, err)
	}
}

func testFileTransferRecord(now time.Time) FileTransferRecord {
	content := []byte("payload")
	digest := sha256.Sum256(content)
	return FileTransferRecord{
		TransferID:     "transfer_1",
		Direction:      protocol.TransferUpload,
		Operation:      fileOperationUpload,
		ProfileAlias:   "project",
		PolicyRevision: "project-v1",
		Path:           "/project/config.txt",
		Publication:    filePublicationCreate,
		TotalSize:      uint64(len(content)),
		SHA256:         hex.EncodeToString(digest[:]),
		ExpiresAt:      now.Add(time.Hour).Unix(),
		State:          FileTransferAccepted,
		StageName:      ".mintclaw-transfer-stage.tmp",
		StageIdentity:  fileIdentity{Device: 1, Inode: 2, Links: 1},
		CreatedAt:      now.UnixNano(),
		UpdatedAt:      now.UnixNano(),
	}
}
