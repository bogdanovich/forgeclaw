package nodes

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

func TestGatewayTransferSpoolCommitResolveAndRelease(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	store := openTestTransferSpool(t, now, 0, 0)
	owner := testTransferOwner("actor-a")
	content := []byte("bounded transfer payload")
	spec := testTransferSpec(content, now)

	writer, staged, created, err := store.Begin(owner, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !created || staged.State != TransferArtifactStaging {
		t.Fatalf("Begin() = created %v, record %#v", created, staged)
	}
	if err := writer.WriteChunk(1, content[:8]); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(2, content[8:]); err != nil {
		t.Fatal(err)
	}
	committed, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != TransferArtifactCommitted ||
		committed.ObservedSize != int64(len(content)) ||
		committed.StagingName != "" {
		t.Fatalf("Commit() record = %#v", committed)
	}

	file, resolved, err := store.Resolve(owner, spec, committed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) || resolved != committed {
		t.Fatalf("Resolve() = %q, %#v", got, resolved)
	}
	if err := store.Release(owner, spec, committed.Ref); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Resolve(
		owner,
		spec,
		committed.Ref,
	); !errors.Is(err, ErrTransferArtifactNotFound) {
		t.Fatalf("Resolve() after release error = %v", err)
	}
}

func TestGatewayTransferSpoolBindsOwnerAndTransferIdentity(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	store := openTestTransferSpool(t, now, 0, 0)
	owner := testTransferOwner("actor-a")
	content := []byte("payload")
	spec := testTransferSpec(content, now)
	writer, _, _, err := store.Begin(owner, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	committed, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := store.Resolve(
		testTransferOwner("actor-b"),
		spec,
		committed.Ref,
	); !errors.Is(err, ErrTransferArtifactNotFound) {
		t.Fatalf("cross-owner Resolve() error = %v", err)
	}
	duplicateWriter, duplicate, created, err := store.Begin(owner, spec)
	if err != nil {
		t.Fatal(err)
	}
	if duplicateWriter != nil || created || duplicate.Ref != committed.Ref {
		t.Fatalf("duplicate Begin() = writer %v, created %v, record %#v", duplicateWriter, created, duplicate)
	}
	changed := spec
	changed.Filename = "changed.bin"
	if _, _, _, err := store.Begin(owner, changed); !errors.Is(err, ErrTransferArtifactConflict) {
		t.Fatalf("changed Begin() error = %v", err)
	}
	if _, _, err := store.Resolve(owner, changed, committed.Ref); !errors.Is(
		err,
		ErrTransferArtifactNotFound,
	) {
		t.Fatalf("changed Resolve() error = %v", err)
	}
	if err := store.Release(owner, changed, committed.Ref); !errors.Is(
		err,
		ErrTransferArtifactNotFound,
	) {
		t.Fatalf("changed Release() error = %v", err)
	}
}

func TestGatewayTransferSpoolRetainsOneDeliveryClaimAcrossRestart(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	root := filepath.Join(t.TempDir(), "spool")
	store, err := newGatewayTransferSpool(root, 8, 1024*1024, time.Hour, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := testTransferOwner("actor-a")
	content := []byte("delivery payload")
	spec := testTransferSpec(content, now)
	writer, _, _, err := store.Begin(owner, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	committed, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	claimed, first, err := store.ClaimDelivery(
		owner,
		committed.Ref,
		"media://node-transfer-test",
		"delivery_test",
	)
	if err != nil || !first || claimed.DeliveryAt == 0 {
		t.Fatalf("ClaimDelivery() = (%#v, %v, %v)", claimed, first, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := newGatewayTransferSpool(root, 8, 1024*1024, time.Hour, func() time.Time {
		return now.Add(time.Minute)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	file, retained, err := restarted.ResolveOwned(owner, committed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if retained.DeliveryAt != claimed.DeliveryAt ||
		retained.MediaRef != claimed.MediaRef ||
		retained.DeliveryKey != claimed.DeliveryKey {
		t.Fatalf("restarted delivery claim = %#v, want %#v", retained, claimed)
	}
	duplicate, second, err := restarted.ClaimDelivery(
		owner,
		committed.Ref,
		claimed.MediaRef,
		claimed.DeliveryKey,
	)
	if err != nil || second || duplicate.DeliveryAt != claimed.DeliveryAt {
		t.Fatalf("duplicate ClaimDelivery() = (%#v, %v, %v)", duplicate, second, err)
	}
	otherOwner := owner
	otherOwner.RouteID = "other-route"
	if _, _, err := restarted.ClaimDelivery(
		otherOwner,
		committed.Ref,
		claimed.MediaRef,
		claimed.DeliveryKey,
	); !errors.Is(err, ErrTransferArtifactNotFound) {
		t.Fatalf("cross-route ClaimDelivery() error = %v", err)
	}
}

func TestGatewayTransferSpoolScopesTransferIDToOwnerAndTransferIdentity(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	store := openTestTransferSpool(t, now, 0, 0)
	content := []byte("payload")
	spec := testTransferSpec(content, now)
	first, _, _, err := store.Begin(testTransferOwner("actor-a"), spec)
	if err != nil {
		t.Fatal(err)
	}
	second, _, created, err := store.Begin(testTransferOwner("actor-b"), spec)
	if err != nil {
		t.Fatal(err)
	}
	if !created || second == nil {
		t.Fatal("same transfer ID in an independent owner scope was rejected")
	}
	if err := first.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := second.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayTransferSpoolRejectsSequenceSizeAndDigestMismatch(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	store := openTestTransferSpool(t, now, 0, 0)
	content := []byte("payload")
	writer, _, _, err := store.Begin(
		testTransferOwner("actor-a"),
		testTransferSpec(content, now),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(2, content); !errors.Is(err, ErrTransferChunkSequence) {
		t.Fatalf("WriteChunk(gap) error = %v", err)
	}
	if err := writer.WriteChunk(1, append(content, '!')); !errors.Is(err, ErrTransferSizeExceeded) {
		t.Fatalf("WriteChunk(oversize) error = %v", err)
	}
	if err := writer.WriteChunk(1, []byte("PAYLOAD")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Commit(); !errors.Is(err, ErrTransferDigestMismatch) {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := writer.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayTransferSpoolEnforcesQuotaAndRetention(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	current := now
	root := filepath.Join(t.TempDir(), "spool")
	store, err := newGatewayTransferSpool(root, 1, 7, time.Minute, func() time.Time {
		return current
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	content := []byte("payload")
	writer, _, _, err := store.Begin(
		testTransferOwner("actor-a"),
		testTransferSpec(content, now),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Begin(
		testTransferOwner("actor-b"),
		testTransferSpecWithID(content, now, "transfer_2"),
	); !errors.Is(err, ErrTransferSpoolFull) {
		t.Fatalf("Begin() over quota error = %v", err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	record, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	current = current.Add(time.Minute)
	removed, err := store.Cleanup()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("Cleanup() removed %d records", removed)
	}
	if _, _, err := store.Resolve(
		testTransferOwner("actor-a"),
		testTransferSpec(content, now),
		record.Ref,
	); !errors.Is(err, ErrTransferArtifactNotFound) {
		t.Fatalf("Resolve() after retention error = %v", err)
	}
}

func TestGatewayTransferSpoolRecoversPublicationAfterIndexFailure(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	root := filepath.Join(t.TempDir(), "spool")
	store, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	owner := testTransferOwner("actor-a")
	content := []byte("payload")
	writer, staged, _, err := store.Begin(owner, testTransferSpec(content, now))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	persistFailure := errors.New("injected index failure")
	store.writeIndex = func([]byte) error { return persistFailure }
	committed, err := writer.Commit()
	if committed.State != TransferArtifactCommitted ||
		!fileutil.IsCommittedWriteError(err) ||
		!errors.Is(err, persistFailure) {
		t.Fatalf("Commit() = %#v, %v", committed, err)
	}
	store.writeIndex = func(data []byte) error {
		return store.directory.writeFileAtomic(transferSpoolIndexName, data, 0o600)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time {
		return now.Add(time.Second)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	file, record, err := recovered.Resolve(
		owner,
		testTransferSpec(content, now),
		staged.Ref,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if record.State != TransferArtifactCommitted || record.Ref != staged.Ref {
		t.Fatalf("recovered record = %#v", record)
	}
}

func TestGatewayTransferSpoolRetainsCommittedIndexMutations(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	current := now
	store, err := newGatewayTransferSpool(
		filepath.Join(t.TempDir(), "spool"),
		0,
		0,
		time.Minute,
		func() time.Time { return current },
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	owner := testTransferOwner("actor-a")
	content := []byte("payload")
	spec := testTransferSpec(content, now)
	warning := errors.New("directory sync warning")

	restore := injectCommittedTransferIndexWarning(store, warning)
	writer, staged, created, beginErr := store.Begin(owner, spec)
	restore()
	if !created ||
		writer == nil ||
		!fileutil.IsCommittedWriteError(beginErr) ||
		!errors.Is(beginErr, warning) {
		t.Fatalf("Begin() = writer %v, created %v, error %v", writer, created, beginErr)
	}
	if store.records[staged.ArtifactID].State != TransferArtifactStaging {
		t.Fatal("committed Begin mutation was rolled back in memory")
	}
	if _, err := os.Stat(filepath.Join(store.root, staged.StagingName)); err != nil {
		t.Fatalf("committed Begin staging file: %v", err)
	}

	restore = injectCommittedTransferIndexWarning(store, warning)
	abortErr := writer.Abort()
	restore()
	if !fileutil.IsCommittedWriteError(abortErr) || !errors.Is(abortErr, warning) {
		t.Fatalf("Abort() error = %v", abortErr)
	}
	if _, found := store.records[staged.ArtifactID]; found {
		t.Fatal("committed Abort mutation was rolled back in memory")
	}
	if _, err := os.Stat(filepath.Join(store.root, staged.StagingName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("committed Abort staging file remains: %v", err)
	}

	writer, _, _, err = store.Begin(owner, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	committed, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	restore = injectCommittedTransferIndexWarning(store, warning)
	releaseErr := store.Release(owner, spec, committed.Ref)
	restore()
	if !fileutil.IsCommittedWriteError(releaseErr) || !errors.Is(releaseErr, warning) {
		t.Fatalf("Release() error = %v", releaseErr)
	}
	if _, found := store.records[committed.ArtifactID]; found {
		t.Fatal("committed Release mutation was rolled back in memory")
	}
	if _, err := os.Stat(filepath.Join(store.root, committed.DataName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("committed Release data file remains: %v", err)
	}

	cleanupSpec := testTransferSpecWithID(content, now, "transfer_cleanup")
	writer, _, _, err = store.Begin(owner, cleanupSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	cleanupRecord, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	current = current.Add(time.Minute)
	restore = injectCommittedTransferIndexWarning(store, warning)
	removed, cleanupErr := store.Cleanup()
	restore()
	if removed != 1 ||
		!fileutil.IsCommittedWriteError(cleanupErr) ||
		!errors.Is(cleanupErr, warning) {
		t.Fatalf("Cleanup() = removed %d, error %v", removed, cleanupErr)
	}
	if _, found := store.records[cleanupRecord.ArtifactID]; found {
		t.Fatal("committed Cleanup mutation was rolled back in memory")
	}
}

func TestGatewayTransferSpoolReconcilesAbandonedStaging(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	root := filepath.Join(t.TempDir(), "spool")
	store, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	writer, record, _, err := store.Begin(
		testTransferOwner("actor-a"),
		testTransferSpec([]byte("payload"), now),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, []byte("pay")); err != nil {
		t.Fatal(err)
	}
	if err := writer.file.Close(); err != nil {
		t.Fatal(err)
	}
	store.release()
	store.release = nil
	if err := store.directory.close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time {
		return now.Add(time.Second)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if _, _, err := recovered.Resolve(
		testTransferOwner("actor-a"),
		testTransferSpec([]byte("payload"), now),
		record.Ref,
	); !errors.Is(err, ErrTransferArtifactNotFound) {
		t.Fatalf("Resolve() abandoned staging error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, record.StagingName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned staging file remains: %v", err)
	}
}

func TestGatewayTransferSpoolReconcilesOrphansAndCommittedCorruption(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	root := filepath.Join(t.TempDir(), "spool")
	store, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	owner := testTransferOwner("actor-a")
	content := []byte("payload")
	spec := testTransferSpec(content, now)
	writer, _, _, err := store.Begin(owner, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	record, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, record.DataName), []byte("PAYLOAD"), 0o600); err != nil {
		t.Fatal(err)
	}
	orphanPart := "artifact_11111111111111111111111111111111.part"
	orphanData := "artifact_22222222222222222222222222222222.data"
	if err := os.WriteFile(filepath.Join(root, orphanPart), []byte("part"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, orphanData), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}

	recovered, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time {
		return now.Add(time.Second)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if _, _, err := recovered.Resolve(owner, spec, record.Ref); !errors.Is(
		err,
		ErrTransferArtifactNotFound,
	) {
		t.Fatalf("Resolve() corrupt committed record error = %v", err)
	}
	for _, name := range []string{record.DataName, orphanPart, orphanData} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reconciled file %q remains: %v", name, err)
		}
	}
}

func TestGatewayTransferSpoolEnforcesActiveTransferCeilings(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	perProfileStore := openTestTransferSpool(t, now, 0, 0)
	content := []byte("payload")
	for index := 0; index < MaxTargetProfileActiveTransfers; index++ {
		spec := testTransferSpecWithID(content, now, fmt.Sprintf("profile_%d", index))
		if _, _, _, err := perProfileStore.Begin(
			testTransferOwner(fmt.Sprintf("actor-%d", index)),
			spec,
		); err != nil {
			t.Fatal(err)
		}
	}
	blockedSpec := testTransferSpecWithID(content, now, "profile_blocked")
	if _, _, _, err := perProfileStore.Begin(
		testTransferOwner("actor-blocked"),
		blockedSpec,
	); !errors.Is(err, ErrTransferSpoolFull) {
		t.Fatalf("per-profile active ceiling error = %v", err)
	}

	globalStore := openTestTransferSpool(t, now, 0, 0)
	for index := 0; index < MaxGatewayActiveTransfers; index++ {
		spec := testTransferSpecWithID(content, now, fmt.Sprintf("global_%d", index))
		spec.Target = fmt.Sprintf("target_%d", index)
		if _, _, _, err := globalStore.Begin(
			testTransferOwner(fmt.Sprintf("actor-%d", index)),
			spec,
		); err != nil {
			t.Fatal(err)
		}
	}
	blockedSpec = testTransferSpecWithID(content, now, "global_blocked")
	blockedSpec.Target = "target_blocked"
	if _, _, _, err := globalStore.Begin(
		testTransferOwner("actor-blocked"),
		blockedSpec,
	); !errors.Is(err, ErrTransferSpoolFull) {
		t.Fatalf("gateway active ceiling error = %v", err)
	}
}

func TestGatewayTransferSpoolRejectsConcurrentOwnerAndLinkedRoot(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	root := filepath.Join(t.TempDir(), "spool")
	store, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newGatewayTransferSpool(root, 0, 0, 0, func() time.Time {
		return now
	}); !errors.Is(err, ErrTransferSpoolInUse) {
		t.Fatalf("second NewGatewayTransferSpool() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if runtime.GOOS == "windows" {
		return
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "linked-spool")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := newGatewayTransferSpool(link, 0, 0, 0, func() time.Time {
		return now
	}); err == nil {
		t.Fatal("linked spool root was accepted")
	}
}

func TestGatewayTransferSpoolRejectsLinkedCommittedData(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	store := openTestTransferSpool(t, now, 0, 0)
	owner := testTransferOwner("actor-a")
	content := []byte("payload")
	writer, _, _, err := store.Begin(owner, testTransferSpec(content, now))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(1, content); err != nil {
		t.Fatal(err)
	}
	record, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(store.root, record.DataName)
	if err := os.Remove(dataPath); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, dataPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Resolve(
		owner,
		testTransferSpec(content, now),
		record.Ref,
	); !errors.Is(err, ErrTransferArtifactNotFound) {
		t.Fatalf("Resolve() linked data error = %v", err)
	}
}

func openTestTransferSpool(
	t *testing.T,
	now time.Time,
	maxRecords int,
	maxBytes int64,
) *GatewayTransferSpool {
	t.Helper()
	store, err := newGatewayTransferSpool(
		filepath.Join(t.TempDir(), "spool"),
		maxRecords,
		maxBytes,
		0,
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testTransferOwner(actor string) TransferArtifactOwner {
	return TransferArtifactOwner{
		WorkspaceID: "workspace",
		AgentID:     "main",
		ActorID:     actor,
		RouteID:     "route",
		SessionID:   "session",
		ToolCallID:  "tool-call",
	}
}

func testTransferSpec(content []byte, now time.Time) TransferArtifactSpec {
	return testTransferSpecWithID(content, now, "transfer_1")
}

func testTransferSpecWithID(
	content []byte,
	now time.Time,
	transferID string,
) TransferArtifactSpec {
	digest := sha256.Sum256(content)
	return TransferArtifactSpec{
		TransferID:      transferID,
		Direction:       TransferDirectionDownload,
		Target:          "personal-vpn",
		ProfileRevision: "files-v1",
		Filename:        "artifact.bin",
		ContentType:     "application/octet-stream",
		DeclaredSize:    int64(len(content)),
		SHA256:          hex.EncodeToString(digest[:]),
		ExpiresAt:       now.Add(time.Hour).Unix(),
	}
}

func injectCommittedTransferIndexWarning(
	store *GatewayTransferSpool,
	warning error,
) func() {
	original := store.writeIndex
	store.writeIndex = func(data []byte) error {
		if err := original(data); err != nil {
			return err
		}
		return &fileutil.CommittedWriteError{Err: warning}
	}
	return func() {
		store.writeIndex = original
	}
}

func TestTransferArtifactSpecHardLimits(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	spec := testTransferSpec([]byte("payload"), now)
	spec.DeclaredSize = MaxTransferArtifactBytes + 1
	if err := spec.Validate(); err == nil {
		t.Fatal("oversized transfer artifact was accepted")
	}
}
