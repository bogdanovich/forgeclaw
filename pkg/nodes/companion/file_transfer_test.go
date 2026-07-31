//go:build linux || darwin

package companion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestFileTransferRuntimeAdvertisesOnlyExplicitEnabledProfiles(t *testing.T) {
	runtime, _, _ := newTestFileTransferRuntime(t)
	descriptors := runtime.Descriptors()
	names := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		names = append(names, descriptor.Name)
		if descriptor.ModelContract == nil ||
			descriptor.ModelContract.Availability != nodes.ModelUnavailable ||
			!slices.Equal(
				descriptor.ModelContract.Constraints.ProfileAliases,
				[]string{"project"},
			) {
			t.Fatalf("file descriptor = %#v", descriptor)
		}
	}
	if !slices.Equal(
		names,
		[]string{"file.info.v1", "file.download.v1", "file.upload.v1"},
	) {
		t.Fatalf("file capabilities = %v", names)
	}
	commandRuntime, err := NewRuntime(
		nodes.ID("node_files"),
		"test",
		testRuntimePolicy([]string{"node.info.v1"}),
		newMemoryInvocationLedger(),
		WithFileCapabilities(runtime),
	)
	if err != nil {
		t.Fatal(err)
	}
	catalogNames := make([]string, 0, len(commandRuntime.Catalog().Commands))
	for _, descriptor := range commandRuntime.Catalog().Commands {
		catalogNames = append(catalogNames, descriptor.Name)
	}
	for _, name := range names {
		if !slices.Contains(catalogNames, name) {
			t.Fatalf("authenticated catalog lacks %q: %v", name, catalogNames)
		}
	}
}

func TestFileCapabilityAuthorityChangesInvalidateCatalog(t *testing.T) {
	root := canonicalTempDir(t)
	first := testFilePolicies(t, root)
	firstDescriptors, err := fileCapabilityDescriptors(first)
	if err != nil {
		t.Fatal(err)
	}
	firstCatalog := nodes.CapabilityCatalog{Commands: firstDescriptors}
	firstHash, err := firstCatalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	changed := testFilePolicies(t, root)
	profile := changed["project"]
	profile.Revision = "project-v2"
	profile.Approval.Read = FileApprovalRequired
	changed["project"] = profile
	changedDescriptors, err := fileCapabilityDescriptors(changed)
	if err != nil {
		t.Fatal(err)
	}
	changedHash, err := (nodes.CapabilityCatalog{
		Commands: changedDescriptors,
	}).Hash()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash == changedHash {
		t.Fatal("authority-bearing file profile change retained catalog hash")
	}
}

func TestFileTransferRuntimeMetadataIsBoundedAndRejectsSymlink(t *testing.T) {
	runtime, root, _ := newTestFileTransferRuntime(t)
	content := []byte("metadata payload")
	path := filepath.Join(root, "config.txt")
	if err := os.WriteFile(path, content, 0o640); err != nil {
		t.Fatal(err)
	}
	frame := testFilePrepareFrame(
		t,
		"metadata_1",
		protocol.TransferDownload,
		emptyTransferDigest,
		0,
		fileTransferPrepare{
			Operation: fileOperationInfo,
			Path:      path,
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	responses := collectTransferResponses(t, runtime, frame)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameCommitted {
		t.Fatalf("metadata responses = %#v", responses)
	}
	var result fileTransferResult
	if err := json.Unmarshal(responses[0].Payload, &result); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	if result.State != FileTransferCommitted ||
		result.Type != "regular_file" ||
		result.Size != uint64(len(content)) ||
		result.Mode != 0o640 ||
		result.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("metadata result = %#v", result)
	}

	link := filepath.Join(root, "linked.txt")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	frame.TransferID = "metadata_2"
	frame.Payload = mustPreparePayload(t, fileTransferPrepare{
		Operation: fileOperationInfo,
		Path:      link,
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
	})
	responses = collectTransferResponses(t, runtime, frame)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameDeny ||
		!bytes.Contains(responses[0].Payload, []byte(`"code":"PATH_DENIED"`)) {
		t.Fatalf("symlink metadata responses = %#v", responses)
	}
}

func TestFileTransferRuntimeUploadCreateAndDuplicateCommit(t *testing.T) {
	runtime, root, _ := newTestFileTransferRuntime(t)
	content := bytes.Repeat([]byte("bounded upload"), 30000)
	destination := filepath.Join(root, "created.bin")
	digest := sha256.Sum256(content)
	prepare := testFilePrepareFrame(
		t,
		"upload_create",
		protocol.TransferUpload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        destination,
			Publication: filePublicationCreate,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	responses := collectTransferResponses(t, runtime, prepare)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameAccept {
		t.Fatalf("upload prepare responses = %#v", responses)
	}
	chunks := chunkBytes(content)
	for index, chunk := range chunks {
		frame := transferFrameFromBinding(prepare, protocol.TransferFrameChunk)
		frame.Sequence = uint64(index + 1)
		frame.Payload = chunk
		responses = collectTransferResponses(t, runtime, frame)
		if len(responses) != 1 ||
			responses[0].Type != protocol.TransferFrameAck ||
			responses[0].Sequence != frame.Sequence {
			t.Fatalf("chunk %d responses = %#v", index+1, responses)
		}
		if index == 0 {
			duplicate := collectTransferResponses(t, runtime, frame)
			if len(duplicate) != 1 ||
				duplicate[0].Type != protocol.TransferFrameAck {
				t.Fatalf("duplicate chunk responses = %#v", duplicate)
			}
		}
	}
	commit := transferFrameFromBinding(prepare, protocol.TransferFrameCommit)
	responses = collectTransferResponses(t, runtime, commit)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameCommitted {
		t.Fatalf("upload commit responses = %#v", responses)
	}
	published, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(published, content) {
		t.Fatal("published upload differs from source")
	}
	responses = collectTransferResponses(t, runtime, commit)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameCommitted {
		t.Fatalf("duplicate commit responses = %#v", responses)
	}
}

func TestFileTransferRuntimeCreateRefusesExistingAndReplaceIsAtomic(t *testing.T) {
	runtime, root, _ := newTestFileTransferRuntime(t)
	destination := filepath.Join(root, "config.txt")
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	createContent := []byte("must-not-publish")
	createDigest := sha256.Sum256(createContent)
	create := testFilePrepareFrame(
		t,
		"upload_existing",
		protocol.TransferUpload,
		createDigest,
		uint64(len(createContent)),
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        destination,
			Publication: filePublicationCreate,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	collectTransferResponses(t, runtime, create)
	sendUploadChunks(t, runtime, create, createContent)
	responses := collectTransferResponses(
		t,
		runtime,
		transferFrameFromBinding(create, protocol.TransferFrameCommit),
	)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameFailure {
		t.Fatalf("existing create responses = %#v", responses)
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old" {
		t.Fatalf("failed create changed destination to %q", data)
	}

	replaceContent := bytes.Repeat([]byte("new"), 1000)
	replaceDigest := sha256.Sum256(replaceContent)
	replace := testFilePrepareFrame(
		t,
		"upload_replace",
		protocol.TransferUpload,
		replaceDigest,
		uint64(len(replaceContent)),
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        destination,
			Publication: filePublicationReplace,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	collectTransferResponses(t, runtime, replace)
	sendUploadChunks(t, runtime, replace, replaceContent)
	entered := make(chan struct{})
	release := make(chan struct{})
	originalRename := renameFileStage
	renameFileStage = func(directoryFD int, stagingName, finalName string) error {
		close(entered)
		<-release
		return originalRename(directoryFD, stagingName, finalName)
	}
	t.Cleanup(func() { renameFileStage = originalRename })
	commitDone := make(chan transferCallResult, 1)
	go func() {
		responses, callErr := callTransfer(
			runtime,
			t.Context(),
			transferFrameFromBinding(replace, protocol.TransferFrameCommit),
		)
		commitDone <- transferCallResult{responses: responses, err: callErr}
	}()
	<-entered
	data, err = os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old" {
		t.Fatalf("destination exposed partial data: %q", data)
	}
	close(release)
	commitResult := <-commitDone
	if commitResult.err != nil {
		t.Fatal(commitResult.err)
	}
	responses = commitResult.responses
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameCommitted {
		t.Fatalf("replace responses = %#v", responses)
	}
	data, err = os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, replaceContent) {
		t.Fatal("atomic replace did not publish exact bytes")
	}
}

func TestFileTransferRuntimeDownloadUsesPinnedSourceAndExactDigest(t *testing.T) {
	runtime, root, _ := newTestFileTransferRuntime(t)
	content := bytes.Repeat([]byte{0, 1, 2, 3, 255}, 70000)
	source := filepath.Join(root, "image.bin")
	if err := os.WriteFile(source, content, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	prepare := testFilePrepareFrame(
		t,
		"download_1",
		protocol.TransferDownload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation: fileOperationDownload,
			Path:      source,
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	var received bytes.Buffer
	var responseMu sync.Mutex
	responses := make([]protocol.TransferFrame, 0)
	send := func(frame protocol.TransferFrame) error {
		responseMu.Lock()
		responses = append(responses, frame)
		if frame.Type == protocol.TransferFrameChunk {
			_, _ = received.Write(frame.Payload)
		}
		responseMu.Unlock()
		if frame.Type == protocol.TransferFrameChunk {
			ack := transferFrameFromBinding(frame, protocol.TransferFrameAck)
			ack.Sequence = frame.Sequence
			if err := runtime.HandleTransferFrame(t.Context(), ack, func(
				protocol.TransferFrame,
			) error {
				return nil
			}); err != nil {
				return err
			}
			return runtime.HandleTransferFrame(t.Context(), ack, func(
				protocol.TransferFrame,
			) error {
				return nil
			})
		}
		return nil
	}
	if err := runtime.HandleTransferFrame(t.Context(), prepare, send); err != nil {
		t.Fatal(err)
	}
	active := runtime.getActive(prepare.TransferID)
	if active == nil {
		t.Fatal("download did not become active")
	}
	select {
	case <-active.received:
	case <-time.After(10 * time.Second):
		record, _, _ := runtime.ledger.Lookup(prepare.TransferID)
		t.Fatalf("download did not reach received state: %#v", record)
	}
	if !bytes.Equal(received.Bytes(), content) {
		t.Fatal("downloaded bytes differ from pinned source")
	}
	commit := transferFrameFromBinding(prepare, protocol.TransferFrameCommit)
	if err := runtime.HandleTransferFrame(t.Context(), commit, send); err != nil {
		t.Fatal(err)
	}
	responseMu.Lock()
	defer responseMu.Unlock()
	if len(responses) < 3 ||
		responses[0].Type != protocol.TransferFrameAccept ||
		responses[len(responses)-1].Type != protocol.TransferFrameCommitted {
		t.Fatalf("download responses = %#v", responses)
	}
}

func TestFileTransferRuntimeDisconnectCancelsBeforePublication(t *testing.T) {
	runtime, root, ledger := newTestFileTransferRuntime(t)
	content := []byte("not published")
	digest := sha256.Sum256(content)
	destination := filepath.Join(root, "canceled.txt")
	ctx, cancel := context.WithCancel(t.Context())
	prepare := testFilePrepareFrame(
		t,
		"upload_disconnect",
		protocol.TransferUpload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        destination,
			Publication: filePublicationCreate,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	if err := runtime.HandleTransferFrame(ctx, prepare, func(
		protocol.TransferFrame,
	) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	active := runtime.getActive(prepare.TransferID)
	if active == nil {
		t.Fatal("upload did not become active")
	}
	cancel()
	select {
	case <-active.done:
	case <-time.After(10 * time.Second):
		t.Fatal("disconnect cleanup did not finish")
	}
	record, found, err := ledger.Lookup(prepare.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || record.State != FileTransferCanceled {
		t.Fatalf("canceled record = %#v, found %v", record, found)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disconnect published destination: %v", err)
	}
	if matches, err := filepath.Glob(filepath.Join(root, ".mintclaw-transfer-*")); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("disconnect retained staging files: %v", matches)
	}
}

func TestFileTransferRuntimeRejectsChangedDigestAndStaleRevision(t *testing.T) {
	runtime, root, _ := newTestFileTransferRuntime(t)
	content := []byte("source")
	path := filepath.Join(root, "source.txt")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	wrong := sha256.Sum256([]byte("wrong"))
	prepare := testFilePrepareFrame(
		t,
		"download_wrong_digest",
		protocol.TransferDownload,
		wrong,
		uint64(len(content)),
		fileTransferPrepare{
			Operation: fileOperationDownload,
			Path:      path,
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	failure := make(chan protocol.TransferFrame, 1)
	var initial []protocol.TransferFrame
	send := func(frame protocol.TransferFrame) error {
		switch frame.Type {
		case protocol.TransferFrameAccept:
			initial = append(initial, frame)
		case protocol.TransferFrameChunk:
			ack := transferFrameFromBinding(frame, protocol.TransferFrameAck)
			ack.Sequence = frame.Sequence
			return runtime.HandleTransferFrame(t.Context(), ack, func(
				protocol.TransferFrame,
			) error {
				return nil
			})
		case protocol.TransferFrameFailure:
			failure <- frame
		}
		return nil
	}
	if err := runtime.HandleTransferFrame(t.Context(), prepare, send); err != nil {
		t.Fatal(err)
	}
	if len(initial) != 1 || initial[0].Type != protocol.TransferFrameAccept {
		t.Fatalf("wrong digest initial responses = %#v", initial)
	}
	select {
	case response := <-failure:
		if !bytes.Contains(response.Payload, []byte(`"code":"SOURCE_CHANGED"`)) {
			t.Fatalf("wrong digest failure = %#v", response)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("wrong digest download did not fail")
	}
	prepare.TransferID = "download_stale_revision"
	prepare.PolicyRevision = "project-v2"
	responses := collectTransferResponses(t, runtime, prepare)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameDeny ||
		!bytes.Contains(responses[0].Payload, []byte(`"code":"PROFILE_DENIED"`)) {
		t.Fatalf("stale revision responses = %#v", responses)
	}
}

func TestFileTransferRuntimeEnforcesPerProfileConcurrency(t *testing.T) {
	runtime, root, _ := newTestFileTransferRuntime(t)
	for index := 0; index < nodes.MaxTargetProfileActiveTransfers; index++ {
		content := []byte{byte(index + 1)}
		digest := sha256.Sum256(content)
		prepare := testFilePrepareFrame(
			t,
			fmt.Sprintf("capacity_%d", index),
			protocol.TransferUpload,
			digest,
			uint64(len(content)),
			fileTransferPrepare{
				Operation:   fileOperationUpload,
				Path:        filepath.Join(root, fmt.Sprintf("capacity-%d", index)),
				Publication: filePublicationCreate,
				ExpiresAt:   time.Now().Add(time.Minute).Unix(),
			},
		)
		responses := collectTransferResponses(t, runtime, prepare)
		if len(responses) != 1 ||
			responses[0].Type != protocol.TransferFrameAccept {
			t.Fatalf("capacity prepare %d = %#v", index, responses)
		}
	}
	content := []byte("blocked")
	digest := sha256.Sum256(content)
	blocked := testFilePrepareFrame(
		t,
		"capacity_blocked",
		protocol.TransferUpload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        filepath.Join(root, "capacity-blocked"),
			Publication: filePublicationCreate,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	responses := collectTransferResponses(t, runtime, blocked)
	if len(responses) != 1 ||
		responses[0].Type != protocol.TransferFrameDeny ||
		!bytes.Contains(responses[0].Payload, []byte(`"code":"CAPACITY_EXCEEDED"`)) {
		t.Fatalf("capacity denial = %#v", responses)
	}
}

func TestFileTransferRuntimeExpiresAndCleansPreCommitUpload(t *testing.T) {
	runtime, root, ledger := newTestFileTransferRuntime(t)
	content := []byte("expires")
	digest := sha256.Sum256(content)
	destination := filepath.Join(root, "expired.txt")
	prepare := testFilePrepareFrame(
		t,
		"upload_expired",
		protocol.TransferUpload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        destination,
			Publication: filePublicationCreate,
			ExpiresAt:   time.Now().Add(2 * time.Second).Unix(),
		},
	)
	collectTransferResponses(t, runtime, prepare)
	active := runtime.getActive(prepare.TransferID)
	if active == nil {
		t.Fatal("expiring upload did not become active")
	}
	select {
	case <-active.done:
	case <-time.After(5 * time.Second):
		t.Fatal("expiring upload did not clean up")
	}
	record, found, err := ledger.Lookup(prepare.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if !found ||
		record.State != FileTransferExpired ||
		record.FailureCode != "EXPIRED" {
		t.Fatalf("expired transfer = %#v, found %v", record, found)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired upload published destination: %v", err)
	}
}

func TestFileTransferRuntimeRestartReconcilesPreCommitAndPublishedUpload(
	t *testing.T,
) {
	root := canonicalTempDir(t)
	policies := testFilePolicies(t, root)
	ledgerPath := filepath.Join(canonicalTempDir(t), "file-transfers.json")
	ledger, err := NewFileTransferLedger(
		ledgerPath,
		DefaultFileTransferLedgerLimit,
		DefaultFileTransferLedgerBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	profile := policies["project"]
	opened, err := openFileProfile(profile)
	if err != nil {
		t.Fatal(err)
	}

	canceledContent := []byte("cleanup on restart")
	canceledDigest := sha256.Sum256(canceledContent)
	canceledPath := filepath.Join(root, "canceled.txt")
	canceledParent, err := opened.resolveWritableParent(canceledPath)
	if err != nil {
		t.Fatal(err)
	}
	canceledStage, err := canceledParent.createStage("restart_canceled")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canceledStage.file.Write(canceledContent); err != nil {
		t.Fatal(err)
	}
	if err := canceledStage.file.Sync(); err != nil {
		t.Fatal(err)
	}
	canceledRecord := FileTransferRecord{
		TransferID:     "restart_canceled",
		Direction:      protocol.TransferUpload,
		Operation:      fileOperationUpload,
		ProfileAlias:   "project",
		PolicyRevision: "project-v1",
		Path:           canceledPath,
		Publication:    filePublicationCreate,
		TotalSize:      uint64(len(canceledContent)),
		SHA256:         hex.EncodeToString(canceledDigest[:]),
		ExpiresAt:      time.Now().Add(time.Minute).Unix(),
		State:          FileTransferAccepted,
		StageName:      canceledStage.name,
		StageIdentity:  canceledStage.identity,
	}
	if _, _, err := ledger.Accept(canceledRecord); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transition(canceledRecord.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferStreaming
		record.Sequence = 1
		record.ObservedBytes = uint64(len(canceledContent))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = canceledStage.file.Close()
	_ = canceledParent.close()

	publishedContent := []byte("published before ledger result")
	publishedDigest := sha256.Sum256(publishedContent)
	publishedPath := filepath.Join(root, "published.txt")
	publishedParent, err := opened.resolveWritableParent(publishedPath)
	if err != nil {
		t.Fatal(err)
	}
	publishedStage, err := publishedParent.createStage("restart_published")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publishedStage.file.Write(publishedContent); err != nil {
		t.Fatal(err)
	}
	if err := publishedStage.file.Sync(); err != nil {
		t.Fatal(err)
	}
	publishedRecord := FileTransferRecord{
		TransferID:     "restart_published",
		Direction:      protocol.TransferUpload,
		Operation:      fileOperationUpload,
		ProfileAlias:   "project",
		PolicyRevision: "project-v1",
		Path:           publishedPath,
		Publication:    filePublicationCreate,
		TotalSize:      uint64(len(publishedContent)),
		SHA256:         hex.EncodeToString(publishedDigest[:]),
		ExpiresAt:      time.Now().Add(time.Minute).Unix(),
		State:          FileTransferAccepted,
		StageName:      publishedStage.name,
		StageIdentity:  publishedStage.identity,
	}
	if _, _, err := ledger.Accept(publishedRecord); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transition(publishedRecord.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferStaged
		record.Sequence = 1
		record.ObservedBytes = uint64(len(publishedContent))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transition(publishedRecord.TransferID, func(
		record *FileTransferRecord,
		_ time.Time,
	) error {
		record.State = FileTransferCommitRequested
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := publishedStage.publish(filePublicationCreate); err != nil {
		t.Fatal(err)
	}
	_ = publishedStage.file.Close()
	_ = publishedParent.close()
	opened.close()
	ledger.Close()

	reloaded, err := NewFileTransferLedger(
		ledgerPath,
		DefaultFileTransferLedgerLimit,
		DefaultFileTransferLedgerBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	runtime, err := NewFileTransferRuntime(policies, reloaded)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	canceled, found, err := reloaded.Lookup(canceledRecord.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || canceled.State != FileTransferCanceled {
		t.Fatalf("reconciled canceled record = %#v, found %v", canceled, found)
	}
	if _, err := os.Stat(canceledPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart exposed canceled destination: %v", err)
	}
	published, found, err := reloaded.Lookup(publishedRecord.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || published.State != FileTransferPublished {
		t.Fatalf("reconciled published record = %#v, found %v", published, found)
	}
	data, err := os.ReadFile(publishedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, publishedContent) {
		t.Fatal("reconciled publication differs from exact staged bytes")
	}
}

func TestFileTransferRuntimeCancelVersusCommitHasOneTruthfulOutcome(t *testing.T) {
	runtime, root, ledger := newTestFileTransferRuntime(t)
	destination := filepath.Join(root, "race.txt")
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("replacement"), 1000)
	digest := sha256.Sum256(content)
	prepare := testFilePrepareFrame(
		t,
		"cancel_commit_race",
		protocol.TransferUpload,
		digest,
		uint64(len(content)),
		fileTransferPrepare{
			Operation:   fileOperationUpload,
			Path:        destination,
			Publication: filePublicationReplace,
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		},
	)
	collectTransferResponses(t, runtime, prepare)
	sendUploadChunks(t, runtime, prepare, content)

	entered := make(chan struct{})
	release := make(chan struct{})
	originalRename := renameFileStage
	renameFileStage = func(directoryFD int, stagingName, finalName string) error {
		close(entered)
		<-release
		return originalRename(directoryFD, stagingName, finalName)
	}
	t.Cleanup(func() { renameFileStage = originalRename })
	commitDone := make(chan transferCallResult, 1)
	go func() {
		responses, callErr := callTransfer(
			runtime,
			t.Context(),
			transferFrameFromBinding(prepare, protocol.TransferFrameCommit),
		)
		commitDone <- transferCallResult{responses: responses, err: callErr}
	}()
	<-entered
	cancelDone := make(chan transferCallResult, 1)
	go func() {
		responses, callErr := callTransfer(
			runtime,
			t.Context(),
			transferFrameFromBinding(prepare, protocol.TransferFrameCancel),
		)
		cancelDone <- transferCallResult{responses: responses, err: callErr}
	}()
	close(release)
	commitResult := <-commitDone
	cancelResult := <-cancelDone
	if commitResult.err != nil {
		t.Fatal(commitResult.err)
	}
	if cancelResult.err != nil {
		t.Fatal(cancelResult.err)
	}
	commitResponses := commitResult.responses
	cancelResponses := cancelResult.responses
	if len(commitResponses) != 1 ||
		commitResponses[0].Type != protocol.TransferFrameCommitted ||
		len(cancelResponses) != 1 ||
		cancelResponses[0].Type != protocol.TransferFrameCommitted {
		t.Fatalf(
			"commit responses = %#v, cancel responses = %#v",
			commitResponses,
			cancelResponses,
		)
	}
	record, found, err := ledger.Lookup(prepare.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || record.State != FileTransferPublished {
		t.Fatalf("cancel/commit record = %#v, found %v", record, found)
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, content) {
		t.Fatal("cancel/commit race did not retain exact publication")
	}
}

func newTestFileTransferRuntime(
	t *testing.T,
) (*FileTransferRuntime, string, *FileTransferLedger) {
	t.Helper()
	root := canonicalTempDir(t)
	policies := testFilePolicies(t, root)
	ledger := newMemoryFileTransferLedger()
	runtime, err := NewFileTransferRuntime(policies, ledger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	return runtime, root, ledger
}

func testFilePolicies(t *testing.T, root string) FilePolicies {
	t.Helper()
	cfg, err := (Config{
		GatewayURL: "wss://gateway.example",
		FilePolicies: FilePolicies{
			"project": {
				Enabled:        true,
				Revision:       "project-v1",
				ReadableRoots:  []string{root},
				WritableRoots:  []string{root},
				AllowCreate:    true,
				AllowOverwrite: true,
				MaxFileBytes:   protocol.MaxTransferFileBytes,
			},
		},
	}).Normalize(root)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.FilePolicies
}

func testFilePrepareFrame(
	t *testing.T,
	transferID string,
	direction protocol.TransferDirection,
	digest [32]byte,
	size uint64,
	prepare fileTransferPrepare,
) protocol.TransferFrame {
	t.Helper()
	frame := protocol.TransferFrame{
		Type:           protocol.TransferFramePrepare,
		Direction:      direction,
		TransferID:     transferID,
		PolicyRevision: "project-v1",
		TotalSize:      size,
		SHA256:         digest,
		Payload:        mustPreparePayload(t, prepare),
	}
	if err := frame.Validate(); err != nil {
		t.Fatalf("test prepare frame: %v", err)
	}
	return frame
}

func mustPreparePayload(t *testing.T, prepare fileTransferPrepare) []byte {
	t.Helper()
	payload, err := json.Marshal(prepare)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func transferFrameFromBinding(
	binding protocol.TransferFrame,
	frameType protocol.TransferFrameType,
) protocol.TransferFrame {
	return protocol.TransferFrame{
		Type:           frameType,
		Direction:      binding.Direction,
		TransferID:     binding.TransferID,
		PolicyRevision: binding.PolicyRevision,
		TotalSize:      binding.TotalSize,
		SHA256:         binding.SHA256,
	}
}

func collectTransferResponses(
	t *testing.T,
	runtime *FileTransferRuntime,
	frame protocol.TransferFrame,
) []protocol.TransferFrame {
	t.Helper()
	responses, err := callTransfer(runtime, t.Context(), frame)
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range responses {
		if err := response.Validate(); err != nil {
			t.Fatalf("invalid transfer response %#v: %v", response, err)
		}
	}
	return responses
}

type transferCallResult struct {
	responses []protocol.TransferFrame
	err       error
}

func callTransfer(
	runtime *FileTransferRuntime,
	ctx context.Context,
	frame protocol.TransferFrame,
) ([]protocol.TransferFrame, error) {
	var responses []protocol.TransferFrame
	err := runtime.HandleTransferFrame(
		ctx,
		frame,
		func(response protocol.TransferFrame) error {
			responses = append(responses, response)
			return nil
		},
	)
	return responses, err
}

func sendUploadChunks(
	t *testing.T,
	runtime *FileTransferRuntime,
	binding protocol.TransferFrame,
	content []byte,
) {
	t.Helper()
	for index, chunk := range chunkBytes(content) {
		frame := transferFrameFromBinding(binding, protocol.TransferFrameChunk)
		frame.Sequence = uint64(index + 1)
		frame.Payload = chunk
		responses := collectTransferResponses(t, runtime, frame)
		if len(responses) != 1 ||
			responses[0].Type != protocol.TransferFrameAck {
			t.Fatalf("upload chunk responses = %#v", responses)
		}
	}
}

func chunkBytes(content []byte) [][]byte {
	chunks := make([][]byte, 0)
	for len(content) > 0 {
		count := min(len(content), protocol.MaxTransferChunkBytes)
		chunks = append(chunks, append([]byte(nil), content[:count]...))
		content = content[count:]
	}
	return chunks
}
