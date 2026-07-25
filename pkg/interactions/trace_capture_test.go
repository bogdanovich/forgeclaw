package interactions

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

func resolveInteraction(
	t *testing.T,
	registry *Registry,
	record Record,
) Record {
	t.Helper()
	var err error
	record, err = registry.ClaimAnswer(
		record.ID,
		record.Revision,
		Answer{Text: "Staging"},
		OutcomeAnswered,
	)
	if err != nil {
		t.Fatalf("claim answer: %v", err)
	}
	record, err = registry.MarkResuming(record.ID, record.Revision)
	if err != nil {
		t.Fatalf("mark resuming: %v", err)
	}
	record, err = registry.Resolve(record.ID, record.Revision)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	return record
}

func TestTraceCaptureTerminalMarkerIsAtomicWithLifecycleEvent(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	record := makeWaiting(
		t,
		registry,
		clock,
		"interaction_trace_atomic",
		"trace-atomic",
	)
	var terminalObservation EventObservation
	unsubscribe := registry.Subscribe(func(observation EventObservation) {
		if observation.Event.Type == EventResolved {
			terminalObservation = observation
		}
	})
	defer unsubscribe()
	record = resolveInteraction(t, registry, record)

	if !record.TraceCapturePending ||
		len(record.TraceCaptureEvents) == 0 ||
		record.TraceCaptureEvents[len(record.TraceCaptureEvents)-1].Sequence !=
			record.LastEventSeq {
		t.Fatalf("terminal record lacks complete pending journal: %#v", record)
	}
	if !terminalObservation.Record.TraceCapturePending ||
		terminalObservation.Event.Sequence != record.LastEventSeq {
		t.Fatalf("terminal observation was not atomic: %#v", terminalObservation)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	stored, journal, ok := reloaded.GetTraceCapture(record.ID)
	if err := reloaded.LastLoadError(); err != nil || !ok ||
		!stored.TraceCapturePending ||
		len(journal) == 0 ||
		journal[len(journal)-1].Sequence != stored.LastEventSeq {
		t.Fatalf("reloaded trace capture = (%#v, %#v, %v, %v)", stored, journal, ok, err)
	}
}

func TestTraceCaptureProtectionAppliesAcrossRegistryInstances(t *testing.T) {
	owner, clock, path := newTestRegistry(t)
	record := makeWaiting(
		t,
		owner,
		clock,
		"interaction_multi_instance",
		"trace-multi-instance",
	)
	writer := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if err := owner.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	current, ok := writer.Get(record.ID)
	if !ok {
		t.Fatal("second registry instance did not load interaction")
	}
	if _, err := writer.Cancel(
		current.ID,
		current.Revision,
		"test_cancel",
	); err != nil {
		t.Fatalf("terminalize from second registry instance: %v", err)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	stored, journal, ok := reloaded.GetTraceCapture(record.ID)
	if err := reloaded.LastLoadError(); err != nil || !ok ||
		!stored.TraceCapturePending || len(journal) == 0 ||
		journal[len(journal)-1].Sequence != stored.LastEventSeq {
		t.Fatalf(
			"cross-instance terminal capture = (%#v, %#v, %v, %v)",
			stored,
			journal,
			ok,
			err,
		)
	}
}

func TestTraceCapturePendingProtectsRetentionAndCapacityPruning(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	terminal := resolveInteraction(
		t,
		registry,
		makeWaiting(t, registry, clock, "interaction_trace_keep", "trace-keep"),
	)
	active := makeWaiting(
		t,
		registry,
		clock,
		"interaction_trace_active",
		"trace-active",
	)
	registry.mu.Lock()
	registry.options.MaxRecords = 1
	registry.mu.Unlock()
	clock.Advance(DefaultRetention + time.Hour)
	if err := registry.Prune(time.Time{}); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if stored, ok := registry.Get(terminal.ID); !ok || !stored.TraceCapturePending {
		t.Fatalf("pending terminal record was pruned: (%#v, %v)", stored, ok)
	}
	if _, ok := registry.Get(active.ID); !ok {
		t.Fatal("active record was pruned")
	}
}

func TestTraceCaptureJournalIsBoundedAndHotLimitApplies(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	record := resolveInteraction(
		t,
		registry,
		makeWaiting(t, registry, clock, "interaction_trace_bound", "trace-bound"),
	)
	_, journal, ok := registry.GetTraceCapture(record.ID)
	if !ok || len(journal) < 3 {
		t.Fatalf("initial journal = %#v", journal)
	}
	if err := registry.SetTraceCaptureProtection(true, 2); err != nil {
		t.Fatalf("apply hot trace limit: %v", err)
	}
	stored, journal, ok := registry.GetTraceCapture(record.ID)
	if !ok || len(journal) != 2 ||
		journal[0].Sequence+1 != journal[1].Sequence ||
		journal[1].Sequence != stored.LastEventSeq ||
		stored.TraceCaptureDropped != int(journal[0].Sequence-1) {
		t.Fatalf("bounded journal = (%#v, %#v, %v)", stored, journal, ok)
	}
}

func TestTraceCaptureRejectsTamperedJournalFingerprint(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	record := resolveInteraction(
		t,
		registry,
		makeWaiting(t, registry, clock, "interaction_trace_tamper", "trace-tamper"),
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	for index := range snapshot.Records {
		if snapshot.Records[index].ID == record.ID {
			snapshot.Records[index].TraceCaptureEvents[0].Code = "tampered"
		}
	}
	data, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if reloaded := NewRegistryWithOptions(
		path,
		Options{Now: clock.Now},
	); reloaded.LastLoadError() == nil {
		t.Fatal("tampered trace journal loaded successfully")
	}
}

func TestTraceCaptureConfirmationUsesLastEventSequenceCAS(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	record := resolveInteraction(
		t,
		registry,
		makeWaiting(t, registry, clock, "interaction_trace_cas", "trace-cas"),
	)
	current, confirmed, err := registry.ConfirmTraceCapturePersisted(
		context.Background(),
		record.ID,
		record.LastEventSeq-1,
	)
	if err != nil || confirmed || !current.TraceCapturePending {
		t.Fatalf("stale confirmation = (%#v, %v, %v)", current, confirmed, err)
	}
	current, confirmed, err = registry.ConfirmTraceCapturePersisted(
		context.Background(),
		record.ID,
		record.LastEventSeq,
	)
	if err != nil || !confirmed || current.TraceCapturePending ||
		len(current.TraceCaptureEvents) != 0 {
		t.Fatalf("current confirmation = (%#v, %v, %v)", current, confirmed, err)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if stored, _, ok := reloaded.GetTraceCapture(record.ID); !ok ||
		stored.TraceCapturePending {
		t.Fatalf("confirmed marker persisted = (%#v, %v)", stored, ok)
	}
}

func TestTraceCaptureConfirmationRetriesCommittedWrite(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	record := resolveInteraction(
		t,
		registry,
		makeWaiting(t, registry, clock, "interaction_trace_sync", "trace-sync"),
	)
	realWrite := registry.writeAtomic
	writes := 0
	registry.writeAtomic = func(path string, data []byte, mode os.FileMode) error {
		writes++
		if err := realWrite(path, data, mode); err != nil {
			return err
		}
		if writes == 1 {
			return &fileutil.CommittedWriteError{
				Err: errors.New("injected directory sync failure"),
			}
		}
		return nil
	}
	current, confirmed, err := registry.ConfirmTraceCapturePersisted(
		context.Background(),
		record.ID,
		record.LastEventSeq,
	)
	if confirmed || !fileutil.IsCommittedWriteError(err) ||
		!current.TraceCapturePending || !registry.unsyncedWrite {
		t.Fatalf("first confirmation = (%#v, %v, %v)", current, confirmed, err)
	}
	current, confirmed, err = registry.ConfirmTraceCapturePersisted(
		context.Background(),
		record.ID,
		record.LastEventSeq,
	)
	if err != nil || !confirmed || current.TraceCapturePending ||
		registry.unsyncedWrite || writes != 2 {
		t.Fatalf("retry confirmation = (%#v, %v, %v), writes=%d", current, confirmed, err, writes)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if stored, _, ok := reloaded.GetTraceCapture(record.ID); !ok ||
		stored.TraceCapturePending {
		t.Fatalf("reloaded confirmation = (%#v, %v)", stored, ok)
	}
}

func TestTraceCaptureProtectionInstallationFailsClosed(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	record := resolveInteraction(
		t,
		registry,
		makeWaiting(t, registry, clock, "interaction_trace_install", "trace-install"),
	)
	active := makeWaiting(
		t,
		registry,
		clock,
		"active_install_recovery",
		"trace-install-active",
	)
	realWrite := registry.writeAtomic
	writes := 0
	registry.writeAtomic = func(path string, data []byte, mode os.FileMode) error {
		writes++
		if writes == 1 {
			return errors.New("injected pre-commit write failure")
		}
		return realWrite(path, data, mode)
	}
	if err := registry.SetTraceCaptureProtection(true, 20); err == nil {
		t.Fatal("trace protection installation succeeded during injected failure")
	}
	if !registry.traceCaptureProtectionPending ||
		!registry.traceCaptureProtectionDesired {
		t.Fatal("failed installation did not retain fail-closed runtime state")
	}
	clock.Advance(DefaultRetention + time.Hour)
	if err := registry.Prune(time.Time{}); err != nil {
		t.Fatalf("prune while protection pending: %v", err)
	}
	if _, ok := registry.Get(record.ID); !ok {
		t.Fatal("terminal record pruned while protection installation was pending")
	}
	if _, err := registry.ClaimAnswer(
		active.ID,
		active.Revision,
		Answer{Text: "Staging"},
		OutcomeAnswered,
	); err != nil {
		t.Fatalf("intervening successful mutation: %v", err)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	stored, journal, ok := reloaded.GetTraceCapture(record.ID)
	if err := reloaded.LastLoadError(); err != nil || !ok ||
		!stored.TraceCapturePending || len(journal) == 0 ||
		registry.traceCaptureProtectionPending ||
		!registry.traceCaptureProtection ||
		!reloaded.traceCaptureProtection {
		t.Fatalf(
			"successful mutation did not durably complete protection: (%#v, %#v, %v, %v)",
			stored,
			journal,
			ok,
			err,
		)
	}
}

func TestTraceCaptureFailedEnableIntentProtectsAcrossRegistryInstances(
	t *testing.T,
) {
	registry, clock, path := newTestRegistry(t)
	record := resolveInteraction(
		t,
		registry,
		makeWaiting(
			t,
			registry,
			clock,
			"cross_instance_enable_failure",
			"trace-cross-instance-enable",
		),
	)
	realWrite := registry.writeAtomic
	registry.writeAtomic = func(string, []byte, os.FileMode) error {
		return errors.New("injected snapshot write failure")
	}
	if err := registry.SetTraceCaptureProtection(true, 20); err == nil {
		t.Fatal("trace protection installation succeeded during injected failure")
	}

	other := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if err := other.LastLoadError(); err != nil {
		t.Fatal(err)
	}
	if !other.traceCaptureProtectionPending ||
		!other.traceCaptureProtectionDesired {
		t.Fatal("durable enable intent was not loaded by another instance")
	}
	clock.Advance(DefaultRetention + time.Hour)
	if err := other.Prune(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := other.Get(record.ID); !ok {
		t.Fatal("another instance pruned while enable intent was unresolved")
	}

	registry.writeAtomic = realWrite
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("retry trace protection: %v", err)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	stored, ok := reloaded.Get(record.ID)
	if err := reloaded.LastLoadError(); err != nil || !ok ||
		!reloaded.traceCaptureProtection || !stored.TraceCapturePending {
		t.Fatalf("durable protection retry = (%#v, %v, %v)", stored, ok, err)
	}
}

func TestTraceCaptureAcknowledgedDisableSupersedesStaleEnableIntent(
	t *testing.T,
) {
	registry, clock, path := newTestRegistry(t)
	active := makeWaiting(
		t,
		registry,
		clock,
		"stale_enable_active",
		"trace-stale-enable-active",
	)
	realWrite := registry.writeAtomic
	registry.writeAtomic = func(string, []byte, os.FileMode) error {
		return errors.New("injected snapshot write failure")
	}
	if err := registry.SetTraceCaptureProtection(true, 20); err == nil {
		t.Fatal("trace protection installation succeeded during injected failure")
	}

	other := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if err := other.SetTraceCaptureProtection(false, 0); err != nil {
		t.Fatalf("disable from another instance: %v", err)
	}
	registry.writeAtomic = realWrite
	if _, err := registry.ClaimAnswer(
		active.ID,
		active.Revision,
		Answer{Text: "Staging"},
		OutcomeAnswered,
	); err != nil {
		t.Fatalf("mutation through stale instance: %v", err)
	}

	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	stored, journal, ok := reloaded.GetTraceCapture(active.ID)
	if err := reloaded.LastLoadError(); err != nil || !ok ||
		reloaded.traceCaptureProtection ||
		reloaded.traceCaptureProtectionPending ||
		stored.TraceCapturePending || len(journal) != 0 {
		t.Fatalf(
			"acknowledged disable was resurrected = (%#v, %#v, %v, %v)",
			stored,
			journal,
			ok,
			err,
		)
	}
}

func TestTraceCaptureDisableFailureRetainsDesiredDisabledState(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	active := makeWaiting(
		t,
		registry,
		clock,
		"disable_failure_active",
		"trace-disable-failure",
	)
	realWrite := registry.writeAtomic
	writes := 0
	registry.writeAtomic = func(path string, data []byte, mode os.FileMode) error {
		writes++
		if writes == 1 {
			return errors.New("injected disable write failure")
		}
		return realWrite(path, data, mode)
	}
	if err := registry.SetTraceCaptureProtection(false, 0); err == nil {
		t.Fatal("trace protection disable succeeded during injected failure")
	}
	if !registry.traceCaptureProtectionPending ||
		registry.traceCaptureProtectionDesired {
		t.Fatal("failed disable did not retain desired disabled state")
	}
	if _, err := registry.ClaimAnswer(
		active.ID,
		active.Revision,
		Answer{Text: "Staging"},
		OutcomeAnswered,
	); err != nil {
		t.Fatalf("intervening successful mutation: %v", err)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	stored, journal, ok := reloaded.GetTraceCapture(active.ID)
	if err := reloaded.LastLoadError(); err != nil || !ok ||
		reloaded.traceCaptureProtection ||
		reloaded.traceCaptureProtectionPending ||
		stored.TraceCapturePending || len(journal) != 0 {
		t.Fatalf(
			"failed disable recovery = (%#v, %#v, %v, %v)",
			stored,
			journal,
			ok,
			err,
		)
	}
}

func TestTraceCaptureDisablePreservesJournalsUntilNextMutation(t *testing.T) {
	registry, clock, _ := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	active := makeWaiting(
		t,
		registry,
		clock,
		"interaction_disactv1_trace",
		"trace-disable-active",
	)
	terminal := resolveInteraction(
		t,
		registry,
		makeWaiting(
			t,
			registry,
			clock,
			"interaction_disterm1_trace",
			"trace-disable-terminal",
		),
	)
	if err := registry.SetTraceCaptureProtection(false, 0); err != nil {
		t.Fatalf("disable trace capture: %v", err)
	}
	active, activeJournal, _ := registry.GetTraceCapture(active.ID)
	if active.TraceCapturePending || len(activeJournal) == 0 {
		t.Fatalf("active journal was lost during disable: (%#v, %#v)", active, activeJournal)
	}
	terminal, terminalJournal, _ := registry.GetTraceCapture(terminal.ID)
	if !terminal.TraceCapturePending || len(terminalJournal) == 0 {
		t.Fatalf(
			"pending terminal journal was lost on disable: (%#v, %#v)",
			terminal,
			terminalJournal,
		)
	}
	if _, err := registry.ClaimAnswer(
		active.ID,
		active.Revision,
		Answer{Text: "Staging"},
		OutcomeAnswered,
	); err != nil {
		t.Fatalf("mutate while capture disabled: %v", err)
	}
	active, activeJournal, _ = registry.GetTraceCapture(active.ID)
	if active.TraceCapturePending || len(activeJournal) != 0 ||
		active.TraceCaptureDropped != 0 {
		t.Fatalf("disabled mutation retained active journal: (%#v, %#v)", active, activeJournal)
	}
}

func TestDurableTraceCaptureReadsObserveAnotherRegistryInstance(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	other := NewRegistryWithOptions(path, Options{Now: clock.Now})
	terminal := resolveInteraction(
		t,
		other,
		makeWaiting(
			t,
			other,
			clock,
			"interaction_cross_instance_trace",
			"trace-cross-instance",
		),
	)

	pending, err := registry.ListPendingTraceCaptures(context.Background(), 10)
	if err != nil || len(pending) != 1 || pending[0].ID != terminal.ID {
		t.Fatalf("durable pending captures = (%#v, %v)", pending, err)
	}
	record, events, ok, err := registry.LoadTraceCapture(
		context.Background(),
		terminal.ID,
	)
	if err != nil || !ok || record.LastEventSeq != terminal.LastEventSeq ||
		len(events) == 0 ||
		events[len(events)-1].Sequence != terminal.LastEventSeq {
		t.Fatalf(
			"durable trace capture = (%#v, %#v, %v, %v)",
			record,
			events,
			ok,
			err,
		)
	}
}

func TestDurableTraceCaptureReadHonorsCancellationDuringLockContention(
	t *testing.T,
) {
	t.Run("registry mutex", func(t *testing.T) {
		registry, _, _ := newTestRegistry(t)
		registry.mu.Lock()
		ctx, cancel := context.WithTimeout(
			context.Background(),
			50*time.Millisecond,
		)
		defer cancel()

		started := time.Now()
		_, err := registry.ListPendingTraceCaptures(ctx, 10)
		registry.mu.Unlock()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("mutex contention error = %v", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("mutex cancellation took %v", elapsed)
		}
	})
	t.Run("store lock", func(t *testing.T) {
		registry, _, path := newTestRegistry(t)
		release, err := acquireStoreFileLock(path + ".lock")
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		ctx, cancel := context.WithTimeout(
			context.Background(),
			50*time.Millisecond,
		)
		defer cancel()

		started := time.Now()
		_, err = registry.ListPendingTraceCaptures(ctx, 10)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("store contention error = %v", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("store cancellation took %v", elapsed)
		}
	})
}

func TestTraceCaptureConfirmationHonorsCancellationDuringStoreContention(
	t *testing.T,
) {
	registry, clock, path := newTestRegistry(t)
	if err := registry.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatal(err)
	}
	record := resolveInteraction(
		t,
		registry,
		makeWaiting(
			t,
			registry,
			clock,
			"interaction_trace_confirm_cancel",
			"trace-confirm-cancel",
		),
	)
	release, err := acquireStoreFileLock(path + ".lock")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, confirmed, err := registry.ConfirmTraceCapturePersisted(
		ctx,
		record.ID,
		record.LastEventSeq,
	)
	if !errors.Is(err, context.DeadlineExceeded) || confirmed {
		t.Fatalf("confirmation contention = (%v, %v)", confirmed, err)
	}
}

func TestTraceCaptureDoesNotPromoteLegacyGlobalEvents(t *testing.T) {
	registry, clock, path := newTestRegistry(t)
	record := makeWaiting(
		t,
		registry,
		clock,
		"interaction_trace_legacy",
		"trace-legacy",
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	for index := range snapshot.Events {
		snapshot.Events[index].Fingerprint = ""
	}
	data, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	reloaded := NewRegistryWithOptions(path, Options{Now: clock.Now})
	if err = reloaded.LastLoadError(); err != nil {
		t.Fatalf("reload legacy global events: %v", err)
	}
	if err = reloaded.SetTraceCaptureProtection(true, 20); err != nil {
		t.Fatalf("enable trace capture: %v", err)
	}
	if _, journal, _ := reloaded.GetTraceCapture(record.ID); len(journal) != 0 {
		t.Fatalf("legacy events were promoted into journal: %#v", journal)
	}
	record, err = reloaded.ClaimAnswer(
		record.ID,
		record.Revision,
		Answer{Text: "Staging"},
		OutcomeAnswered,
	)
	if err != nil {
		t.Fatal(err)
	}
	stored, journal, _ := reloaded.GetTraceCapture(record.ID)
	if len(journal) != 1 || journal[0].Sequence != stored.LastEventSeq ||
		stored.TraceCaptureDropped != int(stored.LastEventSeq-1) {
		t.Fatalf("valid suffix journal = (%#v, %#v)", stored, journal)
	}
}
