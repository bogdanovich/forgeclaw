package channels

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func newTestToolFeedbackCoordinator(separate bool) *ToolFeedbackCoordinator {
	return NewToolFeedbackCoordinator(ToolFeedbackAnimatorConfig{
		AnimationInterval: time.Hour,
	}, separate)
}

func TestToolFeedbackCoordinator_PermanentEditFailureSendsReplacement(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	var events []string
	operations := toolFeedbackOperations{
		edit: func(_ context.Context, _, messageID, _ string) error {
			events = append(events, "edit:"+messageID)
			if messageID == "progress-1" {
				return fmt.Errorf("card cannot be patched: %w", ErrSendFailed)
			}
			return nil
		},
		delete: func(_ context.Context, _, messageID string) error {
			events = append(events, "delete:"+messageID)
			return nil
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) {
			events = append(events, "send:progress-1")
			return []string{"progress-1"}, nil
		},
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	ids, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "second", operations,
		func(context.Context, string) ([]string, error) {
			events = append(events, "send:progress-2")
			return []string{"progress-2"}, nil
		},
	)
	if err != nil {
		t.Fatalf("replacement Deliver() error = %v", err)
	}
	if !slices.Equal(ids, []string{"progress-2"}) {
		t.Fatalf("replacement IDs = %v, want [progress-2]", ids)
	}
	want := []string{
		"send:progress-1", "edit:progress-1", "send:progress-2", "delete:progress-1",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestToolFeedbackCoordinator_TransientEditFailureRetainsEntry(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "temporary", err: ErrTemporary},
		{name: "rate limit", err: ErrRateLimit},
		{name: "timeout", err: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator := newTestToolFeedbackCoordinator(false)
			defer coordinator.StopAll()
			deletes := 0
			sends := 0
			operations := toolFeedbackOperations{
				edit: func(context.Context, string, string, string) error { return tt.err },
				delete: func(context.Context, string, string) error {
					deletes++
					return nil
				},
			}
			if _, err := coordinator.Deliver(
				context.Background(), "feishu:chat-1", "chat-1", "first", operations,
				func(context.Context, string) ([]string, error) {
					sends++
					return []string{"progress-1"}, nil
				},
			); err != nil {
				t.Fatalf("initial Deliver() error = %v", err)
			}
			_, err := coordinator.Deliver(
				context.Background(), "feishu:chat-1", "chat-1", "second", operations,
				func(context.Context, string) ([]string, error) {
					sends++
					return []string{"progress-2"}, nil
				},
			)
			if !errors.Is(err, tt.err) {
				t.Fatalf("update error = %v, want %v", err, tt.err)
			}
			if sends != 1 || deletes != 0 || coordinator.ActiveCount() != 1 {
				t.Fatalf(
					"sends=%d deletes=%d active=%d, want 1/0/1",
					sends,
					deletes,
					coordinator.ActiveCount(),
				)
			}
		})
	}
}

func TestToolFeedbackCoordinator_ReplacementSendFailureRetainsCurrent(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	failEdit := true
	deletes := 0
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error {
			if failEdit {
				return ErrSendFailed
			}
			return nil
		},
		delete: func(context.Context, string, string) error {
			deletes++
			return nil
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	_, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "second", operations,
		func(context.Context, string) ([]string, error) { return nil, ErrTemporary },
	)
	if !errors.Is(err, ErrTemporary) {
		t.Fatalf("replacement error = %v, want ErrTemporary", err)
	}
	if deletes != 0 || coordinator.ActiveCount() != 1 {
		t.Fatalf("deletes=%d active=%d, want 0/1", deletes, coordinator.ActiveCount())
	}

	failEdit = false
	ids, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "third", operations,
		func(context.Context, string) ([]string, error) {
			t.Fatal("retained current message unexpectedly sent a replacement")
			return nil, nil
		},
	)
	if err != nil || !slices.Equal(ids, []string{"progress-1"}) {
		t.Fatalf("retained update = (%v, %v), want [progress-1], nil", ids, err)
	}
}

func TestToolFeedbackCoordinator_CleanupFailureIsRetried(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	deleteAttempts := 0
	operations := toolFeedbackOperations{
		edit: func(_ context.Context, _, messageID, _ string) error {
			if messageID == "progress-1" {
				return ErrSendFailed
			}
			return nil
		},
		delete: func(context.Context, string, string) error {
			deleteAttempts++
			if deleteAttempts == 1 {
				return ErrTemporary
			}
			return nil
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	ids, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "second", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-2"}, nil },
	)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) {
		t.Fatalf("replacement = (%v, %v), want [progress-2], nil", ids, err)
	}
	ids, err = coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "third", operations,
		func(context.Context, string) ([]string, error) {
			t.Fatal("cleanup retry unexpectedly sent another replacement")
			return nil, nil
		},
	)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) || deleteAttempts != 2 {
		t.Fatalf(
			"cleanup retry = (%v, %v), attempts=%d; want [progress-2], nil, 2",
			ids, err, deleteAttempts,
		)
	}
}

func TestToolFeedbackCoordinator_CleanupFailureDoesNotBlockCurrentDelivery(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	deleteAttempts := 0
	var editedMessageID string
	operations := toolFeedbackOperations{
		edit: func(_ context.Context, _, messageID, _ string) error {
			if messageID == "progress-1" {
				return ErrSendFailed
			}
			editedMessageID = messageID
			return nil
		},
		delete: func(context.Context, string, string) error {
			deleteAttempts++
			return ErrTemporary
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	ids, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "second", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-2"}, nil },
	)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) {
		t.Fatalf("replacement = (%v, %v), want [progress-2], nil", ids, err)
	}
	ids, err = coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "third", operations,
		func(context.Context, string) ([]string, error) {
			t.Fatal("cleanup failure unexpectedly forced another replacement")
			return nil, nil
		},
	)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) ||
		editedMessageID != "progress-2" || deleteAttempts != 2 {
		t.Fatalf(
			"current update = (%v, %v), edited=%q deletes=%d; want [progress-2], nil, progress-2, 2",
			ids, err, editedMessageID, deleteAttempts,
		)
	}
}

func TestToolFeedbackCoordinator_ReplacementTerminalDeletesBothMessages(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	replacementStarted := make(chan struct{})
	releaseReplacement := make(chan struct{})
	deleted := make(chan string, 2)
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return ErrSendFailed },
		delete: func(_ context.Context, _, messageID string) error {
			deleted <- messageID
			return nil
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	deliverDone := make(chan []string, 1)
	go func() {
		ids, _ := coordinator.Deliver(
			context.Background(), "feishu:chat-1", "chat-1", "second", operations,
			func(context.Context, string) ([]string, error) {
				close(replacementStarted)
				<-releaseReplacement
				return []string{"progress-2"}, nil
			},
		)
		deliverDone <- ids
	}()
	<-replacementStarted
	terminal := coordinator.BeginTerminal("feishu:chat-1")
	terminalDone := make(chan struct{})
	go func() {
		coordinator.CompleteTerminal(context.Background(), terminal, true)
		close(terminalDone)
	}()
	close(releaseReplacement)
	if ids := <-deliverDone; len(ids) != 0 {
		t.Fatalf("replacement IDs = %v, want none after terminal", ids)
	}
	<-terminalDone
	got := []string{<-deleted, <-deleted}
	if want := []string{"progress-2", "progress-1"}; !slices.Equal(got, want) {
		t.Fatalf("deleted = %v, want %v", got, want)
	}
}

func TestToolFeedbackCoordinator_TerminalRetainsFailedCleanupUntilRetry(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	allowCleanup := false
	var cleaned []string
	operations := toolFeedbackOperations{
		edit: func(_ context.Context, _, messageID, _ string) error {
			if messageID == "progress-1" {
				return ErrSendFailed
			}
			return nil
		},
		delete: func(_ context.Context, _, messageID string) error {
			if !allowCleanup {
				return ErrTemporary
			}
			cleaned = append(cleaned, messageID)
			return nil
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	ids, err := coordinator.Deliver(
		context.Background(), "feishu:chat-1", "chat-1", "second", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-2"}, nil },
	)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) {
		t.Fatalf("replacement = (%v, %v), want [progress-2], nil", ids, err)
	}
	terminal := coordinator.BeginTransientTerminal("feishu:chat-1")
	coordinator.CompleteTerminal(context.Background(), terminal, true)
	if count := coordinator.ActiveCount(); count != 1 {
		t.Fatalf("ActiveCount() after failed terminal cleanup = %d, want 1", count)
	}
	entry := coordinator.findEntry("feishu:chat-1")
	if entry == nil {
		t.Fatal("terminal entry removed with pending cleanup")
	}
	entry.mu.Lock()
	pending := len(entry.pendingCleanup)
	entry.mu.Unlock()
	if pending != 2 {
		t.Fatalf("pending cleanup = %d, want old and current messages", pending)
	}

	allowCleanup = true
	coordinator.maintainCleanup("feishu:chat-1", entry)
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() after cleanup retry = %d, want 0", count)
	}
	if want := []string{"progress-1", "progress-2"}; !slices.Equal(cleaned, want) {
		t.Fatalf("cleaned = %v, want %v", cleaned, want)
	}
}

func TestToolFeedbackCoordinator_TerminalDropsPermanentCleanupFailure(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	deleteCalls := 0
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(context.Context, string, string) error {
			deleteCalls++
			return ErrSendFailed
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "working", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	terminal := coordinator.BeginTransientTerminal("telegram:chat-1")
	coordinator.CompleteTerminal(context.Background(), terminal, true)
	if deleteCalls != 1 {
		t.Fatalf("delete calls = %d, want 1", deleteCalls)
	}
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() after permanent cleanup failure = %d, want 0", count)
	}
}

func TestToolFeedbackCoordinator_TerminalExpiresUnknownCleanupFailure(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	deleteCalls := 0
	deleteErr := errors.New("unclassified delete failure")
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(context.Context, string, string) error {
			deleteCalls++
			return deleteErr
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "working", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	terminal := coordinator.BeginTransientTerminal("telegram:chat-1")
	coordinator.CompleteTerminal(context.Background(), terminal, true)
	entry := coordinator.findEntry("telegram:chat-1")
	if entry == nil {
		t.Fatal("terminal entry removed before cleanup retention expired")
	}
	entry.mu.Lock()
	pending := len(entry.pendingCleanup)
	if pending != 1 {
		entry.mu.Unlock()
		t.Fatalf("pending cleanup = %d, want 1", pending)
	}
	entry.pendingCleanup[0].expiresAt = time.Now().Add(-time.Second)
	entry.mu.Unlock()

	coordinator.maintainCleanup("telegram:chat-1", entry)
	if deleteCalls != 1 {
		t.Fatalf("delete calls after expiry = %d, want 1", deleteCalls)
	}
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() after cleanup expiry = %d, want 0", count)
	}
}

func TestToolFeedbackCoordinator_PendingSendTerminalDeletesLateMessage(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	deleted := make(chan string, 1)
	result := make(chan []string, 1)

	go func() {
		ids, err := coordinator.Deliver(
			context.Background(),
			"telegram:chat-1",
			"chat-1",
			"Working...\n- tool: exec",
			toolFeedbackOperations{delete: func(_ context.Context, _, messageID string) error {
				deleted <- messageID
				return nil
			}},
			func(context.Context, string) ([]string, error) {
				close(sendStarted)
				<-releaseSend
				return []string{"progress-1"}, nil
			},
		)
		if err != nil {
			t.Errorf("Deliver() error = %v", err)
		}
		result <- ids
	}()
	<-sendStarted

	started := time.Now()
	terminal := coordinator.BeginTerminal("telegram:chat-1")
	if terminal == nil {
		t.Fatal("BeginTerminal() = nil, want pending state")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("BeginTerminal() blocked for %v", elapsed)
	}
	completed := make(chan struct{})
	go func() {
		coordinator.CompleteTerminal(context.Background(), terminal, true)
		close(completed)
	}()

	select {
	case <-completed:
		t.Fatal("terminal cleanup completed before pending send settled")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseSend)
	if ids := <-result; len(ids) != 0 {
		t.Fatalf("superseded Deliver() IDs = %v, want none", ids)
	}
	select {
	case messageID := <-deleted:
		if messageID != "progress-1" {
			t.Fatalf("deleted message = %q, want progress-1", messageID)
		}
	case <-time.After(time.Second):
		t.Fatal("late progress message was not deleted")
	}
	<-completed
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() = %d, want 0", count)
	}
}

func TestToolFeedbackCoordinator_AbsentTerminalBlocksLateDelivery(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	terminal := coordinator.BeginTerminal("telegram:chat-1")
	if terminal == nil {
		t.Fatal("BeginTerminal() = nil, want absent-entry barrier")
	}
	coordinator.CompleteTerminal(context.Background(), terminal, true)

	sends := 0
	ids, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "stale", toolFeedbackOperations{},
		func(context.Context, string) ([]string, error) {
			sends++
			return []string{"progress-1"}, nil
		},
	)
	if err != nil || len(ids) != 0 || sends != 0 {
		t.Fatalf("blocked Deliver() = (%v, %v), sends %d", ids, err, sends)
	}
	coordinator.ReleaseTerminal("telegram:chat-1")
	ids, err = coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "next turn", toolFeedbackOperations{},
		func(context.Context, string) ([]string, error) {
			sends++
			return []string{"progress-2"}, nil
		},
	)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) || sends != 1 {
		t.Fatalf("released Deliver() = (%v, %v), sends %d", ids, err, sends)
	}
}

func TestToolFeedbackCoordinator_FailedAbsentTerminalReleasesBarrier(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	terminal := coordinator.BeginTerminal("telegram:chat-1")
	coordinator.CompleteTerminal(context.Background(), terminal, false)

	sends := 0
	ids, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "feedback", toolFeedbackOperations{},
		func(context.Context, string) ([]string, error) {
			sends++
			return []string{"progress-1"}, nil
		},
	)
	if err != nil || !slices.Equal(ids, []string{"progress-1"}) || sends != 1 {
		t.Fatalf("Deliver() = (%v, %v), sends %d", ids, err, sends)
	}
}

func TestToolFeedbackCoordinator_ConcurrentTerminalSuccessWins(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	var deleted []string
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(_ context.Context, _, messageID string) error {
			deleted = append(deleted, messageID)
			return nil
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "working", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	first := coordinator.BeginTerminal("telegram:chat-1")
	second := coordinator.BeginTerminal("telegram:chat-1")
	coordinator.CompleteTerminal(context.Background(), first, true)
	coordinator.CompleteTerminal(context.Background(), second, false)

	sends := 0
	ids, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "stale", operations,
		func(context.Context, string) ([]string, error) {
			sends++
			return []string{"progress-2"}, nil
		},
	)
	if err != nil || len(ids) != 0 || sends != 0 {
		t.Fatalf("late Deliver() = (%v, %v), sends %d, want suppressed", ids, err, sends)
	}
	if !slices.Equal(deleted, []string{"progress-1"}) {
		t.Fatalf("deleted = %v, want [progress-1]", deleted)
	}
}

func TestToolFeedbackCoordinator_ConcurrentTerminalWaitsForAllFailures(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
	}
	if _, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "working", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	first := coordinator.BeginTerminal("telegram:chat-1")
	second := coordinator.BeginTerminal("telegram:chat-1")
	coordinator.CompleteTerminal(context.Background(), first, false)
	if ids, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "blocked", operations,
		func(context.Context, string) ([]string, error) {
			t.Fatal("feedback sent while another terminal attempt was pending")
			return nil, nil
		},
	); err != nil || len(ids) != 0 {
		t.Fatalf("blocked Deliver() = (%v, %v)", ids, err)
	}
	coordinator.CompleteTerminal(context.Background(), second, false)

	ids, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "resumed", operations,
		func(context.Context, string) ([]string, error) {
			t.Fatal("failed terminals should resume the tracked message")
			return nil, nil
		},
	)
	if err != nil || !slices.Equal(ids, []string{"progress-1"}) {
		t.Fatalf("resumed Deliver() = (%v, %v), want progress-1", ids, err)
	}
}

func TestToolFeedbackCoordinator_NewTurnSupersedesTerminalTombstone(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	turnOneKey := "telegram:chat-1\x00turn\x00workspace\x00turn-1"
	turnTwoKey := "telegram:chat-1\x00turn\x00workspace\x00turn-2"
	terminal := coordinator.BeginTerminal(turnOneKey)
	coordinator.CompleteTerminal(context.Background(), terminal, true)

	sends := 0
	send := func(context.Context, string) ([]string, error) {
		sends++
		return []string{fmt.Sprintf("progress-%d", sends)}, nil
	}
	ids, err := coordinator.Deliver(
		context.Background(), turnOneKey, "chat-1", "stale",
		toolFeedbackOperations{}, send,
	)
	if err != nil || len(ids) != 0 || sends != 0 {
		t.Fatalf("same-turn Deliver() = (%v, %v), sends %d", ids, err, sends)
	}
	ids, err = coordinator.Deliver(
		context.Background(), turnTwoKey, "chat-1", "next turn",
		toolFeedbackOperations{}, send,
	)
	if err != nil || !slices.Equal(ids, []string{"progress-1"}) || sends != 1 {
		t.Fatalf("next-turn Deliver() = (%v, %v), sends %d", ids, err, sends)
	}
}

func TestToolFeedbackCoordinator_TransientTerminalDoesNotBlockLaterUnscopedFeedback(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	terminal := coordinator.BeginTransientTerminal("telegram:chat-1")
	coordinator.CompleteTerminal(context.Background(), terminal, true)

	sends := 0
	ids, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "next unscoped turn",
		toolFeedbackOperations{},
		func(context.Context, string) ([]string, error) {
			sends++
			return []string{"progress-1"}, nil
		},
	)
	if err != nil || !slices.Equal(ids, []string{"progress-1"}) || sends != 1 {
		t.Fatalf("Deliver() = (%v, %v), sends %d, want later unscoped delivery", ids, err, sends)
	}
}

func TestToolFeedbackCoordinator_TransientCleanupFailureDoesNotBlockLaterFeedback(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	deleteCalls := 0
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(context.Context, string, string) error {
			deleteCalls++
			return ErrTemporary
		},
	}
	sends := 0
	send := func(context.Context, string) ([]string, error) {
		sends++
		return []string{fmt.Sprintf("progress-%d", sends)}, nil
	}
	if _, err := coordinator.Deliver(
		t.Context(), "telegram:chat-1", "chat-1", "checking", operations, send,
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	terminal := coordinator.BeginTransientTerminal("telegram:chat-1")
	coordinator.CompleteTerminal(t.Context(), terminal, true)

	ids, err := coordinator.Deliver(
		t.Context(), "telegram:chat-1", "chat-1", "checking ports", operations, send,
	)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) || sends != 2 {
		t.Fatalf("later Deliver() = (%v, %v), sends %d, want progress-2", ids, err, sends)
	}
	if deleteCalls < 2 {
		t.Fatalf("delete calls = %d, want initial attempt and delivery retry", deleteCalls)
	}
}

func TestToolFeedbackCoordinator_RetainedTerminalWinsOverOverlappingTransient(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	var deleted []string
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(_ context.Context, _ string, messageID string) error {
			deleted = append(deleted, messageID)
			return nil
		},
	}
	sends := 0
	send := func(context.Context, string) ([]string, error) {
		sends++
		return []string{fmt.Sprintf("progress-%d", sends)}, nil
	}
	const key = "telegram:chat-1"
	if _, err := coordinator.Deliver(t.Context(), key, "chat-1", "checking", operations, send); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	transient := coordinator.BeginTransientTerminal(key)
	retained := coordinator.BeginTerminal(key)
	coordinator.CompleteTerminal(t.Context(), transient, true)

	if ids, err := coordinator.Deliver(
		t.Context(), key, "chat-1", "between terminals", operations, send,
	); err != nil || len(ids) != 0 || sends != 1 {
		t.Fatalf("pending-final Deliver() = (%v, %v), sends %d, want suppressed", ids, err, sends)
	}
	coordinator.CompleteTerminal(t.Context(), retained, true)
	if !slices.Equal(deleted, []string{"progress-1"}) {
		t.Fatalf("deleted = %v, want retained final cleanup", deleted)
	}
	if ids, err := coordinator.Deliver(
		t.Context(), key, "chat-1", "after final", operations, send,
	); err != nil || len(ids) != 0 || sends != 1 {
		t.Fatalf("post-final Deliver() = (%v, %v), sends %d, want tombstone suppression", ids, err, sends)
	}
}

func TestToolFeedbackCoordinator_TransientSuccessSurvivesRetainedFailure(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	deleteCalls := 0
	editCalls := 0
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error {
			editCalls++
			return nil
		},
		delete: func(context.Context, string, string) error {
			deleteCalls++
			if deleteCalls < 3 {
				return ErrTemporary
			}
			return nil
		},
	}
	sends := 0
	send := func(context.Context, string) ([]string, error) {
		sends++
		return []string{fmt.Sprintf("progress-%d", sends)}, nil
	}
	const key = "telegram:chat-1"
	if _, err := coordinator.Deliver(t.Context(), key, "chat-1", "checking", operations, send); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	transient := coordinator.BeginTransientTerminal(key)
	retained := coordinator.BeginTerminal(key)
	coordinator.CompleteTerminal(t.Context(), transient, true)
	coordinator.CompleteTerminal(t.Context(), retained, false)

	ids, err := coordinator.Deliver(t.Context(), key, "chat-1", "checking ports", operations, send)
	if err != nil || !slices.Equal(ids, []string{"progress-2"}) || sends != 2 {
		t.Fatalf("later Deliver() = (%v, %v), sends %d, want new progress-2", ids, err, sends)
	}
	if editCalls != 0 {
		t.Fatalf("edit calls = %d, want stale carrier left detached", editCalls)
	}
	if deleteCalls != 3 {
		t.Fatalf("delete calls = %d, want transient attempt, final retry, and delivery retry", deleteCalls)
	}
}

func TestToolFeedbackCoordinator_TransientCleanupRetriesWhileRetainedPending(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	retried := make(chan struct{})
	var retryOnce sync.Once
	deleteCalls := 0
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(context.Context, string, string) error {
			deleteCalls++
			if deleteCalls == 1 {
				return ErrTemporary
			}
			retryOnce.Do(func() { close(retried) })
			return nil
		},
	}
	sends := 0
	send := func(context.Context, string) ([]string, error) {
		sends++
		return []string{fmt.Sprintf("progress-%d", sends)}, nil
	}
	const key = "telegram:chat-1"
	if _, err := coordinator.Deliver(t.Context(), key, "chat-1", "checking", operations, send); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	transient := coordinator.BeginTransientTerminal(key)
	retained := coordinator.BeginTerminal(key)
	coordinator.CompleteTerminal(t.Context(), transient, true)

	select {
	case <-retried:
	case <-time.After(toolFeedbackCleanupRetryDelay + 2*time.Second):
		t.Fatal("transient cleanup was not retried while retained terminal remained pending")
	}
	if deleteCalls != 2 {
		t.Fatalf("delete calls = %d, want initial attempt and timer-driven retry", deleteCalls)
	}
	if ids, err := coordinator.Deliver(
		t.Context(), key, "chat-1", "before final outcome", operations, send,
	); err != nil || len(ids) != 0 || sends != 1 {
		t.Fatalf("pending-final Deliver() = (%v, %v), sends %d, want retained barrier", ids, err, sends)
	}

	coordinator.CompleteTerminal(t.Context(), retained, false)
	if ids, err := coordinator.Deliver(
		t.Context(), key, "chat-1", "after failed final", operations, send,
	); err != nil || !slices.Equal(ids, []string{"progress-2"}) || sends != 2 {
		t.Fatalf("post-final Deliver() = (%v, %v), sends %d, want new progress-2", ids, err, sends)
	}
}

func TestToolFeedbackCoordinator_TransientCleanupSurvivesSequentialRetainedAdmission(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	retried := make(chan struct{})
	var retryOnce sync.Once
	deleteCalls := 0
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(context.Context, string, string) error {
			deleteCalls++
			if deleteCalls == 1 {
				return ErrTemporary
			}
			retryOnce.Do(func() { close(retried) })
			return nil
		},
	}
	sends := 0
	send := func(context.Context, string) ([]string, error) {
		sends++
		return []string{fmt.Sprintf("progress-%d", sends)}, nil
	}
	const key = "telegram:chat-1"
	if _, err := coordinator.Deliver(t.Context(), key, "chat-1", "checking", operations, send); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	transient := coordinator.BeginTransientTerminal(key)
	coordinator.CompleteTerminal(t.Context(), transient, true)
	retained := coordinator.BeginTerminal(key)

	select {
	case <-retried:
	case <-time.After(toolFeedbackCleanupRetryDelay + 2*time.Second):
		t.Fatal("cleanup did not survive the sequential retained terminal generation")
	}
	if deleteCalls != 2 {
		t.Fatalf("delete calls = %d, want initial attempt and timer-driven retry", deleteCalls)
	}
	if ids, err := coordinator.Deliver(
		t.Context(), key, "chat-1", "before final outcome", operations, send,
	); err != nil || len(ids) != 0 || sends != 1 {
		t.Fatalf("pending-final Deliver() = (%v, %v), sends %d, want retained barrier", ids, err, sends)
	}
	coordinator.CompleteTerminal(t.Context(), retained, false)
}

func TestToolFeedbackCoordinator_RetainedCleanupContinuesIntoTombstoneExpiry(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	allowCleanup := false
	deleteCalls := 0
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(context.Context, string, string) error {
			deleteCalls++
			if !allowCleanup {
				return ErrTemporary
			}
			return nil
		},
	}
	const key = "telegram:chat-1"
	if _, err := coordinator.Deliver(
		t.Context(), key, "chat-1", "checking", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	retained := coordinator.BeginTerminal(key)
	coordinator.CompleteTerminal(t.Context(), retained, true)
	entry := coordinator.findEntry(key)
	if entry == nil {
		t.Fatal("retained terminal entry removed before cleanup retry")
	}

	allowCleanup = true
	coordinator.maintainCleanup(key, entry)
	if deleteCalls != 2 {
		t.Fatalf("delete calls = %d, want initial attempt and cleanup retry", deleteCalls)
	}
	if got := coordinator.findEntry(key); got != entry {
		t.Fatalf("entry after cleanup = %p, want retained tombstone %p", got, entry)
	}
	entry.mu.Lock()
	entry.terminalUntil = time.Now().Add(-time.Second)
	entry.mu.Unlock()
	coordinator.maintainTerminal(retained)
	if got := coordinator.findEntry(key); got != nil {
		t.Fatalf("entry after tombstone expiry = %p, want nil", got)
	}
}

func TestToolFeedbackCoordinator_RetainedTerminalJoinsTransientCleanup(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	deleteStarted := make(chan struct{})
	releaseDelete := make(chan struct{})
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error { return nil },
		delete: func(context.Context, string, string) error {
			close(deleteStarted)
			<-releaseDelete
			return nil
		},
	}
	const key = "telegram:chat-1"
	sends := 0
	send := func(context.Context, string) ([]string, error) {
		sends++
		return []string{fmt.Sprintf("progress-%d", sends)}, nil
	}
	if _, err := coordinator.Deliver(t.Context(), key, "chat-1", "checking", operations, send); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	transient := coordinator.BeginTransientTerminal(key)
	transientDone := make(chan struct{})
	go func() {
		coordinator.CompleteTerminal(t.Context(), transient, true)
		close(transientDone)
	}()
	select {
	case <-deleteStarted:
	case <-time.After(time.Second):
		t.Fatal("transient cleanup did not start")
	}

	retainedResult := make(chan *toolFeedbackTerminal, 1)
	go func() { retainedResult <- coordinator.BeginTerminal(key) }()
	var retained *toolFeedbackTerminal
	select {
	case retained = <-retainedResult:
		if retained == nil || retained.absorbed {
			t.Fatalf("retained terminal = %#v, want active terminal", retained)
		}
	case <-time.After(time.Second):
		t.Fatal("retained terminal did not join in-progress transient cleanup")
	}
	close(releaseDelete)
	select {
	case <-transientDone:
	case <-time.After(time.Second):
		t.Fatal("transient cleanup did not finish")
	}
	coordinator.CompleteTerminal(t.Context(), retained, true)

	if ids, err := coordinator.Deliver(
		t.Context(), key, "chat-1", "after final", operations, send,
	); err != nil || len(ids) != 0 || sends != 1 {
		t.Fatalf("post-final Deliver() = (%v, %v), sends %d, want tombstone suppression", ids, err, sends)
	}
}

func TestToolFeedbackCoordinator_SeparateDeliveryAndStopDoNotDeadlock(t *testing.T) {
	for range 100 {
		coordinator := newTestToolFeedbackCoordinator(true)
		if _, err := coordinator.Deliver(
			context.Background(), "telegram:chat-1", "chat-1", "first",
			toolFeedbackOperations{edit: func(context.Context, string, string, string) error { return nil }},
			func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
		); err != nil {
			t.Fatalf("initial Deliver() error = %v", err)
		}
		done := make(chan struct{}, 2)
		go func() {
			_, _ = coordinator.Deliver(
				context.Background(), "telegram:chat-1", "chat-1", "second",
				toolFeedbackOperations{edit: func(context.Context, string, string, string) error { return nil }},
				func(context.Context, string) ([]string, error) { return []string{"progress-2"}, nil },
			)
			done <- struct{}{}
		}()
		go func() {
			coordinator.StopAll()
			done <- struct{}{}
		}()
		for range 2 {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Deliver and StopAll deadlocked")
			}
		}
	}
}

func TestToolFeedbackCoordinator_UpdateTerminalSerializesCleanup(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	editStarted := make(chan struct{})
	releaseEdit := make(chan struct{})
	var deleted []string
	var mu sync.Mutex
	operations := toolFeedbackOperations{
		edit: func(context.Context, string, string, string) error {
			close(editStarted)
			<-releaseEdit
			return nil
		},
		delete: func(_ context.Context, _, messageID string) error {
			mu.Lock()
			deleted = append(deleted, messageID)
			mu.Unlock()
			return nil
		},
	}
	if _, err := coordinator.Deliver(
		context.Background(), "slack:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	updateDone := make(chan error, 1)
	go func() {
		_, err := coordinator.Deliver(
			context.Background(), "slack:chat-1", "chat-1", "second", operations,
			func(context.Context, string) ([]string, error) {
				t.Error("active update unexpectedly sent a new message")
				return nil, nil
			},
		)
		updateDone <- err
	}()
	<-editStarted
	terminal := coordinator.BeginTerminal("slack:chat-1")
	completed := make(chan struct{})
	go func() {
		coordinator.CompleteTerminal(context.Background(), terminal, true)
		close(completed)
	}()
	select {
	case <-completed:
		t.Fatal("terminal cleanup overtook active edit")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseEdit)
	if err := <-updateDone; err != nil {
		t.Fatalf("update Deliver() error = %v", err)
	}
	<-completed
	mu.Lock()
	defer mu.Unlock()
	if len(deleted) != 1 || deleted[0] != "progress-1" {
		t.Fatalf("deleted messages = %v, want [progress-1]", deleted)
	}
}

func TestToolFeedbackCoordinator_FailedTerminalResumesActiveMessage(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	var edits []string
	operations := toolFeedbackOperations{edit: func(_ context.Context, _, _ string, content string) error {
		edits = append(edits, content)
		return nil
	}}
	if _, err := coordinator.Deliver(
		context.Background(), "discord:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, nil },
	); err != nil {
		t.Fatalf("initial Deliver() error = %v", err)
	}
	terminal := coordinator.BeginTerminal("discord:chat-1")
	coordinator.CompleteTerminal(context.Background(), terminal, false)
	ids, err := coordinator.Deliver(
		context.Background(), "discord:chat-1", "chat-1", "second", operations,
		func(context.Context, string) ([]string, error) {
			t.Fatal("failed terminal should resume the active message")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("resumed Deliver() error = %v", err)
	}
	if len(ids) != 1 || ids[0] != "progress-1" || len(edits) != 1 {
		t.Fatalf("resumed update = ids %v, edits %v", ids, edits)
	}
}

func TestToolFeedbackCoordinator_SendFailureRemovesIdleState(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	sendErr := errors.New("send failed")
	_, err := coordinator.Deliver(
		context.Background(), "matrix:chat-1", "chat-1", "feedback", toolFeedbackOperations{},
		func(context.Context, string) ([]string, error) { return nil, sendErr },
	)
	if !errors.Is(err, sendErr) {
		t.Fatalf("Deliver() error = %v, want send failure", err)
	}
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() = %d, want 0", count)
	}
}

func TestToolFeedbackCoordinator_PartialSendRemainsTracked(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	partialErr := errors.New("second chunk failed")
	var edits []string
	operations := toolFeedbackOperations{edit: func(_ context.Context, _, messageID, _ string) error {
		edits = append(edits, messageID)
		return nil
	}}
	ids, err := coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "first", operations,
		func(context.Context, string) ([]string, error) { return []string{"progress-1"}, partialErr },
	)
	if !errors.Is(err, partialErr) || len(ids) != 1 || ids[0] != "progress-1" {
		t.Fatalf("partial Deliver() = (%v, %v), want progress-1 and partial error", ids, err)
	}
	ids, err = coordinator.Deliver(
		context.Background(), "telegram:chat-1", "chat-1", "second", operations,
		func(context.Context, string) ([]string, error) {
			t.Fatal("tracked partial send should be edited")
			return nil, nil
		},
	)
	if err != nil || len(ids) != 1 || ids[0] != "progress-1" || !slices.Equal(edits, []string{"progress-1"}) {
		t.Fatalf("tracked update = (%v, %v, %v)", ids, err, edits)
	}
}

func TestToolFeedbackCoordinator_SeparateMessagesDoesNotEditOrDelete(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(true)
	defer coordinator.StopAll()
	var sends, edits, deletes int
	operations := toolFeedbackOperations{
		edit:   func(context.Context, string, string, string) error { edits++; return nil },
		delete: func(context.Context, string, string) error { deletes++; return nil },
	}
	for _, id := range []string{"progress-1", "progress-2"} {
		if _, err := coordinator.Deliver(
			context.Background(), "mintclaw:chat-1", "chat-1", id, operations,
			func(context.Context, string) ([]string, error) { sends++; return []string{id}, nil },
		); err != nil {
			t.Fatalf("Deliver(%s) error = %v", id, err)
		}
	}
	terminal := coordinator.BeginTerminal("mintclaw:chat-1")
	coordinator.CompleteTerminal(context.Background(), terminal, true)
	if sends != 2 || edits != 0 || deletes != 0 {
		t.Fatalf("separate operations = sends %d edits %d deletes %d", sends, edits, deletes)
	}
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() = %d, want 0", count)
	}
}

func TestToolFeedbackCoordinator_NonEditableTransportSendsEachUpdate(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	defer coordinator.StopAll()
	var sends int
	for _, content := range []string{"first", "second"} {
		if _, err := coordinator.Deliver(
			context.Background(), "irc:chat-1", "chat-1", content, toolFeedbackOperations{},
			func(context.Context, string) ([]string, error) {
				sends++
				return []string{fmt.Sprintf("message-%d", sends)}, nil
			},
		); err != nil {
			t.Fatalf("Deliver(%q) error = %v", content, err)
		}
	}
	if sends != 2 {
		t.Fatalf("sends = %d, want 2", sends)
	}
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() = %d, want 0", count)
	}
}

func TestToolFeedbackCoordinator_StopDeletesLateInitialSend(t *testing.T) {
	coordinator := newTestToolFeedbackCoordinator(false)
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	deleted := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		_, err := coordinator.Deliver(
			context.Background(),
			"telegram:chat-1",
			"chat-1",
			"Working...",
			toolFeedbackOperations{delete: func(_ context.Context, _, messageID string) error {
				deleted <- messageID
				return nil
			}},
			func(context.Context, string) ([]string, error) {
				close(sendStarted)
				<-releaseSend
				return []string{"progress-1"}, nil
			},
		)
		done <- err
	}()
	<-sendStarted
	coordinator.StopAll()
	close(releaseSend)
	if err := <-done; err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	select {
	case messageID := <-deleted:
		if messageID != "progress-1" {
			t.Fatalf("deleted message = %q, want progress-1", messageID)
		}
	case <-time.After(time.Second):
		t.Fatal("late progress message was not deleted after StopAll")
	}
	if count := coordinator.ActiveCount(); count != 0 {
		t.Fatalf("ActiveCount() = %d, want 0", count)
	}
}
