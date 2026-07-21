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
		"send:progress-1", "edit:progress-1", "delete:progress-1", "send:progress-2",
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
			context.Background(), "pico:chat-1", "chat-1", id, operations,
			func(context.Context, string) ([]string, error) { sends++; return []string{id}, nil },
		); err != nil {
			t.Fatalf("Deliver(%s) error = %v", id, err)
		}
	}
	terminal := coordinator.BeginTerminal("pico:chat-1")
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
