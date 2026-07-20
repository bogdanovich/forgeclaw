package channels

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFormatAnimatedToolFeedbackContent(t *testing.T) {
	got := formatAnimatedToolFeedbackContent("🔧 `read_file`\nReading config file", "running..")
	want := "🔧 `read_filerunning..`\nReading config file"
	if got != want {
		t.Fatalf("formatAnimatedToolFeedbackContent() = %q, want %q", got, want)
	}
}

func TestInitialAnimatedToolFeedbackContent(t *testing.T) {
	got := InitialAnimatedToolFeedbackContent("🔧 `exec`\nRunning command")
	want := "🔧 `exec`\nRunning command"
	if got != want {
		t.Fatalf("InitialAnimatedToolFeedbackContent() = %q, want %q", got, want)
	}
}

func TestFormatAnimatedToolFeedbackContent_WithoutCodeSpan(t *testing.T) {
	got := formatAnimatedToolFeedbackContent("hello", "running..")
	want := "hellorunning.."
	if got != want {
		t.Fatalf("formatAnimatedToolFeedbackContent() without code span = %q, want %q", got, want)
	}
}

func TestToolFeedbackAnimator_RecordCurrentAndClear(t *testing.T) {
	animator := NewToolFeedbackAnimator(nil)
	animator.Record("chat-1", "msg-1", "🔧 `read_file`")

	msgID, ok := animator.Current("chat-1")
	if !ok || msgID != "msg-1" {
		t.Fatalf("Current() = (%q, %v), want (msg-1, true)", msgID, ok)
	}

	animator.Clear("chat-1")

	msgID, ok = animator.Current("chat-1")
	if ok || msgID != "" {
		t.Fatalf("Current() after Clear = (%q, %v), want (\"\", false)", msgID, ok)
	}
}

func TestToolFeedbackAnimator_ClearIfCurrentPreservesReplacement(t *testing.T) {
	animator := NewToolFeedbackAnimator(nil)
	animator.Record("chat-1", "msg-1", "first")
	animator.Record("chat-1", "msg-2", "replacement")

	if animator.ClearIfCurrent("chat-1", "msg-1") {
		t.Fatal("ClearIfCurrent() cleared a replacement entry")
	}
	if msgID, ok := animator.Current("chat-1"); !ok || msgID != "msg-2" {
		t.Fatalf("Current() after stale clear = (%q, %v), want (msg-2, true)", msgID, ok)
	}
	if !animator.ClearIfCurrent("chat-1", "msg-2") {
		t.Fatal("ClearIfCurrent() did not clear the matching entry")
	}
	if msgID, ok := animator.Current("chat-1"); ok || msgID != "" {
		t.Fatalf("Current() after matching clear = (%q, %v), want (\"\", false)", msgID, ok)
	}
}

func TestToolFeedbackAnimator_RecordDeletesDisplacedMessage(t *testing.T) {
	deleted := make(chan string, 1)
	animator := NewToolFeedbackAnimator(nil, func(_ context.Context, _, messageID string) error {
		deleted <- messageID
		return nil
	})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "first")
	animator.Record("chat-1", "msg-2", "replacement")

	select {
	case messageID := <-deleted:
		if messageID != "msg-1" {
			t.Fatalf("deleted message = %q, want msg-1", messageID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for displaced message cleanup")
	}
	if msgID, ok := animator.Current("chat-1"); !ok || msgID != "msg-2" {
		t.Fatalf("Current() after replacement = (%q, %v), want (msg-2, true)", msgID, ok)
	}
}

func TestToolFeedbackAnimator_RecordDoesNotDeleteDetachedFinalization(t *testing.T) {
	deleted := make(chan string, 1)
	animator := NewToolFeedbackAnimator(nil, func(_ context.Context, _, messageID string) error {
		deleted <- messageID
		return nil
	})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "progress")
	snapshot, ok := animator.TakeRestorable("chat-1")
	if !ok || snapshot.MessageID != "msg-1" {
		t.Fatalf("TakeRestorable() = (%q, %v), want (msg-1, true)", snapshot.MessageID, ok)
	}
	animator.Record("chat-1", "msg-2", "new progress")

	select {
	case messageID := <-deleted:
		t.Fatalf("Record() deleted detached finalized message %q", messageID)
	case <-time.After(50 * time.Millisecond):
	}
	if msgID, ok := animator.Current("chat-1"); !ok || msgID != "msg-2" {
		t.Fatalf("Current() after finalization race = (%q, %v), want (msg-2, true)", msgID, ok)
	}
}

func TestToolFeedbackAnimator_TakeStopsTrackingAndReturnsState(t *testing.T) {
	animator := NewToolFeedbackAnimator(nil)
	animator.Record("chat-1", "msg-1", "🔧 `read_file`\nChecking config")

	msgID, baseContent, ok := animator.Take("chat-1")
	if !ok {
		t.Fatal("Take() = not found, want tracked message")
	}
	if msgID != "msg-1" {
		t.Fatalf("Take() msgID = %q, want msg-1", msgID)
	}
	if baseContent != "🔧 `read_file`\nChecking config" {
		t.Fatalf("Take() baseContent = %q", baseContent)
	}
	if _, ok := animator.Current("chat-1"); ok {
		t.Fatal("expected tracked message to be removed after Take()")
	}
}

func TestToolFeedbackAnimator_RestoreSnapshotWithoutInterveningMutation(t *testing.T) {
	animator := NewToolFeedbackAnimator(nil)
	defer animator.StopAll()
	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")

	snapshot, ok := animator.TakeRestorable("chat-1")
	if !ok {
		t.Fatal("TakeRestorable() = false, want snapshot")
	}
	if !animator.Restore(snapshot) {
		t.Fatal("Restore() = false without an intervening mutation")
	}
	if msgID, ok := animator.Current("chat-1"); !ok || msgID != "msg-1" {
		t.Fatalf("Current() after Restore = (%q, %v), want (msg-1, true)", msgID, ok)
	}
}

func TestToolFeedbackAnimator_RestoreSnapshotRejectsLaterLifecycleMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ToolFeedbackAnimator)
		wantID string
	}{
		{
			name:   "clear",
			mutate: func(animator *ToolFeedbackAnimator) { animator.Clear("chat-1") },
		},
		{
			name: "record replacement",
			mutate: func(animator *ToolFeedbackAnimator) {
				animator.Record("chat-1", "msg-2", "Working...\n• tool: `exec`")
			},
			wantID: "msg-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			animator := NewToolFeedbackAnimator(nil)
			defer animator.StopAll()
			animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")

			snapshot, ok := animator.TakeRestorable("chat-1")
			if !ok {
				t.Fatal("TakeRestorable() = false, want snapshot")
			}
			tt.mutate(animator)
			if animator.Restore(snapshot) {
				t.Fatal("Restore() = true after a later lifecycle mutation")
			}
			msgID, ok := animator.Current("chat-1")
			if tt.wantID == "" {
				if ok || msgID != "" {
					t.Fatalf("Current() = (%q, %v), want no restored message", msgID, ok)
				}
			} else if !ok || msgID != tt.wantID {
				t.Fatalf("Current() = (%q, %v), want (%s, true)", msgID, ok, tt.wantID)
			}
		})
	}
}

func TestToolFeedbackAnimator_UpdateKeepsTrackingDuringEdit(t *testing.T) {
	tests := []struct {
		name     string
		previous string
		next     string
		want     string
	}{
		{
			name:     "merge working summary",
			previous: "Working...\n• tool: `read_file`",
			next:     "Working...\n• tool: `write_file`",
			want:     "Working...\n• tool: `read_file`\n• tool: `write_file`",
		},
		{
			name:     "replace raw feedback",
			previous: "🔧 `read_file`\nReading config",
			next:     "🔧 `write_file`\nWriting config",
			want:     "🔧 `write_file`\nWriting config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var animator *ToolFeedbackAnimator
			animator = NewToolFeedbackAnimator(
				func(_ context.Context, chatID, messageID, content string) error {
					if currentID, ok := animator.Current(chatID); !ok || currentID != messageID {
						t.Fatalf("Current() during edit = (%q, %v), want (%q, true)", currentID, ok, messageID)
					}
					if messageID != "msg-1" {
						t.Fatalf("messageID = %q, want msg-1", messageID)
					}
					if content != tt.want {
						t.Fatalf("content = %q, want %q", content, tt.want)
					}
					return nil
				},
			)
			defer animator.StopAll()

			animator.Record("chat-1", "msg-1", tt.previous)
			msgID, handled, err := animator.Update(context.Background(), "chat-1", tt.next)
			if err != nil {
				t.Fatalf("Update() error = %v", err)
			}
			if !handled || msgID != "msg-1" {
				t.Fatalf("Update() = (%q, %v), want (msg-1, true)", msgID, handled)
			}
		})
	}
}

func TestToolFeedbackAnimator_UpdateAppliesRequestDeadline(t *testing.T) {
	animator := NewToolFeedbackAnimator(func(ctx context.Context, _, _, _ string) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("progress edit context has no deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 14*time.Second || remaining > defaultToolFeedbackEditTimeout {
			t.Fatalf("progress edit deadline remaining = %v, want about %v", remaining, defaultToolFeedbackEditTimeout)
		}
		return nil
	})
	defer animator.StopAll()
	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")

	_, handled, err := animator.Update(
		context.Background(),
		"chat-1",
		"Working...\n• tool: `write_file`",
	)
	if err != nil || !handled {
		t.Fatalf("Update() = (handled=%v, err=%v), want (true, nil)", handled, err)
	}
}

func TestToolFeedbackAnimationIntervalForWorkingSummary(t *testing.T) {
	got := toolFeedbackAnimationIntervalFor("Working...\n• tool: `read_file`")
	if got != workingSummaryToolFeedbackAnimationInterval {
		t.Fatalf("toolFeedbackAnimationIntervalFor() = %v, want %v", got, workingSummaryToolFeedbackAnimationInterval)
	}
}

func TestToolFeedbackAnimationIntervalForRawFeedback(t *testing.T) {
	got := toolFeedbackAnimationIntervalFor("🔧 `read_file`\nReading config")
	if got != defaultToolFeedbackAnimationInterval {
		t.Fatalf("toolFeedbackAnimationIntervalFor() = %v, want %v", got, defaultToolFeedbackAnimationInterval)
	}
}

func TestToolFeedbackAnimator_UpdateFailureRestoresTracking(t *testing.T) {
	editErr := errors.New("edit failed")
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		return editErr
	})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")

	msgID, handled, err := animator.Update(context.Background(), "chat-1", "Working...\n• tool: `write_file`")
	if !handled {
		t.Fatal("Update() handled = false, want true")
	}
	if !errors.Is(err, editErr) {
		t.Fatalf("Update() error = %v, want editErr", err)
	}
	if msgID != "" {
		t.Fatalf("Update() msgID = %q, want empty on failed edit", msgID)
	}
	if currentID, ok := animator.Current("chat-1"); !ok || currentID != "msg-1" {
		t.Fatalf("Current() after failed Update = (%q, %v), want (msg-1, true)", currentID, ok)
	}
}

func TestToolFeedbackAnimator_ClearDuringUpdatePreventsStaleRestore(t *testing.T) {
	editStarted := make(chan struct{})
	releaseEdit := make(chan struct{})
	editErr := errors.New("edit timed out")
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		close(editStarted)
		<-releaseEdit
		return editErr
	})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")
	result := make(chan error, 1)
	go func() {
		_, handled, err := animator.Update(
			context.Background(),
			"chat-1",
			"Working...\n• tool: `write_file`",
		)
		if !handled {
			result <- errors.New("Update() handled = false, want true")
			return
		}
		result <- err
	}()

	<-editStarted
	clearDone := make(chan struct{})
	go func() {
		animator.Clear("chat-1")
		close(clearDone)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		currentID, ok := animator.Current("chat-1")
		if !ok && currentID == "" {
			break
		}
		if time.Now().After(deadline) {
			close(releaseEdit)
			t.Fatalf("Current() while Clear waits = (%q, %v), want (\"\", false)", currentID, ok)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-clearDone:
		t.Fatal("Clear() returned before the in-flight edit completed")
	default:
	}

	close(releaseEdit)

	if err := <-result; err != nil {
		t.Fatalf("stale Update() error = %v, want nil", err)
	}
	<-clearDone
	if currentID, ok := animator.Current("chat-1"); ok || currentID != "" {
		t.Fatalf("Current() after stale Update = (%q, %v), want (\"\", false)", currentID, ok)
	}
}

func TestToolFeedbackAnimator_ConcurrentUpdatesSerializeAndApplyLatest(t *testing.T) {
	editStarted := make(chan struct{})
	releaseEdit := make(chan struct{})
	var editCalls atomic.Int32
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		if editCalls.Add(1) == 1 {
			close(editStarted)
			<-releaseEdit
		}
		return nil
	})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")
	firstResult := make(chan error, 1)
	go func() {
		_, _, err := animator.Update(
			context.Background(),
			"chat-1",
			"Working...\n• tool: `write_file`",
		)
		firstResult <- err
	}()

	<-editStarted
	secondResult := make(chan error, 1)
	go func() {
		msgID, handled, err := animator.Update(
			context.Background(),
			"chat-1",
			"Working...\n• tool: `exec`",
		)
		if err == nil && (!handled || msgID != "msg-1") {
			err = fmt.Errorf("concurrent Update() = (%q, %v, nil), want (msg-1, true, nil)", msgID, handled)
		}
		secondResult <- err
	}()

	if calls := editCalls.Load(); calls != 1 {
		t.Fatalf("edit calls while first update is in flight = %d, want 1", calls)
	}
	select {
	case err := <-secondResult:
		t.Fatalf("second Update() returned before the first completed: %v", err)
	default:
	}

	close(releaseEdit)
	if err := <-firstResult; err != nil {
		t.Fatalf("first Update() error = %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second Update() error = %v", err)
	}
	if calls := editCalls.Load(); calls != 2 {
		t.Fatalf("edit calls after both updates = %d, want 2", calls)
	}

	_, content, ok := animator.Take("chat-1")
	if !ok {
		t.Fatal("Take() = false, want tracked final update")
	}
	for _, tool := range []string{"`read_file`", "`write_file`", "`exec`"} {
		if !strings.Contains(content, tool) {
			t.Fatalf("final feedback content %q does not contain %s", content, tool)
		}
	}
}

func TestToolFeedbackAnimator_TakeRestorableWaitsForInFlightUpdate(t *testing.T) {
	editStarted := make(chan struct{})
	releaseEdit := make(chan struct{})
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		close(editStarted)
		<-releaseEdit
		return nil
	})
	defer animator.StopAll()
	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")

	updateResult := make(chan error, 1)
	go func() {
		_, _, err := animator.Update(
			context.Background(),
			"chat-1",
			"Working...\n• tool: `write_file`",
		)
		updateResult <- err
	}()
	<-editStarted

	type takeResult struct {
		snapshot ToolFeedbackSnapshot
		ok       bool
	}
	takeResultCh := make(chan takeResult, 1)
	go func() {
		snapshot, ok := animator.TakeRestorable("chat-1")
		takeResultCh <- takeResult{snapshot: snapshot, ok: ok}
	}()
	select {
	case <-takeResultCh:
		t.Fatal("TakeRestorable() returned before the in-flight update settled")
	default:
	}

	close(releaseEdit)
	if err := <-updateResult; err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	got := <-takeResultCh
	if !got.ok {
		t.Fatal("TakeRestorable() = false after the in-flight update settled")
	}
	for _, tool := range []string{"`read_file`", "`write_file`"} {
		if !strings.Contains(got.snapshot.Content, tool) {
			t.Fatalf("snapshot content %q does not contain %s", got.snapshot.Content, tool)
		}
	}
}

func TestToolFeedbackAnimator_RecordDuringUpdateStartsNewGeneration(t *testing.T) {
	editStarted := make(chan struct{})
	releaseEdit := make(chan struct{})
	var editCalls atomic.Int32
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		if editCalls.Add(1) == 1 {
			close(editStarted)
			<-releaseEdit
		}
		return nil
	})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")
	updateResult := make(chan error, 1)
	go func() {
		_, _, err := animator.Update(
			context.Background(),
			"chat-1",
			"Working...\n• tool: `write_file`",
		)
		updateResult <- err
	}()

	<-editStarted
	recordDone := make(chan struct{})
	go func() {
		animator.Record("chat-1", "msg-2", "Working...\n• tool: `exec`")
		close(recordDone)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		currentID, ok := animator.Current("chat-1")
		if ok && currentID == "msg-2" {
			break
		}
		if time.Now().After(deadline) {
			close(releaseEdit)
			t.Fatalf("Current() while Record waits = (%q, %v), want (msg-2, true)", currentID, ok)
		}
		time.Sleep(time.Millisecond)
	}

	close(releaseEdit)
	if err := <-updateResult; err != nil {
		t.Fatalf("superseded Update() error = %v, want nil", err)
	}
	<-recordDone
	if calls := editCalls.Load(); calls != 1 {
		t.Fatalf("edit calls after Record replaced the in-flight update = %d, want 1", calls)
	}

	msgID, content, ok := animator.Take("chat-1")
	if !ok || msgID != "msg-2" {
		t.Fatalf("Take() = (%q, %q, %v), want msg-2 generation", msgID, content, ok)
	}
	if strings.Contains(content, "`write_file`") || !strings.Contains(content, "`exec`") {
		t.Fatalf("replacement feedback content = %q, want only the new generation", content)
	}
}

func TestToolFeedbackAnimator_StaleRateLimitDoesNotPauseNewGeneration(t *testing.T) {
	editStarted := make(chan struct{})
	releaseEdit := make(chan struct{})
	var editCalls atomic.Int32
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		if editCalls.Add(1) == 1 {
			close(editStarted)
			<-releaseEdit
			return errors.New("429: retry after 60")
		}
		return nil
	})
	animator.Configure(ToolFeedbackAnimatorConfig{MinEditInterval: time.Millisecond})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")
	updateResult := make(chan error, 1)
	go func() {
		_, _, err := animator.Update(
			context.Background(),
			"chat-1",
			"Working...\n• tool: `write_file`",
		)
		updateResult <- err
	}()

	<-editStarted
	recordDone := make(chan struct{})
	go func() {
		animator.Record("chat-1", "msg-2", "Working...\n• tool: `exec`")
		close(recordDone)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		currentID, ok := animator.Current("chat-1")
		if ok && currentID == "msg-2" {
			break
		}
		if time.Now().After(deadline) {
			close(releaseEdit)
			t.Fatalf("Current() while Record waits = (%q, %v), want (msg-2, true)", currentID, ok)
		}
		time.Sleep(time.Millisecond)
	}

	close(releaseEdit)
	if err := <-updateResult; err != nil {
		t.Fatalf("superseded rate-limited Update() error = %v, want nil", err)
	}
	<-recordDone

	msgID, handled, err := animator.Update(
		context.Background(),
		"chat-1",
		"Working...\n• tool: `web_fetch`",
	)
	if err != nil || !handled || msgID != "msg-2" {
		t.Fatalf("new-generation Update() = (%q, %v, %v), want (msg-2, true, nil)", msgID, handled, err)
	}
	if calls := editCalls.Load(); calls != 2 {
		t.Fatalf("edit calls after stale rate limit = %d, want 2", calls)
	}
}

func TestMergeToolFeedbackContent_PreservesNamedWorkingSummaryHeader(t *testing.T) {
	got := mergeToolFeedbackContent(
		"Deep Research working...\n• tool: `read_file`",
		"Deep Research working...\n• tool: `web_fetch`",
	)
	want := "Deep Research working...\n• tool: `read_file`\n• tool: `web_fetch`"
	if got != want {
		t.Fatalf("mergeToolFeedbackContent() = %q, want %q", got, want)
	}
}

func TestIsWorkingSummaryToolFeedback_AcceptsNamedHeader(t *testing.T) {
	if !isWorkingSummaryToolFeedback("Deep Research working...\n• tool: `read_file`") {
		t.Fatal("expected named working summary to be recognized")
	}
}

func TestToolFeedbackAnimator_UpdateDoesNotDebounceByDefault(t *testing.T) {
	editCalls := 0
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		editCalls++
		return nil
	})
	defer animator.StopAll()

	animator.Record("chat-1", "msg-1", "🔧 `read_file`")
	animator.markEdit("chat-1", animator.current("chat-1").generation)

	msgID, handled, err := animator.Update(context.Background(), "chat-1", "🔧 `write_file`")
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if !handled || msgID != "msg-1" {
		t.Fatalf("Update() = (%q, %v), want (msg-1, true)", msgID, handled)
	}
	if editCalls != 1 {
		t.Fatalf("edit calls = %d, want 1 when min edit interval is unset", editCalls)
	}
}

func TestToolFeedbackAnimator_UpdateDebouncesRecentEditWhenConfigured(t *testing.T) {
	editCalls := 0
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		editCalls++
		return nil
	})
	defer animator.StopAll()
	animator.Configure(ToolFeedbackAnimatorConfig{MinEditInterval: 10 * time.Second})

	animator.Record("chat-1", "msg-1", "🔧 `read_file`")
	animator.markEdit("chat-1", animator.current("chat-1").generation)

	msgID, handled, err := animator.Update(context.Background(), "chat-1", "🔧 `write_file`")
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if !handled || msgID != "msg-1" {
		t.Fatalf("Update() = (%q, %v), want (msg-1, true)", msgID, handled)
	}
	if editCalls != 0 {
		t.Fatalf("edit calls = %d, want 0 while debounced", editCalls)
	}
	_, baseContent, ok := animator.Take("chat-1")
	if !ok {
		t.Fatal("expected debounced update to keep tracking")
	}
	if baseContent != "🔧 `write_file`" {
		t.Fatalf("tracked content = %q, want latest content", baseContent)
	}
}

func TestToolFeedbackAnimator_StaleLastEditDoesNotDebounceNewGeneration(t *testing.T) {
	editCalls := 0
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		editCalls++
		return nil
	})
	defer animator.StopAll()
	animator.Configure(ToolFeedbackAnimatorConfig{MinEditInterval: 10 * time.Second})

	animator.Record("chat-1", "msg-1", "Working...\n• tool: `read_file`")
	oldGeneration := animator.current("chat-1").generation
	animator.Record("chat-1", "msg-2", "Working...\n• tool: `exec`")

	animator.markEdit("chat-1", oldGeneration)
	msgID, handled, err := animator.Update(
		context.Background(),
		"chat-1",
		"Working...\n• tool: `web_fetch`",
	)
	if err != nil || !handled || msgID != "msg-2" {
		t.Fatalf("new-generation Update() = (%q, %v, %v), want (msg-2, true, nil)", msgID, handled, err)
	}
	if editCalls != 1 {
		t.Fatalf("edit calls after stale last edit = %d, want 1", editCalls)
	}
}

func TestToolFeedbackAnimator_UpdateDebouncesAfterRecordedEdit(t *testing.T) {
	editCalls := 0
	animator := NewToolFeedbackAnimator(func(context.Context, string, string, string) error {
		editCalls++
		return nil
	})
	defer animator.StopAll()
	animator.Configure(ToolFeedbackAnimatorConfig{MinEditInterval: 10 * time.Second})

	animator.RecordEdited("chat-1", "msg-1", "Working...\n• tool: `read_file`")

	msgID, handled, err := animator.Update(context.Background(), "chat-1", "Working...\n• tool: `write_file`")
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if !handled || msgID != "msg-1" {
		t.Fatalf("Update() = (%q, %v), want (msg-1, true)", msgID, handled)
	}
	if editCalls != 0 {
		t.Fatalf("edit calls = %d, want 0 while debounced after recorded edit", editCalls)
	}
	_, baseContent, ok := animator.Take("chat-1")
	if !ok {
		t.Fatal("expected debounced update to keep tracking")
	}
	want := "Working...\n• tool: `read_file`\n• tool: `write_file`"
	if baseContent != want {
		t.Fatalf("tracked content = %q, want %q", baseContent, want)
	}
}

func TestToolFeedbackAnimator_RetryAfterDelayParsesTelegramErrorWhenConfigured(t *testing.T) {
	animator := NewToolFeedbackAnimator(nil)
	animator.Configure(ToolFeedbackAnimatorConfig{MinEditInterval: 10 * time.Second})

	err := fmt.Errorf("telego: editMessageText: api: 429 %q: %w", "Too Many Requests: retry after 14", ErrRateLimit)
	delay, ok := animator.retryAfterDelay(err)
	if !ok {
		t.Fatal("retryAfterDelay() ok = false, want true")
	}
	if delay != 14*time.Second {
		t.Fatalf("retryAfterDelay() = %v, want 14s", delay)
	}
}

func TestToolFeedbackAnimator_RetryAfterDisabledByDefault(t *testing.T) {
	animator := NewToolFeedbackAnimator(nil)
	err := fmt.Errorf("telego: editMessageText: api: 429 %q: %w", "Too Many Requests: retry after 14", ErrRateLimit)
	if delay, ok := animator.retryAfterDelay(err); ok {
		t.Fatalf("retryAfterDelay() = (%v, true), want disabled by default", delay)
	}
}
