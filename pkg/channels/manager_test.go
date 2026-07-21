package channels

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/utils"
)

// mockChannel is a test double that delegates Send to a configurable function.
type mockChannel struct {
	BaseChannel
	sendFn            func(ctx context.Context, msg bus.OutboundMessage) error
	startFn           func(ctx context.Context) error
	stopFn            func(ctx context.Context) error
	sentMessages      []bus.OutboundMessage
	placeholdersSent  int
	editedMessages    int
	lastPlaceholderID string
}

func (m *mockChannel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
	m.sentMessages = append(m.sentMessages, msg)
	if m.sendFn == nil {
		return nil, nil
	}
	return nil, m.sendFn(ctx, msg)
}

func (m *mockChannel) Start(ctx context.Context) error {
	if m.startFn != nil {
		return m.startFn(ctx)
	}
	return nil
}

func (m *mockChannel) Stop(ctx context.Context) error {
	if m.stopFn != nil {
		return m.stopFn(ctx)
	}
	return nil
}

func (m *mockChannel) SendPlaceholder(ctx context.Context, chatID string) (string, error) {
	m.placeholdersSent++
	m.lastPlaceholderID = "mock-ph-123"
	return m.lastPlaceholderID, nil
}

func (m *mockChannel) EditMessage(ctx context.Context, chatID, messageID, content string) error {
	m.editedMessages++
	return nil
}

type mockMediaChannel struct {
	mockChannel
	sendMediaFn       func(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error)
	sentMediaMessages []bus.OutboundMediaMessage
}

func (m *mockMediaChannel) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error) {
	m.sentMediaMessages = append(m.sentMediaMessages, msg)
	if m.sendMediaFn != nil {
		return m.sendMediaFn(ctx, msg)
	}
	return nil, nil
}

type mockDeletingMediaChannel struct {
	mockMediaChannel
	deleteCalls     int
	dismissedChatID string
	lastDeleted     struct {
		chatID    string
		messageID string
	}
}

func (m *mockDeletingMediaChannel) DeleteMessage(
	_ context.Context,
	chatID string,
	messageID string,
) error {
	m.deleteCalls++
	m.lastDeleted.chatID = chatID
	m.lastDeleted.messageID = messageID
	return nil
}

func (m *mockDeletingMediaChannel) DismissToolFeedbackMessage(_ context.Context, chatID string) {
	m.dismissedChatID = chatID
}

type mockStreamer struct {
	finalizeFn            func(context.Context, string) error
	finalizeWithContextFn func(context.Context, string, *bus.ContextUsage) error
}

func (m *mockStreamer) Update(context.Context, string) error { return nil }

func (m *mockStreamer) Finalize(ctx context.Context, content string) error {
	if m.finalizeFn != nil {
		return m.finalizeFn(ctx, content)
	}
	return nil
}

func (m *mockStreamer) FinalizeWithContext(ctx context.Context, content string, usage *bus.ContextUsage) error {
	if m.finalizeWithContextFn != nil {
		return m.finalizeWithContextFn(ctx, content, usage)
	}
	return m.Finalize(ctx, content)
}

func (m *mockStreamer) Cancel(context.Context) {}

type mockReasoningStreamer struct {
	mockStreamer
	reasoningUpdates []string
	reasoningFinal   string
}

func (m *mockReasoningStreamer) UpdateReasoning(_ context.Context, content string) error {
	m.reasoningUpdates = append(m.reasoningUpdates, content)
	return nil
}

func (m *mockReasoningStreamer) FinalizeReasoning(_ context.Context, content string) error {
	m.reasoningFinal = content
	return nil
}

type modelTrackingReasoningStreamer struct {
	mockReasoningStreamer
	modelNames []string
}

func (m *modelTrackingReasoningStreamer) SetModelName(modelName string) {
	m.modelNames = append(m.modelNames, strings.TrimSpace(modelName))
}

type recordingStreamSegment struct {
	updates       []string
	finals        []string
	finalUsage    *bus.ContextUsage
	canceledCount int
	modelNames    []string
}

func (s *recordingStreamSegment) Update(_ context.Context, content string) error {
	s.updates = append(s.updates, content)
	return nil
}

func (s *recordingStreamSegment) Finalize(ctx context.Context, content string) error {
	return s.FinalizeWithContext(ctx, content, nil)
}

func (s *recordingStreamSegment) FinalizeWithContext(_ context.Context, content string, usage *bus.ContextUsage) error {
	s.finals = append(s.finals, content)
	s.finalUsage = usage
	return nil
}

func (s *recordingStreamSegment) Cancel(context.Context) {
	s.canceledCount++
}

func (s *recordingStreamSegment) SetModelName(modelName string) {
	s.modelNames = append(s.modelNames, strings.TrimSpace(modelName))
}

type mockStreamingChannel struct {
	mockMessageEditor
	streamer        Streamer
	beginStreamFn   func(context.Context, string) (Streamer, error)
	resolveChatIDFn func(chatID string, outboundCtx *bus.InboundContext) string
}

func (m *mockStreamingChannel) BeginStream(ctx context.Context, chatID string) (Streamer, error) {
	if m.beginStreamFn != nil {
		return m.beginStreamFn(ctx, chatID)
	}
	if m.streamer == nil {
		return nil, errors.New("missing streamer")
	}
	return m.streamer, nil
}

func (m *mockStreamingChannel) ResolveOutboundChatID(
	chatID string,
	outboundCtx *bus.InboundContext,
) string {
	if m.resolveChatIDFn != nil {
		return m.resolveChatIDFn(chatID, outboundCtx)
	}
	return chatID
}

// newTestManager creates a minimal Manager suitable for unit tests.
func newTestManager() *Manager {
	return &Manager{
		channels:               make(map[string]Channel),
		workers:                make(map[string]*channelWorker),
		deliveryOwners:         make(map[string]*deliveryOwner),
		bus:                    bus.NewMessageBus(),
		channelHashes:          make(map[string]string),
		channelRestartRequired: make(map[string]string),
	}
}

type toolFeedbackTestChannel struct {
	mockChannel
	mu          sync.Mutex
	nextID      int
	sendErr     error
	operations  []string
	edited      []string
	deleted     []string
	resolvedID  string
	preparedTag string
}

func (c *toolFeedbackTestChannel) Send(_ context.Context, msg bus.OutboundMessage) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, "send:"+msg.Content)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	c.nextID++
	return []string{fmt.Sprintf("msg-%d", c.nextID)}, nil
}

func (c *toolFeedbackTestChannel) EditMessage(
	_ context.Context, chatID, messageID, content string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, "edit:"+messageID)
	c.edited = append(c.edited, chatID+"|"+messageID+"|"+content)
	return nil
}

func (c *toolFeedbackTestChannel) DeleteMessage(_ context.Context, chatID, messageID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, "delete:"+messageID)
	c.deleted = append(c.deleted, chatID+"|"+messageID)
	return nil
}

func (c *toolFeedbackTestChannel) ToolFeedbackMessageChatID(
	chatID string, _ *bus.InboundContext,
) string {
	if c.resolvedID != "" {
		return c.resolvedID
	}
	return chatID
}

func (c *toolFeedbackTestChannel) PrepareToolFeedbackMessageContent(content string) string {
	return c.preparedTag + content
}

func enableTestToolFeedbackCoordinator(t *testing.T, m *Manager, separate bool) {
	t.Helper()
	m.toolFeedback = NewToolFeedbackCoordinator(ToolFeedbackAnimatorConfig{
		AnimationInterval: time.Hour,
	}, separate)
	t.Cleanup(m.toolFeedback.StopAll)
}

func TestSetMediaStorePropagatesToExistingChannels(t *testing.T) {
	oldStore := media.NewFileMediaStore()
	newStore := media.NewFileMediaStore()
	ch := &mockChannel{}
	ch.SetMediaStore(oldStore)

	m := newTestManager()
	m.mediaStore = oldStore
	m.channels["telegram"] = ch

	m.SetMediaStore(newStore)

	if m.mediaStore != newStore {
		t.Fatal("manager media store was not updated")
	}
	if got := ch.GetMediaStore(); got != newStore {
		t.Fatalf("channel media store = %p, want %p", got, newStore)
	}
}

func TestReload_ChangedExistingChannelRequiresRestart(t *testing.T) {
	oldCfg := config.DefaultConfig()
	oldCfg.Channels["test"] = &config.Channel{
		Enabled:  true,
		Settings: config.RawNode(`{"enabled":true,"key":"old-value"}`),
	}
	newCfg := config.DefaultConfig()
	newCfg.Channels["test"] = &config.Channel{
		Enabled:  true,
		Settings: config.RawNode(`{"enabled":true,"key":"new-value"}`),
	}

	var stopCalls int
	ch := &mockChannel{
		stopFn: func(context.Context) error {
			stopCalls++
			return nil
		},
	}
	ch.SetRunning(true)

	m := newTestManager()
	m.config = oldCfg
	m.channels["test"] = ch
	m.workers["test"] = newChannelWorker("test", ch, "test")
	m.channelHashes = toChannelHashes(oldCfg)
	oldHash := m.channelHashes["test"]
	newHash := toChannelHashes(newCfg)["test"]
	if oldHash == newHash {
		t.Fatal("test setup expected channel hash to change")
	}

	if err := m.Reload(t.Context(), newCfg); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}

	if stopCalls != 0 {
		t.Fatalf("changed channel was stopped during reload, calls = %d", stopCalls)
	}
	if got := m.channels["test"]; got != ch {
		t.Fatal("changed channel instance was replaced during reload")
	}
	if got := m.workers["test"].ch; got != ch {
		t.Fatal("changed channel worker was replaced during reload")
	}
	if got := m.channelHashes["test"]; got != oldHash {
		t.Fatalf("active channel hash = %q, want old active hash %q", got, oldHash)
	}

	status := m.GetStatus()["test"].(map[string]any)
	if got, _ := status["restart_required"].(bool); !got {
		t.Fatalf("restart_required status = %v, want true", status["restart_required"])
	}

	if err := m.SendMessage(t.Context(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "still routed",
	})); err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
	if len(ch.sentMessages) != 1 {
		t.Fatalf("old channel sent messages = %d, want 1", len(ch.sentMessages))
	}

	if err := m.Reload(t.Context(), oldCfg); err != nil {
		t.Fatalf("Reload(oldCfg) error = %v", err)
	}
	status = m.GetStatus()["test"].(map[string]any)
	if _, ok := status["restart_required"]; ok {
		t.Fatalf("restart_required was not cleared after config returned to active hash: %#v", status)
	}
}

func TestReload_ChangedInactiveChannelIsRecreated(t *testing.T) {
	const channelType = "reload-inactive-test"

	var started int
	recreated := &mockChannel{
		startFn: func(context.Context) error {
			started++
			return nil
		},
	}
	RegisterFactory(channelType, func(string, string, *config.Config, *bus.MessageBus) (Channel, error) {
		return recreated, nil
	})

	oldCfg := config.DefaultConfig()
	oldCfg.Channels["test"] = &config.Channel{
		Enabled:  true,
		Type:     channelType,
		Settings: config.RawNode(`{"enabled":true,"key":"old-value"}`),
	}
	newCfg := config.DefaultConfig()
	newCfg.Channels["test"] = &config.Channel{
		Enabled:  true,
		Type:     channelType,
		Settings: config.RawNode(`{"enabled":true,"key":"new-value"}`),
	}

	var staleStopCalls int
	stale := &mockChannel{
		stopFn: func(context.Context) error {
			staleStopCalls++
			return nil
		},
	}

	m := newTestManager()
	m.config = oldCfg
	m.channels["test"] = stale
	m.channelHashes = toChannelHashes(oldCfg)

	if err := m.Reload(t.Context(), newCfg); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}

	if staleStopCalls != 1 {
		t.Fatalf("stale inactive channel stop calls = %d, want 1", staleStopCalls)
	}
	if got := m.channels["test"]; got != recreated {
		t.Fatal("inactive changed channel was not recreated")
	}
	if got := m.workers["test"].ch; got != recreated {
		t.Fatal("inactive changed channel worker was not recreated")
	}
	if started != 1 {
		t.Fatalf("recreated channel start calls = %d, want 1", started)
	}
	if _, ok := m.GetStatus()["test"].(map[string]any)["restart_required"]; ok {
		t.Fatal("inactive changed channel should not require restart after recreation")
	}
	if got, want := m.channelHashes["test"], toChannelHashes(newCfg)["test"]; got != want {
		t.Fatalf("channel hash = %q, want recreated hash %q", got, want)
	}
}

func TestReload_ChangedInactiveChannelPreservedWhenReplacementInitFails(t *testing.T) {
	const channelType = "reload-inactive-fail-test"

	RegisterFactory(channelType, func(string, string, *config.Config, *bus.MessageBus) (Channel, error) {
		return nil, errors.New("replacement init failed")
	})

	oldCfg := config.DefaultConfig()
	oldCfg.Channels["test"] = &config.Channel{
		Enabled:  true,
		Type:     channelType,
		Settings: config.RawNode(`{"enabled":true,"key":"old-value"}`),
	}
	newCfg := config.DefaultConfig()
	newCfg.Channels["test"] = &config.Channel{
		Enabled:  true,
		Type:     channelType,
		Settings: config.RawNode(`{"enabled":true,"key":"new-value"}`),
	}

	var staleStopCalls int
	stale := &mockChannel{
		stopFn: func(context.Context) error {
			staleStopCalls++
			return nil
		},
	}

	m := newTestManager()
	m.config = oldCfg
	m.channels["test"] = stale
	m.channelHashes = toChannelHashes(oldCfg)
	oldHash := m.channelHashes["test"]

	err := m.Reload(t.Context(), newCfg)
	if err == nil || !strings.Contains(err.Error(), "replacement channel test was not initialized") {
		t.Fatalf("Reload() error = %v, want replacement init failure", err)
	}
	if got := m.channels["test"]; got != stale {
		t.Fatal("stale inactive channel was not preserved after replacement init failure")
	}
	if staleStopCalls != 0 {
		t.Fatalf("stale inactive channel stop calls = %d, want 0", staleStopCalls)
	}
	if got := m.channelHashes["test"]; got != oldHash {
		t.Fatalf("channel hash = %q, want old hash %q", got, oldHash)
	}
	if m.config != oldCfg {
		t.Fatal("manager config was not restored after replacement init failure")
	}
}

func TestReload_RemovedChannelDrainsDeliveryBeforeStop(t *testing.T) {
	oldCfg := config.DefaultConfig()
	oldCfg.Channels["test"] = &config.Channel{
		Enabled:  true,
		Settings: config.RawNode(`{"enabled":true}`),
	}
	newCfg := config.DefaultConfig()

	sendStarted := make(chan struct{})
	proceedSend := make(chan struct{})
	stopCalled := make(chan struct{})
	var stopOnce sync.Once
	var sendFinished atomic.Bool
	var stopBeforeDrain atomic.Bool
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			close(sendStarted)
			<-proceedSend
			sendFinished.Store(true)
			return nil
		},
		stopFn: func(context.Context) error {
			if !sendFinished.Load() {
				stopBeforeDrain.Store(true)
			}
			stopOnce.Do(func() { close(stopCalled) })
			return nil
		},
	}
	owner := newDeliveryOwner("test", ch, "test")

	m := newTestManager()
	m.config = oldCfg
	m.channels["test"] = ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner
	m.channelHashes = toChannelHashes(oldCfg)
	owner.StartDelivery(context.Background(), m)

	queued, err := owner.Enqueue(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "queued before removal",
	}))
	if !queued || err != nil {
		t.Fatalf("Enqueue() queued=%v err=%v, want true nil", queued, err)
	}
	select {
	case <-sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not start sending queued message")
	}

	if err := m.Reload(t.Context(), newCfg); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	waitForDeliveryOwnerClosed(t, owner)
	select {
	case <-stopCalled:
		t.Fatal("removed channel stopped before delivery drained")
	default:
	}

	close(proceedSend)
	select {
	case <-stopCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("removed channel was not stopped after delivery drained")
	}
	if stopBeforeDrain.Load() {
		t.Fatal("removed channel Stop ran before queued delivery completed")
	}

	m.mu.RLock()
	_, ownerExists := m.deliveryOwners["test"]
	_, workerExists := m.workers["test"]
	_, channelExists := m.channels["test"]
	m.mu.RUnlock()
	if ownerExists || workerExists || channelExists {
		t.Fatalf(
			"removed channel left maps populated after reload: owner=%v worker=%v channel=%v",
			ownerExists,
			workerExists,
			channelExists,
		)
	}
}

func TestStartAll_AllChannelsFail_ReturnsJoinedError(t *testing.T) {
	m := newTestManager()
	errA := errors.New("channel-a start failed")
	errB := errors.New("channel-b start failed")

	m.channels["a"] = &mockChannel{
		startFn: func(_ context.Context) error { return errA },
	}
	m.channels["b"] = &mockChannel{
		startFn: func(_ context.Context) error { return errB },
	}

	err := m.StartAll(t.Context())
	if err == nil {
		t.Fatal("expected StartAll to fail when all channels fail")
	}
	if !strings.Contains(err.Error(), "failed to start any enabled channels") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !errors.Is(err, errA) {
		t.Fatalf("expected error to wrap errA, got: %v", err)
	}
	if !errors.Is(err, errB) {
		t.Fatalf("expected error to wrap errB, got: %v", err)
	}
	if len(m.workers) != 0 {
		t.Fatalf("expected no workers on full startup failure, got %d", len(m.workers))
	}
	if m.dispatchTask != nil {
		t.Fatal("expected dispatch task to be cleared on full startup failure")
	}
}

func TestStartAll_PartialFailure_StartsSuccessfulWorkers(t *testing.T) {
	m := newTestManager()
	errBad := errors.New("bad channel start failed")
	processed := make(chan struct{}, 1)

	m.channels["good"] = &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			if msg.Channel == "good" {
				select {
				case processed <- struct{}{}:
				default:
				}
			}
			return nil
		},
	}
	m.channels["bad"] = &mockChannel{
		startFn: func(_ context.Context) error { return errBad },
	}

	err := m.StartAll(t.Context())
	if err != nil {
		t.Fatalf("expected StartAll to succeed with partial channel failures, got: %v", err)
	}
	if len(m.workers) != 1 {
		t.Fatalf("expected exactly 1 active worker, got %d", len(m.workers))
	}
	if _, ok := m.workers["good"]; !ok {
		t.Fatal("expected worker for successful channel 'good'")
	}
	if _, ok := m.workers["bad"]; ok {
		t.Fatal("did not expect worker for failed channel 'bad'")
	}
	if m.dispatchTask == nil {
		t.Fatal("expected dispatch task to run when at least one channel starts")
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := m.bus.PublishOutbound(pubCtx, testOutboundMessage(bus.OutboundMessage{
		Channel: "good",
		ChatID:  "chat-1",
		Content: "hello",
	})); err != nil {
		t.Fatalf("PublishOutbound() error = %v", err)
	}

	select {
	case <-processed:
		// worker processed outbound message as expected
	case <-time.After(2 * time.Second):
		t.Fatal("expected successful channel worker to process outbound message")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := m.StopAll(stopCtx); err != nil {
		t.Fatalf("StopAll() error = %v", err)
	}
}

func TestStartAll_CreatesDeliveryOwnerForStartedChannel(t *testing.T) {
	m := newTestManager()
	ch := &mockChannel{}
	m.channels["test"] = ch

	if err := m.StartAll(t.Context()); err != nil {
		t.Fatalf("StartAll() error = %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.StopAll(stopCtx); err != nil {
			t.Errorf("StopAll() error = %v", err)
		}
	})

	owner := m.deliveryOwners["test"]
	if owner == nil {
		t.Fatal("expected delivery owner for started channel")
	}
	if owner.ch != ch {
		t.Fatal("delivery owner channel does not match started channel")
	}
	if owner.Worker() == nil {
		t.Fatal("delivery owner missing worker")
	}
	if m.workers["test"] != owner.Worker() {
		t.Fatal("worker map and delivery owner point at different workers")
	}
}

func TestDeliveryOwner_CloseDeliveryDrainsAndRejectsNewWork(t *testing.T) {
	sent := make(chan bus.OutboundMessage, 1)
	ch := &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			sent <- msg
			return nil
		},
	}
	m := newTestManager()
	owner := newDeliveryOwner("test", ch, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner.StartDelivery(ctx, m)

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "hello",
	})
	queued, err := owner.Enqueue(context.Background(), msg)
	if !queued || err != nil {
		t.Fatalf("Enqueue() queued=%v err=%v, want true nil", queued, err)
	}

	owner.CloseDeliveryAndWait()

	select {
	case got := <-sent:
		if got.Content != "hello" {
			t.Fatalf("sent content = %q, want hello", got.Content)
		}
	default:
		t.Fatal("CloseDeliveryAndWait returned before queued message was delivered")
	}

	queued, err = owner.Enqueue(context.Background(), msg)
	if queued || !errors.Is(err, errDeliveryClosed) {
		t.Fatalf("Enqueue() after close queued=%v err=%v, want false errDeliveryClosed", queued, err)
	}
}

func TestDeliveryOwner_CloseDeliveryAndWaitIsWaitIdempotent(t *testing.T) {
	sendStarted := make(chan struct{})
	proceedSend := make(chan struct{})
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			close(sendStarted)
			<-proceedSend
			return nil
		},
	}
	m := newTestManager()
	owner := newDeliveryOwner("test", ch, "test")
	owner.StartDelivery(context.Background(), m)

	queued, err := owner.Enqueue(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "hello",
	}))
	if !queued || err != nil {
		t.Fatalf("Enqueue() queued=%v err=%v, want true nil", queued, err)
	}

	select {
	case <-sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not start sending queued message")
	}

	firstClosed := make(chan struct{})
	go func() {
		defer close(firstClosed)
		owner.CloseDeliveryAndWait()
	}()
	secondClosed := make(chan struct{})
	go func() {
		defer close(secondClosed)
		owner.CloseDeliveryAndWait()
	}()

	select {
	case <-secondClosed:
		t.Fatal("second CloseDeliveryAndWait returned before delivery drained")
	case <-time.After(50 * time.Millisecond):
	}

	close(proceedSend)
	for name, ch := range map[string]<-chan struct{}{
		"first":  firstClosed,
		"second": secondClosed,
	} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s CloseDeliveryAndWait did not return after delivery drained", name)
		}
	}
}

func TestDeliveryOwnerCancellationFailsEveryAcceptedMessage(t *testing.T) {
	for _, media := range []bool{false, true} {
		kind := "text"
		if media {
			kind = "media"
		}
		t.Run(kind, func(t *testing.T) {
			eventBus := runtimeevents.NewBus()
			t.Cleanup(func() { _ = eventBus.Close() })
			_, eventsCh, err := eventBus.Channel().OfKind(
				runtimeevents.KindChannelMessageOutboundFailed,
			).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "cancel-drain", Buffer: 4})
			if err != nil {
				t.Fatalf("SubscribeChan failed: %v", err)
			}

			started := make(chan struct{})
			var startedOnce sync.Once
			channel := &mockMediaChannel{
				mockChannel: mockChannel{sendFn: func(ctx context.Context, _ bus.OutboundMessage) error {
					startedOnce.Do(func() { close(started) })
					<-ctx.Done()
					return ErrTemporary
				}},
				sendMediaFn: func(ctx context.Context, _ bus.OutboundMediaMessage) ([]string, error) {
					startedOnce.Do(func() { close(started) })
					<-ctx.Done()
					return nil, ErrTemporary
				},
			}
			m := newTestManager()
			m.runtimeEvents = eventBus
			owner := newDeliveryOwner("test", channel, "test")
			ctx, cancel := context.WithCancel(context.Background())
			owner.StartDelivery(ctx, m)

			wantScopes := []runtimeevents.TraceScope{
				runtimeevents.NewTraceScope("/workspace/main", "turn-1"),
				runtimeevents.NewTraceScope("/workspace/main", "turn-2"),
			}
			for _, traceScope := range wantScopes {
				if media {
					queued, enqueueErr := owner.EnqueueMedia(context.Background(), bus.OutboundMediaMessage{
						Context:     bus.NewOutboundContext("test", "chat-1", ""),
						TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
					})
					if !queued || enqueueErr != nil {
						t.Fatalf("EnqueueMedia = (%v, %v)", queued, enqueueErr)
					}
					continue
				}
				queued, enqueueErr := owner.Enqueue(context.Background(), bus.OutboundMessage{
					Context: bus.NewOutboundContext("test", "chat-1", ""), Content: "hello",
					TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
				})
				if !queued || enqueueErr != nil {
					t.Fatalf("Enqueue = (%v, %v)", queued, enqueueErr)
				}
			}
			<-started
			cancel()

			drained := make(chan struct{})
			go func() {
				owner.CloseDeliveryAndWait()
				close(drained)
			}()
			select {
			case <-drained:
			case <-time.After(2 * time.Second):
				t.Fatal("canceled delivery owner did not drain")
			}

			seen := make(map[runtimeevents.TraceScope]bool, len(wantScopes))
			for range wantScopes {
				failed := receiveChannelRuntimeEvent(t, eventsCh)
				payload, ok := failed.Payload.(ChannelOutboundPayload)
				if !ok || !payload.TraceSettlement || payload.Media != media ||
					len(payload.TraceScopes) != 1 {
					t.Fatalf("canceled delivery outcome = %#v", failed)
				}
				seen[payload.TraceScopes[0]] = true
			}
			for _, traceScope := range wantScopes {
				if !seen[traceScope] {
					t.Fatalf("missing failed outcome for %+v", traceScope)
				}
			}
		})
	}
}

func TestDispatchOutbound_ClosedOwnerPublishesFailureAndContinues(t *testing.T) {
	eventBus := runtimeevents.NewBus()
	defer func() {
		if err := eventBus.Close(); err != nil {
			t.Errorf("event bus close failed: %v", err)
		}
	}()

	_, eventsCh, err := eventBus.Channel().OfKind(
		runtimeevents.KindChannelMessageOutboundFailed,
	).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "closed-owner-failure", Buffer: 1})
	if err != nil {
		t.Fatalf("SubscribeChan failed: %v", err)
	}

	m := newTestManager()
	m.runtimeEvents = eventBus
	owner := newDeliveryOwner("test", &mockChannel{}, "test")
	owner.closed = true
	m.channels["test"] = owner.ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.dispatchOutbound(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("dispatchOutbound did not stop after cancel")
		}
	}()

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := m.bus.PublishOutbound(pubCtx, testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "hello",
	})); err != nil {
		t.Fatalf("PublishOutbound() error = %v", err)
	}

	failed := receiveChannelRuntimeEvent(t, eventsCh)
	if failed.Kind != runtimeevents.KindChannelMessageOutboundFailed || failed.Scope.ChatID != "chat-1" {
		t.Fatalf("failed event = %+v", failed)
	}
	if failed.Attrs["error"] != errDeliveryClosed.Error() {
		t.Fatalf("failed attrs = %#v, want delivery closed error", failed.Attrs)
	}
}

func TestUnregisterChannel_DrainsDeliveryOutsideManagerLock(t *testing.T) {
	m := newTestManager()
	sendStarted := make(chan struct{})
	proceedSend := make(chan struct{})
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			close(sendStarted)
			<-proceedSend
			m.mu.RLock()
			m.mu.RUnlock()
			return nil
		},
	}
	owner := newDeliveryOwner("test", ch, "test")
	m.channels["test"] = ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner
	owner.StartDelivery(context.Background(), m)

	queued, err := owner.Enqueue(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "hello",
	}))
	if !queued || err != nil {
		t.Fatalf("Enqueue() queued=%v err=%v, want true nil", queued, err)
	}

	select {
	case <-sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not start sending queued message")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.UnregisterChannel("test")
	}()

	waitForDeliveryOwnerClosed(t, owner)
	m.mu.RLock()
	visibleOwner := m.deliveryOwners["test"]
	visibleChannel := m.channels["test"]
	m.mu.RUnlock()
	if visibleOwner != owner {
		t.Fatal("UnregisterChannel removed delivery owner before drain completed")
	}
	if visibleChannel != ch {
		t.Fatal("UnregisterChannel removed channel before drain completed")
	}
	err = m.SendMessage(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-2",
		Content: "late direct",
	}))
	if !errors.Is(err, errDeliveryClosed) {
		t.Fatalf("SendMessage() during unregister err=%v, want errDeliveryClosed", err)
	}

	close(proceedSend)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("UnregisterChannel deadlocked while draining delivery")
	}

	m.mu.RLock()
	_, ownerExists := m.deliveryOwners["test"]
	_, workerExists := m.workers["test"]
	_, channelExists := m.channels["test"]
	m.mu.RUnlock()
	if ownerExists || workerExists || channelExists {
		t.Fatalf(
			"UnregisterChannel left maps populated after drain: owner=%v worker=%v channel=%v",
			ownerExists,
			workerExists,
			channelExists,
		)
	}
}

func TestStopAll_DrainsDeliveryOutsideManagerLock(t *testing.T) {
	m := newTestManager()
	sendStarted := make(chan struct{})
	proceedSend := make(chan struct{})
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			close(sendStarted)
			<-proceedSend
			m.mu.RLock()
			m.mu.RUnlock()
			return nil
		},
	}
	owner := newDeliveryOwner("test", ch, "test")
	m.channels["test"] = ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner
	owner.StartDelivery(context.Background(), m)

	queued, err := owner.Enqueue(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "hello",
	}))
	if !queued || err != nil {
		t.Fatalf("Enqueue() queued=%v err=%v, want true nil", queued, err)
	}

	select {
	case <-sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not start sending queued message")
	}

	done := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- m.StopAll(stopCtx)
	}()
	close(proceedSend)

	select {
	case stopErr := <-done:
		if stopErr != nil {
			t.Fatalf("StopAll() error = %v", stopErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StopAll deadlocked while draining delivery")
	}

	if got := m.deliveryOwners["test"]; got != owner {
		t.Fatal("StopAll removed delivery owner visibility")
	}
	queued, err = owner.Enqueue(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "late",
	}))
	if queued || !errors.Is(err, errDeliveryClosed) {
		t.Fatalf("late Enqueue() queued=%v err=%v, want false errDeliveryClosed", queued, err)
	}
}

func waitForDeliveryOwnerClosed(t *testing.T, owner *deliveryOwner) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		owner.mu.Lock()
		closed := owner.closed
		owner.mu.Unlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("delivery owner was not closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStartAllPublishesLifecycleRuntimeEvents(t *testing.T) {
	eventBus := runtimeevents.NewBus()
	defer func() {
		if err := eventBus.Close(); err != nil {
			t.Errorf("event bus close failed: %v", err)
		}
	}()

	_, eventsCh, err := eventBus.Channel().SubscribeChan(
		t.Context(),
		runtimeevents.SubscribeOptions{Name: "channel-lifecycle", Buffer: 4},
	)
	if err != nil {
		t.Fatalf("SubscribeChan failed: %v", err)
	}

	m := newTestManager()
	m.runtimeEvents = eventBus
	m.config = &config.Config{Channels: config.ChannelsConfig{}}
	m.channels["good"] = &mockChannel{}
	m.channels["bad"] = &mockChannel{
		startFn: func(_ context.Context) error { return errors.New("bad start") },
	}

	if err := m.StartAll(t.Context()); err != nil {
		t.Fatalf("StartAll() error = %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.StopAll(stopCtx); err != nil {
			t.Errorf("StopAll() error = %v", err)
		}
	})

	events := []runtimeevents.Event{
		receiveChannelRuntimeEvent(t, eventsCh),
		receiveChannelRuntimeEvent(t, eventsCh),
	}
	seen := map[runtimeevents.Kind]runtimeevents.Event{}
	for _, evt := range events {
		seen[evt.Kind] = evt
	}
	if evt, ok := seen[runtimeevents.KindChannelLifecycleStarted]; !ok || evt.Scope.Channel != "good" {
		t.Fatalf("missing started event for good channel: %+v", events)
	}
	if evt, ok := seen[runtimeevents.KindChannelLifecycleStartFailed]; !ok || evt.Scope.Channel != "bad" {
		t.Fatalf("missing failed event for bad channel: %+v", events)
	}
}

func testOutboundMessage(msg bus.OutboundMessage) bus.OutboundMessage {
	if msg.Context.Channel == "" && msg.Context.ChatID == "" {
		msg.Context = bus.NewOutboundContext(msg.Channel, msg.ChatID, msg.ReplyToMessageID)
	}
	normalized, err := bus.NormalizeOutboundMessage(msg)
	if err != nil {
		panic(err)
	}
	return normalized
}

func testOutboundMediaMessage(msg bus.OutboundMediaMessage) bus.OutboundMediaMessage {
	if msg.Context.Channel == "" && msg.Context.ChatID == "" {
		msg.Context = bus.NewOutboundContext(msg.Channel, msg.ChatID, "")
	}
	normalized, err := bus.NormalizeOutboundMediaMessage(msg)
	if err != nil {
		panic(err)
	}
	return normalized
}

func receiveChannelRuntimeEvent(t *testing.T, ch <-chan runtimeevents.Event) runtimeevents.Event {
	t.Helper()

	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatal("runtime event channel closed before expected event")
		}
		return evt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runtime event")
		return runtimeevents.Event{}
	}
}

func TestSendWithRetry_Success(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 1 {
		t.Fatalf("expected 1 Send call, got %d", callCount)
	}
}

func TestSendWithRetryPublishesOutboundRuntimeEvents(t *testing.T) {
	eventBus := runtimeevents.NewBus()
	defer func() {
		if err := eventBus.Close(); err != nil {
			t.Errorf("event bus close failed: %v", err)
		}
	}()

	_, eventsCh, err := eventBus.Channel().OfKind(
		runtimeevents.KindChannelMessageOutboundSent,
		runtimeevents.KindChannelMessageOutboundFailed,
	).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "channel-outbound", Buffer: 2})
	if err != nil {
		t.Fatalf("SubscribeChan failed: %v", err)
	}

	m := newTestManager()
	m.runtimeEvents = eventBus

	successWorker := &channelWorker{
		ch:      &mockChannel{},
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.sendWithRetry(
		context.Background(),
		"test",
		successWorker,
		testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "chat-1", Content: "hello"}),
	)
	sent := receiveChannelRuntimeEvent(t, eventsCh)
	if sent.Kind != runtimeevents.KindChannelMessageOutboundSent || sent.Scope.ChatID != "chat-1" {
		t.Fatalf("sent event = %+v", sent)
	}
	if sent.Attrs["content_len"] != 5 {
		t.Fatalf("sent attrs = %#v, want content_len", sent.Attrs)
	}

	failWorker := &channelWorker{
		ch: &mockChannel{
			sendFn: func(context.Context, bus.OutboundMessage) error {
				return fmt.Errorf("send failed: %w", ErrSendFailed)
			},
		},
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.sendWithRetry(
		context.Background(),
		"test",
		failWorker,
		testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "chat-2", Content: "hello"}),
	)
	failed := receiveChannelRuntimeEvent(t, eventsCh)
	if failed.Kind != runtimeevents.KindChannelMessageOutboundFailed || failed.Scope.ChatID != "chat-2" {
		t.Fatalf("failed event = %+v", failed)
	}
	if failed.Severity != runtimeevents.SeverityError {
		t.Fatalf("failed severity = %q", failed.Severity)
	}
	if failed.Attrs["error"] == "" || failed.Attrs["retries"] != maxRetries {
		t.Fatalf("failed attrs = %#v, want error and retries", failed.Attrs)
	}
}

func TestOutboundRuntimeEventsPreserveTraceScopes(t *testing.T) {
	eventBus := runtimeevents.NewBus()
	t.Cleanup(func() { _ = eventBus.Close() })
	_, eventsCh, err := eventBus.Channel().OfKind(
		runtimeevents.KindChannelRateLimited,
		runtimeevents.KindChannelMessageOutboundQueued,
		runtimeevents.KindChannelMessageOutboundSent,
		runtimeevents.KindChannelMessageOutboundFailed,
	).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "trace-transport", Buffer: 12})
	if err != nil {
		t.Fatalf("SubscribeChan failed: %v", err)
	}

	m := newTestManager()
	m.runtimeEvents = eventBus
	traceScopes := []runtimeevents.TraceScope{
		runtimeevents.NewTraceScope("/workspace/main", "turn-1"),
		runtimeevents.NewTraceScope("/workspace/main", "turn-2"),
	}
	text := testOutboundMessage(bus.OutboundMessage{
		Channel: "test", ChatID: "chat-1", Content: "hello", TraceScopes: traceScopes,
		TraceSettlement: true,
	})
	media := testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel: "test", ChatID: "chat-1", TraceScopes: traceScopes, TraceSettlement: true,
	})

	m.publishOutboundQueued("test", text)
	m.publishOutboundSent("test", text, []string{"text-1"})
	m.publishOutboundFailed("test", text, errors.New("text failed"), false)
	m.publishOutboundMediaQueued("test", media)
	m.publishOutboundMediaSent("test", media, []string{"media-1"})
	m.publishOutboundMediaFailed("test", media, errors.New("media failed"))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	textWorker := &channelWorker{ch: &mockChannel{}, limiter: rate.NewLimiter(0, 0)}
	messageIDs, delivered, ambiguous, sendErr := m.sendWithRetry(canceled, "test", textWorker, text)
	if sendErr == nil || delivered || ambiguous || len(messageIDs) != 0 {
		t.Fatalf("canceled text send = ids %v, delivered %v, ambiguous %v, err %v",
			messageIDs, delivered, ambiguous, sendErr)
	}
	mediaWorker := &channelWorker{ch: &mockMediaChannel{}, limiter: rate.NewLimiter(0, 0)}
	mediaIDs, mediaErr := m.sendMediaWithRetry(canceled, "test", mediaWorker, media)
	if mediaErr == nil || len(mediaIDs) != 0 {
		t.Fatalf("canceled media send = ids %v, err %v", mediaIDs, mediaErr)
	}

	for i := 0; i < 10; i++ {
		event := receiveChannelRuntimeEvent(t, eventsCh)
		payload, ok := event.Payload.(ChannelOutboundPayload)
		if !ok || !payload.TraceSettlement || !slices.Equal(payload.TraceScopes, traceScopes) {
			t.Fatalf("event %d payload = %#v, want trace scopes %+v", i, event.Payload, traceScopes)
		}
		if event.Attrs["trace_scopes_count"] != 2 {
			t.Fatalf("event %d attrs = %#v, want trace_scopes_count=2", i, event.Attrs)
		}
	}
}

func TestProvisionalSendPublishesSuccessButSuppressesFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		send func(context.Context, *Manager) error
	}{
		{
			name: "text",
			send: func(ctx context.Context, m *Manager) error {
				return m.SendMessageProvisional(ctx, testOutboundMessage(bus.OutboundMessage{
					Channel: "test", ChatID: "chat-1", Content: "hello",
				}))
			},
		},
		{
			name: "media",
			send: func(ctx context.Context, m *Manager) error {
				return m.SendMediaProvisional(ctx, testOutboundMediaMessage(bus.OutboundMediaMessage{
					Channel: "test", ChatID: "chat-1",
				}))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, fail := range []bool{false, true} {
				name := "success"
				if fail {
					name = "failure"
				}
				t.Run(name, func(t *testing.T) {
					eventBus := runtimeevents.NewBus()
					t.Cleanup(func() { _ = eventBus.Close() })
					_, eventsCh, err := eventBus.Channel().OfKind(
						runtimeevents.KindChannelMessageOutboundSent,
						runtimeevents.KindChannelMessageOutboundFailed,
					).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
						Name: "provisional", Buffer: 1,
					})
					if err != nil {
						t.Fatalf("SubscribeChan failed: %v", err)
					}

					sendErr := error(nil)
					if fail {
						sendErr = ErrSendFailed
					}
					channel := &mockMediaChannel{
						mockChannel: mockChannel{sendFn: func(context.Context, bus.OutboundMessage) error {
							return sendErr
						}},
						sendMediaFn: func(context.Context, bus.OutboundMediaMessage) ([]string, error) {
							return nil, sendErr
						},
					}
					m := newTestManager()
					m.runtimeEvents = eventBus
					m.channels["test"] = channel
					m.workers["test"] = &channelWorker{
						ch: channel, limiter: rate.NewLimiter(rate.Inf, 1),
					}

					err = tc.send(context.Background(), m)
					if (err != nil) != fail {
						t.Fatalf("provisional send error = %v, fail = %v", err, fail)
					}
					if fail {
						select {
						case event := <-eventsCh:
							t.Fatalf("provisional failure emitted terminal event: %#v", event)
						case <-time.After(25 * time.Millisecond):
						}
						return
					}
					event := receiveChannelRuntimeEvent(t, eventsCh)
					if event.Kind != runtimeevents.KindChannelMessageOutboundSent {
						t.Fatalf("provisional success event = %#v", event)
					}
				})
			}
		})
	}
}

func TestProvisionalAmbiguousFailureRemainsTerminal(t *testing.T) {
	for _, tc := range []struct {
		name string
		send func(context.Context, *Manager) error
	}{
		{
			name: "text",
			send: func(ctx context.Context, m *Manager) error {
				return m.SendMessageProvisional(ctx, testOutboundMessage(bus.OutboundMessage{
					Channel: "test", ChatID: "chat-1", Content: "hello",
				}))
			},
		},
		{
			name: "media",
			send: func(ctx context.Context, m *Manager) error {
				return m.SendMediaProvisional(ctx, testOutboundMediaMessage(bus.OutboundMediaMessage{
					Channel: "test", ChatID: "chat-1",
				}))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventBus := runtimeevents.NewBus()
			t.Cleanup(func() { _ = eventBus.Close() })
			_, eventsCh, err := eventBus.Channel().OfKind(
				runtimeevents.KindChannelMessageOutboundFailed,
			).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "ambiguous", Buffer: 1})
			if err != nil {
				t.Fatalf("SubscribeChan failed: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			channel := &mockMediaChannel{
				mockChannel: mockChannel{sendFn: func(context.Context, bus.OutboundMessage) error {
					cancel()
					return ErrTemporary
				}},
				sendMediaFn: func(context.Context, bus.OutboundMediaMessage) ([]string, error) {
					cancel()
					return nil, ErrTemporary
				},
			}
			m := newTestManager()
			m.runtimeEvents = eventBus
			m.channels["test"] = channel
			m.workers["test"] = &channelWorker{ch: channel, limiter: rate.NewLimiter(rate.Inf, 1)}

			err = tc.send(ctx, m)
			if err == nil || DeliveryDefinitelyNotSent(err) {
				t.Fatalf("ambiguous provisional error = %v", err)
			}
			event := receiveChannelRuntimeEvent(t, eventsCh)
			if event.Kind != runtimeevents.KindChannelMessageOutboundFailed {
				t.Fatalf("ambiguous provisional event = %#v", event)
			}
		})
	}
}

func TestSynchronousPreWorkerRejectionPublishesOnlyDefinitiveOutcome(t *testing.T) {
	for _, media := range []bool{false, true} {
		kind := "text"
		if media {
			kind = "media"
		}
		for _, condition := range []string{"unknown", "no_worker", "closed"} {
			for _, provisional := range []bool{false, true} {
				mode := "definitive"
				if provisional {
					mode = "provisional"
				}
				t.Run(kind+"/"+condition+"/"+mode, func(t *testing.T) {
					eventBus := runtimeevents.NewBus()
					t.Cleanup(func() { _ = eventBus.Close() })
					_, eventsCh, err := eventBus.Channel().OfKind(
						runtimeevents.KindChannelMessageOutboundFailed,
					).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
						Name: "pre-worker-rejection", Buffer: 1,
					})
					if err != nil {
						t.Fatalf("SubscribeChan failed: %v", err)
					}

					m := newTestManager()
					m.runtimeEvents = eventBus
					channel := &mockMediaChannel{}
					if condition != "unknown" {
						m.channels["test"] = channel
					}
					if condition == "closed" {
						owner := newDeliveryOwner("test", channel, "test")
						owner.closed = true
						m.workers["test"] = owner.Worker()
						m.deliveryOwners["test"] = owner
					}

					traceScope := runtimeevents.NewTraceScope("/workspace/main", "turn-1")
					if media {
						msg := testOutboundMediaMessage(bus.OutboundMediaMessage{
							Channel: "test", ChatID: "chat-1",
							TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
						})
						if provisional {
							err = m.SendMediaProvisional(context.Background(), msg)
						} else {
							err = m.SendMedia(context.Background(), msg)
						}
					} else {
						msg := testOutboundMessage(bus.OutboundMessage{
							Channel: "test", ChatID: "chat-1", Content: "hello",
							TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
						})
						if provisional {
							err = m.SendMessageProvisional(context.Background(), msg)
						} else {
							err = m.SendMessage(context.Background(), msg)
						}
					}
					if err == nil || !DeliveryDefinitelyNotSent(err) {
						t.Fatalf("pre-worker error = %v", err)
					}

					if provisional {
						select {
						case event := <-eventsCh:
							t.Fatalf("provisional rejection emitted terminal event: %#v", event)
						case <-time.After(25 * time.Millisecond):
						}
						return
					}
					failed := receiveChannelRuntimeEvent(t, eventsCh)
					payload, ok := failed.Payload.(ChannelOutboundPayload)
					if !ok || !payload.TraceSettlement ||
						!slices.Equal(payload.TraceScopes, []runtimeevents.TraceScope{traceScope}) {
						t.Fatalf("definitive rejection event = %#v", failed)
					}
				})
			}
		}
	}
}

func TestSendMessagePublishesOneLogicalChunkOutcome(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failChunk int
		wantKind  runtimeevents.Kind
		wantErr   bool
	}{
		{name: "success", wantKind: runtimeevents.KindChannelMessageOutboundSent},
		{name: "partial_failure", failChunk: 2, wantKind: runtimeevents.KindChannelMessageOutboundFailed, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventBus := runtimeevents.NewBus()
			t.Cleanup(func() { _ = eventBus.Close() })
			_, eventsCh, err := eventBus.Channel().OfKind(
				runtimeevents.KindChannelMessageOutboundSent,
				runtimeevents.KindChannelMessageOutboundFailed,
			).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "chunk-outcome", Buffer: 4})
			if err != nil {
				t.Fatalf("SubscribeChan failed: %v", err)
			}

			calls := 0
			channel := &mockChannelWithLength{
				mockChannel: mockChannel{sendFn: func(context.Context, bus.OutboundMessage) error {
					calls++
					if calls == tc.failChunk {
						return fmt.Errorf("chunk rejected: %w", ErrSendFailed)
					}
					return nil
				}},
				maxLen: 5,
			}
			m := newTestManager()
			m.runtimeEvents = eventBus
			m.channels["test"] = channel
			m.workers["test"] = &channelWorker{ch: channel, limiter: rate.NewLimiter(rate.Inf, 1)}
			traceScope := runtimeevents.NewTraceScope("/workspace/main", "turn-1")
			msg := testOutboundMessage(bus.OutboundMessage{
				Channel: "test", ChatID: "chat-1", Content: "hello world",
				TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
			})

			err = m.SendMessage(context.Background(), msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SendMessage error = %v, wantErr %v", err, tc.wantErr)
			}
			outcome := receiveChannelRuntimeEvent(t, eventsCh)
			payload, ok := outcome.Payload.(ChannelOutboundPayload)
			if outcome.Kind != tc.wantKind || !ok || !payload.TraceSettlement ||
				!slices.Equal(payload.TraceScopes, []runtimeevents.TraceScope{traceScope}) {
				t.Fatalf("outcome = %#v, want kind %q and settlement", outcome, tc.wantKind)
			}
			select {
			case extra := <-eventsCh:
				t.Fatalf("unexpected per-chunk terminal event: %#v", extra)
			case <-time.After(25 * time.Millisecond):
			}
		})
	}
}

func TestRetryCancellationPublishesTerminalFailure(t *testing.T) {
	for _, sendErr := range []error{ErrRateLimit, ErrTemporary} {
		t.Run(classifySendError(sendErr), func(t *testing.T) {
			eventBus := runtimeevents.NewBus()
			t.Cleanup(func() { _ = eventBus.Close() })
			_, eventsCh, err := eventBus.Channel().OfKind(
				runtimeevents.KindChannelMessageOutboundFailed,
			).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "retry-cancel", Buffer: 1})
			if err != nil {
				t.Fatalf("SubscribeChan failed: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			worker := &channelWorker{
				ch: &mockChannel{sendFn: func(context.Context, bus.OutboundMessage) error {
					cancel()
					return sendErr
				}},
				limiter: rate.NewLimiter(rate.Inf, 1),
			}
			m := newTestManager()
			m.runtimeEvents = eventBus
			traceScope := runtimeevents.NewTraceScope("/workspace/main", "turn-1")
			_, _, _, _ = m.sendWithRetry(ctx, "test", worker, testOutboundMessage(bus.OutboundMessage{
				Channel: "test", ChatID: "chat-1", Content: "hello",
				TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
			}))

			failed := receiveChannelRuntimeEvent(t, eventsCh)
			payload, ok := failed.Payload.(ChannelOutboundPayload)
			if failed.Kind != runtimeevents.KindChannelMessageOutboundFailed || !ok ||
				!payload.TraceSettlement ||
				!slices.Equal(payload.TraceScopes, []runtimeevents.TraceScope{traceScope}) {
				t.Fatalf("failed event = %#v", failed)
			}
		})
	}
}

func TestSendWithRetry_TemporaryThenSuccess(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount <= 2 {
				return fmt.Errorf("network error: %w", ErrTemporary)
			}
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 3 {
		t.Fatalf("expected 3 Send calls (2 failures + 1 success), got %d", callCount)
	}
}

func TestSendWithRetry_PermanentFailure(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return fmt.Errorf("bad chat ID: %w", ErrSendFailed)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 1 {
		t.Fatalf("expected 1 Send call (no retry for permanent failure), got %d", callCount)
	}
}

func TestSendWithRetry_NotRunning(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return ErrNotRunning
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 1 {
		t.Fatalf("expected 1 Send call (no retry for ErrNotRunning), got %d", callCount)
	}
}

func TestSendWithRetry_RateLimitRetry(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount == 1 {
				return fmt.Errorf("429: %w", ErrRateLimit)
			}
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	start := time.Now()
	m.sendWithRetry(ctx, "test", w, msg)
	elapsed := time.Since(start)

	if callCount != 2 {
		t.Fatalf("expected 2 Send calls (1 rate limit + 1 success), got %d", callCount)
	}
	// Should have waited at least rateLimitDelay (1s) but allow some slack
	if elapsed < 900*time.Millisecond {
		t.Fatalf("expected at least ~1s delay for rate limit retry, got %v", elapsed)
	}
}

func TestSendWithRetry_MaxRetriesExhausted(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return fmt.Errorf("timeout: %w", ErrTemporary)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	m.sendWithRetry(ctx, "test", w, msg)

	expected := maxRetries + 1 // initial attempt + maxRetries retries
	if callCount != expected {
		t.Fatalf("expected %d Send calls, got %d", expected, callCount)
	}
}

func TestSendMedia_Success(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockMediaChannel{
		sendMediaFn: func(_ context.Context, _ bus.OutboundMediaMessage) ([]string, error) {
			callCount++
			return nil, nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	err := m.SendMedia(context.Background(), testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel: "test",
		ChatID:  "chat1",
		Parts:   []bus.MediaPart{{Ref: "media://abc"}},
	}))
	if err != nil {
		t.Fatalf("SendMedia() error = %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 SendMedia call, got %d", callCount)
	}
}

func TestSendMedia_PropagatesFailure(t *testing.T) {
	m := newTestManager()
	ch := &mockMediaChannel{
		sendMediaFn: func(_ context.Context, _ bus.OutboundMediaMessage) ([]string, error) {
			return nil, fmt.Errorf("bad upload: %w", ErrSendFailed)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	err := m.SendMedia(context.Background(), testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel: "test",
		ChatID:  "chat1",
		Parts:   []bus.MediaPart{{Ref: "media://abc"}},
	}))
	if err == nil {
		t.Fatal("expected SendMedia to return error")
	}
	if !errors.Is(err, ErrSendFailed) {
		t.Fatalf("expected ErrSendFailed, got %v", err)
	}
}

func TestSendMedia_UnsupportedChannelReturnsError(t *testing.T) {
	eventBus := runtimeevents.NewBus()
	t.Cleanup(func() { _ = eventBus.Close() })
	_, eventsCh, err := eventBus.Channel().OfKind(
		runtimeevents.KindChannelMessageOutboundFailed,
	).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "unsupported-media", Buffer: 1})
	if err != nil {
		t.Fatalf("SubscribeChan failed: %v", err)
	}

	m := newTestManager()
	m.runtimeEvents = eventBus
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w
	traceScopes := []runtimeevents.TraceScope{
		runtimeevents.NewTraceScope("/workspace/main", "turn-1"),
	}

	err = m.SendMedia(context.Background(), testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel:     "test",
		ChatID:      "chat1",
		TraceScopes: traceScopes,
		Parts:       []bus.MediaPart{{Ref: "media://abc"}},
	}))
	if err == nil {
		t.Fatal("expected SendMedia to return error for unsupported channel")
	}
	if !strings.Contains(err.Error(), "does not support media sending") {
		t.Fatalf("unexpected error: %v", err)
	}
	failed := receiveChannelRuntimeEvent(t, eventsCh)
	payload, ok := failed.Payload.(ChannelOutboundPayload)
	if failed.Kind != runtimeevents.KindChannelMessageOutboundFailed || !ok || !payload.Media ||
		!strings.Contains(payload.Error, "does not support media sending") ||
		!slices.Equal(payload.TraceScopes, traceScopes) {
		t.Fatalf("failed event = %#v", failed)
	}
}

func TestSendMedia_ClosedDeliveryOwnerReturnsError(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockMediaChannel{
		sendMediaFn: func(_ context.Context, _ bus.OutboundMediaMessage) ([]string, error) {
			callCount++
			return nil, nil
		},
	}
	owner := newDeliveryOwner("test", ch, "test")
	owner.closed = true
	m.channels["test"] = ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner

	err := m.SendMedia(context.Background(), testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel: "test",
		ChatID:  "chat1",
		Parts:   []bus.MediaPart{{Ref: "media://abc"}},
	}))
	if !errors.Is(err, errDeliveryClosed) {
		t.Fatalf("SendMedia() err=%v, want errDeliveryClosed", err)
	}
	if callCount != 0 {
		t.Fatalf("SendMedia called closed channel %d times", callCount)
	}
}

func TestSendMedia_DeletesPlaceholderBeforeSending(t *testing.T) {
	m := newTestManager()
	ch := &mockDeletingMediaChannel{
		mockMediaChannel: mockMediaChannel{
			sendMediaFn: func(_ context.Context, _ bus.OutboundMediaMessage) ([]string, error) {
				return nil, nil
			},
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w
	m.RecordPlaceholder("test", "chat1", "placeholder-1")

	err := m.SendMedia(context.Background(), testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel: "test",
		ChatID:  "chat1",
		Parts:   []bus.MediaPart{{Ref: "media://abc"}},
	}))
	if err != nil {
		t.Fatalf("SendMedia() error = %v", err)
	}
	if ch.deleteCalls != 1 {
		t.Fatalf("expected placeholder delete to be called once, got %d", ch.deleteCalls)
	}
	if ch.lastDeleted.chatID != "chat1" || ch.lastDeleted.messageID != "placeholder-1" {
		t.Fatalf("unexpected placeholder deletion target: %+v", ch.lastDeleted)
	}
	if len(ch.sentMediaMessages) != 1 {
		t.Fatalf("expected media to be sent once, got %d", len(ch.sentMediaMessages))
	}
}

func TestSendWithRetry_UnknownError(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount == 1 {
				return errors.New("random unexpected error")
			}
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 2 {
		t.Fatalf("expected 2 Send calls (unknown error treated as temporary), got %d", callCount)
	}
}

func TestSendWithRetry_ContextCancelled(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return fmt.Errorf("timeout: %w", ErrTemporary)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	// Cancel context after first Send attempt returns
	ch.sendFn = func(_ context.Context, _ bus.OutboundMessage) error {
		callCount++
		cancel()
		return fmt.Errorf("timeout: %w", ErrTemporary)
	}

	m.sendWithRetry(ctx, "test", w, msg)

	// Should have called Send once, then noticed ctx canceled during backoff
	if callCount != 1 {
		t.Fatalf("expected 1 Send call before context cancellation, got %d", callCount)
	}
}

func TestWorkerRateLimiter(t *testing.T) {
	m := newTestManager()

	var mu sync.Mutex
	var sendTimes []time.Time

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			mu.Lock()
			sendTimes = append(sendTimes, time.Now())
			mu.Unlock()
			return nil
		},
	}

	// Create a worker with a low rate: 2 msg/s, burst 1
	w := &channelWorker{
		ch:      ch,
		queue:   make(chan bus.OutboundMessage, 10),
		done:    make(chan struct{}),
		limiter: rate.NewLimiter(2, 1),
	}

	ctx := t.Context()

	go m.runWorker(ctx, "test", w)

	// Enqueue 4 messages
	for i := range 4 {
		w.queue <- testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: fmt.Sprintf("msg%d", i)})
	}

	// Wait enough time for all messages to be sent (4 msgs at 2/s = ~2s, give extra margin)
	time.Sleep(3 * time.Second)

	mu.Lock()
	times := make([]time.Time, len(sendTimes))
	copy(times, sendTimes)
	mu.Unlock()

	if len(times) != 4 {
		t.Fatalf("expected 4 sends, got %d", len(times))
	}

	// Verify rate limiting: total duration should be at least 1s
	// (first message immediate, then ~500ms between each subsequent one at 2/s)
	totalDuration := times[len(times)-1].Sub(times[0])
	if totalDuration < 1*time.Second {
		t.Fatalf("expected total duration >= 1s for 4 msgs at 2/s rate, got %v", totalDuration)
	}
}

func TestNewChannelWorker_DefaultRate(t *testing.T) {
	ch := &mockChannel{}
	w := newChannelWorker("unknown_channel", ch, "unknown_channel")

	if w.limiter == nil {
		t.Fatal("expected limiter to be non-nil")
	}
	if w.limiter.Limit() != rate.Limit(defaultRateLimit) {
		t.Fatalf("expected rate limit %v, got %v", rate.Limit(defaultRateLimit), w.limiter.Limit())
	}
}

func TestNewChannelWorker_ConfiguredRate(t *testing.T) {
	ch := &mockChannel{}

	for channelType, expectedRate := range channelRateConfig {
		w := newChannelWorker(channelType, ch, channelType)
		if w.limiter.Limit() != rate.Limit(expectedRate) {
			t.Fatalf("channel %s: expected rate %v, got %v", channelType, expectedRate, w.limiter.Limit())
		}
	}
}

func TestRunWorker_MessageSplitting(t *testing.T) {
	m := newTestManager()
	eventBus := runtimeevents.NewBus()
	t.Cleanup(func() { _ = eventBus.Close() })
	m.runtimeEvents = eventBus
	_, eventsCh, err := eventBus.Channel().OfKind(
		runtimeevents.KindChannelMessageOutboundSent,
		runtimeevents.KindChannelMessageOutboundFailed,
	).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "worker-chunk-outcome", Buffer: 4})
	if err != nil {
		t.Fatalf("SubscribeChan failed: %v", err)
	}

	var mu sync.Mutex
	var received []string

	ch := &mockChannelWithLength{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
				mu.Lock()
				received = append(received, msg.Content)
				mu.Unlock()
				return nil
			},
		},
		maxLen: 5,
	}

	w := &channelWorker{
		ch:      ch,
		queue:   make(chan bus.OutboundMessage, 10),
		done:    make(chan struct{}),
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := t.Context()

	go m.runWorker(ctx, "test", w)

	// Send a message that should be split
	w.queue <- testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello world"})

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count < 2 {
		t.Fatalf("expected message to be split into at least 2 chunks, got %d", count)
	}
	if event := receiveChannelRuntimeEvent(t, eventsCh); event.Kind != runtimeevents.KindChannelMessageOutboundSent {
		t.Fatalf("logical worker outcome = %#v", event)
	}
	select {
	case extra := <-eventsCh:
		t.Fatalf("unexpected per-chunk worker outcome: %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestRunWorkerCancellationBeforeFirstSendPublishesTerminalFailure(t *testing.T) {
	eventBus := runtimeevents.NewBus()
	t.Cleanup(func() { _ = eventBus.Close() })
	_, eventsCh, err := eventBus.Channel().OfKind(
		runtimeevents.KindChannelMessageOutboundFailed,
	).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "worker-cancel", Buffer: 1})
	if err != nil {
		t.Fatalf("SubscribeChan failed: %v", err)
	}

	limiter := rate.NewLimiter(rate.Every(time.Hour), 1)
	if !limiter.Allow() {
		t.Fatal("failed to consume initial rate-limit token")
	}
	sendCalls := 0
	worker := &channelWorker{
		ch: &mockChannel{sendFn: func(context.Context, bus.OutboundMessage) error {
			sendCalls++
			return nil
		}},
		queue:   make(chan bus.OutboundMessage, 1),
		done:    make(chan struct{}),
		limiter: limiter,
	}
	m := newTestManager()
	m.runtimeEvents = eventBus
	ctx, cancel := context.WithCancel(context.Background())
	go m.runWorker(ctx, "test", worker)

	traceScope := runtimeevents.NewTraceScope("/workspace/main", "turn-1")
	worker.queue <- testOutboundMessage(bus.OutboundMessage{
		Channel: "test", ChatID: "chat-1", Content: "hello",
		TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
	})
	deadline := time.Now().Add(time.Second)
	for len(worker.queue) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(worker.queue) != 0 {
		t.Fatal("worker did not accept queued message")
	}
	cancel()
	<-worker.done

	failed := receiveChannelRuntimeEvent(t, eventsCh)
	payload, ok := failed.Payload.(ChannelOutboundPayload)
	if !ok || !payload.TraceSettlement || payload.Error != context.Canceled.Error() ||
		!slices.Equal(payload.TraceScopes, []runtimeevents.TraceScope{traceScope}) {
		t.Fatalf("canceled worker outcome = %#v", failed)
	}
	if sendCalls != 0 {
		t.Fatalf("remote send calls = %d, want 0", sendCalls)
	}
}

// mockChannelWithLength implements MessageLengthProvider.
type mockChannelWithLength struct {
	mockChannel
	maxLen int
}

func (m *mockChannelWithLength) MaxMessageLength() int {
	return m.maxLen
}

func TestSendWithRetry_ExponentialBackoff(t *testing.T) {
	m := newTestManager()

	var callTimes []time.Time
	var callCount atomic.Int32
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callTimes = append(callTimes, time.Now())
			callCount.Add(1)
			return fmt.Errorf("timeout: %w", ErrTemporary)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"})

	start := time.Now()
	m.sendWithRetry(ctx, "test", w, msg)
	totalElapsed := time.Since(start)

	// With maxRetries=3: attempts at 0, ~500ms, ~1.5s, ~3.5s
	// Total backoff: 500ms + 1s + 2s = 3.5s
	// Allow some margin
	if totalElapsed < 3*time.Second {
		t.Fatalf("expected total elapsed >= 3s for exponential backoff, got %v", totalElapsed)
	}

	if int(callCount.Load()) != maxRetries+1 {
		t.Fatalf("expected %d calls, got %d", maxRetries+1, callCount.Load())
	}
}

// --- Phase 10: preSend orchestration tests ---

// mockMessageEditor is a channel that supports MessageEditor.
type mockMessageEditor struct {
	mockChannel
	editFn            func(ctx context.Context, chatID, messageID, content string) error
	finalizeFn        func(ctx context.Context, msg bus.OutboundMessage) ([]string, bool)
	finalizeCalled    bool
	recordedChatID    string
	recordedMessageID string
	recordedContent   string
	clearedChatID     string
	dismissedChatID   string
	dismissedChatIDs  []string
}

func (m *mockMessageEditor) EditMessage(ctx context.Context, chatID, messageID, content string) error {
	return m.editFn(ctx, chatID, messageID, content)
}

func (m *mockMessageEditor) RecordToolFeedbackMessage(chatID, messageID, content string) {
	m.recordedChatID = chatID
	m.recordedMessageID = messageID
	m.recordedContent = content
}

func (m *mockMessageEditor) ClearToolFeedbackMessage(chatID string) {
	m.clearedChatID = chatID
}

func (m *mockMessageEditor) DismissToolFeedbackMessage(_ context.Context, chatID string) {
	m.dismissedChatID = chatID
	m.dismissedChatIDs = append(m.dismissedChatIDs, chatID)
}

func (m *mockMessageEditor) FinalizeToolFeedbackMessage(
	ctx context.Context,
	msg bus.OutboundMessage,
) ([]string, bool) {
	m.finalizeCalled = true
	if m.finalizeFn == nil {
		return nil, false
	}
	return m.finalizeFn(ctx, msg)
}

type mockResolvedToolFeedbackEditor struct {
	mockMessageEditor
	resolveChatIDFn func(chatID string, outboundCtx *bus.InboundContext) string
}

type mockDeletingMessageEditor struct {
	mockMessageEditor
	deleteCalls      int
	deletedChatID    string
	deletedMessageID string
}

func (m *mockDeletingMessageEditor) DeleteMessage(_ context.Context, chatID, messageID string) error {
	m.deleteCalls++
	m.deletedChatID = chatID
	m.deletedMessageID = messageID
	return nil
}

func (m *mockResolvedToolFeedbackEditor) ResolveOutboundChatID(
	chatID string,
	outboundCtx *bus.InboundContext,
) string {
	if m.resolveChatIDFn != nil {
		return m.resolveChatIDFn(chatID, outboundCtx)
	}
	return chatID
}

func TestToolFeedbackTarget_UsesResolvedTopicScopedKey(t *testing.T) {
	ch := &mockResolvedToolFeedbackEditor{
		resolveChatIDFn: func(chatID string, outboundCtx *bus.InboundContext) string {
			if chatID != "-100123" {
				t.Fatalf("chatID = %q, want -100123", chatID)
			}
			if outboundCtx == nil || outboundCtx.TopicID != "6" {
				t.Fatalf("unexpected outbound context: %+v", outboundCtx)
			}
			return "-100123/6"
		},
	}
	key, deliveryChatID := toolFeedbackTarget(
		"telegram",
		ch,
		"-100123",
		&bus.InboundContext{Channel: "telegram", ChatID: "-100123", TopicID: "6"},
		"subturn-1",
	)
	if deliveryChatID != "-100123/6" || key != "telegram:-100123/6#session:subturn-1" {
		t.Fatalf("target = %q/%q, want resolved topic/session key", deliveryChatID, key)
	}
}

func TestSendWithRetry_ToolFeedbackLifecycleOwnedByManager(t *testing.T) {
	m := newTestManager()
	enableTestToolFeedbackCoordinator(t, m, false)
	ch := &toolFeedbackTestChannel{resolvedID: "topic-42", preparedTag: "prepared:"}
	w := &channelWorker{ch: ch, limiter: rate.NewLimiter(rate.Inf, 1)}

	feedback := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "Working...\n- tool: exec",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "chat-1",
			Raw:     map[string]string{"message_kind": "tool_feedback"},
		},
	})
	ids, sent, _, err := m.sendWithRetry(context.Background(), "test", w, feedback)
	if err != nil || !sent || !slices.Equal(ids, []string{"msg-1"}) {
		t.Fatalf("initial feedback = (%v, %v, %v), want msg-1 sent", ids, sent, err)
	}

	feedback.Content = "Working...\n- tool: read_file"
	ids, sent, _, err = m.sendWithRetry(context.Background(), "test", w, feedback)
	if err != nil || !sent || !slices.Equal(ids, []string{"msg-1"}) {
		t.Fatalf("feedback update = (%v, %v, %v), want edit of msg-1", ids, sent, err)
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.operations) != 2 || !strings.HasPrefix(ch.operations[0], "send:") || ch.operations[1] != "edit:msg-1" {
		t.Fatalf("operations = %v, want one send followed by one edit", ch.operations)
	}
	if len(ch.edited) != 1 || !strings.HasPrefix(ch.edited[0], "topic-42|msg-1|prepared:") {
		t.Fatalf("edits = %v, want resolved/prepared feedback edit", ch.edited)
	}
}

func TestSendWithRetry_FinalSendsBeforeProgressDelete(t *testing.T) {
	m := newTestManager()
	enableTestToolFeedbackCoordinator(t, m, false)
	ch := &toolFeedbackTestChannel{}
	w := &channelWorker{ch: ch, limiter: rate.NewLimiter(rate.Inf, 1)}

	feedback := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "Working...",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "chat-1",
			Raw:     map[string]string{"message_kind": "tool_feedback"},
		},
	})
	if _, sent, _, err := m.sendWithRetry(context.Background(), "test", w, feedback); err != nil || !sent {
		t.Fatalf("feedback send = (%v, %v)", sent, err)
	}
	final := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "done",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "chat-1",
			Raw:     map[string]string{"message_kind": "final_reply"},
		},
	})
	if _, sent, _, err := m.sendWithRetry(context.Background(), "test", w, final); err != nil || !sent {
		t.Fatalf("final send = (%v, %v)", sent, err)
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.operations) != 3 || ch.operations[1] != "send:done" || ch.operations[2] != "delete:msg-1" {
		t.Fatalf("operations = %v, want final send before progress delete", ch.operations)
	}
	if m.toolFeedback.ActiveCount() != 0 {
		t.Fatalf("active coordinator entries = %d, want 0", m.toolFeedback.ActiveCount())
	}
}

func TestSendWithRetry_FailedFinalKeepsProgressEditable(t *testing.T) {
	m := newTestManager()
	enableTestToolFeedbackCoordinator(t, m, false)
	ch := &toolFeedbackTestChannel{}
	w := &channelWorker{ch: ch, limiter: rate.NewLimiter(rate.Inf, 1)}

	feedback := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "Working...",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "chat-1",
			Raw:     map[string]string{"message_kind": "tool_feedback"},
		},
	})
	if _, _, _, err := m.sendWithRetry(context.Background(), "test", w, feedback); err != nil {
		t.Fatalf("feedback send error = %v", err)
	}
	ch.mu.Lock()
	ch.sendErr = ErrSendFailed
	ch.mu.Unlock()
	final := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "chat-1",
		Content: "done",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "chat-1",
			Raw:     map[string]string{"message_kind": "final_reply"},
		},
	})
	if _, sent, _, err := m.sendWithRetry(context.Background(), "test", w, final); err == nil || sent {
		t.Fatalf("failed final = (%v, %v), want failure", sent, err)
	}
	ch.mu.Lock()
	ch.sendErr = nil
	ch.mu.Unlock()
	feedback.Content = "Working...\n- tool: retry"
	ids, sent, _, err := m.sendWithRetry(context.Background(), "test", w, feedback)
	if err != nil || !sent || !slices.Equal(ids, []string{"msg-1"}) {
		t.Fatalf("resumed feedback = (%v, %v, %v), want msg-1 edit", ids, sent, err)
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.deleted) != 0 || ch.operations[len(ch.operations)-1] != "edit:msg-1" {
		t.Fatalf("operations = %v deleted = %v, want retained progress edit", ch.operations, ch.deleted)
	}
}

type mockPreparedToolFeedbackEditor struct {
	mockMessageEditor
	prepareFn func(content string) string
}

func (m *mockPreparedToolFeedbackEditor) PrepareToolFeedbackMessageContent(content string) string {
	if m.prepareFn != nil {
		return m.prepareFn(content)
	}
	return content
}

func TestPreSend_PlaceholderEditSuccess(t *testing.T) {
	m := newTestManager()
	var sendCalled bool
	var editCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				sendCalled = true
				return nil
			},
		},
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			editCalled = true
			if chatID != "123" {
				t.Fatalf("expected chatID 123, got %s", chatID)
			}
			if messageID != "456" {
				t.Fatalf("expected messageID 456, got %s", messageID)
			}
			if content != "hello" {
				t.Fatalf("expected content 'hello', got %s", content)
			}
			return nil
		},
	}

	// Register placeholder
	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"})
	_, edited := m.preSend(context.Background(), "test", msg, ch)

	if !edited {
		t.Fatal("expected preSend to return true (placeholder edited)")
	}
	if !editCalled {
		t.Fatal("expected EditMessage to be called")
	}
	if sendCalled {
		t.Fatal("expected Send to NOT be called when placeholder edited")
	}
}

func TestPreSend_ToolFeedbackBypassesPlaceholderEdit(t *testing.T) {
	m := newTestManager()

	ch := &mockMessageEditor{
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			if chatID != "123" || messageID != "456" || content != "hello" {
				t.Fatalf("unexpected edit args: %s %s %s", chatID, messageID, content)
			}
			return nil
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "hello",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})
	_, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatal("expected tool feedback to bypass placeholder editing")
	}
	if ch.recordedMessageID != "" {
		t.Fatalf("expected coordinator-owned tracking, got adapter record %q", ch.recordedMessageID)
	}
}

func TestPreSend_ToolFeedbackBypassesPlaceholderEditWithResolvedChannel(t *testing.T) {
	m := newTestManager()

	ch := &mockResolvedToolFeedbackEditor{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, chatID, messageID, content string) error {
				if chatID != "-100123" || messageID != "456" || content != "hello" {
					t.Fatalf("unexpected edit args: %s %s %s", chatID, messageID, content)
				}
				return nil
			},
		},
		resolveChatIDFn: func(chatID string, outboundCtx *bus.InboundContext) string {
			if chatID != "-100123" {
				t.Fatalf("expected raw chat ID, got %q", chatID)
			}
			if outboundCtx == nil || outboundCtx.TopicID != "42" {
				t.Fatalf("expected topic-aware outbound context, got %+v", outboundCtx)
			}
			return chatID + "/" + outboundCtx.TopicID
		},
	}

	m.RecordPlaceholder("test", "-100123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "-100123",
		Content: "hello",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "-100123",
			TopicID: "42",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})
	_, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatal("expected tool feedback to bypass placeholder editing")
	}
	if ch.recordedMessageID != "" {
		t.Fatalf("expected coordinator-owned tracking, got adapter record %q", ch.recordedMessageID)
	}
}

func TestPreSend_ToolFeedbackBypassesPlaceholderEditWithSession(t *testing.T) {
	m := newTestManager()

	ch := &mockResolvedToolFeedbackEditor{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, chatID, messageID, content string) error {
				if chatID != "-100123" || messageID != "456" || content != "hello" {
					t.Fatalf("unexpected edit args: %s %s %s", chatID, messageID, content)
				}
				return nil
			},
		},
		resolveChatIDFn: func(chatID string, outboundCtx *bus.InboundContext) string {
			if outboundCtx == nil || outboundCtx.TopicID != "42" {
				t.Fatalf("expected topic-aware outbound context, got %+v", outboundCtx)
			}
			return chatID + "/" + outboundCtx.TopicID
		},
	}

	m.RecordPlaceholder("test", "-100123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel:    "test",
		ChatID:     "-100123",
		SessionKey: "subturn-9",
		Content:    "hello",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "-100123",
			TopicID: "42",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})
	_, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatal("expected tool feedback to bypass placeholder editing")
	}
	if ch.recordedMessageID != "" {
		t.Fatalf("expected coordinator-owned tracking, got adapter record %q", ch.recordedMessageID)
	}
}

func TestPreSend_ToolFeedbackDefersContentPreparationToCoordinator(t *testing.T) {
	m := newTestManager()

	const rawContent = "🔧 `read_file`\n" + "<raw>"
	const preparedContent = "🔧 `read_file`\n&lt;raw&gt;"

	ch := &mockPreparedToolFeedbackEditor{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, chatID, messageID, content string) error {
				if chatID != "123" || messageID != "456" {
					t.Fatalf("unexpected edit target: %s/%s", chatID, messageID)
				}
				if content != InitialAnimatedToolFeedbackContent(preparedContent) {
					t.Fatalf("unexpected prepared content: %q", content)
				}
				return nil
			},
		},
		prepareFn: func(content string) string {
			if content != rawContent {
				t.Fatalf("unexpected raw tool feedback: %q", content)
			}
			return preparedContent
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: rawContent,
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})

	_, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatal("expected tool feedback to bypass placeholder editing")
	}
	if ch.recordedContent != "" {
		t.Fatalf("expected coordinator-owned tracking, got adapter content %q", ch.recordedContent)
	}
}

func TestPreSend_NonToolFeedbackLeavesTrackedMessageForChannelSend(t *testing.T) {
	m := newTestManager()
	ch := &mockMessageEditor{}

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
		},
	})

	_, edited := m.preSend(context.Background(), "test", msg, ch)
	if edited {
		t.Fatal("expected preSend to fall through when no placeholder exists")
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected tracked tool feedback cleanup to be deferred to channel send, got %q", ch.dismissedChatID)
	}
}

func TestPreSend_NonToolFeedbackDefersTrackedMessageFinalizationToChannelSend(t *testing.T) {
	m := newTestManager()
	ch := &mockMessageEditor{
		finalizeFn: func(_ context.Context, msg bus.OutboundMessage) ([]string, bool) {
			if msg.ChatID != "123" || msg.Content != "final reply" {
				t.Fatalf("unexpected finalize msg: %+v", msg)
			}
			return []string{"tool-msg-1"}, true
		},
	}

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
		},
	})

	msgIDs, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatalf("expected preSend to defer to channel Send, got msgIDs=%v", msgIDs)
	}
	if len(msgIDs) != 0 {
		t.Fatalf("expected no msgIDs from preSend, got %v", msgIDs)
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected tracked cleanup to remain in channel Send, got %q", ch.dismissedChatID)
	}
	if ch.finalizeCalled {
		t.Fatal("expected preSend to skip channel tool feedback finalization")
	}
}

func TestPreSend_ToolFeedbackDeletesPlaceholderAndSkipsEdit(t *testing.T) {
	m := newTestManager()

	ch := &mockDeletingMessageEditor{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, _, _, _ string) error {
				t.Fatal("expected placeholder edit to be skipped in separate message mode")
				return nil
			},
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "hello",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})

	msgIDs, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatalf("expected preSend to fall through so the channel can send a new message, got %v", msgIDs)
	}
	if ch.deleteCalls != 1 {
		t.Fatalf("expected placeholder deletion, got %d delete calls", ch.deleteCalls)
	}
	if ch.deletedChatID != "123" || ch.deletedMessageID != "456" {
		t.Fatalf("unexpected placeholder deletion target: %s/%s", ch.deletedChatID, ch.deletedMessageID)
	}
	if ch.recordedMessageID != "" {
		t.Fatalf("expected no tracked placeholder record, got %q", ch.recordedMessageID)
	}
	if ch.clearedChatID != "" {
		t.Fatalf("expected no adapter-owned state cleanup, got %q", ch.clearedChatID)
	}
}

func TestPreSend_ThoughtPlaceholderDeleteAndSkipsEdit(t *testing.T) {
	m := newTestManager()

	ch := &mockDeletingMessageEditor{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, _, _, _ string) error {
				t.Fatal("expected thought message to bypass placeholder edit")
				return nil
			},
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "thinking trace",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "thought",
			},
		},
	})

	msgIDs, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatalf(
			"expected thought message to fall through so the channel can send a structured message, got %v",
			msgIDs,
		)
	}
	if ch.deleteCalls != 1 {
		t.Fatalf("expected placeholder deletion, got %d delete calls", ch.deleteCalls)
	}
	if ch.deletedChatID != "123" || ch.deletedMessageID != "456" {
		t.Fatalf("unexpected placeholder deletion target: %s/%s", ch.deletedChatID, ch.deletedMessageID)
	}
	if _, ok := m.placeholders.Load("test:123"); ok {
		t.Fatal("expected placeholder to be consumed before structured thought send")
	}
}

func TestPreSend_FinalReplyDeletesPlaceholderWithoutAdapterLifecycle(t *testing.T) {
	m := newTestManager()

	ch := &mockDeletingMessageEditor{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, _, _, _ string) error {
				t.Fatal("expected final reply to bypass placeholder edit")
				return nil
			},
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final aggregated reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "final_reply",
			},
		},
	})

	msgIDs, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatalf("expected preSend to fall through so the channel can send a new final reply, got %v", msgIDs)
	}
	if ch.deleteCalls != 1 {
		t.Fatalf("expected placeholder deletion, got %d delete calls", ch.deleteCalls)
	}
	if ch.deletedChatID != "123" || ch.deletedMessageID != "456" {
		t.Fatalf("unexpected placeholder deletion target: %s/%s", ch.deletedChatID, ch.deletedMessageID)
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected no adapter-owned lifecycle call, got %q", ch.dismissedChatID)
	}
}

func TestSendWithRetry_FinalReplyBypassesToolFeedbackFinalization(t *testing.T) {
	m := newTestManager()

	ch := &mockDeletingMessageEditor{
		mockMessageEditor: mockMessageEditor{
			mockChannel: mockChannel{
				sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
					if msg.Content != "final aggregated reply" {
						t.Fatalf("sent content = %q, want final aggregated reply", msg.Content)
					}
					if got := msg.Context.Raw["message_kind"]; got != "final_reply" {
						t.Fatalf("message_kind = %q, want final_reply", got)
					}
					return nil
				},
			},
			editFn: func(_ context.Context, _, _, _ string) error {
				t.Fatal("expected final reply to bypass placeholder/tool-feedback edit")
				return nil
			},
			finalizeFn: func(_ context.Context, _ bus.OutboundMessage) ([]string, bool) {
				t.Fatal("expected final reply to bypass channel tool-feedback finalization")
				return nil, false
			},
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final aggregated reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "final_reply",
			},
		},
	})

	messageIDs, sent, ambiguous, sendErr := m.sendWithRetry(context.Background(), "test", w, msg)
	if !sent {
		t.Fatal("expected final reply to be sent as a new message")
	}
	if sendErr != nil || ambiguous {
		t.Fatalf(
			"sendWithRetry() = (%v, %v, %v, %v), want confirmed delivery",
			messageIDs,
			sent,
			ambiguous,
			sendErr,
		)
	}
	if ch.finalizeCalled {
		t.Fatal("expected final reply not to call FinalizeToolFeedbackMessage")
	}
	if ch.deleteCalls != 1 {
		t.Fatalf("expected placeholder deletion, got %d", ch.deleteCalls)
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected no adapter-owned lifecycle call, got %q", ch.dismissedChatID)
	}
	if len(ch.sentMessages) != 1 {
		t.Fatalf("expected one sent final reply, got %d", len(ch.sentMessages))
	}
}

func TestSendWithRetry_ToolCallsPlaceholderDeleteAndFallsThroughToSend(t *testing.T) {
	m := newTestManager()

	ch := &mockDeletingMessageEditor{
		mockMessageEditor: mockMessageEditor{
			mockChannel: mockChannel{
				sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
					if got := msg.Context.Raw["message_kind"]; got != "tool_calls" {
						t.Fatalf("expected tool_calls message kind, got %q", got)
					}
					if msg.Content != "" {
						t.Fatalf("expected empty tool_calls content, got %q", msg.Content)
					}
					return nil
				},
			},
			editFn: func(_ context.Context, _, _, _ string) error {
				t.Fatal("expected tool_calls message to bypass placeholder edit")
				return nil
			},
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_calls",
				"tool_calls":   `[{"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{}"},"extra_content":{"tool_feedback_explanation":"Looking up config"}}]`,
			},
		},
	})

	m.sendWithRetry(context.Background(), "test", w, msg)

	if ch.deleteCalls != 1 {
		t.Fatalf("expected placeholder deletion, got %d delete calls", ch.deleteCalls)
	}
	if ch.deletedChatID != "123" || ch.deletedMessageID != "456" {
		t.Fatalf("unexpected placeholder deletion target: %s/%s", ch.deletedChatID, ch.deletedMessageID)
	}
	if len(ch.sentMessages) != 1 {
		t.Fatalf("expected structured tool_calls message to be sent once, got %d", len(ch.sentMessages))
	}
}

func TestPreSend_NonToolFeedbackDoesNotInvokeAdapterLifecycle(t *testing.T) {
	m := newTestManager()

	ch := &mockMessageEditor{}

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
		},
	})

	_, handled := m.preSend(context.Background(), "test", msg, ch)
	if handled {
		t.Fatal("expected preSend to leave final delivery to the channel")
	}
	if ch.clearedChatID != "" {
		t.Fatalf("expected no adapter-owned state cleanup, got %q", ch.clearedChatID)
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected tracked tool feedback message to be preserved, got dismissal for %q", ch.dismissedChatID)
	}
	if ch.finalizeCalled {
		t.Fatal("expected separate message mode to skip in-place finalization")
	}
}

func TestPreSend_StaleToolFeedbackDoesNotConsumeStreamActiveMarker(t *testing.T) {
	m := newTestManager()
	m.streamActive.Store("test:123", true)
	m.RecordPlaceholder("test", "123", "placeholder-1")

	var editedContent string
	ch := &mockMessageEditor{
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			if chatID != "123" || messageID != "placeholder-1" {
				t.Fatalf("unexpected edit target: %s/%s", chatID, messageID)
			}
			editedContent = content
			return nil
		},
	}

	toolFeedback := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "🔧 `read_file`\nReading config",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})

	msgIDs, handled := m.preSend(context.Background(), "test", toolFeedback, ch)
	if !handled {
		t.Fatal("expected stale tool feedback to be dropped after stream finalize")
	}
	if len(msgIDs) != 0 {
		t.Fatalf("expected no delivered message IDs for stale feedback, got %v", msgIDs)
	}
	if _, ok := m.streamActive.Load("test:123"); !ok {
		t.Fatal("expected streamActive marker to remain for the final outbound message")
	}
	if _, ok := m.placeholders.Load("test:123"); !ok {
		t.Fatal("expected placeholder cleanup to remain deferred to the final outbound message")
	}
	if ch.editedMessages != 0 {
		t.Fatalf("expected no placeholder edit for stale feedback, got %d edits", ch.editedMessages)
	}

	finalMsg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final streamed reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"outbound_kind": "final",
			},
		},
	})

	_, handled = m.preSend(context.Background(), "test", finalMsg, ch)
	if !handled {
		t.Fatal("expected final outbound message to consume streamActive marker")
	}
	if _, ok := m.streamActive.Load("test:123"); ok {
		t.Fatal("expected streamActive marker to be cleared by final outbound message")
	}
	if _, ok := m.placeholders.Load("test:123"); ok {
		t.Fatal("expected placeholder to be cleaned up by final outbound message")
	}
	if editedContent != "final streamed reply" {
		t.Fatalf("editedContent = %q, want final streamed reply", editedContent)
	}
}

func TestPreSend_StaleThoughtDoesNotConsumeStreamActiveMarker(t *testing.T) {
	m := newTestManager()
	m.streamActive.Store("test:123", true)
	m.streamAuxiliaryTombstones.Store("test:123", time.Now())
	m.RecordPlaceholder("test", "123", "placeholder-1")

	var editedContent string
	ch := &mockMessageEditor{
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			if chatID != "123" || messageID != "placeholder-1" {
				t.Fatalf("unexpected edit target: %s/%s", chatID, messageID)
			}
			editedContent = content
			return nil
		},
	}

	thought := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "late reasoning",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "thought",
			},
		},
	})

	msgIDs, handled := m.preSend(context.Background(), "test", thought, ch)
	if !handled {
		t.Fatal("expected stale thought to be dropped after stream finalize")
	}
	if len(msgIDs) != 0 {
		t.Fatalf("expected no delivered message IDs for stale thought, got %v", msgIDs)
	}
	if _, ok := m.streamActive.Load("test:123"); !ok {
		t.Fatal("expected streamActive marker to remain for the final outbound message")
	}
	if _, ok := m.placeholders.Load("test:123"); !ok {
		t.Fatal("expected placeholder cleanup to remain deferred to the final outbound message")
	}
	if ch.editedMessages != 0 {
		t.Fatalf("expected no placeholder edit for stale thought, got %d edits", ch.editedMessages)
	}

	finalMsg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final streamed reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"outbound_kind": "final",
			},
		},
	})

	_, handled = m.preSend(context.Background(), "test", finalMsg, ch)
	if !handled {
		t.Fatal("expected final outbound message to consume streamActive marker")
	}
	if _, ok := m.streamActive.Load("test:123"); ok {
		t.Fatal("expected streamActive marker to be cleared by final outbound message")
	}
	if _, ok := m.placeholders.Load("test:123"); ok {
		t.Fatal("expected placeholder to be cleaned up by final outbound message")
	}
	if editedContent != "final streamed reply" {
		t.Fatalf("editedContent = %q, want final streamed reply", editedContent)
	}

	lateThought := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "later reasoning",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "thought",
			},
		},
	})
	msgIDs, handled = m.preSend(context.Background(), "test", lateThought, ch)
	if !handled {
		t.Fatal("expected tombstone to drop late thought after final outbound was suppressed")
	}
	if len(msgIDs) != 0 {
		t.Fatalf("expected no delivered message IDs for late thought, got %v", msgIDs)
	}
}

func TestPreSend_StreamActiveDoesNotConsumeEarlierVisibleMessage(t *testing.T) {
	m := newTestManager()
	m.streamActive.Store("test:123", true)
	m.streamAuxiliaryTombstones.Store("test:123", time.Now())
	m.RecordPlaceholder("test", "123", "placeholder-1")

	editCalls := 0
	ch := &mockMessageEditor{
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			editCalls++
			if chatID != "123" || messageID != "placeholder-1" || content != "final streamed reply" {
				t.Fatalf("unexpected placeholder edit for %s/%s: %q", chatID, messageID, content)
			}
			return nil
		},
	}

	earlierVisible := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "earlier visible message",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
		},
	})
	_, handled := m.preSend(context.Background(), "test", earlierVisible, ch)
	if handled {
		t.Fatal("expected earlier visible message to be delivered normally")
	}
	if editCalls != 0 {
		t.Fatalf("placeholder edits after earlier visible message = %d, want 0", editCalls)
	}
	if _, ok := m.streamActive.Load("test:123"); !ok {
		t.Fatal("expected streamActive marker to remain for final outbound")
	}
	if _, ok := m.streamAuxiliaryTombstones.Load("test:123"); !ok {
		t.Fatal("expected auxiliary tombstone to remain")
	}
	if _, ok := m.placeholders.Load("test:123"); !ok {
		t.Fatal("expected placeholder cleanup to remain deferred to final outbound")
	}

	finalMsg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "final streamed reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"outbound_kind": "final",
			},
		},
	})
	_, handled = m.preSend(context.Background(), "test", finalMsg, ch)
	if !handled {
		t.Fatal("expected final outbound message to consume streamActive marker")
	}
	if _, ok := m.streamActive.Load("test:123"); ok {
		t.Fatal("expected streamActive marker to be cleared by final outbound message")
	}
	if editCalls != 1 {
		t.Fatalf("placeholder edits after final outbound = %d, want 1", editCalls)
	}
}

func TestPreSend_StreamActiveDoesNotConsumeOtherSessionFinal(t *testing.T) {
	m := newTestManager()
	m.streamActive.Store("test:123", true)
	m.RecordPlaceholder("test", "123", "placeholder-1")

	ch := &mockMessageEditor{
		editFn: func(_ context.Context, _, _, _ string) error {
			t.Fatal("placeholder edit should remain deferred for the streaming session")
			return nil
		},
	}

	otherSessionFinal := testOutboundMessage(bus.OutboundMessage{
		Channel:    "test",
		ChatID:     "123",
		SessionKey: "session-other",
		Content:    "other session final",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"outbound_kind": "final",
			},
		},
	})

	_, handled := m.preSend(context.Background(), "test", otherSessionFinal, ch)
	if handled {
		t.Fatal("expected final outbound from a different session to be delivered normally")
	}
	if _, ok := m.streamActive.Load("test:123"); !ok {
		t.Fatal("expected streaming marker to remain for the streaming session")
	}
	if _, ok := m.placeholders.Load("test:123"); !ok {
		t.Fatal("expected placeholder cleanup to remain deferred to the streaming session")
	}
}

func TestPreSendMedia_DoesNotInvokeAdapterLifecycle(t *testing.T) {
	m := newTestManager()
	ch := &mockDeletingMediaChannel{}

	m.preSendMedia(context.Background(), "test", bus.OutboundMediaMessage{
		ChatID: "123",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
		},
	}, ch)

	if ch.dismissedChatID != "" {
		t.Fatalf("expected no adapter-owned lifecycle call, got %q", ch.dismissedChatID)
	}
}

func TestPreSendMedia_WithConfigDoesNotInvokeAdapterLifecycle(t *testing.T) {
	m := newTestManager()
	m.config = &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				ToolFeedback: config.ToolFeedbackConfig{
					Enabled:          true,
					SeparateMessages: true,
				},
			},
		},
	}

	ch := &mockMessageEditor{}

	m.preSendMedia(context.Background(), "test", bus.OutboundMediaMessage{
		ChatID: "123",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
		},
	}, ch)

	if ch.dismissedChatID != "" {
		t.Fatalf("expected no adapter-owned lifecycle call, got %q", ch.dismissedChatID)
	}
	if ch.clearedChatID != "" {
		t.Fatalf("expected dismissal to handle tracked state cleanup, got clear for %q", ch.clearedChatID)
	}
}

func TestPreSendMedia_DoesNotUseLegacySessionCleanup(t *testing.T) {
	m := newTestManager()
	ch := &mockResolvedToolFeedbackEditor{
		resolveChatIDFn: func(chatID string, outboundCtx *bus.InboundContext) string {
			if outboundCtx == nil || outboundCtx.TopicID != "42" {
				return chatID
			}
			return chatID + "/" + outboundCtx.TopicID
		},
	}

	m.preSendMedia(context.Background(), "test", bus.OutboundMediaMessage{
		ChatID:     "123",
		SessionKey: "subturn-1",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			TopicID: "42",
		},
	}, ch)

	if len(ch.dismissedChatIDs) != 0 {
		t.Fatalf("unexpected adapter lifecycle calls: %v", ch.dismissedChatIDs)
	}
}

func TestSplitOutboundMessageContent_ToolFeedbackTruncatesInsteadOfSplitting(t *testing.T) {
	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "\U0001f527 `read_file`\nRead README.md first to confirm the current project structure before editing the config example.",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})

	chunks := splitOutboundMessageContent(msg, 40)
	if len(chunks) != 1 {
		t.Fatalf("len(chunks) = %d, want 1", len(chunks))
	}
	want := utils.FitToolFeedbackMessage(msg.Content, 40-MaxToolFeedbackAnimationFrameLength())
	if chunks[0] != want {
		t.Fatalf("chunk = %q, want %q", chunks[0], want)
	}
}

func TestSplitOutboundMessageContent_ToolFeedbackReservesAnimationFrame(t *testing.T) {
	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "🔧 `read_file`\n1234567890",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})

	chunks := splitOutboundMessageContent(msg, len([]rune(msg.Content)))
	if len(chunks) != 1 {
		t.Fatalf("len(chunks) = %d, want 1", len(chunks))
	}

	animated := formatAnimatedToolFeedbackContent(chunks[0], strings.Repeat(".", MaxToolFeedbackAnimationFrameLength()))
	if got, maxLen := len([]rune(animated)), len([]rune(msg.Content)); got > maxLen {
		t.Fatalf("animated len = %d, want <= %d; content=%q", got, maxLen, animated)
	}
}

func TestGetStreamer_FinalizeDismissesTrackedToolFeedback(t *testing.T) {
	m := newTestManager()
	ch := &mockStreamingChannel{
		mockMessageEditor: mockMessageEditor{},
		streamer: &mockStreamer{
			finalizeFn: func(_ context.Context, content string) error {
				if content != "final reply" {
					t.Fatalf("unexpected finalize content: %q", content)
				}
				return nil
			},
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	if err := streamer.Finalize(context.Background(), "final reply"); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected coordinator-owned cleanup, got adapter call %q", ch.dismissedChatID)
	}
	if _, ok := m.streamActive.Load("test:123"); !ok {
		t.Fatal("expected streamActive marker to be recorded after finalize")
	}
}

func TestGetStreamer_FinalizeCleansPlaceholderImmediately(t *testing.T) {
	m := newTestManager()
	m.RecordPlaceholder("test", "123", "placeholder-1")
	var editedContent string
	editCalls := 0
	ch := &mockStreamingChannel{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, chatID, messageID, content string) error {
				if chatID != "123" || messageID != "placeholder-1" {
					t.Fatalf("unexpected edit target: %s/%s", chatID, messageID)
				}
				editCalls++
				editedContent = content
				return nil
			},
		},
		streamer: &mockStreamer{},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	if err := streamer.Finalize(context.Background(), "final reply"); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if editedContent != "final reply" {
		t.Fatalf("edited placeholder content = %q, want final reply", editedContent)
	}
	if _, placeholderExists := m.placeholders.Load("test:123"); placeholderExists {
		t.Fatal("expected placeholder to be cleaned up during finalize")
	}
	if _, streamActiveExists := m.streamActive.Load("test:123"); !streamActiveExists {
		t.Fatal("expected streamActive marker to be recorded after finalize")
	}
	cleaner, ok := streamer.(interface{ ClearFinalizedStreamMarker() })
	if !ok {
		t.Fatal("expected streamer to expose marker cleanup")
	}
	cleaner.ClearFinalizedStreamMarker()
	if _, streamActiveExists := m.streamActive.Load("test:123"); streamActiveExists {
		t.Fatal("expected streamActive marker to be cleared")
	}
	if _, ok := m.streamAuxiliaryTombstones.Load("test:123"); !ok {
		t.Fatal("expected auxiliary tombstone to remain after final marker cleanup")
	}

	lateThought := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "late reasoning",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "thought",
			},
		},
	})
	msgIDs, handled := m.preSend(context.Background(), "test", lateThought, ch)
	if !handled {
		t.Fatal("expected auxiliary tombstone to drop late thought")
	}
	if len(msgIDs) != 0 {
		t.Fatalf("expected no delivered message IDs for late thought, got %v", msgIDs)
	}
	if editCalls != 1 {
		t.Fatalf("expected late thought not to edit placeholder, got %d edits", editCalls)
	}

	finalOutbound := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "visible final reply",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
		},
	})
	_, handled = m.preSend(context.Background(), "test", finalOutbound, ch)
	if handled {
		t.Fatal("expected cleared final marker to let normal outbound send")
	}
	if _, ok := m.streamAuxiliaryTombstones.Load("test:123"); ok {
		t.Fatal("expected normal outbound to clear auxiliary tombstone")
	}
}

func TestGetStreamer_FinalizeCleansPlaceholderWithSessionKey(t *testing.T) {
	m := newTestManager()
	m.RecordPlaceholder("test", "123", "placeholder-1")
	ch := &mockStreamingChannel{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, chatID, messageID, content string) error {
				if chatID != "123" || messageID != "placeholder-1" || content != "final reply" {
					t.Fatalf("unexpected edit for %s/%s: %q", chatID, messageID, content)
				}
				return nil
			},
		},
		streamer: &mockStreamer{},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "session-1")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	if err := streamer.Finalize(context.Background(), "final reply"); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if _, placeholderExists := m.placeholders.Load("test:123"); placeholderExists {
		t.Fatal("expected placeholder to be cleaned up during finalize")
	}
	if _, streamActiveExists := m.streamActive.Load("test:123:session-1"); !streamActiveExists {
		t.Fatal("expected session streamActive marker to be recorded after finalize")
	}
}

func TestGetStreamer_PreservesContextUsageStreamer(t *testing.T) {
	m := newTestManager()
	var gotUsage *bus.ContextUsage
	ch := &mockStreamingChannel{
		streamer: &mockStreamer{
			finalizeWithContextFn: func(_ context.Context, content string, usage *bus.ContextUsage) error {
				if content != "final reply" {
					t.Fatalf("unexpected finalize content: %q", content)
				}
				gotUsage = usage
				return nil
			},
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	contextStreamer, ok := streamer.(bus.ContextUsageStreamer)
	if !ok {
		t.Fatal("manager-wrapped streamer should preserve ContextUsageStreamer")
	}
	usage := &bus.ContextUsage{UsedTokens: 10, TotalTokens: 100, CompressAtTokens: 80, UsedPercent: 10}
	if err := contextStreamer.FinalizeWithContext(context.Background(), "final reply", usage); err != nil {
		t.Fatalf("FinalizeWithContext() error = %v", err)
	}
	if gotUsage != usage {
		t.Fatalf("context usage = %#v, want original usage", gotUsage)
	}
	if _, ok := m.streamActive.Load("test:123"); !ok {
		t.Fatal("expected streamActive marker to be recorded after finalize with context")
	}
}

func TestGetStreamer_PreservesReasoningStreamer(t *testing.T) {
	m := newTestManager()
	inner := &mockReasoningStreamer{}
	ch := &mockStreamingChannel{
		streamer: inner,
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	reasoningStreamer, ok := streamer.(bus.ReasoningStreamer)
	if !ok {
		t.Fatal("manager-wrapped streamer should preserve ReasoningStreamer")
	}
	if err := reasoningStreamer.UpdateReasoning(context.Background(), "thinking"); err != nil {
		t.Fatalf("UpdateReasoning() error = %v", err)
	}
	if err := reasoningStreamer.FinalizeReasoning(context.Background(), "final thought"); err != nil {
		t.Fatalf("FinalizeReasoning() error = %v", err)
	}
	if got := inner.reasoningUpdates; len(got) != 1 || got[0] != "thinking" {
		t.Fatalf("reasoning updates = %v, want [thinking]", got)
	}
	if inner.reasoningFinal != "final thought" {
		t.Fatalf("reasoning final = %q, want final thought", inner.reasoningFinal)
	}
}

func TestGetStreamer_PreservesModelNameSetter(t *testing.T) {
	m := newTestManager()
	inner := &modelTrackingReasoningStreamer{}
	ch := &mockStreamingChannel{
		streamer: inner,
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	setter, ok := streamer.(interface{ SetModelName(modelName string) })
	if !ok {
		t.Fatal("manager-wrapped streamer should preserve SetModelName")
	}
	setter.SetModelName("gpt-5.4")
	if err := streamer.Update(context.Background(), "hello"); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	reasoningStreamer, ok := streamer.(bus.ReasoningStreamer)
	if !ok {
		t.Fatal("manager-wrapped streamer should preserve ReasoningStreamer")
	}
	setter.SetModelName("gpt-5.4")
	if err := reasoningStreamer.UpdateReasoning(context.Background(), "thinking"); err != nil {
		t.Fatalf("UpdateReasoning() error = %v", err)
	}
	if len(inner.modelNames) != 2 {
		t.Fatalf("model name calls = %v, want 2 forwarded calls", inner.modelNames)
	}
	if inner.modelNames[0] != "gpt-5.4" || inner.modelNames[1] != "gpt-5.4" {
		t.Fatalf("model name calls = %v, want both forwarded as gpt-5.4", inner.modelNames)
	}
}

func TestGetStreamer_SplitOnMarkerStreamsSeparateSegments(t *testing.T) {
	m := newTestManager()
	m.config = &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				SplitOnMarker: true,
			},
		},
	}

	var segments []*recordingStreamSegment
	ch := &mockStreamingChannel{
		beginStreamFn: func(context.Context, string) (Streamer, error) {
			segment := &recordingStreamSegment{}
			segments = append(segments, segment)
			return segment, nil
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "session-1")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	contextStreamer, ok := streamer.(bus.ContextUsageStreamer)
	if !ok {
		t.Fatal("split streamer should preserve ContextUsageStreamer")
	}

	if err := streamer.Update(context.Background(), "hello"); err != nil {
		t.Fatalf("Update(first) error = %v", err)
	}
	if err := streamer.Update(context.Background(), "hello<|[SPLIT]|>world"); err != nil {
		t.Fatalf("Update(split) error = %v", err)
	}
	if err := streamer.Update(context.Background(), "hello<|[SPLIT]|>world!"); err != nil {
		t.Fatalf("Update(second segment) error = %v", err)
	}
	usage := &bus.ContextUsage{UsedTokens: 10, TotalTokens: 100}
	if err := contextStreamer.FinalizeWithContext(
		context.Background(),
		"hello<|[SPLIT]|>world!",
		usage,
	); err != nil {
		t.Fatalf("FinalizeWithContext() error = %v", err)
	}

	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
	if got := segments[0].updates; len(got) != 1 || got[0] != "hello" {
		t.Fatalf("segment 0 updates = %v, want [hello]", got)
	}
	if got := segments[0].finals; len(got) != 1 || got[0] != "hello" {
		t.Fatalf("segment 0 finals = %v, want [hello]", got)
	}
	if got := segments[1].updates; len(got) != 2 || got[0] != "world" || got[1] != "world!" {
		t.Fatalf("segment 1 updates = %v, want [world world!]", got)
	}
	if got := segments[1].finals; len(got) != 1 || got[0] != "world!" {
		t.Fatalf("segment 1 finals = %v, want [world!]", got)
	}
	if segments[1].finalUsage != usage {
		t.Fatalf("final usage = %#v, want original usage", segments[1].finalUsage)
	}
	if _, ok := m.streamActive.Load("test:123:session-1"); !ok {
		t.Fatal("expected streamActive marker to be recorded after split stream finalize")
	}
}

func TestGetStreamer_SplitOnMarkerKeepsReasoningOnInitialStreamer(t *testing.T) {
	m := newTestManager()
	m.config = &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				SplitOnMarker: true,
			},
		},
	}

	initial := &mockReasoningStreamer{}
	next := &recordingStreamSegment{}
	callCount := 0
	ch := &mockStreamingChannel{
		beginStreamFn: func(context.Context, string) (Streamer, error) {
			callCount++
			if callCount == 1 {
				return initial, nil
			}
			return next, nil
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	if err := streamer.Update(context.Background(), "hello<|[SPLIT]|>world"); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	reasoningStreamer, ok := streamer.(bus.ReasoningStreamer)
	if !ok {
		t.Fatal("split streamer should preserve ReasoningStreamer")
	}
	if err := reasoningStreamer.UpdateReasoning(context.Background(), "thinking"); err != nil {
		t.Fatalf("UpdateReasoning() error = %v", err)
	}
	if err := reasoningStreamer.FinalizeReasoning(context.Background(), "final thought"); err != nil {
		t.Fatalf("FinalizeReasoning() error = %v", err)
	}

	if got := initial.reasoningUpdates; len(got) != 1 || got[0] != "thinking" {
		t.Fatalf("initial reasoning updates = %v, want [thinking]", got)
	}
	if initial.reasoningFinal != "final thought" {
		t.Fatalf("initial reasoning final = %q, want final thought", initial.reasoningFinal)
	}
}

func TestGetStreamer_SplitOnMarkerPreservesModelNameSetter(t *testing.T) {
	m := newTestManager()
	m.config = &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				SplitOnMarker: true,
			},
		},
	}

	initial := &modelTrackingReasoningStreamer{}
	next := &recordingStreamSegment{}
	callCount := 0
	ch := &mockStreamingChannel{
		beginStreamFn: func(context.Context, string) (Streamer, error) {
			callCount++
			if callCount == 1 {
				return initial, nil
			}
			return next, nil
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	setter, ok := streamer.(interface{ SetModelName(modelName string) })
	if !ok {
		t.Fatal("split streamer should preserve SetModelName")
	}
	setter.SetModelName("gpt-5.4-mini")
	if err := streamer.Update(context.Background(), "hello<|[SPLIT]|>world"); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	reasoningStreamer, ok := streamer.(bus.ReasoningStreamer)
	if !ok {
		t.Fatal("split streamer should preserve ReasoningStreamer")
	}
	if err := reasoningStreamer.UpdateReasoning(context.Background(), "thinking"); err != nil {
		t.Fatalf("UpdateReasoning() error = %v", err)
	}

	if len(initial.modelNames) == 0 || initial.modelNames[0] != "gpt-5.4-mini" {
		t.Fatalf("initial model names = %v, want forwarded gpt-5.4-mini", initial.modelNames)
	}
	if len(next.modelNames) == 0 || next.modelNames[0] != "gpt-5.4-mini" {
		t.Fatalf("next model names = %v, want forwarded gpt-5.4-mini", next.modelNames)
	}
}

func TestGetStreamer_FinalizeWithConfigDoesNotInvokeAdapterLifecycle(t *testing.T) {
	m := newTestManager()
	m.config = &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				ToolFeedback: config.ToolFeedbackConfig{
					Enabled:          true,
					SeparateMessages: true,
				},
			},
		},
	}
	ch := &mockStreamingChannel{
		mockMessageEditor: mockMessageEditor{},
		streamer: &mockStreamer{
			finalizeFn: func(_ context.Context, content string) error {
				if content != "final reply" {
					t.Fatalf("unexpected finalize content: %q", content)
				}
				return nil
			},
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	if err := streamer.Finalize(context.Background(), "final reply"); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if ch.clearedChatID != "" {
		t.Fatalf("expected coordinator-owned cleanup, got adapter clear %q", ch.clearedChatID)
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected tracked tool feedback message to be preserved, got dismissal for %q", ch.dismissedChatID)
	}
	if _, ok := m.streamActive.Load("test:123"); !ok {
		t.Fatal("expected streamActive marker to be recorded after finalize")
	}
}

func TestGetStreamer_FinalizeDismissesResolvedTrackedToolFeedback(t *testing.T) {
	m := newTestManager()
	ch := &mockStreamingChannel{
		mockMessageEditor: mockMessageEditor{},
		streamer: &mockStreamer{
			finalizeFn: func(_ context.Context, content string) error {
				if content != "final reply" {
					t.Fatalf("unexpected finalize content: %q", content)
				}
				return nil
			},
		},
		resolveChatIDFn: func(chatID string, outboundCtx *bus.InboundContext) string {
			if outboundCtx == nil {
				t.Fatal("expected outbound context during stream finalize")
			}
			if outboundCtx.ChatID != "-100123/42" {
				t.Fatalf("unexpected outbound context: %+v", outboundCtx)
			}
			return outboundCtx.ChatID
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "-100123/42", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	if err := streamer.Finalize(context.Background(), "final reply"); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected coordinator-owned cleanup, got adapter call %q", ch.dismissedChatID)
	}
	if _, ok := m.streamActive.Load("test:-100123/42"); !ok {
		t.Fatal("expected streamActive marker to be recorded after finalize")
	}
}

func TestPreSend_PlaceholderEditSuccessDismissesResolvedTrackedToolFeedback(t *testing.T) {
	m := newTestManager()

	ch := &mockResolvedToolFeedbackEditor{
		mockMessageEditor: mockMessageEditor{
			editFn: func(_ context.Context, chatID, messageID, content string) error {
				if chatID != "-100123" || messageID != "456" || content != "done" {
					t.Fatalf("unexpected edit args: %s %s %s", chatID, messageID, content)
				}
				return nil
			},
		},
		resolveChatIDFn: func(chatID string, outboundCtx *bus.InboundContext) string {
			if outboundCtx == nil || outboundCtx.TopicID != "42" {
				t.Fatalf("expected topic-aware outbound context, got %+v", outboundCtx)
			}
			return chatID + "/" + outboundCtx.TopicID
		},
	}

	m.RecordPlaceholder("test", "-100123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "-100123",
		Content: "done",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "-100123",
			TopicID: "42",
		},
	})

	_, edited := m.preSend(context.Background(), "test", msg, ch)
	if !edited {
		t.Fatal("expected preSend to edit placeholder")
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected no adapter-owned lifecycle call, got %q", ch.dismissedChatID)
	}
}

func TestGetStreamer_FinalizeFailureDoesNotDismissTrackedToolFeedback(t *testing.T) {
	m := newTestManager()
	ch := &mockStreamingChannel{
		mockMessageEditor: mockMessageEditor{},
		streamer: &mockStreamer{
			finalizeFn: func(context.Context, string) error {
				return errors.New("finalize failed")
			},
		},
	}
	m.channels["test"] = ch

	streamer, ok := m.GetStreamer(context.Background(), "test", "123", "")
	if !ok {
		t.Fatal("expected streamer to be available")
	}
	if err := streamer.Finalize(context.Background(), "final reply"); err == nil {
		t.Fatal("expected Finalize() to fail")
	}
	if ch.dismissedChatID != "" {
		t.Fatalf("expected no tool feedback dismissal on finalize failure, got %q", ch.dismissedChatID)
	}
	if _, ok := m.streamActive.Load("test:123"); ok {
		t.Fatal("expected no streamActive marker after finalize failure")
	}
}

func TestRunWorker_ToolFeedbackSkipsMarkerSplitting(t *testing.T) {
	m := newTestManager()
	m.config = &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				SplitOnMarker: true,
			},
		},
	}

	var (
		mu       sync.Mutex
		received []string
	)
	ch := &mockChannelWithLength{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
				mu.Lock()
				received = append(received, msg.Content)
				mu.Unlock()
				return nil
			},
		},
		maxLen: 200,
	}

	w := &channelWorker{
		ch:      ch,
		queue:   make(chan bus.OutboundMessage, 1),
		done:    make(chan struct{}),
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.runWorker(ctx, "test", w)

	content := "🔧 `read_file`\nRead current config first.<|[SPLIT]|>Then update the example."
	w.queue <- testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: content,
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"message_kind": "tool_feedback",
			},
		},
	})

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("len(received) = %d, want 1", len(received))
	}
	if received[0] != content {
		t.Fatalf("received[0] = %q, want %q", received[0], content)
	}
}

func TestRunWorker_FinalizedStreamSuppressesMarkerSplitBeforeSending(t *testing.T) {
	m := newTestManager()
	m.config = &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				SplitOnMarker: true,
			},
		},
	}

	var (
		mu       sync.Mutex
		received []string
	)
	ch := &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			mu.Lock()
			received = append(received, msg.Content)
			mu.Unlock()
			return nil
		},
	}

	w := &channelWorker{
		ch:      ch,
		queue:   make(chan bus.OutboundMessage, 1),
		done:    make(chan struct{}),
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.runWorker(ctx, "test", w)

	streamKey := streamSuppressionKey("test", "123", "session-1")
	m.streamActive.Store(streamKey, true)
	w.queue <- testOutboundMessage(bus.OutboundMessage{
		Channel:    "test",
		ChatID:     "123",
		SessionKey: "session-1",
		Content:    "streamed full reply<|[SPLIT]|>duplicate chunk",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			Raw: map[string]string{
				"outbound_kind": "final",
			},
		},
	})

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 0 {
		t.Fatalf("received split duplicate messages = %v, want none", received)
	}
	if _, ok := m.streamActive.Load(streamKey); ok {
		t.Fatal("expected finalized stream marker to be consumed")
	}
}

func TestPreSend_PlaceholderEditFails_FallsThrough(t *testing.T) {
	m := newTestManager()

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				return nil
			},
		},
		editFn: func(_ context.Context, _, _, _ string) error {
			return fmt.Errorf("edit failed")
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"})
	_, edited := m.preSend(context.Background(), "test", msg, ch)

	if edited {
		t.Fatal("expected preSend to return false when edit fails")
	}
}

func TestInvokeTypingStop_CallsRegisteredStop(t *testing.T) {
	m := newTestManager()
	var stopCalled bool

	m.RecordTypingStop("telegram", "chat123", func() {
		stopCalled = true
	})

	m.InvokeTypingStop("telegram", "chat123")

	if !stopCalled {
		t.Fatal("expected typing stop func to be called")
	}
}

func TestInvokeTypingStop_NoOpWhenNoEntry(t *testing.T) {
	m := newTestManager()
	// Should not panic
	m.InvokeTypingStop("telegram", "nonexistent")
}

func TestInvokeTypingStop_Idempotent(t *testing.T) {
	m := newTestManager()
	var callCount int

	m.RecordTypingStop("telegram", "chat123", func() {
		callCount++
	})

	m.InvokeTypingStop("telegram", "chat123")
	m.InvokeTypingStop("telegram", "chat123") // Second call: entry already removed, no-op

	if callCount != 1 {
		t.Fatalf("expected stop to be called once, got %d", callCount)
	}
}

func TestPreSend_TypingStopCalled(t *testing.T) {
	m := newTestManager()
	var stopCalled bool

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return nil
		},
	}

	m.RecordTypingStop("test", "123", func() {
		stopCalled = true
	})

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"})
	m.preSend(context.Background(), "test", msg, ch)

	if !stopCalled {
		t.Fatal("expected typing stop func to be called")
	}
}

func TestPreSend_TypingStopUsesResolvedChatID(t *testing.T) {
	m := newTestManager()
	var stopCalled bool

	ch := &mockResolvedToolFeedbackEditor{
		resolveChatIDFn: func(chatID string, outboundCtx *bus.InboundContext) string {
			if outboundCtx == nil || outboundCtx.TopicID != "42" {
				return chatID
			}
			return chatID + "/" + outboundCtx.TopicID
		},
	}

	m.RecordTypingStop("test", "123/42", func() {
		stopCalled = true
	})

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "hello",
		Context: bus.InboundContext{
			Channel: "test",
			ChatID:  "123",
			TopicID: "42",
		},
	})
	m.preSend(context.Background(), "test", msg, ch)

	if !stopCalled {
		t.Fatal("expected typing stop func to be called for resolved topic chat ID")
	}
}

func TestPreSend_NoRegisteredState(t *testing.T) {
	m := newTestManager()

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return nil
		},
	}

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"})
	_, edited := m.preSend(context.Background(), "test", msg, ch)

	if edited {
		t.Fatal("expected preSend to return false with no registered state")
	}
}

func TestPreSend_TypingAndPlaceholder(t *testing.T) {
	m := newTestManager()
	var stopCalled bool
	var editCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				return nil
			},
		},
		editFn: func(_ context.Context, _, _, _ string) error {
			editCalled = true
			return nil
		},
	}

	m.RecordTypingStop("test", "123", func() {
		stopCalled = true
	})
	m.RecordPlaceholder("test", "123", "456")

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"})
	_, edited := m.preSend(context.Background(), "test", msg, ch)

	if !stopCalled {
		t.Fatal("expected typing stop to be called")
	}
	if !editCalled {
		t.Fatal("expected EditMessage to be called")
	}
	if !edited {
		t.Fatal("expected preSend to return true")
	}
}

func TestRecordPlaceholder_ConcurrentSafe(t *testing.T) {
	m := newTestManager()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			chatID := fmt.Sprintf("chat_%d", i%10)
			m.RecordPlaceholder("test", chatID, fmt.Sprintf("msg_%d", i))
		}(i)
	}
	wg.Wait()
}

func TestRecordTypingStop_ConcurrentSafe(t *testing.T) {
	m := newTestManager()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			chatID := fmt.Sprintf("chat_%d", i%10)
			m.RecordTypingStop("test", chatID, func() {})
		}(i)
	}
	wg.Wait()
}

func TestRecordTypingStop_ReplacesExistingStop(t *testing.T) {
	m := newTestManager()
	var oldStopCalls int
	var newStopCalls int

	m.RecordTypingStop("test", "123", func() {
		oldStopCalls++
	})

	m.RecordTypingStop("test", "123", func() {
		newStopCalls++
	})

	if oldStopCalls != 1 {
		t.Fatalf("expected previous typing stop to be called once when replaced, got %d", oldStopCalls)
	}
	if newStopCalls != 0 {
		t.Fatalf("expected replacement typing stop to stay active until preSend, got %d calls", newStopCalls)
	}

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"})
	m.preSend(context.Background(), "test", msg, &mockChannel{})

	if newStopCalls != 1 {
		t.Fatalf("expected replacement typing stop to be called by preSend, got %d", newStopCalls)
	}
	if oldStopCalls != 1 {
		t.Fatalf("expected previous typing stop to not be called again, got %d", oldStopCalls)
	}
}

func TestSendWithRetry_PreSendEditsPlaceholder(t *testing.T) {
	m := newTestManager()
	var sendCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				sendCalled = true
				return nil
			},
		},
		editFn: func(_ context.Context, _, _, _ string) error {
			return nil // edit succeeds
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"})
	m.sendWithRetry(context.Background(), "test", w, msg)

	if sendCalled {
		t.Fatal("expected Send to NOT be called when placeholder was edited")
	}
}

// --- Dispatcher exit tests (Step 1) ---

func TestDispatcherPublishesTerminalRejection(t *testing.T) {
	for _, media := range []bool{false, true} {
		kind := "text"
		if media {
			kind = "media"
		}
		for _, registered := range []bool{false, true} {
			state := "unknown"
			if registered {
				state = "no_worker"
			}
			t.Run(kind+"/"+state, func(t *testing.T) {
				eventBus := runtimeevents.NewBus()
				t.Cleanup(func() { _ = eventBus.Close() })
				_, eventsCh, err := eventBus.Channel().OfKind(
					runtimeevents.KindChannelMessageOutboundFailed,
				).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{Name: "dispatch-reject", Buffer: 1})
				if err != nil {
					t.Fatalf("SubscribeChan failed: %v", err)
				}

				m := newTestManager()
				t.Cleanup(m.bus.Close)
				m.runtimeEvents = eventBus
				if registered {
					m.channels["test"] = &mockMediaChannel{}
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() {
					defer close(done)
					if media {
						m.dispatchOutboundMedia(ctx)
					} else {
						m.dispatchOutbound(ctx)
					}
				}()
				t.Cleanup(func() {
					cancel()
					<-done
				})

				traceScope := runtimeevents.NewTraceScope("/workspace/main", "turn-1")
				if media {
					err = m.bus.PublishOutboundMedia(context.Background(), bus.OutboundMediaMessage{
						Context:     bus.NewOutboundContext("test", "chat-1", ""),
						Parts:       []bus.MediaPart{{Ref: "media://test"}},
						TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
					})
				} else {
					err = m.bus.PublishOutbound(context.Background(), bus.OutboundMessage{
						Context: bus.NewOutboundContext("test", "chat-1", ""), Content: "hello",
						TraceScopes: []runtimeevents.TraceScope{traceScope}, TraceSettlement: true,
					})
				}
				if err != nil {
					t.Fatalf("publish outbound: %v", err)
				}

				failed := receiveChannelRuntimeEvent(t, eventsCh)
				payload, ok := failed.Payload.(ChannelOutboundPayload)
				if !ok || !payload.TraceSettlement ||
					!slices.Equal(payload.TraceScopes, []runtimeevents.TraceScope{traceScope}) ||
					payload.Error == "" {
					t.Fatalf("dispatch rejection = %#v", failed)
				}
			})
		}
	}
}

func TestDispatcherRejectsOnlySettlingInternalOutbounds(t *testing.T) {
	for _, mediaMessage := range []bool{false, true} {
		kind := "text"
		if mediaMessage {
			kind = "media"
		}
		for _, settlement := range []bool{false, true} {
			state := "unsettled"
			if settlement {
				state = "settling"
			}
			t.Run(kind+"/"+state, func(t *testing.T) {
				eventBus := runtimeevents.NewBus()
				t.Cleanup(func() { _ = eventBus.Close() })
				_, eventsCh, err := eventBus.Channel().OfKind(
					runtimeevents.KindChannelMessageOutboundFailed,
				).SubscribeChan(t.Context(), runtimeevents.SubscribeOptions{
					Name: "internal-dispatch-reject", Buffer: 1,
				})
				if err != nil {
					t.Fatalf("SubscribeChan failed: %v", err)
				}

				m := newTestManager()
				t.Cleanup(m.bus.Close)
				m.runtimeEvents = eventBus
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() {
					defer close(done)
					if mediaMessage {
						m.dispatchOutboundMedia(ctx)
					} else {
						m.dispatchOutbound(ctx)
					}
				}()
				t.Cleanup(func() {
					cancel()
					<-done
				})

				traceScopes := []runtimeevents.TraceScope(nil)
				if settlement {
					traceScopes = []runtimeevents.TraceScope{
						runtimeevents.NewTraceScope("/workspace/main", "turn-internal"),
					}
				}
				if mediaMessage {
					err = m.bus.PublishOutboundMedia(context.Background(), bus.OutboundMediaMessage{
						Context:     bus.NewOutboundContext("system", "chat-1", ""),
						Parts:       []bus.MediaPart{{Ref: "media://test"}},
						TraceScopes: traceScopes, TraceSettlement: settlement,
					})
				} else {
					err = m.bus.PublishOutbound(context.Background(), bus.OutboundMessage{
						Context: bus.NewOutboundContext("system", "chat-1", ""), Content: "hello",
						TraceScopes: traceScopes, TraceSettlement: settlement,
					})
				}
				if err != nil {
					t.Fatalf("publish internal outbound: %v", err)
				}

				if !settlement {
					select {
					case event := <-eventsCh:
						t.Fatalf("unsettled internal outbound event = %#v", event)
					case <-time.After(50 * time.Millisecond):
					}
					return
				}

				failed := receiveChannelRuntimeEvent(t, eventsCh)
				payload, ok := failed.Payload.(ChannelOutboundPayload)
				if !ok || !payload.TraceSettlement || payload.Media != mediaMessage ||
					!slices.Equal(payload.TraceScopes, traceScopes) ||
					!strings.Contains(payload.Error, "no external delivery owner") {
					t.Fatalf("internal dispatch rejection = %#v", failed)
				}
			})
		}
	}
}

func TestDispatcherExitsOnCancel(t *testing.T) {
	mb := bus.NewMessageBus()
	defer mb.Close()

	m := &Manager{
		channels: make(map[string]Channel),
		workers:  make(map[string]*channelWorker),
		bus:      mb,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		m.dispatchOutbound(ctx)
		close(done)
	}()

	// Cancel context and verify the dispatcher exits quickly
	cancel()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchOutbound did not exit within 2s after context cancel")
	}
}

func TestDispatcherMediaExitsOnCancel(t *testing.T) {
	mb := bus.NewMessageBus()
	defer mb.Close()

	m := &Manager{
		channels: make(map[string]Channel),
		workers:  make(map[string]*channelWorker),
		bus:      mb,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		m.dispatchOutboundMedia(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchOutboundMedia did not exit within 2s after context cancel")
	}
}

// --- TTL Janitor tests (Step 2) ---

func TestTypingStopJanitorEviction(t *testing.T) {
	m := newTestManager()

	var stopCalled atomic.Bool
	// Store a typing entry with a creation time far in the past
	m.typingStops.Store("test:123", typingEntry{
		stop:      func() { stopCalled.Store(true) },
		createdAt: time.Now().Add(-10 * time.Minute), // well past typingStopTTL
	})

	// Run janitor with a short-lived context
	ctx, cancel := context.WithCancel(context.Background())

	// Manually trigger the janitor logic once by simulating a tick
	go func() {
		// Override janitor to run immediately
		now := time.Now()
		m.typingStops.Range(func(key, value any) bool {
			if entry, ok := value.(typingEntry); ok {
				if now.Sub(entry.createdAt) > typingStopTTL {
					if _, loaded := m.typingStops.LoadAndDelete(key); loaded {
						entry.stop()
					}
				}
			}
			return true
		})
		cancel()
	}()

	<-ctx.Done()

	if !stopCalled.Load() {
		t.Fatal("expected typing stop function to be called by janitor eviction")
	}

	// Verify entry was deleted
	if _, loaded := m.typingStops.Load("test:123"); loaded {
		t.Fatal("expected typing entry to be deleted after eviction")
	}
}

func TestPlaceholderJanitorEviction(t *testing.T) {
	m := newTestManager()

	// Store a placeholder entry with a creation time far in the past
	m.placeholders.Store("test:456", placeholderEntry{
		id:        "msg_old",
		createdAt: time.Now().Add(-20 * time.Minute), // well past placeholderTTL
	})

	// Simulate janitor logic
	now := time.Now()
	m.placeholders.Range(func(key, value any) bool {
		if entry, ok := value.(placeholderEntry); ok {
			if now.Sub(entry.createdAt) > placeholderTTL {
				m.placeholders.Delete(key)
			}
		}
		return true
	})

	// Verify entry was deleted
	if _, loaded := m.placeholders.Load("test:456"); loaded {
		t.Fatal("expected placeholder entry to be deleted after eviction")
	}
}

func TestPreSendStillWorksWithWrappedTypes(t *testing.T) {
	m := newTestManager()
	var stopCalled bool
	var editCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				return nil
			},
		},
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			editCalled = true
			if messageID != "ph_id" {
				t.Fatalf("expected messageID ph_id, got %s", messageID)
			}
			return nil
		},
	}

	// Use the new wrapped types via the public API
	m.RecordTypingStop("test", "chat1", func() {
		stopCalled = true
	})
	m.RecordPlaceholder("test", "chat1", "ph_id")

	msg := testOutboundMessage(bus.OutboundMessage{Channel: "test", ChatID: "chat1", Content: "response"})
	_, edited := m.preSend(context.Background(), "test", msg, ch)

	if !stopCalled {
		t.Fatal("expected typing stop to be called via wrapped type")
	}
	if !editCalled {
		t.Fatal("expected EditMessage to be called via wrapped type")
	}
	if !edited {
		t.Fatal("expected preSend to return true")
	}
}

// --- Lazy worker creation tests (Step 6) ---

func TestLazyWorkerCreation(t *testing.T) {
	m := newTestManager()

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return nil
		},
	}

	// RegisterChannel should NOT create a worker
	m.RegisterChannel("lazy", ch)

	m.mu.RLock()
	_, chExists := m.channels["lazy"]
	_, wExists := m.workers["lazy"]
	m.mu.RUnlock()

	if !chExists {
		t.Fatal("expected channel to be registered")
	}
	if wExists {
		t.Fatal("expected worker to NOT be created by RegisterChannel (lazy creation)")
	}
}

// --- FastID uniqueness test (Step 5) ---

func TestBuildMediaScope_FastIDUniqueness(t *testing.T) {
	seen := make(map[string]bool)

	for range 1000 {
		scope := BuildMediaScope("test", "chat1", "")
		if seen[scope] {
			t.Fatalf("duplicate scope generated: %s", scope)
		}
		seen[scope] = true
	}

	// Verify format: "channel:chatID:id"
	scope := BuildMediaScope("telegram", "42", "")
	parts := 0
	for _, c := range scope {
		if c == ':' {
			parts++
		}
	}
	if parts != 2 {
		t.Fatalf("expected scope to have 2 colons (channel:chatID:id), got: %s", scope)
	}
}

func TestBuildMediaScope_WithMessageID(t *testing.T) {
	scope := BuildMediaScope("discord", "chat99", "msg123")
	expected := "discord:chat99:msg123"
	if scope != expected {
		t.Fatalf("expected %s, got %s", expected, scope)
	}
}

func TestManager_PlaceholderConsumedByResponse(t *testing.T) {
	mgr := &Manager{
		channels:     make(map[string]Channel),
		workers:      make(map[string]*channelWorker),
		placeholders: sync.Map{},
	}

	mockCh := &mockChannel{
		sendFn: func(ctx context.Context, msg bus.OutboundMessage) error {
			return nil
		},
	}
	worker := newChannelWorker("mock", mockCh, "mock")
	mgr.channels["mock"] = mockCh
	mgr.workers["mock"] = worker

	ctx := context.Background()
	key := "mock:chat-1"

	// Simulate a placeholder recorded by base.go HandleMessage
	mgr.RecordPlaceholder("mock", "chat-1", "ph-123")

	if _, ok := mgr.placeholders.Load(key); !ok {
		t.Fatal("expected placeholder to be recorded")
	}

	// Transcription feedback arrives first — it should consume the placeholder
	// and be delivered via EditMessage, not Send.
	msgTranscript := testOutboundMessage(bus.OutboundMessage{
		Channel: "mock",
		ChatID:  "chat-1",
		Content: "Transcript: hello",
	})
	mgr.sendWithRetry(ctx, "mock", worker, msgTranscript)

	if mockCh.editedMessages != 1 {
		t.Errorf("expected 1 edited message (placeholder consumed by transcript), got %d", mockCh.editedMessages)
	}
	if len(mockCh.sentMessages) != 0 {
		t.Errorf("expected 0 normal messages (transcript used edit), got %d", len(mockCh.sentMessages))
	}

	// Placeholder should be gone now
	if _, ok := mgr.placeholders.Load(key); ok {
		t.Error("expected placeholder to be removed after being consumed")
	}

	// Final LLM response arrives — no placeholder left, so it goes through Send
	msgFinal := testOutboundMessage(bus.OutboundMessage{
		Channel: "mock",
		ChatID:  "chat-1",
		Content: "Final Answer",
	})
	mgr.sendWithRetry(ctx, "mock", worker, msgFinal)

	if len(mockCh.sentMessages) != 1 {
		t.Errorf("expected 1 normal message sent, got %d", len(mockCh.sentMessages))
	}
}

func TestSendMessage_Synchronous(t *testing.T) {
	m := newTestManager()

	var received []bus.OutboundMessage
	ch := &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			received = append(received, msg)
			return nil
		},
	}

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel:          "test",
		ChatID:           "123",
		Content:          "hello world",
		ReplyToMessageID: "msg-456",
	})

	err := m.SendMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// SendMessage is synchronous — message should already be delivered
	if len(received) != 1 {
		t.Fatalf("expected 1 message sent, got %d", len(received))
	}
	if received[0].ReplyToMessageID != "msg-456" {
		t.Fatalf("expected ReplyToMessageID msg-456, got %s", received[0].ReplyToMessageID)
	}
	if received[0].Content != "hello world" {
		t.Fatalf("expected content 'hello world', got %s", received[0].Content)
	}
}

func TestSendMessage_UnknownChannel(t *testing.T) {
	m := newTestManager()

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "nonexistent",
		ChatID:  "123",
		Content: "hello",
	})

	err := m.SendMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for unknown channel")
	}
}

func TestSendMessage_ClosedDeliveryOwnerReturnsError(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return nil
		},
	}
	owner := newDeliveryOwner("test", ch, "test")
	owner.closed = true
	m.channels["test"] = ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner

	err := m.SendMessage(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "hello",
	}))
	if !errors.Is(err, errDeliveryClosed) {
		t.Fatalf("SendMessage() err=%v, want errDeliveryClosed", err)
	}
	if callCount != 0 {
		t.Fatalf("SendMessage called closed channel %d times", callCount)
	}
}

func TestSendMessage_NoWorker(t *testing.T) {
	m := newTestManager()

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error { return nil },
	}
	m.channels["test"] = ch
	// No worker registered

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "hello",
	})

	err := m.SendMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error when no worker exists")
	}
	if !DeliveryDefinitelyNotSent(err) {
		t.Fatalf("no-worker error was not classified as definitely not sent: %v", err)
	}
}

func TestSendMessage_WithRetry(t *testing.T) {
	m := newTestManager()

	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount == 1 {
				return fmt.Errorf("transient: %w", ErrTemporary)
			}
			return nil
		},
	}

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "retry me",
	})

	err := m.SendMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if callCount != 2 {
		t.Fatalf("expected 2 Send calls (1 failure + 1 success), got %d", callCount)
	}
}

func TestSendMessageDefiniteRetryOnlyStopsAfterAmbiguousFailure(t *testing.T) {
	m := newTestManager()
	callCount := 0
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount == 1 {
				return fmt.Errorf("timeout after acceptance is unknown: %w", ErrTemporary)
			}
			return nil
		},
	}
	m.channels["test"] = ch
	m.workers["test"] = &channelWorker{ch: ch, limiter: rate.NewLimiter(rate.Inf, 1)}

	err := m.SendMessageDefiniteRetryOnly(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test", ChatID: "123", Content: "do not duplicate",
	}))
	if err == nil {
		t.Fatal("ambiguous delivery unexpectedly succeeded after retry")
	}
	if DeliveryDefinitelyNotSent(err) {
		t.Fatalf("temporary failure was classified as definitely not sent: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("Send calls = %d, want 1 after ambiguous failure", callCount)
	}
}

func TestSendMediaDoesNotRetryAfterPartialDelivery(t *testing.T) {
	m := newTestManager()
	callCount := 0
	ch := &mockMediaChannel{sendMediaFn: func(
		context.Context, bus.OutboundMediaMessage,
	) ([]string, error) {
		callCount++
		return []string{"sent-1"}, fmt.Errorf("second part failed: %w", ErrSendFailed)
	}}
	m.channels["test"] = ch
	m.workers["test"] = &channelWorker{ch: ch, limiter: rate.NewLimiter(rate.Inf, 1)}

	err := m.SendMedia(context.Background(), testOutboundMediaMessage(bus.OutboundMediaMessage{
		Channel: "test", ChatID: "123", Parts: []bus.MediaPart{
			{Type: "image", Ref: "media://first"},
			{Type: "image", Ref: "media://second"},
		},
	}))
	if err == nil {
		t.Fatal("partial media delivery unexpectedly succeeded")
	}
	if DeliveryDefinitelyNotSent(err) {
		t.Fatalf("partial media delivery was classified as safe to retry: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("SendMedia calls = %d, want 1 after partial delivery", callCount)
	}
}

func TestSendMessage_ReturnsErrorAfterDeliveryFailure(t *testing.T) {
	m := newTestManager()
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return fmt.Errorf("permanent: %w", ErrSendFailed)
		},
	}
	m.channels["test"] = ch
	m.workers["test"] = &channelWorker{ch: ch, limiter: rate.NewLimiter(rate.Inf, 1)}

	err := m.SendMessage(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test", ChatID: "123", Content: "must fail",
	}))
	if err == nil {
		t.Fatal("SendMessage() succeeded after channel delivery failed")
	}
	if !DeliveryDefinitelyNotSent(err) {
		t.Fatalf("permanent channel rejection was not classified as not sent: %v", err)
	}
}

func TestSendMessage_PartialChunkFailureIsAmbiguous(t *testing.T) {
	m := newTestManager()
	callCount := 0
	ch := &mockChannelWithLength{
		mockChannel: mockChannel{sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount == 2 {
				return fmt.Errorf("second chunk rejected: %w", ErrSendFailed)
			}
			return nil
		}},
		maxLen: 5,
	}
	m.channels["test"] = ch
	m.workers["test"] = &channelWorker{ch: ch, limiter: rate.NewLimiter(rate.Inf, 1)}

	err := m.SendMessage(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test", ChatID: "123", Content: "hello world",
	}))
	if err == nil {
		t.Fatal("partial chunk delivery unexpectedly succeeded")
	}
	if DeliveryDefinitelyNotSent(err) {
		t.Fatalf("partial chunk delivery was classified as safe to retry: %v", err)
	}
}

func TestSendMessage_ContextOnlyUsesContextAddressing(t *testing.T) {
	m := newTestManager()

	var received []bus.OutboundMessage
	ch := &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			received = append(received, msg)
			return nil
		},
	}

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	msg := testOutboundMessage(bus.OutboundMessage{
		Context: bus.NewOutboundContext("test", "123", "msg-9"),
		Content: "hello",
	})

	if err := m.SendMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message sent, got %d", len(received))
	}
	if received[0].Channel != "test" || received[0].ChatID != "123" {
		t.Fatalf("expected mirrored legacy address, got %+v", received[0])
	}
	if received[0].Context.Channel != "test" || received[0].Context.ChatID != "123" {
		t.Fatalf("expected context address to be preserved, got %+v", received[0].Context)
	}
	if received[0].ReplyToMessageID != "msg-9" {
		t.Fatalf("expected reply_to_message_id msg-9, got %q", received[0].ReplyToMessageID)
	}
}

func TestSendMessage_WithSplitting(t *testing.T) {
	m := newTestManager()

	var received []string
	ch := &mockChannelWithLength{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
				received = append(received, msg.Content)
				return nil
			},
		},
		maxLen: 5,
	}

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	msg := testOutboundMessage(bus.OutboundMessage{
		Channel: "test",
		ChatID:  "123",
		Content: "hello world",
	})

	err := m.SendMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(received) < 2 {
		t.Fatalf("expected message to be split into at least 2 chunks, got %d", len(received))
	}
}

func TestSendMedia_ContextOnlyUsesContextAddressing(t *testing.T) {
	m := newTestManager()

	var received []bus.OutboundMediaMessage
	ch := &mockMediaChannel{
		sendMediaFn: func(_ context.Context, msg bus.OutboundMediaMessage) ([]string, error) {
			received = append(received, msg)
			return nil, nil
		},
	}

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	msg := testOutboundMediaMessage(bus.OutboundMediaMessage{
		Context: bus.NewOutboundContext("test", "media-chat", ""),
		Parts:   []bus.MediaPart{{Type: "image", Ref: "media://1"}},
	})

	if err := m.SendMedia(context.Background(), msg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 media message sent, got %d", len(received))
	}
	if received[0].Channel != "test" || received[0].ChatID != "media-chat" {
		t.Fatalf("expected mirrored legacy media address, got %+v", received[0])
	}
	if received[0].Context.Channel != "test" || received[0].Context.ChatID != "media-chat" {
		t.Fatalf("expected media context address to be preserved, got %+v", received[0].Context)
	}
}

func TestSendMessage_PreservesOrdering(t *testing.T) {
	m := newTestManager()

	var order []string
	ch := &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			order = append(order, msg.Content)
			return nil
		},
	}

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	m.channels["test"] = ch
	m.workers["test"] = w

	// Send two messages sequentially — they must arrive in order
	_ = m.SendMessage(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test", ChatID: "1", Content: "first",
	}))
	_ = m.SendMessage(context.Background(), testOutboundMessage(bus.OutboundMessage{
		Channel: "test", ChatID: "1", Content: "second",
	}))

	if len(order) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(order))
	}
	if order[0] != "first" || order[1] != "second" {
		t.Fatalf("expected [first, second], got %v", order)
	}
}

func TestSendToChannel_QueuesThroughDeliveryOwner(t *testing.T) {
	m := newTestManager()
	sent := make(chan bus.OutboundMessage, 1)
	ch := &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			sent <- msg
			return nil
		},
	}
	owner := newDeliveryOwner("test", ch, "test")
	m.channels["test"] = ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner
	owner.StartDelivery(context.Background(), m)
	t.Cleanup(owner.CloseDeliveryAndWait)

	if err := m.SendToChannel(context.Background(), "test", "chat-1", "hello"); err != nil {
		t.Fatalf("SendToChannel() error = %v", err)
	}

	select {
	case got := <-sent:
		if got.Context.Channel != "test" || got.Context.ChatID != "chat-1" || got.Content != "hello" {
			t.Fatalf("queued message = %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queued SendToChannel message")
	}
}

func TestSendToChannel_ClosedDeliveryOwnerReturnsError(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return nil
		},
	}
	owner := newDeliveryOwner("test", ch, "test")
	owner.closed = true
	m.channels["test"] = ch
	m.workers["test"] = owner.Worker()
	m.deliveryOwners["test"] = owner

	err := m.SendToChannel(context.Background(), "test", "chat-1", "hello")
	if !errors.Is(err, errDeliveryClosed) {
		t.Fatalf("SendToChannel() err=%v, want errDeliveryClosed", err)
	}
	if callCount != 0 {
		t.Fatalf("SendToChannel called closed channel %d times", callCount)
	}
}

func TestSendToChannel_FallbackUsesLockedChannelSnapshot(t *testing.T) {
	m := newTestManager()
	var received []bus.OutboundMessage
	ch := &mockChannel{
		sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
			received = append(received, msg)
			return nil
		},
	}
	m.channels["test"] = ch

	if err := m.SendToChannel(context.Background(), "test", "chat-1", "hello"); err != nil {
		t.Fatalf("SendToChannel() error = %v", err)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 fallback message, got %d", len(received))
	}
	if received[0].Context.Channel != "test" || received[0].Context.ChatID != "chat-1" {
		t.Fatalf("fallback message context = %+v", received[0].Context)
	}
}

func TestManager_SendPlaceholder(t *testing.T) {
	mgr := &Manager{
		channels:     make(map[string]Channel),
		workers:      make(map[string]*channelWorker),
		placeholders: sync.Map{},
	}

	mockCh := &mockChannel{
		sendFn: func(ctx context.Context, msg bus.OutboundMessage) error {
			return nil
		},
	}
	mgr.channels["mock"] = mockCh

	ctx := context.Background()

	// SendPlaceholder should send a placeholder and record it
	ok := mgr.SendPlaceholder(ctx, "mock", "chat-1")
	if !ok {
		t.Fatal("expected SendPlaceholder to succeed")
	}
	if mockCh.placeholdersSent != 1 {
		t.Errorf("expected 1 placeholder sent, got %d", mockCh.placeholdersSent)
	}

	key := "mock:chat-1"
	if _, loaded := mgr.placeholders.Load(key); !loaded {
		t.Error("expected placeholder to be recorded in manager")
	}

	// SendPlaceholder on unknown channel should return false
	ok = mgr.SendPlaceholder(ctx, "unknown", "chat-1")
	if ok {
		t.Error("expected SendPlaceholder to fail for unknown channel")
	}
}

// turnUsageTrackingStreamer is a mockStreamer that records SetTurnUsage calls,
// used to verify the manager's streamer wrappers forward per-turn token usage
// to the inner streamer (regression: the wrappers previously dropped it because
// SetTurnUsage is not part of the bus.Streamer interface).
type turnUsageTrackingStreamer struct {
	mockStreamer
	inputTokens  int
	outputTokens int
	usageCalls   int
}

func (m *turnUsageTrackingStreamer) SetTurnUsage(inputTokens, outputTokens int) {
	m.usageCalls++
	m.inputTokens = inputTokens
	m.outputTokens = outputTokens
}

func TestFinalizeHookStreamerForwardsTurnUsage(t *testing.T) {
	inner := &turnUsageTrackingStreamer{}
	wrapper := &finalizeHookStreamer{Streamer: inner}

	setter, ok := any(wrapper).(turnUsageStreamer)
	if !ok {
		t.Fatal("finalizeHookStreamer does not satisfy turnUsageStreamer")
	}
	setter.SetTurnUsage(1234, 567)

	if inner.usageCalls != 1 {
		t.Fatalf("inner SetTurnUsage calls = %d, want 1", inner.usageCalls)
	}
	if inner.inputTokens != 1234 || inner.outputTokens != 567 {
		t.Errorf("inner usage = (%d, %d), want (1234, 567)", inner.inputTokens, inner.outputTokens)
	}
}

func TestSplitMarkerStreamerForwardsTurnUsage(t *testing.T) {
	inner := &turnUsageTrackingStreamer{}
	wrapper := &splitMarkerStreamer{current: inner}

	setter, ok := any(wrapper).(turnUsageStreamer)
	if !ok {
		t.Fatal("splitMarkerStreamer does not satisfy turnUsageStreamer")
	}
	setter.SetTurnUsage(1234, 567)

	if inner.usageCalls != 1 {
		t.Fatalf("inner SetTurnUsage calls = %d, want 1", inner.usageCalls)
	}
	if inner.inputTokens != 1234 || inner.outputTokens != 567 {
		t.Errorf("inner usage = (%d, %d), want (1234, 567)", inner.inputTokens, inner.outputTokens)
	}
}
