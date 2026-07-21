package channels

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	toolFeedbackTerminalTombstoneTTL = 30 * time.Second
	toolFeedbackCleanupRetryDelay    = 5 * time.Second
	toolFeedbackCleanupRetention     = 30 * time.Second
)

type toolFeedbackOperations struct {
	edit   func(context.Context, string, string, string) error
	delete func(context.Context, string, string) error
}

type toolFeedbackSendResult struct {
	messageIDs []string
	editable   bool
}

type trackedToolFeedbackMessage struct {
	chatID     string
	messageID  string
	editable   bool
	content    string
	operations toolFeedbackOperations
}

type pendingToolFeedbackCleanup struct {
	message   trackedToolFeedbackMessage
	expiresAt time.Time
}

type toolFeedbackEntry struct {
	opMu sync.Mutex
	mu   sync.Mutex

	terminalGeneration uint64
	terminal           bool
	terminalUntil      time.Time
	terminalPending    int
	terminalSucceeded  bool
	retired            bool
	sending            bool
	current            trackedToolFeedbackMessage
	pendingCleanup     []pendingToolFeedbackCleanup
}

type toolFeedbackTerminal struct {
	key        string
	entry      *toolFeedbackEntry
	generation uint64
	retain     bool
	absorbed   bool
	completed  bool
}

// ToolFeedbackCoordinator is the single owner of editable tool-feedback
// message state. Channel adapters provide only send, edit, and delete
// operations; lifecycle transitions are serialized here.
type ToolFeedbackCoordinator struct {
	mu       sync.Mutex
	entries  map[string]*toolFeedbackEntry
	animator *ToolFeedbackAnimator
	separate bool
	stopped  bool
}

func NewToolFeedbackCoordinator(cfg ToolFeedbackAnimatorConfig, separate bool) *ToolFeedbackCoordinator {
	c := &ToolFeedbackCoordinator{
		entries:  make(map[string]*toolFeedbackEntry),
		separate: separate,
	}
	c.animator = NewToolFeedbackAnimator(c.editAnimated)
	c.animator.Configure(cfg)
	return c
}

func (c *ToolFeedbackCoordinator) Configure(cfg ToolFeedbackAnimatorConfig, separate bool) {
	if c == nil {
		return
	}
	c.animator.Configure(cfg)
	c.mu.Lock()
	c.separate = separate
	c.mu.Unlock()
}

func (c *ToolFeedbackCoordinator) Deliver(
	ctx context.Context,
	key string,
	chatID string,
	content string,
	operations toolFeedbackOperations,
	send func(context.Context, string) ([]string, error),
) ([]string, error) {
	if send == nil {
		return nil, ErrSendFailed
	}
	return c.deliver(ctx, key, chatID, content, operations, func(
		sendCtx context.Context,
		prepared string,
	) (toolFeedbackSendResult, error) {
		messageIDs, err := send(sendCtx, prepared)
		return toolFeedbackSendResult{messageIDs: messageIDs, editable: operations.edit != nil}, err
	})
}

func (c *ToolFeedbackCoordinator) deliver(
	ctx context.Context,
	key string,
	chatID string,
	content string,
	operations toolFeedbackOperations,
	send func(context.Context, string) (toolFeedbackSendResult, error),
) ([]string, error) {
	if send == nil {
		return nil, ErrSendFailed
	}
	if c == nil || strings.TrimSpace(key) == "" {
		result, err := send(ctx, content)
		return result.messageIDs, err
	}
	key = strings.TrimSpace(key)
	content = strings.TrimSpace(content)
	separate := c.separateMessages()
	entry := c.lockEntry(key)
	if entry == nil {
		return nil, ErrNotRunning
	}
	defer entry.opMu.Unlock()
	if err := c.retryPendingCleanup(ctx, entry); err != nil {
		return nil, err
	}

	entry.mu.Lock()
	if entry.terminal {
		if entry.terminalUntil.IsZero() || time.Now().Before(entry.terminalUntil) {
			entry.mu.Unlock()
			return nil, nil
		}
		entry.terminal = false
		entry.terminalUntil = time.Time{}
		entry.terminalPending = 0
		entry.terminalSucceeded = false
		entry.terminalGeneration++
	}
	if separate && entry.current.messageID != "" {
		entry.current = trackedToolFeedbackMessage{}
		entry.mu.Unlock()
		c.animator.Clear(key)
		entry.mu.Lock()
		if entry.terminal {
			entry.mu.Unlock()
			return nil, nil
		}
	}
	if entry.current.messageID != "" {
		current := entry.current
		if !current.editable {
			entry.mu.Unlock()
			return c.replaceTrackedMessage(ctx, key, entry, current, chatID, content, operations, send)
		}
		mergedContent := content
		if isWorkingSummaryToolFeedback(current.content) || isWorkingSummaryToolFeedback(content) {
			mergedContent = mergeToolFeedbackContent(current.content, content)
		}
		entry.mu.Unlock()

		updatedID, handled, err := c.animator.Update(ctx, key, content)
		entry.mu.Lock()
		terminal := entry.terminal
		retired := entry.retired
		unchanged := entry.current.messageID == current.messageID
		if err == nil && handled && unchanged && !terminal && !retired {
			entry.current.chatID = chatID
			entry.current.content = mergedContent
			entry.current.operations = operations
		}
		entry.mu.Unlock()
		if terminal || retired {
			c.animator.Clear(key)
		}
		if !handled {
			return []string{current.messageID}, nil
		}
		if err == nil {
			return []string{updatedID}, nil
		}
		if !errors.Is(err, ErrSendFailed) || current.operations.delete == nil ||
			terminal || retired || !unchanged {
			return nil, err
		}
		return c.replaceTrackedMessage(ctx, key, entry, current, chatID, content, operations, send)
	}

	entry.sending = true
	entry.mu.Unlock()

	result, err := send(ctx, InitialAnimatedToolFeedbackContent(content))
	messageIDs := result.messageIDs
	entry.mu.Lock()
	entry.sending = false
	terminal := entry.terminal
	retired := entry.retired
	trackable := (result.editable && operations.edit != nil) || operations.delete != nil
	if len(messageIDs) > 0 && trackable && !terminal && !retired {
		entry.current = trackedToolFeedbackMessage{
			chatID: chatID, messageID: messageIDs[0],
			editable: result.editable && operations.edit != nil,
			content:  content, operations: operations,
		}
		entry.mu.Unlock()
		if result.editable && operations.edit != nil {
			c.animator.RecordEdited(key, messageIDs[0], content)
		}
		return messageIDs, err
	}
	entry.mu.Unlock()

	if len(messageIDs) > 0 && (terminal || retired) {
		deleteToolFeedbackMessage(ctx, operations.delete, chatID, messageIDs[0])
		messageIDs = nil
	}
	if !terminal && !retired {
		c.retireIdleEntryLocked(key, entry)
	}
	return messageIDs, err
}

func (c *ToolFeedbackCoordinator) replaceTrackedMessage(
	ctx context.Context,
	key string,
	entry *toolFeedbackEntry,
	current trackedToolFeedbackMessage,
	chatID string,
	content string,
	operations toolFeedbackOperations,
	send func(context.Context, string) (toolFeedbackSendResult, error),
) ([]string, error) {
	entry.mu.Lock()
	if entry.terminal || entry.retired || entry.current.messageID != current.messageID {
		entry.mu.Unlock()
		return nil, nil
	}
	entry.sending = true
	entry.mu.Unlock()

	result, sendErr := send(ctx, InitialAnimatedToolFeedbackContent(content))
	messageIDs := result.messageIDs
	trackable := (result.editable && operations.edit != nil) || operations.delete != nil
	entry.mu.Lock()
	entry.sending = false
	terminal := entry.terminal
	retired := entry.retired
	unchanged := entry.current.messageID == current.messageID
	if len(messageIDs) == 0 || !trackable || terminal || retired || !unchanged {
		entry.mu.Unlock()
		if len(messageIDs) > 0 && (terminal || retired || !unchanged) {
			deleteToolFeedbackMessage(ctx, operations.delete, chatID, messageIDs[0])
			messageIDs = nil
		}
		return messageIDs, sendErr
	}
	replacement := trackedToolFeedbackMessage{
		chatID: chatID, messageID: messageIDs[0],
		editable: result.editable && operations.edit != nil,
		content:  content, operations: operations,
	}
	entry.current = replacement
	entry.mu.Unlock()

	c.animator.Clear(key)
	if replacement.editable {
		c.animator.RecordEdited(key, replacement.messageID, replacement.content)
	}
	if cleanupErr := tryDeleteToolFeedbackMessage(
		ctx, current.operations.delete, current.chatID, current.messageID,
	); cleanupErr != nil {
		entry.mu.Lock()
		entry.pendingCleanup = append(entry.pendingCleanup, newPendingToolFeedbackCleanup(current))
		entry.mu.Unlock()
		return messageIDs, cleanupErr
	}
	return messageIDs, sendErr
}

func (c *ToolFeedbackCoordinator) retryPendingCleanup(
	ctx context.Context,
	entry *toolFeedbackEntry,
) error {
	entry.mu.Lock()
	pending := append([]pendingToolFeedbackCleanup(nil), entry.pendingCleanup...)
	entry.pendingCleanup = nil
	entry.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	remaining := make([]pendingToolFeedbackCleanup, 0, len(pending))
	var firstErr error
	for _, cleanup := range pending {
		if !time.Now().Before(cleanup.expiresAt) {
			continue
		}
		message := cleanup.message
		if err := tryDeleteToolFeedbackMessage(
			ctx, message.operations.delete, message.chatID, message.messageID,
		); err != nil {
			if errors.Is(err, ErrSendFailed) || errors.Is(err, ErrNotRunning) ||
				!time.Now().Before(cleanup.expiresAt) {
				continue
			}
			remaining = append(remaining, cleanup)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	entry.mu.Lock()
	entry.pendingCleanup = append(remaining, entry.pendingCleanup...)
	entry.mu.Unlock()
	return firstErr
}

func newPendingToolFeedbackCleanup(message trackedToolFeedbackMessage) pendingToolFeedbackCleanup {
	return pendingToolFeedbackCleanup{
		message:   message,
		expiresAt: time.Now().Add(toolFeedbackCleanupRetention),
	}
}

func (c *ToolFeedbackCoordinator) BeginTerminal(key string) *toolFeedbackTerminal {
	return c.beginTerminal(key, true)
}

func (c *ToolFeedbackCoordinator) BeginTransientTerminal(key string) *toolFeedbackTerminal {
	return c.beginTerminal(key, false)
}

func (c *ToolFeedbackCoordinator) beginTerminal(key string, retain bool) *toolFeedbackTerminal {
	if c == nil || strings.TrimSpace(key) == "" {
		return nil
	}
	key = strings.TrimSpace(key)
	for {
		entry := c.getOrCreateEntry(key)
		if entry == nil {
			return nil
		}
		entry.mu.Lock()
		if entry.retired {
			entry.mu.Unlock()
			c.removeEntry(key, entry)
			continue
		}
		if entry.terminal && entry.terminalSucceeded {
			generation := entry.terminalGeneration
			entry.mu.Unlock()
			return &toolFeedbackTerminal{
				key: key, entry: entry, generation: generation, retain: retain, absorbed: true,
			}
		}
		if !entry.terminal {
			entry.terminalGeneration++
			entry.terminalPending = 0
			entry.terminalSucceeded = false
		}
		entry.terminal = true
		entry.terminalUntil = time.Time{}
		entry.terminalPending++
		generation := entry.terminalGeneration
		entry.mu.Unlock()

		return &toolFeedbackTerminal{key: key, entry: entry, generation: generation, retain: retain}
	}
}

func (c *ToolFeedbackCoordinator) CompleteTerminal(
	ctx context.Context,
	terminal *toolFeedbackTerminal,
	success bool,
) {
	if c == nil || terminal == nil || terminal.entry == nil {
		return
	}
	separate := c.separateMessages()
	entry := terminal.entry
	entry.opMu.Lock()
	_ = c.retryPendingCleanup(ctx, entry)
	entry.mu.Lock()
	if terminal.completed || terminal.absorbed || entry.retired || !entry.terminal ||
		entry.terminalGeneration != terminal.generation {
		entry.mu.Unlock()
		entry.opMu.Unlock()
		return
	}
	terminal.completed = true
	if entry.terminalPending > 0 {
		entry.terminalPending--
	}
	if !success {
		if entry.terminalSucceeded || entry.terminalPending > 0 {
			entry.mu.Unlock()
			entry.opMu.Unlock()
			return
		}
		entry.terminal = false
		entry.terminalUntil = time.Time{}
		current := entry.current
		entry.mu.Unlock()
		if current.messageID != "" && current.editable {
			c.animator.Record(terminal.key, current.messageID, current.content)
		} else if current.messageID == "" {
			c.retireIdleEntryLocked(terminal.key, entry)
		}
		entry.opMu.Unlock()
		return
	}
	if entry.terminalSucceeded {
		entry.mu.Unlock()
		entry.opMu.Unlock()
		return
	}

	entry.terminalSucceeded = true
	current := entry.current
	entry.current = trackedToolFeedbackMessage{}
	if !separate && current.messageID != "" && current.operations.delete != nil {
		entry.pendingCleanup = append(entry.pendingCleanup, newPendingToolFeedbackCleanup(current))
	}
	if terminal.retain {
		entry.terminalUntil = time.Now().Add(toolFeedbackTerminalTombstoneTTL)
	}
	entry.mu.Unlock()
	c.animator.Clear(terminal.key)
	_ = c.retryPendingCleanup(ctx, entry)
	entry.mu.Lock()
	pendingCleanup := len(entry.pendingCleanup) != 0
	if !pendingCleanup && !terminal.retain {
		entry.retired = true
	}
	entry.mu.Unlock()
	entry.opMu.Unlock()

	if pendingCleanup {
		c.scheduleTerminalMaintenance(terminal, toolFeedbackCleanupRetryDelay)
	} else if terminal.retain {
		c.scheduleTerminalMaintenance(terminal, toolFeedbackTerminalTombstoneTTL)
	} else {
		c.removeEntry(terminal.key, entry)
	}
}

func (c *ToolFeedbackCoordinator) Dismiss(ctx context.Context, key string) {
	terminal := c.BeginTerminal(key)
	c.CompleteTerminal(ctx, terminal, true)
}

func (c *ToolFeedbackCoordinator) DismissTransient(ctx context.Context, key string) {
	terminal := c.BeginTransientTerminal(key)
	c.CompleteTerminal(ctx, terminal, true)
}

func (c *ToolFeedbackCoordinator) ReleaseTerminal(key string) {
	if c == nil || strings.TrimSpace(key) == "" {
		return
	}
	key = strings.TrimSpace(key)
	entry := c.findEntry(key)
	if entry == nil {
		return
	}
	entry.opMu.Lock()
	entry.mu.Lock()
	if entry.retired || !entry.terminal || entry.current.messageID != "" ||
		entry.sending || len(entry.pendingCleanup) != 0 {
		entry.mu.Unlock()
		entry.opMu.Unlock()
		return
	}
	entry.retired = true
	entry.mu.Unlock()
	entry.opMu.Unlock()
	c.removeEntry(key, entry)
}

func (c *ToolFeedbackCoordinator) RetireChannel(ctx context.Context, channelName string) {
	if c == nil || strings.TrimSpace(channelName) == "" {
		return
	}
	prefix := strings.TrimSpace(channelName) + ":"
	type retiredFeedback struct {
		key       string
		chatID    string
		messageID string
		delete    func(context.Context, string, string) error
	}
	var retired []retiredFeedback
	type keyedEntry struct {
		key   string
		entry *toolFeedbackEntry
	}
	var entries []keyedEntry
	c.mu.Lock()
	for key, entry := range c.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entries = append(entries, keyedEntry{key: key, entry: entry})
	}
	c.mu.Unlock()
	for _, candidate := range entries {
		key, entry := candidate.key, candidate.entry
		entry.opMu.Lock()
		entry.mu.Lock()
		entry.retired = true
		pending := append([]pendingToolFeedbackCleanup(nil), entry.pendingCleanup...)
		messages := make([]trackedToolFeedbackMessage, 0, len(pending)+1)
		for _, cleanup := range pending {
			messages = append(messages, cleanup.message)
		}
		messages = append(messages, entry.current)
		entry.current = trackedToolFeedbackMessage{}
		entry.pendingCleanup = nil
		entry.mu.Unlock()
		entry.opMu.Unlock()
		for _, message := range messages {
			retired = append(retired, retiredFeedback{
				key: key, chatID: message.chatID, messageID: message.messageID,
				delete: message.operations.delete,
			})
		}
		c.removeEntry(key, entry)
	}
	for _, feedback := range retired {
		c.animator.Clear(feedback.key)
		deleteToolFeedbackMessage(ctx, feedback.delete, feedback.chatID, feedback.messageID)
	}
}

func (c *ToolFeedbackCoordinator) StopAll() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stopped = true
	for _, entry := range c.entries {
		entry.mu.Lock()
		entry.retired = true
		entry.mu.Unlock()
	}
	c.entries = make(map[string]*toolFeedbackEntry)
	c.mu.Unlock()
	c.animator.StopAll()
}

func (c *ToolFeedbackCoordinator) ActiveCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, entry := range c.entries {
		entry.mu.Lock()
		if !entry.retired && (entry.sending || entry.current.messageID != "" ||
			len(entry.pendingCleanup) != 0) {
			count++
		}
		entry.mu.Unlock()
	}
	return count
}

// singleActiveScopedKey resolves a scope-less compatibility target only when
// exactly one visible or in-flight turn exists below that target.
func (c *ToolFeedbackCoordinator) singleActiveScopedKey(baseKey string) (string, bool) {
	if c == nil || strings.TrimSpace(baseKey) == "" {
		return "", false
	}
	prefix := strings.TrimSpace(baseKey) + "\x00turn\x00"
	matched := ""
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry.mu.Lock()
		active := !entry.retired && (entry.sending || entry.current.messageID != "" ||
			len(entry.pendingCleanup) != 0)
		entry.mu.Unlock()
		if !active {
			continue
		}
		if matched != "" {
			return "", false
		}
		matched = key
	}
	return matched, matched != ""
}

func (c *ToolFeedbackCoordinator) editAnimated(
	ctx context.Context,
	key string,
	messageID string,
	content string,
) error {
	entry := c.findEntry(key)
	if entry == nil {
		return nil
	}
	entry.mu.Lock()
	if entry.retired || entry.terminal || entry.current.messageID != messageID {
		entry.mu.Unlock()
		return nil
	}
	chatID := entry.current.chatID
	editFn := entry.current.operations.edit
	entry.mu.Unlock()
	if editFn == nil {
		return nil
	}
	return editFn(ctx, chatID, messageID, content)
}

func (c *ToolFeedbackCoordinator) lockEntry(key string) *toolFeedbackEntry {
	for {
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			return nil
		}
		entry := c.entries[key]
		if entry == nil {
			entry = &toolFeedbackEntry{}
			c.entries[key] = entry
		}
		c.mu.Unlock()
		entry.opMu.Lock()
		entry.mu.Lock()
		retired := entry.retired
		entry.mu.Unlock()
		if !retired {
			return entry
		}
		entry.opMu.Unlock()
	}
}

func (c *ToolFeedbackCoordinator) getOrCreateEntry(key string) *toolFeedbackEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return nil
	}
	entry := c.entries[key]
	if entry == nil {
		entry = &toolFeedbackEntry{}
		c.entries[key] = entry
	}
	return entry
}

func (c *ToolFeedbackCoordinator) findEntry(key string) *toolFeedbackEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[key]
}

func (c *ToolFeedbackCoordinator) retireIdleEntryLocked(key string, entry *toolFeedbackEntry) {
	entry.mu.Lock()
	if entry.terminal || entry.sending || entry.current.messageID != "" ||
		len(entry.pendingCleanup) != 0 {
		entry.mu.Unlock()
		return
	}
	entry.retired = true
	entry.mu.Unlock()
	c.removeEntry(key, entry)
}

func deleteToolFeedbackMessage(
	ctx context.Context,
	deleteFn func(context.Context, string, string) error,
	chatID string,
	messageID string,
) {
	_ = tryDeleteToolFeedbackMessage(ctx, deleteFn, chatID, messageID)
}

func tryDeleteToolFeedbackMessage(
	ctx context.Context,
	deleteFn func(context.Context, string, string) error,
	chatID string,
	messageID string,
) error {
	if deleteFn == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(messageID) == "" {
		return nil
	}
	deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return deleteFn(deleteCtx, chatID, messageID)
}

func (c *ToolFeedbackCoordinator) removeEntry(key string, entry *toolFeedbackEntry) {
	c.mu.Lock()
	if c.entries[key] == entry {
		delete(c.entries, key)
	}
	c.mu.Unlock()
}

func (c *ToolFeedbackCoordinator) scheduleTerminalMaintenance(
	terminal *toolFeedbackTerminal,
	delay time.Duration,
) {
	if delay < 0 {
		delay = 0
	}
	time.AfterFunc(delay, func() { c.maintainTerminal(terminal) })
}

func (c *ToolFeedbackCoordinator) maintainTerminal(terminal *toolFeedbackTerminal) {
	if c == nil || terminal == nil || terminal.entry == nil {
		return
	}
	entry := terminal.entry
	entry.opMu.Lock()
	entry.mu.Lock()
	if entry.retired || !entry.terminal || !entry.terminalSucceeded ||
		entry.terminalGeneration != terminal.generation {
		entry.mu.Unlock()
		entry.opMu.Unlock()
		return
	}
	entry.mu.Unlock()
	_ = c.retryPendingCleanup(context.Background(), entry)
	entry.mu.Lock()
	if len(entry.pendingCleanup) != 0 {
		entry.mu.Unlock()
		entry.opMu.Unlock()
		c.scheduleTerminalMaintenance(terminal, toolFeedbackCleanupRetryDelay)
		return
	}
	if terminal.retain && time.Now().Before(entry.terminalUntil) {
		delay := time.Until(entry.terminalUntil)
		entry.mu.Unlock()
		entry.opMu.Unlock()
		c.scheduleTerminalMaintenance(terminal, delay)
		return
	}
	entry.retired = true
	entry.mu.Unlock()
	entry.opMu.Unlock()
	c.removeEntry(terminal.key, entry)
}

func (c *ToolFeedbackCoordinator) separateMessages() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.separate
}
