package channels

import (
	"context"
	"strings"
	"sync"
	"time"
)

const toolFeedbackTerminalTombstoneTTL = 30 * time.Second

type toolFeedbackOperations struct {
	edit   func(context.Context, string, string, string) error
	delete func(context.Context, string, string) error
	turnID string
}

type toolFeedbackSendResult struct {
	messageIDs []string
	editable   bool
}

type toolFeedbackEntry struct {
	opMu sync.Mutex
	mu   sync.Mutex

	terminalGeneration uint64
	terminal           bool
	terminalUntil      time.Time
	terminalTurnID     string
	terminalPending    int
	terminalSucceeded  bool
	retired            bool
	sending            bool
	chatID             string
	messageID          string
	content            string
	operations         toolFeedbackOperations
}

type toolFeedbackTerminal struct {
	key        string
	entry      *toolFeedbackEntry
	generation uint64
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
	turnID := strings.TrimSpace(operations.turnID)
	separate := c.separateMessages()
	entry := c.lockEntry(key)
	if entry == nil {
		return nil, ErrNotRunning
	}
	defer entry.opMu.Unlock()

	entry.mu.Lock()
	if entry.terminal {
		sameOrUnknownTurn := entry.terminalTurnID == "" || turnID == "" || entry.terminalTurnID == turnID
		if sameOrUnknownTurn && (entry.terminalUntil.IsZero() || time.Now().Before(entry.terminalUntil)) {
			entry.mu.Unlock()
			return nil, nil
		}
		entry.terminal = false
		entry.terminalUntil = time.Time{}
		entry.terminalTurnID = ""
		entry.terminalPending = 0
		entry.terminalSucceeded = false
		entry.terminalGeneration++
	}
	if separate && entry.messageID != "" {
		entry.messageID = ""
		entry.content = ""
		entry.mu.Unlock()
		c.animator.Clear(key)
		entry.mu.Lock()
		if entry.terminal {
			entry.mu.Unlock()
			return nil, nil
		}
	}
	if entry.messageID != "" {
		messageID := entry.messageID
		mergedContent := content
		if isWorkingSummaryToolFeedback(entry.content) || isWorkingSummaryToolFeedback(content) {
			mergedContent = mergeToolFeedbackContent(entry.content, content)
		}
		entry.chatID = chatID
		entry.operations = operations
		entry.mu.Unlock()

		updatedID, handled, err := c.animator.Update(ctx, key, content)
		entry.mu.Lock()
		if err == nil && handled {
			entry.content = mergedContent
		}
		terminal := entry.terminal
		retired := entry.retired
		entry.mu.Unlock()
		if terminal || retired {
			c.animator.Clear(key)
		}
		if !handled {
			return []string{messageID}, nil
		}
		if err != nil {
			return nil, err
		}
		return []string{updatedID}, nil
	}

	entry.sending = true
	entry.chatID = chatID
	entry.content = content
	entry.operations = operations
	entry.mu.Unlock()

	result, err := send(ctx, InitialAnimatedToolFeedbackContent(content))
	messageIDs := result.messageIDs
	entry.mu.Lock()
	entry.sending = false
	terminal := entry.terminal
	retired := entry.retired
	if len(messageIDs) > 0 && result.editable && operations.edit != nil && !terminal && !retired {
		entry.messageID = messageIDs[0]
		entry.mu.Unlock()
		c.animator.RecordEdited(key, messageIDs[0], content)
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

func (c *ToolFeedbackCoordinator) BeginTerminal(key string) *toolFeedbackTerminal {
	return c.BeginTerminalForTurn(key, "")
}

func (c *ToolFeedbackCoordinator) BeginTerminalForTurn(key, turnID string) *toolFeedbackTerminal {
	if c == nil || strings.TrimSpace(key) == "" {
		return nil
	}
	key = strings.TrimSpace(key)
	turnID = strings.TrimSpace(turnID)
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
		sameOrUnknownTurn := entry.terminalTurnID == "" || turnID == "" || entry.terminalTurnID == turnID
		if entry.terminal && entry.terminalSucceeded && sameOrUnknownTurn {
			generation := entry.terminalGeneration
			entry.mu.Unlock()
			return &toolFeedbackTerminal{
				key: key, entry: entry, generation: generation, absorbed: true,
			}
		}
		if !entry.terminal || !sameOrUnknownTurn {
			entry.terminalGeneration++
			entry.terminalPending = 0
			entry.terminalSucceeded = false
			entry.terminalTurnID = turnID
		}
		entry.terminal = true
		entry.terminalUntil = time.Time{}
		if entry.terminalPending > 0 && entry.terminalTurnID != turnID {
			entry.terminalTurnID = ""
		}
		entry.terminalPending++
		generation := entry.terminalGeneration
		entry.mu.Unlock()

		return &toolFeedbackTerminal{key: key, entry: entry, generation: generation}
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
	entry := terminal.entry
	entry.opMu.Lock()
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
		entry.terminalTurnID = ""
		messageID := entry.messageID
		content := entry.content
		entry.mu.Unlock()
		if messageID != "" {
			c.animator.Record(terminal.key, messageID, content)
		} else {
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
	messageID := entry.messageID
	chatID := entry.chatID
	deleteFn := entry.operations.delete
	entry.messageID = ""
	entry.content = ""
	entry.operations = toolFeedbackOperations{}
	entry.terminalUntil = time.Now().Add(toolFeedbackTerminalTombstoneTTL)
	entry.mu.Unlock()
	c.animator.Clear(terminal.key)
	entry.opMu.Unlock()

	if !c.separateMessages() && messageID != "" && deleteFn != nil {
		deleteToolFeedbackMessage(ctx, deleteFn, chatID, messageID)
	}
	c.expireTerminal(terminal)
}

func (c *ToolFeedbackCoordinator) Dismiss(ctx context.Context, key string) {
	c.DismissForTurn(ctx, key, "")
}

func (c *ToolFeedbackCoordinator) DismissForTurn(ctx context.Context, key, turnID string) {
	terminal := c.BeginTerminalForTurn(key, turnID)
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
	if entry.retired || !entry.terminal || entry.messageID != "" || entry.sending {
		entry.mu.Unlock()
		entry.opMu.Unlock()
		return
	}
	entry.retired = true
	entry.mu.Unlock()
	entry.opMu.Unlock()
	c.removeEntry(key, entry)
}

func (c *ToolFeedbackCoordinator) RetireChannel(channelName string) {
	if c == nil || strings.TrimSpace(channelName) == "" {
		return
	}
	prefix := strings.TrimSpace(channelName) + ":"
	c.mu.Lock()
	var retiredKeys []string
	for key, entry := range c.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry.mu.Lock()
		entry.retired = true
		entry.mu.Unlock()
		retiredKeys = append(retiredKeys, key)
		delete(c.entries, key)
	}
	c.mu.Unlock()
	for _, key := range retiredKeys {
		c.animator.Clear(key)
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
		if !entry.retired && (entry.sending || entry.messageID != "") {
			count++
		}
		entry.mu.Unlock()
	}
	return count
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
	if entry.retired || entry.terminal || entry.messageID != messageID {
		entry.mu.Unlock()
		return nil
	}
	chatID := entry.chatID
	editFn := entry.operations.edit
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
	if entry.terminal || entry.sending || entry.messageID != "" {
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
	if deleteFn == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(messageID) == "" {
		return
	}
	deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = deleteFn(deleteCtx, chatID, messageID)
}

func (c *ToolFeedbackCoordinator) removeEntry(key string, entry *toolFeedbackEntry) {
	c.mu.Lock()
	if c.entries[key] == entry {
		delete(c.entries, key)
	}
	c.mu.Unlock()
}

func (c *ToolFeedbackCoordinator) expireTerminal(terminal *toolFeedbackTerminal) {
	time.AfterFunc(toolFeedbackTerminalTombstoneTTL, func() {
		entry := terminal.entry
		entry.opMu.Lock()
		entry.mu.Lock()
		if entry.retired || !entry.terminal || entry.terminalGeneration != terminal.generation ||
			time.Now().Before(entry.terminalUntil) {
			entry.mu.Unlock()
			entry.opMu.Unlock()
			return
		}
		entry.retired = true
		entry.mu.Unlock()
		entry.opMu.Unlock()
		c.removeEntry(terminal.key, entry)
	})
}

func (c *ToolFeedbackCoordinator) separateMessages() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.separate
}
