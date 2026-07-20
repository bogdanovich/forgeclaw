package channels

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultToolFeedbackAnimationInterval        = 3 * time.Second
	workingSummaryToolFeedbackAnimationInterval = 10 * time.Second
	defaultToolFeedbackEditTimeout              = 15 * time.Second
	maxMergedToolFeedbackLines                  = 8
)

const initialToolFeedbackAnimationFrame = ""

var (
	toolFeedbackAnimationFrames = []string{"..", "."}
	retryAfterPattern           = regexp.MustCompile(`retry after:? (\d+)`)
)

// MaxToolFeedbackAnimationFrameLength returns the largest frame suffix length
// so callers can reserve room before sending messages to length-limited APIs.
func MaxToolFeedbackAnimationFrameLength() int {
	maxLen := len([]rune(initialToolFeedbackAnimationFrame))
	for _, frame := range toolFeedbackAnimationFrames {
		if frameLen := len([]rune(frame)); frameLen > maxLen {
			maxLen = frameLen
		}
	}
	return maxLen
}

type toolFeedbackAnimationState struct {
	messageID   string
	baseContent string
	generation  uint64
	stop        chan struct{}
	done        chan struct{}
	stopOnce    sync.Once
	operationMu sync.Mutex
}

type toolFeedbackEditPause struct {
	generation uint64
	until      time.Time
}

type toolFeedbackLastEdit struct {
	generation uint64
	at         time.Time
}

// ToolFeedbackSnapshot captures a detached feedback message for conditional
// restoration after a failed terminal edit.
type ToolFeedbackSnapshot struct {
	ChatID    string
	MessageID string
	Content   string

	generation uint64
	revision   uint64
}

// ToolFeedbackAnimatorConfig controls how often editable progress messages are
// updated. Zero values preserve the legacy behavior: animation edits every
// three seconds and no minimum interval between content edits.
type ToolFeedbackAnimatorConfig struct {
	AnimationInterval time.Duration
	MinEditInterval   time.Duration
}

type ToolFeedbackAnimator struct {
	mu                sync.Mutex
	editFn            func(ctx context.Context, chatID, messageID, content string) error
	deleteFn          func(ctx context.Context, chatID, messageID string) error
	entries           map[string]*toolFeedbackAnimationState
	animationInterval time.Duration
	minEditInterval   time.Duration
	lastEdits         map[string]toolFeedbackLastEdit
	editPauses        map[string]toolFeedbackEditPause
	nextGeneration    uint64
	revisions         map[string]uint64
}

func NewToolFeedbackAnimator(
	editFn func(ctx context.Context, chatID, messageID, content string) error,
	deleteFns ...func(ctx context.Context, chatID, messageID string) error,
) *ToolFeedbackAnimator {
	animator := &ToolFeedbackAnimator{
		editFn:            editFn,
		entries:           make(map[string]*toolFeedbackAnimationState),
		animationInterval: defaultToolFeedbackAnimationInterval,
		lastEdits:         make(map[string]toolFeedbackLastEdit),
		editPauses:        make(map[string]toolFeedbackEditPause),
		revisions:         make(map[string]uint64),
	}
	if len(deleteFns) > 0 {
		animator.deleteFn = deleteFns[0]
	}
	return animator
}

func (a *ToolFeedbackAnimator) Configure(cfg ToolFeedbackAnimatorConfig) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if cfg.AnimationInterval > 0 {
		a.animationInterval = cfg.AnimationInterval
	} else {
		a.animationInterval = defaultToolFeedbackAnimationInterval
	}
	if cfg.MinEditInterval > 0 {
		a.minEditInterval = cfg.MinEditInterval
	} else {
		a.minEditInterval = 0
	}
}

func (a *ToolFeedbackAnimator) Current(chatID string) (string, bool) {
	if a == nil || strings.TrimSpace(chatID) == "" {
		return "", false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.entries[chatID]
	if !ok || strings.TrimSpace(entry.messageID) == "" {
		return "", false
	}
	return entry.messageID, true
}

func (a *ToolFeedbackAnimator) Record(chatID, messageID, content string) {
	a.record(chatID, messageID, content, false)
}

// RecordEdited tracks a feedback message after a real send/edit reached the
// channel. This makes MinEditInterval apply from the first visible progress
// update, not only after later in-place edits.
func (a *ToolFeedbackAnimator) RecordEdited(chatID, messageID, content string) {
	a.record(chatID, messageID, content, true)
}

func (a *ToolFeedbackAnimator) record(chatID, messageID, content string, edited bool) {
	if a == nil {
		return
	}
	chatID = strings.TrimSpace(chatID)
	messageID = strings.TrimSpace(messageID)
	content = strings.TrimSpace(content)
	if chatID == "" || messageID == "" || content == "" {
		return
	}

	entry := &toolFeedbackAnimationState{
		messageID:   messageID,
		baseContent: content,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	var previous *toolFeedbackAnimationState
	a.mu.Lock()
	a.nextGeneration++
	entry.generation = a.nextGeneration
	if old, ok := a.entries[chatID]; ok {
		previous = old
	}
	a.entries[chatID] = entry
	a.revisions[chatID]++
	if edited {
		a.lastEdits[chatID] = toolFeedbackLastEdit{
			generation: entry.generation,
			at:         time.Now(),
		}
	}
	a.mu.Unlock()

	stopToolFeedbackOperation(previous)
	if previous != nil && previous.messageID != entry.messageID {
		a.deleteDisplaced(chatID, previous.messageID)
	}
	go a.run(chatID, entry)
}

// deleteDisplaced removes a progress message only when Record atomically
// proved that a newer tracked message replaced it. A detached entry may have
// been finalized, so delayed cleanup cannot make the same inference safely.
func (a *ToolFeedbackAnimator) deleteDisplaced(chatID, messageID string) {
	if a == nil || a.deleteFn == nil || strings.TrimSpace(messageID) == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultToolFeedbackEditTimeout)
		defer cancel()
		_ = a.deleteFn(ctx, chatID, messageID)
	}()
}

func (a *ToolFeedbackAnimator) Clear(chatID string) {
	if a == nil || strings.TrimSpace(chatID) == "" {
		return
	}
	entry, _ := a.detach(chatID)
	stopToolFeedbackOperation(entry)
}

// ClearIfCurrent removes the tracked entry only when it still refers to the
// expected channel message. It lets delayed cleanup delete an old remote
// message without detaching a replacement progress message recorded meanwhile.
func (a *ToolFeedbackAnimator) ClearIfCurrent(chatID, messageID string) bool {
	if a == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(messageID) == "" {
		return false
	}
	chatID = strings.TrimSpace(chatID)
	messageID = strings.TrimSpace(messageID)

	a.mu.Lock()
	entry := a.entries[chatID]
	if entry == nil || entry.messageID != messageID {
		a.mu.Unlock()
		return false
	}
	delete(a.entries, chatID)
	a.revisions[chatID]++
	a.mu.Unlock()

	stopToolFeedbackOperation(entry)
	return true
}

func (a *ToolFeedbackAnimator) Take(chatID string) (string, string, bool) {
	snapshot, ok := a.TakeRestorable(chatID)
	if !ok {
		return "", "", false
	}
	return snapshot.MessageID, snapshot.Content, true
}

// TakeRestorable detaches a feedback message and returns a snapshot that can
// only be restored while no later lifecycle mutation has occurred.
func (a *ToolFeedbackAnimator) TakeRestorable(chatID string) (ToolFeedbackSnapshot, bool) {
	if a == nil || strings.TrimSpace(chatID) == "" {
		return ToolFeedbackSnapshot{}, false
	}
	chatID = strings.TrimSpace(chatID)
	var generation uint64
	for {
		entry := a.current(chatID)
		if entry == nil || strings.TrimSpace(entry.messageID) == "" {
			return ToolFeedbackSnapshot{}, false
		}
		if generation == 0 {
			generation = entry.generation
		} else if entry.generation != generation {
			return ToolFeedbackSnapshot{}, false
		}

		entry.operationMu.Lock()
		current := a.current(chatID)
		if current != entry {
			entry.operationMu.Unlock()
			if current == nil || current.generation != generation {
				return ToolFeedbackSnapshot{}, false
			}
			continue
		}

		stopToolFeedbackAnimation(entry)
		revision, detached := a.detachIfCurrent(chatID, entry)
		if !detached {
			entry.operationMu.Unlock()
			continue
		}
		snapshot := ToolFeedbackSnapshot{
			ChatID:     chatID,
			MessageID:  entry.messageID,
			Content:    entry.baseContent,
			generation: entry.generation,
			revision:   revision,
		}
		entry.operationMu.Unlock()
		return snapshot, true
	}
}

// Restore reinstates a detached snapshot only if no Clear, Record, Update, or
// other lifecycle mutation happened after TakeRestorable.
func (a *ToolFeedbackAnimator) Restore(snapshot ToolFeedbackSnapshot) bool {
	if a == nil || strings.TrimSpace(snapshot.ChatID) == "" ||
		strings.TrimSpace(snapshot.MessageID) == "" || strings.TrimSpace(snapshot.Content) == "" {
		return false
	}
	entry := &toolFeedbackAnimationState{
		messageID:   snapshot.MessageID,
		baseContent: snapshot.Content,
		generation:  snapshot.generation,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	a.mu.Lock()
	if a.entries[snapshot.ChatID] != nil || a.revisions[snapshot.ChatID] != snapshot.revision {
		a.mu.Unlock()
		return false
	}
	a.entries[snapshot.ChatID] = entry
	a.revisions[snapshot.ChatID]++
	a.mu.Unlock()

	go a.run(snapshot.ChatID, entry)
	return true
}

// Update edits an existing tracked feedback message. If the edit fails, the
// previous feedback state is restored so callers can retry without orphaning
// the old progress message.
func (a *ToolFeedbackAnimator) Update(ctx context.Context, chatID, content string) (string, bool, error) {
	if a == nil || a.editFn == nil {
		return "", false, nil
	}
	chatID = strings.TrimSpace(chatID)
	for {
		entry := a.current(chatID)
		if entry == nil || strings.TrimSpace(entry.messageID) == "" {
			return "", false, nil
		}

		entry.operationMu.Lock()
		current := a.current(chatID)
		if current != entry {
			entry.operationMu.Unlock()
			if current == nil || current.generation != entry.generation {
				return entry.messageID, true, nil
			}
			continue
		}

		stopToolFeedbackAnimation(entry)
		msgID := entry.messageID
		baseContent := entry.baseContent
		mergedContent := content
		if isWorkingSummaryToolFeedback(baseContent) || isWorkingSummaryToolFeedback(content) {
			mergedContent = mergeToolFeedbackContent(baseContent, content)
		}
		if a.shouldSkipEdit(chatID, entry.generation) {
			replaced := a.replaceIfCurrent(chatID, entry, msgID, mergedContent)
			entry.operationMu.Unlock()
			current = a.current(chatID)
			if replaced || current == nil || current.generation != entry.generation {
				return msgID, true, nil
			}
			continue
		}

		animatedContent := InitialAnimatedToolFeedbackContent(mergedContent)
		editCtx, cancel := context.WithTimeout(ctx, defaultToolFeedbackEditTimeout)
		err := a.editFn(editCtx, chatID, msgID, animatedContent)
		cancel()
		if err != nil && !isMessageNotModifiedError(err) {
			if delay, ok := a.retryAfterDelay(err); ok {
				a.pauseEdits(chatID, entry.generation, delay)
			}
			replaced := a.replaceIfCurrent(chatID, entry, msgID, baseContent)
			entry.operationMu.Unlock()
			if replaced {
				return "", true, err
			}
			current = a.current(chatID)
			if current == nil || current.generation != entry.generation {
				return msgID, true, nil
			}
			continue
		}

		replaced := a.replaceIfCurrent(chatID, entry, msgID, mergedContent)
		entry.operationMu.Unlock()
		if replaced {
			a.markEdit(chatID, entry.generation)
			return msgID, true, nil
		}
		current = a.current(chatID)
		if current == nil || current.generation != entry.generation {
			return msgID, true, nil
		}
	}
}

func (a *ToolFeedbackAnimator) current(chatID string) *toolFeedbackAnimationState {
	if a == nil || strings.TrimSpace(chatID) == "" {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.entries[chatID]
}

func (a *ToolFeedbackAnimator) replaceIfCurrent(
	chatID string,
	expected *toolFeedbackAnimationState,
	messageID, content string,
) bool {
	entry := &toolFeedbackAnimationState{
		messageID:   messageID,
		baseContent: content,
		generation:  expected.generation,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	a.mu.Lock()
	if a.entries[chatID] != expected {
		a.mu.Unlock()
		return false
	}
	a.entries[chatID] = entry
	a.revisions[chatID]++
	a.mu.Unlock()

	go a.run(chatID, entry)
	return true
}

func (a *ToolFeedbackAnimator) StopAll() {
	if a == nil {
		return
	}
	a.mu.Lock()
	entries := make([]*toolFeedbackAnimationState, 0, len(a.entries))
	for chatID, entry := range a.entries {
		entries = append(entries, entry)
		delete(a.entries, chatID)
		a.revisions[chatID]++
	}
	a.mu.Unlock()

	for _, entry := range entries {
		stopToolFeedbackOperation(entry)
	}
}

func (a *ToolFeedbackAnimator) detach(chatID string) (*toolFeedbackAnimationState, uint64) {
	if a == nil || strings.TrimSpace(chatID) == "" {
		return nil, 0
	}
	chatID = strings.TrimSpace(chatID)
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.entries[chatID]
	delete(a.entries, chatID)
	a.revisions[chatID]++
	return entry, a.revisions[chatID]
}

func (a *ToolFeedbackAnimator) detachIfCurrent(
	chatID string,
	expected *toolFeedbackAnimationState,
) (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.entries[chatID] != expected {
		return 0, false
	}
	delete(a.entries, chatID)
	a.revisions[chatID]++
	return a.revisions[chatID], true
}

func (a *ToolFeedbackAnimator) run(chatID string, entry *toolFeedbackAnimationState) {
	defer close(entry.done)

	ticker := time.NewTicker(a.getAnimationInterval(entry.baseContent))
	defer ticker.Stop()

	frameIdx := 1

	for {
		select {
		case <-entry.stop:
			return
		case <-ticker.C:
			if a.editFn == nil {
				continue
			}
			if a.shouldSkipEdit(chatID, entry.generation) {
				continue
			}
			frame := toolFeedbackAnimationFrames[frameIdx%len(toolFeedbackAnimationFrames)]
			content := formatAnimatedToolFeedbackContent(entry.baseContent, frame)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := a.editFn(ctx, chatID, entry.messageID, content); err != nil {
				if delay, ok := a.retryAfterDelay(err); ok {
					a.pauseEdits(chatID, entry.generation, delay)
				}
			} else {
				a.markEdit(chatID, entry.generation)
			}
			cancel()
			frameIdx++
		}
	}
}

func (a *ToolFeedbackAnimator) getAnimationInterval(content string) time.Duration {
	if a == nil {
		return toolFeedbackAnimationIntervalFor(content)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.animationInterval > 0 {
		return a.animationInterval
	}
	return toolFeedbackAnimationIntervalFor(content)
}

func (a *ToolFeedbackAnimator) shouldSkipEdit(chatID string, generation uint64) bool {
	if a == nil {
		return true
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if pause := a.editPauses[chatID]; pause.generation == generation && pause.until.After(now) {
		return true
	}
	if a.minEditInterval <= 0 {
		return false
	}
	if last := a.lastEdits[chatID]; last.generation == generation &&
		!last.at.IsZero() && now.Sub(last.at) < a.minEditInterval {
		return true
	}
	return false
}

func (a *ToolFeedbackAnimator) markEdit(chatID string, generation uint64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	if entry := a.entries[chatID]; entry == nil || entry.generation != generation {
		a.mu.Unlock()
		return
	}
	a.lastEdits[chatID] = toolFeedbackLastEdit{
		generation: generation,
		at:         time.Now(),
	}
	a.mu.Unlock()
}

func (a *ToolFeedbackAnimator) pauseEdits(chatID string, generation uint64, delay time.Duration) {
	if a == nil || delay <= 0 {
		return
	}
	a.mu.Lock()
	if entry := a.entries[chatID]; entry == nil || entry.generation != generation {
		a.mu.Unlock()
		return
	}
	a.editPauses[chatID] = toolFeedbackEditPause{
		generation: generation,
		until:      time.Now().Add(delay),
	}
	a.mu.Unlock()
}

func (a *ToolFeedbackAnimator) retryAfterDelay(err error) (time.Duration, bool) {
	if err == nil || a == nil {
		return 0, false
	}
	a.mu.Lock()
	minInterval := a.minEditInterval
	a.mu.Unlock()
	if minInterval <= 0 {
		return 0, false
	}
	errText := strings.ToLower(err.Error())
	if !errors.Is(err, ErrRateLimit) && !strings.Contains(errText, "too many requests") &&
		!strings.Contains(errText, "429") {
		return 0, false
	}
	match := retryAfterPattern.FindStringSubmatch(errText)
	if len(match) != 2 {
		return minInterval, true
	}
	seconds, parseErr := strconv.Atoi(match[1])
	if parseErr != nil || seconds <= 0 {
		return minInterval, true
	}
	return time.Duration(seconds) * time.Second, true
}

func isMessageNotModifiedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "message is not modified")
}

func InitialAnimatedToolFeedbackContent(baseContent string) string {
	return formatAnimatedToolFeedbackContent(baseContent, initialToolFeedbackAnimationFrame)
}

func formatAnimatedToolFeedbackContent(baseContent, frame string) string {
	baseContent = strings.TrimSpace(baseContent)
	frame = strings.TrimSpace(frame)
	if baseContent == "" {
		return ""
	}
	if frame == "" {
		return baseContent
	}
	lineBreak := strings.IndexByte(baseContent, '\n')
	if lineBreak < 0 {
		return appendToolFeedbackFrame(baseContent, frame)
	}
	return appendToolFeedbackFrame(baseContent[:lineBreak], frame) + baseContent[lineBreak:]
}

func appendToolFeedbackFrame(firstLine, frame string) string {
	firstLine = strings.TrimSpace(firstLine)
	frame = strings.TrimSpace(frame)
	if firstLine == "" {
		return ""
	}
	if frame == "" {
		return firstLine
	}

	openTick := strings.IndexByte(firstLine, '`')
	if openTick >= 0 {
		if closeOffset := strings.IndexByte(firstLine[openTick+1:], '`'); closeOffset >= 0 {
			closeTick := openTick + 1 + closeOffset
			return firstLine[:closeTick] + frame + firstLine[closeTick:]
		}
	}

	return firstLine + frame
}

func mergeToolFeedbackContent(previous, next string) string {
	previous = strings.TrimSpace(previous)
	next = strings.TrimSpace(next)
	if previous == "" {
		return next
	}
	if next == "" {
		return previous
	}

	lines := make([]string, 0, maxMergedToolFeedbackLines+1)
	seen := make(map[string]struct{})
	addLine := func(line string) {
		line = strings.TrimSpace(line)
		if line == "" || isWorkingSummaryHeader(line) {
			return
		}
		if _, ok := seen[line]; ok {
			return
		}
		seen[line] = struct{}{}
		lines = append(lines, line)
	}

	for _, line := range strings.Split(previous, "\n") {
		addLine(line)
	}
	for _, line := range strings.Split(next, "\n") {
		addLine(line)
	}
	if len(lines) > maxMergedToolFeedbackLines {
		lines = lines[len(lines)-maxMergedToolFeedbackLines:]
	}
	header := workingSummaryHeader(previous)
	if candidate := workingSummaryHeader(next); candidate != "" {
		header = candidate
	}
	if header == "" {
		header = "Working..."
	}
	if len(lines) == 0 {
		return header
	}
	return header + "\n" + strings.Join(lines, "\n")
}

func isWorkingSummaryToolFeedback(content string) bool {
	return isWorkingSummaryHeader(workingSummaryHeader(content))
}

func workingSummaryHeader(content string) string {
	firstLine, _, _ := strings.Cut(strings.TrimSpace(content), "\n")
	firstLine = strings.TrimSpace(firstLine)
	if isWorkingSummaryHeader(firstLine) {
		return firstLine
	}
	return ""
}

func isWorkingSummaryHeader(line string) bool {
	line = strings.TrimSpace(line)
	return strings.EqualFold(line, "Working...") || strings.HasSuffix(strings.ToLower(line), " working...")
}

func toolFeedbackAnimationIntervalFor(content string) time.Duration {
	if isWorkingSummaryToolFeedback(content) {
		return workingSummaryToolFeedbackAnimationInterval
	}
	return defaultToolFeedbackAnimationInterval
}

func stopToolFeedbackAnimation(entry *toolFeedbackAnimationState) {
	if entry == nil {
		return
	}
	entry.stopOnce.Do(func() { close(entry.stop) })
	<-entry.done
}

func stopToolFeedbackOperation(entry *toolFeedbackAnimationState) {
	if entry == nil {
		return
	}
	entry.operationMu.Lock()
	stopToolFeedbackAnimation(entry)
	entry.operationMu.Unlock()
}
