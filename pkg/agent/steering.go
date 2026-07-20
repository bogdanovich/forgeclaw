package agent

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/sipeed/picoclaw/pkg/bus"
	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
)

// SteeringMode controls how queued steering messages are dequeued.
type SteeringMode string

const (
	// SteeringOneAtATime dequeues only the first queued message per poll.
	SteeringOneAtATime SteeringMode = "one-at-a-time"
	// SteeringAll drains the entire queue in a single poll.
	SteeringAll SteeringMode = "all"
	// MaxQueueSize number of possible messages in the Steering Queue
	MaxQueueSize = 10
)

// parseSteeringMode normalizes a config string into a SteeringMode.
func parseSteeringMode(s string) SteeringMode {
	switch s {
	case "all":
		return SteeringAll
	default:
		return SteeringOneAtATime
	}
}

// steeringQueue is a thread-safe queue of user messages that can be injected
// into a running agent loop to interrupt it between tool calls.
type steeringQueue struct {
	mu     sync.Mutex
	queues map[runtimeSessionScope][]steeringEntry
	mode   SteeringMode
}

type steeringEntry struct {
	msg      providers.Message
	senderID string
}

type steeringBatch struct {
	entries  []steeringEntry
	senderID string
}

func newSteeringQueue(mode SteeringMode) *steeringQueue {
	return &steeringQueue{
		queues: make(map[runtimeSessionScope][]steeringEntry),
		mode:   mode,
	}
}

func (sq *steeringQueue) pushScopeWithSender(
	scope runtimeSessionScope,
	msg providers.Message,
	senderID string,
) error {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	queue := sq.queues[scope]
	if len(queue) >= MaxQueueSize {
		return fmt.Errorf("steering queue is full")
	}
	sq.queues[scope] = append(queue, steeringEntry{
		msg:      msg,
		senderID: strings.TrimSpace(senderID),
	})
	return nil
}

// dequeueScope removes and returns pending steering messages for the provided
// scope according to the configured mode.
func (sq *steeringQueue) dequeueScope(scope runtimeSessionScope) []providers.Message {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	return entryMessages(sq.dequeueLocked(scope))
}

func (sq *steeringQueue) dequeueLocked(scope runtimeSessionScope) []steeringEntry {
	queue := sq.queues[scope]
	if len(queue) == 0 {
		return nil
	}

	switch sq.mode {
	case SteeringAll:
		msgs := append([]steeringEntry(nil), queue...)
		delete(sq.queues, scope)
		return msgs
	default:
		msg := queue[0]
		queue[0] = steeringEntry{} // Clear reference for GC
		queue = queue[1:]
		if len(queue) == 0 {
			delete(sq.queues, scope)
		} else {
			sq.queues[scope] = queue
		}
		return []steeringEntry{msg}
	}
}

func (sq *steeringQueue) dequeueScopeForTurn(
	scope runtimeSessionScope,
	senderID string,
) []providers.Message {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return entryMessages(
		sq.dequeueForTurnLocked(scope, strings.TrimSpace(senderID)),
	)
}

func (sq *steeringQueue) dequeueSteeringMessagesForTurn(
	scope runtimeSessionScope,
	senderID string,
) []providers.Message {
	if sq == nil {
		return nil
	}
	return sq.dequeueScopeForTurn(scope, senderID)
}

func (sq *steeringQueue) dequeueScopeForContinuation(scope runtimeSessionScope) []providers.Message {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return entryMessages(sq.dequeueContinuationLocked(scope).entries)
}

func (sq *steeringQueue) dequeueScopeForContinuationBatch(
	scope runtimeSessionScope,
) steeringBatch {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	if !scope.complete() {
		return steeringBatch{}
	}
	return sq.dequeueContinuationLocked(scope)
}

func (sq *steeringQueue) dequeueForTurnLocked(
	scope runtimeSessionScope,
	senderID string,
) []steeringEntry {
	if senderID == "" {
		return sq.dequeueLocked(scope)
	}

	queue := sq.queues[scope]
	if len(queue) == 0 {
		return nil
	}

	selected := make([]steeringEntry, 0, len(queue))
	remaining := make([]steeringEntry, 0, len(queue))
	for _, entry := range queue {
		switch entry.senderID {
		case "", senderID:
			selected = append(selected, entry)
		default:
			remaining = append(remaining, entry)
		}
	}

	if len(selected) == 0 {
		return nil
	}
	if len(remaining) == 0 {
		delete(sq.queues, scope)
	} else {
		sq.queues[scope] = remaining
	}
	return selected
}

func (sq *steeringQueue) dequeueContinuationLocked(scope runtimeSessionScope) steeringBatch {
	queue := sq.queues[scope]
	if len(queue) == 0 {
		return steeringBatch{}
	}

	firstSender := strings.TrimSpace(queue[0].senderID)
	if firstSender == "" {
		for _, entry := range queue[1:] {
			if senderID := strings.TrimSpace(entry.senderID); senderID != "" {
				firstSender = senderID
				break
			}
		}
		if firstSender == "" {
			return steeringBatch{entries: sq.dequeueLocked(scope)}
		}
	}

	selected := make([]steeringEntry, 0, len(queue))
	remaining := make([]steeringEntry, 0, len(queue))
	for _, entry := range queue {
		senderID := strings.TrimSpace(entry.senderID)
		if senderID == "" || senderID == firstSender {
			selected = append(selected, entry)
			continue
		}
		remaining = append(remaining, entry)
	}

	if len(remaining) == 0 {
		delete(sq.queues, scope)
	} else {
		sq.queues[scope] = remaining
	}
	return steeringBatch{entries: selected, senderID: firstSender}
}

func entryMessages(entries []steeringEntry) []providers.Message {
	if len(entries) == 0 {
		return nil
	}
	msgs := make([]providers.Message, 0, len(entries))
	for _, entry := range entries {
		msgs = append(msgs, entry.msg)
	}
	return msgs
}

// len returns the number of queued messages across all scopes.
func (sq *steeringQueue) len() int {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	total := 0
	for _, queue := range sq.queues {
		total += len(queue)
	}
	return total
}

// lenScope returns the number of queued messages for a specific scope.
func (sq *steeringQueue) lenScope(scope runtimeSessionScope) int {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return len(sq.queues[scope])
}

func (sq *steeringQueue) uniqueScopeForSession(
	sessionKey string,
) (runtimeSessionScope, bool, bool) {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	sessionKey = strings.TrimSpace(sessionKey)
	var found runtimeSessionScope
	for scope, queue := range sq.queues {
		if len(queue) == 0 || scope.sessionKey != sessionKey {
			continue
		}
		if found.complete() && found != scope {
			return runtimeSessionScope{}, false, true
		}
		found = scope
	}
	return found, found.complete(), false
}

func (sq *steeringQueue) clearScope(scope runtimeSessionScope) int {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	count := len(sq.queues[scope])
	if count > 0 {
		delete(sq.queues, scope)
	}
	return count
}

// setMode updates the steering mode.
func (sq *steeringQueue) setMode(mode SteeringMode) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sq.mode = mode
}

// getMode returns the current steering mode.
func (sq *steeringQueue) getMode() SteeringMode {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return sq.mode
}

// Steer enqueues a user message to be injected into the currently running
// agent loop. The message will be picked up after the current tool finishes
// executing, causing any remaining tool calls in the batch to be skipped.
func (al *AgentLoop) Steer(
	workspace, sessionKey, agentID string,
	msg providers.Message,
) error {
	scope := newRuntimeSessionScope(workspace, sessionKey)
	if !scope.complete() {
		return fmt.Errorf("steering workspace and session are required")
	}
	return al.enqueueSteeringMessageWithSender(scope, agentID, "", msg)
}

func (al *AgentLoop) enqueueSteeringMessageWithSender(
	scope runtimeSessionScope,
	agentID, senderID string,
	msg providers.Message,
) error {
	if al.steering == nil {
		return fmt.Errorf("steering queue is not initialized")
	}
	if !scope.complete() {
		return fmt.Errorf("steering workspace and session are required")
	}
	if strings.TrimSpace(agentID) == "" || al.agentForRuntimeScope(scope, agentID) == nil {
		return fmt.Errorf("steering agent %q does not own workspace %q", agentID, scope.workspace)
	}

	msg = steeringPromptMessage(msg)
	if err := al.steering.pushScopeWithSender(scope, msg, senderID); err != nil {
		logger.WarnCF("agent", "Failed to enqueue steering message", map[string]any{
			"error":     err.Error(),
			"role":      msg.Role,
			"workspace": scope.workspace,
			"scope":     scope.sessionKey,
			"sender_id": strings.TrimSpace(senderID),
		})
		return err
	}

	queueDepth := al.steering.lenScope(scope)
	logger.DebugCF("agent", "Steering message enqueued", map[string]any{
		"role":        msg.Role,
		"content_len": len(msg.Content),
		"media_count": len(msg.Media),
		"queue_len":   queueDepth,
		"workspace":   scope.workspace,
		"scope":       scope.sessionKey,
		"sender_id":   strings.TrimSpace(senderID),
	})

	meta := HookMeta{
		Source:    "Steer",
		TracePath: "turn.interrupt.received",
	}
	if ts := al.getActiveTurnState(scope); ts != nil {
		meta = ts.eventMeta("Steer", "turn.interrupt.received")
	} else {
		if strings.TrimSpace(agentID) != "" {
			meta.AgentID = agentID
		}
		if scope.complete() {
			meta.SessionKey = scope.sessionKey
		}
		if meta.AgentID == "" {
			if registry := al.GetRegistry(); registry != nil {
				if agent := registry.GetDefaultAgent(); agent != nil {
					meta.AgentID = agent.ID
				}
			}
		}
	}

	al.emitEvent(
		runtimeevents.KindAgentInterruptReceived,
		meta,
		InterruptReceivedPayload{
			Kind:        InterruptKindSteering,
			Role:        msg.Role,
			ContentLen:  len(msg.Content),
			QueueDepth:  queueDepth,
			MessageHash: evaluationSafeHash(al.GetConfig(), msg.Content),
		},
	)

	return nil
}

// SteeringMode returns the current steering mode.
func (al *AgentLoop) SteeringMode() SteeringMode {
	if al.steering == nil {
		return SteeringOneAtATime
	}
	return al.steering.getMode()
}

// SetSteeringMode updates the steering mode.
func (al *AgentLoop) SetSteeringMode(mode SteeringMode) {
	if al.steering == nil {
		return
	}
	al.steering.setMode(mode)
}

func (al *AgentLoop) dequeueSteeringMessagesForScope(scope runtimeSessionScope) []providers.Message {
	if al.steering == nil {
		return nil
	}
	return al.steering.dequeueScope(scope)
}

func (al *AgentLoop) dequeueSteeringMessagesForTurn(
	scope runtimeSessionScope,
	senderID string,
) []providers.Message {
	if al.steering == nil {
		return nil
	}
	return al.steering.dequeueScopeForTurn(scope, senderID)
}

func (al *AgentLoop) dequeueSteeringBatchForContinuation(
	scope runtimeSessionScope,
) steeringBatch {
	if al.steering == nil {
		return steeringBatch{}
	}
	return al.steering.dequeueScopeForContinuationBatch(scope)
}

func (al *AgentLoop) ackAcceptedSteeringMessages(ctx context.Context, msgs []providers.Message) {
	for _, msg := range msgs {
		if msg.InboundSpoolID == "" {
			continue
		}
		al.ackInboundMessage(ctx, bus.InboundMessage{SpoolID: msg.InboundSpoolID})
	}
}

func (al *AgentLoop) releaseSteeringMessages(
	ctx context.Context,
	msgs []providers.Message,
	cause error,
) {
	for _, msg := range msgs {
		if msg.InboundSpoolID == "" {
			continue
		}
		al.releaseInboundMessage(ctx, bus.InboundMessage{SpoolID: msg.InboundSpoolID}, cause)
	}
}

func (al *AgentLoop) pendingSteeringCountForScope(scope runtimeSessionScope) int {
	if al.steering == nil {
		return 0
	}
	return al.steering.lenScope(scope)
}

func (al *AgentLoop) clearSteeringMessagesForScope(scope runtimeSessionScope) int {
	if al.steering == nil {
		return 0
	}
	return al.steering.clearScope(scope)
}

func (al *AgentLoop) continueWithSteeringMessages(
	ctx context.Context,
	agent *AgentInstance,
	sessionKey, channel, chatID string,
	scope *session.SessionScope,
	senderID string,
	modelBinding effectiveModelBinding,
	steeringMsgs []providers.Message,
	observeTurn func(runtimeevents.TraceScope),
) (string, error) {
	routeSessionKey := sessionKey
	if scope != nil && strings.TrimSpace(scope.RouteScopeKey) != "" {
		routeSessionKey = strings.TrimSpace(scope.RouteScopeKey)
	}
	dispatch := DispatchRequest{
		RouteSessionKey: routeSessionKey,
		BaseSessionKey:  sessionKey,
		SessionKey:      sessionKey,
		SessionScope:    session.CloneScope(scope),
	}
	if channel != "" || chatID != "" {
		dispatch.InboundContext = &bus.InboundContext{
			Channel:  channel,
			ChatID:   chatID,
			ChatType: inferChatTypeFromSessionScope(scope),
			SenderID: strings.TrimSpace(senderID),
		}
	}
	return al.runAgentLoop(ctx, agent, processOptions{
		ModelBinding:             modelBinding,
		Dispatch:                 dispatch,
		DefaultResponse:          defaultResponse,
		EnableSummary:            true,
		SendResponse:             false,
		ExpectFinalDelivery:      observeTurn != nil,
		ObserveFinalDeliveryTurn: observeTurn,
		InitialSteeringMessages:  steeringMsgs,
		SkipInitialSteeringPoll:  true,
	})
}

func (al *AgentLoop) agentForRuntimeScope(
	scope runtimeSessionScope,
	agentID string,
) *AgentInstance {
	registry := al.GetRegistry()
	if registry == nil || !scope.complete() {
		return nil
	}
	if agentID = strings.TrimSpace(agentID); agentID != "" {
		agent, ok := registry.GetAgent(agentID)
		if !ok || agent == nil || normalizeRuntimeWorkspace(agent.Workspace) != scope.workspace {
			return nil
		}
		return agent
	}

	var workspaceCandidate *AgentInstance
	var resolvedCandidate *AgentInstance
	workspaceAmbiguous := false
	agentIDs := registry.ListAgentIDs()
	sort.Strings(agentIDs)
	for _, candidateID := range agentIDs {
		agent, ok := registry.GetAgent(candidateID)
		if !ok || agent == nil || normalizeRuntimeWorkspace(agent.Workspace) != scope.workspace {
			continue
		}
		if resolvedID := session.ResolveAgentID(agent.Sessions, scope.sessionKey); resolvedID != "" {
			resolved, ok := registry.GetAgent(resolvedID)
			if ok && resolved != nil && normalizeRuntimeWorkspace(resolved.Workspace) == scope.workspace {
				if resolvedCandidate != nil && resolvedCandidate.ID != resolved.ID {
					return nil
				}
				resolvedCandidate = resolved
			}
		}
		if workspaceCandidate != nil && workspaceCandidate.ID != agent.ID {
			workspaceAmbiguous = true
		} else {
			workspaceCandidate = agent
		}
	}
	if resolvedCandidate != nil {
		return resolvedCandidate
	}
	if workspaceAmbiguous {
		return nil
	}
	return workspaceCandidate
}

// Continue resumes an idle agent by dequeuing any pending steering messages
// and running them through the agent loop. This is used when the agent's last
// message was from the assistant (i.e., it has stopped processing) and the
// user has since enqueued steering messages.
//
// If no steering messages are pending, it returns an empty string.
func (al *AgentLoop) Continue(
	ctx context.Context,
	workspace, sessionKey, channel, chatID string,
) (string, error) {
	return al.continueRuntimeSession(ctx, &continuationTarget{
		Workspace: workspace, SessionKey: sessionKey,
		Channel: channel, ChatID: chatID,
	})
}

func (al *AgentLoop) continueRuntimeSession(
	ctx context.Context,
	target *continuationTarget,
) (string, error) {
	if target == nil {
		return "", fmt.Errorf("continuation target is required")
	}
	scope := newRuntimeSessionScope(target.Workspace, target.SessionKey)
	if !scope.complete() {
		return "", fmt.Errorf("continuation workspace and session are required")
	}
	agent := al.agentForRuntimeScope(scope, target.AgentID)
	if agent == nil {
		return "", fmt.Errorf("no agent available for session %q in workspace %q", scope.sessionKey, scope.workspace)
	}
	claim, claimed := al.claimRuntimeSession(
		scope,
		"pending-continue-"+scope.sessionKey+"-"+fmt.Sprintf("%d", al.turnSeq.Add(1)),
	)
	if !claimed {
		if active := al.GetActiveTurnByScope(scope.workspace, scope.sessionKey); active != nil {
			return "", fmt.Errorf("turn %s is still active for session %q", active.TurnID, scope.sessionKey)
		}
		return "", nil
	}
	defer claim.releaseIfOwned()

	if err := al.ensureHooksInitialized(ctx); err != nil {
		return "", err
	}
	if err := al.ensureMCPInitialized(ctx); err != nil {
		return "", err
	}

	steeringBatch := al.dequeueSteeringBatchForContinuation(scope)
	if len(steeringBatch.entries) == 0 {
		return "", nil
	}
	steeringMsgs := entryMessages(steeringBatch.entries)

	if tool, ok := agent.Tools.Get("message"); ok {
		if resetter, ok := tool.(interface{ ResetSentInRound(sessionKey string) }); ok {
			resetter.ResetSentInRound(scope.sessionKey)
		}
	}

	var sessionScope *session.SessionScope
	if metaStore, ok := agent.Sessions.(session.MetadataAwareSessionStore); ok {
		sessionScope = metaStore.GetSessionScope(scope.sessionKey)
	}
	routeSessionKey := scope.sessionKey
	if sessionScope != nil && strings.TrimSpace(sessionScope.RouteScopeKey) != "" {
		routeSessionKey = strings.TrimSpace(sessionScope.RouteScopeKey)
	}
	modelBinding := al.bindEffectiveModel(routeSessionKey, agent)
	defer modelBinding.Cleanup()

	response, err := al.continueWithSteeringMessages(
		ctx,
		agent,
		scope.sessionKey,
		target.Channel,
		target.ChatID,
		sessionScope,
		steeringBatch.senderID,
		modelBinding,
		steeringMsgs,
		target.ObserveFinalDeliveryTurn,
	)
	if err != nil {
		al.releaseSteeringMessages(context.Background(), steeringMsgs, err)
		return response, err
	}
	al.ackAcceptedSteeringMessages(context.Background(), steeringMsgs)
	return response, err
}

func (al *AgentLoop) InterruptGraceful(hint string) error {
	ts := al.getAnyActiveTurnState()
	if ts == nil {
		return fmt.Errorf("no active turn")
	}
	if !ts.requestGracefulInterrupt(hint) {
		return fmt.Errorf("turn %s cannot accept graceful interrupt", ts.turnID)
	}

	al.emitEvent(
		runtimeevents.KindAgentInterruptReceived,
		ts.eventMeta("InterruptGraceful", "turn.interrupt.received"),
		InterruptReceivedPayload{
			Kind:    InterruptKindGraceful,
			HintLen: len(hint),
		},
	)

	return nil
}

// InterruptHard aborts an arbitrary active turn. In parallel mode this may
// target the wrong session. Prefer HardAbort(sessionKey) instead.
//
// Deprecated: Use HardAbort(sessionKey) for session-safe aborts.
func (al *AgentLoop) InterruptHard() error {
	ts := al.getAnyActiveTurnState()
	if ts == nil {
		return fmt.Errorf("no active turn")
	}
	if strings.HasPrefix(ts.turnID, "pending-") {
		return fmt.Errorf("turn is still initializing for session %s", ts.sessionKey)
	}
	if !ts.requestHardAbort() {
		return fmt.Errorf("turn %s is already aborting", ts.turnID)
	}

	al.emitEvent(
		runtimeevents.KindAgentInterruptReceived,
		ts.eventMeta("InterruptHard", "turn.interrupt.received"),
		InterruptReceivedPayload{
			Kind: InterruptKindHard,
		},
	)

	return nil
}

// ====================== SubTurn Result Polling ======================

// dequeuePendingSubTurnResults polls the SubTurn result channel for the given
// session and returns all available results without blocking.
// Returns nil if no active turn state exists for this session.
func (al *AgentLoop) dequeuePendingSubTurnResults(sessionKey string) []*tools.ToolResult {
	ts, ambiguous := al.uniqueActiveTurnForSession(sessionKey)
	if ts == nil || ambiguous {
		return nil
	}

	var results []*tools.ToolResult
	for {
		result, ok := ts.dequeuePendingResult()
		if !ok {
			return results
		}
		if result != nil {
			results = append(results, result)
		}
	}
}

// ====================== Hard Abort ======================

// HardAbort immediately cancels the running agent loop for the given session,
// cascading the cancellation to all child SubTurns. This is a destructive operation
// that terminates execution without waiting for graceful cleanup.
//
// Use this when the user explicitly requests immediate termination (e.g., "stop now", "abort").
// For graceful interruption that allows the agent to finish the current tool and summarize,
// use Steer() instead.
func (al *AgentLoop) HardAbort(sessionKey string) error {
	ts, ambiguous := al.uniqueActiveTurnForSession(sessionKey)
	if ambiguous {
		return fmt.Errorf("session %s is active in multiple workspaces", sessionKey)
	}
	if ts == nil {
		return fmt.Errorf("no active turn state found for session %s", sessionKey)
	}
	return al.hardAbortScope(ts.runtimeSessionScope())
}

func (al *AgentLoop) hardAbortScope(scope runtimeSessionScope) error {
	tsInterface, ok := al.activeTurnStates.Load(scope)
	if !ok {
		return fmt.Errorf("no active turn state found for session %s", scope.sessionKey)
	}
	ts, ok := tsInterface.(*turnState)
	if !ok {
		return fmt.Errorf("invalid turn state type for session %s", scope.sessionKey)
	}

	if strings.HasPrefix(ts.turnID, "pending-") {
		return fmt.Errorf("turn is still initializing for session %s", scope.sessionKey)
	}

	logger.InfoCF("agent", "Hard abort triggered", map[string]any{
		"workspace":              scope.workspace,
		"session_key":            scope.sessionKey,
		"turn_id":                ts.turnID,
		"depth":                  ts.depth,
		"initial_history_length": ts.initialHistoryLength,
	})

	// Cancel the active provider/tool turn contexts immediately so long-running
	// execution stops as soon as possible on the root turn.
	_ = ts.requestHardAbort()

	// IMPORTANT: Trigger cascading cancellation FIRST to stop all child SubTurns
	// from adding more messages to the session. This prevents race conditions
	// where rollback happens while children are still writing.
	// Use isHardAbort=true for hard abort to immediately cancel all children.
	ts.Finish(true)

	// Roll back session history to the state before the turn started.
	if ts.session != nil {
		history := ts.session.GetHistory(scope.sessionKey)
		if ts.initialHistoryLength < len(history) {
			ts.session.SetHistory(scope.sessionKey, history[:ts.initialHistoryLength])
		}
	}

	return nil
}

// ====================== Follow-Up Injection ======================

// InjectFollowUp enqueues a message to be automatically processed after the current
// turn completes. Unlike Steer(), which interrupts the current execution, InjectFollowUp
// waits for the current turn to finish naturally before processing the message.
//
// This is useful for:
// - Automated workflows that need to chain multiple turns
// - Background tasks that should run after the main task completes
// - Scheduled follow-up actions
//
// The message will be processed via Continue() when the agent becomes idle.
func (al *AgentLoop) InjectFollowUp(
	workspace, sessionKey, agentID string,
	msg providers.Message,
) error {
	// InjectFollowUp uses the same steering queue mechanism as Steer(),
	// but the semantic difference is in when it's called:
	// - Steer() is called during active execution to interrupt
	// - InjectFollowUp() is called when planning future work
	//
	// Both end up in the same queue and are processed by Continue()
	// when the agent is idle.
	return al.Steer(workspace, sessionKey, agentID, msg)
}

// ====================== API Aliases for Design Document Compatibility ======================

// InjectSteering is an alias for Steer() to match the design document naming.
// It injects a steering message into the currently running agent loop.
func (al *AgentLoop) InjectSteering(
	workspace, sessionKey, agentID string,
	msg providers.Message,
) error {
	return al.Steer(workspace, sessionKey, agentID, msg)
}
