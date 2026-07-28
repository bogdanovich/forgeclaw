package companion

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	MaxNodeTerminalSessions          = 8
	MaxTerminalMetadataRecords       = 256
	DefaultTerminalAttachDeadline    = 30 * time.Second
	DefaultTerminalMetadataRetention = 30 * 24 * time.Hour

	TerminalSessionPendingAttach = "pending_attach"
	TerminalSessionLive          = "live"
	TerminalSessionClosing       = "closing"
	TerminalSessionClosed        = "closed"
	TerminalSessionUnknown       = "unknown"

	TerminalCloseAttachTimeout = "attach_timeout"
)

var (
	ErrTerminalNotFound        = errors.New("terminal session not found")
	ErrTerminalOwnerMismatch   = errors.New("terminal session owner mismatch")
	ErrTerminalAlreadyAttached = errors.New("terminal session was already attached")
	ErrTerminalOpenConflict    = errors.New("terminal open conflicts with existing session")
)

type terminalBrokerOpener interface {
	openTerminal(
		context.Context,
		TerminalBrokerOpenRequest,
	) (terminalBrokerSession, TerminalBrokerEvent, error)
}

type terminalBrokerSession interface {
	ID() string
	Send(context.Context, TerminalBrokerControl) error
	Receive(context.Context) (TerminalBrokerEvent, error)
	Close() error
}

type terminalSession struct {
	mu              sync.Mutex
	controlMu       sync.Mutex
	planHash        string
	openID          string
	idempotencyKey  string
	metadata        nodes.TerminalMetadata
	terminal        terminalBrokerSession
	sessionCancel   context.CancelFunc
	attached        bool
	attachmentDone  chan struct{}
	attachmentOnce  sync.Once
	events          chan TerminalBrokerEvent
	readerStarted   bool
	closing         bool
	closeDispatched bool
	highestSequence uint64
	terminalDone    chan struct{}
}

type TerminalCoordinator struct {
	nodeID        nodes.ID
	catalogHash   string
	authorityHash string
	snapshot      ShellBrokerSnapshot
	profile       ShellBrokerProfile
	broker        terminalBrokerOpener
	now           func() time.Time
	attachTimeout time.Duration

	mu          sync.Mutex
	byID        map[string]*terminalSession
	byOpenID    map[string]*terminalSession
	failedOpens map[string]failedTerminalOpen
	opening     map[string]*terminalOpenReservation
	closed      bool
}

type failedTerminalOpen struct {
	planHash       string
	idempotencyKey string
	expiresAt      int64
}

type terminalOpenReservation struct {
	planHash       string
	idempotencyKey string
	done           chan struct{}
	cancel         context.CancelFunc
	metadata       nodes.TerminalMetadata
	err            error
}

type TerminalAttachment struct {
	coordinator *TerminalCoordinator
	session     *terminalSession
	owner       nodes.TerminalOwner
	closeOnce   sync.Once
}

func NewTerminalCoordinator(
	nodeID nodes.ID,
	catalog nodes.CapabilityCatalog,
	snapshot ShellBrokerSnapshot,
	broker terminalBrokerOpener,
) (*TerminalCoordinator, error) {
	if err := nodeID.Validate(); err != nil {
		return nil, err
	}
	if broker == nil {
		return nil, errors.New("terminal broker is required")
	}
	normalized, err := normalizeShellBrokerSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	catalogHash, err := catalog.Hash()
	if err != nil {
		return nil, err
	}
	profile := normalized.Profiles[0]
	authorityHash, err := shellBrokerAuthorityDigest(normalized, profile)
	if err != nil {
		return nil, err
	}
	return &TerminalCoordinator{
		nodeID:        nodeID,
		catalogHash:   catalogHash,
		authorityHash: authorityHash,
		snapshot:      normalized,
		profile:       profile,
		broker:        broker,
		now:           time.Now,
		attachTimeout: DefaultTerminalAttachDeadline,
		byID:          make(map[string]*terminalSession),
		byOpenID:      make(map[string]*terminalSession),
		failedOpens:   make(map[string]failedTerminalOpen),
		opening:       make(map[string]*terminalOpenReservation),
	}, nil
}

func (coordinator *TerminalCoordinator) Open(
	ctx context.Context,
	plan nodes.TerminalOpenPlan,
) (nodes.TerminalMetadata, error) {
	if err := coordinator.authorizeOpen(plan); err != nil {
		return nodes.TerminalMetadata{}, err
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nodes.TerminalMetadata{}, errors.New("terminal coordinator is closed")
	}
	coordinator.pruneLocked(coordinator.now())
	if failed, exists := coordinator.failedOpens[plan.OpenID]; exists {
		coordinator.mu.Unlock()
		if failed.idempotencyKey != plan.IdempotencyKey ||
			subtle.ConstantTimeCompare([]byte(failed.planHash), []byte(plan.PlanHash)) != 1 {
			return nodes.TerminalMetadata{}, ErrTerminalOpenConflict
		}
		return nodes.TerminalMetadata{}, fmt.Errorf(
			"%w: previous terminal open did not return a session",
			ErrTerminalOutcomeUnknown,
		)
	}
	if reservation := coordinator.opening[plan.OpenID]; reservation != nil {
		if reservation.idempotencyKey != plan.IdempotencyKey ||
			subtle.ConstantTimeCompare([]byte(reservation.planHash), []byte(plan.PlanHash)) != 1 {
			coordinator.mu.Unlock()
			return nodes.TerminalMetadata{}, ErrTerminalOpenConflict
		}
		coordinator.mu.Unlock()
		select {
		case <-ctx.Done():
			return nodes.TerminalMetadata{}, ctx.Err()
		case <-reservation.done:
			return reservation.metadata, reservation.err
		}
	}
	if existing := coordinator.byOpenID[plan.OpenID]; existing != nil {
		coordinator.mu.Unlock()
		existing.mu.Lock()
		defer existing.mu.Unlock()
		if existing.idempotencyKey != plan.IdempotencyKey ||
			subtle.ConstantTimeCompare([]byte(existing.planHash), []byte(plan.PlanHash)) != 1 {
			return nodes.TerminalMetadata{}, ErrTerminalOpenConflict
		}
		return existing.metadata, nil
	}
	if len(coordinator.byID)+len(coordinator.opening)+len(coordinator.failedOpens) >=
		MaxTerminalMetadataRecords {
		coordinator.mu.Unlock()
		return nodes.TerminalMetadata{}, errors.New("node terminal metadata limit reached")
	}
	if coordinator.activeSessionsLocked()+len(coordinator.opening) >= MaxNodeTerminalSessions {
		coordinator.mu.Unlock()
		return nodes.TerminalMetadata{}, errors.New("node terminal session limit reached")
	}
	openWindow := time.Unix(plan.ExpiresAt, 0).Sub(coordinator.now())
	openCtx, cancelOpen := context.WithTimeout(ctx, openWindow)
	sessionCtx, cancelSession := context.WithCancel(context.Background())
	stopOpenCancellation := context.AfterFunc(openCtx, cancelSession)
	reservation := &terminalOpenReservation{
		planHash:       plan.PlanHash,
		idempotencyKey: plan.IdempotencyKey,
		done:           make(chan struct{}),
		cancel:         cancelOpen,
	}
	coordinator.opening[plan.OpenID] = reservation
	coordinator.mu.Unlock()
	terminal, opened, err := coordinator.broker.openTerminal(sessionCtx, TerminalBrokerOpenRequest{
		OpenID:          plan.OpenID,
		PlanHash:        plan.PlanHash,
		Profile:         coordinator.profile.Alias,
		ProfileRevision: coordinator.profile.Revision,
		WorkingScope:    plan.WorkingScope,
		Environment:     map[string]string{},
		Columns:         plan.Columns,
		Rows:            plan.Rows,
		IdleSeconds:     coordinator.profile.TerminalIdleSeconds,
		LifetimeSeconds: coordinator.profile.TerminalLifetimeSeconds,
		BufferBytes:     coordinator.profile.TerminalBufferBytes,
	})
	openCancellationStopped := stopOpenCancellation()
	cancelOpen()
	if err != nil {
		cancelSession()
		coordinator.mu.Lock()
		delete(coordinator.opening, plan.OpenID)
		coordinator.failedOpens[plan.OpenID] = failedTerminalOpen{
			planHash:       plan.PlanHash,
			idempotencyKey: plan.IdempotencyKey,
			expiresAt:      plan.ExpiresAt,
		}
		reservation.err = err
		close(reservation.done)
		coordinator.mu.Unlock()
		return nodes.TerminalMetadata{}, err
	}
	if !openCancellationStopped || sessionCtx.Err() != nil {
		cancelSession()
		_ = terminal.Close()
		err = context.Cause(openCtx)
		if err == nil {
			err = context.Canceled
		}
		coordinator.mu.Lock()
		delete(coordinator.opening, plan.OpenID)
		coordinator.failedOpens[plan.OpenID] = failedTerminalOpen{
			planHash:       plan.PlanHash,
			idempotencyKey: plan.IdempotencyKey,
			expiresAt:      plan.ExpiresAt,
		}
		reservation.err = err
		close(reservation.done)
		coordinator.mu.Unlock()
		return nodes.TerminalMetadata{}, err
	}
	if terminal == nil || opened.validate() != nil ||
		opened.Type != TerminalEventOpened ||
		terminal.ID() != opened.TerminalID {
		cancelSession()
		if terminal != nil {
			_ = terminal.Close()
		}
		err = fmt.Errorf("%w: broker returned an invalid terminal", ErrTerminalOutcomeUnknown)
		coordinator.mu.Lock()
		delete(coordinator.opening, plan.OpenID)
		coordinator.failedOpens[plan.OpenID] = failedTerminalOpen{
			planHash:       plan.PlanHash,
			idempotencyKey: plan.IdempotencyKey,
			expiresAt:      plan.ExpiresAt,
		}
		reservation.err = err
		close(reservation.done)
		coordinator.mu.Unlock()
		return nodes.TerminalMetadata{}, err
	}
	session := &terminalSession{
		planHash:       plan.PlanHash,
		openID:         plan.OpenID,
		idempotencyKey: plan.IdempotencyKey,
		metadata: nodes.TerminalMetadata{
			TerminalID: opened.TerminalID,
			Owner:      plan.Owner,
			State:      TerminalSessionPendingAttach,
			StartedAt:  opened.StartedAt,
		},
		terminal:       terminal,
		sessionCancel:  cancelSession,
		attachmentDone: make(chan struct{}),
		events:         make(chan TerminalBrokerEvent),
		terminalDone:   make(chan struct{}),
	}
	coordinator.mu.Lock()
	delete(coordinator.opening, plan.OpenID)
	if coordinator.closed || coordinator.byID[opened.TerminalID] != nil {
		coordinator.failedOpens[plan.OpenID] = failedTerminalOpen{
			planHash:       plan.PlanHash,
			idempotencyKey: plan.IdempotencyKey,
			expiresAt:      plan.ExpiresAt,
		}
		reservation.err = ErrTerminalOpenConflict
		close(reservation.done)
		coordinator.mu.Unlock()
		cancelSession()
		_ = terminal.Close()
		return nodes.TerminalMetadata{}, ErrTerminalOpenConflict
	}
	coordinator.byOpenID[plan.OpenID] = session
	coordinator.byID[opened.TerminalID] = session
	reservation.metadata = session.metadata
	close(reservation.done)
	coordinator.mu.Unlock()
	go coordinator.expireUnattached(session)
	return session.metadata, nil
}

func (coordinator *TerminalCoordinator) Attach(
	request nodes.TerminalSessionRequest,
) (*TerminalAttachment, error) {
	session, err := coordinator.ownedSession(request)
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	if session.metadata.State != TerminalSessionPendingAttach {
		session.mu.Unlock()
		return nil, ErrTerminalAlreadyAttached
	}
	if session.attached {
		session.mu.Unlock()
		return nil, ErrTerminalAlreadyAttached
	}
	session.attached = true
	session.metadata.State = TerminalSessionLive
	coordinator.startReaderLocked(session)
	session.mu.Unlock()
	return &TerminalAttachment{
		coordinator: coordinator,
		session:     session,
		owner:       request.Owner,
	}, nil
}

func (attachment *TerminalAttachment) Events() <-chan TerminalBrokerEvent {
	if attachment == nil || attachment.session == nil {
		closed := make(chan TerminalBrokerEvent)
		close(closed)
		return closed
	}
	return attachment.session.events
}

func (attachment *TerminalAttachment) Send(
	ctx context.Context,
	request nodes.TerminalControlRequest,
) error {
	if attachment == nil || attachment.session == nil {
		return ErrTerminalNotFound
	}
	if err := request.TerminalSessionRequest.Validate(); err != nil {
		return err
	}
	if !terminalOwnersEqual(request.Owner, attachment.owner) ||
		request.TerminalID != attachment.session.metadata.TerminalID {
		return ErrTerminalOwnerMismatch
	}
	control := TerminalBrokerControl{
		Version:        AuthorityBrokerProtocolVersion,
		Sequence:       request.Sequence,
		IdempotencyKey: request.IdempotencyKey,
		InputBase64:    request.InputBase64,
		Signal:         request.Signal,
		Close:          request.Close,
	}
	if request.Columns != 0 || request.Rows != 0 {
		control.Resize = &TerminalSize{Columns: request.Columns, Rows: request.Rows}
	}
	if _, err := control.validate(); err != nil {
		return err
	}
	attachment.session.controlMu.Lock()
	defer attachment.session.controlMu.Unlock()
	attachment.session.mu.Lock()
	if attachment.session.metadata.State != TerminalSessionLive ||
		attachment.session.closing {
		attachment.session.mu.Unlock()
		return ErrTerminalNotFound
	}
	if request.Sequence > attachment.session.highestSequence+1 {
		attachment.session.mu.Unlock()
		return fmt.Errorf("%w: terminal control sequence has a gap", nodes.ErrInvalidTerminal)
	}
	if request.Sequence == attachment.session.highestSequence+1 {
		attachment.session.highestSequence = request.Sequence
	}
	if request.Close {
		attachment.session.closing = true
		attachment.session.metadata.State = TerminalSessionClosing
		attachment.session.metadata.Reason = TerminalCloseRequested
	}
	attachment.session.mu.Unlock()
	if err := attachment.session.terminal.Send(ctx, control); err != nil {
		_ = attachment.session.terminal.Close()
		attachment.coordinator.finishUnknown(attachment.session)
		return err
	}
	if request.Close {
		attachment.session.mu.Lock()
		attachment.session.closeDispatched = true
		attachment.session.mu.Unlock()
	}
	return nil
}

func (attachment *TerminalAttachment) Close() error {
	if attachment == nil || attachment.session == nil {
		return nil
	}
	var closeErr error
	attachment.closeOnce.Do(func() {
		attachment.session.attachmentOnce.Do(func() {
			close(attachment.session.attachmentDone)
		})
		closeErr = attachment.coordinator.terminate(
			attachment.session,
			TerminalCloseDisconnected,
		)
	})
	return closeErr
}

func (coordinator *TerminalCoordinator) Status(
	request nodes.TerminalSessionRequest,
) (nodes.TerminalMetadata, error) {
	session, err := coordinator.ownedSession(request)
	if err != nil {
		return nodes.TerminalMetadata{}, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.metadata, nil
}

func (coordinator *TerminalCoordinator) Close() error {
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nil
	}
	coordinator.closed = true
	openings := coordinator.openingsLocked()
	sessions := make([]*terminalSession, 0, len(coordinator.byID))
	for _, session := range coordinator.byID {
		sessions = append(sessions, session)
	}
	coordinator.mu.Unlock()
	openErr := cancelAndWaitTerminalOpens(openings)
	return errors.Join(
		openErr,
		coordinator.terminateSessions(sessions, TerminalCloseDisconnected),
	)
}

// Disconnect ends every live terminal owned by the lost authenticated gateway
// generation while keeping the coordinator available for a later generation.
func (coordinator *TerminalCoordinator) Disconnect() error {
	coordinator.mu.Lock()
	sessions := make([]*terminalSession, 0, len(coordinator.byID))
	for _, session := range coordinator.byID {
		sessions = append(sessions, session)
	}
	openings := coordinator.openingsLocked()
	coordinator.mu.Unlock()
	openErr := cancelAndWaitTerminalOpens(openings)
	return errors.Join(
		openErr,
		coordinator.terminateSessions(sessions, TerminalCloseDisconnected),
	)
}

func (coordinator *TerminalCoordinator) authorizeOpen(plan nodes.TerminalOpenPlan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	now := coordinator.now()
	if plan.NodeID != coordinator.nodeID ||
		plan.Owner.Profile != coordinator.profile.Alias ||
		plan.CatalogHash != coordinator.catalogHash ||
		plan.AuthorityDigest != coordinator.authorityHash ||
		!slices.Contains(coordinator.profile.WorkingScopes, plan.WorkingScope) ||
		now.Unix() < plan.PreparedAt ||
		now.Unix() >= plan.ExpiresAt {
		return fmt.Errorf("%w: terminal authority is stale or denied", nodes.ErrCommandDenied)
	}
	return nil
}

func (coordinator *TerminalCoordinator) ownedSession(
	request nodes.TerminalSessionRequest,
) (*terminalSession, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	coordinator.mu.Lock()
	session := coordinator.byID[request.TerminalID]
	coordinator.mu.Unlock()
	if session == nil {
		return nil, ErrTerminalNotFound
	}
	session.mu.Lock()
	equal := terminalOwnersEqual(session.metadata.Owner, request.Owner)
	session.mu.Unlock()
	if !equal {
		return nil, ErrTerminalOwnerMismatch
	}
	return session, nil
}

func (coordinator *TerminalCoordinator) expireUnattached(session *terminalSession) {
	timer := time.NewTimer(coordinator.attachTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		session.mu.Lock()
		pending := session.metadata.State == TerminalSessionPendingAttach && !session.attached
		if pending {
			session.closing = true
			session.metadata.State = TerminalSessionClosing
			session.metadata.Reason = TerminalCloseAttachTimeout
			coordinator.startReaderLocked(session)
		}
		session.mu.Unlock()
		if pending {
			_ = coordinator.terminate(session, TerminalCloseAttachTimeout)
		}
	case <-session.terminalDone:
	}
}

func (coordinator *TerminalCoordinator) startReaderLocked(session *terminalSession) {
	if session.readerStarted {
		return
	}
	session.readerStarted = true
	go coordinator.readEvents(session)
}

func (coordinator *TerminalCoordinator) readEvents(session *terminalSession) {
	defer session.sessionCancel()
	defer close(session.events)
	defer close(session.terminalDone)
	for {
		event, err := session.terminal.Receive(context.Background())
		if err != nil {
			coordinator.finishUnknown(session)
			return
		}
		session.mu.Lock()
		attached := session.attached
		if event.AcceptedSequence > session.highestSequence {
			session.highestSequence = event.AcceptedSequence
		}
		switch event.Type {
		case TerminalEventClosed:
			coordinator.finishLocked(session, event, TerminalSessionClosed)
		case TerminalEventUnknown:
			coordinator.finishLocked(session, event, TerminalSessionUnknown)
		}
		terminal := event.Type == TerminalEventClosed || event.Type == TerminalEventUnknown
		session.mu.Unlock()
		if attached {
			select {
			case session.events <- event:
			case <-session.attachmentDone:
			}
		}
		if terminal {
			return
		}
	}
}

func (coordinator *TerminalCoordinator) openingsLocked() []*terminalOpenReservation {
	openings := make([]*terminalOpenReservation, 0, len(coordinator.opening))
	for _, reservation := range coordinator.opening {
		openings = append(openings, reservation)
	}
	return openings
}

func cancelAndWaitTerminalOpens(openings []*terminalOpenReservation) error {
	for _, reservation := range openings {
		reservation.cancel()
	}
	if len(openings) == 0 {
		return nil
	}
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for _, reservation := range openings {
		select {
		case <-reservation.done:
		case <-timeout.C:
			return errors.New("terminal open cancellation was not confirmed")
		}
	}
	return nil
}

func (coordinator *TerminalCoordinator) terminate(
	session *terminalSession,
	reason string,
) error {
	session.attachmentOnce.Do(func() {
		close(session.attachmentDone)
	})
	session.controlMu.Lock()
	session.mu.Lock()
	if session.metadata.State == TerminalSessionClosed ||
		session.metadata.State == TerminalSessionUnknown {
		session.mu.Unlock()
		session.controlMu.Unlock()
		return nil
	}
	coordinator.startReaderLocked(session)
	alreadyDispatched := session.closeDispatched
	if !session.closing {
		session.closing = true
		session.metadata.State = TerminalSessionClosing
		session.metadata.Reason = reason
	}
	if !alreadyDispatched {
		session.highestSequence++
	}
	sequence := session.highestSequence
	session.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !alreadyDispatched {
		err := session.terminal.Send(ctx, TerminalBrokerControl{
			Version:        AuthorityBrokerProtocolVersion,
			Sequence:       sequence,
			IdempotencyKey: fmt.Sprintf("coordinator-%s-%d", reason, sequence),
			Close:          true,
		})
		if err != nil {
			session.controlMu.Unlock()
			_ = session.terminal.Close()
			coordinator.finishUnknown(session)
			return err
		}
		session.mu.Lock()
		session.closeDispatched = true
		session.mu.Unlock()
	}
	session.controlMu.Unlock()
	select {
	case <-session.terminalDone:
		session.mu.Lock()
		if session.metadata.Reason == TerminalCloseRequested {
			session.metadata.Reason = reason
		}
		session.mu.Unlock()
		return nil
	case <-ctx.Done():
		_ = session.terminal.Close()
		coordinator.finishUnknown(session)
		return ctx.Err()
	}
}

func (coordinator *TerminalCoordinator) terminateSessions(
	sessions []*terminalSession,
	reason string,
) error {
	results := make(chan error, len(sessions))
	for _, session := range sessions {
		go func() {
			results <- coordinator.terminate(session, reason)
		}()
	}
	var result error
	for range sessions {
		result = errors.Join(result, <-results)
	}
	return result
}

func (coordinator *TerminalCoordinator) finishUnknown(session *terminalSession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.metadata.State == TerminalSessionClosed ||
		session.metadata.State == TerminalSessionUnknown {
		return
	}
	session.metadata.State = TerminalSessionUnknown
	session.metadata.Reason = "transport_unknown"
	session.metadata.CompletedAt = coordinator.now().Unix()
	session.metadata.TerminationConfirmed = false
}

func (coordinator *TerminalCoordinator) finishLocked(
	session *terminalSession,
	event TerminalBrokerEvent,
	state string,
) {
	session.metadata.State = state
	session.metadata.Reason = event.Reason
	session.metadata.CompletedAt = event.CompletedAt
	session.metadata.ExitCode = event.ExitCode
	session.metadata.Signal = event.Signal
	session.metadata.TerminationConfirmed = event.TerminationConfirmed
}

func terminalOwnersEqual(left, right nodes.TerminalOwner) bool {
	return left == right
}

func (coordinator *TerminalCoordinator) activeSessionsLocked() int {
	active := 0
	for _, session := range coordinator.byID {
		session.mu.Lock()
		state := session.metadata.State
		session.mu.Unlock()
		if state == TerminalSessionPendingAttach || state == TerminalSessionLive {
			active++
		}
	}
	return active
}

func (coordinator *TerminalCoordinator) pruneLocked(now time.Time) {
	for openID, failed := range coordinator.failedOpens {
		if now.Unix() >= failed.expiresAt {
			delete(coordinator.failedOpens, openID)
		}
	}
	cutoff := now.Add(-DefaultTerminalMetadataRetention).Unix()
	type completedSession struct {
		id          string
		openID      string
		completedAt int64
	}
	completed := make([]completedSession, 0, len(coordinator.byID))
	for id, session := range coordinator.byID {
		session.mu.Lock()
		state := session.metadata.State
		completedAt := session.metadata.CompletedAt
		openID := session.openID
		session.mu.Unlock()
		if state != TerminalSessionClosed && state != TerminalSessionUnknown {
			continue
		}
		if completedAt > 0 && completedAt <= cutoff {
			delete(coordinator.byID, id)
			delete(coordinator.byOpenID, openID)
			continue
		}
		completed = append(completed, completedSession{
			id: id, openID: openID, completedAt: completedAt,
		})
	}
	if len(coordinator.byID) <= MaxTerminalMetadataRecords {
		return
	}
	slices.SortFunc(completed, func(left, right completedSession) int {
		switch {
		case left.completedAt < right.completedAt:
			return -1
		case left.completedAt > right.completedAt:
			return 1
		default:
			return 0
		}
	})
	remove := len(coordinator.byID) - MaxTerminalMetadataRecords
	for _, record := range completed {
		if remove == 0 {
			break
		}
		delete(coordinator.byID, record.id)
		delete(coordinator.byOpenID, record.openID)
		remove--
	}
}
