package browser

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

type WorkerStatus string

const (
	WorkerReady WorkerStatus = "ready"
	WorkerLost  WorkerStatus = "lost"
)

type WorkerOpenRequest struct {
	SessionID string
	Target    string
	Profile   string
	DryRun    bool
	Limits    config.BrowserLimitsConfig
}

type Worker interface {
	Status(context.Context) (WorkerStatus, error)
	Close(context.Context) error
}

type ActionWorker interface {
	Worker
	Observe(context.Context) (DriverObservation, error)
	Resolve(context.Context, string) (DriverElement, string, error)
	Execute(context.Context, DriverAction) error
	CatalogRevision() string
}

type ScreenshotWorker interface {
	ActionWorker
	CaptureScreenshot(context.Context, int) (DriverScreenshot, error)
}

type UploadBinding struct {
	Ref, SHA256, Filename, ContentType, Path string
	Size                                     int64
}

type DriverDownload struct {
	Path, Filename, ContentType, SHA256 string
	Size                                int64
}

type TransferWorker interface {
	ActionWorker
	Upload(context.Context, DriverAction) error
	Download(context.Context, DriverAction, int64) (DriverDownload, error)
}

type DownloadSink func(context.Context, PreparedAction, DriverDownload) (json.RawMessage, error)

// DownloadRecoveryVerifier proves that the exact retained artifact for an
// accepted download was committed outside the browser ledger before restart.
type DownloadRecoveryVerifier func(context.Context, PreparedAction) (bool, error)

func (broker *Broker) PreparedAction(ctx context.Context, owner Owner, id string) (PreparedAction, error) {
	if owner.Validate() != nil || !validIdentifier(id) {
		return PreparedAction{}, ErrInvalid
	}
	prepared, err := broker.store.GetPreparedAction(ctx, id)
	if err != nil {
		return PreparedAction{}, err
	}
	if prepared.Owner != owner {
		return PreparedAction{}, ErrNotFound
	}
	return prepared, nil
}

func (broker *Broker) RecoverableDownloadPreparation(
	ctx context.Context, request PrepareActionRequest,
) (Preparation, bool, error) {
	if request.Owner.Validate() != nil || !validIdentifier(request.RequestID) ||
		!validIdentifier(request.SessionID) || !validIdentifier(request.TabID) ||
		!validIdentifier(request.SnapshotID) || request.SnapshotGeneration == 0 ||
		request.Action.Kind != ActionDownload ||
		request.Action.Validate(broker.config.Limits.Effective().TextInputBytes) != nil {
		return Preparation{}, false, ErrInvalid
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	preparedID := derivedIdentifier("prepared", request.Owner, request.SessionID, request.RequestID)
	prepared, err := broker.store.GetPreparedAction(ctx, preparedID)
	if errors.Is(err, ErrNotFound) {
		return Preparation{}, false, nil
	}
	if err != nil {
		return Preparation{}, false, err
	}
	if prepared.Owner != request.Owner || prepared.SessionID != request.SessionID ||
		prepared.TabID != request.TabID || prepared.SnapshotID != request.SnapshotID ||
		prepared.SnapshotGeneration != request.SnapshotGeneration || prepared.Action != request.Action {
		return Preparation{}, false, ErrConflict
	}
	invocationID := derivedIdentifier("invocation", request.Owner, request.SessionID, request.RequestID)
	invocation, err := broker.store.GetInvocation(ctx, invocationID)
	if err != nil {
		return Preparation{}, false, err
	}
	if invocation.State != InvocationAccepted && invocation.State != InvocationSucceeded {
		return Preparation{}, false, nil
	}
	return preparationView(prepared), true, nil
}

func (broker *Broker) RecoverAcceptedDownload(
	ctx context.Context, owner Owner, preparedID string, terminal json.RawMessage,
) (Invocation, error) {
	if owner.Validate() != nil || !validIdentifier(preparedID) || len(terminal) == 0 ||
		len(terminal) > MaxTerminalBytes {
		return Invocation{}, ErrInvalid
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	prepared, err := broker.store.GetPreparedAction(ctx, preparedID)
	if err != nil || prepared.Owner != owner || prepared.Action.Kind != ActionDownload {
		return Invocation{}, ErrNotFound
	}
	invocationID := derivedIdentifier("invocation", owner, prepared.SessionID, prepared.RequestID)
	invocation, err := broker.store.GetInvocation(ctx, invocationID)
	if err != nil {
		return Invocation{}, err
	}
	if invocation.State == InvocationSucceeded {
		return invocation, nil
	}
	if invocation.State != InvocationAccepted {
		return Invocation{}, ErrConflict
	}
	recovered, completeErr := broker.completeInvocationLocked(ctx, invocation, InvocationSucceeded, terminal, "")
	if completeErr != nil {
		return recovered, completeErr
	}
	return recovered, broker.invalidateSnapshotLocked(ctx, prepared.SessionID)
}

// WorkerOpenResult transfers exactly one lifecycle owner to the broker. Owner
// is admitted as a worker only when Open succeeds; after a failed startup it is
// retained solely so cleanup can be retried.
type WorkerOpenResult struct {
	Owner Worker
}

type WorkerFactory interface {
	Open(context.Context, WorkerOpenRequest) (WorkerOpenResult, error)
}

type OpenRequest struct {
	Owner   Owner
	Target  string
	Profile string
}

type ProfileAvailability struct {
	Status string
	Reason string
}

// InvocationExecutor crosses the driver acceptance boundary exactly once. It
// must stop driver work and return when its context is done; a caller must not
// add retries below this callback.
type InvocationExecutor func(context.Context) (json.RawMessage, error)

// workerSlot is the broker's sole owner of a live worker. A successful cleanup
// is remembered separately from durable session completion so a CAS retry never
// requires Worker.Close to be idempotent.
type workerSlot struct {
	worker          Worker
	refs            map[string]DriverElement
	inputs          map[string]string
	uploads         map[string]UploadBinding
	safeFailure     string
	cleanupComplete bool
}

type Broker struct {
	config         config.BrowserToolsConfig
	policyRevision string
	store          Store
	factory        WorkerFactory
	now            func() time.Time
	newID          func() (string, error)
	lookupIP       func(context.Context, string, string) ([]net.IP, error)
	bindingKey     []byte

	mu    sync.Mutex
	slots map[string]*workerSlot
}

func NewBroker(rootConfig *config.Config, store Store, factory WorkerFactory) (*Broker, error) {
	if rootConfig == nil {
		return nil, errors.New("browser broker requires a root config")
	}
	if err := rootConfig.ValidateBrowserConfig(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("browser broker requires a store")
	}
	if factory == nil {
		return nil, errors.New("browser broker requires a worker factory")
	}
	browserConfig := cloneBrowserConfig(rootConfig.Tools.Browser)
	policyRevision, err := browserConfig.PolicyRevision()
	if err != nil {
		return nil, err
	}
	for index, agentID := range browserConfig.Agents {
		browserConfig.Agents[index] = OpaqueAgentID(agentID)
	}
	bindingKey := make([]byte, 32)
	if _, err = rand.Read(bindingKey); err != nil {
		return nil, fmt.Errorf("generate browser action binding key: %w", err)
	}
	return &Broker{
		config: browserConfig, policyRevision: policyRevision, store: store, factory: factory,
		now: time.Now, newID: randomID, lookupIP: net.DefaultResolver.LookupIP,
		bindingKey: bindingKey, slots: make(map[string]*workerSlot),
	}, nil
}

func (broker *Broker) Open(ctx context.Context, request OpenRequest) (Session, error) {
	if err := request.Owner.Validate(); err != nil {
		return Session{}, err
	}
	_, profile, err := broker.authorize(request)
	if err != nil {
		return Session{}, err
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	id, err := broker.newID()
	if err != nil {
		return Session{}, fmt.Errorf("generate browser session ID: %w", err)
	}
	now := broker.now().UTC()
	limits := broker.config.Limits.Effective()
	session := Session{
		ID: id, Owner: request.Owner, Target: request.Target, Profile: request.Profile,
		State: SessionOpening, DryRun: profile.DryRun, PolicyRevision: broker.policyRevision,
		ControllerGeneration: 1, TabID: "tab_primary", Revision: 1, CreatedAt: now.UnixNano(),
		UpdatedAt: now.UnixNano(), LastActivityAt: now.UnixNano(),
		ExpiresAt: now.Add(time.Duration(limits.SessionSeconds) * time.Second).UnixNano(),
	}
	if err = broker.store.CreateSession(ctx, session); err != nil {
		if fileutil.IsCommittedWriteError(err) {
			current, getErr := broker.store.GetSession(context.WithoutCancel(ctx), session.ID)
			if getErr != nil {
				return Session{}, errors.Join(err, getErr)
			}
			return broker.reconcileFailedSessionMutationLocked(ctx, current, "worker_unavailable", err)
		}
		return Session{}, err
	}
	opened, openErr := broker.factory.Open(ctx, WorkerOpenRequest{
		SessionID: session.ID, Target: session.Target, Profile: session.Profile,
		DryRun: session.DryRun, Limits: limits,
	})
	if openErr != nil {
		return broker.finishFailedOpen(ctx, session, opened.Owner)
	}
	if opened.Owner == nil {
		return broker.finishFailedOpen(ctx, session, nil)
	}
	slot := &workerSlot{worker: opened.Owner}
	broker.slots[session.ID] = slot
	ready := session
	ready.State = SessionReady
	ready.Revision++
	ready.UpdatedAt = broker.now().UTC().UnixNano()
	ready.LastActivityAt = ready.UpdatedAt
	if err = broker.store.UpdateSession(ctx, ready.Revision-1, ready); err != nil {
		persistReadyErr := fmt.Errorf("persist ready browser session: %w", err)
		slot.safeFailure = "worker_unavailable"
		if fileutil.IsCommittedWriteError(err) {
			current, getErr := broker.store.GetSession(context.WithoutCancel(ctx), session.ID)
			if getErr != nil {
				return session, errors.Join(persistReadyErr, getErr, ErrWorkerUnavailable)
			}
			return broker.reconcileFailedSessionMutationLocked(ctx, current, slot.safeFailure, persistReadyErr)
		}
		if closeErr := broker.cleanupSlot(ctx, slot); closeErr != nil {
			return session, errors.Join(persistReadyErr, ErrWorkerUnavailable)
		}
		session.State = SessionLost
		clearSessionSnapshot(&session)
		session.SafeFailure = slot.safeFailure
		session.Revision++
		session.UpdatedAt = broker.now().UTC().UnixNano()
		session.LastActivityAt = session.UpdatedAt
		if updateErr := broker.store.UpdateSession(ctx, session.Revision-1, session); updateErr != nil {
			if fileutil.IsCommittedWriteError(updateErr) {
				if current, getErr := broker.store.GetSession(
					context.WithoutCancel(ctx),
					session.ID,
				); getErr == nil &&
					current.State.Terminal() {
					delete(broker.slots, session.ID)
					return current, errors.Join(persistReadyErr, updateErr, ErrWorkerUnavailable)
				}
			}
			return session, errors.Join(persistReadyErr, updateErr, ErrWorkerUnavailable)
		}
		delete(broker.slots, session.ID)
		return session, errors.Join(persistReadyErr, ErrWorkerUnavailable)
	}
	return ready, nil
}

// ProfileAvailability reports whether a configured profile can accept a new
// session without starting a worker, reconciling state, or renewing activity.
func (broker *Broker) ProfileAvailability(
	ctx context.Context,
	targetName string,
	profileName string,
) (ProfileAvailability, error) {
	target, ok := broker.config.Targets[targetName]
	if !ok || !target.Enabled {
		return ProfileAvailability{}, ErrDenied
	}
	profile, ok := target.Profiles[profileName]
	if !ok || !profile.Enabled {
		return ProfileAvailability{}, ErrDenied
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	sessions, err := broker.store.ListSessions(ctx)
	if err != nil {
		return ProfileAvailability{}, err
	}
	for _, session := range sessions {
		if session.Target != targetName || session.Profile != profileName || session.State.Terminal() {
			continue
		}
		slot := broker.slots[session.ID]
		if session.State == SessionReady && slot != nil && slot.safeFailure == "" {
			return ProfileAvailability{Status: "busy", Reason: "profile_busy"}, nil
		}
		return ProfileAvailability{Status: "degraded", Reason: "recovery_required"}, nil
	}
	return ProfileAvailability{Status: "ready"}, nil
}

// PassiveReadiness reports configured and last-observed driver readiness plus
// durable profile lease state. It does not call Worker.Status, start or close a
// worker, reconcile state, renew activity, or resolve an artifact reference.
func (broker *Broker) PassiveReadiness(
	ctx context.Context,
	targetName string,
	profileName string,
) (PassiveReadiness, error) {
	availability, err := broker.ProfileAvailability(ctx, targetName, profileName)
	if err != nil {
		return PassiveReadiness{}, err
	}
	driver := configuredDriverReadiness()
	if factory, ok := broker.factory.(readinessFactory); ok {
		driver = factory.PassiveReadiness()
	}
	result := PassiveReadiness{
		Status: driver.Status, Broker: ReadinessReady, Worker: driver.Status,
		Driver: driver.Driver, Browser: driver.Browser, Proxy: driver.Proxy,
		Compatibility: driver.Compatibility, Profile: availability,
		Code: driver.Code, Action: driver.Action, Passive: true,
	}
	if result.Code != "" {
		return result, nil
	}
	switch availability.Status {
	case ReadinessBusy:
		result.Status, result.Worker = ReadinessBusy, ReadinessReady
		result.Code, result.Action = "profile_busy", "wait_or_close_session"
	case ReadinessDegraded:
		result.Status, result.Worker = ReadinessDegraded, ReadinessDegraded
		result.Code, result.Action = "recovery_required", "close_or_recover_session"
	}
	return result, nil
}

func (broker *Broker) finishFailedOpen(
	ctx context.Context,
	session Session,
	cleanup Worker,
) (Session, error) {
	if cleanup == nil {
		session.State = SessionLost
		clearSessionSnapshot(&session)
		session.SafeFailure = "worker_unavailable"
		session.Revision++
		session.UpdatedAt = broker.now().UTC().UnixNano()
		if updateErr := broker.store.UpdateSession(ctx, session.Revision-1, session); updateErr != nil {
			if fileutil.IsCommittedWriteError(updateErr) {
				current, getErr := broker.store.GetSession(context.WithoutCancel(ctx), session.ID)
				if getErr != nil {
					return Session{}, errors.Join(ErrWorkerUnavailable, updateErr, getErr)
				}
				return broker.reconcileFailedSessionMutationLocked(ctx, current, "worker_unavailable", updateErr)
			}
			return Session{}, errors.Join(ErrWorkerUnavailable, updateErr)
		}
		return session, ErrWorkerUnavailable
	}

	slot := &workerSlot{worker: cleanup, safeFailure: "worker_unavailable"}
	broker.slots[session.ID] = slot
	closing := session
	closing.State = SessionClosing
	closing.Revision++
	closing.UpdatedAt = broker.now().UTC().UnixNano()
	if updateErr := broker.store.UpdateSession(ctx, closing.Revision-1, closing); updateErr != nil {
		if fileutil.IsCommittedWriteError(updateErr) {
			current, getErr := broker.store.GetSession(context.WithoutCancel(ctx), session.ID)
			if getErr != nil {
				return session, errors.Join(ErrWorkerUnavailable, updateErr, getErr)
			}
			return broker.reconcileFailedSessionMutationLocked(ctx, current, slot.safeFailure, updateErr)
		}
		_ = broker.cleanupSlot(ctx, slot)
		return session, errors.Join(ErrWorkerUnavailable, updateErr)
	}
	session = closing
	if closeErr := broker.cleanupSlot(ctx, slot); closeErr != nil {
		return session, ErrWorkerUnavailable
	}

	session.State = SessionLost
	clearSessionSnapshot(&session)
	session.SafeFailure = slot.safeFailure
	session.Revision++
	session.UpdatedAt = broker.now().UTC().UnixNano()
	session.LastActivityAt = session.UpdatedAt
	if updateErr := broker.store.UpdateSession(ctx, session.Revision-1, session); updateErr != nil {
		if fileutil.IsCommittedWriteError(updateErr) {
			if current, getErr := broker.store.GetSession(
				context.WithoutCancel(ctx),
				session.ID,
			); getErr == nil &&
				current.State.Terminal() {
				delete(broker.slots, session.ID)
				return current, errors.Join(ErrWorkerUnavailable, updateErr)
			}
		}
		return session, errors.Join(ErrWorkerUnavailable, updateErr)
	}
	delete(broker.slots, session.ID)
	return session, ErrWorkerUnavailable
}

func (broker *Broker) reconcileFailedSessionMutationLocked(
	ctx context.Context,
	current Session,
	safeFailure string,
	cause error,
) (Session, error) {
	completionTimeout := time.Duration(broker.config.Limits.Effective().ActionSeconds) * time.Second
	completionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completionTimeout)
	defer cancel()
	if current.State.Terminal() {
		return current, errors.Join(cause, ErrWorkerUnavailable)
	}
	finished, finishErr := broker.finishSessionLocked(completionCtx, current, SessionLost, safeFailure)
	return finished, errors.Join(cause, finishErr, ErrWorkerUnavailable)
}

func (broker *Broker) Status(ctx context.Context, owner Owner, sessionID string) (Session, error) {
	if err := owner.Validate(); err != nil {
		return Session{}, err
	}
	if !validIdentifier(sessionID) {
		return Session{}, fmt.Errorf("%w: malformed session ID", ErrInvalid)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	session, err := broker.store.GetSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	if !session.Owner.Equal(owner) {
		return Session{}, ErrNotFound
	}
	if !session.State.Terminal() && session.PolicyRevision != broker.policyRevision {
		return broker.finishSessionLocked(ctx, session, SessionLost, "policy_changed")
	}
	if !session.State.Terminal() && broker.sessionExpired(session, broker.now().UTC()) {
		return broker.finishSessionLocked(ctx, session, SessionExpired, "")
	}
	if session.State != SessionReady {
		return session, nil
	}
	slot := broker.slots[session.ID]
	var safeFailure string
	if slot == nil {
		safeFailure = "worker_lost"
	} else {
		safeFailure = slot.safeFailure
	}
	if safeFailure == "" {
		status, statusErr := slot.worker.Status(ctx)
		switch {
		case statusErr != nil && ctx.Err() != nil:
			return Session{}, ctx.Err()
		case statusErr != nil:
			safeFailure = "worker_unavailable"
		case status == WorkerLost:
			safeFailure = "worker_lost"
		case status != WorkerReady:
			safeFailure = "worker_status_invalid"
		}
	}
	if safeFailure == "" {
		return session, nil
	}
	if slot != nil {
		slot.safeFailure = safeFailure
		if closeErr := broker.cleanupSlot(ctx, slot); closeErr != nil {
			return Session{}, fmt.Errorf("%w: worker cleanup failed", ErrWorkerUnavailable)
		}
	}
	if err = broker.terminateInvocationsLocked(ctx, session.ID, safeFailure); err != nil {
		return Session{}, err
	}
	session.State = SessionLost
	clearSessionSnapshot(&session)
	session.SafeFailure = safeFailure
	session.Revision++
	session.UpdatedAt = broker.now().UTC().UnixNano()
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		if fileutil.IsCommittedWriteError(err) {
			if current, getErr := broker.store.GetSession(
				context.WithoutCancel(ctx),
				session.ID,
			); getErr == nil &&
				current.State.Terminal() {
				delete(broker.slots, session.ID)
				return current, err
			}
		}
		return Session{}, err
	}
	delete(broker.slots, session.ID)
	return session, nil
}

// Touch records activity after an admitted observe or action. Status and
// discovery deliberately do not renew the idle deadline.
func (broker *Broker) Touch(ctx context.Context, owner Owner, sessionID string) (Session, error) {
	if err := owner.Validate(); err != nil {
		return Session{}, err
	}
	if !validIdentifier(sessionID) {
		return Session{}, fmt.Errorf("%w: malformed session ID", ErrInvalid)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	session, err := broker.store.GetSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	if !session.Owner.Equal(owner) {
		return Session{}, ErrNotFound
	}
	if session.State != SessionReady || broker.slots[session.ID] == nil {
		return Session{}, ErrWorkerUnavailable
	}
	if session.PolicyRevision != broker.policyRevision {
		return broker.finishSessionLocked(ctx, session, SessionLost, "policy_changed")
	}
	now := broker.now().UTC()
	if broker.sessionExpired(session, now) {
		return broker.finishSessionLocked(ctx, session, SessionExpired, "")
	}
	session.Revision++
	session.UpdatedAt = now.UnixNano()
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		return Session{}, err
	}
	return session, nil
}

// Sweep expires sessions without treating a status check as activity.
func (broker *Broker) Sweep(ctx context.Context) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	sessions, err := broker.store.ListSessions(ctx)
	if err != nil {
		return err
	}
	now := broker.now().UTC()
	for _, session := range sessions {
		if !session.State.Terminal() &&
			(session.PolicyRevision != broker.policyRevision || broker.sessionExpired(session, now)) {
			state, failure := SessionExpired, ""
			if session.PolicyRevision != broker.policyRevision {
				state, failure = SessionLost, "policy_changed"
			}
			if _, err = broker.finishSessionLocked(ctx, session, state, failure); err != nil {
				return err
			}
		}
	}
	retention := time.Duration(broker.config.Limits.Effective().RetentionSecs) * time.Second
	if err = broker.store.PruneInvocations(ctx, now.Add(-retention).UnixNano()); err != nil {
		return err
	}
	return broker.store.PrunePreparedActions(ctx, now.Add(-retention).UnixNano())
}

// Recover reconciles state after a gateway restart. B1 workers are
// in-process, so continuity cannot be proven: live sessions become lost and
// accepted operations become unknown without dispatch, except typed downloads
// whose exact committed P2 artifact can still complete without replay.
func (broker *Broker) Recover(ctx context.Context, verifiers ...DownloadRecoveryVerifier) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	var verifier DownloadRecoveryVerifier
	if len(verifiers) > 0 {
		verifier = verifiers[0]
	}
	sessions, err := broker.store.ListSessions(ctx)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if session.State.Terminal() {
			continue
		}
		if err = broker.terminateInvocationsForRestartLocked(ctx, session.ID, verifier); err != nil {
			return err
		}
		now := timestampAtLeast(broker.now().UTC().UnixNano(), session.UpdatedAt)
		session.State = SessionLost
		clearSessionSnapshot(&session)
		session.SafeFailure = "gateway_restarted"
		session.Revision++
		session.UpdatedAt = now
		session.LastActivityAt = now
		if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
			return err
		}
	}
	return nil
}

func (broker *Broker) terminateInvocationsForRestartLocked(
	ctx context.Context,
	sessionID string,
	verifier DownloadRecoveryVerifier,
) error {
	invocations, err := broker.store.ListInvocations(ctx, sessionID)
	if err != nil {
		return err
	}
	for _, invocation := range invocations {
		if invocation.State.Terminal() {
			continue
		}
		if invocation.State == InvocationAccepted {
			prepared, preparedErr := broker.store.GetPreparedAction(ctx, invocation.PreparedActionID)
			if preparedErr == nil && prepared.Action.Kind == ActionDownload && verifier != nil {
				committed, verifyErr := verifier(ctx, prepared)
				if verifyErr != nil {
					return verifyErr
				}
				if committed {
					continue
				}
			}
			if preparedErr != nil && !errors.Is(preparedErr, ErrNotFound) {
				return preparedErr
			}
		}
		now := timestampAtLeast(broker.now().UTC().UnixNano(), invocation.UpdatedAt)
		if invocation.State == InvocationPrepared {
			invocation.State = InvocationCanceled
		} else {
			invocation.State = InvocationUnknown
		}
		invocation.SafeFailure = "gateway_restarted"
		invocation.Revision++
		invocation.UpdatedAt = now
		invocation.CompletedAt = now
		if err = broker.store.UpdateInvocation(ctx, invocation.Revision-1, invocation); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown closes every live browser session before the gateway releases the
// durable store. It is intentionally separate from Recover: a clean gateway
// stop proves that no worker survived, while restart recovery handles the
// opposite case after an unclean exit.
func (broker *Broker) Shutdown(ctx context.Context) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	sessions, err := broker.store.ListSessions(ctx)
	if err != nil {
		return err
	}
	var shutdownErr error
	for _, session := range sessions {
		if ctx.Err() != nil {
			return errors.Join(shutdownErr, ctx.Err())
		}
		if session.State.Terminal() {
			continue
		}
		if _, err = broker.finishSessionLocked(ctx, session, SessionClosed, ""); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}
	return shutdownErr
}

func (broker *Broker) Close(ctx context.Context, owner Owner, sessionID string) (Session, error) {
	if err := owner.Validate(); err != nil {
		return Session{}, err
	}
	if !validIdentifier(sessionID) {
		return Session{}, fmt.Errorf("%w: malformed session ID", ErrInvalid)
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	session, err := broker.store.GetSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	if !session.Owner.Equal(owner) {
		return Session{}, ErrNotFound
	}
	if session.State.Terminal() {
		return session, nil
	}
	return broker.finishSessionLocked(ctx, session, SessionClosed, "")
}

func (broker *Broker) finishSessionLocked(
	ctx context.Context,
	session Session,
	desired SessionState,
	safeFailure string,
) (Session, error) {
	if session.State.Terminal() {
		return session, nil
	}
	if err := broker.terminateInvocationsLocked(
		ctx,
		session.ID,
		terminalInvocationFailure(desired, safeFailure),
	); err != nil {
		return Session{}, err
	}
	if session.State != SessionClosing {
		session.State = SessionClosing
		session.Revision++
		session.UpdatedAt = broker.now().UTC().UnixNano()
		if err := broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
			return Session{}, err
		}
	}
	slot := broker.slots[session.ID]
	if slot == nil {
		desired = SessionLost
		if safeFailure == "" {
			safeFailure = "worker_lost"
		}
	} else if closeErr := broker.cleanupSlot(ctx, slot); closeErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Session{}, errors.Join(
				fmt.Errorf("%w: worker cleanup failed", ErrWorkerUnavailable),
				ctxErr,
			)
		}
		return Session{}, fmt.Errorf("%w: worker cleanup failed", ErrWorkerUnavailable)
	} else if slot.safeFailure != "" {
		desired = SessionLost
		safeFailure = slot.safeFailure
	}
	session.State = desired
	clearSessionSnapshot(&session)
	session.SafeFailure = safeFailure
	if desired != SessionLost {
		session.SafeFailure = ""
	}
	session.Revision++
	session.UpdatedAt = broker.now().UTC().UnixNano()
	session.LastActivityAt = session.UpdatedAt
	if err := broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		if fileutil.IsCommittedWriteError(err) {
			if current, getErr := broker.store.GetSession(
				context.WithoutCancel(ctx),
				session.ID,
			); getErr == nil &&
				current.State.Terminal() {
				delete(broker.slots, session.ID)
				return current, err
			}
		}
		return Session{}, err
	}
	delete(broker.slots, session.ID)
	return session, nil
}

func terminalInvocationFailure(state SessionState, safeFailure string) string {
	if safeFailure != "" {
		return safeFailure
	}
	if state == SessionExpired {
		return "session_expired"
	}
	return "session_closed"
}

func (broker *Broker) sessionExpired(session Session, now time.Time) bool {
	if now.UnixNano() >= session.ExpiresAt {
		return true
	}
	idle := time.Duration(broker.config.Limits.Effective().IdleSeconds) * time.Second
	return now.Sub(time.Unix(0, session.LastActivityAt)) >= idle
}

func (broker *Broker) terminateInvocationsLocked(ctx context.Context, sessionID, failure string) error {
	invocations, err := broker.store.ListInvocations(ctx, sessionID)
	if err != nil {
		return err
	}
	for _, invocation := range invocations {
		if invocation.State.Terminal() {
			continue
		}
		now := timestampAtLeast(broker.now().UTC().UnixNano(), invocation.UpdatedAt)
		if invocation.State == InvocationPrepared {
			invocation.State = InvocationCanceled
		} else {
			invocation.State = InvocationUnknown
		}
		invocation.SafeFailure = failure
		invocation.Revision++
		invocation.UpdatedAt = now
		invocation.CompletedAt = now
		if err = broker.store.UpdateInvocation(ctx, invocation.Revision-1, invocation); err != nil {
			return err
		}
	}
	return nil
}

// ExecutePrepared durably accepts one prepared invocation before dispatch.
// Existing terminal records are returned idempotently; an existing accepted
// record becomes unknown and is never dispatched again.
func (broker *Broker) ExecutePrepared(
	ctx context.Context,
	owner Owner,
	invocationID string,
	actionHash string,
	execute InvocationExecutor,
) (Invocation, error) {
	if err := owner.Validate(); err != nil {
		return Invocation{}, err
	}
	if !validIdentifier(invocationID) || !validDigest(actionHash) || execute == nil {
		return Invocation{}, fmt.Errorf("%w: malformed invocation dispatch", ErrInvalid)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	return broker.executePreparedLocked(ctx, owner, invocationID, actionHash, execute)
}

func (broker *Broker) executePreparedLocked(
	ctx context.Context,
	owner Owner,
	invocationID string,
	actionHash string,
	execute InvocationExecutor,
) (Invocation, error) {
	invocation, err := broker.store.GetInvocation(ctx, invocationID)
	if err != nil {
		return Invocation{}, err
	}
	if !invocation.Owner.Equal(owner) || invocation.ActionHash != actionHash {
		return Invocation{}, ErrNotFound
	}
	if invocation.State.Terminal() {
		return invocation, nil
	}
	if invocation.State == InvocationAccepted {
		return broker.completeInvocationLocked(ctx, invocation, InvocationUnknown, nil, "worker_lost")
	}
	if ctx.Err() != nil {
		return broker.completeInvocationLocked(
			context.WithoutCancel(ctx),
			invocation,
			InvocationCanceled,
			nil,
			"canceled_before_acceptance",
		)
	}
	now := broker.now().UTC()
	if now.UnixNano() >= invocation.ExpiresAt {
		return broker.completeInvocationLocked(ctx, invocation, InvocationCanceled, nil, "invocation_expired")
	}
	session, err := broker.store.GetSession(ctx, invocation.SessionID)
	if err != nil || session.State != SessionReady || !session.Owner.Equal(owner) {
		return Invocation{}, ErrWorkerUnavailable
	}
	if session.PolicyRevision != broker.policyRevision {
		if _, finishErr := broker.finishSessionLocked(ctx, session, SessionLost, "policy_changed"); finishErr != nil {
			return Invocation{}, errors.Join(ErrWorkerUnavailable, finishErr)
		}
		return Invocation{}, ErrWorkerUnavailable
	}
	if broker.sessionExpired(session, now) {
		if _, finishErr := broker.finishSessionLocked(ctx, session, SessionExpired, ""); finishErr != nil {
			return Invocation{}, errors.Join(ErrWorkerUnavailable, finishErr)
		}
		return Invocation{}, ErrWorkerUnavailable
	}
	if broker.slots[session.ID] == nil {
		if _, finishErr := broker.finishSessionLocked(ctx, session, SessionLost, "worker_lost"); finishErr != nil {
			return Invocation{}, errors.Join(ErrWorkerUnavailable, finishErr)
		}
		return Invocation{}, ErrWorkerUnavailable
	}
	invocation.State = InvocationAccepted
	invocation.AcceptedAt = timestampAtLeast(now.UnixNano(), invocation.UpdatedAt)
	invocation.UpdatedAt = invocation.AcceptedAt
	invocation.Revision++
	if err = broker.store.UpdateInvocation(ctx, invocation.Revision-1, invocation); err != nil {
		return Invocation{}, err
	}
	executionDeadline := now.Add(time.Duration(broker.config.Limits.Effective().ActionSeconds) * time.Second)
	sessionDeadline := time.Unix(0, session.ExpiresAt)
	if sessionDeadline.Before(executionDeadline) {
		executionDeadline = sessionDeadline
	}
	idleDeadline := time.Unix(0, session.LastActivityAt).
		Add(time.Duration(broker.config.Limits.Effective().IdleSeconds) * time.Second)
	if idleDeadline.Before(executionDeadline) {
		executionDeadline = idleDeadline
	}
	executionCtx, cancelExecution := context.WithDeadline(ctx, executionDeadline)
	result, executeErr := execute(executionCtx)
	executionContextErr := executionCtx.Err()
	cancelExecution()
	completionCtx, cancelCompletion := context.WithTimeout(
		context.WithoutCancel(ctx),
		time.Duration(broker.config.Limits.Effective().ActionSeconds)*time.Second,
	)
	defer cancelCompletion()
	if executeErr != nil || executionContextErr != nil {
		return broker.completeInvocationLocked(
			completionCtx,
			invocation,
			InvocationUnknown,
			nil,
			"outcome_unknown",
		)
	}
	if len(result) == 0 || len(result) > MaxTerminalBytes || !json.Valid(result) {
		return broker.completeInvocationLocked(
			completionCtx,
			invocation,
			InvocationUnknown,
			nil,
			"result_invalid",
		)
	}
	completed, err := broker.completeInvocationLocked(
		completionCtx,
		invocation,
		InvocationSucceeded,
		result,
		"",
	)
	if err != nil {
		return completed, err
	}
	// A completed action is activity, but never extends the absolute lifetime.
	session.Revision++
	session.UpdatedAt = timestampAtLeast(broker.now().UTC().UnixNano(), session.UpdatedAt)
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(completionCtx, session.Revision-1, session); err != nil {
		return completed, err
	}
	return completed, nil
}

func (broker *Broker) completeInvocationLocked(
	ctx context.Context,
	invocation Invocation,
	state InvocationState,
	result json.RawMessage,
	failure string,
) (Invocation, error) {
	now := timestampAtLeast(broker.now().UTC().UnixNano(), invocation.UpdatedAt)
	invocation.State = state
	invocation.Revision++
	invocation.UpdatedAt = now
	invocation.CompletedAt = now
	invocation.TerminalResult = cloneBytes(result)
	invocation.SafeFailure = failure
	if state == InvocationCanceled {
		invocation.AcceptedAt = 0
	}
	if err := broker.store.UpdateInvocation(ctx, invocation.Revision-1, invocation); err != nil {
		return invocation, err
	}
	return invocation, nil
}

func timestampAtLeast(value, minimum int64) int64 {
	if value < minimum {
		return minimum
	}
	return value
}

func clearSessionSnapshot(session *Session) {
	if session == nil {
		return
	}
	session.SnapshotID = ""
	session.SnapshotOrigin = ""
}

func (broker *Broker) cleanupSlot(ctx context.Context, slot *workerSlot) error {
	if slot.cleanupComplete {
		return nil
	}
	cleanupTimeout := time.Duration(broker.config.Limits.Effective().ActionSeconds) * time.Second
	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, cleanupTimeout)
	defer cancelCleanup()
	if err := slot.worker.Close(cleanupCtx); err != nil {
		return err
	}
	slot.cleanupComplete = true
	return nil
}

func (broker *Broker) authorize(request OpenRequest) (config.BrowserTargetConfig, config.BrowserProfileConfig, error) {
	if !broker.config.Enabled || !contains(broker.config.Agents, request.Owner.AgentID) {
		return config.BrowserTargetConfig{}, config.BrowserProfileConfig{}, ErrDenied
	}
	if !validIdentifier(request.Target) || !validIdentifier(request.Profile) {
		return config.BrowserTargetConfig{}, config.BrowserProfileConfig{}, ErrDenied
	}
	target, ok := broker.config.Targets[request.Target]
	if !ok || !target.Enabled {
		return config.BrowserTargetConfig{}, config.BrowserProfileConfig{}, ErrDenied
	}
	profile, ok := target.Profiles[request.Profile]
	if !ok || !profile.Enabled {
		return config.BrowserTargetConfig{}, config.BrowserProfileConfig{}, ErrDenied
	}
	return target, profile, nil
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func randomID() (string, error) {
	return randomOpaqueID("session")
}

func randomOpaqueID(prefix string) (string, error) {
	if !validIdentifier(prefix) {
		return "", ErrInvalid
	}
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func cloneBrowserConfig(source config.BrowserToolsConfig) config.BrowserToolsConfig {
	cloned := source
	cloned.Agents = append([]string(nil), source.Agents...)
	cloned.Targets = make(map[string]config.BrowserTargetConfig, len(source.Targets))
	for targetName, target := range source.Targets {
		clonedTarget := target
		clonedTarget.Profiles = make(map[string]config.BrowserProfileConfig, len(target.Profiles))
		for profileName, profile := range target.Profiles {
			clonedProfile := profile
			clonedProfile.AllowedOrigins = append([]string(nil), profile.AllowedOrigins...)
			clonedTarget.Profiles[profileName] = clonedProfile
		}
		cloned.Targets[targetName] = clonedTarget
	}
	return cloned
}
