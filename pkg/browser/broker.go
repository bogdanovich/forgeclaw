package browser

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
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

type WorkerFactory interface {
	Open(context.Context, WorkerOpenRequest) (Worker, error)
}

type OpenRequest struct {
	Owner   Owner
	Target  string
	Profile string
}

type Broker struct {
	config         config.BrowserToolsConfig
	policyRevision string
	store          Store
	factory        WorkerFactory
	now            func() time.Time
	newID          func() (string, error)

	mu      sync.Mutex
	workers map[string]Worker
	// pendingLoss retains the bounded failure classification until both worker
	// cleanup and the durable lost transition succeed. This prevents a failed
	// cleanup or CAS from releasing the profile while a worker may still live.
	pendingLoss map[string]string
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
	return &Broker{
		config: browserConfig, policyRevision: policyRevision, store: store, factory: factory,
		now: time.Now, newID: randomID, workers: make(map[string]Worker),
		pendingLoss: make(map[string]string),
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
		ControllerGeneration: 1, Revision: 1, CreatedAt: now.UnixNano(),
		UpdatedAt: now.UnixNano(), LastActivityAt: now.UnixNano(),
		ExpiresAt: now.Add(time.Duration(limits.SessionSeconds) * time.Second).UnixNano(),
	}
	if err = broker.store.CreateSession(ctx, session); err != nil {
		return Session{}, err
	}
	worker, openErr := broker.factory.Open(ctx, WorkerOpenRequest{
		SessionID: session.ID, Target: session.Target, Profile: session.Profile,
		DryRun: session.DryRun, Limits: limits,
	})
	if openErr != nil {
		session.State = SessionLost
		session.SafeFailure = "worker_unavailable"
		session.Revision++
		session.UpdatedAt = broker.now().UTC().UnixNano()
		if updateErr := broker.store.UpdateSession(ctx, session.Revision-1, session); updateErr != nil {
			return Session{}, errors.Join(ErrWorkerUnavailable, updateErr)
		}
		return session, ErrWorkerUnavailable
	}
	session.State = SessionReady
	session.Revision++
	session.UpdatedAt = broker.now().UTC().UnixNano()
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		_ = worker.Close(context.WithoutCancel(ctx))
		return Session{}, fmt.Errorf("persist ready browser session: %w", err)
	}
	broker.workers[session.ID] = worker
	return session, nil
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
	if session.State != SessionReady {
		return session, nil
	}
	worker := broker.workers[session.ID]
	safeFailure := broker.pendingLoss[session.ID]
	if safeFailure == "" && worker == nil {
		safeFailure = "worker_lost"
	} else if safeFailure == "" {
		status, statusErr := worker.Status(ctx)
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
	broker.pendingLoss[session.ID] = safeFailure
	if worker != nil {
		if closeErr := broker.closeWorker(ctx, worker); closeErr != nil {
			return Session{}, fmt.Errorf("%w: worker cleanup failed", ErrWorkerUnavailable)
		}
	}
	session.State = SessionLost
	session.SafeFailure = safeFailure
	session.Revision++
	session.UpdatedAt = broker.now().UTC().UnixNano()
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		return Session{}, err
	}
	delete(broker.workers, session.ID)
	delete(broker.pendingLoss, session.ID)
	return session, nil
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
	if session.State != SessionClosing {
		session.State = SessionClosing
		session.Revision++
		session.UpdatedAt = broker.now().UTC().UnixNano()
		if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
			return Session{}, err
		}
	}
	worker := broker.workers[session.ID]
	if worker == nil {
		session.State = SessionLost
		session.SafeFailure = "worker_lost"
	} else if closeErr := broker.closeWorker(ctx, worker); closeErr != nil {
		return Session{}, fmt.Errorf("%w: worker cleanup failed", ErrWorkerUnavailable)
	} else {
		session.State = SessionClosed
	}
	session.Revision++
	session.UpdatedAt = broker.now().UTC().UnixNano()
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		return Session{}, err
	}
	delete(broker.workers, session.ID)
	delete(broker.pendingLoss, session.ID)
	return session, nil
}

func (broker *Broker) closeWorker(ctx context.Context, worker Worker) error {
	cleanupTimeout := time.Duration(broker.config.Limits.Effective().ActionSeconds) * time.Second
	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancelCleanup()
	return worker.Close(cleanupCtx)
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
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
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
