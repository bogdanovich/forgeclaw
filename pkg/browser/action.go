package browser

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

type Observation struct {
	SessionID          string `json:"browser_session_id"`
	TabID              string `json:"tab_id"`
	SnapshotID         string `json:"snapshot_id"`
	SnapshotGeneration uint64 `json:"snapshot_generation"`
	URL                string `json:"url"`
	Origin             string `json:"origin"`
	Title              string `json:"title,omitempty"`
	Snapshot           string `json:"snapshot"`
}

type PrepareActionRequest struct {
	Owner              Owner
	RequestID          string
	SessionID          string
	TabID              string
	SnapshotID         string
	SnapshotGeneration uint64
	Action             Action
}

type Preparation struct {
	Action           PreparedAction
	Approval         ApprovalBinding
	RequiresApproval bool
}

func (broker *Broker) Observe(ctx context.Context, owner Owner, sessionID, tabID string) (Observation, error) {
	if err := owner.Validate(); err != nil {
		return Observation{}, err
	}
	if !validIdentifier(sessionID) || !validIdentifier(tabID) {
		return Observation{}, fmt.Errorf("%w: malformed observation identity", ErrInvalid)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	session, slot, worker, err := broker.actionSessionLocked(ctx, owner, sessionID, tabID)
	if err != nil {
		return Observation{}, err
	}
	driverObservation, err := worker.Observe(ctx)
	if err != nil {
		return Observation{}, err
	}
	if !broker.originAllowed(session, driverObservation.Origin) ||
		!broker.originNetworkAllowed(ctx, driverObservation.Origin) {
		_, finishErr := broker.finishSessionLocked(ctx, session, SessionLost, "network_denied")
		return Observation{}, errors.Join(ErrDenied, finishErr)
	}
	snapshotID, err := randomOpaqueID("snapshot")
	if err != nil {
		return Observation{}, fmt.Errorf("generate browser snapshot ID: %w", err)
	}
	refs := make(map[string]DriverElement, len(driverObservation.Elements))
	visibleSnapshot := driverObservation.Snapshot
	for _, element := range driverObservation.Elements {
		ref := stableElementRef(snapshotID, element.Target)
		refs[ref] = element
		visibleSnapshot = strings.ReplaceAll(visibleSnapshot, "[ref="+element.Target+"]", "[ref="+ref+"]")
	}
	now := broker.now().UTC().UnixNano()
	session.SnapshotGeneration++
	session.SnapshotID = snapshotID
	session.SnapshotOrigin = driverObservation.Origin
	session.Revision++
	session.UpdatedAt = timestampAtLeast(now, session.UpdatedAt)
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		return Observation{}, err
	}
	slot.refs = refs
	return Observation{
		SessionID: session.ID, TabID: session.TabID, SnapshotID: snapshotID,
		SnapshotGeneration: session.SnapshotGeneration, URL: driverObservation.URL,
		Origin: driverObservation.Origin, Title: driverObservation.Title, Snapshot: visibleSnapshot,
	}, nil
}

func (broker *Broker) PrepareAction(ctx context.Context, request PrepareActionRequest) (Preparation, error) {
	if err := request.Owner.Validate(); err != nil {
		return Preparation{}, err
	}
	if !validIdentifier(request.RequestID) || !validIdentifier(request.SessionID) ||
		!validIdentifier(request.TabID) || !validIdentifier(request.SnapshotID) ||
		request.SnapshotGeneration == 0 ||
		request.Action.Validate(broker.config.Limits.Effective().TextInputBytes) != nil {
		return Preparation{}, fmt.Errorf("%w: malformed action preparation", ErrInvalid)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	session, slot, worker, err := broker.actionSessionLocked(
		ctx, request.Owner, request.SessionID, request.TabID,
	)
	if err != nil {
		return Preparation{}, err
	}
	if session.SnapshotID != request.SnapshotID ||
		session.SnapshotGeneration != request.SnapshotGeneration {
		return Preparation{}, ErrStale
	}
	preparedID := derivedIdentifier("prepared", request.Owner, request.SessionID, request.RequestID)
	if existing, getErr := broker.store.GetPreparedAction(ctx, preparedID); getErr == nil {
		if broker.now().UTC().UnixNano() >= existing.ExpiresAt {
			return Preparation{}, ErrStale
		}
		if existing.Owner != request.Owner || existing.SessionID != request.SessionID ||
			existing.TabID != request.TabID || existing.SnapshotID != request.SnapshotID ||
			existing.SnapshotGeneration != request.SnapshotGeneration || existing.Action != request.Action {
			return Preparation{}, ErrConflict
		}
		return preparationView(existing), nil
	} else if !errors.Is(getErr, ErrNotFound) {
		return Preparation{}, getErr
	}
	prepared, err := broker.resolvePreparedActionLocked(ctx, session, slot, worker, request)
	if err != nil {
		return Preparation{}, err
	}
	prepared.ID = preparedID
	prepared.RequestID = request.RequestID
	prepared.ActionHash, err = hashPreparedAction(prepared)
	if err != nil {
		return Preparation{}, err
	}
	invocation := Invocation{
		ID:               derivedIdentifier("invocation", request.Owner, request.SessionID, request.RequestID),
		PreparedActionID: prepared.ID, SessionID: session.ID, Owner: request.Owner,
		ActionHash: prepared.ActionHash, Effect: prepared.Effect, State: InvocationPrepared,
		Revision: 1, CreatedAt: prepared.CreatedAt, UpdatedAt: prepared.CreatedAt,
		ExpiresAt: prepared.ExpiresAt,
	}
	if err = broker.store.CreatePreparation(ctx, prepared, invocation); err != nil {
		return Preparation{}, err
	}
	return preparationView(prepared), nil
}

func (broker *Broker) ExecuteAction(
	ctx context.Context,
	owner Owner,
	preparedID string,
	approval *ApprovalBinding,
) (Invocation, error) {
	if err := owner.Validate(); err != nil {
		return Invocation{}, err
	}
	if !validIdentifier(preparedID) {
		return Invocation{}, fmt.Errorf("%w: malformed prepared action ID", ErrInvalid)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	prepared, err := broker.store.GetPreparedAction(ctx, preparedID)
	if err != nil {
		return Invocation{}, err
	}
	if prepared.Owner != owner {
		return Invocation{}, ErrNotFound
	}
	invocationID := derivedIdentifier("invocation", owner, prepared.SessionID, prepared.RequestID)
	currentInvocation, err := broker.store.GetInvocation(ctx, invocationID)
	if err != nil {
		return Invocation{}, err
	}
	if currentInvocation.State.Terminal() {
		return currentInvocation, nil
	}
	if currentInvocation.State == InvocationAccepted {
		return broker.executePreparedLocked(
			ctx,
			owner,
			invocationID,
			prepared.ActionHash,
			func(context.Context) (json.RawMessage, error) {
				return nil, errors.New("accepted browser action cannot be replayed")
			},
		)
	}
	if broker.now().UTC().UnixNano() >= prepared.ExpiresAt {
		expired, completeErr := broker.completeInvocationLocked(
			ctx, currentInvocation, InvocationCanceled, nil, "invocation_expired",
		)
		return expired, errors.Join(ErrStale, completeErr)
	}
	requiresApproval := actionRequiresApproval(prepared.Effect)
	if requiresApproval && !approvalMatches(prepared, approval) {
		return Invocation{}, ErrApprovalRequired
	}
	session, slot, worker, err := broker.actionSessionLocked(
		ctx, owner, prepared.SessionID, prepared.TabID,
	)
	if err != nil {
		return Invocation{}, err
	}
	if err = broker.revalidatePreparedLocked(ctx, session, slot, worker, prepared); err != nil {
		return Invocation{}, err
	}
	if prepared.DryRun && requiresApproval {
		denied, completeErr := broker.completeInvocationLocked(
			ctx, currentInvocation, InvocationCanceled, nil, "dry_run_denied",
		)
		return denied, errors.Join(ErrDenied, completeErr)
	}
	driverAction, err := driverActionForPrepared(slot, prepared)
	if err != nil {
		return Invocation{}, err
	}
	invocation, executeErr := broker.executePreparedLocked(
		ctx,
		owner,
		invocationID,
		prepared.ActionHash,
		func(executeCtx context.Context) (json.RawMessage, error) {
			if executeErr := worker.Execute(executeCtx, driverAction); executeErr != nil {
				return nil, executeErr
			}
			return json.RawMessage(`{"status":"completed"}`), nil
		},
	)
	if invocation.State == InvocationAccepted || invocation.State.Terminal() {
		if invalidateErr := broker.invalidateSnapshotLocked(ctx, prepared.SessionID); invalidateErr != nil {
			return invocation, errors.Join(executeErr, invalidateErr)
		}
	}
	return invocation, executeErr
}

func (broker *Broker) resolvePreparedActionLocked(
	ctx context.Context,
	session Session,
	slot *workerSlot,
	worker ActionWorker,
	request PrepareActionRequest,
) (PreparedAction, error) {
	now := broker.now().UTC()
	prepared := PreparedAction{
		SessionID: session.ID, Owner: session.Owner, Target: session.Target, Profile: session.Profile,
		ControllerGeneration: session.ControllerGeneration, TabID: session.TabID,
		SnapshotID: session.SnapshotID, SnapshotGeneration: session.SnapshotGeneration,
		CurrentOrigin: session.SnapshotOrigin, Action: request.Action, DryRun: session.DryRun,
		PolicyRevision: session.PolicyRevision, CatalogRevision: worker.CatalogRevision(),
		CreatedAt: now.UnixNano(),
		ExpiresAt: now.Add(time.Duration(broker.config.Limits.Effective().PreparedSeconds) * time.Second).UnixNano(),
	}
	if !validDigest(prepared.CatalogRevision) {
		return PreparedAction{}, ErrDriverIncompatible
	}
	switch request.Action.Kind {
	case ActionNavigate:
		observation, observeErr := worker.Observe(ctx)
		if observeErr != nil {
			return PreparedAction{}, observeErr
		}
		if observation.Origin != session.SnapshotOrigin {
			return PreparedAction{}, ErrStale
		}
		normalized, err := normalizeDriverNavigationURL(request.Action.URL)
		if err != nil {
			return PreparedAction{}, err
		}
		destination, err := originFromURL(normalized)
		if err != nil || !broker.originAllowed(session, destination) ||
			!broker.originNetworkAllowed(ctx, destination) {
			return PreparedAction{}, ErrDenied
		}
		prepared.Action.URL = normalized
		prepared.DestinationOrigin = destination
		prepared.Effect = EffectNavigation
	case ActionClick, ActionFill:
		element, ok := slot.refs[request.Action.Ref]
		if !ok {
			return PreparedAction{}, ErrStale
		}
		resolved, origin, resolveErr := worker.Resolve(ctx, element.Target)
		if resolveErr != nil {
			return PreparedAction{}, resolveErr
		}
		if resolved != element || origin != session.SnapshotOrigin {
			return PreparedAction{}, ErrStale
		}
		element = resolved
		prepared.ElementRole = element.Role
		prepared.ElementName = element.Name
		if request.Action.Kind == ActionFill {
			if !editableElementRole(element.Role) {
				return PreparedAction{}, ErrDenied
			}
			prepared.Effect = EffectLocalEdit
		} else {
			prepared.Effect = classifyClickEffect(element)
		}
	default:
		return PreparedAction{}, ErrInvalid
	}
	return prepared, nil
}

func (broker *Broker) revalidatePreparedLocked(
	ctx context.Context,
	session Session,
	slot *workerSlot,
	worker ActionWorker,
	prepared PreparedAction,
) error {
	if broker.now().UTC().UnixNano() >= prepared.ExpiresAt || session.PolicyRevision != prepared.PolicyRevision ||
		session.Target != prepared.Target || session.Profile != prepared.Profile ||
		session.ControllerGeneration != prepared.ControllerGeneration || session.TabID != prepared.TabID ||
		session.SnapshotID != prepared.SnapshotID || session.SnapshotGeneration != prepared.SnapshotGeneration ||
		session.SnapshotOrigin != prepared.CurrentOrigin || worker.CatalogRevision() != prepared.CatalogRevision {
		return ErrStale
	}
	if !broker.originNetworkAllowed(ctx, prepared.CurrentOrigin) ||
		(prepared.DestinationOrigin != "" &&
			!broker.originNetworkAllowed(ctx, prepared.DestinationOrigin)) {
		_, finishErr := broker.finishSessionLocked(ctx, session, SessionLost, "network_denied")
		return errors.Join(ErrDenied, finishErr)
	}
	if prepared.Action.Kind == ActionNavigate {
		observation, err := worker.Observe(ctx)
		if err != nil {
			return err
		}
		if observation.Origin != prepared.CurrentOrigin {
			return ErrStale
		}
		return nil
	}
	element, ok := slot.refs[prepared.Action.Ref]
	if !ok {
		return ErrStale
	}
	resolved, origin, err := worker.Resolve(ctx, element.Target)
	if err != nil {
		return err
	}
	if origin != prepared.CurrentOrigin || resolved != element ||
		resolved.Role != prepared.ElementRole || resolved.Name != prepared.ElementName {
		return ErrStale
	}
	return nil
}

func (broker *Broker) actionSessionLocked(
	ctx context.Context,
	owner Owner,
	sessionID string,
	tabID string,
) (Session, *workerSlot, ActionWorker, error) {
	session, err := broker.store.GetSession(ctx, sessionID)
	if err != nil {
		return Session{}, nil, nil, err
	}
	if !session.Owner.Equal(owner) {
		return Session{}, nil, nil, ErrNotFound
	}
	if session.State != SessionReady || session.TabID != tabID {
		return Session{}, nil, nil, ErrWorkerUnavailable
	}
	if session.PolicyRevision != broker.policyRevision {
		_, finishErr := broker.finishSessionLocked(ctx, session, SessionLost, "policy_changed")
		return Session{}, nil, nil, errors.Join(ErrWorkerUnavailable, finishErr)
	}
	if broker.sessionExpired(session, broker.now().UTC()) {
		_, finishErr := broker.finishSessionLocked(ctx, session, SessionExpired, "")
		return Session{}, nil, nil, errors.Join(ErrWorkerUnavailable, finishErr)
	}
	slot := broker.slots[session.ID]
	if slot == nil {
		return Session{}, nil, nil, ErrWorkerUnavailable
	}
	worker, ok := slot.worker.(ActionWorker)
	if !ok {
		return Session{}, nil, nil, ErrDriverIncompatible
	}
	return session, slot, worker, nil
}

func (broker *Broker) invalidateSnapshotLocked(ctx context.Context, sessionID string) error {
	session, err := broker.store.GetSession(ctx, sessionID)
	if err != nil || session.State != SessionReady {
		return err
	}
	session.SnapshotID = ""
	session.SnapshotOrigin = ""
	session.Revision++
	session.UpdatedAt = timestampAtLeast(broker.now().UTC().UnixNano(), session.UpdatedAt)
	session.LastActivityAt = session.UpdatedAt
	if err = broker.store.UpdateSession(ctx, session.Revision-1, session); err != nil {
		return err
	}
	if slot := broker.slots[sessionID]; slot != nil {
		slot.refs = nil
	}
	return nil
}

func preparationView(prepared PreparedAction) Preparation {
	return Preparation{
		Action: prepared,
		Approval: ApprovalBinding{
			PreparedActionID: prepared.ID, ActionHash: prepared.ActionHash,
			PolicyRevision: prepared.PolicyRevision, ExpiresAt: prepared.ExpiresAt,
		},
		RequiresApproval: actionRequiresApproval(prepared.Effect),
	}
}

func approvalMatches(prepared PreparedAction, approval *ApprovalBinding) bool {
	return approval != nil && approval.PreparedActionID == prepared.ID &&
		approval.ActionHash == prepared.ActionHash && approval.PolicyRevision == prepared.PolicyRevision &&
		approval.ExpiresAt == prepared.ExpiresAt
}

func actionRequiresApproval(effect Effect) bool {
	return effect == EffectExternalCommit || effect == EffectUnknown
}

func classifyClickEffect(element DriverElement) Effect {
	switch element.Role {
	case "button":
		return EffectExternalCommit
	default:
		// Accessibility role alone cannot prove that a click is a side-effect-free
		// navigation. A later adapter may lower this only after resolving a plain
		// destination with no submit or script semantics.
		return EffectUnknown
	}
}

func editableElementRole(role string) bool {
	return role == "textbox" || role == "searchbox" || role == "combobox"
}

func driverActionForPrepared(slot *workerSlot, prepared PreparedAction) (DriverAction, error) {
	switch prepared.Action.Kind {
	case ActionNavigate:
		return DriverAction{Kind: DriverNavigate, URL: prepared.Action.URL}, nil
	case ActionClick, ActionFill:
		element, ok := slot.refs[prepared.Action.Ref]
		if !ok {
			return DriverAction{}, ErrStale
		}
		kind := DriverClick
		if prepared.Action.Kind == ActionFill {
			kind = DriverFill
		}
		return DriverAction{
			Kind: kind, Target: element.Target, Element: element.Name, Value: prepared.Action.Value,
		}, nil
	default:
		return DriverAction{}, ErrInvalid
	}
}

func (broker *Broker) originAllowed(session Session, origin string) bool {
	target, ok := broker.config.Targets[session.Target]
	if !ok {
		return false
	}
	profile, ok := target.Profiles[session.Profile]
	if !ok {
		return false
	}
	for _, allowed := range profile.AllowedOrigins {
		normalized, err := config.NormalizeBrowserOrigin(allowed)
		if err == nil && normalized == origin {
			return true
		}
	}
	return false
}

func (broker *Broker) originNetworkAllowed(ctx context.Context, origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Hostname() == "" || broker.lookupIP == nil {
		return false
	}
	host := parsed.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		return publicBrowserIP(ip)
	}
	addresses, err := broker.lookupIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 || len(addresses) > 32 {
		return false
	}
	for _, address := range addresses {
		if !publicBrowserIP(address) {
			return false
		}
	}
	return true
}

func publicBrowserIP(ip net.IP) bool {
	return ip != nil && !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified()
}

func originFromURL(raw string) (string, error) {
	parsed, err := normalizeDriverNavigationURL(raw)
	if err != nil {
		return "", err
	}
	_, origin, err := sanitizeObservedURL(parsed)
	return origin, err
}

func stableElementRef(snapshotID, target string) string {
	digest := sha256.Sum256([]byte(snapshotID + "\x00" + target))
	return "ref_" + hex.EncodeToString(digest[:16])
}

func derivedIdentifier(prefix string, owner Owner, values ...string) string {
	payload := strings.Join([]string{
		owner.ActorID, owner.AgentID, owner.SessionKey, owner.ExecutionID,
	}, "\x00") + "\x00" + strings.Join(values, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return prefix + "_" + hex.EncodeToString(digest[:16])
}

func hashPreparedAction(prepared PreparedAction) (string, error) {
	prepared.ActionHash = ""
	encoded, err := json.Marshal(prepared)
	if err != nil {
		return "", fmt.Errorf("encode prepared browser action: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
