// PicoClaw - Ultra-lightweight personal AI agent

package agent

type runtimeSessionClaim struct {
	al            *AgentLoop
	sessionKey    string
	routeScopeKey string
	placeholder   *turnState
}

func (al *AgentLoop) claimRuntimeRouteSession(
	target *inboundDispatchTarget,
	turnID string,
) (*runtimeSessionClaim, *inboundDispatchTarget, bool) {
	routeScopeKey := target.Allocation.RouteScopeKey
	if existing, loaded := al.activeRouteSessions.LoadOrStore(routeScopeKey, target); loaded {
		activeTarget, ok := existing.(*inboundDispatchTarget)
		if !ok {
			al.activeRouteSessions.CompareAndDelete(routeScopeKey, existing)
			return nil, target, false
		}
		return nil, activeTarget, false
	}
	claim, claimed := al.claimRuntimeSession(target.SessionKey, turnID)
	if !claimed {
		al.activeRouteSessions.CompareAndDelete(routeScopeKey, target)
		return nil, target, false
	}
	claim.routeScopeKey = routeScopeKey
	return claim, target, true
}

func (al *AgentLoop) claimRuntimeSession(sessionKey, turnID string) (*runtimeSessionClaim, bool) {
	placeholder := &turnState{
		turnID: turnID,
		phase:  TurnPhaseSetup,
	}
	if _, loaded := al.activeTurnStates.LoadOrStore(sessionKey, placeholder); loaded {
		return nil, false
	}
	return &runtimeSessionClaim{
		al:          al,
		sessionKey:  sessionKey,
		placeholder: placeholder,
	}, true
}

func (claim *runtimeSessionClaim) releaseIfOwned() {
	if claim == nil || claim.placeholder == nil || claim.al == nil {
		return
	}
	if actual, ok := claim.al.activeTurnStates.Load(claim.sessionKey); ok && actual == claim.placeholder {
		claim.al.activeTurnStates.Delete(claim.sessionKey)
	}
	if claim.routeScopeKey != "" {
		if target, ok := claim.al.activeRouteSessions.Load(claim.routeScopeKey); ok {
			activeTarget, targetOK := target.(*inboundDispatchTarget)
			if targetOK && activeTarget.SessionKey == claim.sessionKey {
				claim.al.activeRouteSessions.CompareAndDelete(claim.routeScopeKey, target)
			}
		}
	}
}
