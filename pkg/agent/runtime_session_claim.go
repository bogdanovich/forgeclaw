// PicoClaw - Ultra-lightweight personal AI agent

package agent

type runtimeSessionClaim struct {
	al            *AgentLoop
	sessionKey    string
	routeClaimKey string
	placeholder   *turnState
}

func (al *AgentLoop) claimRuntimeRouteSession(
	target *inboundDispatchTarget,
	turnID string,
) (*runtimeSessionClaim, *inboundDispatchTarget, bool) {
	routeClaimKey := target.RouteClaimKey
	if existing, loaded := al.activeRouteSessions.LoadOrStore(routeClaimKey, target); loaded {
		activeTarget, ok := existing.(*inboundDispatchTarget)
		if !ok {
			al.activeRouteSessions.CompareAndDelete(routeClaimKey, existing)
			return nil, target, false
		}
		return nil, activeTarget, false
	}
	claim, claimed := al.claimRuntimeSession(target.SessionKey, turnID)
	if !claimed {
		al.activeRouteSessions.CompareAndDelete(routeClaimKey, target)
		return nil, target, false
	}
	claim.routeClaimKey = routeClaimKey
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
	if claim.routeClaimKey != "" {
		if target, ok := claim.al.activeRouteSessions.Load(claim.routeClaimKey); ok {
			activeTarget, targetOK := target.(*inboundDispatchTarget)
			if targetOK && activeTarget.SessionKey == claim.sessionKey {
				claim.al.activeRouteSessions.CompareAndDelete(claim.routeClaimKey, target)
			}
		}
	}
}
