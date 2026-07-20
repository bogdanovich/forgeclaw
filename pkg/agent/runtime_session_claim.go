// PicoClaw - Ultra-lightweight personal AI agent

package agent

type runtimeSessionClaim struct {
	al          *AgentLoop
	scope       runtimeSessionScope
	routeScope  runtimeRouteScope
	placeholder *turnState
}

func (al *AgentLoop) claimRuntimeRouteSession(
	target *inboundDispatchTarget,
	turnID string,
) (*runtimeSessionClaim, *inboundDispatchTarget, bool) {
	routeScope := target.runtimeRouteScope()
	if routeScope.workspace == "" || routeScope.claimKey == "" {
		return nil, target, false
	}
	if existing, loaded := al.activeRouteSessions.LoadOrStore(routeScope, target); loaded {
		activeTarget, ok := existing.(*inboundDispatchTarget)
		if !ok {
			al.activeRouteSessions.CompareAndDelete(routeScope, existing)
			return nil, target, false
		}
		return nil, activeTarget, false
	}
	claim, claimed := al.claimRuntimeSession(target.runtimeSessionScope(), turnID)
	if !claimed {
		al.activeRouteSessions.CompareAndDelete(routeScope, target)
		return nil, target, false
	}
	claim.routeScope = routeScope
	return claim, target, true
}

func (al *AgentLoop) claimRuntimeSession(scope runtimeSessionScope, turnID string) (*runtimeSessionClaim, bool) {
	if !scope.complete() {
		return nil, false
	}
	placeholder := &turnState{
		turnID:     turnID,
		workspace:  scope.workspace,
		sessionKey: scope.sessionKey,
		phase:      TurnPhaseSetup,
	}
	if _, loaded := al.activeTurnStates.LoadOrStore(scope, placeholder); loaded {
		return nil, false
	}
	return &runtimeSessionClaim{
		al:          al,
		scope:       scope,
		placeholder: placeholder,
	}, true
}

func (claim *runtimeSessionClaim) releaseIfOwned() {
	if claim == nil || claim.placeholder == nil || claim.al == nil {
		return
	}
	if actual, ok := claim.al.activeTurnStates.Load(claim.scope); ok && actual == claim.placeholder {
		claim.al.activeTurnStates.Delete(claim.scope)
	}
	if claim.routeScope.claimKey != "" {
		if target, ok := claim.al.activeRouteSessions.Load(claim.routeScope); ok {
			activeTarget, targetOK := target.(*inboundDispatchTarget)
			if targetOK && activeTarget.runtimeSessionScope() == claim.scope {
				claim.al.activeRouteSessions.CompareAndDelete(claim.routeScope, target)
			}
		}
	}
}
