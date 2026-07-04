// PicoClaw - Ultra-lightweight personal AI agent

package agent

type runtimeSessionClaim struct {
	al          *AgentLoop
	sessionKey  string
	placeholder *turnState
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
}
