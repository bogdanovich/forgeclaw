package agent

import (
	"path/filepath"
	"strings"
)

// runtimeSessionScope is the process-local identity for session-owned runtime
// state. Session keys are only unique inside an agent workspace.
type runtimeSessionScope struct {
	workspace  string
	sessionKey string
}

func newRuntimeSessionScope(workspace, sessionKey string) runtimeSessionScope {
	return runtimeSessionScope{
		workspace:  normalizeRuntimeWorkspace(workspace),
		sessionKey: strings.TrimSpace(sessionKey),
	}
}

func (s runtimeSessionScope) complete() bool {
	return s.workspace != "" && s.sessionKey != ""
}

type runtimeRouteScope struct {
	workspace string
	claimKey  string
}

func newRuntimeRouteScope(workspace, claimKey string) runtimeRouteScope {
	return runtimeRouteScope{
		workspace: normalizeRuntimeWorkspace(workspace),
		claimKey:  strings.TrimSpace(claimKey),
	}
}

type runtimeSubTurnScope struct {
	workspace string
	turnID    string
}

func newRuntimeSubTurnScope(workspace, turnID string) runtimeSubTurnScope {
	return runtimeSubTurnScope{
		workspace: normalizeRuntimeWorkspace(workspace),
		turnID:    strings.TrimSpace(turnID),
	}
}

func normalizeRuntimeWorkspace(workspace string) string {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return ""
	}
	return filepath.Clean(workspace)
}

func (t *inboundDispatchTarget) runtimeSessionScope() runtimeSessionScope {
	if t == nil || t.Agent == nil {
		return runtimeSessionScope{}
	}
	return newRuntimeSessionScope(t.Agent.Workspace, t.SessionKey)
}

func (t *inboundDispatchTarget) runtimeRouteScope() runtimeRouteScope {
	if t == nil || t.Agent == nil {
		return runtimeRouteScope{}
	}
	return newRuntimeRouteScope(t.Agent.Workspace, t.RouteClaimKey)
}

func (ts *turnState) runtimeSessionScope() runtimeSessionScope {
	if ts == nil {
		return runtimeSessionScope{}
	}
	return newRuntimeSessionScope(ts.workspace, ts.sessionKey)
}
