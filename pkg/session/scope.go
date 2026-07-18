package session

import "time"

// ScopeVersionV1 is the first structured session-scope schema version.
const ScopeVersionV1 = 1

// ScopeVersionV2 adds stable route-scope and lifecycle-epoch provenance.
const ScopeVersionV2 = 2

type SessionEpoch struct {
	Strategy string    `json:"strategy"`
	ID       string    `json:"id"`
	Start    time.Time `json:"start"`
}

// SessionScope describes the semantic session partition selected for a turn.
type SessionScope struct {
	Version       int               `json:"version"`
	AgentID       string            `json:"agent_id"`
	Channel       string            `json:"channel"`
	Account       string            `json:"account"`
	Dimensions    []string          `json:"dimensions"`
	Values        map[string]string `json:"values"`
	RouteScopeKey string            `json:"route_scope_key,omitempty"`
	Epoch         *SessionEpoch     `json:"epoch,omitempty"`
}

// CloneScope returns a deep copy of scope.
func CloneScope(scope *SessionScope) *SessionScope {
	if scope == nil {
		return nil
	}
	cloned := *scope
	if len(scope.Dimensions) > 0 {
		cloned.Dimensions = append([]string(nil), scope.Dimensions...)
	}
	if len(scope.Values) > 0 {
		cloned.Values = make(map[string]string, len(scope.Values))
		for key, value := range scope.Values {
			cloned.Values[key] = value
		}
	}
	if scope.Epoch != nil {
		epoch := *scope.Epoch
		cloned.Epoch = &epoch
	}
	return &cloned
}
