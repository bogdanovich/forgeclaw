package seahorse

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/tools"
)

type retrievalScope string

const (
	retrievalScopeCurrentEpoch retrievalScope = "current_epoch"
	retrievalScopeConversation retrievalScope = "conversation"
	retrievalScopeWorkspace    retrievalScope = "workspace"
)

var orderedRetrievalScopes = []retrievalScope{
	retrievalScopeCurrentEpoch,
	retrievalScopeConversation,
	retrievalScopeWorkspace,
}

func parseRetrievalScope(value any) (retrievalScope, error) {
	scope, _ := value.(string)
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return retrievalScopeCurrentEpoch, nil
	}
	switch retrievalScope(scope) {
	case retrievalScopeCurrentEpoch, retrievalScopeConversation, retrievalScopeWorkspace:
		return retrievalScope(scope), nil
	default:
		return "", fmt.Errorf(
			"invalid retrieval_scope %q; use current_epoch, conversation, or workspace",
			scope,
		)
	}
}

func (c Config) effectiveMaxRetrievalScope() (retrievalScope, error) {
	if strings.TrimSpace(c.MaxRetrievalScope) == "" {
		return retrievalScopeConversation, nil
	}
	scope, err := parseRetrievalScope(c.MaxRetrievalScope)
	if err != nil {
		return "", fmt.Errorf(
			"maxRetrievalScope %q must be current_epoch, conversation, or workspace",
			c.MaxRetrievalScope,
		)
	}
	return scope, nil
}

func (r retrievalScope) rank() int {
	for index, scope := range orderedRetrievalScopes {
		if r == scope {
			return index
		}
	}
	return -1
}

func (e *RetrievalEngine) allowedRetrievalScopes() []string {
	if e == nil {
		return []string{string(retrievalScopeCurrentEpoch)}
	}
	maxScope, err := e.config.effectiveMaxRetrievalScope()
	if err != nil {
		return []string{string(retrievalScopeCurrentEpoch)}
	}
	allowed := make([]string, 0, maxScope.rank()+1)
	for _, scope := range orderedRetrievalScopes {
		if scope.rank() > maxScope.rank() {
			break
		}
		allowed = append(allowed, string(scope))
	}
	return allowed
}

func resolveToolConversationIDs(
	ctx context.Context,
	engine *RetrievalEngine,
	scope retrievalScope,
) ([]int64, error) {
	if engine == nil || engine.store == nil {
		return nil, fmt.Errorf("retrieval engine is not initialized")
	}
	maxScope, err := engine.config.effectiveMaxRetrievalScope()
	if err != nil {
		return nil, fmt.Errorf("invalid operator retrieval policy: %w", err)
	}
	if scope.rank() > maxScope.rank() {
		return nil, fmt.Errorf(
			"retrieval scope %q exceeds operator maximum %q",
			scope,
			maxScope,
		)
	}
	sessionKey := strings.TrimSpace(tools.ToolSessionKey(ctx))
	sessionScope := tools.ToolSessionScope(ctx)
	if sessionKey != "" && sessionScope != nil && sessionScope.RouteScopeKey != "" && sessionScope.AgentID != "" {
		if err := engine.store.SetConversationProvenance(
			ctx,
			sessionKey,
			sessionScope.RouteScopeKey,
			sessionScope.AgentID,
		); err != nil {
			return nil, err
		}
	}

	switch scope {
	case retrievalScopeCurrentEpoch:
		conversationID, found, err := engine.ConversationIDForSession(ctx, sessionKey)
		if err != nil || !found {
			return nil, err
		}
		return []int64{conversationID}, nil
	case retrievalScopeConversation:
		if sessionScope == nil || sessionScope.RouteScopeKey == "" || sessionScope.AgentID == "" {
			return nil, fmt.Errorf("conversation retrieval requires trusted route-scope provenance")
		}
		return engine.store.conversationIDsForRouteScope(
			ctx,
			sessionScope.RouteScopeKey,
			sessionScope.AgentID,
		)
	case retrievalScopeWorkspace:
		if sessionScope == nil || sessionScope.AgentID == "" {
			return nil, fmt.Errorf("workspace retrieval requires trusted agent provenance")
		}
		return engine.store.conversationIDsForAgent(ctx, sessionScope.AgentID)
	default:
		return nil, fmt.Errorf("unsupported retrieval scope %q", scope)
	}
}
