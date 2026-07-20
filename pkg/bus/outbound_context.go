package bus

import (
	"errors"
	"fmt"
	"strings"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
)

// ErrMixedTraceScopeWorkspaces rejects one physical outbound that attempts to
// correlate turns owned by different workspaces.
var ErrMixedTraceScopeWorkspaces = errors.New("outbound trace scopes span multiple workspaces")

// NewOutboundContext builds the minimal normalized addressing context required
// to deliver an outbound text message or reply.
func NewOutboundContext(channel, chatID, replyToMessageID string) InboundContext {
	return normalizeInboundContext(InboundContext{
		Channel:          strings.TrimSpace(channel),
		ChatID:           strings.TrimSpace(chatID),
		ReplyToMessageID: strings.TrimSpace(replyToMessageID),
	})
}

// NormalizeOutboundMessage ensures Context is normalized and keeps convenience
// mirrors in sync for runtime consumers.
func NormalizeOutboundMessage(msg OutboundMessage) (OutboundMessage, error) {
	msg.Channel = strings.TrimSpace(msg.Channel)
	msg.ChatID = strings.TrimSpace(msg.ChatID)
	msg.ReplyToMessageID = strings.TrimSpace(msg.ReplyToMessageID)
	if msg.Context.Channel == "" {
		msg.Context.Channel = msg.Channel
	}
	if msg.Context.ChatID == "" {
		msg.Context.ChatID = msg.ChatID
	}
	if msg.Context.ReplyToMessageID == "" {
		msg.Context.ReplyToMessageID = msg.ReplyToMessageID
	}
	msg.Context = normalizeInboundContext(msg.Context)
	if msg.Channel == "" {
		msg.Channel = msg.Context.Channel
	}
	if msg.ChatID == "" {
		msg.ChatID = msg.Context.ChatID
	}
	if msg.ReplyToMessageID == "" {
		msg.ReplyToMessageID = msg.Context.ReplyToMessageID
	}
	if msg.Context.ReplyToMessageID == "" {
		msg.Context.ReplyToMessageID = msg.ReplyToMessageID
	}
	msg.Scope = cloneOutboundScope(msg.Scope)
	var err error
	msg.TraceScopes, err = NormalizeTraceScopes(msg.TraceScopes)
	if len(msg.TraceScopes) == 0 {
		msg.TraceSettlement = false
	}
	return msg, err
}

// NormalizeTraceScopes returns complete, distinct scopes for one workspace.
// A physical outbound cannot correlate turns from different workspaces.
func NormalizeTraceScopes(scopes []runtimeevents.TraceScope) ([]runtimeevents.TraceScope, error) {
	normalized := make([]runtimeevents.TraceScope, 0, len(scopes))
	workspace := ""
	for _, scope := range scopes {
		scope = runtimeevents.NewTraceScope(scope.Workspace, scope.TurnID)
		if !scope.Complete() {
			continue
		}
		if workspace == "" {
			workspace = scope.Workspace
		} else if scope.Workspace != workspace {
			return nil, fmt.Errorf(
				"%w: %q and %q", ErrMixedTraceScopeWorkspaces, workspace, scope.Workspace,
			)
		}
		duplicate := false
		for _, existing := range normalized {
			if existing == scope {
				duplicate = true
				break
			}
		}
		if !duplicate {
			normalized = append(normalized, scope)
		}
	}
	return normalized, nil
}

// SetOutboundTraceScopes records every turn correlated with one text outbound.
func SetOutboundTraceScopes(msg *OutboundMessage, scopes []runtimeevents.TraceScope) error {
	if msg == nil {
		return nil
	}
	var err error
	msg.TraceScopes, err = NormalizeTraceScopes(scopes)
	return err
}

// SetOutboundMediaTraceScopes records every turn correlated with one media outbound.
func SetOutboundMediaTraceScopes(msg *OutboundMediaMessage, scopes []runtimeevents.TraceScope) error {
	if msg == nil {
		return nil
	}
	var err error
	msg.TraceScopes, err = NormalizeTraceScopes(scopes)
	return err
}

// NormalizeOutboundMediaMessage ensures media outbound messages also carry a
// normalized context while keeping convenience mirrors in sync.
func NormalizeOutboundMediaMessage(msg OutboundMediaMessage) (OutboundMediaMessage, error) {
	msg.Channel = strings.TrimSpace(msg.Channel)
	msg.ChatID = strings.TrimSpace(msg.ChatID)
	if msg.Context.Channel == "" {
		msg.Context.Channel = msg.Channel
	}
	if msg.Context.ChatID == "" {
		msg.Context.ChatID = msg.ChatID
	}
	msg.Context = normalizeInboundContext(msg.Context)
	if msg.Channel == "" {
		msg.Channel = msg.Context.Channel
	}
	if msg.ChatID == "" {
		msg.ChatID = msg.Context.ChatID
	}
	msg.Scope = cloneOutboundScope(msg.Scope)
	var err error
	msg.TraceScopes, err = NormalizeTraceScopes(msg.TraceScopes)
	if len(msg.TraceScopes) == 0 {
		msg.TraceSettlement = false
	}
	return msg, err
}

func cloneOutboundScope(scope *OutboundScope) *OutboundScope {
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
	return &cloned
}
