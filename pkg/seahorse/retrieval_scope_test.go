package seahorse

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestGrepToolTrustedRetrievalScopes(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	type seededConversation struct {
		sessionKey string
		routeKey   string
		messageID  int64
	}
	seed := func(sessionKey, routeKey, agentID, content string) seededConversation {
		t.Helper()
		conv, createErr := store.GetOrCreateConversation(ctx, sessionKey)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if routeKey != "" {
			if provenanceErr := store.SetConversationProvenance(
				ctx,
				sessionKey,
				routeKey,
				agentID,
			); provenanceErr != nil {
				t.Fatal(provenanceErr)
			}
		}
		msg, addErr := store.AddMessage(ctx, conv.ConversationID, "user", content+" scope-needle", 5)
		if addErr != nil {
			t.Fatal(addErr)
		}
		return seededConversation{
			sessionKey: sessionKey,
			routeKey:   routeKey,
			messageID:  msg.ID,
		}
	}

	current := seed("epoch:current", "route:account-a:chat-a:topic-a", "main", "current")
	previous := seed("epoch:previous", current.routeKey, "main", "previous")
	otherTopic := seed("epoch:other-topic", "route:account-a:chat-a:topic-b", "main", "other-topic")
	otherChat := seed("epoch:other-chat", "route:account-a:chat-b:topic-a", "main", "other-chat")
	otherAccount := seed("epoch:other-account", "route:account-b:chat-a:topic-a", "main", "other-account")
	otherAgent := seed("epoch:other-agent", current.routeKey, "reviewer", "other-agent")
	unprovenanced := seed("epoch:legacy", "", "", "legacy")

	tool := NewGrepTool(&RetrievalEngine{
		store:  store,
		config: Config{MaxRetrievalScope: string(retrievalScopeWorkspace)},
	})
	toolCtx := tools.WithToolSessionContext(
		ctx,
		"main",
		current.sessionKey,
		retrievalTestScope(current.routeKey, "main"),
	)
	tests := []struct {
		name      string
		scope     string
		wantIDs   []int64
		rejectIDs []int64
	}{
		{
			name:    "current epoch",
			wantIDs: []int64{current.messageID},
			rejectIDs: []int64{
				previous.messageID,
				otherTopic.messageID,
				otherChat.messageID,
				otherAccount.messageID,
				otherAgent.messageID,
				unprovenanced.messageID,
			},
		},
		{
			name:    "route conversation",
			scope:   "conversation",
			wantIDs: []int64{current.messageID, previous.messageID},
			rejectIDs: []int64{
				otherTopic.messageID,
				otherChat.messageID,
				otherAccount.messageID,
				otherAgent.messageID,
				unprovenanced.messageID,
			},
		},
		{
			name:  "agent workspace",
			scope: "workspace",
			wantIDs: []int64{
				current.messageID,
				previous.messageID,
				otherTopic.messageID,
				otherChat.messageID,
				otherAccount.messageID,
			},
			rejectIDs: []int64{otherAgent.messageID, unprovenanced.messageID},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := map[string]any{"pattern": "scope-needle", "scope": "message"}
			if tt.scope != "" {
				args["retrieval_scope"] = tt.scope
			}
			result := tool.Execute(toolCtx, args)
			if result.IsError {
				t.Fatalf("execute: %s", result.ContentForLLM())
			}
			var output struct {
				Messages []GrepMessageResult `json:"messages"`
			}
			if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
				t.Fatal(err)
			}
			got := make(map[int64]bool, len(output.Messages))
			for _, message := range output.Messages {
				got[message.ID] = true
			}
			for _, id := range tt.wantIDs {
				if !got[id] {
					t.Errorf("message %d missing from result", id)
				}
			}
			for _, id := range tt.rejectIDs {
				if got[id] {
					t.Errorf("message %d leaked across retrieval scope", id)
				}
			}
		})
	}
}

func TestRetrievalScopeDefaultsToConversationMaximum(t *testing.T) {
	engine := &RetrievalEngine{}
	if got := engine.allowedRetrievalScopes(); !reflect.DeepEqual(got, []string{"current_epoch", "conversation"}) {
		t.Fatalf("allowed scopes = %#v", got)
	}
}

func TestNewEngineRejectsInvalidMaxRetrievalScope(t *testing.T) {
	_, err := NewEngine(Config{
		DBPath:            t.TempDir() + "/seahorse.db",
		MaxRetrievalScope: "global",
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "maxRetrievalScope") {
		t.Fatalf("NewEngine error = %v", err)
	}
}

func TestRetrievalToolSchemasHonorOperatorMaximum(t *testing.T) {
	engine := &RetrievalEngine{config: Config{MaxRetrievalScope: "current_epoch"}}
	want := []string{"current_epoch"}
	for _, tool := range []interface{ Parameters() map[string]any }{
		NewGrepTool(engine),
		NewExpandTool(engine),
	} {
		properties := tool.Parameters()["properties"].(map[string]any)
		scope := properties["retrieval_scope"].(map[string]any)
		if got := scope["enum"]; !reflect.DeepEqual(got, want) {
			t.Fatalf("retrieval scope enum = %#v, want %#v", got, want)
		}
	}
}

func TestRetrievalScopeRejectsRequestsAboveOperatorMaximum(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	current, err := store.GetOrCreateConversation(ctx, "epoch:current")
	if err != nil {
		t.Fatal(err)
	}
	message, err := store.AddMessage(ctx, current.ConversationID, "user", "scope-needle", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetConversationProvenance(ctx, "epoch:current", "route:a", "main"); err != nil {
		t.Fatal(err)
	}
	toolCtx := tools.WithToolSessionContext(ctx, "main", "epoch:current", retrievalTestScope("route:a", "main"))
	engine := &RetrievalEngine{store: store}

	grepResult := NewGrepTool(engine).Execute(toolCtx, map[string]any{
		"pattern":         "scope-needle",
		"retrieval_scope": "workspace",
	})
	if !grepResult.IsError || !strings.Contains(grepResult.ContentForLLM(), "exceeds operator maximum") {
		t.Fatalf("grep result = %#v", grepResult)
	}

	expandResult := NewExpandTool(engine).Execute(toolCtx, map[string]any{
		"message_ids":     []any{float64(message.ID)},
		"retrieval_scope": "workspace",
	})
	if !expandResult.IsError || !strings.Contains(expandResult.ContentForLLM(), "exceeds operator maximum") {
		t.Fatalf("expand result = %#v", expandResult)
	}
}

func TestConversationRetrievalSeparatesSenders(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	seed := func(sessionKey, routeKey, content string) int64 {
		t.Helper()
		conversation, err := store.GetOrCreateConversation(ctx, sessionKey)
		if err != nil {
			t.Fatal(err)
		}
		if provenanceErr := store.SetConversationProvenance(ctx, sessionKey, routeKey, "main"); provenanceErr != nil {
			t.Fatal(provenanceErr)
		}
		message, addErr := store.AddMessage(ctx, conversation.ConversationID, "user", content+" sender-needle", 5)
		if addErr != nil {
			t.Fatal(addErr)
		}
		return message.ID
	}

	currentID := seed("epoch:sender-a:current", "route:chat-a:sender-a", "current")
	previousID := seed("epoch:sender-a:previous", "route:chat-a:sender-a", "previous")
	otherSenderID := seed("epoch:sender-b", "route:chat-a:sender-b", "other")
	toolCtx := tools.WithToolSessionContext(
		ctx,
		"main",
		"epoch:sender-a:current",
		retrievalTestScope("route:chat-a:sender-a", "main"),
	)
	result := NewGrepTool(&RetrievalEngine{store: store}).Execute(toolCtx, map[string]any{
		"pattern":         "sender-needle",
		"scope":           "message",
		"retrieval_scope": "conversation",
	})
	if result.IsError {
		t.Fatal(result.ContentForLLM())
	}
	var output struct {
		Messages []GrepMessageResult `json:"messages"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatal(err)
	}
	got := make(map[int64]bool, len(output.Messages))
	for _, message := range output.Messages {
		got[message.ID] = true
	}
	if !got[currentID] || !got[previousID] || got[otherSenderID] {
		t.Fatalf("sender-scoped messages = %#v", got)
	}
}

func TestExpandToolRejectsCrossScopeMessageIDs(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	current, err := store.GetOrCreateConversation(ctx, "epoch:current")
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.GetOrCreateConversation(ctx, "epoch:other")
	if err != nil {
		t.Fatal(err)
	}
	currentMessage, err := store.AddMessage(ctx, current.ConversationID, "user", "current", 5)
	if err != nil {
		t.Fatal(err)
	}
	otherMessage, err := store.AddMessage(ctx, other.ConversationID, "user", "other", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetConversationProvenance(ctx, "epoch:current", "route:a", "main"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetConversationProvenance(ctx, "epoch:other", "route:b", "main"); err != nil {
		t.Fatal(err)
	}

	toolCtx := tools.WithToolSessionContext(ctx, "main", "epoch:current", retrievalTestScope("route:a", "main"))
	result := NewExpandTool(&RetrievalEngine{store: store}).Execute(toolCtx, map[string]any{
		"message_ids": []any{float64(currentMessage.ID), float64(otherMessage.ID)},
	})
	if result.IsError {
		t.Fatalf("execute: %s", result.ContentForLLM())
	}
	var output struct {
		Messages           []map[string]any `json:"messages"`
		RejectedMessageIDs []int64          `json:"rejectedMessageIds"`
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Messages) != 1 || len(output.RejectedMessageIDs) != 1 ||
		output.RejectedMessageIDs[0] != otherMessage.ID {
		t.Fatalf("unexpected scoped expansion: %#v", output)
	}
}

func TestBroadRetrievalRequiresTrustedProvenance(t *testing.T) {
	store := openTestStore(t)
	tool := NewGrepTool(&RetrievalEngine{store: store})
	ctx := tools.WithToolSessionContext(context.Background(), "main", "epoch:current", nil)
	for _, scope := range []string{"conversation", "workspace"} {
		result := tool.Execute(ctx, map[string]any{"pattern": "needle", "retrieval_scope": scope})
		if !result.IsError {
			t.Errorf("scope %q did not fail closed without trusted provenance", scope)
		}
	}
}

func TestSetConversationProvenanceRejectsConflict(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if err := store.SetConversationProvenance(ctx, "epoch:a", "route:a", "main"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetConversationProvenance(ctx, "epoch:a", "route:b", "main"); err == nil {
		t.Fatal("expected route provenance conflict")
	}
	if err := store.SetConversationProvenance(ctx, "epoch:a", "route:a", "reviewer"); err == nil {
		t.Fatal("expected agent provenance conflict")
	}
}
