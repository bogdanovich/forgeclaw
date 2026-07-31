package agent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

func TestBindNodeFileMediaOwnerUsesExactActorAndRoute(t *testing.T) {
	store := media.NewFileMediaStore()
	path := filepath.Join(t.TempDir(), "inbound.bin")
	if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := store.Store(path, media.MediaMeta{Source: "telegram"}, "inbound")
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewToolRegistry()
	registry.Register(tools.NewNodeUploadTool(nil, nil))
	ts := &turnState{
		agent:     &AgentInstance{ID: "main", Tools: registry},
		workspace: "/workspace/main",
		channel:   "telegram",
		chatID:    "chat-1",
		opts: processOptions{Dispatch: DispatchRequest{
			RouteSessionKey: "telegram:chat-1:topic-1",
			SessionKey:      "session-1",
			InboundContext: &bus.InboundContext{
				Channel: "telegram", ChatID: "chat-1", TopicID: "topic-1", ActorID: "actor-a",
			},
		}},
	}
	if err := bindNodeFileMediaOwner(store, ts, []string{ref}); err != nil {
		t.Fatal(err)
	}
	ownerA, err := nodeFileMediaOwnerForTurn(ts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ResolveOwnedWithMeta(ref, ownerA); err != nil {
		t.Fatalf("exact owner failed to resolve: %v", err)
	}
	ts.opts.Dispatch.InboundContext.ActorID = "actor-b"
	ownerB, err := nodeFileMediaOwnerForTurn(ts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ResolveOwnedWithMeta(ref, ownerB); err == nil {
		t.Fatal("other actor resolved bound inbound media")
	}
}

func TestBindNodeFileMediaOwnerDoesNothingWithoutUploadAuthority(t *testing.T) {
	store := media.NewFileMediaStore()
	path := filepath.Join(t.TempDir(), "inbound.bin")
	if err := os.WriteFile(path, []byte("unbound"), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := store.Store(path, media.MediaMeta{Source: "telegram"}, "inbound")
	if err != nil {
		t.Fatal(err)
	}
	ts := &turnState{
		agent:     &AgentInstance{ID: "main", Tools: tools.NewToolRegistry()},
		workspace: "/workspace/main",
		channel:   "telegram",
		chatID:    "chat-1",
		opts: processOptions{Dispatch: DispatchRequest{
			RouteSessionKey: "telegram:chat-1",
			SessionKey:      "session-1",
			InboundContext: &bus.InboundContext{
				Channel: "telegram", ChatID: "chat-1", ActorID: "actor-a",
			},
		}},
	}
	if err := bindNodeFileMediaOwner(store, ts, []string{ref}); err != nil {
		t.Fatal(err)
	}
	owner, err := nodeFileMediaOwnerForTurn(ts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ResolveOwnedWithMeta(ref, owner); err == nil {
		t.Fatal("profile without nodes_upload authority unexpectedly bound media")
	}
}
