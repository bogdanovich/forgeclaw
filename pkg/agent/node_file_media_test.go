package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/media"
	"github.com/bogdanovich/mintclaw/pkg/providers"
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
	if bindErr := bindNodeFileMediaOwner(store, ts, []string{ref}); bindErr != nil {
		t.Fatal(bindErr)
	}
	ownerA, err := nodeFileMediaOwnerForTurn(ts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, resolveErr := store.ResolveOwnedWithMeta(ref, ownerA); resolveErr != nil {
		t.Fatalf("exact owner failed to resolve: %v", resolveErr)
	}
	ts.opts.Dispatch.InboundContext.ActorID = "actor-b"
	ownerB, err := nodeFileMediaOwnerForTurn(ts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, resolveErr := store.ResolveOwnedWithMeta(ref, ownerB); resolveErr == nil {
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
	if bindErr := bindNodeFileMediaOwner(store, ts, []string{ref}); bindErr != nil {
		t.Fatal(bindErr)
	}
	owner, err := nodeFileMediaOwnerForTurn(ts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, resolveErr := store.ResolveOwnedWithMeta(ref, owner); resolveErr == nil {
		t.Fatal("profile without nodes_upload authority unexpectedly bound media")
	}
}

func TestProjectNodeFileMediaAttachmentsExposesOpaqueRefWithoutGatewayPath(t *testing.T) {
	store := media.NewFileMediaStore()
	path := filepath.Join(t.TempDir(), "inbound.png")
	if err := os.WriteFile(path, []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := store.Store(path, media.MediaMeta{
		Filename: "inbound.png", ContentType: "image/png",
	}, "inbound")
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewToolRegistry()
	registry.Register(tools.NewNodeUploadTool(nil, nil))
	ts := &turnState{agent: &AgentInstance{ID: "main", Tools: registry}}
	messages := projectNodeFileMediaAttachments(
		[]providers.Message{{Role: "user", Content: "upload this", Media: []string{ref}}},
		ts,
		[]string{ref},
		store,
	)
	resolved := resolveMediaRefs(messages, store, 1024)
	if len(resolved) != 1 || len(resolved[0].Attachments) != 1 ||
		resolved[0].Attachments[0].Ref != ref || resolved[0].Attachments[0].Filename != "inbound.png" {
		t.Fatalf("projected messages = %#v", resolved)
	}
	if len(resolved[0].Media) != 0 || strings.Contains(resolved[0].Content, path) {
		t.Fatalf("projected provider content leaked gateway path: %#v", resolved[0])
	}
}
