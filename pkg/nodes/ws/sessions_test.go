package ws

import (
	"sync/atomic"
	"testing"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

type trackingCloser struct {
	closed atomic.Int32
}

func (closer *trackingCloser) Close() error {
	closer.closed.Add(1)
	return nil
}

func TestSessionHubNewestClaimOwnsDisconnect(t *testing.T) {
	hub := NewSessionHub()
	first := &trackingCloser{}
	second := &trackingCloser{}
	releaseFirst := hub.Claim(nodes.ID("node_test"), first)
	releaseSecond := hub.Claim(nodes.ID("node_test"), second)

	if first.closed.Load() != 1 {
		t.Fatalf("replaced connection close count = %d", first.closed.Load())
	}
	if releaseFirst() {
		t.Fatal("replaced connection retained session ownership")
	}
	if !hub.Connected(nodes.ID("node_test")) {
		t.Fatal("replacement connection is not tracked")
	}
	if !releaseSecond() {
		t.Fatal("current connection could not release ownership")
	}
	if hub.Connected(nodes.ID("node_test")) {
		t.Fatal("released connection remains tracked")
	}
}

func TestSessionHubCloseRejectsNewClaimsAndLetsOwnersRelease(t *testing.T) {
	hub := NewSessionHub()
	active := &trackingCloser{}
	release := hub.Claim(nodes.ID("node_active"), active)
	hub.Close()

	if active.closed.Load() != 1 {
		t.Fatalf("active connection close count = %d", active.closed.Load())
	}
	if !release() {
		t.Fatal("shutdown prevented current owner from persisting disconnect")
	}
	late := &trackingCloser{}
	if hub.Claim(nodes.ID("node_late"), late)() {
		t.Fatal("closed hub accepted a new owner")
	}
	if late.closed.Load() != 1 {
		t.Fatalf("late connection close count = %d", late.closed.Load())
	}

	hub.Close()
	if active.closed.Load() != 1 {
		t.Fatalf("second Close() closed active connection %d times", active.closed.Load())
	}
}
