package ws

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

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
	releaseFirst, err := hub.Claim(nodes.ID("node_test"), first, nil)
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond, err := hub.Claim(nodes.ID("node_test"), second, nil)
	if err != nil {
		t.Fatal(err)
	}

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
	release, err := hub.Claim(nodes.ID("node_active"), active, nil)
	if err != nil {
		t.Fatal(err)
	}
	hub.Close()

	if active.closed.Load() != 1 {
		t.Fatalf("active connection close count = %d", active.closed.Load())
	}
	if !release() {
		t.Fatal("shutdown prevented current owner from persisting disconnect")
	}
	late := &trackingCloser{}
	if _, err := hub.Claim(nodes.ID("node_late"), late, nil); !errors.Is(err, ErrSessionHubClosed) {
		t.Fatalf("closed hub Claim() error = %v", err)
	}
	if hub.Connected(nodes.ID("node_late")) {
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

func TestSessionHubActivatesReplacementBeforeOldOwnerCanRelease(t *testing.T) {
	hub := NewSessionHub()
	first := &trackingCloser{}
	releaseFirst, err := hub.Claim(nodes.ID("node_test"), first, nil)
	if err != nil {
		t.Fatal(err)
	}
	activationStarted := make(chan struct{})
	allowActivation := make(chan struct{})
	type claimResult struct {
		release func() bool
		err     error
	}
	claimed := make(chan claimResult, 1)
	go func() {
		release, claimErr := hub.Claim(nodes.ID("node_test"), &trackingCloser{}, func() error {
			close(activationStarted)
			<-allowActivation
			return nil
		})
		claimed <- claimResult{release: release, err: claimErr}
	}()
	<-activationStarted
	oldReleased := make(chan bool, 1)
	go func() { oldReleased <- releaseFirst() }()
	select {
	case <-oldReleased:
		t.Fatal("old owner released while replacement activation was incomplete")
	case <-time.After(25 * time.Millisecond):
	}
	close(allowActivation)
	result := <-claimed
	if result.err != nil {
		t.Fatal(result.err)
	}
	if <-oldReleased {
		t.Fatal("old owner disconnected the activated replacement")
	}
	if !result.release() {
		t.Fatal("activated replacement lost ownership")
	}
}

func TestSessionHubRollsBackFailedActivation(t *testing.T) {
	hub := NewSessionHub()
	first := &trackingCloser{}
	releaseFirst, err := hub.Claim(nodes.ID("node_test"), first, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("activation failed")
	second := &trackingCloser{}
	if _, err := hub.Claim(nodes.ID("node_test"), second, func() error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("replacement Claim() error = %v", err)
	}
	if first.closed.Load() != 0 {
		t.Fatal("failed replacement closed the previous connection")
	}
	if !releaseFirst() {
		t.Fatal("failed replacement displaced the previous owner")
	}
}
