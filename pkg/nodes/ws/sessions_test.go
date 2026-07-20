package ws

import (
	"context"
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
	releaseFirst, err := hub.Claim(nodes.ID("node_test"), first, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond, err := hub.Claim(nodes.ID("node_test"), second, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if first.closed.Load() != 1 {
		t.Fatalf("replaced connection close count = %d", first.closed.Load())
	}
	if owned, _ := releaseFirst(); owned {
		t.Fatal("replaced connection retained session ownership")
	}
	if !hub.Connected(nodes.ID("node_test")) {
		t.Fatal("replacement connection is not tracked")
	}
	if owned, _ := releaseSecond(); !owned {
		t.Fatal("current connection could not release ownership")
	}
	if hub.Connected(nodes.ID("node_test")) {
		t.Fatal("released connection remains tracked")
	}
}

func TestSessionHubCloseRejectsNewClaimsAndLetsOwnersRelease(t *testing.T) {
	hub := NewSessionHub()
	active := &trackingCloser{}
	release, err := hub.Claim(nodes.ID("node_active"), active, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- hub.Close(t.Context()) }()

	deadline := time.Now().Add(time.Second)
	for active.closed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active.closed.Load() != 1 {
		t.Fatalf("active connection close count = %d", active.closed.Load())
	}
	if owned, _ := release(); !owned {
		t.Fatal("shutdown prevented current owner from persisting disconnect")
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	late := &trackingCloser{}
	if _, err := hub.Claim(nodes.ID("node_late"), late, nil, nil); !errors.Is(err, ErrSessionHubClosed) {
		t.Fatalf("closed hub Claim() error = %v", err)
	}
	if hub.Connected(nodes.ID("node_late")) {
		t.Fatal("closed hub accepted a new owner")
	}
	if late.closed.Load() != 1 {
		t.Fatalf("late connection close count = %d", late.closed.Load())
	}

	if err := hub.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if active.closed.Load() != 1 {
		t.Fatalf("second Close() closed active connection %d times", active.closed.Load())
	}
}

func TestSessionHubActivatesReplacementBeforeOldOwnerCanRelease(t *testing.T) {
	hub := NewSessionHub()
	first := &trackingCloser{}
	releaseFirst, err := hub.Claim(nodes.ID("node_test"), first, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	activationStarted := make(chan struct{})
	allowActivation := make(chan struct{})
	type claimResult struct {
		release func() (bool, error)
		err     error
	}
	claimed := make(chan claimResult, 1)
	go func() {
		release, claimErr := hub.Claim(nodes.ID("node_test"), &trackingCloser{}, func() error {
			close(activationStarted)
			<-allowActivation
			return nil
		}, nil)
		claimed <- claimResult{release: release, err: claimErr}
	}()
	<-activationStarted
	oldReleased := make(chan bool, 1)
	go func() {
		owned, _ := releaseFirst()
		oldReleased <- owned
	}()
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
	if owned, _ := result.release(); !owned {
		t.Fatal("activated replacement lost ownership")
	}
}

func TestSessionHubFailedActivationClosesPreviousOwner(t *testing.T) {
	hub := NewSessionHub()
	first := &trackingCloser{}
	releaseFirst, err := hub.Claim(nodes.ID("node_test"), first, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("activation failed")
	second := &trackingCloser{}
	if _, err := hub.Claim(
		nodes.ID("node_test"), second, func() error { return wantErr }, nil,
	); !errors.Is(err, wantErr) {
		t.Fatalf("replacement Claim() error = %v", err)
	}
	if first.closed.Load() != 1 {
		t.Fatal("failed replacement did not close the previous connection")
	}
	if owned, _ := releaseFirst(); owned {
		t.Fatal("failed replacement left the previous owner current")
	}
}

func TestSessionHubCloseWaitsForDurableDeactivation(t *testing.T) {
	hub := NewSessionHub()
	deactivationStarted := make(chan struct{})
	allowDeactivation := make(chan struct{})
	release, err := hub.Claim(
		nodes.ID("node_test"),
		&trackingCloser{},
		nil,
		func() error {
			close(deactivationStarted)
			<-allowDeactivation
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan error, 1)
	go func() {
		_, releaseErr := release()
		released <- releaseErr
	}()
	<-deactivationStarted
	closed := make(chan error, 1)
	go func() { closed <- hub.Close(t.Context()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close() returned before durable deactivation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(allowDeactivation)
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestSessionHubCloseHonorsDeadlineDuringBlockedDeactivation(t *testing.T) {
	hub := NewSessionHub()
	deactivationStarted := make(chan struct{})
	allowDeactivation := make(chan struct{})
	release, err := hub.Claim(
		nodes.ID("node_test"),
		&trackingCloser{},
		nil,
		func() error {
			close(deactivationStarted)
			<-allowDeactivation
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		_, _ = release()
		close(released)
	}()
	<-deactivationStarted
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if err := hub.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v", err)
	}
	close(allowDeactivation)
	<-released
	if err := hub.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSessionHubCloseReturnsDisconnectError(t *testing.T) {
	hub := NewSessionHub()
	wantErr := errors.New("registry unavailable")
	release, err := hub.Claim(
		nodes.ID("node_test"),
		&trackingCloser{},
		nil,
		func() error { return wantErr },
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := release(); !errors.Is(err, wantErr) {
		t.Fatalf("release() error = %v", err)
	}
	if err := hub.Close(t.Context()); !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v", err)
	}
}
