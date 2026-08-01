package mcp

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestExclusiveServerLeaseIsNonBlockingAndReleasable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "playwright.lock")
	lease, err := acquireExclusiveServerLease("playwright", path)
	if err != nil {
		t.Fatalf("acquireExclusiveServerLease(first) error = %v", err)
	}
	t.Cleanup(lease.release)

	assertExclusiveLeaseFileSecurity(t, path)

	contender, err := acquireExclusiveServerLease("playwright", path)
	if contender != nil {
		contender.release()
		t.Fatal("acquireExclusiveServerLease(contender) returned a lease")
	}
	var busyErr *ExclusiveLeaseBusyError
	if !errors.As(err, &busyErr) || busyErr.Server != "playwright" {
		t.Fatalf("acquireExclusiveServerLease(contender) error = %v, want busy classification", err)
	}
	if !errors.Is(err, errExclusiveLeaseBusy) {
		t.Fatalf("acquireExclusiveServerLease(contender) error = %v, want busy sentinel", err)
	}

	lease.release()
	reacquired, err := acquireExclusiveServerLease("playwright", path)
	if err != nil {
		t.Fatalf("acquireExclusiveServerLease(after release) error = %v", err)
	}
	reacquired.release()
}
