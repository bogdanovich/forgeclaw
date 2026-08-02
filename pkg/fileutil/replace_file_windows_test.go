//go:build windows

package fileutil

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestReplaceFileAtomicWith_RetriesTransientError(t *testing.T) {
	attempts := 0
	sleeps := 0
	err := replaceFileAtomicWith(
		`C:\source`,
		`C:\destination`,
		func(_, _ *uint16, _ uint32) error {
			attempts++
			if attempts < 3 {
				return windows.ERROR_ACCESS_DENIED
			}
			return nil
		},
		func(delay time.Duration) {
			sleeps++
			if delay != replaceFileRetryDelay {
				t.Errorf("retry delay = %v, want %v", delay, replaceFileRetryDelay)
			}
		},
	)
	if err != nil {
		t.Fatalf("replaceFileAtomicWith() error = %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if sleeps != 2 {
		t.Errorf("sleeps = %d, want 2", sleeps)
	}
}

func TestReplaceFileAtomicWith_DoesNotRetryPermanentError(t *testing.T) {
	attempts := 0
	wantErr := windows.ERROR_INVALID_PARAMETER
	err := replaceFileAtomicWith(
		`C:\source`,
		`C:\destination`,
		func(_, _ *uint16, _ uint32) error {
			attempts++
			return wantErr
		},
		func(time.Duration) {
			t.Fatal("unexpected retry delay")
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("replaceFileAtomicWith() error = %v, want %v", err, wantErr)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
}
