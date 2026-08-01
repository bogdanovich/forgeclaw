package nodes

import (
	"errors"
	"strings"
	"testing"
)

func TestInvocationQueryErrorBoundsClassificationAndHidesCause(t *testing.T) {
	privateCause := errors.New("dial private.example:443 with token")
	err := NewInvocationQueryError("UNTRUSTED_REMOTE_CODE", privateCause)
	code, classified := InvocationQueryErrorCode(err)
	if !classified || code != InvocationQueryRejected {
		t.Fatalf("query error classification = %q, %v", code, classified)
	}
	if !errors.Is(err, privateCause) {
		t.Fatal("query error did not retain its internal cause")
	}
	if strings.Contains(err.Error(), "private.example") || strings.Contains(err.Error(), "token") {
		t.Fatalf("query error exposed its cause: %q", err)
	}
}
