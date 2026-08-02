package nodes

import "errors"

const (
	InvocationDispatchCommandDenied       = "COMMAND_DENIED"
	InvocationDispatchCommandUnavailable  = "COMMAND_UNAVAILABLE"
	InvocationDispatchExecutionFailed     = "EXECUTION_FAILED"
	InvocationDispatchIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	InvocationDispatchInvalidPlan         = "INVALID_PLAN"
	InvocationDispatchNodeBusy            = "NODE_BUSY"
	InvocationDispatchCanceled            = "INVOCATION_CANCELED"
	InvocationDispatchUnknown             = "INVOCATION_UNKNOWN"
	InvocationDispatchRejected            = "DISPATCH_REJECTED"
)

// InvocationDispatchError carries a bounded companion response classification
// across the WebSocket, gateway, and model-facing invocation boundary. Its
// Error output intentionally excludes the remote message and transport detail.
type InvocationDispatchError struct {
	code  string
	cause error
}

func NewInvocationDispatchError(code string, cause error) error {
	return &InvocationDispatchError{code: normalizeInvocationDispatchErrorCode(code), cause: cause}
}

func (err *InvocationDispatchError) Error() string {
	return "node invocation dispatch failed (" + err.code + ")"
}

func (err *InvocationDispatchError) Unwrap() error {
	return err.cause
}

func InvocationDispatchErrorCode(err error) (string, bool) {
	var dispatchErr *InvocationDispatchError
	if !errors.As(err, &dispatchErr) {
		return "", false
	}
	return dispatchErr.code, true
}

func normalizeInvocationDispatchErrorCode(code string) string {
	switch code {
	case InvocationDispatchCommandDenied,
		InvocationDispatchCommandUnavailable,
		InvocationDispatchExecutionFailed,
		InvocationDispatchIdempotencyConflict,
		InvocationDispatchInvalidPlan,
		InvocationDispatchNodeBusy,
		InvocationDispatchCanceled,
		InvocationDispatchUnknown:
		return code
	default:
		return InvocationDispatchRejected
	}
}
