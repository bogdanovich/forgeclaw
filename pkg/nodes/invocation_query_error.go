package nodes

import "errors"

const (
	InvocationQueryNotFound             = "INVOCATION_NOT_FOUND"
	InvocationQueryLedgerUnavailable    = "LEDGER_UNAVAILABLE"
	InvocationQueryNodeUnavailable      = "NODE_UNAVAILABLE"
	InvocationQueryTimeout              = "STATUS_TIMEOUT"
	InvocationQueryCanceled             = "STATUS_CANCELED"
	InvocationQueryTransportUnavailable = "STATUS_TRANSPORT_UNAVAILABLE"
	InvocationQueryRejected             = "STATUS_REJECTED"
)

// InvocationQueryError carries one bounded, safe classification across the
// companion, gateway, and model-facing recovery boundary without exposing a
// transport endpoint or remote error message.
type InvocationQueryError struct {
	code  string
	cause error
}

func NewInvocationQueryError(code string, cause error) error {
	return &InvocationQueryError{code: normalizeInvocationQueryErrorCode(code), cause: cause}
}

func (err *InvocationQueryError) Error() string {
	return "node invocation status query failed (" + err.code + ")"
}

func (err *InvocationQueryError) Unwrap() error {
	return err.cause
}

func InvocationQueryErrorCode(err error) (string, bool) {
	var queryErr *InvocationQueryError
	if !errors.As(err, &queryErr) {
		return "", false
	}
	return queryErr.code, true
}

func normalizeInvocationQueryErrorCode(code string) string {
	switch code {
	case InvocationQueryNotFound,
		InvocationQueryLedgerUnavailable,
		InvocationQueryNodeUnavailable,
		InvocationQueryTimeout,
		InvocationQueryCanceled,
		InvocationQueryTransportUnavailable:
		return code
	default:
		return InvocationQueryRejected
	}
}
