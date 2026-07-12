package gateway

type RestartDispatchOutcome string

const (
	RestartDispatchAccepted      RestartDispatchOutcome = "accepted"
	RestartDispatchIndeterminate RestartDispatchOutcome = "indeterminate"
	RestartDispatchFailed        RestartDispatchOutcome = "failed"
)

type RestartDispatchResult struct {
	Outcome RestartDispatchOutcome
	Err     error
}

func failedRestartDispatch(err error) RestartDispatchResult {
	return RestartDispatchResult{Outcome: RestartDispatchFailed, Err: err}
}
