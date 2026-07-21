package companion

// systemExecProcess owns platform containment for one command tree.
type systemExecProcess interface {
	wait() error
	terminate() error
	terminationConfirmed() bool
	close()
}
