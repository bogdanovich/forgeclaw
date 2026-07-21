package companion

// systemExecProcess owns platform containment for one command tree.
type systemExecProcess interface {
	wait() error
	terminate() error
	// finish runs after the direct child has been reaped and guarantees that no
	// owned descendant remains before a result crosses the node boundary.
	finish() error
	close()
}
