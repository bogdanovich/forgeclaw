//go:build windows

package nodes

import "os"

func notifyTerminalResize(chan<- os.Signal) func() {
	return func() {}
}

func terminalTerminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
