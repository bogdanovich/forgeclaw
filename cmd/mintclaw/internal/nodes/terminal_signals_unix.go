//go:build !windows

package nodes

import (
	"os"
	"os/signal"
	"syscall"
)

func notifyTerminalResize(channel chan<- os.Signal) func() {
	signal.Notify(channel, syscall.SIGWINCH)
	return func() { signal.Stop(channel) }
}

func terminalTerminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP}
}
