//go:build windows

package nodes

import (
	"os"
	"sync"
	"time"
)

const windowsTerminalResizePollInterval = 500 * time.Millisecond

type terminalResizePollSignal struct{}

func (terminalResizePollSignal) Signal()        {}
func (terminalResizePollSignal) String() string { return "terminal resize poll" }

func notifyTerminalResize(channel chan<- os.Signal) func() {
	ticker := time.NewTicker(windowsTerminalResizePollInterval)
	done := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		for {
			select {
			case <-ticker.C:
				select {
				case channel <- terminalResizePollSignal{}:
				default:
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		stopOnce.Do(func() {
			ticker.Stop()
			close(done)
		})
	}
}

func terminalTerminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
