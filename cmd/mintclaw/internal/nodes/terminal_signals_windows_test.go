//go:build windows

package nodes

import (
	"os"
	"testing"
	"time"
)

func TestNotifyTerminalResizePollsOnWindows(t *testing.T) {
	resize := make(chan os.Signal, 1)
	stop := notifyTerminalResize(resize)
	defer stop()
	select {
	case signal := <-resize:
		if _, ok := signal.(terminalResizePollSignal); !ok {
			t.Fatalf("resize signal = %T", signal)
		}
	case <-time.After(3 * windowsTerminalResizePollInterval):
		t.Fatal("Windows resize polling did not emit a notification")
	}
	stop()
}
