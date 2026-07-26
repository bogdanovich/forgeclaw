package tools

import (
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/providers"
)

func TestSubagentManager_SetMediaResolver_StoresResolver(t *testing.T) {
	manager := NewSubagentManager(nil, "gpt-test", t.TempDir())

	called := false
	manager.SetMediaResolver(func(msgs []providers.Message) []providers.Message {
		called = true
		return msgs
	})

	manager.mu.RLock()
	got := manager.mediaResolver
	manager.mu.RUnlock()

	if got == nil {
		t.Fatal("expected mediaResolver to be set")
	}

	if called {
		t.Fatal("resolver should not be called during SetMediaResolver")
	}
}
