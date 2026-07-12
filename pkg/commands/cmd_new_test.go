package commands

import (
	"context"
	"strings"
	"testing"
)

func TestNewCommandStartsFreshSession(t *testing.T) {
	called := false
	rt := &Runtime{
		StartFreshSession: func() (string, error) {
			called = true
			return "fresh-session", nil
		},
	}
	executor := NewExecutor(NewRegistry(BuiltinDefinitions()), rt)
	var reply string
	result := executor.Execute(context.Background(), Request{
		Text: "/new",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if result.Outcome != OutcomeHandled || result.Err != nil {
		t.Fatalf("/new result = %+v", result)
	}
	if !called {
		t.Fatal("expected StartFreshSession to be called")
	}
	if !strings.Contains(reply, "cleared the current goal") || !strings.Contains(reply, "fresh-session") {
		t.Fatalf("/new reply = %q", reply)
	}
}

func TestNewCommandValidatesArgumentsAndRuntime(t *testing.T) {
	executor := NewExecutor(NewRegistry(BuiltinDefinitions()), nil)
	var reply string
	result := executor.Execute(context.Background(), Request{
		Text: "/new extra",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if result.Outcome != OutcomeHandled || reply != unavailableMsg {
		t.Fatalf("unavailable /new = (%+v, %q)", result, reply)
	}

	executor = NewExecutor(NewRegistry(BuiltinDefinitions()), &Runtime{
		StartFreshSession: func() (string, error) { return "", nil },
	})
	reply = ""
	result = executor.Execute(context.Background(), Request{
		Text: "/new extra",
		Reply: func(text string) error {
			reply = text
			return nil
		},
	})
	if result.Outcome != OutcomeHandled || reply != "Usage: /new" {
		t.Fatalf("invalid /new = (%+v, %q)", result, reply)
	}
}
