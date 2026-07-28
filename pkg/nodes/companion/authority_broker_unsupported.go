//go:build !linux

package companion

import (
	"context"
	"errors"
)

type (
	AuthorityBrokerClient   struct{}
	AuthorityBrokerTerminal struct{}
)

func NewAuthorityBrokerClient(string) (*AuthorityBrokerClient, error) {
	return nil, errors.New("authority broker requires Linux")
}

func (*AuthorityBrokerClient) Snapshot(context.Context) (ShellBrokerSnapshot, error) {
	return ShellBrokerSnapshot{}, errors.New("authority broker requires Linux")
}

func (*AuthorityBrokerClient) Execute(
	context.Context,
	ShellBrokerRequest,
) (ShellBrokerResult, error) {
	return ShellBrokerResult{}, errors.New("authority broker requires Linux")
}

func (*AuthorityBrokerClient) OpenTerminal(
	context.Context,
	TerminalBrokerOpenRequest,
) (*AuthorityBrokerTerminal, TerminalBrokerEvent, error) {
	return nil, TerminalBrokerEvent{}, errors.New("authority broker requires Linux")
}

func (*AuthorityBrokerTerminal) ID() string { return "" }

func (*AuthorityBrokerTerminal) Send(context.Context, TerminalBrokerControl) error {
	return errors.New("authority broker requires Linux")
}

func (*AuthorityBrokerTerminal) Receive(context.Context) (TerminalBrokerEvent, error) {
	return TerminalBrokerEvent{}, errors.New("authority broker requires Linux")
}

func (*AuthorityBrokerTerminal) Close() error { return nil }

func LoadAuthorityBrokerConfig(string) (AuthorityBrokerConfig, error) {
	return AuthorityBrokerConfig{}, errors.New("authority broker requires Linux")
}

func RunAuthorityBroker(context.Context, AuthorityBrokerConfig, string) error {
	return errors.New("authority broker requires Linux")
}

func RunAuthorityBrokerWorker(context.Context, bool) error {
	return errors.New("authority broker requires Linux")
}
