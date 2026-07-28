//go:build !linux

package companion

import (
	"context"
	"errors"
)

type AuthorityBrokerClient struct{}

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

func LoadAuthorityBrokerConfig(string) (AuthorityBrokerConfig, error) {
	return AuthorityBrokerConfig{}, errors.New("authority broker requires Linux")
}

func RunAuthorityBroker(context.Context, AuthorityBrokerConfig, string) error {
	return errors.New("authority broker requires Linux")
}

func RunAuthorityBrokerWorker(context.Context, bool) error {
	return errors.New("authority broker requires Linux")
}
