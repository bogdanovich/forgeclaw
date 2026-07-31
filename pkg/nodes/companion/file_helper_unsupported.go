//go:build !linux

package companion

import (
	"context"
	"errors"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

type FileHelperClient struct{}

func NewFileHelperClient(context.Context, string) (*FileHelperClient, error) {
	return nil, errors.New("privileged file helper is supported only on Linux")
}

func (*FileHelperClient) Descriptors() []nodes.CommandDescriptor {
	return nil
}

func (*FileHelperClient) HandleTransferFrame(
	context.Context,
	protocol.TransferFrame,
	func(protocol.TransferFrame) error,
) error {
	return errors.New("privileged file helper is supported only on Linux")
}

func (*FileHelperClient) Close() error {
	return nil
}
