//go:build !linux && !darwin

package gateway

import (
	"context"
	"errors"
	"os"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func (*gatewayBrowserToolSource) ScreenshotAvailable() bool { return false }

func (*gatewayBrowserToolSource) ArtifactTransferAvailable() bool { return false }

func (*gatewayBrowserToolSource) DownloadAvailable() bool { return false }

func openNodeTransferMedia(string) (*os.File, os.FileInfo, error) {
	return nil, nil, errors.New("node file transfer is unsupported on this gateway platform")
}

func copyNodeTransferDelivery(
	context.Context,
	*os.File,
	nodes.TransferArtifactRecord,
	string,
	string,
) (string, error) {
	return "", errors.New("node file transfer is unsupported on this gateway platform")
}

func copyNodeTransferDeliveryTracked(
	context.Context,
	*os.File,
	nodes.TransferArtifactRecord,
	string,
	string,
) (string, bool, error) {
	return "", false, errors.New("node file transfer is unsupported on this gateway platform")
}

func removeNodeTransferDelivery(string, string) error {
	return errors.New("node file transfer is unsupported on this gateway platform")
}
