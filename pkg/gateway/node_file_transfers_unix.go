//go:build linux || darwin

package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const nodeTransferDeliveryTempAttempts = 16

func (*gatewayBrowserToolSource) ScreenshotAvailable() bool { return true }

func openNodeTransferMedia(path string) (*os.File, os.FileInfo, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, nil, errors.New("open gateway upload artifact")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		if err == nil {
			err = errors.New("gateway upload artifact is not a regular file")
		}
		return nil, nil, err
	}
	return file, info, nil
}

func copyNodeTransferDelivery(
	ctx context.Context,
	source *os.File,
	artifact nodes.TransferArtifactRecord,
	workspace string,
	name string,
) (string, error) {
	path, _, err := copyNodeTransferDeliveryTracked(ctx, source, artifact, workspace, name)
	return path, err
}

func copyNodeTransferDeliveryTracked(
	ctx context.Context,
	source *os.File,
	artifact nodes.TransferArtifactRecord,
	workspace string,
	name string,
) (string, bool, error) {
	if source == nil {
		return "", false, nodes.ErrTransferArtifactNotFound
	}
	directory, err := openNodeTransferDeliveryDirectory(workspace)
	if err != nil {
		return "", false, err
	}
	defer directory.Close()
	destination := filepath.Join(directory.Name(), name)
	if existing, existingErr := openNodeTransferDeliveryFile(directory, name); existingErr == nil {
		defer existing.Close()
		return destination, false, verifyNodeTransferDelivery(existing, artifact)
	} else if !errors.Is(existingErr, os.ErrNotExist) {
		return "", false, existingErr
	}
	temp, tempName, err := createNodeTransferDeliveryTemp(directory)
	if err != nil {
		return "", false, err
	}
	removeTemp := true
	defer func() {
		_ = temp.Close()
		if removeTemp {
			_ = unix.Unlinkat(int(directory.Fd()), tempName, 0)
		}
	}()
	hasher := sha256.New()
	written, copyErr := io.Copy(
		io.MultiWriter(temp, hasher),
		&contextBoundedReader{
			ctx:       ctx,
			reader:    source,
			remaining: artifact.Spec.DeclaredSize,
		},
	)
	if copyErr != nil ||
		written != artifact.Spec.DeclaredSize ||
		hex.EncodeToString(hasher.Sum(nil)) != artifact.Spec.SHA256 {
		if copyErr == nil {
			copyErr = nodes.ErrTransferDigestMismatch
		}
		return "", false, copyErr
	}
	var extra [1]byte
	if count, readErr := source.Read(extra[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return "", false, nodes.ErrTransferSizeExceeded
	}
	if err := temp.Sync(); err != nil {
		return "", false, err
	}
	if err := temp.Close(); err != nil {
		return "", false, err
	}
	if err := unix.Renameat(
		int(directory.Fd()),
		tempName,
		int(directory.Fd()),
		name,
	); err != nil {
		return "", false, err
	}
	removeTemp = false
	if err := directory.Sync(); err != nil {
		return destination, true, err
	}
	return destination, true, nil
}

func removeNodeTransferDelivery(workspace, name string) error {
	directory, err := openNodeTransferDeliveryDirectory(workspace)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err = unix.Unlinkat(int(directory.Fd()), name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	return directory.Sync()
}

func openNodeTransferDeliveryDirectory(workspace string) (*os.File, error) {
	descriptor, err := unix.Open(
		workspace,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, err
	}
	current := os.NewFile(uintptr(descriptor), workspace)
	if current == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open node transfer workspace directory")
	}
	currentPath := filepath.Clean(workspace)
	for _, component := range []string{"state", "media", "node-transfers"} {
		if mkdirErr := unix.Mkdirat(int(current.Fd()), component, 0o700); mkdirErr != nil &&
			!errors.Is(mkdirErr, unix.EEXIST) {
			_ = current.Close()
			return nil, mkdirErr
		}
		nextDescriptor, openErr := unix.Openat(
			int(current.Fd()),
			component,
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
			0,
		)
		if openErr != nil {
			_ = current.Close()
			return nil, openErr
		}
		nextPath := filepath.Join(currentPath, component)
		next := os.NewFile(uintptr(nextDescriptor), nextPath)
		if next == nil {
			_ = unix.Close(nextDescriptor)
			_ = current.Close()
			return nil, errors.New("open node transfer delivery directory")
		}
		_ = current.Close()
		current = next
		currentPath = nextPath
	}
	info, err := current.Stat()
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		_ = current.Close()
		if err == nil {
			err = errors.New("node transfer delivery directory is not private")
		}
		return nil, err
	}
	return current, nil
}

func openNodeTransferDeliveryFile(directory *os.File, name string) (*os.File, error) {
	descriptor, err := unix.Openat(
		int(directory.Fd()),
		name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), name)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open node transfer delivery file")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		if err == nil {
			err = errors.New("node transfer delivery is not a regular file")
		}
		return nil, err
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		_ = file.Close()
		return nil, errors.New("node transfer delivery has multiple links")
	}
	return file, nil
}

func createNodeTransferDeliveryTemp(directory *os.File) (*os.File, string, error) {
	for range nodeTransferDeliveryTempAttempts {
		var random [12]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := ".node-transfer-" + hex.EncodeToString(random[:])
		descriptor, err := unix.Openat(
			int(directory.Fd()),
			name,
			unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0o600,
		)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		file := os.NewFile(uintptr(descriptor), name)
		if file == nil {
			_ = unix.Close(descriptor)
			_ = unix.Unlinkat(int(directory.Fd()), name, 0)
			return nil, "", errors.New("create node transfer delivery temp file")
		}
		return file, name, nil
	}
	return nil, "", fmt.Errorf("create node transfer delivery temp file: name collisions")
}
