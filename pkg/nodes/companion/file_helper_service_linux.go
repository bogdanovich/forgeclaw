//go:build linux

package companion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

func LoadFileHelperServiceConfig(path string) (FileHelperServiceConfig, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return FileHelperServiceConfig{}, errors.New("file helper config path must be absolute")
	}
	if err := verifyAuthorityBrokerDirectoryChain(filepath.Dir(path)); err != nil {
		return FileHelperServiceConfig{}, fmt.Errorf("validate file helper config directory: %w", err)
	}
	descriptor, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return FileHelperServiceConfig{}, fmt.Errorf("open file helper config: %w", err)
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return FileHelperServiceConfig{}, errors.New("open file helper config: invalid descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return FileHelperServiceConfig{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	pathInfo, pathErr := os.Lstat(path)
	pathStat, pathStatOK := pathInfoSyscallStat(pathInfo)
	if !ok ||
		pathErr != nil ||
		!pathStatOK ||
		stat.Dev != pathStat.Dev ||
		stat.Ino != pathStat.Ino ||
		stat.Uid != 0 ||
		!info.Mode().IsRegular() ||
		info.Mode().Perm()&0o022 != 0 {
		return FileHelperServiceConfig{}, errors.New(
			"file helper config must be a root-owned non-writable regular file",
		)
	}
	raw, err := io.ReadAll(io.LimitReader(file, MaxFileHelperConfigBytes+1))
	if err != nil {
		return FileHelperServiceConfig{}, fmt.Errorf("read file helper config: %w", err)
	}
	if len(raw) > MaxFileHelperConfigBytes {
		return FileHelperServiceConfig{}, errors.New("file helper config exceeds size limit")
	}
	if _, err := jsonstrict.Decode(raw); err != nil {
		return FileHelperServiceConfig{}, fmt.Errorf("validate file helper config: %w", err)
	}
	var config FileHelperServiceConfig
	if err := decodeStrictJSON(raw, &config); err != nil {
		return FileHelperServiceConfig{}, fmt.Errorf("decode file helper config: %w", err)
	}
	return NormalizeFileHelperServiceConfig(config, filepath.Dir(path))
}

func RunFileHelper(
	ctx context.Context,
	config FileHelperServiceConfig,
) error {
	if os.Geteuid() != 0 {
		return errors.New("file helper must run as root")
	}
	if !config.normalized {
		return errors.New("file helper config is not normalized")
	}
	if err := prepareFileHelperStateDirectory(config.StateDir); err != nil {
		return err
	}
	ledger, err := NewFileTransferLedger(
		FileTransferLedgerPath(config.StateDir),
		DefaultFileTransferLedgerLimit,
		DefaultFileTransferLedgerBytes,
	)
	if err != nil {
		return err
	}
	defer ledger.Close()
	runtime, err := NewFileTransferRuntime(config.Profiles, ledger)
	if err != nil {
		return err
	}
	defer runtime.Close()
	server, err := newFileHelperServer(config, runtime)
	if err != nil {
		return err
	}
	directory, err := openAuthorityBrokerSocketDirectory(config.SocketPath)
	if err != nil {
		return fmt.Errorf("open file helper socket directory: %w", err)
	}
	defer directory.Close()
	if prepareErr := directory.prepare(); prepareErr != nil {
		return fmt.Errorf("prepare file helper socket: %w", prepareErr)
	}
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: directory.descriptorPath(), Net: "unix"},
	)
	if err != nil {
		return fmt.Errorf("listen file helper socket: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	defer listener.Close()
	defer func() { _ = directory.unlink() }()
	if chownErr := unix.Fchownat(
		directory.descriptor,
		directory.name,
		0,
		int(config.AllowedGID),
		unix.AT_SYMLINK_NOFOLLOW,
	); chownErr != nil {
		return fmt.Errorf("own file helper socket: %w", chownErr)
	}
	if chmodErr := unix.Fchmodat(directory.descriptor, directory.name, 0o660, 0); chmodErr != nil {
		return fmt.Errorf("protect file helper socket: %w", chmodErr)
	}
	return server.Serve(ctx, listener)
}

func prepareFileHelperStateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create file helper state directory: %w", err)
	}
	if err := verifyAuthorityBrokerDirectoryChain(path); err != nil {
		return fmt.Errorf("validate file helper state directory: %w", err)
	}
	return nil
}
