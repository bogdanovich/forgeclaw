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

const MaxAuthorityBrokerConfigBytes = 1024 * 1024

func LoadAuthorityBrokerConfig(path string) (AuthorityBrokerConfig, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return AuthorityBrokerConfig{}, errors.New("authority broker config path must be absolute")
	}
	if err := verifyAuthorityBrokerDirectoryChain(filepath.Dir(path)); err != nil {
		return AuthorityBrokerConfig{}, err
	}
	descriptor, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return AuthorityBrokerConfig{}, fmt.Errorf("open authority broker config: %w", err)
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return AuthorityBrokerConfig{}, errors.New("open authority broker config: invalid descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return AuthorityBrokerConfig{}, err
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
		return AuthorityBrokerConfig{}, errors.New(
			"authority broker config must be a root-owned non-writable regular file",
		)
	}
	raw, err := io.ReadAll(io.LimitReader(file, MaxAuthorityBrokerConfigBytes+1))
	if err != nil {
		return AuthorityBrokerConfig{}, fmt.Errorf("read authority broker config: %w", err)
	}
	if len(raw) > MaxAuthorityBrokerConfigBytes {
		return AuthorityBrokerConfig{}, errors.New("authority broker config exceeds size limit")
	}
	if _, err := jsonstrict.Decode(raw); err != nil {
		return AuthorityBrokerConfig{}, fmt.Errorf("validate authority broker config: %w", err)
	}
	var config AuthorityBrokerConfig
	if err := decodeStrictJSON(raw, &config); err != nil {
		return AuthorityBrokerConfig{}, fmt.Errorf("decode authority broker config: %w", err)
	}
	return NormalizeAuthorityBrokerConfig(config, filepath.Dir(path))
}

func verifyAuthorityBrokerDirectoryChain(path string) error {
	for {
		info, err := os.Lstat(path)
		stat, statOK := pathInfoSyscallStat(info)
		if err != nil ||
			!statOK ||
			stat.Uid != 0 ||
			!info.IsDir() ||
			info.Mode().Perm()&0o022 != 0 {
			return errors.New(
				"authority broker config directory chain must be root-owned and non-writable",
			)
		}
		if path == string(filepath.Separator) {
			return nil
		}
		parent := filepath.Dir(path)
		if parent == path {
			return errors.New("authority broker config directory chain is invalid")
		}
		path = parent
	}
}

func pathInfoSyscallStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

func RunAuthorityBroker(
	ctx context.Context,
	config AuthorityBrokerConfig,
	executable string,
) error {
	if os.Geteuid() != 0 {
		return errors.New("authority broker must run as root")
	}
	if len(config.normalizedProfile) != MaxShellBrokerProfiles {
		return errors.New("authority broker config is not normalized")
	}
	runner, err := newAuthorityBrokerProcessRunner(executable)
	if err != nil {
		return err
	}
	server, err := newAuthorityBrokerServer(config, runner)
	if err != nil {
		return err
	}
	if prepareErr := prepareAuthorityBrokerSocket(config.SocketPath); prepareErr != nil {
		return prepareErr
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.SocketPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen authority broker socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(config.SocketPath)
	if chownErr := os.Chown(config.SocketPath, 0, int(config.AllowedGID)); chownErr != nil {
		return fmt.Errorf("own authority broker socket: %w", chownErr)
	}
	if chmodErr := os.Chmod(config.SocketPath, 0o660); chmodErr != nil {
		return fmt.Errorf("protect authority broker socket: %w", chmodErr)
	}
	return server.Serve(ctx, listener)
}

func prepareAuthorityBrokerSocket(path string) error {
	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("stat authority broker socket directory: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return errors.New("authority broker socket directory must be root-owned and non-writable")
	}
	info, err = os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	stat, ok = info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || info.Mode()&os.ModeSocket == 0 {
		return errors.New("refusing to replace non-broker socket path")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale authority broker socket: %w", err)
	}
	return nil
}
