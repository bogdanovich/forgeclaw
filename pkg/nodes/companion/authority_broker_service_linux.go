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
				"authority broker directory chain must be root-owned and non-writable",
			)
		}
		if path == string(filepath.Separator) {
			return nil
		}
		parent := filepath.Dir(path)
		if parent == path {
			return errors.New("authority broker directory chain is invalid")
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
	identity := newAuthorityBrokerCgroupIdentity(config.CompanionCgroup)
	server, err := newAuthorityBrokerServer(config, runner, identity)
	if err != nil {
		return err
	}
	directory, err := openAuthorityBrokerSocketDirectory(config.SocketPath)
	if err != nil {
		return err
	}
	defer directory.Close()
	if prepareErr := directory.prepare(); prepareErr != nil {
		return prepareErr
	}
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: directory.descriptorPath(), Net: "unix"},
	)
	if err != nil {
		return fmt.Errorf("listen authority broker socket: %w", err)
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
		return fmt.Errorf("own authority broker socket: %w", chownErr)
	}
	if chmodErr := unix.Fchmodat(
		directory.descriptor,
		directory.name,
		0o660,
		0,
	); chmodErr != nil {
		return fmt.Errorf("protect authority broker socket: %w", chmodErr)
	}
	return server.Serve(ctx, listener)
}

type authorityBrokerSocketDirectory struct {
	descriptor int
	name       string
}

func openAuthorityBrokerSocketDirectory(
	socketPath string,
) (*authorityBrokerSocketDirectory, error) {
	socketPath = filepath.Clean(socketPath)
	parent := filepath.Dir(socketPath)
	name := filepath.Base(socketPath)
	if !filepath.IsAbs(socketPath) ||
		name == "." ||
		name == string(filepath.Separator) {
		return nil, errors.New("authority broker socket path is invalid")
	}
	if err := verifyAuthorityBrokerDirectoryChain(parent); err != nil {
		return nil, err
	}
	descriptor, err := unix.Open(
		parent,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open authority broker socket directory: %w", err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(descriptor, &opened); err != nil {
		_ = unix.Close(descriptor)
		return nil, fmt.Errorf("stat authority broker socket directory: %w", err)
	}
	info, pathErr := os.Lstat(parent)
	pathStat, pathStatOK := pathInfoSyscallStat(info)
	if pathErr != nil ||
		!pathStatOK ||
		opened.Dev != pathStat.Dev ||
		opened.Ino != pathStat.Ino {
		_ = unix.Close(descriptor)
		return nil, errors.New("authority broker socket directory changed during validation")
	}
	return &authorityBrokerSocketDirectory{
		descriptor: descriptor, name: name,
	}, nil
}

func (directory *authorityBrokerSocketDirectory) descriptorPath() string {
	return fmt.Sprintf(
		"/proc/self/fd/%d/%s",
		directory.descriptor,
		directory.name,
	)
}

func (directory *authorityBrokerSocketDirectory) prepare() error {
	var stat unix.Stat_t
	err := unix.Fstatat(
		directory.descriptor,
		directory.name,
		&stat,
		unix.AT_SYMLINK_NOFOLLOW,
	)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	if stat.Uid != 0 || stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return errors.New("refusing to replace non-broker socket path")
	}
	if err := directory.unlink(); err != nil {
		return fmt.Errorf("remove stale authority broker socket: %w", err)
	}
	return nil
}

func (directory *authorityBrokerSocketDirectory) unlink() error {
	err := unix.Unlinkat(directory.descriptor, directory.name, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

func (directory *authorityBrokerSocketDirectory) Close() error {
	if directory == nil || directory.descriptor < 0 {
		return nil
	}
	err := unix.Close(directory.descriptor)
	directory.descriptor = -1
	return err
}

func prepareAuthorityBrokerSocket(path string) error {
	directory, err := openAuthorityBrokerSocketDirectory(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.prepare(); err != nil {
		return err
	}
	return nil
}
