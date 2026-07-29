//go:build !windows

package nodes

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/bogdanovich/mintclaw/pkg/fileutil"
)

type anchoredDirectory struct {
	file *os.File
}

func openAnchoredDirectory(path string) (*anchoredDirectory, error) {
	descriptor, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open anchored directory: invalid descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.IsDir() {
		_ = file.Close()
		return nil, fmt.Errorf("open anchored directory: non-directory %q", path)
	}
	return &anchoredDirectory{file: file}, nil
}

func (directory *anchoredDirectory) openRegular(name string) (*os.File, os.FileInfo, error) {
	if directory == nil || directory.file == nil {
		return nil, nil, errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(name); err != nil {
		return nil, nil, err
	}
	descriptor, err := unix.Openat(
		int(directory.file.Fd()),
		name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(descriptor), name)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, nil, errors.New("open anchored regular file: invalid descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, fmt.Errorf("open anchored regular file: non-regular file %q", name)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		_ = file.Close()
		return nil, nil, fmt.Errorf("open anchored regular file: multiply linked file %q", name)
	}
	return file, info, nil
}

func (directory *anchoredDirectory) acquireLock(name string) (func(), error) {
	if directory == nil || directory.file == nil {
		return nil, errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(name); err != nil {
		return nil, err
	}
	descriptor, err := unix.Openat(
		int(directory.file.Fd()),
		name,
		unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open gateway terminal store lock: %w", err)
	}
	lock := os.NewFile(uintptr(descriptor), name)
	if lock == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open gateway terminal store lock: invalid descriptor")
	}
	info, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("inspect gateway terminal store lock: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = lock.Close()
		return nil, fmt.Errorf("gateway terminal store lock is non-regular: %q", name)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		_ = lock.Close()
		return nil, fmt.Errorf("gateway terminal store lock is multiply linked: %q", name)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("lock gateway terminal store: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}, nil
}

func (directory *anchoredDirectory) tryAcquireLock(name string) (func(), error) {
	if directory == nil || directory.file == nil {
		return nil, errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(name); err != nil {
		return nil, err
	}
	descriptor, err := unix.Openat(
		int(directory.file.Fd()),
		name,
		unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open anchored directory lock: %w", err)
	}
	lock := os.NewFile(uintptr(descriptor), name)
	if lock == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open anchored directory lock: invalid descriptor")
	}
	info, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("inspect anchored directory lock: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = lock.Close()
		return nil, fmt.Errorf("anchored directory lock is non-regular: %q", name)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		_ = lock.Close()
		return nil, fmt.Errorf("anchored directory lock is multiply linked: %q", name)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errAnchoredDirectoryLockBusy
		}
		return nil, fmt.Errorf("lock anchored directory: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}, nil
}

func (directory *anchoredDirectory) createRegularExclusive(
	name string,
	mode os.FileMode,
) (*os.File, error) {
	if directory == nil || directory.file == nil {
		return nil, errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(name); err != nil {
		return nil, err
	}
	descriptor, err := unix.Openat(
		int(directory.file.Fd()),
		name,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		uint32(mode.Perm()),
	)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), name)
	if file == nil {
		_ = unix.Close(descriptor)
		_ = unix.Unlinkat(int(directory.file.Fd()), name, 0)
		return nil, errors.New("create anchored regular file: invalid descriptor")
	}
	return file, nil
}

func (directory *anchoredDirectory) publishRegularNoReplace(
	stagingName string,
	finalName string,
) error {
	if directory == nil || directory.file == nil {
		return errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(stagingName); err != nil {
		return err
	}
	if err := validateAnchoredName(finalName); err != nil {
		return err
	}
	if err := unix.Linkat(
		int(directory.file.Fd()),
		stagingName,
		int(directory.file.Fd()),
		finalName,
		0,
	); err != nil {
		return err
	}
	if err := unix.Unlinkat(int(directory.file.Fd()), stagingName, 0); err != nil {
		return &fileutil.CommittedWriteError{Err: fmt.Errorf(
			"remove published staging link: %w",
			err,
		)}
	}
	if err := directory.file.Sync(); err != nil {
		return &fileutil.CommittedWriteError{Err: fmt.Errorf(
			"sync anchored directory after publication: %w",
			err,
		)}
	}
	return nil
}

func (directory *anchoredDirectory) removeRegular(name string) error {
	if directory == nil || directory.file == nil {
		return errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(name); err != nil {
		return err
	}
	err := unix.Unlinkat(int(directory.file.Fd()), name, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

func (directory *anchoredDirectory) writeFileAtomic(
	name string,
	data []byte,
	mode os.FileMode,
) error {
	if directory == nil || directory.file == nil {
		return errors.New("anchored directory is closed")
	}
	if err := validateAnchoredName(name); err != nil {
		return err
	}
	tempName, err := randomAnchoredTempName()
	if err != nil {
		return fmt.Errorf("generate gateway terminal store temp name: %w", err)
	}
	descriptor, err := unix.Openat(
		int(directory.file.Fd()),
		tempName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		uint32(mode.Perm()),
	)
	if err != nil {
		return fmt.Errorf("create gateway terminal store temp file: %w", err)
	}
	temp := os.NewFile(uintptr(descriptor), tempName)
	if temp == nil {
		_ = unix.Close(descriptor)
		_ = unix.Unlinkat(int(directory.file.Fd()), tempName, 0)
		return errors.New("create gateway terminal store temp file: invalid descriptor")
	}
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = unix.Unlinkat(int(directory.file.Fd()), tempName, 0)
		}
	}()
	if _, err := temp.Write(data); err != nil {
		return fmt.Errorf("write gateway terminal store temp file: %w", err)
	}
	if err := temp.Chmod(mode); err != nil {
		return fmt.Errorf("set gateway terminal store permissions: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync gateway terminal store temp file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close gateway terminal store temp file: %w", err)
	}
	if err := unix.Renameat(
		int(directory.file.Fd()),
		tempName,
		int(directory.file.Fd()),
		name,
	); err != nil {
		return fmt.Errorf("replace gateway terminal store: %w", err)
	}
	cleanup = false
	if err := directory.file.Sync(); err != nil {
		return &fileutil.CommittedWriteError{
			Err: fmt.Errorf("sync gateway terminal store directory: %w", err),
		}
	}
	return nil
}

func (directory *anchoredDirectory) close() error {
	if directory == nil || directory.file == nil {
		return nil
	}
	err := directory.file.Close()
	directory.file = nil
	return err
}
