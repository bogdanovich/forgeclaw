//go:build linux || darwin

package companion

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

var (
	ErrFileAccessDenied = errors.New("node file access denied")
	ErrFileNotFound     = errors.New("node file not found")
	ErrFileConflict     = errors.New("node file conflicts with transfer state")
)

type fileIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Links  uint64 `json:"links"`
}

type fileMountIdentity struct {
	primary   uint64
	secondary uint64
}

type fileRoot struct {
	path  string
	file  *os.File
	mount fileMountIdentity
}

type resolvedFile struct {
	file     *os.File
	info     os.FileInfo
	identity fileIdentity
}

type resolvedParent struct {
	root        *fileRoot
	file        *os.File
	staging     *os.File
	basename    string
	crossMounts bool
}

type stagedFile struct {
	parent   *resolvedParent
	file     *os.File
	name     string
	identity fileIdentity
}

var (
	descriptorMountIdentity = platformDescriptorMountIdentity
	publishFileStage        = platformPublishFileStage
)

const fileStageDirectoryName = ".mintclaw-transfer-staging"

func openFileRoot(path string) (*fileRoot, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, ErrFileAccessDenied
	}
	rootDescriptor, err := unix.Open(
		string(filepath.Separator),
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open filesystem root: %w", err)
	}
	current := os.NewFile(uintptr(rootDescriptor), string(filepath.Separator))
	if current == nil {
		_ = unix.Close(rootDescriptor)
		return nil, errors.New("open filesystem root descriptor")
	}
	components := pathComponents(path)
	for _, component := range components {
		nextDescriptor, openErr := unix.Openat(
			int(current.Fd()),
			component,
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
			0,
		)
		if openErr != nil {
			_ = current.Close()
			return nil, classifyFileAccessError(openErr)
		}
		next := os.NewFile(uintptr(nextDescriptor), component)
		if next == nil {
			_ = unix.Close(nextDescriptor)
			_ = current.Close()
			return nil, errors.New("open file policy root descriptor")
		}
		_ = current.Close()
		current = next
	}
	info, err := current.Stat()
	if err != nil {
		_ = current.Close()
		return nil, classifyFileAccessError(err)
	}
	_, err = identityFromInfo(info)
	if err != nil || !info.IsDir() {
		_ = current.Close()
		return nil, ErrFileAccessDenied
	}
	if denied, deniedErr := deniedFileSystem(
		int(current.Fd()),
	); deniedErr != nil || denied {
		_ = current.Close()
		return nil, ErrFileAccessDenied
	}
	mount, err := descriptorMountIdentity(int(current.Fd()))
	if err != nil {
		_ = current.Close()
		return nil, ErrFileAccessDenied
	}
	return &fileRoot{
		path:  path,
		file:  current,
		mount: mount,
	}, nil
}

func (root *fileRoot) close() error {
	if root == nil || root.file == nil {
		return nil
	}
	err := root.file.Close()
	root.file = nil
	return err
}

func (root *fileRoot) openRegular(
	path string,
	maxBytes int64,
	crossMounts bool,
) (*resolvedFile, error) {
	parent, err := root.resolveParent(path, crossMounts)
	if err != nil {
		return nil, err
	}
	defer parent.close()
	descriptor, err := unix.Openat(
		int(parent.file.Fd()),
		parent.basename,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, classifyFileAccessError(err)
	}
	file := os.NewFile(uintptr(descriptor), parent.basename)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open regular file descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, classifyFileAccessError(err)
	}
	identity, identityErr := identityFromInfo(info)
	mount, mountErr := descriptorMountIdentity(int(file.Fd()))
	if identityErr != nil ||
		mountErr != nil ||
		!info.Mode().IsRegular() ||
		info.Size() < 0 ||
		info.Size() > maxBytes ||
		(!crossMounts && mount != root.mount) {
		_ = file.Close()
		return nil, ErrFileAccessDenied
	}
	if denied, deniedErr := deniedFileSystem(int(file.Fd())); deniedErr != nil || denied {
		_ = file.Close()
		return nil, ErrFileAccessDenied
	}
	return &resolvedFile{file: file, info: info, identity: identity}, nil
}

func (root *fileRoot) resolveParent(
	path string,
	crossMounts bool,
) (*resolvedParent, error) {
	relative, err := root.relativePath(path)
	if err != nil {
		return nil, err
	}
	components := strings.Split(relative, string(filepath.Separator))
	if len(components) == 0 {
		return nil, ErrFileAccessDenied
	}
	basename := components[len(components)-1]
	if componentErr := validateFilePathComponent(basename); componentErr != nil {
		return nil, componentErr
	}
	descriptor, err := unix.FcntlInt(
		root.file.Fd(),
		unix.F_DUPFD_CLOEXEC,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("duplicate file policy root: %w", err)
	}
	current := os.NewFile(uintptr(descriptor), root.path)
	if current == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("duplicate file policy root descriptor")
	}
	for _, component := range components[:len(components)-1] {
		if err := validateFilePathComponent(component); err != nil {
			_ = current.Close()
			return nil, err
		}
		nextDescriptor, openErr := unix.Openat(
			int(current.Fd()),
			component,
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
			0,
		)
		if openErr != nil {
			_ = current.Close()
			return nil, classifyFileAccessError(openErr)
		}
		next := os.NewFile(uintptr(nextDescriptor), component)
		if next == nil {
			_ = unix.Close(nextDescriptor)
			_ = current.Close()
			return nil, errors.New("open file policy path descriptor")
		}
		info, statErr := next.Stat()
		if statErr != nil {
			_ = next.Close()
			_ = current.Close()
			return nil, classifyFileAccessError(statErr)
		}
		_, identityErr := identityFromInfo(info)
		mount, mountErr := descriptorMountIdentity(int(next.Fd()))
		if identityErr != nil ||
			mountErr != nil ||
			!info.IsDir() ||
			(!crossMounts && mount != root.mount) {
			_ = next.Close()
			_ = current.Close()
			return nil, ErrFileAccessDenied
		}
		if denied, deniedErr := deniedFileSystem(int(next.Fd())); deniedErr != nil || denied {
			_ = next.Close()
			_ = current.Close()
			return nil, ErrFileAccessDenied
		}
		_ = current.Close()
		current = next
	}
	return &resolvedParent{
		root:        root,
		file:        current,
		basename:    basename,
		crossMounts: crossMounts,
	}, nil
}

func (root *fileRoot) relativePath(path string) (string, error) {
	if err := validateFilePath(path); err != nil {
		return "", err
	}
	if root.path == string(filepath.Separator) {
		return strings.TrimPrefix(path, string(filepath.Separator)), nil
	}
	prefix := root.path + string(filepath.Separator)
	if !strings.HasPrefix(path, prefix) {
		return "", ErrFileAccessDenied
	}
	relative := strings.TrimPrefix(path, prefix)
	if relative == "" {
		return "", ErrFileAccessDenied
	}
	return relative, nil
}

func (parent *resolvedParent) createStage(transferID string) (*stagedFile, error) {
	if parent == nil || parent.file == nil {
		return nil, ErrFileAccessDenied
	}
	staging, err := parent.openStageDirectory(true)
	if err != nil {
		return nil, err
	}
	name, err := randomFileStageName()
	if err != nil {
		return nil, err
	}
	descriptor, err := unix.Openat(
		int(staging.Fd()),
		name,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, classifyFileAccessError(err)
	}
	file := os.NewFile(uintptr(descriptor), name)
	if file == nil {
		_ = unix.Close(descriptor)
		_ = unix.Unlinkat(int(staging.Fd()), name, 0)
		return nil, errors.New("create file transfer stage descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		_ = unix.Unlinkat(int(staging.Fd()), name, 0)
		return nil, classifyFileAccessError(err)
	}
	identity, identityErr := identityFromInfo(info)
	stageMount, mountErr := descriptorMountIdentity(int(file.Fd()))
	parentMount, parentMountErr := descriptorMountIdentity(int(parent.file.Fd()))
	if identityErr != nil ||
		mountErr != nil ||
		parentMountErr != nil ||
		!info.Mode().IsRegular() ||
		identity.Links != 1 ||
		stageMount != parentMount {
		_ = file.Close()
		_ = unix.Unlinkat(int(staging.Fd()), name, 0)
		return nil, ErrFileAccessDenied
	}
	_ = transferID
	return &stagedFile{
		parent:   parent,
		file:     file,
		name:     name,
		identity: identity,
	}, nil
}

func (stage *stagedFile) publish(publication string) error {
	if stage == nil || stage.file == nil || stage.parent == nil {
		return ErrFileConflict
	}
	if err := stage.file.Sync(); err != nil {
		return fmt.Errorf("sync staged transfer: %w", err)
	}
	info, err := stage.file.Stat()
	if err != nil {
		return classifyFileAccessError(err)
	}
	identity, err := identityFromInfo(info)
	if err != nil ||
		identity.Device != stage.identity.Device ||
		identity.Inode != stage.identity.Inode ||
		identity.Links != 1 ||
		!info.Mode().IsRegular() {
		return ErrFileConflict
	}
	switch publication {
	case filePublicationCreate:
		if err := stage.parent.ensureFinalAbsent(); err != nil {
			return err
		}
	case filePublicationReplace:
		if err := stage.parent.ensureFinalRegular(); err != nil {
			return err
		}
	default:
		return ErrFileAccessDenied
	}
	if err := publishFileStage(
		int(stage.file.Fd()),
		int(stage.parent.staging.Fd()),
		stage.name,
		int(stage.parent.file.Fd()),
		stage.parent.basename,
		publication,
	); err != nil {
		return classifyFileAccessError(err)
	}
	if err := stage.parent.removePublishedStage(
		stage.identity,
		stage.name,
	); err != nil {
		return &committedFileMutationError{err: err}
	}
	if err := stage.parent.file.Sync(); err != nil {
		return &committedFileMutationError{err: err}
	}
	return nil
}

func (parent *resolvedParent) ensureFinalAbsent() error {
	var stat unix.Stat_t
	err := unix.Fstatat(
		int(parent.file.Fd()),
		parent.basename,
		&stat,
		unix.AT_SYMLINK_NOFOLLOW,
	)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return classifyFileAccessError(err)
	}
	return ErrFileConflict
}

func (parent *resolvedParent) ensureFinalRegular() error {
	finalDescriptor, err := unix.Openat(
		int(parent.file.Fd()),
		parent.basename,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return classifyFileAccessError(err)
	}
	defer unix.Close(finalDescriptor)
	var stat unix.Stat_t
	if err := unix.Fstat(finalDescriptor, &stat); err != nil {
		return classifyFileAccessError(err)
	}
	finalMount, mountErr := descriptorMountIdentity(finalDescriptor)
	if stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		mountErr != nil ||
		(!parent.rootAllowsCrossMounts() && finalMount != parent.root.mount) {
		return ErrFileAccessDenied
	}
	return nil
}

func (parent *resolvedParent) rootAllowsCrossMounts() bool {
	return parent.crossMounts
}

func (parent *resolvedParent) removeStage(identity fileIdentity, name string) error {
	exists, err := parent.stageMatches(identity, name, true)
	if err != nil || !exists {
		return err
	}
	if err := unix.Unlinkat(int(parent.staging.Fd()), name, 0); err != nil {
		return classifyFileAccessError(err)
	}
	return parent.staging.Sync()
}

func (parent *resolvedParent) removePublishedStage(
	identity fileIdentity,
	name string,
) error {
	exists, err := parent.stageMatches(identity, name, false)
	if err != nil || !exists {
		return err
	}
	if err := unix.Unlinkat(int(parent.staging.Fd()), name, 0); err != nil {
		return classifyFileAccessError(err)
	}
	return parent.staging.Sync()
}

func (parent *resolvedParent) stageMatches(
	identity fileIdentity,
	name string,
	requireOriginalLinks bool,
) (bool, error) {
	if parent == nil || parent.file == nil || name == "" {
		return false, ErrFileConflict
	}
	staging, err := parent.openStageDirectory(false)
	if errors.Is(err, ErrFileNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var stat unix.Stat_t
	err = unix.Fstatat(
		int(staging.Fd()),
		name,
		&stat,
		unix.AT_SYMLINK_NOFOLLOW,
	)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, classifyFileAccessError(err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		fileStatDevice(&stat) != identity.Device ||
		stat.Ino != identity.Inode ||
		fileStatLinks(&stat) == 0 ||
		(requireOriginalLinks && fileStatLinks(&stat) != identity.Links) {
		return false, ErrFileConflict
	}
	return true, nil
}

func (parent *resolvedParent) finalIdentity() (fileIdentity, os.FileInfo, error) {
	file, err := parent.openFinalRegular()
	if err != nil {
		return fileIdentity{}, nil, err
	}
	defer file.file.Close()
	return file.identity, file.info, nil
}

func (parent *resolvedParent) openFinalRegular() (*resolvedFile, error) {
	descriptor, err := unix.Openat(
		int(parent.file.Fd()),
		parent.basename,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, classifyFileAccessError(err)
	}
	file := os.NewFile(uintptr(descriptor), parent.basename)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open final transfer descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, classifyFileAccessError(err)
	}
	identity, identityErr := identityFromInfo(info)
	mount, mountErr := descriptorMountIdentity(int(file.Fd()))
	if identityErr != nil ||
		mountErr != nil ||
		!info.Mode().IsRegular() ||
		(!parent.crossMounts && mount != parent.root.mount) {
		_ = file.Close()
		return nil, ErrFileAccessDenied
	}
	if denied, deniedErr := deniedFileSystem(int(file.Fd())); deniedErr != nil || denied {
		_ = file.Close()
		return nil, ErrFileAccessDenied
	}
	return &resolvedFile{file: file, info: info, identity: identity}, nil
}

func (parent *resolvedParent) close() error {
	if parent == nil {
		return nil
	}
	var result error
	if parent.staging != nil {
		result = parent.staging.Close()
		parent.staging = nil
	}
	if parent.file != nil {
		if err := parent.file.Close(); result == nil {
			result = err
		}
		parent.file = nil
	}
	return result
}

func (parent *resolvedParent) openStageDirectory(create bool) (*os.File, error) {
	if parent == nil || parent.file == nil {
		return nil, ErrFileAccessDenied
	}
	if parent.staging != nil {
		return parent.staging, nil
	}
	parentInfo, err := parent.file.Stat()
	if err != nil || !parentInfo.IsDir() || !protectedFileStageParent(parentInfo) {
		return nil, ErrFileAccessDenied
	}
	if create {
		err = unix.Mkdirat(
			int(parent.file.Fd()),
			fileStageDirectoryName,
			0o700,
		)
		if err != nil && !errors.Is(err, unix.EEXIST) {
			return nil, classifyFileAccessError(err)
		}
	}
	descriptor, err := unix.Openat(
		int(parent.file.Fd()),
		fileStageDirectoryName,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, classifyFileAccessError(err)
	}
	staging := os.NewFile(uintptr(descriptor), fileStageDirectoryName)
	if staging == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open file transfer staging directory")
	}
	info, err := staging.Stat()
	mount, mountErr := descriptorMountIdentity(int(staging.Fd()))
	parentMount, parentMountErr := descriptorMountIdentity(int(parent.file.Fd()))
	if err != nil ||
		mountErr != nil ||
		parentMountErr != nil ||
		!info.IsDir() ||
		info.Mode().Perm() != 0o700 ||
		!fileStageDirectoryOwnedByProcess(info) ||
		mount != parentMount {
		_ = staging.Close()
		return nil, ErrFileAccessDenied
	}
	if denied, deniedErr := deniedFileSystem(int(staging.Fd())); deniedErr != nil || denied {
		_ = staging.Close()
		return nil, ErrFileAccessDenied
	}
	parent.staging = staging
	return staging, nil
}

func protectedFileStageParent(info os.FileInfo) bool {
	return info.Mode().Perm()&0o022 == 0 || info.Mode()&os.ModeSticky != 0
}

func fileStageDirectoryOwnedByProcess(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func randomFileStageName() (string, error) {
	var suffix [16]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate file transfer stage name: %w", err)
	}
	return ".mintclaw-transfer-" + hex.EncodeToString(suffix[:]) + ".tmp", nil
}

func validateFilePath(path string) error {
	if path == "" ||
		len(path) > MaxFilePathBytes ||
		!utf8.ValidString(path) ||
		strings.ContainsRune(path, 0) ||
		!filepath.IsAbs(path) ||
		filepath.Clean(path) != path ||
		path == string(filepath.Separator) {
		return ErrFileAccessDenied
	}
	for _, component := range pathComponents(path) {
		if err := validateFilePathComponent(component); err != nil {
			return err
		}
	}
	return nil
}

func validateFilePathComponent(component string) error {
	if component == "" ||
		component == "." ||
		component == ".." ||
		len(component) > 255 ||
		!utf8.ValidString(component) ||
		strings.IndexFunc(component, unicode.IsControl) >= 0 ||
		strings.ContainsAny(component, `/\`) {
		return ErrFileAccessDenied
	}
	return nil
}

func pathComponents(path string) []string {
	trimmed := strings.Trim(path, string(filepath.Separator))
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, string(filepath.Separator))
}

func identityFromInfo(info os.FileInfo) (fileIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, ErrFileAccessDenied
	}
	return fileIdentity{
		Device: syscallStatDevice(stat),
		Inode:  stat.Ino,
		Links:  syscallStatLinks(stat),
	}, nil
}

func classifyFileAccessError(err error) error {
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, unix.ENOENT):
		return ErrFileNotFound
	case errors.Is(err, unix.EEXIST), errors.Is(err, unix.ENOTEMPTY):
		return ErrFileConflict
	case errors.Is(err, unix.ELOOP),
		errors.Is(err, unix.ENOTDIR),
		errors.Is(err, unix.EACCES),
		errors.Is(err, unix.EPERM),
		errors.Is(err, unix.EXDEV):
		return ErrFileAccessDenied
	default:
		return err
	}
}

type committedFileMutationError struct {
	err error
}

func (err *committedFileMutationError) Error() string {
	return "file publication committed with durability warning: " + err.err.Error()
}

func (err *committedFileMutationError) Unwrap() error {
	return err.err
}
