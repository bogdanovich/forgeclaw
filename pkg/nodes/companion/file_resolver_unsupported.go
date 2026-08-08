//go:build !linux && !darwin

package companion

import (
	"errors"
	"os"
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

type fileRoot struct {
	path string
}

type resolvedFile struct {
	file     *os.File
	info     os.FileInfo
	identity fileIdentity
}

type resolvedParent struct{}

type stagedFile struct {
	parent   *resolvedParent
	file     *os.File
	name     string
	identity fileIdentity
}

func openFileRoot(string) (*fileRoot, error) {
	return nil, ErrFileAccessDenied
}

func (*fileRoot) close() error {
	return nil
}

func (*fileRoot) relativePath(string) (string, error) {
	return "", ErrFileAccessDenied
}

func (*fileRoot) openRegular(string, int64, bool) (*resolvedFile, error) {
	return nil, ErrFileAccessDenied
}

func (*fileRoot) resolveParent(string, bool) (*resolvedParent, error) {
	return nil, ErrFileAccessDenied
}

func (*resolvedParent) createStage(string) (*stagedFile, error) {
	return nil, ErrFileAccessDenied
}

func (*resolvedParent) close() error {
	return nil
}

func (*resolvedParent) removeStage(fileIdentity, string) error {
	return ErrFileAccessDenied
}

func (*resolvedParent) removePublishedStage(fileIdentity, string) error {
	return ErrFileAccessDenied
}

func (*resolvedParent) stageMatches(
	fileIdentity,
	string,
	bool,
) (bool, error) {
	return false, ErrFileAccessDenied
}

func (*resolvedParent) openFinalRegular() (*resolvedFile, error) {
	return nil, ErrFileAccessDenied
}

func (*stagedFile) publish(string) error {
	return ErrFileAccessDenied
}

func validateFilePath(string) error {
	return ErrFileAccessDenied
}

func identityFromInfo(os.FileInfo) (fileIdentity, error) {
	return fileIdentity{}, ErrFileAccessDenied
}

type committedFileMutationError struct {
	err error
}

func (err *committedFileMutationError) Error() string {
	return "file publication unsupported: " + err.err.Error()
}

func (err *committedFileMutationError) Unwrap() error {
	return err.err
}
