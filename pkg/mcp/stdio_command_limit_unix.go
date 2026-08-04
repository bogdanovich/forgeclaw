//go:build linux || darwin

package mcp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

const stdioFileLimitLauncher = "__mintclaw_internal_stdio_file_limit__"

func init() {
	if len(os.Args) < 2 || os.Args[1] != stdioFileLimitLauncher {
		return
	}
	if err := launchWithStdioFileLimit(os.Args[2:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "unable to launch bounded stdio process:", err)
		os.Exit(127)
	}
}

func stdioCommand(
	ctx context.Context,
	command string,
	args []string,
	fileSizeLimitBytes int64,
) (*exec.Cmd, error) {
	if fileSizeLimitBytes <= 0 {
		return exec.CommandContext(ctx, command, args...), nil
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	launcherArgs := []string{
		stdioFileLimitLauncher,
		strconv.FormatInt(fileSizeLimitBytes, 10),
		command,
	}
	launcherArgs = append(launcherArgs, args...)
	return exec.CommandContext(ctx, executable, launcherArgs...), nil
}

func launchWithStdioFileLimit(args []string) error {
	if len(args) < 2 {
		return errors.New("missing file limit or command")
	}
	limit, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil || limit == 0 {
		return errors.New("invalid file size limit")
	}
	resourceLimit := syscall.Rlimit{Cur: limit, Max: limit}
	if err = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &resourceLimit); err != nil {
		return err
	}
	command, err := exec.LookPath(args[1])
	if err != nil {
		return err
	}
	return syscall.Exec(command, append([]string{args[1]}, args[2:]...), os.Environ())
}
