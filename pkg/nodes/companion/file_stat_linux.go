//go:build linux

package companion

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func fileStatDevice(stat *unix.Stat_t) uint64 {
	return stat.Dev
}

func fileStatLinks(stat *unix.Stat_t) uint64 {
	return stat.Nlink
}

func syscallStatDevice(stat *syscall.Stat_t) uint64 {
	return stat.Dev
}

func syscallStatLinks(stat *syscall.Stat_t) uint64 {
	return stat.Nlink
}
