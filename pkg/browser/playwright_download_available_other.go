//go:build !linux && !darwin

package browser

func playwrightDownloadBoundaryAvailable() bool { return false }
