//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

type systemdUninstallState struct {
	active          bool
	enableArguments []string
}

type systemdRemover func(publishedSystemdUnit) (bool, error)

func (lifecycle *systemdLifecycle) Uninstall(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	directory, err := openSystemdUnitDirectory(lifecycle.unitDir, false)
	if errors.Is(err, os.ErrNotExist) {
		return lifecycle.requireUninstalled(ctx, status)
	}
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer directory.Close()

	lock, err := acquireSystemdUnitLock(ctx, directory, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer func() { _ = lock.Close() }()

	unit, err := captureSystemdUnitAt(directory, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if !unit.exists {
		return lifecycle.requireUninstalled(ctx, status)
	}
	if !unit.managed {
		return lifecycleStatus{}, unownedSystemdUnitError(status.UnitPath)
	}
	if err = validateManagedSystemdInstallSection(unit.publication.data, lifecycle.system); err != nil {
		return lifecycleStatus{}, err
	}
	properties, err := lifecycle.queryUnitProperties(ctx, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if !properties.resolvesTo(status.UnitPath) {
		return lifecycleStatus{}, resolvedSystemdServiceError(status.Service, properties)
	}
	previous, err := captureSystemdUninstallState(properties)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if err = requirePublishedSystemdUnit(unit.publication); err != nil {
		return lifecycleStatus{}, err
	}
	if err = lifecycle.requireSuccess(ctx, "disable", "--now", status.Service); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackFailedDisable(unit.publication, status, previous, err)
	}
	if err = requirePublishedSystemdUnit(unit.publication); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackFailedDisable(unit.publication, status, previous, err)
	}

	quarantined, err := quarantinePublishedSystemdUnit(unit.publication)
	if err != nil {
		return lifecycleStatus{}, lifecycle.rollbackFailedDisable(unit.publication, status, previous, err)
	}
	if err = lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackUninstall(quarantined, status, previous, err)
	}
	if _, err = lifecycle.requireUninstalled(ctx, status); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackUninstall(quarantined, status, previous, err)
	}
	remove := lifecycle.remove
	if remove == nil {
		remove = removeQuarantinedSystemdUnit
	}
	removed, err := remove(quarantined)
	if err != nil && !removed {
		return lifecycleStatus{}, lifecycle.rollbackUninstall(quarantined, status, previous, err)
	}
	if err != nil {
		return lifecycleStatus{}, fmt.Errorf("remove uninstalled systemd unit: %w", err)
	}
	return status, nil
}

func (lifecycle *systemdLifecycle) requireUninstalled(
	ctx context.Context,
	status lifecycleStatus,
) (lifecycleStatus, error) {
	properties, err := lifecycle.queryUnitProperties(ctx, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if !properties.missing() {
		return lifecycleStatus{}, resolvedSystemdServiceError(status.Service, properties)
	}
	return status, nil
}

func captureSystemdUninstallState(properties systemdUnitProperties) (systemdUninstallState, error) {
	state := systemdUninstallState{}
	switch properties.activeState {
	case "active":
		state.active = true
	case "inactive":
	default:
		return systemdUninstallState{}, fmt.Errorf(
			"refusing systemd managed unit with unsupported ActiveState %q",
			properties.activeState,
		)
	}
	switch properties.unitFileState {
	case "enabled":
		state.enableArguments = []string{"enable"}
	case "enabled-runtime":
		state.enableArguments = []string{"enable", "--runtime"}
	case "disabled":
	case "":
		return systemdUninstallState{}, errors.New("systemd managed unit omitted UnitFileState")
	default:
		return systemdUninstallState{}, fmt.Errorf(
			"refusing systemd managed unit with unsupported UnitFileState %q",
			properties.unitFileState,
		)
	}
	return state, nil
}

func validateManagedSystemdInstallSection(data []byte, system bool) error {
	target := "default.target"
	if system {
		target = "multi-user.target"
	}
	wantedBy := "WantedBy=" + target
	inInstall := false
	seenInstall := false
	seenWantedBy := false
	for _, rawLine := range bytes.Split(data, []byte("\n")) {
		line := strings.TrimSpace(string(rawLine))
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inInstall = line == "[Install]"
			if inInstall {
				if seenInstall {
					return errors.New("managed systemd unit has duplicate [Install] sections")
				}
				seenInstall = true
			}
			continue
		}
		if !inInstall || line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if line != wantedBy || seenWantedBy {
			return fmt.Errorf("managed systemd unit has unsafe [Install] directive %q", line)
		}
		seenWantedBy = true
	}
	if !seenInstall || !seenWantedBy {
		return fmt.Errorf("managed systemd unit must install only into %s", target)
	}
	return nil
}

func (lifecycle *systemdLifecycle) rollbackFailedDisable(
	publication publishedSystemdUnit,
	status lifecycleStatus,
	previous systemdUninstallState,
	cause error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	if err := lifecycle.requirePublishedUnitLoaded(ctx, publication, status); err != nil {
		return errors.Join(cause, fmt.Errorf("verify systemd unit after failed disable: %w", err))
	}
	errorsSeen := append([]error{cause}, lifecycle.restoreSystemdServiceState(ctx, status, previous)...)
	return errors.Join(errorsSeen...)
}

func (lifecycle *systemdLifecycle) rollbackUninstall(
	quarantined publishedSystemdUnit,
	status lifecycleStatus,
	previous systemdUninstallState,
	cause error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	errorsSeen := []error{cause}
	restored, err := restoreQuarantinedSystemdUnit(quarantined, status.Service)
	if err != nil {
		if restored.name == "" {
			return errors.Join(cause, fmt.Errorf("restore systemd unit: %w", err))
		}
		errorsSeen = append(errorsSeen, fmt.Errorf("commit restored systemd unit: %w", err))
	}
	if err = lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("reload restored systemd unit: %w", err))
		return errors.Join(errorsSeen...)
	}
	if err = lifecycle.requirePublishedUnitLoaded(ctx, restored, status); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("verify restored systemd unit: %w", err))
		return errors.Join(errorsSeen...)
	}
	errorsSeen = append(errorsSeen, lifecycle.restoreSystemdServiceState(ctx, status, previous)...)
	return errors.Join(errorsSeen...)
}

func (lifecycle *systemdLifecycle) restoreSystemdServiceState(
	ctx context.Context,
	status lifecycleStatus,
	previous systemdUninstallState,
) []error {
	var errorsSeen []error
	if len(previous.enableArguments) != 0 {
		arguments := append(append([]string(nil), previous.enableArguments...), status.Service)
		if err := lifecycle.requireSuccess(ctx, arguments...); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("restore systemd enablement: %w", err))
		}
	}
	if previous.active {
		if err := lifecycle.requireSuccess(ctx, "start", status.Service); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("restore active systemd service: %w", err))
		}
	}
	return errorsSeen
}

func restoreQuarantinedSystemdUnit(
	quarantined publishedSystemdUnit,
	originalName string,
) (publishedSystemdUnit, error) {
	if err := requirePublishedSystemdUnit(quarantined); err != nil {
		return publishedSystemdUnit{}, err
	}
	if err := renamePublishedSystemdUnit(quarantined, originalName); err != nil {
		return publishedSystemdUnit{}, err
	}
	restored := quarantined
	restored.name = originalName
	if err := requirePublishedSystemdUnit(restored); err != nil {
		return publishedSystemdUnit{}, err
	}
	if err := unix.Fsync(restored.directory.fd()); err != nil {
		return restored, err
	}
	return restored, nil
}
