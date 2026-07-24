//go:build linux || darwin

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (lifecycle *launchdLifecycle) Uninstall(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	directory, err := openExistingLaunchdPlistDirectory(lifecycle.plistDir, lifecycle.system)
	if errors.Is(err, os.ErrNotExist) {
		return lifecycle.requireUninstalled(ctx, status)
	}
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer directory.Close()

	lock, err := acquireLaunchdInstallLock(ctx, directory, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer func() { _ = lock.Close() }()

	plist, err := captureLaunchdPlistAt(directory, filepath.Base(status.UnitPath))
	if err != nil {
		return lifecycleStatus{}, err
	}
	if !plist.exists {
		return lifecycle.requireUninstalled(ctx, status)
	}
	if !plist.managed {
		return lifecycleStatus{}, fmt.Errorf("refusing to manage unowned launchd plist %s", status.UnitPath)
	}
	if plist.label != status.Service {
		return lifecycleStatus{}, fmt.Errorf(
			"refusing launchd plist %s with label %q, want %q",
			status.UnitPath,
			plist.label,
			status.Service,
		)
	}
	job, loaded, err := lifecycle.queryJob(ctx, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if loaded && filepath.Clean(job.path) != filepath.Clean(status.UnitPath) {
		return lifecycleStatus{}, fmt.Errorf(
			"refusing launchd service %s resolved outside its managed plist (path %q)",
			status.Service,
			job.path,
		)
	}
	if err = requirePublishedLaunchdPlist(plist.publication); err != nil {
		return lifecycleStatus{}, err
	}
	if loaded {
		if err = lifecycle.requireSuccess(ctx, "bootout", job.domain+"/"+status.Service); err != nil {
			return lifecycleStatus{}, lifecycle.rollbackFailedBootout(plist.publication, status, job, err)
		}
		if _, stillLoaded, verifyErr := lifecycle.queryJob(ctx, status.Service); verifyErr != nil {
			return lifecycleStatus{}, lifecycle.rollbackLaunchdLoadedState(
				plist.publication,
				status,
				job,
				fmt.Errorf("verify launchd bootout: %w", verifyErr),
			)
		} else if stillLoaded {
			return lifecycleStatus{}, errors.New("launchd service remained loaded after bootout")
		}
	}
	if err = requirePublishedLaunchdPlist(plist.publication); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackLaunchdLoadedState(plist.publication, status, job, err)
	}
	quarantined, err := quarantinePublishedLaunchdPlist(plist.publication)
	if err != nil {
		return lifecycleStatus{}, lifecycle.rollbackLaunchdLoadedState(plist.publication, status, job, err)
	}
	remove := lifecycle.remove
	if remove == nil {
		remove = removeQuarantinedLaunchdPlist
	}
	removed, err := remove(quarantined)
	if err != nil && !removed {
		return lifecycleStatus{}, lifecycle.rollbackLaunchdUninstall(quarantined, status, job, err)
	}
	if err != nil {
		return lifecycleStatus{}, fmt.Errorf("remove uninstalled launchd plist: %w", err)
	}
	return status, nil
}

func (lifecycle *launchdLifecycle) requireUninstalled(
	ctx context.Context,
	status lifecycleStatus,
) (lifecycleStatus, error) {
	job, loaded, err := lifecycle.queryJob(ctx, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if loaded {
		return lifecycleStatus{}, fmt.Errorf(
			"refusing loaded launchd service %s without its managed plist (path %q)",
			status.Service,
			job.path,
		)
	}
	return status, nil
}

func (lifecycle *launchdLifecycle) rollbackFailedBootout(
	publication publishedLaunchdPlist,
	status lifecycleStatus,
	previous launchdJobState,
	cause error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	job, loaded, err := lifecycle.queryJob(ctx, status.Service)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("inspect launchd service after failed bootout: %w", err))
	}
	if loaded {
		if filepath.Clean(job.path) != filepath.Clean(status.UnitPath) || job.domain != previous.domain {
			return errors.Join(cause, errors.New("launchd service identity changed after failed bootout"))
		}
		return cause
	}
	return lifecycle.restoreLaunchdLoadedState(ctx, publication, status, previous, cause)
}

func (lifecycle *launchdLifecycle) rollbackLaunchdLoadedState(
	publication publishedLaunchdPlist,
	status lifecycleStatus,
	previous launchdJobState,
	cause error,
) error {
	if previous.domain == "" {
		return cause
	}
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	return lifecycle.restoreLaunchdLoadedState(ctx, publication, status, previous, cause)
}

func (lifecycle *launchdLifecycle) rollbackLaunchdUninstall(
	quarantined publishedLaunchdPlist,
	status lifecycleStatus,
	previous launchdJobState,
	cause error,
) error {
	restored, err := restoreQuarantinedLaunchdPlist(quarantined, filepath.Base(status.UnitPath))
	if err != nil {
		return errors.Join(cause, fmt.Errorf("restore launchd plist: %w", err))
	}
	return lifecycle.rollbackLaunchdLoadedState(restored, status, previous, cause)
}

func (lifecycle *launchdLifecycle) restoreLaunchdLoadedState(
	ctx context.Context,
	publication publishedLaunchdPlist,
	status lifecycleStatus,
	previous launchdJobState,
	cause error,
) error {
	if err := requirePublishedLaunchdPlist(publication); err != nil {
		return errors.Join(cause, fmt.Errorf("verify launchd plist during rollback: %w", err))
	}
	if err := lifecycle.requireSuccess(ctx, "bootstrap", previous.domain, status.UnitPath); err != nil {
		return errors.Join(cause, fmt.Errorf("restore launchd service: %w", err))
	}
	job, loaded, err := lifecycle.queryJob(ctx, status.Service)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("verify restored launchd service: %w", err))
	}
	if !loaded || job.domain != previous.domain ||
		filepath.Clean(job.path) != filepath.Clean(status.UnitPath) {
		return errors.Join(cause, errors.New("restored launchd service has unexpected identity"))
	}
	return cause
}
