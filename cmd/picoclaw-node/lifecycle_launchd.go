package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const managedLaunchdPlistMarker = "<!-- Managed by ForgeClaw picoclaw-node lifecycle v1 -->"

type launchdRunResult struct {
	Output   string
	ExitCode int
}

type launchdRunner func(context.Context, ...string) (launchdRunResult, error)

type launchdLifecycle struct {
	system   bool
	plistDir string
	domains  []string
	run      launchdRunner
}

type launchdPlistState struct {
	exists  bool
	managed bool
}

type launchdJobState struct {
	path  string
	state string
}

func (lifecycle *launchdLifecycle) Install(
	context.Context,
	lifecycleRequest,
) (lifecycleStatus, error) {
	return lifecycleStatus{}, errors.New("launchd installation is not implemented yet")
}

func (lifecycle *launchdLifecycle) Uninstall(
	context.Context,
	lifecycleRequest,
) (lifecycleStatus, error) {
	return lifecycleStatus{}, errors.New("launchd uninstallation is not implemented yet")
}

func (lifecycle *launchdLifecycle) Status(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	plist, err := captureLaunchdPlist(status.UnitPath)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if plist.exists && !plist.managed {
		return lifecycleStatus{}, fmt.Errorf(
			"refusing to manage unowned launchd plist %s",
			status.UnitPath,
		)
	}

	job, loaded, err := lifecycle.queryJob(ctx, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if loaded {
		if !plist.exists {
			return lifecycleStatus{}, fmt.Errorf(
				"refusing loaded launchd service %s without its managed plist",
				status.Service,
			)
		}
		if filepath.Clean(job.path) != filepath.Clean(status.UnitPath) {
			return lifecycleStatus{}, fmt.Errorf(
				"refusing launchd service %s resolved outside its managed plist (path %q)",
				status.Service,
				job.path,
			)
		}
		status.Installed = true
		status.State = job.state
		status.Active = job.state == "running"
		return status, nil
	}
	if plist.exists {
		status.Installed = true
		status.State = "not-loaded"
	}
	return status, nil
}

func (lifecycle *launchdLifecycle) baseStatus(instance string) lifecycleStatus {
	service := "com.forgeclaw.picoclaw-node." + instance
	scope := "user"
	if lifecycle.system {
		scope = "system"
	}
	return lifecycleStatus{
		Instance: instance,
		Manager:  "launchd",
		Scope:    scope,
		Service:  service,
		UnitPath: filepath.Join(lifecycle.plistDir, service+".plist"),
		State:    "not-installed",
	}
}

func (lifecycle *launchdLifecycle) queryJob(
	ctx context.Context,
	service string,
) (launchdJobState, bool, error) {
	var loaded []launchdJobState
	inspected := false
	for _, domain := range lifecycle.domains {
		target := domain + "/" + service
		result, err := lifecycle.run(ctx, "print", target)
		if err != nil {
			return launchdJobState{}, false, err
		}
		if result.ExitCode != 0 {
			if launchdJobMissing(result) {
				inspected = true
				continue
			}
			if launchdDomainUnavailable(result) || launchdOptionalDomainMissing(domain, result) {
				continue
			}
			return launchdJobState{}, false, fmt.Errorf(
				"launchctl print %s failed with exit code %d: %s",
				target,
				result.ExitCode,
				result.Output,
			)
		}
		job, err := parseLaunchdJob(target, result.Output)
		if err != nil {
			return launchdJobState{}, false, err
		}
		inspected = true
		loaded = append(loaded, job)
	}
	if len(loaded) > 1 {
		return launchdJobState{}, false, fmt.Errorf(
			"launchd service %s is loaded in multiple domains",
			service,
		)
	}
	if len(loaded) == 0 {
		if !inspected {
			return launchdJobState{}, false, fmt.Errorf(
				"launchd service %s could not be inspected in any candidate domain",
				service,
			)
		}
		return launchdJobState{}, false, nil
	}
	return loaded[0], true, nil
}

func parseLaunchdJob(target, output string) (launchdJobState, error) {
	lines := strings.Split(output, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != target+" = {" {
		return launchdJobState{}, fmt.Errorf(
			"launchctl print %s returned an unexpected service identity",
			target,
		)
	}
	values := make(map[string]string, 2)
	depth := 1
	for _, line := range lines[1:] {
		trimmed := strings.TrimSpace(line)
		if trimmed == "}" {
			depth--
			if depth < 0 {
				return launchdJobState{}, fmt.Errorf(
					"launchctl print %s returned unbalanced output",
					target,
				)
			}
			continue
		}
		if depth == 1 {
			key, value, found := strings.Cut(trimmed, " = ")
			if found && (key == "path" || key == "state") {
				if _, duplicate := values[key]; duplicate {
					return launchdJobState{}, fmt.Errorf(
						"launchctl print %s returned duplicate %s",
						target,
						key,
					)
				}
				values[key] = strings.TrimSpace(value)
			}
		}
		if strings.HasSuffix(trimmed, " = {") {
			depth++
		}
	}
	if depth != 0 {
		return launchdJobState{}, fmt.Errorf(
			"launchctl print %s returned unbalanced output",
			target,
		)
	}
	if values["path"] == "" || values["state"] == "" {
		return launchdJobState{}, fmt.Errorf(
			"launchctl print %s omitted path or state",
			target,
		)
	}
	return launchdJobState{path: values["path"], state: values["state"]}, nil
}

func launchdJobMissing(result launchdRunResult) bool {
	return (result.ExitCode == 3 || result.ExitCode == 113) &&
		strings.Contains(result.Output, "Could not find service")
}

func launchdDomainUnavailable(result launchdRunResult) bool {
	return (result.ExitCode == 5 || result.ExitCode == 125) &&
		strings.Contains(result.Output, "Domain does not support specified action")
}

func launchdOptionalDomainMissing(domain string, result launchdRunResult) bool {
	uid, optional := strings.CutPrefix(domain, "gui/")
	return optional && result.ExitCode != 0 &&
		strings.Contains(result.Output, "Could not find domain for user gui: "+uid)
}

func captureLaunchdPlist(path string) (launchdPlistState, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return launchdPlistState{}, nil
	}
	if err != nil {
		return launchdPlistState{}, fmt.Errorf("inspect existing launchd plist: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 1024*1024 {
		return launchdPlistState{}, errors.New(
			"existing launchd plist is not a bounded regular file",
		)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return launchdPlistState{}, fmt.Errorf("read existing launchd plist: %w", err)
	}
	return launchdPlistState{
		exists:  true,
		managed: hasLaunchdPlistMarker(data),
	}, nil
}

func hasLaunchdPlistMarker(data []byte) bool {
	for _, line := range bytes.Split(data, []byte("\n")) {
		if string(line) == managedLaunchdPlistMarker {
			return true
		}
	}
	return false
}
