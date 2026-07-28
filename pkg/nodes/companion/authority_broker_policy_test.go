package companion

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestAuthorityBrokerPolicyProjectsOnlySafeAliases(t *testing.T) {
	config := validAuthorityBrokerConfig(t)
	snapshot, err := config.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != "broker-v1" || len(snapshot.Profiles) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	profile := snapshot.Profiles[0]
	if profile.Alias != "owner-root" ||
		profile.Revision != "profile-v1" ||
		!slices.Equal(profile.WorkingScopes, []string{"workspace"}) ||
		!slices.Equal(profile.EnvironmentNames, []string{"LANG"}) {
		t.Fatalf("profile projection = %#v", profile)
	}
}

func TestAuthorityBrokerPolicyResolvesExecutionWithoutCallerAuthority(t *testing.T) {
	config := validAuthorityBrokerConfig(t)
	prepared, err := config.prepareExecution(ShellBrokerRequest{
		InvocationID: "inv_test", PlanHash: strings.Repeat("a", 64),
		Profile: "owner-root", ProfileRevision: "profile-v1",
		Script: "id -u", WorkingScope: "workspace",
		Environment:    map[string]string{"LANG": "C"},
		TimeoutSeconds: 5, OutputBytesMax: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := config.normalizedProfile["owner-root"]
	if prepared.shellPath != profile.ShellPath ||
		prepared.workingDirectory != profile.WorkingScopes["workspace"] ||
		prepared.profile.UID != 0 ||
		!slices.Equal(prepared.shellArguments, []string{"-c", "id -u"}) ||
		!slices.Equal(prepared.environment, []string{"LANG=C", "PATH=/usr/bin"}) {
		t.Fatalf("prepared execution = %#v", prepared)
	}
}

func TestAuthorityBrokerPolicyRejectsAlteredRequests(t *testing.T) {
	config := validAuthorityBrokerConfig(t)
	valid := ShellBrokerRequest{
		InvocationID: "inv_test", PlanHash: strings.Repeat("a", 64),
		Profile: "owner-root", ProfileRevision: "profile-v1",
		Script: "true", WorkingScope: "workspace",
		Environment:    map[string]string{"LANG": "C"},
		TimeoutSeconds: 5, OutputBytesMax: 4096,
	}
	tests := []struct {
		name   string
		mutate func(*ShellBrokerRequest)
	}{
		{name: "profile", mutate: func(request *ShellBrokerRequest) {
			request.Profile = "invented"
		}},
		{name: "revision", mutate: func(request *ShellBrokerRequest) {
			request.ProfileRevision = "stale"
		}},
		{name: "scope", mutate: func(request *ShellBrokerRequest) {
			request.WorkingScope = "/"
		}},
		{name: "environment", mutate: func(request *ShellBrokerRequest) {
			request.Environment = map[string]string{"PATH": "/tmp"}
		}},
		{name: "timeout", mutate: func(request *ShellBrokerRequest) {
			request.TimeoutSeconds = 31
		}},
		{name: "output", mutate: func(request *ShellBrokerRequest) {
			request.OutputBytesMax = 8193
		}},
		{name: "script", mutate: func(request *ShellBrokerRequest) {
			request.Script = strings.Repeat("界", 21846)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			request.Environment = map[string]string{"LANG": "C"}
			test.mutate(&request)
			if _, err := config.prepareExecution(request); err == nil {
				t.Fatal("altered broker request was accepted")
			}
		})
	}
}

func TestAuthorityBrokerPolicyFailsClosed(t *testing.T) {
	base := t.TempDir()
	shell := filepath.Join(base, "shell")
	if err := os.WriteFile(shell, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	validProfile := AuthorityBrokerProfile{
		Revision: "profile-v1", ShellPath: shell, UID: 0, GID: 0,
		WorkingScopes:             map[string]string{"workspace": root},
		FixedEnvironment:          map[string]string{"PATH": "/usr/bin"},
		PermittedEnvironmentNames: []string{},
		Network:                   "inherit", TimeoutSecondsMax: 30,
		OutputBytesMax: 8192, ConcurrentCommands: 1,
	}
	tests := []struct {
		name   string
		mutate func(*AuthorityBrokerConfig)
	}{
		{name: "missing profile", mutate: func(config *AuthorityBrokerConfig) {
			config.Profiles = nil
		}},
		{name: "invalid revision", mutate: func(config *AuthorityBrokerConfig) {
			config.Revision = "../broker"
		}},
		{name: "root peer", mutate: func(config *AuthorityBrokerConfig) {
			config.AllowedUID = 0
		}},
		{name: "network claim", mutate: func(config *AuthorityBrokerConfig) {
			profile := config.Profiles["owner-root"]
			profile.Network = "isolated"
			config.Profiles["owner-root"] = profile
		}},
		{name: "unknown shell", mutate: func(config *AuthorityBrokerConfig) {
			profile := config.Profiles["owner-root"]
			profile.ShellPath = filepath.Join(base, "missing")
			config.Profiles["owner-root"] = profile
		}},
		{name: "fixed supplied collision", mutate: func(config *AuthorityBrokerConfig) {
			profile := config.Profiles["owner-root"]
			profile.PermittedEnvironmentNames = []string{"PATH"}
			config.Profiles["owner-root"] = profile
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := AuthorityBrokerConfig{
				SocketPath: filepath.Join(base, "broker.sock"),
				AllowedUID: uint32(os.Getuid()),
				AllowedGID: uint32(os.Getgid()),
				Revision:   "broker-v1",
				Profiles:   map[string]AuthorityBrokerProfile{"owner-root": validProfile},
			}
			test.mutate(&config)
			if _, err := NormalizeAuthorityBrokerConfig(config, base); err == nil {
				t.Fatal("invalid broker policy was accepted")
			}
		})
	}
}

func validAuthorityBrokerConfig(t *testing.T) AuthorityBrokerConfig {
	t.Helper()
	base := t.TempDir()
	shell := filepath.Join(base, "shell")
	if err := os.WriteFile(shell, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	config, err := NormalizeAuthorityBrokerConfig(AuthorityBrokerConfig{
		SocketPath: filepath.Join(base, "broker.sock"),
		AllowedUID: uint32(os.Getuid()),
		AllowedGID: uint32(os.Getgid()),
		Revision:   "broker-v1",
		Profiles: map[string]AuthorityBrokerProfile{
			"owner-root": {
				Revision: "profile-v1", ShellPath: shell,
				UID: 0, GID: 0, WorkingScopes: map[string]string{"workspace": root},
				FixedEnvironment:          map[string]string{"PATH": "/usr/bin"},
				PermittedEnvironmentNames: []string{"LANG"},
				Network:                   "inherit", TimeoutSecondsMax: 30,
				OutputBytesMax: 8192, ConcurrentCommands: 1,
			},
		},
	}, base)
	if err != nil {
		t.Fatal(err)
	}
	return config
}
