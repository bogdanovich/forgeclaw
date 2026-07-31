//go:build linux || darwin

package companion

import (
	"path/filepath"
	"testing"
)

func TestFileHelperServiceConfigRequiresOneApprovedProfile(t *testing.T) {
	root := canonicalTempDir(t)
	base := FileHelperServiceConfig{
		SocketPath: filepath.Join(root, "helper.sock"),
		StateDir:   filepath.Join(root, "state"),
		AllowedUID: 1000,
		AllowedGID: 1000,
		Profiles:   normalizedFilePoliciesForTest(t, "server-admin", "server-admin-v1", root),
	}
	config, err := NormalizeFileHelperServiceConfig(base, root)
	if err != nil {
		t.Fatal(err)
	}
	if !config.normalized || len(config.Profiles) != 1 {
		t.Fatalf("normalized helper config = %#v", config)
	}
	descriptors, err := config.Descriptors()
	if err != nil || len(descriptors) != 3 ||
		descriptors[0].FileProfiles[0].Alias != "server-admin" {
		t.Fatalf("helper descriptors = (%#v, %v)", descriptors, err)
	}

	missingApproval := base
	missingApproval.Profiles = normalizedFilePoliciesForTest(
		t,
		"server-admin",
		"server-admin-v2",
		root,
	)
	profile := missingApproval.Profiles["server-admin"]
	profile.Approval.Read = FileApprovalNone
	missingApproval.Profiles["server-admin"] = profile
	if _, err := NormalizeFileHelperServiceConfig(missingApproval, root); err == nil {
		t.Fatal("approval-free helper read authority was accepted")
	}

	disabled := base
	profile = disabled.Profiles["server-admin"]
	profile.Enabled = false
	profile.ReadableRoots = nil
	profile.WritableRoots = nil
	profile.AllowCreate = false
	profile.AllowOverwrite = false
	disabled.Profiles = FilePolicies{"server-admin": profile}
	if _, err := NormalizeFileHelperServiceConfig(disabled, root); err == nil {
		t.Fatal("disabled helper profile was accepted")
	}

	multiple := base
	multiple.Profiles = FilePolicies{
		"server-admin": base.Profiles["server-admin"],
		"other": {
			Enabled:       true,
			Revision:      "other-v1",
			ReadableRoots: []string{root},
			Approval: FileApprovalPolicy{
				Read:  FileApprovalRequired,
				Write: FileApprovalRequired,
			},
		},
	}
	if _, err := NormalizeFileHelperServiceConfig(multiple, root); err == nil {
		t.Fatal("multiple helper profiles were accepted")
	}
}
