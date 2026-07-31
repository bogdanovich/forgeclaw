//go:build linux || darwin

package companion

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

func TestFileTransferRouterMergesProfilesAndRoutesByRevision(t *testing.T) {
	projectRuntime, _, _ := newTestFileTransferRuntime(t)
	adminRoot := canonicalTempDir(t)
	adminPolicies := normalizedFilePoliciesForTest(t, "server-admin", "server-admin-v1", adminRoot)
	adminRuntime, err := NewFileTransferRuntime(adminPolicies, newMemoryFileTransferLedger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminRuntime.Close)
	router, err := NewFileTransferRouter(projectRuntime, adminRuntime)
	if err != nil {
		t.Fatal(err)
	}
	descriptors := router.Descriptors()
	if len(descriptors) != 3 {
		t.Fatalf("router descriptors = %#v", descriptors)
	}
	for _, descriptor := range descriptors {
		if !slices.Equal(
			descriptor.ModelContract.Constraints.ProfileAliases,
			[]string{"project", "server-admin"},
		) || len(descriptor.FileProfiles) != 2 {
			t.Fatalf("merged descriptor = %#v", descriptor)
		}
	}
	path := filepath.Join(adminRoot, "root-owned.conf")
	if err := os.WriteFile(path, []byte("admin fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	prepare := testFilePrepareFrame(
		t,
		"router_admin_info",
		protocol.TransferDownload,
		emptyTransferDigest,
		0,
		fileTransferPrepare{
			Operation: fileOperationInfo,
			Path:      path,
			ExpiresAt: time.Now().Add(time.Minute).Unix(),
		},
	)
	prepare.PolicyRevision = "server-admin-v1"
	responses := collectRouterResponses(t, router, prepare)
	if len(responses) != 1 || responses[0].Type != protocol.TransferFrameCommitted {
		t.Fatalf("admin route responses = %#v", responses)
	}
	prepare.TransferID = "router_unknown"
	prepare.PolicyRevision = "unknown-v1"
	responses = collectRouterResponses(t, router, prepare)
	if len(responses) != 1 || responses[0].Type != protocol.TransferFrameDeny {
		t.Fatalf("unknown route responses = %#v", responses)
	}
}

func TestFileTransferRouterRejectsAliasAndRevisionCollisions(t *testing.T) {
	runtime, _, _ := newTestFileTransferRuntime(t)
	base := runtime.Descriptors()
	aliasCollision := cloneFileCapabilityDescriptors(base)
	for index := range aliasCollision {
		aliasCollision[index].FileProfiles[0].Alias = "PROJECT"
		aliasCollision[index].ModelContract.Constraints.ProfileAliases = []string{"PROJECT"}
	}
	if _, err := NewFileTransferRouter(
		runtime,
		staticFileCapability{descriptors: aliasCollision},
	); err == nil {
		t.Fatal("case-colliding helper profile was accepted")
	}
	revisionCollision := cloneFileCapabilityDescriptors(base)
	for index := range revisionCollision {
		revisionCollision[index].FileProfiles[0].Alias = "other"
		revisionCollision[index].ModelContract.Constraints.ProfileAliases = []string{"other"}
	}
	if _, err := NewFileTransferRouter(
		runtime,
		staticFileCapability{descriptors: revisionCollision},
	); err == nil {
		t.Fatal("duplicate helper profile revision was accepted")
	}
}

type staticFileCapability struct {
	descriptors []nodes.CommandDescriptor
}

func (capability staticFileCapability) Descriptors() []nodes.CommandDescriptor {
	return cloneFileCapabilityDescriptors(capability.descriptors)
}

func (staticFileCapability) HandleTransferFrame(
	context.Context,
	protocol.TransferFrame,
	func(protocol.TransferFrame) error,
) error {
	return nil
}

func normalizedFilePoliciesForTest(
	t *testing.T,
	alias string,
	revision string,
	root string,
) FilePolicies {
	t.Helper()
	config, err := (Config{
		GatewayURL: "wss://gateway.example",
		FilePolicies: FilePolicies{
			alias: {
				Enabled:        true,
				Revision:       revision,
				ReadableRoots:  []string{root},
				WritableRoots:  []string{root},
				AllowCreate:    true,
				AllowOverwrite: true,
				Approval: FileApprovalPolicy{
					Read:  FileApprovalRequired,
					Write: FileApprovalRequired,
				},
			},
		},
	}).Normalize(root)
	if err != nil {
		t.Fatal(err)
	}
	return config.FilePolicies
}

func collectRouterResponses(
	t *testing.T,
	router *FileTransferRouter,
	frame protocol.TransferFrame,
) []protocol.TransferFrame {
	t.Helper()
	var responses []protocol.TransferFrame
	if err := router.HandleTransferFrame(
		t.Context(),
		frame,
		func(response protocol.TransferFrame) error {
			responses = append(responses, response)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	return responses
}
