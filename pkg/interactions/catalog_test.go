package interactions

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWorkspaceCatalogRoundTrip(t *testing.T) {
	root := t.TempDir()
	catalog := NewWorkspaceCatalog(root)
	workspaceA := filepath.Join(root, "workspace-a")
	workspaceB := filepath.Join(root, "workspace-b")

	if err := catalog.Register(workspaceB); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(workspaceA); err != nil {
		t.Fatal(err)
	}
	workspaces, err := catalog.List()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{workspaceA, workspaceB}; !reflect.DeepEqual(workspaces, want) {
		t.Fatalf("List() = %#v, want %#v", workspaces, want)
	}
	if err = catalog.Remove(workspaceA); err != nil {
		t.Fatal(err)
	}
	workspaces, err = catalog.List()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{workspaceB}; !reflect.DeepEqual(workspaces, want) {
		t.Fatalf("List() after Remove = %#v, want %#v", workspaces, want)
	}
}

func TestWorkspaceCatalogRetainsValidEntriesWithMalformedNeighbor(t *testing.T) {
	root := t.TempDir()
	catalog := NewWorkspaceCatalog(root)
	workspace := filepath.Join(root, "workspace")
	if err := catalog.Register(workspace); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalog.directory, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspaces, err := catalog.List()
	if err == nil {
		t.Fatal("List() error = nil, want malformed-entry diagnostic")
	}
	if want := []string{workspace}; !reflect.DeepEqual(workspaces, want) {
		t.Fatalf("List() = %#v, want %#v", workspaces, want)
	}
}
