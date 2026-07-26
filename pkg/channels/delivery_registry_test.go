package channels

import "testing"

func TestDeliveryRegistryInstallAndSnapshot(t *testing.T) {
	registry := newDeliveryRegistry()
	owner := newDeliveryOwner("test", &mockChannel{}, "test")

	registry.install(owner)

	if registry.workerCount() != 1 || !registry.hasActiveWorker("test") {
		t.Fatalf("installed registry state = %+v", registry)
	}
	if got := registry.owner("test", owner.ch); got != owner {
		t.Fatalf("owner() = %p, want %p", got, owner)
	}
	targets := registry.snapshot()
	if len(targets) != 1 || targets[0].owner != owner || targets[0].worker != owner.Worker() {
		t.Fatalf("snapshot() = %+v", targets)
	}
}

func TestDeliveryRegistryProvidesLegacyWorkerOwner(t *testing.T) {
	registry := newDeliveryRegistry()
	channel := &mockChannel{}
	worker := newChannelWorker("legacy", channel, "test")
	registry.workers["legacy"] = worker

	owner := registry.owner("legacy", channel)

	if owner == nil || owner.Worker() != worker || owner.ch != channel {
		t.Fatalf("legacy owner = %+v", owner)
	}
}

func TestDeliveryRegistryConditionalRemovePreservesReplacement(t *testing.T) {
	registry := newDeliveryRegistry()
	oldOwner := newDeliveryOwner("test", &mockChannel{}, "test")
	newOwner := newDeliveryOwner("test", &mockChannel{}, "test")
	registry.install(newOwner)

	registry.removeIfMatches("test", oldOwner, oldOwner.Worker())

	if registry.deliveryOwners["test"] != newOwner || registry.workers["test"] != newOwner.Worker() {
		t.Fatal("conditional remove deleted replacement delivery state")
	}

	registry.removeIfMatches("test", newOwner, newOwner.Worker())
	if registry.workerCount() != 0 || registry.deliveryOwners["test"] != nil {
		t.Fatalf("matching remove left registry state = %+v", registry)
	}
}
