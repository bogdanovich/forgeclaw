package channels

// deliveryRegistry owns the worker and delivery-owner indexes. Manager's mutex
// protects registry access while channel lifecycle operations are in progress.
// The embedded maps remain promoted temporarily for package-test compatibility.
type deliveryRegistry struct {
	workers        map[string]*channelWorker
	deliveryOwners map[string]*deliveryOwner
}

type deliveryCloseTarget struct {
	owner  *deliveryOwner
	worker *channelWorker
}

func newDeliveryRegistry() deliveryRegistry {
	return deliveryRegistry{
		workers:        make(map[string]*channelWorker),
		deliveryOwners: make(map[string]*deliveryOwner),
	}
}

func (r *deliveryRegistry) ensureInitialized() {
	if r.workers == nil {
		r.workers = make(map[string]*channelWorker)
	}
	if r.deliveryOwners == nil {
		r.deliveryOwners = make(map[string]*deliveryOwner)
	}
}

func (r *deliveryRegistry) install(owner *deliveryOwner) {
	if owner == nil {
		return
	}
	r.ensureInitialized()
	r.workers[owner.name] = owner.Worker()
	r.deliveryOwners[owner.name] = owner
}

func (r *deliveryRegistry) workerCount() int {
	return len(r.workers)
}

func (r *deliveryRegistry) hasActiveWorker(name string) bool {
	return r.workers[name] != nil
}

func (r *deliveryRegistry) snapshot() []deliveryCloseTarget {
	targets := make([]deliveryCloseTarget, 0, len(r.workers))
	for name, worker := range r.workers {
		targets = append(targets, deliveryCloseTarget{
			owner:  r.deliveryOwners[name],
			worker: worker,
		})
	}
	return targets
}

func (r *deliveryRegistry) owner(name string, channel Channel) *deliveryOwner {
	if owner := r.deliveryOwners[name]; owner != nil {
		return owner
	}
	return deliveryOwnerFromWorker(name, channel, r.workers[name])
}

func (r *deliveryRegistry) lookup(name string) (*deliveryOwner, *channelWorker) {
	return r.deliveryOwners[name], r.workers[name]
}

func (r *deliveryRegistry) removeWorkerIfUnowned(name string) {
	if r.deliveryOwners[name] == nil {
		delete(r.workers, name)
	}
}

func (r *deliveryRegistry) removeIfMatches(
	name string,
	owner *deliveryOwner,
	worker *channelWorker,
) {
	if owner != nil && r.deliveryOwners[name] == owner {
		delete(r.deliveryOwners, name)
	}
	if worker != nil && r.workers[name] == worker {
		delete(r.workers, name)
	}
}
