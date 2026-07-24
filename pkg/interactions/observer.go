package interactions

import "sync"

type observerDelivery struct {
	mu        sync.Mutex
	observer  Observer
	active    bool
	closed    bool
	notifying bool
	pending   []EventObservation
}

func newObserverDelivery(observer Observer, active bool) *observerDelivery {
	return &observerDelivery{observer: observer, active: active}
}

func (d *observerDelivery) enqueue(observation EventObservation) {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.pending = append(d.pending, observation)
	drain := d.active && !d.notifying
	if drain {
		d.notifying = true
	}
	d.mu.Unlock()
	if drain {
		d.drain()
	}
}

func (d *observerDelivery) activate() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.closed || d.active {
		d.mu.Unlock()
		return
	}
	d.active = true
	drain := len(d.pending) > 0 && !d.notifying
	if drain {
		d.notifying = true
	}
	d.mu.Unlock()
	if drain {
		d.drain()
	}
}

func (d *observerDelivery) unsubscribe() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.active {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.pending = nil
	d.notifying = false
	d.mu.Unlock()
}

func (d *observerDelivery) drain() {
	for {
		observation, observer, ok := d.claimNext()
		if !ok {
			return
		}
		notifyObserver(observer, observation)
	}
}

func (d *observerDelivery) claimNext() (EventObservation, Observer, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || !d.active || len(d.pending) == 0 {
		d.notifying = false
		return EventObservation{}, nil, false
	}
	observation := d.pending[0]
	d.pending[0] = EventObservation{}
	d.pending = d.pending[1:]
	return observation, d.observer, true
}
