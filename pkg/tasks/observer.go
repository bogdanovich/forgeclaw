package tasks

import (
	"sync"
	"sync/atomic"

	"github.com/sipeed/picoclaw/pkg/logger"
)

type EventObservation struct {
	Event        TaskEvent
	Record       Record
	FinalForTask bool
}

type EventObserver func(EventObservation)

// ObservationSnapshot is the retained registry state at an atomic observer
// subscription boundary. Earlier commits appear only here; later commits are
// delivered through the observer in registry commit order.
type ObservationSnapshot struct {
	Records []Record
	Events  []TaskEvent
}

type observerEntry struct {
	id       uint64
	delivery *eventObserverDelivery
}

var taskObserverSequence atomic.Uint64

type eventObserverDelivery struct {
	mu        sync.Mutex
	observer  EventObserver
	active    bool
	closed    bool
	notifying bool
	pending   []EventObservation
}

// SubscribeEvents observes newly persisted task events. The callback runs
// after the registry lock is released and must not be used as task authority.
func (r *Registry) SubscribeEvents(observer EventObserver) func() {
	if r == nil || observer == nil {
		return func() {}
	}
	entry := observerEntry{
		id: taskObserverSequence.Add(1), delivery: newEventObserverDelivery(observer, true),
	}
	r.mu.Lock()
	r.observers = append(r.observers, entry)
	r.mu.Unlock()
	return r.unsubscribeEventObserver(entry)
}

// SubscribeSnapshot atomically installs a gated observer and captures retained
// state. Events committed before the boundary appear only in the snapshot;
// later commits are buffered in order until the caller applies the snapshot
// and invokes the returned activate function.
func (r *Registry) SubscribeSnapshot(
	observer EventObserver,
) (ObservationSnapshot, func(), func()) {
	if r == nil || observer == nil {
		return ObservationSnapshot{}, func() {}, func() {}
	}
	entry := observerEntry{
		id: taskObserverSequence.Add(1), delivery: newEventObserverDelivery(observer, false),
	}
	r.mu.Lock()
	r.observers = append(r.observers, entry)
	snapshot := r.observationSnapshotLocked()
	r.mu.Unlock()
	return snapshot, entry.delivery.activate, r.unsubscribeEventObserver(entry)
}

func (r *Registry) unsubscribeEventObserver(entry observerEntry) func() {
	return func() {
		r.mu.Lock()
		for i := range r.observers {
			if r.observers[i].id != entry.id {
				continue
			}
			r.observers = append(r.observers[:i], r.observers[i+1:]...)
			break
		}
		r.mu.Unlock()
		entry.delivery.close()
	}
}

func (r *Registry) observationSnapshotLocked() ObservationSnapshot {
	snapshot := r.snapshotLocked()
	records := make([]Record, len(snapshot.Tasks))
	for i := range snapshot.Tasks {
		records[i] = cloneTaskRecord(snapshot.Tasks[i])
	}
	events := make([]TaskEvent, len(snapshot.Events))
	for i := range snapshot.Events {
		events[i] = cloneTaskEvent(snapshot.Events[i])
	}
	return ObservationSnapshot{Records: records, Events: events}
}

func (r *Registry) eventsSinceLocked(start int) []TaskEvent {
	if start < 0 || start >= len(r.events) {
		return nil
	}
	return append([]TaskEvent(nil), r.events[start:]...)
}

func (r *Registry) queueNotificationsAfterCommitLocked(
	commitErr error,
	events []TaskEvent,
) []*eventObserverDelivery {
	if r == nil || commitErr != nil || len(events) == 0 {
		return nil
	}
	observers := append([]observerEntry(nil), r.observers...)
	records := make(map[string]Record, len(events))
	for _, event := range events {
		records[event.TaskID] = cloneTaskRecord(r.records[event.TaskID])
	}
	lastByTask := make(map[string]int, len(events))
	for i, event := range events {
		lastByTask[event.TaskID] = i
	}
	var deliveries []*eventObserverDelivery
	for i, event := range events {
		observation := EventObservation{
			Event: cloneTaskEvent(event), Record: records[event.TaskID],
			FinalForTask: lastByTask[event.TaskID] == i,
		}
		for _, entry := range observers {
			if entry.delivery.queue(observation) {
				deliveries = append(deliveries, entry.delivery)
			}
		}
	}
	return deliveries
}

func drainEventObservers(deliveries []*eventObserverDelivery) {
	for _, delivery := range deliveries {
		delivery.drain()
	}
}

func newEventObserverDelivery(
	observer EventObserver,
	active bool,
) *eventObserverDelivery {
	return &eventObserverDelivery{observer: observer, active: active}
}

func (d *eventObserverDelivery) queue(observation EventObservation) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return false
	}
	d.pending = append(d.pending, observation)
	drain := d.active && !d.notifying
	if drain {
		d.notifying = true
	}
	return drain
}

func (d *eventObserverDelivery) activate() {
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

func (d *eventObserverDelivery) close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.closed = true
	d.pending = nil
	d.mu.Unlock()
}

func (d *eventObserverDelivery) drain() {
	for {
		d.mu.Lock()
		if d.closed || !d.active || len(d.pending) == 0 {
			d.notifying = false
			d.mu.Unlock()
			return
		}
		observation := d.pending[0]
		d.pending[0] = EventObservation{}
		d.pending = d.pending[1:]
		observer := d.observer
		d.mu.Unlock()
		notifyObserver(observer, observation)
	}
}

func cloneTaskRecord(record Record) Record {
	cloned := record
	if record.Completion != nil {
		completion := *record.Completion
		completion.Media = append([]CompletionMedia(nil), record.Completion.Media...)
		cloned.Completion = &completion
	}
	if record.Deliverable != nil {
		deliverable := *record.Deliverable
		deliverable.Artifacts = append([]DeliverableItem(nil), record.Deliverable.Artifacts...)
		deliverable.Metadata = copyStringMap(record.Deliverable.Metadata)
		deliverable.Report = cloneDeliverableReport(record.Deliverable.Report)
		cloned.Deliverable = &deliverable
	}
	return cloned
}

func cloneTaskEvent(event TaskEvent) TaskEvent {
	cloned := event
	cloned.Payload = copyStringMap(event.Payload)
	return cloned
}

func notifyObserver(observer EventObserver, observation EventObservation) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.WarnCF("tasks", "Recovered task event observer panic", map[string]any{
				"event_id": observation.Event.EventID,
			})
		}
	}()
	observer(observation)
}
