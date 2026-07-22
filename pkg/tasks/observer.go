package tasks

import (
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
	observer EventObserver
}

type queuedObservation struct {
	observation EventObservation
	observers   []observerEntry
}

var taskObserverSequence atomic.Uint64

// SubscribeEvents observes newly persisted task events. The callback runs
// after the registry lock is released and must not be used as task authority.
func (r *Registry) SubscribeEvents(observer EventObserver) func() {
	if r == nil || observer == nil {
		return func() {}
	}
	entry := observerEntry{id: taskObserverSequence.Add(1), observer: observer}
	r.mu.Lock()
	r.observers = append(r.observers, entry)
	r.mu.Unlock()
	return r.unsubscribeEventObserver(entry.id)
}

// SubscribeSnapshot atomically installs an observer and captures retained
// state. Events committed before the boundary appear only in the snapshot;
// later commits are delivered through the observer in commit order.
func (r *Registry) SubscribeSnapshot(
	observer EventObserver,
) (ObservationSnapshot, func()) {
	if r == nil || observer == nil {
		return ObservationSnapshot{}, func() {}
	}
	entry := observerEntry{id: taskObserverSequence.Add(1), observer: observer}
	r.mu.Lock()
	r.observers = append(r.observers, entry)
	snapshot := r.observationSnapshotLocked()
	r.mu.Unlock()
	return snapshot, r.unsubscribeEventObserver(entry.id)
}

func (r *Registry) unsubscribeEventObserver(id uint64) func() {
	return func() {
		r.mu.Lock()
		for i := range r.observers {
			if r.observers[i].id != id {
				continue
			}
			r.observers = append(r.observers[:i], r.observers[i+1:]...)
			break
		}
		r.mu.Unlock()
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

func (r *Registry) queueNotificationsLocked(events []TaskEvent) bool {
	if r == nil || len(events) == 0 {
		return false
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
	for i, event := range events {
		r.notifications = append(r.notifications, queuedObservation{
			observation: EventObservation{
				Event: cloneTaskEvent(event), Record: records[event.TaskID],
				FinalForTask: lastByTask[event.TaskID] == i,
			},
			observers: observers,
		})
	}
	if r.notifying {
		return false
	}
	r.notifying = true
	return true
}

func (r *Registry) drainNotifications() {
	for {
		r.mu.Lock()
		if len(r.notifications) == 0 {
			r.notifying = false
			r.mu.Unlock()
			return
		}
		queued := r.notifications[0]
		r.notifications[0] = queuedObservation{}
		r.notifications = r.notifications[1:]
		r.mu.Unlock()
		for _, entry := range queued.observers {
			notifyObserver(entry.observer, queued.observation)
		}
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
