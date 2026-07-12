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

type observerEntry struct {
	id       uint64
	observer EventObserver
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
	}
}

func (r *Registry) eventsSinceLocked(start int) []TaskEvent {
	if start < 0 || start >= len(r.events) {
		return nil
	}
	return append([]TaskEvent(nil), r.events[start:]...)
}

func (r *Registry) notifyEvents(events []TaskEvent) {
	if r == nil || len(events) == 0 {
		return
	}
	r.mu.RLock()
	observers := append([]observerEntry(nil), r.observers...)
	records := make(map[string]Record, len(events))
	for _, event := range events {
		records[event.TaskID] = r.records[event.TaskID]
	}
	r.mu.RUnlock()
	lastByTask := make(map[string]int, len(events))
	for i, event := range events {
		lastByTask[event.TaskID] = i
	}
	for i, event := range events {
		observation := EventObservation{
			Event: event, Record: records[event.TaskID], FinalForTask: lastByTask[event.TaskID] == i,
		}
		for _, entry := range observers {
			notifyObserver(entry.observer, observation)
		}
	}
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
