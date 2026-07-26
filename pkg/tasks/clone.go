package tasks

func (r *Registry) eventsSinceLocked(start int) []TaskEvent {
	if start < 0 || start >= len(r.events) {
		return nil
	}
	return append([]TaskEvent(nil), r.events[start:]...)
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
