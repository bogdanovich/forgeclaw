// Package tasks provides durable task/run bookkeeping for background work.
//
// The registry is intentionally small: it records task lifecycle, completion
// summaries, and delivery state with bounded retention. It does not deliver
// messages itself. The intended architecture is:
//
//	TaskRegistry -> typed async completion event -> delivery coordinator
//
// Today the registry is used by spawn/subagent status so completed background
// work survives manager recreation and service restarts. A future delivery
// coordinator can use the same records to route user_only, parent_only, and
// user_and_parent completions, then update DeliveryStatus after delivery.
package tasks
