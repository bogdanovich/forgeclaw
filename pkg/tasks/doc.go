// Package tasks provides durable task/run bookkeeping for background work.
//
// The registry is intentionally small: it records task lifecycle, completion
// summaries, and delivery state with bounded retention. It does not deliver
// messages itself. The intended architecture is:
//
//	TaskRegistry -> typed async completion event -> delivery coordinator
//
// Today the registry is used by spawn, delegate, cron execution, and status
// tools so completed background work survives manager recreation and service
// restarts. Additional long-running runtimes should prefer this registry
// instead of inventing parallel status stores.
package tasks
