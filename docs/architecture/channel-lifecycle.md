# Channel Lifecycle Architecture

This document defines the target architecture for channel lifecycle management.
It exists because startup retry, reload, worker ownership, shared HTTP
registration, and gateway readiness are currently too tightly coupled inside
`pkg/channels/manager.go`.

## Problem

Today `channels.Manager` owns too many responsibilities at once:

- channel instance creation and replacement
- worker creation and teardown
- startup sequencing
- retry behavior
- shared HTTP/webhook registration
- reload coordination
- readiness implications

That coupling makes small operational fixes expensive. A bounded startup-retry
change can easily expand into reload races, stale worker reinstallation,
incorrect readiness, or inbound acceptance before delivery is actually live.

## Goals

- Make channel runtime state explicit and inspectable.
- Ensure only active channel runtimes can accept inbound traffic.
- Keep retry policy outside ordinary reload logic.
- Prevent stale channel generations from restarting after reload.
- Make readiness reflect real delivery capability.
- Reduce the amount of special-case logic inside `channels.Manager`.

## Non-Goals

- This is not a channel feature rewrite.
- This does not change channel-specific protocol logic unless required by the
  new lifecycle boundary.
- This does not attempt to solve durable outbound persistence in the same track.

## Required Invariants

1. A configured channel is not automatically a live runtime.
2. Only an `active` runtime may:
   - receive inbound webhook/socket traffic
   - own outbound/media workers
   - contribute to gateway readiness
3. Retry logic must be generation-aware. A runtime created for an old config
   generation must not reappear after reload.
4. Reload must reconcile desired channel state atomically at the runtime layer.
5. Shared HTTP handler registration must follow runtime activation, not config
   presence.
6. Shutdown and reload must be able to cancel any in-flight startup attempt.

## Target Model

### 1. `ChannelSupervisor`

Owns the lifecycle state machine for each configured channel name.

Responsibilities:

- create a runtime for a specific config generation
- start/stop runtime instances
- schedule bounded retry/backoff for startup failures
- reject stale retry completions after generation changes
- expose runtime state for readiness and diagnostics

It should be the only component allowed to transition a channel between:

- `inactive`
- `starting`
- `active`
- `stopping`
- `failed`

### 2. `ChannelRuntime`

Represents one live generation of one channel.

Responsibilities:

- hold the channel instance
- own worker goroutines and cancel context
- expose activation/deactivation hooks
- stop exactly once

It should be impossible to install workers or inbound handlers without a
runtime object.

### 3. `InboundRegistry`

Owns shared HTTP registration for channel runtimes.

Responsibilities:

- mount webhook handlers for active runtimes only
- unmount handlers when a runtime leaves `active`
- avoid unregistering a replacement runtime's handler during same-name reload

This removes HTTP exposure concerns from general manager cleanup paths.

### 4. `channels.Manager`

Becomes an orchestrator rather than a lifecycle kitchen sink.

Responsibilities:

- diff desired config against current supervised set
- ask the supervisor to reconcile state
- provide lookup surfaces used by the bus and gateway

It should stop directly mixing:

- raw channel map mutation
- retry goroutine ownership
- HTTP registration details
- readiness semantics

### 5. Gateway Readiness

Readiness should be derived from runtime capability, not startup progress.

Minimum rule:

- gateway is ready only when at least one channel runtime is `active`

Possible future extension:

- configurable readiness policy for deployments that need stricter guarantees

## Migration Plan

### Phase 1: Runtime State Boundary

- introduce a runtime record per channel name
- add generation IDs
- move worker ownership under runtime objects
- keep existing startup behavior otherwise unchanged

### Phase 2: Inbound Activation Boundary

- extract webhook/health registration from manager cleanup paths
- register handlers only for active runtimes
- make same-name replacement safe by construction

### Phase 3: Startup Supervision

- move startup retry into supervisor-owned state
- ensure retry cancellation on reload/shutdown
- ensure stale generations cannot reinstall workers

### Phase 4: Readiness Simplification

- derive readiness from active runtime count
- remove ad hoc startup-pending readiness behavior

## Acceptance Criteria

- a failed startup retry cannot restart an old runtime after reload
- a same-name reload cannot unregister the replacement webhook handler
- gateway cannot become ready while no active runtime exists
- shutdown/reload cannot race a startup retry into reinstalling a worker
- lifecycle tests become narrower and easier to reason about than the current
  manager-wide integration-style tests

## Implementation Notes

- Prefer additive refactors with compatibility shims over a one-shot rewrite.
- Land this in small PRs with explicit invariants per step.
- Keep operational behavior conservative while the refactor is in progress:
  avoid accepting inbound traffic before delivery capability exists.
