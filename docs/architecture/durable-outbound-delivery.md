# Durable Outbound Delivery

## Status

Implemented on MintClaw `main` through PRs #505, #506, #512, and #513.
This document defines the runtime ownership, retry, and restart contract for
final text and media delivery.

## Guarantee

MintClaw provides at-least-once delivery attempts while it can prove that a
logical outbound payload was not accepted remotely. It does not promise
exactly-once delivery across arbitrary channel APIs.

Once a transport attempt may have reached a remote platform, uncertainty is
persisted as `ambiguous`. Ambiguous work is not automatically replayed. This
avoids turning a timeout, dropped response, or process crash into a duplicate
user-visible message.

The practical contract is:

- `pending` and `definitely_failed` are safe to dispatch again;
- `delivered` is terminal and retains confirmed platform message IDs;
- `attempting` becomes `ambiguous` after a process restart;
- `ambiguous` is terminal until an explicit future operator or platform
  reconciliation policy resolves it.

## Durable Identity And Payload

Each final response participating in inbound acknowledgement receives a stable
delivery ID derived from its source identity, ordinal, and payload kind. Route
fields are deliberately excluded from the hash so a replay cannot create a new
logical delivery by changing channels or chats.

The first persisted record remains canonical. It owns:

- delivery ID and source identity;
- owner workspace;
- text or media payload;
- channel, chat, topic/session routing context, and trace metadata;
- status, attempt count, platform message IDs, absolute retry deadline, and
  last error.

Duplicate admission returns this canonical record instead of regenerating its
payload or route. Records live under `state/outbox` in the configured instance
workspace and are written atomically with durable directory handling.

## Admission And Acknowledgement

Final response publication uses one transaction:

1. Persist the canonical outbox intent.
2. Acquire a process-local dispatch lease.
3. Prepare the lease before making the payload visible on the message bus.
4. Publish the canonical payload.
5. Commit the lease after successful bus publication.

The prepared state lets a fast channel consumer begin and even finish delivery
before the publisher returns, without losing ownership. If bus publication
fails, the exact lease is released and the durable record remains replayable.

Inbound acknowledgement succeeds only after every final text or media output
has transferred to this durable owner. Tool-loop, streaming finalization,
steering, system, async, and delegated-turn paths use the same transaction.
Child transaction failure propagates to the root turn and blocks its inbound
acknowledgement.

## Channel Outcome Boundary

Immediately before an adapter call, the channel manager persists `attempting`.
The adapter then returns a typed result containing:

- confirmed platform message IDs;
- complete, partial, or failed delivery status;
- rejected, accepted, or unknown remote acceptance;
- a known unsent remainder;
- relative and anchored absolute retry timing;
- the transport error.

The channel manager persists the terminal result before publishing runtime
completion events:

- confirmed acceptance becomes `delivered`;
- failure known to precede remote acceptance becomes `definitely_failed`;
- partial delivery or unknown acceptance becomes `ambiguous`.

Only known remainders from definitely rejected operations are retried for a
durable delivery. Legacy messages without a delivery ID retain their existing
ambiguous-retry policy. Telegram carries typed text and media outcomes,
including partial groups, long-caption tails, rate-limit deadlines, and
definitive API 4xx rejection. Other adapters continue through the conservative
legacy projection until they implement the typed interfaces.

Media-only final responses participate in durable ownership and outcome
persistence but do not receive a text footer.

## Restart Reconciliation

The gateway opens and scans the canonical outbox before channel ingress starts.
Recovery fails startup closed if a record cannot be read, validated, or durably
updated.

Recovery performs these transitions:

| Persisted status | Startup action |
| --- | --- |
| `pending` | Claim and publish after channel dispatch is live. |
| `definitely_failed` | Claim and publish at or after its absolute retry deadline. |
| `attempting` | Persist `ambiguous`; never replay automatically. |
| `delivered` | No action. |
| `ambiguous` | No action. |

Due work is published synchronously after channel workers and dispatchers have
started. Future rate-limit retries are sorted by deadline and held by one
gateway-lifetime reconciler. Shutdown cancels that reconciler and releases every
unpublished lease before channel draining, allowing the next process to recover
the same records.

Recovery always republishes the stored payload and route. A failure before bus
acceptance releases the lease. A crash after bus acceptance is classified by
the persisted attempt boundary on the next startup: still-pending work is safe
to replay, while an interrupted attempt is ambiguous.

## Failure Boundaries

The regression suite covers these boundaries:

- persistence failure before inbound acknowledgement;
- bus rejection and lease release before transport;
- consumer completion before publication commit;
- channel queue rejection and cancellation draining;
- crash with `pending`, `definitely_failed`, or `attempting` state;
- adapter success, definitive rejection, partial delivery, and uncertain
  acceptance;
- outcome persistence failure after a transport call;
- canonical text and media replay, including retry deadlines;
- duplicate and concurrent admissions;
- corrupt startup records and coordinator ownership conflicts;
- gateway shutdown with delayed recovery work;
- race checks and Linux/Windows compile checks for the affected packages.

## Non-Goals

- Inferring remote acceptance without a platform receipt.
- Blindly retrying ambiguous work to maximize apparent delivery rate.
- Universal exactly-once delivery.
- Adding text footers to media-only responses.
- Automatically resolving ambiguous records without an explicit policy and a
  platform capability that can support it.
