# Seahorse reconciliation

JSONL session history is canonical. Seahorse SQLite is a derived context and
search index that may be deleted and rebuilt without losing conversation data.

## Evaluated designs

- Full startup reconciliation is simple, but its cost grows with every stored
  message and blocks channel readiness. It also amplified `message_parts` reads
  into one SQL query per message.
- File timestamps alone are cheap but are not a sufficient logical version:
  timestamp precision, atomic replacement, and multi-file metadata updates can
  make equality ambiguous.
- A durable logical revision plus a Seahorse watermark makes clean checks
  constant-size. File size and modification time remain supplementary evidence
  for edits made outside the application.

The revision/watermark design is used. Full reconciliation remains the recovery
path, but it is lazy for active sessions and background work for inactive ones.

## Invariants

1. Every logical JSONL history mutation advances `history_revision` in the
   session metadata. Physical compaction does not.
2. Multi-file mutations write `history_dirty=true` before replacing/appending
   JSONL and clear it only after the durable JSONL operation. Dirty metadata is
   repaired by scanning canonical JSONL while holding the session writer lock.
3. A Seahorse watermark is written only after successful reconciliation or a
   proven one-message incremental append. Failure leaves the prior watermark.
4. Revision, count, skip, file identity, and reconciliation schema generation
   must all match to skip reconciliation. A generation change invalidates every
   old watermark.
5. Assemble, ingest, compact, clear, and background reconciliation share a
   sharded per-session barrier. A live turn cannot observe a partial rebuild or
   append the same canonical message twice.
6. Routed sessions are read from the owning agent's SessionStore. Background
   reconciliation enumerates every registered agent after `AgentLoop.Run`
   begins; context-manager construction never scans historical sessions.

Full reconciliation compares canonical and derived messages and repairs or
rebuilds SQLite as before. Message parts are loaded in bounded SQL batches, so
that recovery path does not issue a query per message.

Operational logs emit one `reconciled canonical history` event with session,
message count, and duration for each full reconciliation. Clean revision checks
emit no event.
