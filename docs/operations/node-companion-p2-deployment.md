# Node Companion P2 File-Transfer Deployment

This runbook records the final deny-by-default rollout and production proof for
the bounded P2 regular-file capability. The admission contract in
[`node-companion-p2-admission.md`](../architecture/node-companion-p2-admission.md)
remains authoritative. Never use this runbook to broaden a profile or enable
automatic approval.

## Preconditions and evidence record

Deploy only after every P2 implementation PR is merged and merged `main` passes
formatting, lint, focused race tests, the full tagged Go suite, and the
real-process WSS vertical-slice test. Record:

- merged commit, builder host, UTC start/end time, and operator;
- SHA-256 and ownership/mode for `mintclaw`, `mintclaw-node`, and the helper;
- config and unit-file backup paths and their SHA-256 values;
- exact workspace, target alias, file-profile alias/revision, actor, routed
  session, channel/chat/topic, roots, limits, approval mode, and helper path;
- pre/post health, readiness, pairing, ledger/spool quota, cleanup, journal,
  and duplicate-effect results; and
- rollback result or a separately recorded rollback rehearsal.

Do not record file bytes, credentials, environment values, artifact references,
absolute transferred paths, approval answers, connection details, or
unrestricted tool output.

## Build and install

1. Fast-forward a clean deployment checkout to the recorded merged commit.
2. Re-run the merged-main validation above on that checkout.
3. Build all three artifacts from that one revision. Verify checksums before
   installation.
4. Back up binaries, workspace configuration, companion configuration,
   service units, and the one reversible administrator fixture.
5. Install each artifact atomically with root ownership and least-permissive
   executable modes. The complete companion continues to run unprivileged; only
   the local typed helper is privileged.
6. Keep file profiles absent or disabled everywhere. Enable one exact
   operator-owned personal-server profile only after its roots, limits,
   revision, actor/route bindings, approval requirement, and local helper
   identity have been reviewed out of band.
7. Restart the companion canary first, verify readiness and pairing, then
   restart its gateway workspace. Do not roll out another profile until the
   proof below is complete.

## Canary proof

Use unique, non-sensitive, bounded fixtures and record hashes and sizes rather
than content.

1. Confirm a fresh or delegated profile advertises none of
   `nodes_file_info`, `nodes_upload`, or `nodes_download` and has no spool or
   helper authority.
2. On the admitted unprivileged profile, upload a binary fixture, query its
   metadata, download it, and compare size and SHA-256 exactly.
3. Require durable human approval for both mutations. Confirm the displayed
   action names the retained target, profile, path, size, digest, publication
   mode, and delivery consequence. Changed or expired continuation must fail.
4. Download one small image and confirm it is delivered once to the original
   authorized channel, chat, and topic, with no duplicate completion reply.
5. Attempt the same artifact or status lookup as a different actor and routed
   session; it must fail without revealing existence or policy.
6. Change the configured profile revision and prove stale discovery and a
   prepared transfer fail before acceptance. Restore the reviewed revision.
7. Disconnect after dispatch at the deterministic test boundary. Status must
   report a recovered terminal result or explicit unknown and must not replay.
8. Through the explicit administrator profile, read and atomically replace one
   backed-up root-owned regular-file fixture. Verify checksum, mode, ownership,
   backup recovery, and that the companion process itself remains unprivileged.
9. Confirm missing/altered helper identity, wrong peer, wrong profile/revision,
   traversal, symlink, special file, oversize request, forbidden mount, and
   invalid publication mode fail closed.
10. Inspect runtime logs, passive events, diagnostic traces, approval records,
    ledger metadata, and the evidence record. They may contain lifecycle state,
    bounded codes, sizes, and digests only where admitted; they must not contain
    fixture bytes, secrets, artifact references, approval answers, or absolute
    transferred paths.
11. Restart gateway and companion. Verify pairing, durable terminal status,
    bounded spool cleanup, no duplicate publication/delivery, and no automatic
    replay of completed or uncertain work.

## Rollback

Disable the exact file profile first and restart the affected gateway and
companion. Restore the backed-up binaries, configs, units, and administrator
fixture atomically; restart companion before gateway. Verify health, readiness,
pairing for unaffected node commands, absence of all three file tools, clean
journals, and no replay or duplicate delivery. Retain only redacted evidence and
allow the configured spool/metadata retention policy to clean bounded state.

Stop the rollout immediately on any authority mismatch, content/path leak,
partial publication, helper-boundary failure, unexplained unknown outcome,
automatic replay, duplicate effect, or rollback failure.

## Intentional limits

P2 transfers bounded regular files only. It does not provide directory or
archive transfer, synchronization, cross-restart resume, symlink following,
special files, arbitrary file descriptors, shell-based transfer, a root-run
companion, general artifact storage, or privileged macOS/Windows access.
