# Node Companion P2 deployment evidence

Date: 2026-08-02

Status: complete

This record closes the Node Companion P2 file-transfer milestone. It combines
merged-code evidence, deterministic failure-injection tests, and bounded live
canaries. It does not authorize P3 or broaden any file profile.

## Merged revisions

The P2 implementation and the approval UX corrections are merged into
`bogdanovich/mintclaw:main`:

| PR | Merge commit | Scope |
| --- | --- | --- |
| #433 | `f215ef7d` | Admission contract and mandatory stop condition |
| #434 | `8074db8a` | Bounded durable gateway transfer spool |
| #436 | `e549c44f` | Authenticated streaming transfer framing |
| #438 | `262662ef` | Unprivileged companion file runtime |
| #439 | `720fbb45` | Model-facing file tools and routed delivery |
| #440 | `49f40913` | Linux privileged file helper |
| #441 | `c1ceb3fd` | Real-process vertical slice and operations runbook |
| #450 | `42fd3180` | Explicit file-transfer approval bypass |
| #465 | `833ae2dc` | Correlated Telegram approval buttons |
| #467 | `7b080469` | Target-scoped operator approval policy |

The deployed gateway was fast-forwarded to
`7b0804698574c835a3a5832eeb7adacf4fc89270`. Its exact-head GitHub checks
passed Tests, Integration Tests, Linter, Security Check, and Browser Windows
before merge.

## Built and deployed artifacts

The Linux gateway was rebuilt from the recorded merged revision and restarted
successfully:

| Artifact | Revision | SHA-256 |
| --- | --- | --- |
| Linux amd64 `mintclaw` gateway | `7b080469` | `063a1687c35c4ea2d90a9a305e9a459dd2c07a6eb5ed1122f2cff6016bf9de79` |
| macOS amd64 `mintclaw-node` canary | `0487a892` | `ad105773ea1a26fa24f4ce5833f14c925c0b25781a6ffedf472095433db23688` |
| Linux amd64 `mintclaw-node` canary | `e085bf62` | `34e5f5c55ad0abd28e080b37ae227b08dcdaa9c53e5bb5959814e445595efb0c` |
| Linux amd64 authority broker | `e085bf62` | `3139aeb0de102463c0b67a0d2196b8f7e894994a121ebf233ba5d9b6c1067c5d` |
| Linux amd64 file helper | `e085bf62` | `8eb0f68bdcc451fcddf8871019a8850eae3c68f8948514fc0a41bd34aa5ada99` |

The macOS canary revision is an ancestor of the deployed gateway revision and
contains the merged P2 vertical slice. The currently connected macOS
companion runs as the unprivileged operator account. The Linux administrator
canary ran the companion unprivileged and the typed helper as root; it was
subsequently removed from the active canary topology without changing the
recorded proof.

The gateway service is active. Telegram reconnected in polling mode, a fresh
model turn completed with a delivered response, and the existing reviewer
service kept the same process ID throughout deployment.

## Validation

On macOS Tahoe 26.5.2, the following exact-main focused race suite passed:

```text
go test -race -tags='goolm stdjson' \
  ./pkg/nodes ./pkg/nodes/companion ./pkg/nodes/protocol \
  ./pkg/nodes/ws ./pkg/tools ./pkg/gateway
```

This includes the real-process WSS approval-and-delivery vertical slice and
the actor, route, stale-revision, path, helper, disconnect, restart, retention,
delivery, and no-replay cases used below. The full tagged suite is not repeated
as a local deployment gate: the exact merged head's GitHub Tests and
Integration Tests are the authoritative broad-suite result.

An attempted local broad run exposed a Tahoe process-launch issue in which
new test executables remained blocked in `dyld` before the Go runtime started.
The same previously blocked package completed normally on Linux. This was not
a P2 test failure and no product test was weakened or changed.

## Live canary evidence

Only aliases, sizes, digests, and outcomes are retained here. Transferred
absolute paths, artifact references, approval answers, credentials, file
bytes, actor identifiers, and conversation identifiers are intentionally
omitted.

- The `ab-local-test` profile inspected, downloaded, and atomically replaced
  a 45-byte text fixture. Size and SHA-256 matched before and after. The
  download was retained without chat delivery and reused by opaque artifact
  reference for upload.
- A binary image canary was inspected and downloaded with matching size and
  SHA-256, then delivered once through the originating Telegram route. No
  duplicate completion reply was emitted.
- A full retained-artifact round trip downloaded and atomically re-uploaded a
  binary fixture without using shell, system execution, terminal, or inline
  file bytes.
- The Linux administrator profile inspected and downloaded a 65-byte
  root-owned regular-file fixture. After two distinct durable human
  approvals, the helper atomically replaced it with the retained identical
  artifact. Final size and digest matched and the companion remained
  unprivileged.
- Approval buttons carried the correlated request identity in callback data.
  One button press produced one authorized answer, one dispatch, one
  completion, and removal of the temporary tool-feedback message. Plain text
  that was not an admitted answer was rejected rather than interpreted as
  approval.
- A stale discovery revision was rejected before transfer. Repeating the
  required discovery step admitted the exact current revision and the
  round-trip then completed.
- After a real gateway restart the macOS companion reconnected through its
  existing outbound tunnel. The post-restart journal contained no node file
  dispatch, artifact reference, approval answer, or canary path, proving the
  restart did not manufacture or replay transfer work.

## Completion gates

| Gate | Authoritative evidence |
| --- | --- |
| 1. Disabled and bounded authority | Config validation and companion tests prove file authority is absent by default; live authority came only from named operator profiles and stale revisions failed closed. |
| 2. Ownership intersection | Tool and spool race tests cover workspace, agent, actor, route, session, tool call, target, profile, artifact, and transfer identity changes independently. |
| 3. Regular-file and path safety | Unix/Linux resolver and helper suites reject traversal, symlink, special file, pseudo-filesystem, mount, size, peer, revision, and rename-race violations. |
| 4. Integrity and publication | Protocol, spool, runtime, and helper tests cover ordered bounded chunks, exact size/digest, atomic create/replace, partial failure, duplicate identity, and publication recovery; live text and binary digests matched. |
| 5. Approval and freshness | Tool tests bind continuation to the exact retained plan and reject changed actor/input/revision/expiry; the live Telegram flow required correlated human input and could not self-approve. |
| 6. Cancellation, restart, and no replay | Runtime, ledger, spool, WebSocket, tool, and vertical-slice tests cover cancellation/commit/disconnect/restart races and explicit unknown outcomes; the live restart produced no dispatch. |
| 7. Model tools and routed delivery | Discovery plus all three model-facing tools completed live round trips; the image and completion were delivered exactly once to the originating route. |
| 8. Linux administrator boundary | The root-owned fixture canary passed through the local typed helper while the companion stayed unprivileged; helper denial tests cover missing or changed authority. |
| 9. Redaction and retention | File-tool log arguments and passive events are structurally redacted. Production gateway logging is `info`; a post-restart scan found no debug entries, artifact references, approval answers, or canary paths. The spool held seven committed records totaling 1,065 bytes under the configured 168-hour bounded retention. |
| 10. Merge and deployment | Every listed PR is merged, exact-head CI is green, focused race/E2E passed, artifacts and backups are recorded, gateway and companion are healthy, and only explicit operator profiles expose file authority. |

## Backups and rollback

The retained deployment backups are:

- gateway binary/config/unit before the final merged-main rollout:
  `/home/server/mintclaw-deploy-backup-20260802T190640Z`;
- gateway config before production log reduction:
  `/home/server/mintclaw-p2-redaction-backup-20260802T184436Z`;
- Linux companion, helper, broker, policy, ledgers, units, and administrator
  fixture before the same-revision canary rollout:
  `/home/deploy/mintclaw-p2-closeout-backup-20260802T183807Z` on that canary.

The disabled-profile restoration rehearsal completed before administrator
enablement. A rollback disables the exact profile first, restores the recorded
binary/config/unit set atomically, restarts companion before gateway, and then
checks pairing, absence of file tools, bounded journals, and no replay as
specified in the P2 deployment runbook.

## Enabled authority and residual limits

Fresh and delegated configurations remain deny-by-default. The active macOS
canary has one explicit regular-file profile rooted at its bounded canary
directory, with human approval required for reads and writes. The disconnected
Linux administrator profile granted root filesystem access only through the
typed helper and no longer contributes live authority.

P2 supports bounded regular files only. It does not provide directory or
archive transfer, synchronization, resumable cross-restart transfer, symlink
following, special files, arbitrary file descriptors, shell-based transfer, a
root-run companion, or privileged macOS/Windows file access.

All ten P2 gates now have authoritative evidence. Under the admission
contract's mandatory stop condition, P2 is complete and implementation stops
here. P3 typed service administration remains unadmitted and requires a new
operator scope decision.
