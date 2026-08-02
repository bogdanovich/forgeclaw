# Node terminal client deployment evidence

Date: 2026-08-02

Status: local CLI slice complete

This record closes the local interactive CLI portion of the Future P1 terminal
follow-up. It does not claim completion of browser terminal UI or agent-driven
PTY control.

## Merged revision

- Interactive terminal client PR: #442.
- Merge commit: `f59640f59179827f78d2f8c38ae975af750756fb`.
- The client uses the existing authenticated terminal transport and opens the
  PTY deterministically without an LLM in the control plane.
- Raw-mode restoration, byte ordering, resize, local escape, close,
  authentication, and failure behavior are covered by the merged focused
  tests documented in the PR.

The deployed Linux gateway and CLI were rebuilt at `91b786611948e1853adbb4d57fab6af4245f67c7`,
which contains the terminal client merge. The active CLI reported
`nightly-265-g91b78661`.

## Live interactive proof

An operator opened a real attached PTY through:

```text
mintclaw nodes terminal open \
  --config /home/server/.mintclaw/main/config.json \
  --target vpn \
  --profile root \
  --working-scope root
```

The client reported the opening, attachment, and local escape instructions
before entering raw mode. The operator then ran ordinary interactive commands,
changed directories, inspected filesystem state, and returned to the local
prompt. Input was not line-buffered by the client, remote output remained in
terminal order, and the remote prompt identified the configured companion
host. No node port or inbound SSH path was exposed through NAT.

## Automated lifecycle proof

The deployed operations smoke used the same gateway profile and target with
the root shell profile. It returned:

| Field | Result |
| --- | --- |
| Target | `vpn` |
| Profile | `root` |
| Process UID | `0` |
| PTY dimensions | 31 rows by 100 columns |
| Marker | `MINTCLAW_PTY_OK` |
| Final state | `closed` |
| Close reason | `close` |

The smoke modified no files. It proved authenticated open, attach, resize,
ordered input/output, marker validation, requested close, and confirmed remote
process-tree termination.

## Runtime health and rollback

All expected MintClaw services were active after deployment. Error-level
journals for the observation window were empty, the launcher returned its
expected redirect, and no legacy product process was running.

The retained rollback snapshot is:

```text
/home/server/mintclaw-target-approval-backup-20260802T191801Z
```

It contains the pre-update core, node, and launcher binaries plus user systemd
units and service state. Restoring the prior binary set and restarting only the
affected MintClaw user units returns the gateway to the previous deployment.

## Remaining terminal work

The following roadmap items remain open:

- browser terminal UI;
- a bounded agent-operated PTY loop; and
- authenticated live-agent invocation smoke that does not depend on Telegram.

These are not required to call the local CLI slice complete.
