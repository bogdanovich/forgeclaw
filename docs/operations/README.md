# Operations

Operational docs for debugging, diagnosis, and production troubleshooting.

- [Troubleshooting](troubleshooting.md): common failures, symptoms, and recovery steps.
- [Debugging MintClaw](debug.md): live logs, passive diagnostic traces, and
  root-cause workflow.
- [Node Companion P0 deployment evidence](node-companion-p0-deployment.md):
  same-SHA rollout, bounded smoke verification, stale-revision drill, evidence,
  and rollback.
- [Node Companion P2 file-transfer deployment](node-companion-p2-deployment.md):
  deny-by-default rollout, reversible transfer fixtures, redaction checks, and
  rollback evidence.
- [Node Companion P2 deployment evidence](node-companion-p2-deployment-evidence.md):
  merged revisions, focused validation, live canaries, completion gates,
  enabled authority, backups, and the mandatory stop before P3.
- [Node Companion P3 service-administration deployment](node-companion-p3-deployment.md):
  deny-by-default Linux systemd profile setup, helper isolation, canary,
  redaction checks, and rollback.
- [Node Companion P3 deployment evidence](node-companion-p3-deployment-evidence.md):
  merged revisions, requirement matrix, live approved restart, no replay,
  checksums, rollback rehearsal, enabled authority, and mandatory stop.
- [Node terminal client and lifecycle smoke test](node-terminal-smoke.md):
  interactive use and automated verification of authenticated PTY open,
  attach, resize, input/output, and confirmed close.
- [Live gateway agent smoke test](live-agent-smoke.md): authenticated,
  bounded testing of the running gateway agent and its live node sessions
  without Telegram or a second agent runtime.
