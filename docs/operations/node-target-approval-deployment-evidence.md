# Node target-scoped approval deployment evidence

Date: 2026-08-02

Status: complete

This record closes the native exact-target approval policy rollout. The policy
lets an operator keep the global approval mode fail-closed while explicitly
bypassing prompts for trusted first-party node tools on selected target
aliases. It is shared node infrastructure and does not complete P3 typed
service administration.

## Merged revision and security boundary

- Target-scoped approval PR: #467.
- Merge commit: `7b0804698574c835a3a5832eeb7adacf4fc89270`.
- Deployed runtime revision: `91b786611948e1853adbb4d57fab6af4245f67c7`.
- Active runtime version: `nightly-265-g91b78661`.

The merged policy validates exact configured target aliases. Eligibility also
requires a first-party package-private capability whose declared owner is the
exact registered tool instance. Approval validation returns an opaque bound
execution token, so an injected tool, embedded wrapper, or concurrent registry
replacement cannot inherit or redirect the bypass. Future first-party node
tools can opt in through the same internal owner capability without adding a
new user-configured tool-name list.

## Final production policy

The active main profile uses:

```json
{
  "tools": {
    "approval": {
      "mode": "required",
      "bypass_node_targets": ["vpn"]
    }
  }
}
```

The previous hook-specific `MINTCLAW_NODE_APPROVAL_FREE_TARGETS` exception was
removed. The native configuration is therefore the only approval-bypass source
for `vpn`. No temporary test alias remains. `mintclaw doctor` loads the config
successfully and returns policy exit code 2 because an approval bypass is
intentionally reported as a high-risk finding.

## Positive native-bypass proof

An authenticated Telegram request asked the agent to execute `id && hostname`
on `vpn` through `shell.exec.v1` with the configured root profile and root
working scope. The invocation completed immediately as UID 0 without an
approval interaction.

Final trace evidence:

- trace ID: `trace-turn-ae7a6a5fb9f8cc9b50d48e3a`;
- schema: `mintclaw.diagnostic_trace.v1`;
- outcome: `completed`;
- records: 22;
- content mode: `redacted_content`;
- truncation: none;
- `nodes_invoke`: started, executed, and completed.

The earlier native-bypass trace
`trace-turn-ea7c47dbaf214ed990fcabec` also contained no
`APPROVAL_REQUIRED` or approval-hook journal event, proving the successful
invocation did not rely on the legacy process hook.

## Negative-control proof

The operator temporarily removed `bypass_node_targets` while preserving
`mode: required` and restarted the main gateway. The identical `vpn`
`shell.exec.v1` request then produced a durable human approval request instead
of executing immediately. An explicit `allow_once` answer resumed the retained
invocation and returned UID 0 and the companion hostname.

The continuation trace `trace-turn-ad0331286ea610236904406c` completed without
truncation. After the control, the exact final policy above was restored and a
second prompt-free invocation produced the final positive trace. This proves
both sides of the boundary using the same physical node, command, profile, and
working scope.

## Deployment health and rollback

All expected MintClaw services were active after the final restart. The main
gateway and web service were active, their five-minute error journals were
empty, and the active config assertions confirmed:

- global approval remains `required`;
- only exact target alias `vpn` is listed for bypass;
- the legacy hook exception is absent; and
- the temporary control alias is absent.

The retained rollback snapshot is:

```text
/home/server/mintclaw-target-approval-backup-20260802T191801Z
```

It contains the prior binaries, systemd units, service state, checksums, and
multiple config checkpoints from the positive and negative controls. A rollback
restores the recorded binaries and pre-change main config, restarts the affected
MintClaw user units, and reruns deployed status, doctor, and a bounded agent
smoke.
