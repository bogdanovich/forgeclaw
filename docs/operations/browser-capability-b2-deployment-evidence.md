# Browser Capability B2 Deployment Evidence

## Status

Browser milestone B2, **Artifacts, Diagnostics, and Human Handoff**, is merged,
deployed, and complete. The production gateway runs merged `main` commit
`f0e27be1452b355eb55db379837edfb5d5e8b132` as
`nightly-451-gf0e27be1`.

This record closes the completion gates in
[Browser Capability B2 Admission](../architecture/browser-capability-b2-admission.md).
It does not admit companion-hosted browsers, attached-user identity, stored
credentials, generic filesystem access, raw Playwright or CDP, or computer
control.

## Merged implementation

| Slice | Pull request | Merge commit |
| --- | --- | --- |
| Consecutive managed-session prerequisite | [#508](https://github.com/bogdanovich/mintclaw/pull/508) | `54600897f8e5bf062db79652a7348804a18566f6` |
| Screenshot retention and delivery | [#510](https://github.com/bogdanovich/mintclaw/pull/510), [#517](https://github.com/bogdanovich/mintclaw/pull/517) | `1022946aaeda6804d2074b955ec1d9d4dbdd2ff5`, `72c50cd23235220d4b5a94f2416d726ea1bf70a8` |
| Upload and download artifacts | [#518](https://github.com/bogdanovich/mintclaw/pull/518), [#519](https://github.com/bogdanovich/mintclaw/pull/519), [#525](https://github.com/bogdanovich/mintclaw/pull/525), [#526](https://github.com/bogdanovich/mintclaw/pull/526) | `258306fc2585b9f79b5e3003e65c3a1ceda50f42`, `0a9fa2496460b76e0c25a7e3836292fc1ffeef2d`, `ad464b3a939c2713de3602a4e4f004ef7d173419`, `f0e27be1452b355eb55db379837edfb5d5e8b132` |
| Passive diagnostics | [#520](https://github.com/bogdanovich/mintclaw/pull/520) | `98fb66830a134d740388c1d4b169ccb83d9708b9` |
| Human handoff, resume, and interaction recovery | [#521](https://github.com/bogdanovich/mintclaw/pull/521), [#522](https://github.com/bogdanovich/mintclaw/pull/522), [#523](https://github.com/bogdanovich/mintclaw/pull/523), [#524](https://github.com/bogdanovich/mintclaw/pull/524) | `39780352196da7b6014c85b080e640dca9f095f7`, `48108ebaf5213d94b057a112ced641dcf003fd46`, `6c745ff47f8022d62f844ad44f54d09899cc41e3`, `86ea09073c6d2cbd9c2a8d3d89878d9aaea5589d` |

Every code pull request passed its required CI and reviewer pipeline before
merge. The two final integration repairs also passed the focused Linux gateway
test and the complete GitHub test, integration, lint, security, and Windows
browser jobs.

## Live completion evidence

### Consecutive sessions

The deployed gateway completed two full managed session lifecycles in one
process without a gateway restart:

```text
open -> observe -> close -> open -> observe -> close
```

The second session received a fresh tab and controller state. No driver,
profile lease, proxy, or policy state leaked across the close boundary.

### Screenshot retention and delivery

A browser observation retained a `197151`-byte PNG in the P2 spool with SHA-256
`03f7a2383804aa774b6c252380cfc3eb70d9407b9f83a9f9a96c4e3db27925fb`.
The routed media delivery used the existing owner-bound, idempotent media
handoff; declared and observed sizes matched.

### Upload and download round-trip

The final canary ran through the delegated browser specialist on the normal
production `managed` profile with `dry_run=true`:

1. It navigated to an operator-controlled loopback fixture.
2. `browser_observe(screenshot=true)` committed an `11015`-byte PNG with
   SHA-256
   `893f1c467e050dea3f4b1da341a2f2d69ac92b38483b429030d3c58766afbc48`.
3. The exact same route, browser session, tab, snapshot, and generation selected
   that screenshot through the typed upload action. The selected filename was
   `browser-screenshot.png`; no form submission occurred.
4. The typed download suspended for an exact one-time approval. After approval,
   it committed a `32`-byte binary artifact with SHA-256
   `89b30aa0908edc4df76f451fc28e3817c641f98adcfb636b464a935c5549791e`.
5. The browser session closed explicitly.

The P2 spool independently reported both records as committed with matching
declared and observed sizes, content types, source kinds, and digests. Neither
artifact requested channel delivery in this canary.

### Passive diagnostics

Trace `trace-turn-c7c4c5908a43dbd336b41504` reported the managed gateway
target ready with screenshot, upload, and download capabilities. It used
schema `mintclaw.diagnostic_trace.v1`, completed without truncation, and kept
the configured redacted-content policy. Reading diagnostics did not open a
session or mutate browser state.

### Human handoff and resume

The deployed local headed browser completed:

```text
open -> observe -> handoff -> operator release -> resume -> observe -> close
```

The handoff trace `trace-turn-d9a4ecd7fea9282857b0e2cb` suspended after the
controller transition. The continuation trace
`trace-turn-edaa3047558533980d8f309c` completed release, resume, fresh
observation, and explicit close. Agent mutation remained denied during human
control, resume rotated snapshot authority, and no implicit close occurred at
handoff.

## Fail-closed and privacy evidence

Automated coverage rejects wrong workspace, agent, actor, route, browser
session, tab, snapshot, generation, digest, expiry, media type, and size.
Cross-session uploads, arbitrary host paths, unsupported inputs, multiple or
oversized downloads, stale approvals, and blind replay also fail closed.

The final canary produced one suspended and one completed bounded trace. Both
used schema `mintclaw.diagnostic_trace.v1`, redacted content, and no truncation.
Searches across the traces and model session JSON found zero PNG base64 markers
and zero fixture-byte markers. Browser, event, node-command, trace, and model
JSON contained metadata and hashes only.

After the canary:

- all `28` retained browser sessions were terminal `closed` records;
- active browser sessions were `0`;
- nonterminal browser invocations were `0`;
- waiting interactions were `0`;
- running or waiting tasks were `0`;
- the temporary fixture service was stopped;
- the original production configuration checksum was restored; and
- the main gateway, web launcher, reviewer gateway, webhook, and review queue
  were active with no recent error-level main-gateway journal entries.

Rollback backups are retained at
`/home/server/mintclaw-deploy-backup-20260805T0454Z` and
`/home/server/mintclaw-deploy-backup-20260805T0520Z`.

## Residual limits and next milestone

B2 remains gateway-local, uses one managed dry-run profile, and provides only
the admitted local headed handoff. It does not place a browser on a companion
or grant browser subturns inherited node file authority. An approved typed
download is the sole unknown-effect dry-run exception; external commits and
all other unknown actions remain denied.

The next dependency-ordered browser milestone is B3, **Companion-Hosted
Browser**. Work should begin with a joint B3 and node P7 admission that selects
one concrete companion, one managed profile, typed commands, disconnect
semantics, and exact deployment evidence before implementation.
