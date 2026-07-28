# Node Companion P1 disabled deployment evidence

Date: 2026-07-28

This record closes the operational portion of Node Companion P1 Owner-Control.
It is evidence for the production-code rollout, not a substitute for the code,
tests, or merged pull requests.

## Merged revision

- Repository: `bogdanovich/mintclaw`
- Deployed `main`: `d21a2830b68d0f819002d6a1af3030f5eac70fc1`
- Terminal control PR head: `a6b8001d7bc60da56615e072132fe2ab168282b4`
- Terminal control PR: #427
- Exact-head GitHub checks: Linter, Security Check, Tests, and Integration Tests
  passed.
- Earlier dependency PRs in the same P1 implementation sequence: #423, #424,
  #425, and #426.

The server repository was clean before deployment, its `origin` was
`git@github.com:bogdanovich/mintclaw.git`, and it was updated from
`8e03e40ffad632be162ba54006a9405fec4efd3f` using a fast-forward-only merge.

## Built and deployed artifacts

All required P1 binaries were built on the target Linux amd64 host from the
recorded merged revision.

| Artifact | SHA-256 |
|---|---|
| `mintclaw` | `8d9c67696d39865c0fa9cbcb02e55d0343b01f4c0f51cd0a58456d4adc948074` |
| `mintclaw-node` | `6fe5a364e8b57d1c7d9b0c4f4ab550a46e253016cddc74146490973983a8e74e` |
| `mintclaw-node-broker` | `8cc10c7360006666737658d7cfa62dd4fa452b1a5433f72be10872cfa0bf6005` |

The running gateway executable and the companion executable matched these
checksums after restart. The companion reported version
`nightly-57-gd21a2830`.

The unchanged launcher was preserved and not restarted. Its optional rebuild
was attempted but the host did not have `pnpm`; launcher code was outside this
P1 change and its existing HTTP endpoint remained healthy.

## Deny-by-default proof

The deployment intentionally installed capability but granted no new
production authority:

- gateway node support remained enabled for the existing P0 companion;
- `nodes.terminal_enabled` remained `false`;
- the gateway configuration contained no `owner_shell` entry;
- the companion configuration contained no `owner_shell` entry and its
  effective owner-shell state remained disabled;
- the companion continued to advertise only `system.exec.v1`;
- the broker binary was installed as mode `0755`, owned by the unprivileged
  deployment user, without setuid;
- no broker service, root policy, or broker socket was installed;
- `/run/mintclaw/node-authority-broker.sock` remained absent.

Production approval-hook configuration and enabling owner, terminal, broker,
or root authority were not part of this deployment.

## Runtime verification

- `mintclaw-main.service` and `mintclaw-node-vpn-smoke.service` restarted and
  remained active.
- The companion completed node admission and returned to `connected`.
- Gateway `/health` and `/ready` returned HTTP 200.
- The launcher returned HTTP 302, as expected.
- The reviewer webhook returned HTTP 404 for an unauthenticated GET, as
  expected.
- All expected MintClaw user services were active.
- Every active profile loaded with `doctor` exit code 2, representing existing
  policy findings rather than load or schema failures.
- Error-level journals for every expected MintClaw service and the companion
  were empty after the successful smoke-test observation point.
- Active processes, systemd units, configs, gateway scripts, companion config,
  and active skills had zero `forgeclaw` or `picoclaw` matches.

One initial WebSocket smoke used a payload session ID that did not match the
connection query session ID. The model turn completed, but delivery failed and
produced one transient, explained error. The corrected authenticated smoke
used the same session ID in both places and returned exactly
`MINTCLAW_P1_GATEWAY_SMOKE_OK`.

The corrected smoke produced:

- trace ID: `trace-turn-fcd6231862a128891ae10bdf`
- schema: `mintclaw.diagnostic_trace.v1`
- outcome: `completed`
- records: 8
- content mode: `redacted_content`
- redactor: `mintclaw.config_filter.v1`
- truncation: none

The host retained one unrelated pre-existing failed optional unit,
`browser-learned-patterns-compact.timer`, whose unit file was already absent.
No MintClaw service was failed.

## Backups

- Gateway host: `/home/server/mintclaw-p1-backup-20260728T231534Z`
- Companion host: `/home/deploy/mintclaw-p1-backup-20260728T231537Z`

The backups contain the previous binaries, affected user unit files and
drop-ins when present, pre-deployment service state, checksums, and the prior
core revision. They are outside active runtime paths and must not be deleted as
part of this rollout.

## Rollback

Rollback is binary-only because this disabled deployment did not migrate or
write mutable production data.

On `server@oc`:

```bash
systemctl --user stop mintclaw-main.service
install -m 0755 \
  /home/server/mintclaw-p1-backup-20260728T231534Z/binaries/mintclaw \
  /home/server/src/mintclaw/build/mintclaw-linux-amd64.new
mv -f \
  /home/server/src/mintclaw/build/mintclaw-linux-amd64.new \
  /home/server/src/mintclaw/build/mintclaw-linux-amd64
install -m 0755 \
  /home/server/mintclaw-p1-backup-20260728T231534Z/binaries/mintclaw \
  /home/server/.local/bin/mintclaw.new
mv -f /home/server/.local/bin/mintclaw.new /home/server/.local/bin/mintclaw
install -m 0755 \
  /home/server/mintclaw-p1-backup-20260728T231534Z/binaries/mintclaw-node \
  /home/server/.local/bin/mintclaw-node.new
mv -f \
  /home/server/.local/bin/mintclaw-node.new \
  /home/server/.local/bin/mintclaw-node
rm -f /home/server/.local/bin/mintclaw-node-broker
systemctl --user start mintclaw-main.service
```

On the companion through `server@oc`:

```bash
ssh deploy@vpn 'systemctl --user stop mintclaw-node-vpn-smoke.service'
ssh deploy@vpn 'install -m 0755 \
  /home/deploy/mintclaw-p1-backup-20260728T231537Z/binaries/mintclaw-node \
  /home/deploy/.local/bin/mintclaw-node.new'
ssh deploy@vpn 'mv -f \
  /home/deploy/.local/bin/mintclaw-node.new \
  /home/deploy/.local/bin/mintclaw-node'
ssh deploy@vpn 'rm -f /home/deploy/.local/bin/mintclaw-node-broker'
ssh deploy@vpn 'systemctl --user start mintclaw-node-vpn-smoke.service'
```

After rollback, rerun the deployed-ops status checks, verify the two restored
binary checksums from the backup manifests, inspect bounded error journals, and
confirm the companion reconnects with only `system.exec.v1`.
