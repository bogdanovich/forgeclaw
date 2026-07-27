# Node Companion P0 Deployment Evidence

Use this runbook only after every Node Companion P0 dependency PR is merged.
It deploys the gateway and companion from one reviewed `main` commit, preserves
deny-by-default authority, and records enough evidence to decide whether P1 is
admissible.

## Completion boundary

The rollout is complete only when all of the following are recorded:

- one merged `main` commit used for both binaries;
- gateway and companion service health;
- the existing explicitly configured smoke profile, with no additional
  command authority;
- one gateway-owned outcome-only model flow through discovery, approval,
  invocation, and retained status;
- one passive `node.invocation.observed` trace with no command input, output,
  path, environment, node identity, or authority hash;
- one `DISCOVERY_STALE` denial after a reversible non-destructive constraint
  change, with no new prepared or dispatched observation;
- successful discovery after restoring the constraint;
- error-level journal comparison, rollback location, and residual
  limitations;
- an explicit P1 admit or defer decision.

Do not broaden the smoke allowlist to make the verification pass. A fresh
installation without an explicit `system_exec` policy must continue to expose
no executable authority.

## Preflight and rollback

1. Fetch `origin/main`, require a clean worktree, and record:

   ```bash
   git rev-parse origin/main
   git status --short
   ```

2. Inspect the active gateway and companion service definitions, binary paths,
   configuration paths, and service accounts. Do not infer them from this
   document.
3. Copy the current binaries and configuration to a timestamped,
   operator-readable rollback directory on the target host. Record the exact
   directory and the pre-deploy binary digests.
4. Capture service status and the error-level journal baseline before making
   changes.
5. Build both artifacts from the recorded commit:

   ```bash
   make build
   make build-node
   ```

   Record the SHA-256 digest of each artifact before transfer.

## Configuration gate

The existing smoke profile may add only model-discovery metadata for authority
it already owns:

- one executable alias mapped to an already allowed executable;
- one working-scope alias mapped to an already allowed root;
- only environment names already present in the environment allowlist;
- bounded guidance and examples that validate against the projected schema.

Validate the companion configuration before replacing a running binary.
Confirm that the model-visible command contract contains aliases and effective
numeric ceilings, but none of the raw destinations or environment values.

## Rollout

1. Replace the companion and gateway binaries using the host's existing
   transactional deployment procedure.
2. Restart the companion first, wait for its authenticated node session, then
   restart or safely reload the gateway as required by the deployed service.
3. Require both service managers to report stable healthy states.
4. Compare post-deploy error-level journals with the baseline. Stop and roll
   back on a new persistent error, pairing regression, catalog reapproval that
   was not planned, or identity change.

The companion and gateway must report the same recorded source commit. A mixed
version rollout is not acceptable evidence.

## Gateway-owned smoke flow

Start from an outcome-only prompt. Do not include target names, command names,
aliases, paths, environment policy, schemas, revisions, or numeric ceilings in
the prompt.

The model must:

1. call `nodes` to list targets;
2. describe the selected target;
3. describe one approved command and read its alias-only schema;
4. call `nodes_invoke` with only discovered values and the returned revision;
5. traverse the existing approval interaction;
6. observe the durable result;
7. call `nodes_status` with the returned invocation ID.

Record the redacted run or turn identifier, invocation identifier, result
state, and trace identifier. Do not copy command input or output into the
evidence record.

## Stale-revision drill

Choose one reversible, non-destructive constraint already bound into discovery,
such as the smoke target executor selection or policy revision. Record the old
opaque revision, change the constraint, and retry the retained request once.

The required result is:

```json
{
  "status": "denied",
  "code": "DISCOVERY_STALE",
  "constraint": "command_policy",
  "action": "refresh_discovery"
}
```

There must be no approval creation, prepared observation, dispatched
observation, or duplicate command effect. Restore the exact prior constraint
immediately and confirm that command-specific discovery succeeds again.

## Evidence record

Store a redacted record with:

- deployment UTC timestamp and operator;
- merged source SHA;
- gateway and companion artifact digests;
- service names and health results;
- pre/post error-journal result;
- rollback directory;
- model run/turn ID, invocation ID, and passive trace ID;
- trace schema or event kind (`node.invocation.observed`);
- stale drill revision digest or redacted correlation, denial code, and zero
  duplicate-effect result;
- restore/recovery result;
- known limitations and the P1 admit/defer decision.

Never include credentials, raw node identity, endpoints, host paths,
environment values, command input/output, policy documents, catalog or plan
hashes, or private keys in the evidence record.

## Rollback

If any gate fails, restore both prior binaries and the exact prior
configuration from the recorded rollback directory, restart services in the
host's established order, and repeat health and journal checks. Record the
failure and defer P1; do not weaken an authority or redaction gate to complete
the rollout.
