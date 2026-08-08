# Node Companion Release Signing

## Scope And Status

This runbook covers the trusted-release foundation for
[`node-companion-p4-admission.md`](../architecture/node-companion-p4-admission.md).
It publishes and verifies companion payloads, but does not enable remote
activation. Until the later P4 coordinator and model-invocation PRs merge, no
`node.update.v1` command is registered.

MintClaw release manifests use a detached Ed25519 signature. The signature is
over the exact manifest bytes. The manifest binds:

- release identity and stable or nightly channel;
- publication and expiration time;
- minimum coordinator version and exact coordinator, node protocol, and node
  config compatibility numbers; and
- filename, size, and SHA-256 digest for Linux and macOS on `amd64` and
  `arm64`.

TLS and GitHub asset checksums remain useful transport checks. They are not the
release trust root.

## Ownership And Secret Boundary

One repository owner or designated release administrator owns the signing
seed. The seed is generated on an operator-controlled machine and copied only
to the GitHub Actions secret `MINTCLAW_NODE_RELEASE_SIGNING_KEY`. It is never
committed, installed on a companion, placed in model context, written to a
MintClaw config, or sent through file transfer.

Companions receive only an operator-pinned public key. The release workflow
already has GitHub's bounded `contents: write` permission to upload release
assets. Signing does not give a workflow, model, gateway, or companion any new
repository permission. Conversely, the normal `GITHUB_TOKEN` cannot create a
valid node manifest without the separate signing seed.

The manual release and nightly workflows fail closed when the signing seed is
missing or malformed. They publish four separately identifiable
`mintclaw-node` archives plus `mintclaw-node-manifest.json` and
`mintclaw-node-manifest.sig`.

Both workflows run only when dispatched from `main`. The stable workflow also
requires the release tag commit to be an ancestor of the exact dispatched
`main` revision. Release tags and tag-derived nightly base versions must match
the manifest's strict SemVer grammar before checkout or shell use. Unsigned
artifacts are built without the signing seed, transferred through a bounded
Actions artifact, and downloaded by a fresh signing job. That job checks out
the trusted dispatched revision and builds the signer before the signing seed
enters its one signing step. Code checked out from the release tag never runs
on the signing runner or executes with the signing seed.
Stable releases remain GitHub drafts until manifest signing and upload succeed;
a signing failure cannot leave an unsigned node archive in a public release.

## Initial Key Provisioning

From a clean checkout of reviewed `main`, create new files in a private
operator directory:

```sh
export MINTCLAW_NODE_PRIVATE_KEY_PATH=/secure/offline/mintclaw-node-release.seed
export MINTCLAW_NODE_PUBLIC_KEY_PATH=/secure/offline/mintclaw-node-release.pub
go run ./cmd/mintclaw-node-release keygen
```

The private seed is created with mode `0600`; the public key uses `0644`.
`keygen` refuses to overwrite either file. Back up the seed in the existing
encrypted operator secret store, with recovery access limited to release
administrators.

Install the unpadded base64 seed as the repository Actions secret using an
authenticated owner session. Do not paste it into a PR, issue, workflow input,
shell history, or chat:

```sh
gh secret set MINTCLAW_NODE_RELEASE_SIGNING_KEY \
  --repo bogdanovich/mintclaw \
  < /secure/offline/mintclaw-node-release.seed
```

Record the public key and its reviewed source revision in the operator config.
An admitted profile remains disabled until the later activation PRs are
deployed:

```json
{
  "node_update_sources": {
    "production": {
      "base_url": "https://github.com/bogdanovich/mintclaw/releases/download",
      "public_key": "UNPADDED_BASE64_ED25519_PUBLIC_KEY"
    }
  },
  "node_update_policies": {
    "stable-node": {
      "enabled": false,
      "revision": "stable-node-v1",
      "source": "production",
      "channel": "stable",
      "approval": "required"
    }
  }
}
```

Disabled profiles cannot retain release aliases or downgrade authority. An
enabled profile must enumerate bounded operator aliases that map to exact tag
and version pairs. URLs, keys, tags, versions, channel, downgrade, and approval
are never model input.

## Release And Offline Verification

The release workflow builds exactly these archives:

- `mintclaw-node_Linux_x86_64.tar.gz`;
- `mintclaw-node_Linux_arm64.tar.gz`;
- `mintclaw-node_Darwin_x86_64.tar.gz`; and
- `mintclaw-node_Darwin_arm64.tar.gz`.

The signing command inspects regular files, enforces the 128 MiB payload
ceiling, computes each digest, requires all four sorted tuples, and writes the
manifest and detached signature without overwriting an existing file. Signed
catalog validity is bounded to 90 days. Stable manifests reject prerelease
versions; nightly manifests require one.

To verify downloaded manifest files without network access:

```sh
export MINTCLAW_NODE_RELEASE_PUBLIC_KEY="$(tr -d '\n' \
  < /secure/offline/mintclaw-node-release.pub)"
export MINTCLAW_NODE_MANIFEST_PATH=./mintclaw-node-manifest.json
export MINTCLAW_NODE_SIGNATURE_PATH=./mintclaw-node-manifest.sig
go run ./cmd/mintclaw-node-release verify
```

Verification rejects unknown or duplicate JSON fields, trailing data,
oversized documents, wrong key identity, invalid signatures, future or expired
catalogs, incompatible contracts, missing tuples, unexpected filenames, and
malformed size or digest values. Later P4 code must additionally hash the
downloaded archive and compare its exact tuple to this verified manifest
before staging.

## Rotation, Revocation, And Recovery

Routine rotation uses a new source alias and a new profile revision:

1. Generate a new seed/public pair out of band.
2. Replace the Actions secret during a release freeze.
3. Publish one reviewed release signed by the new key.
4. Add the new public key under a new source alias and point a disabled profile
   revision at it.
5. Verify the manifest offline, then move targets to the new profile only
   after the P4 canary gates pass.
6. Mark the old source `"revoked": true`, disable policies that reference it,
   and remove its seed from the active secret store.

An enabled policy cannot reference a revoked source. Stale discovery and
prepared work bind the old descriptor/profile revision and therefore fail
closed when the target moves.

For suspected private-key compromise, stop release workflows first, revoke the
source in companion configuration, disable affected target profiles, and
rotate. Deleting only a GitHub asset or changing a checksum is not revocation.
Previously staged payloads signed by the revoked key must not be activated.

If the signing seed is lost but not compromised, existing verified companions
continue to run. Remote update remains unavailable until a new key and profile
revision are provisioned. Never recover availability by accepting unsigned
artifacts, a model-provided digest, or the repository checksum as authority.
