# MintClaw Full Rename Migration Plan

## Status

This document is the authoritative execution plan for renaming the repository
and product from ForgeClaw and PicoClaw to MintClaw.

The rename is intentionally breaking:

- MintClaw becomes an independent project rather than an upstream-tracking
  fork.
- Product-facing compatibility aliases for the old names are not retained.
- Git history is not rewritten.
- Genuine third-party project names and license attributions are not altered.

This planning document necessarily contains the old names. The final migration
must delete this file, or replace it with a completed migration record that
contains no legacy product names, before applying the zero-legacy-string
acceptance gate.

## Goal Reference

Once this plan is merged, a durable implementation goal can use this compact
objective:

> Execute `docs/migration/mintclaw-rename.md` completely from the latest
> `origin/main` using the autonomous PR workflow. Preserve its scope,
> sequencing, stop conditions, validation matrix, and completion gates. Do not
> declare completion until every required PR is merged and the final
> zero-legacy-string audit passes against the actual merged tree.

The implementation goal should link to the file instead of duplicating this
plan. The executor must read the complete file before making changes and reread
it during the final audit.

## Product Decisions

These decisions are part of the requested outcome:

1. The GitHub repository becomes `bogdanovich/mintclaw`.
2. The Go module becomes `github.com/bogdanovich/mintclaw`.
3. The main executable becomes `mintclaw`.
4. Companion executables use the same prefix, including `mintclaw-node` and
   `mintclaw-launcher`.
5. Product home and state default to `~/.mintclaw`.
6. Product-owned environment variables use the `MINTCLAW_` prefix.
7. Product-owned package, image, service, application, artifact, bundle, and
   update identifiers use MintClaw naming.
8. The repository no longer carries an upstream-sync or upstream-PR workflow.
9. No source-level compatibility wrappers, fallback parsing, or legacy aliases
   remain after the final cutover.
10. Existing user data is backed up and moved operationally; it is never
    deleted automatically.

## Observed Baseline

The baseline scan from `origin/main` found:

| Surface | Approximate baseline |
| --- | ---: |
| Tracked files whose content contains a legacy product name or first-party URL | 875 |
| Tracked paths whose filename or directory contains a legacy product name | 130 |
| Go/module files using the old first-party import path | 659 |
| Unique `PICOCLAW_` environment names | 332 |
| Unique `FORGECLAW_` environment names | 5 |

The highest-volume areas are `pkg/`, `cmd/`, `web/`, `docs/`, Docker assets,
workspace skills, integration tests, scripts, and root build files. These
counts are discovery aids, not fixed acceptance targets. The executor must
rerun inventory from the current base because the repository continues to
change.

## Required Ordering

### 1. Rename the GitHub repository first

The owner renames `bogdanovich/forgeclaw` to `bogdanovich/mintclaw` before the
implementation goal starts.

This order gives migration PRs, module references, release links, and package
publishing one final repository identity. GitHub redirects normal web and Git
traffic from the old repository name, but published GitHub Actions and GitHub
Pages require separate checks.

Do not create a new repository using the old repository name after the rename,
because that would break GitHub's redirect.

### 2. Update local and connected repository state

Before creating an implementation branch:

```bash
git remote set-url origin git@github.com:bogdanovich/mintclaw.git
git remote -v
git fetch origin main --prune
```

Remove local `upstream` and `upstream-mirror` remotes if present. Reconnect or
refresh GitHub integrations if they bind permissions to the old repository
identity.

### 3. Freeze identity-affecting changes

Avoid merging unrelated changes to module paths, command directories, build
artifacts, release workflows, installers, or runtime paths during the rename.
Normal unrelated development may continue only when it cannot invalidate the
inventory or create repeated conflict resolution.

## Scope Invariants

The implementation must preserve these invariants:

- Every merged intermediate head builds and passes required CI.
- The functional rename is atomic wherever splitting would leave imports,
  commands, packaging, or tests inconsistent.
- Existing runtime data is copied or moved only after a verified backup.
- The implementation does not silently read, mutate, or delete the old product
  home after cutover.
- Third-party dependency identities remain accurate.
- Git commit history and attribution remain intact.
- No test, linter, security check, or branch protection is weakened to finish
  the rename.
- Unknown external identities are resolved by the owner, not guessed.

## Complete Inventory

Inventory both tracked path names and file content. A normal text search is
necessary but insufficient.

### Repository and source identity

- `go.mod`, first-party Go imports, generated Go references, package comments,
  import-path assertions, test fixtures, and tooling configuration
- command directories under `cmd/`
- exported and internal Go identifiers containing product names
- executable names, CLI `Use`, help, examples, banners, version output,
  user-agent strings, update commands, and error messages
- embedded files, generated source inputs, and workspace bootstrap content

### Runtime and persistent identity

- product home, config, authentication, security, workspace, state, task,
  interaction, cache, media, diagnostics, trace, log, lock, socket, database,
  PID, and temporary paths
- configuration loaders and environment bindings
- environment variables used by production, tests, integration fixtures,
  scripts, Docker, and CI
- service names, process lookup, health checks, deploy workers, restart
  controllers, and status output

### Build and distribution identity

- Makefiles, shell and PowerShell scripts, GoReleaser or equivalent release
  configuration, Dockerfiles, Compose projects and services
- GHCR and other image coordinates
- archives, checksums, Android libraries and APK names, macOS app bundles and
  launchd labels, Linux systemd units, Windows services/tasks and installers
- launcher and node build outputs, desktop files, icons, bundle IDs, package
  IDs, application metadata, and self-update URLs
- release notes, download commands, install/uninstall/upgrade scripts, and
  generated artifacts

### Repository automation and external identity

- GitHub workflows, actions, caches, environments, variables, secrets,
  webhooks, deploy keys, badges, issue templates, and PR templates
- GitHub Pages configuration and custom domains
- published GitHub Actions consumed by other repositories
- package/container registries and signing/notarization identities
- submodules and links to raw GitHub content

### Documentation and assets

- README, AGENTS.md, CONTRIBUTING, ROADMAP, architecture, operations, security,
  channel, migration, testing, and reference documentation
- examples, comments, snapshots, golden files, copied terminal output, image
  alt text, SVG/XML metadata, desktop entries, and binary asset metadata
- workspace skills and reviewer configuration, including product-prefixed
  directories such as `.forgeclaw`
- filenames of images and other assets even when their binary content contains
  no searchable text

## Rename Map

The final mapping is:

| Old class | Final class |
| --- | --- |
| Repository | `bogdanovich/mintclaw` |
| Go module | `github.com/bogdanovich/mintclaw` |
| Main command directory | `cmd/mintclaw` |
| Node command directory | `cmd/mintclaw-node` |
| Main executable | `mintclaw` |
| Node executable | `mintclaw-node` |
| Launcher executable | `mintclaw-launcher` |
| Product home | `~/.mintclaw` |
| Environment prefix | `MINTCLAW_` |
| Product display name | `MintClaw` |
| Lowercase identifier | `mintclaw` |
| Uppercase identifier | `MINTCLAW` |

Additional product-owned names must follow the same mapping. Do not mechanically
rename third-party names merely because they contain the word `claw`.

## Execution Plan

### Phase 0: External preflight

1. Confirm the GitHub rename and repository permissions.
2. Update `origin`; remove upstream remotes.
3. Record the exact base commit.
4. Inventory all current matches by category and path.
5. Identify external decisions:
   - GitHub Pages domain
   - container/package ownership
   - signing and notarization identities
   - Android/macOS/Windows application or bundle IDs
   - published GitHub Actions
   - secrets and environment variables managed outside Git
6. Stop and request only the missing external values. Do not begin a partial
   identity migration when a required final value is unknown.

### Phase 1: Atomic functional rename

This is the primary code PR. It must leave the repository buildable.

1. Rename the Go module and all first-party imports.
2. Move command directories and rename command constructors, CLI identity,
   executable outputs, and tests.
3. Rename product-owned Go identifiers and embedded strings.
4. Rename environment keys, config loaders, build variables, runtime defaults,
   state paths, and tests.
5. Rename node, launcher, gateway, updater, deploy, service, and lifecycle
   identities.
6. Rename Docker, Compose, packaging, installer, release, and artifact
   identities required for builds and CI.
7. Rename tracked files and directories needed by functional code, including
   embedded assets and workspace content.
8. Update CI and build scripts in the same PR so every gate uses the new
   commands and module path.
9. Include a detailed operational cutover note in the PR description rather
   than adding committed legacy compatibility documentation.

Do not split module/import changes from the command/build changes when doing so
would make either PR uncompilable.

### Phase 2: Documentation and remaining tracked assets

After Phase 1 merges:

1. Branch from the new `origin/main`.
2. Rewrite all user, developer, operations, security, and architecture
   documentation for independent MintClaw.
3. Replace fork/upstream policies in AGENTS.md and CONTRIBUTING.
4. Rename remaining assets, skills, examples, snapshots, and metadata.
5. Remove stale upstream comparison instructions and links.
6. Delete this plan before the final zero-string audit, or replace it with a
   legacy-name-free completion record.

This phase may be combined with Phase 1 if repository policy or link validation
requires documentation to match the functional rename atomically.

### Phase 3: External configuration and release cutover

Coordinate repository-external state:

1. Rename or recreate GitHub Pages configuration.
2. Update GitHub repository description, topics, environments, variables,
   secrets, webhooks, and deploy integrations.
3. Update container and package publishing targets.
4. Update signing, notarization, application IDs, and release credentials.
5. Publish new-name artifacts and verify download/update paths.
6. Update every deployed machine's environment names and service definitions.

External destructive changes require exact target verification and a rollback
path.

### Phase 4: User-data cutover

Before starting new binaries:

1. Stop running services.
2. Record the current version and service status.
3. Back up the complete existing product home and external config.
4. Copy or move the required config, authentication, security, workspace,
   state, task, interaction, media, and other durable data into
   `~/.mintclaw`.
5. Preserve permissions and ownership.
6. Start MintClaw against the copied data.
7. Verify sessions, tasks, interactions, schedules, credentials, channels,
   nodes, and diagnostics.
8. Keep the backup until the owner explicitly accepts the cutover.

Do not automatically delete the source data. A temporary migration helper must
remain uncommitted or be removed before the final repository audit.

### Phase 5: Final purge and audit

1. Fetch the final `origin/main`.
2. Repeat content, path, asset metadata, build, runtime, and external scans.
3. Classify any remaining match as:
   - prohibited first-party legacy identity;
   - legitimate third-party identity or license attribution;
   - Git history, which is outside the tracked-tree purge.
4. Remove every prohibited match.
5. Rerun the complete validation matrix.
6. Confirm every implementation PR is merged and no actionable review thread
   remains.

## PR Strategy

Prefer two implementation PRs:

1. **Functional identity PR**: module, imports, commands, runtime/config,
   build, CI, services, packaging, release identity, and their tests.
2. **Documentation and purge PR**: docs, policies, remaining assets and
   metadata, deletion of this planning file, and final zero-string audit.

Use one atomic PR instead when CI, embedded assets, release links, or package
boundaries make the split non-buildable. Do not create dozens of mechanical PRs
that repeatedly conflict or leave mixed identities on `main`.

Each PR must:

- branch from the latest `origin/main`;
- be ready for review;
- state dependencies and remaining phases;
- pass all required CI;
- use merge-commit semantics unless repository policy requires a queue;
- be confirmed merged before dependent work proceeds.

## Validation Matrix

### Static identity checks

- `go.mod` contains only the new module path.
- `go list ./...` and `go list -deps ./...` resolve first-party packages under
  `github.com/bogdanovich/mintclaw`.
- tracked path names contain no prohibited legacy identity.
- tracked text and asset metadata contain no prohibited legacy identity.
- generated files are regenerated from renamed sources rather than patched
  only at their output.

### Go and repository checks

- targeted unit and integration tests for every affected subsystem
- `make fmt`
- changed-package lint
- full lint
- affected-package tests
- `go test ./...`
- practical race tests for shared runtime packages
- `go vet` or repository-equivalent validation

### Executables

Build and smoke-test:

- `mintclaw`
- `mintclaw-node`
- `mintclaw-launcher`
- every other product executable emitted by repository build targets

Verify:

- `--help`, `version`, onboarding, status, gateway, agent, update, install, and
  uninstall paths
- no old executable is produced
- process and service discovery uses only new names

### Configuration and state

- all documented `MINTCLAW_` variables are accepted
- old product prefixes are not accepted
- defaults create only `~/.mintclaw`
- config, authentication, security, workspace, task, interaction, media,
  diagnostics, and logs resolve beneath the new home when applicable
- explicit path overrides still work
- data cutover preserves durable records

### Distribution

- Docker and Compose builds and smoke tests
- container names, services, images, health checks, and volumes
- release dry run, checksums, archive names, and updater URLs
- available Linux, macOS, Windows, Android, node, and launcher packaging tests
- installer install/uninstall behavior and application metadata

### External state

- GitHub Actions and required checks
- GitHub Pages and link validation
- registry publication permissions
- webhooks and deployment integrations
- signing/notarization configuration
- final repository clone/fetch/push using the new URL

## Search and Audit Guidance

Search both content and tracked paths. At minimum, cover case-insensitive,
joined, spaced, hyphenated, and underscored forms of both legacy names, old
GitHub URLs, old module paths, old executable names, and old environment
prefixes.

Do not blindly exempt:

- tests;
- comments;
- fixtures;
- documentation;
- generated manifests;
- reviewer configuration;
- workspace skills;
- release snapshots;
- asset filenames or metadata.

Allowed exceptions are limited to accurate third-party identities and license
attributions. Every exception must be listed with a reason in the final audit.

## Stop Conditions

Stop and request owner input instead of inventing values when any of these is
unknown:

- final domain or GitHub Pages strategy;
- registry namespace or package ownership;
- application, bundle, or signing identity;
- notarization or publishing credentials;
- whether a published GitHub Action requires a compatibility repository;
- a destructive external rename that could orphan releases or user data.

Do not stop merely because the change is large, mechanical, or produces many
test updates.

## Completion Gates

The migration is complete only when all statements are proven:

1. GitHub and local `origin` identify `bogdanovich/mintclaw`.
2. The merged module and all first-party imports use
   `github.com/bogdanovich/mintclaw`.
3. All product binaries, services, packages, images, artifacts, application
   IDs, paths, environment keys, UI, and documentation use only MintClaw
   naming.
4. No prohibited legacy product identity remains in tracked path names,
   tracked content, generated manifests, or asset metadata.
5. No source compatibility aliases, legacy environment parsing, fallback
   paths, or committed migration documentation preserve the old names.
6. Git history remains intact and legitimate third-party identities remain
   accurate.
7. New binaries build and run; install, update, deploy, config, and state flows
   work.
8. User data was backed up and the cutover was verified without automatic
   source-data deletion.
9. Required CI is green on every final head.
10. Every required PR is merged into the actual `origin/main`, with no
    actionable unresolved review thread.

Passing a narrow grep or a single build is not sufficient evidence. The final
audit must prove each gate against the merged repository and relevant external
state.
