# Contributing to MintClaw

MintClaw is an independent Go agent runtime for personal automation, MCP-heavy
workflows, task delivery, media handling, context management, and multi-agent
workflows. Contributions should preserve one canonical MintClaw identity and
optimize for reliable deployed behavior.

## Repository Layout

- `origin`: `git@github.com:bogdanovich/mintclaw.git`
- `main`: public MintClaw development and release branch.
- topic branches: focused changes created from the latest `origin/main`.

## Development Setup

Prerequisites:

- Go 1.26 or later
- the golangci-lint version recorded in `.golangci-lint-version`
- `make`
- Node.js 22+ and pnpm 10.33.0+ for launcher/frontend changes

Build and test:

```bash
make deps
make build
make test
make lint-docs
```

For frontend/launcher work:

```bash
(cd web/frontend && pnpm install --frozen-lockfile)
make build-launcher
```

## Branching

For a MintClaw change:

```bash
git checkout main
git pull origin main
git checkout -b feat/short-description
```

Target MintClaw PRs at `main`.

## Code Style

- Keep changes narrowly scoped.
- Prefer existing package boundaries and local helper APIs.
- Avoid unnecessary abstractions.
- Add or update tests for behavioral changes.
- Run focused package tests before pushing.
- Format all Go files, including tests, then lint non-test code:

```bash
make fmt-check
make lint
```

Run `make fmt` to apply formatting. The pre-push hook checks formatting and
runs the same linter rules expected by GitHub checks.

## Documentation

The root `README.md` is the authoritative MintClaw entry document.

Use `docs/README.md` for documentation layout and naming conventions. Run:

```bash
make lint-docs
```

Documentation guidelines:

- Keep MintClaw-specific docs in English unless translations are intentionally
  maintained.
- Keep project and release links on official MintClaw repository endpoints.
- Do not add unmaintained product domains or download instructions.
- Use only MintClaw command, package, module, environment, and config names.
- Keep command names such as `mintclaw`, paths such as `~/.mintclaw`, and Go
  module references when they describe the actual current binary/runtime.

## PR Expectations

PRs should include:

- a concise description of the change;
- the reason for the change;
- test commands run;
- screenshots/logs when user-facing behavior changes;
- AI assistance disclosure when relevant.

Reviewers should prioritize:

1. correctness and regressions;
2. security and tool-safety boundaries;
3. concurrency and async delivery behavior;
4. context/session isolation;
5. maintainability and simplicity;
6. test coverage.

## AI-Assisted Development

AI assistance is acceptable, but the author remains responsible for the change.

Before opening or merging AI-assisted code:

- read the diff carefully;
- verify behavior with tests or a concrete manual run;
- inspect security-sensitive paths yourself;
- remove speculative or over-engineered output.

## Commit Messages

Use concise imperative messages, preferably with a functional scope:

```text
fix(agent): preserve media delivery status
feat(tasks): add task delivery status view
docs: clarify release workflow
```

Avoid `[codex]` prefixes in commit or PR titles.

## Communication

Use GitHub issues and PR comments for durable project discussion. Keep local
deployment notes that do not belong in project documentation outside this
source repository.
