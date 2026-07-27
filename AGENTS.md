# Local Working Notes

Branch policy:

- `main` is the public MintClaw development and release branch.
- Start each change from the latest `origin/main` in a focused topic branch.
- Keep pull requests narrowly scoped and target `main`.
- Do not use a `[codex]` prefix in PR titles.
- Use conventional PR titles with a functional scope and colon, such as
  `feat(providers): add Gemini search`, `fix(telegram): handle media groups`,
  `fix(agents): preserve topic routing`, or `feat(tools): add update_plan`.

Formatting policy:

- CI enables `golines` as a formatter with a 120-character maximum line
  length (`.golangci-format.yaml`), in addition to `gofmt` and `gofumpt`.
- Before committing changed Go files, run `make fmt` from the repository root,
  then validate the affected non-test packages with `make lint` or
  `scripts/pre-push-lint.sh --changed`. Formatting still covers all Go files,
  including tests. Do not rely on `gofmt` alone.
- Manually wrap composite literals, calls, conditions, and test assertions
  approaching 120 characters when the formatter cannot run locally. This
  prevents avoidable CI-only `golines` failures.
