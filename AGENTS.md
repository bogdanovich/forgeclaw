# Local Working Notes

This fork uses `bogdanovich/forgeclaw:main` as the active local/runtime
development branch. The upstream project is tracked separately as
`sipeed/picoclaw:main` through the `upstream` remote.

Branch policy:

- `main` is the public feature-rich ForgeClaw branch with all changes we
  actually run locally.
- `upstream/main` tracks `sipeed/picoclaw:main` and should never be modified
  directly.
- `upstream-mirror` is an optional local backup mirror of `sipeed/picoclaw:main`.
- Merge or rebuild `main` onto the latest `upstream/main` periodically to stay
  current.
- Do not open upstream PRs directly from `main`.
- For upstream PRs, create a clean topic branch from the latest `upstream/main`
  or `upstream-mirror`, then cherry-pick or manually port only the intended
  patch.
- Do not use a `[codex]` prefix in PR titles.
- Use conventional PR titles with a functional scope and colon, such as
  `feat(providers): add Gemini search`, `fix(telegram): handle media groups`,
  `fix(agents): preserve topic routing`, or `feat(tools): add update_plan`.

Formatting policy:

- CI enables `golines` as a formatter with a 120-character maximum line
  length (`.golangci.yaml`), in addition to `gofmt` and `gofumpt`.
- Before committing changed Go files, run `golangci-lint fmt` from the
  repository root, then validate the affected packages with
  `golangci-lint run --build-tags=goolm,stdjson <changed Go packages>`. Do not
  rely on `gofmt` alone.
- Manually wrap composite literals, calls, conditions, and test assertions
  approaching 120 characters when the formatter cannot run locally. This
  prevents avoidable CI-only `golines` failures.
