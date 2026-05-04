# Local Working Notes

This fork uses `bogdanovich/picoclaw:main` as the active local/runtime development
branch, not as a clean upstream mirror.

Branch policy:

- `main` is our working branch with all changes we actually run locally.
- `upstream-mirror` is a clean mirror of `sipeed/picoclaw:main`.
- Rebase `main` onto the latest `upstream/main` periodically to stay current.
- Do not open upstream PRs directly from `main`.
- For upstream PRs, create a clean topic branch from the latest `upstream/main`
  or `upstream-mirror`, then cherry-pick or manually port only the intended
  patch.
- Do not use a `[codex]` prefix in PR titles.
