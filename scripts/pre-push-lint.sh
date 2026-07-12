#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

run_step() {
	local label="$1"
	shift
	local started elapsed status
	started="$(date +%s)"
	echo "pre-push: starting ${label}"
	if "$@"; then
		elapsed=$(( $(date +%s) - started ))
		echo "pre-push: completed ${label} in ${elapsed}s"
	else
		status=$?
		elapsed=$(( $(date +%s) - started ))
		echo "pre-push: failed ${label} after ${elapsed}s (exit ${status})" >&2
		return "$status"
	fi
}

# Keep these commands aligned with .github/workflows/pr.yml.
run_step "golangci-lint config verify" golangci-lint config verify
run_step "golangci-lint run (build tags: goolm,stdjson)" golangci-lint run --build-tags=goolm,stdjson
