#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

mode="${1:---changed}"
base="${PRE_PUSH_BASE:-origin/main}"

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

lint_all() {
	run_step "all Go packages" golangci-lint run --allow-serial-runners --build-tags=goolm,stdjson
}

run_step "golangci-lint config verify" golangci-lint config verify

case "$mode" in
--all)
	lint_all
	exit
	;;
--changed)
	;;
*)
	echo "usage: $0 [--changed|--all]" >&2
	exit 2
	;;
esac

if ! git rev-parse --verify --quiet "${base}^{commit}" >/dev/null; then
	echo "pre-push: $base is unavailable; falling back to full lint"
	lint_all
	exit
fi

merge_base="$(git merge-base "$base" HEAD)"

# Dependency and lint-policy changes can affect every package.
if ! git diff --quiet "$merge_base"...HEAD -- go.mod go.sum .golangci.yml .golangci.yaml; then
	lint_all
	exit
fi

declare -A changed_dirs=()
while IFS= read -r -d '' file; do
	dir="${file%/*}"
	if [[ "$dir" == "$file" ]]; then
		dir="."
	fi
	changed_dirs["$dir"]=1
done < <(git diff --name-only --diff-filter=ACMRTUXBD -z "$merge_base"...HEAD -- '*.go')

if ((${#changed_dirs[@]} == 0)); then
	echo "pre-push: no changed Go packages relative to $base"
	exit
fi

packages=()
while IFS= read -r dir; do
	if find "$dir" -maxdepth 1 -type f -name '*.go' -print -quit | grep -q .; then
		if [[ "$dir" == "." ]]; then
			packages+=(".")
		else
			packages+=("./$dir")
		fi
	fi
done < <(printf '%s\n' "${!changed_dirs[@]}" | sort)

if ((${#packages[@]} == 0)); then
	echo "pre-push: changed Go files only removed packages"
	exit
fi

echo "pre-push: linting ${#packages[@]} changed Go package(s) relative to $base"
printf '  %s\n' "${packages[@]}"
run_step "changed Go packages" golangci-lint run --allow-serial-runners --build-tags=goolm,stdjson "${packages[@]}"
