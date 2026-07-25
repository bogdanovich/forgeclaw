#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

mode="${1:---changed}"
base="${PRE_PUSH_BASE:-origin/main}"
concurrency="${GOLANGCI_LINT_CONCURRENCY:-4}"
if [[ "$(uname -s)" == "Darwin" ]]; then
	default_cgo_enabled=0
else
	default_cgo_enabled=1
fi
cgo_enabled="${GOLANGCI_LINT_CGO_ENABLED:-$default_cgo_enabled}"
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
	run_step "Go formatting" golangci-lint fmt --config .golangci-format.yaml --diff
	run_step "all Go packages" env CGO_ENABLED="$cgo_enabled" golangci-lint run \
		--allow-serial-runners \
		--tests=false \
		--concurrency "$concurrency" \
		--build-tags=goolm,stdjson
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
if ! git diff --quiet "$merge_base"...HEAD -- \
	go.mod \
	go.sum \
	.golangci.yml \
	.golangci.yaml \
	.golangci-format.yaml \
	.golangci-lint-version; then
	lint_all
	exit
fi

changed_dirs=()
while IFS= read -r -d '' file; do
	dir="${file%/*}"
	if [[ "$dir" == "$file" ]]; then
		dir="."
	fi

	seen=false
	if ((${#changed_dirs[@]} > 0)); then
		for existing_dir in "${changed_dirs[@]}"; do
			if [[ "$existing_dir" == "$dir" ]]; then
				seen=true
				break
			fi
		done
	fi
	if [[ "$seen" == false ]]; then
		changed_dirs+=("$dir")
	fi
done < <(git diff --name-only --diff-filter=ACMRTUXBD -z "$merge_base"...HEAD -- '*.go')

if ((${#changed_dirs[@]} == 0)); then
	echo "pre-push: no changed Go packages relative to $base"
	exit
fi

packages=()
for dir in "${changed_dirs[@]}"; do
	if find "$dir" -maxdepth 1 -type f -name '*.go' -print -quit | grep -q .; then
		if [[ "$dir" == "." ]]; then
			packages+=(".")
		else
			packages+=("./$dir")
		fi
	fi
done

if ((${#packages[@]} == 0)); then
	echo "pre-push: changed Go files only removed packages"
	exit
fi

echo "pre-push: linting ${#packages[@]} changed Go package(s) relative to $base"
printf '  %s\n' "${packages[@]}"
run_step "changed Go package formatting" golangci-lint fmt \
	--config .golangci-format.yaml \
	--diff \
	"${packages[@]}"
run_step "changed Go packages" env CGO_ENABLED="$cgo_enabled" golangci-lint run \
	--allow-serial-runners \
	--tests=false \
	--concurrency "$concurrency" \
	--build-tags=goolm,stdjson \
	"${packages[@]}"
