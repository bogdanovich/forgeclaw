#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

companion_binary="$test_root/mintclaw-node"
integration_tests='^('\
'TestCompanionProcessAuthenticatesAndInvokesOverWSS|'\
'TestCompanionProcessTransfersFilesOverAuthenticatedWSS|'\
'TestNodeInvocationVerticalSliceWithApprovalAndRealCompanion|'\
'TestNodeFileTransferVerticalSliceWithApprovalAndDelivery|'\
'TestNodeServiceStatusModelToSystemdRealProcessVerticalSlice'\
')$'

cd "$repository_root"
go build -o "$companion_binary" ./cmd/mintclaw-node

MINTCLAW_NODE_TEST_BINARY="$companion_binary" go test \
  -count=1 \
  -tags goolm,stdjson,integration \
  -run "$integration_tests" \
  ./cmd/mintclaw-node \
  ./pkg/gateway
