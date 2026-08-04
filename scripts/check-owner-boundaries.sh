#!/usr/bin/env sh
# The repository's structural gates, in one place so pre-commit, check.sh, and
# CI enforce the same set. Each flag is enabled only once the tree has zero
# violations for it; new boundaries join this list when they go green.
set -eu

cd "$(dirname "$0")/.."

cargo run --quiet -p owner-construction-check -- \
    --database-boundary \
    --retained-service-construction \
    --retained-service-returns \
    --retained-capability-parameters \
    --network-boundary \
    --crypto-boundary \
    --keyring-boundary \
    --runtime-boundary \
    --ambient-boundary \
    --filesystem-boundary \
    .
