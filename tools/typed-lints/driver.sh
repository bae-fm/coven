#!/bin/sh
# Points tools/typed-lints/built at the typed lints' driver for this
# checkout's sources and toolchain, building it when it is missing.
# clippy.sh names this in BUILDS_CARGO_BEFORE, so it runs on the machine that
# runs cargo. With the `builds` tool, `builds tool` keeps one driver per key on
# each machine and copies it from another machine that has it (tool.sh);
# without it (CI), the driver builds in this checkout's target folder.
set -eu
cd "$(dirname "$0")/../.."
builds="$HOME/.local/bin/builds"
if [ -x "$builds" ]; then
    exec "$builds" tool typed-lints >/dev/null
fi
out="$PWD/target/typed-lints/built"
rm -rf "$out"
BUILDS_TOOL_WORK="$PWD/target/typed-lints/work" tools/typed-lints/tool.sh build "$out"
"$out/check"
rm -rf tools/typed-lints/built
ln -s "$out" tools/typed-lints/built
