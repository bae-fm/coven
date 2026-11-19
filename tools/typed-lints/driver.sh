#!/bin/sh
# Points tools/typed-lints/built at the typed lints' driver for this
# checkout's sources and toolchain. clippy.sh names this in
# BUILDS_CARGO_BEFORE, so it runs on the machine that runs cargo. With the
# `builds` tool, `builds tool` keeps one driver per key on each machine,
# copying it from another machine that has it or building it (tool.sh); when
# a commit hook's check runs it, `builds tool` only points the link at the
# driver the machine has, and fails at once, naming `builds setup`, when there
# is none. Without the tool (CI), the driver builds in this checkout's target
# folder.
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
