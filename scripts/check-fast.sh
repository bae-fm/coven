#!/bin/bash
# The fast checks of Appendix F.6, which the pre-commit hook runs: formatting,
# clippy, the checker's rules of Appendix F.1 to F.5, every crate built without
# test code, and the spec's readability checks (scripts/check-spec.py).
#
#   scripts/check-fast.sh
# COVEN_CHECK_PACKAGES may select Cargo packages for clippy and checker
# diagnostics. Formatting and the checker's analysis still read the workspace.
# Quick mode (`builds mode`, or HOOK_MODE=quick) lints only what the staged
# files can break: the packages with a staged file and every package that
# depends on one of them, the whole workspace when a manifest, the lockfile or
# a lint or toolchain file is staged or nothing is. It runs the landing's own
# clippy (`--all-targets --all-features -D warnings`), so a denied lint such as
# `map_err_ignore` or a call `clippy.toml` bans fails the hook, not the landing.
# It skips the build without features.
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: scripts/check-fast.sh (takes no arguments)" >&2
    exit 2
fi

cd "$(dirname "$0")/.."

source scripts/hooks/hook-mode.sh
MODE=$(hook_mode)

step() { echo ""; echo "── $1"; }

step "spec/coven.md and spec/format.md against spec/WRITING.md"
"$(command -v python3 || command -v python)" scripts/check-spec.py

step "cargo fmt --check"
cargo fmt --all --check

# The packages, one per line, a change to the staged files can break: the
# packages with a staged path under their directory and each package that
# depends on one of them, through any chain of dependencies of any kind. Prints
# nothing when the whole workspace is affected or nothing is staged.
staged_affected_packages() {
    git --no-pager diff --cached --name-only | python3 -c '
import json, os, subprocess, sys
staged = [line.strip() for line in sys.stdin if line.strip()]
if not staged or any(path in ("Cargo.toml", "Cargo.lock", "clippy.toml", "rust-toolchain.toml")
                     or path.startswith(".cargo/") for path in staged):
    sys.exit(0)
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--no-deps", "--format-version", "1", "--offline"]))
root = metadata["workspace_root"]
names = {package["name"] for package in metadata["packages"]}
directory = {package["name"]: os.path.relpath(os.path.dirname(package["manifest_path"]), root)
             for package in metadata["packages"]}
dependents = {name: set() for name in names}
for package in metadata["packages"]:
    for dependency in package["dependencies"]:
        if dependency["name"] in names:
            dependents[dependency["name"]].add(package["name"])
pending = [name for name in names
           if any(path.startswith(directory[name] + "/") for path in staged)]
affected = set()
while pending:
    name = pending.pop()
    if name not in affected:
        affected.add(name)
        pending.extend(dependents[name])
print("\n".join(sorted(affected)))
'
}

packages=(--workspace)
checker_args=(.)
if [ -z "${COVEN_CHECK_PACKAGES:-}" ] && [ "$MODE" = quick ]; then
    COVEN_CHECK_PACKAGES=$(staged_affected_packages | tr '\n' ' ' | xargs)
fi
if [ -n "${COVEN_CHECK_PACKAGES:-}" ]; then
    read -r -a selected <<< "$COVEN_CHECK_PACKAGES"
    packages=()
    for package in "${selected[@]}"; do
        packages+=(-p "$package")
        checker_args+=("$package")
    done
fi
step "owner-construction-check: Appendix F.1 dependencies, F.2 and F.3 capabilities and owners, F.5 conventions"
scripts/cargo.sh run --quiet -p owner-construction-check -- "${checker_args[@]}"

step "cargo deny check advisories bans licenses: the crates Appendix F.1 refuses and the licenses coven accepts"
cargo deny check advisories bans licenses

step "cargo clippy ${packages[*]} --all-targets --all-features"
scripts/cargo.sh clippy "${packages[@]}" --all-targets --all-features -- -D warnings

if [ "$MODE" = quick ]; then
    exit 0
fi

# Clippy's build turns every feature on, test-utils among them, so code a
# test-only feature gates by mistake compiles there. Each crate's own build,
# with no features, as an app builds it, is checked too.
step "every crate built without test code"
scripts/cargo.sh check "${packages[@]}"
