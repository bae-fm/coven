#!/bin/sh
# Runs cargo with these arguments. On a machine with the `builds` tool it runs
# through `builds cargo`, which records the build copies (one per crate,
# profile and dependency set) cargo used, so `builds setup` keeps those while
# it deletes the ones no build has used for days; a program's or the tests'
# output passes through. Without the tool (CI, another machine) this is cargo
# itself.
#
# BUILDS_CARGO_BEFORE names a program of the checkout, and its arguments, to
# run in the checkout before cargo. `builds cargo` runs it on the machine that
# runs cargo; without the tool it runs here. Either way the program runs
# without the variable, so a cargo it runs runs no program first.

builds="$HOME/.local/bin/builds"
[ -x "$builds" ] && exec "$builds" cargo "$@"
if [ -n "${BUILDS_CARGO_BEFORE-}" ]; then
    before=$BUILDS_CARGO_BEFORE
    unset BUILDS_CARGO_BEFORE
    # shellcheck disable=SC2086
    (cd "$(dirname "$0")/.." && $before) || exit $?
fi
exec cargo "$@"
