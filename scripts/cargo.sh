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
#
# BUILDS_CARGO_LEAN=<variable>=<package>:<exe> names a Lean executable a test
# runs: <package> is a Lean package's folder from the checkout's top. The
# machine that runs cargo builds <exe> there with lake and sets <variable> to
# its path, so the tests and the executable they run come from one machine.
# `builds cargo` does this on whichever host runs cargo; without the tool it
# happens here.

builds="$HOME/.local/bin/builds"
[ -x "$builds" ] && exec "$builds" cargo "$@"
top=$(cd "$(dirname "$0")/.." && pwd)
if [ -n "${BUILDS_CARGO_BEFORE-}" ]; then
    before=$BUILDS_CARGO_BEFORE
    unset BUILDS_CARGO_BEFORE
    # shellcheck disable=SC2086
    (cd "$top" && $before) || exit $?
fi
if [ -n "${BUILDS_CARGO_LEAN-}" ]; then
    case $BUILDS_CARGO_LEAN in
        ?*=?*:?*) ;;
        *)
            echo "BUILDS_CARGO_LEAN must be <variable>=<package>:<exe>, not $BUILDS_CARGO_LEAN" >&2
            exit 2
            ;;
    esac
    lean_variable=${BUILDS_CARGO_LEAN%%=*}
    lean_target=${BUILDS_CARGO_LEAN#*=}
    unset BUILDS_CARGO_LEAN
    # lake query builds the executable and prints its path.
    lean_path=$(cd "$top/${lean_target%:*}" && lake query "${lean_target##*:}") || exit $?
    export "$lean_variable=$lean_path"
fi
exec cargo "$@"
