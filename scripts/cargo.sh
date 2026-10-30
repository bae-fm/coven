#!/bin/sh
# Runs cargo with these arguments. On a machine with the `builds` tool it runs
# through `builds cargo`, which records the build copies (one per crate,
# profile and dependency set) cargo used, so `builds setup` keeps those while
# it deletes the ones no build has used for days; a program's or the tests'
# output passes through. Without the tool (CI, another machine) this is cargo
# itself.

builds="$HOME/.local/bin/builds"
[ -x "$builds" ] && exec "$builds" cargo "$@"
exec cargo "$@"
