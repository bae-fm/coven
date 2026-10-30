#!/bin/sh
# Links this checkout's build output paths, listed in .build-dirs, into the
# machine's build volume, recreates any folder behind a link that was deleted,
# and deletes the build copies there that no build has used for days, through
# the machine's `builds` tool. The hooks and every build entry point run this
# first. Without the tool (CI, another machine) the build tools write into real
# directories at those paths.

builds="$HOME/.local/bin/builds"
[ -x "$builds" ] || exit 0
exec "$builds" setup "$(cd "$(dirname "$0")/.." && pwd -P)"
