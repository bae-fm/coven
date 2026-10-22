#!/bin/sh
# Links this checkout's build output paths, listed in .build-dirs, into the
# machine's build volume and recreates any folder behind a link that was
# deleted, through the machine's `builds` tool. The hooks and every build entry
# point run this first. Without the tool (CI, another machine) the build tools
# write into real directories at those paths.

builds="$HOME/.local/bin/builds"
[ -x "$builds" ] || exit 0
exec "$builds" setup "$(cd "$(dirname "$0")/.." && pwd -P)"
