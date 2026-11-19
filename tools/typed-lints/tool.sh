#!/bin/sh
# The typed lints' driver as a tool that builds keeps once per key on each
# machine (the `tool` line of .build-dirs, `builds help` BUILT TOOLS):
#
#   tool.sh key             print the toolchain (its host and the commit its
#                           rustc was built from) and a hash of the sources the
#                           driver builds from
#   tool.sh build <folder>  build the driver into <folder>, with a `check`
#                           that makes it ready to run on the machine it is on
#
# A build copies the sources into $BUILDS_TOOL_WORK/src, at their places in
# coven's tree, rewriting only the files that changed, and builds there with
# its target folder beside them: one folder per toolchain on each machine, so
# a new key compiles only the lints, the policy model and the driver, not
# clippy's crates. The policy model is a member of coven's workspace and takes
# its package fields, dependencies and lints from coven's manifest, which the
# copy holds too.
set -eu
cd "$(dirname "$0")"
. ./toolchain.sh

# What the driver builds from, besides clippy's source at $commit, relative
# to coven's top folder.
sources() {
    (cd ../.. && find Cargo.toml tools/owner-construction-policy/Cargo.toml \
        tools/owner-construction-policy/src tools/typed-lints/Cargo.toml \
        tools/typed-lints/Cargo.lock tools/typed-lints/lints \
        tools/typed-lints/driver/Cargo.toml tools/typed-lints/driver/build.rs -type f) |
        LC_ALL=C sort
}

case "${1-}:$#" in
key:1)
    hash=$(sources | while read -r file; do
        printf '%s %s\n' "$(git hash-object "../../$file")" "$file"
    done | git hash-object --stdin)
    echo "$host-$commit $hash"
    ;;
build:2)
    out=$2
    work=${BUILDS_TOOL_WORK:?tool.sh build needs BUILDS_TOOL_WORK}
    ./prepare-build.sh
    src="$work/src"
    mkdir -p "$src"
    # cargo rebuilds a crate whose files are newer than its last build, so
    # an unchanged file keeps its time and a changed one gets a new one.
    sources | while read -r file; do
        if ! cmp -s "../../$file" "$src/$file"; then
            mkdir -p "$(dirname "$src/$file")"
            cp "../../$file" "$src/$file"
        fi
    done
    listed=$(sources)
    (cd "$src" && find . -type f | sed 's|^\./||') | while read -r file; do
        printf '%s\n' "$listed" | grep -qxF "$file" || rm "$src/$file"
    done
    rm -rf "$src/tools/typed-lints/clippy"
    ln -s "$clippy_home/$commit" "$src/tools/typed-lints/clippy"
    # The driver builds as a plain compile, never under itself.
    (cd ../.. && env -u RUSTC_WORKSPACE_WRAPPER -u CLIPPY_ARGS RUSTC_BOOTSTRAP=1 \
        scripts/cargo.sh build --quiet --release -p typed-lints-driver \
        --manifest-path "$src/tools/typed-lints/Cargo.toml" \
        --config "build.target-dir=\"$work/target\"")
    mkdir -p "$out"
    cp "$work/target/release/typed-lints-driver$exe" "$out/"
    printf '%s\n' "$toolchain" > "$out/toolchain"
    cat > "$out/check" <<'CHECK'
#!/bin/sh
# Points toolchain-lib at this machine's copy of the toolchain's libraries,
# where the driver finds librustc_driver (driver/build.rs), and runs it.
# Windows finds the DLL through PATH instead, which holds the toolchain's
# bin folder only under rustup, so the driver runs under `rustup run` there.
set -eu
cd "$(dirname "$0")"
toolchain=$(cat toolchain)
case "$toolchain" in
*windows*)
    rustup run "$toolchain" ./typed-lints-driver.exe --version >/dev/null
    ;;
*)
    sysroot=$(rustup run "$toolchain" rustc --print sysroot)
    rm -f toolchain-lib
    ln -s "$sysroot/lib" toolchain-lib
    ./typed-lints-driver --version >/dev/null
    ;;
esac
CHECK
    chmod +x "$out/check"
    ;;
*)
    echo "usage: tools/typed-lints/tool.sh key | tools/typed-lints/tool.sh build <folder>" >&2
    exit 2
    ;;
esac
