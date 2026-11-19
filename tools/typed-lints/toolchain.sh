# What building the typed lints needs from the toolchain rustup picks in the
# folder this is sourced from (coven's pin, or RUSTUP_TOOLCHAIN): sets
# `toolchain`, `host`, `commit` (the commit its rustc was built from),
# `rustc_dev` (the component that holds its compiler crates), `exe` (the
# suffix of a program's file name on the host), `clippy_home` (the folder
# that holds clippy's source for the toolchain, shared by every checkout on
# this machine) and `clippy_commit` (the commit the source that clippy/ leads
# to is from, if any).
version=$(rustc -vV)
commit=$(printf '%s\n' "$version" | sed -n 's/^commit-hash: //p')
host=$(printf '%s\n' "$version" | sed -n 's/^host: //p')
if [ -z "$commit" ] || [ -z "$host" ]; then
    echo "typed lints: rustc -vV names no commit or host" >&2
    exit 1
fi
toolchain=$(rustup show active-toolchain | cut -d' ' -f1)
rustc_dev="rustc-dev-$host"
case "$host" in
*windows*) exe=.exe ;;
*) exe= ;;
esac
# One folder per toolchain channel (1.100.0), the name the `installed` line of
# .build-dirs keeps while a checkout's rust-toolchain.toml names it.
clippy_home="${XDG_CACHE_HOME:-$HOME/.cache}/typed-lints/${toolchain%-"$host"}"
clippy_commit=$(cat clippy/.rust-commit 2>/dev/null || true)
has_rustc_dev() {
    rustup component list --installed --toolchain "$toolchain" | grep -qx "$rustc_dev"
}
