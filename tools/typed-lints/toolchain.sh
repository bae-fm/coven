# What building the typed lints needs from the toolchain rustup picks in the
# folder this is sourced from (coven's pin, or RUSTUP_TOOLCHAIN): sets
# `toolchain`, `host`, `commit` (the commit its rustc was built from) and
# `clippy_commit` (the commit clippy/ holds the source of, if any), and
# `rustc_dev` to the component that holds its compiler crates.
version=$(rustc -vV)
commit=$(printf '%s\n' "$version" | sed -n 's/^commit-hash: //p')
host=$(printf '%s\n' "$version" | sed -n 's/^host: //p')
if [ -z "$commit" ] || [ -z "$host" ]; then
    echo "typed lints: rustc -vV names no commit or host" >&2
    exit 1
fi
toolchain=$(rustup show active-toolchain | cut -d' ' -f1)
rustc_dev="rustc-dev-$host"
clippy_commit=$(cat clippy/.rust-commit 2>/dev/null || true)
has_rustc_dev() {
    rustup component list --installed --toolchain "$toolchain" | grep -qx "$rustc_dev"
}
