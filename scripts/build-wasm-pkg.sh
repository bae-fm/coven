#!/usr/bin/env sh
# Build the browser package that hosts import.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
crate_dir="$repo_root/crates/coven-wasm"
out_dir="$repo_root/pkg"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/coven-wasm-pkg.XXXXXX")
write_output=true

case "${1:-}" in
    "")
        ;;
    --check)
        write_output=false
        ;;
    *)
        echo "usage: $0 [--check]" >&2
        exit 2
        ;;
esac

cleanup() {
    rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

. "$repo_root/scripts/wasm-toolchain-env.sh"

wasm-pack build "$crate_dir" --target web --out-dir "$tmp_dir"

test -f "$tmp_dir/coven_wasm.js"
test -f "$tmp_dir/coven_wasm.d.ts"
test -f "$tmp_dir/coven_wasm_bg.wasm"
grep -q 'static open(config: any, migrations: any, synced_tables: any): Promise<CovenLibrary>;' \
    "$tmp_dir/coven_wasm.d.ts"
grep -q 'export function stamp(device_id: string): string;' "$tmp_dir/coven_wasm.d.ts"

if [ "$write_output" = true ]; then
    rm -rf "$out_dir"
    mkdir -p "$out_dir"
    cp -R "$tmp_dir"/. "$out_dir"/
fi
