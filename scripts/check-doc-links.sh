#!/usr/bin/env sh
# Every `rustdoc:` link in site/docs resolves to a page `cargo doc` actually
# generated. The site rewrites these specs into `/rustdoc/...` URLs at build
# time without checking them, so a moved, renamed, or narrowed item turns a
# link into a 404 that nothing else notices.
#
# Reads the output of `cargo doc --workspace --no-deps --all-features`, which
# must have run first.
set -eu

cd "$(dirname "$0")/.."

doc_dir="$(cargo metadata --format-version 1 --no-deps |
    sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')/doc"

if [ ! -d "$doc_dir" ]; then
    printf '%s\n' "no rustdoc output at $doc_dir; run cargo doc first" >&2
    exit 1
fi

hits=$(mktemp)
failures=$(mktemp)
trap 'rm -f "$hits" "$failures"' EXIT

grep -rHno '(rustdoc:[^)]*)' site/docs --include='*.md' >"$hits" || true

while IFS= read -r hit; do
    file=${hit%%:*}
    rest=${hit#*:}
    line=${rest%%:*}
    spec=${rest#*:}
    spec=${spec#\(rustdoc:}
    spec=${spec%\)}

    kind=${spec%%:*}
    path=${spec#*:}
    crate=${path%%::*}
    tail=${path#*::}
    [ "$tail" = "$path" ] && tail=''

    anchor=''
    prefix=''
    item=''

    case $kind in
    mod) ;;
    method | variant)
        member=${tail##*::}
        owner=${tail%::*}
        item=${owner##*::}
        tail=${owner%::*}
        [ "$tail" = "$owner" ] && tail=''
        if [ "$kind" = method ]; then
            prefix=struct
            anchor="method.$member"
        else
            prefix=enum
            anchor="variant.$member"
        fi
        ;;
    const | enum | fn | struct | trait | type)
        item=${tail##*::}
        owner=$tail
        tail=${owner%::*}
        [ "$tail" = "$owner" ] && tail=''
        case $kind in
        const) prefix=constant ;;
        *) prefix=$kind ;;
        esac
        ;;
    *)
        printf '%s:%s: unknown rustdoc link kind %s\n' "$file" "$line" "$kind" >>"$failures"
        continue
        ;;
    esac

    mods=$(printf '%s' "$tail" | sed 's|::|/|g')
    [ -n "$mods" ] && mods="$mods/"

    if [ "$kind" = mod ]; then
        target="$doc_dir/$crate/${mods}index.html"
    else
        target="$doc_dir/$crate/${mods}${prefix}.${item}.html"
    fi

    if [ ! -f "$target" ]; then
        printf '%s:%s: rustdoc:%s names no generated page\n' "$file" "$line" "$spec" >>"$failures"
    elif [ -n "$anchor" ] && ! grep -q "id=\"$anchor\"" "$target"; then
        printf '%s:%s: rustdoc:%s has no #%s on its page\n' "$file" "$line" "$spec" "$anchor" >>"$failures"
    fi
done <"$hits"

if [ -s "$failures" ]; then
    cat "$failures" >&2
    printf '%s\n' 'site rustdoc links must name a page cargo doc generates' >&2
    exit 1
fi
