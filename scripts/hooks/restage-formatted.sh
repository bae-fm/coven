# Sourced by pre-commit. Bash 3.2 compatible (macOS), so parallel arrays.
#
# snapshot_staged_files runs before the formatter: for each staged file it
# records the working-tree hash and whether the file has unstaged edits.
# restage_and_fail_if_formatted runs after: a staged file whose hash changed
# was reformatted. One with no unstaged edits beforehand gets the formatter's
# change staged; one with unstaged edits is left alone, because staging it
# would commit those edits too.

SNAP_FILES=()
SNAP_HASHES=()
SNAP_DIRTY=()

_worktree_hash() {
    git hash-object -- "$1" 2>/dev/null || echo missing
}

snapshot_staged_files() {
    SNAP_FILES=()
    SNAP_HASHES=()
    SNAP_DIRTY=()
    local file
    while IFS= read -r -d '' file; do
        SNAP_FILES+=("$file")
        SNAP_HASHES+=("$(_worktree_hash "$file")")
        if git diff --quiet -- "$file"; then SNAP_DIRTY+=(0); else SNAP_DIRTY+=(1); fi
    done < <(git diff --cached --name-only -z --diff-filter=ACMR)
}

restage_and_fail_if_formatted() {
    local tool_name="$1" i file restaged=() by_hand=()
    for i in "${!SNAP_FILES[@]}"; do
        file="${SNAP_FILES[$i]}"
        [ "$(_worktree_hash "$file")" = "${SNAP_HASHES[$i]}" ] && continue
        if [ "${SNAP_DIRTY[$i]}" -eq 0 ]; then
            git add -- "$file"
            restaged+=("$file")
        else
            by_hand+=("$file")
        fi
    done
    [ "${#restaged[@]}" -eq 0 ] && [ "${#by_hand[@]}" -eq 0 ] && return 0
    echo ""
    echo "$tool_name reformatted staged files."
    if [ "${#restaged[@]}" -gt 0 ]; then
        echo "   Re-staged:"
        printf '     %s\n' "${restaged[@]}"
    fi
    if [ "${#by_hand[@]}" -gt 0 ]; then
        echo "   Not staged, because they also have unstaged edits; stage $tool_name's change by hand:"
        printf '     %s\n' "${by_hand[@]}"
    fi
    echo "   Review the diff and commit again."
    return 1
}
