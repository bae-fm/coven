#!/usr/bin/env sh
set -eu

matches=$(
    find crates -type f -name '*.rs' \
        -exec grep -nHE 'pub[[:space:]]*\([[:space:]]*in([[:space:]]|::)' {} + ||
        true
)

if [ -n "$matches" ]; then
    printf '%s\n' "$matches"
    printf '%s\n' \
        'restricted-path visibility is forbidden; move the item under its owner and use private or pub(super) access' >&2
    exit 1
fi
