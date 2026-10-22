Edits to `spec/` follow [spec/WRITING.md](spec/WRITING.md); `scripts/check-spec.py` checks the mechanical parts.

The spec moves with every commit: `spec/` states what coven does. Every commit checks whether it changes, or adds, anything the spec states or should state — behavior, a format, an API, a sync rule — and edits the spec in that same commit. A commit without its spec edit is unfinished: never leave the spec for a follow-up.
