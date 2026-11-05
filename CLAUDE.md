Edits to `spec/` follow [spec/WRITING.md](spec/WRITING.md); `scripts/check-spec.py` checks the mechanical parts.

The spec moves with every commit: `spec/` states what coven does. Every commit checks whether it changes, or adds, anything the spec states or should state — behavior, a format, an API, a sync rule — and edits the spec in that same commit. A commit without its spec edit is unfinished: never leave the spec for a follow-up.

A change to how coven syncs, settles, merges or keeps its keys goes Lean, then spec, then code: the model in `spec/proofs/` states and proves it first, the spec cites the theorems (Appendix G), and the code follows with a test, and a differential against the model where one exists. A bug the code shows that the model allowed or doesn't cover is a gap in the model: close it in Lean before the fix.

Build and test coven with one feature set, `--all-features`, as the hook, clippy and CI do: `scripts/cargo.sh test -p <crate> --all-features <filter>`, `scripts/cargo.sh clippy -p <crate> --all-targets --all-features`. Each other feature set (`--features test-utils`, none) is a second copy of every crate's build in the checkout. Use one only to check that feature set itself, as `scripts/check.sh` does.
