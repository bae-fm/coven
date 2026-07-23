# NOTFOUND→OMIT ordering prerequisite for Circles

## Status

Implemented by the dependency-ready Store commit protocol in
`plans/store-commit-protocol.md`. Circles must preserve this invariant when one
Store commit contains several audience packages.

## Failure that required the prerequisite

Three devices can produce this accepted history:

1. Device A inserts row R.
1. Device B materializes A's insert and edits R.
1. Device C discovers B's UPDATE before A's INSERT.

Applying B first makes SQLite report `SQLITE_CHANGESET_NOTFOUND`. Treating that
as a local delete and omitting the UPDATE permanently loses B's edit. Applying A
first keeps the edit, so two devices with the same accepted history can diverge.

The absent subject row alone cannot distinguish:

- its creation has not arrived; from
- an accepted concurrent delete already removed it.

Storage listing order, package order, foreign-key parent order, and audience
routing cannot prove which case occurred.

## Implemented invariant

Every Store commit signs:

- its exact same-stream predecessor; and
- the exact greatest materialized commit in every observed peer stream.

A receiver applies a commit only after all those exact dependencies are
materialized. Therefore:

- B's edit cannot apply before A's observed insert;
- a missing dependency holds the complete commit without row or frontier
  advancement;
- a permanently impossible dependency requires verified exclusion, revocation,
  abandonment, retraction, or retained-history evidence; and
- UPDATE `NOTFOUND` after readiness denotes a winning accepted delete, so
  delete-wins may omit the edit.

The complete package application and frontier advance remain one SQLite
transaction. No later cycle repairs an omitted edit.

## Circles requirement

Audience routing filters only a dependency-ready accepted commit. It never
replaces commit readiness.

For one `StoreBatchCommit` containing Store and Circle packages:

1. verify the activating Store head and complete dependency frontier;
1. exact-load every authority and package required for this recipient;
1. apply the Store routing mirror;
1. authenticate private route rows;
1. select eligible audience rows;
1. apply rows, pruning, blobs, and controls; and
1. validate the final component and advance the Store frontier atomically.

A missing private route, audience package, subject row dependency, foreign-key
parent, blob, or authority object aborts the batch. It is never converted into
an audience omission merely because another object has not arrived.

## Verification

- Apply an INSERT and its causally dependent UPDATE in both discovery orders;
  both paths produce the edited row.
- Apply a ready UPDATE after a concurrent accepted DELETE; both arrival orders
  produce deletion.
- Withhold an exact dependency; the dependent commit remains pending and the
  frontier does not advance.
- Prove the dependency permanently unreachable; the typed failure or retraction
  path settles it without guessing from absence.
- Repeat the cases with Store-only, Circle-only, and mixed-audience packages.
- Fault every package, route, blob, SQLite, and frontier boundary; each failure
  leaves the prior complete state.
