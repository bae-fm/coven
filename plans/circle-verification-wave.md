# Circles verification wave

## Status

Complete. The verification matrix passed against the finished implementation;
the findings were fixed before completion. Public documentation describes the
implemented protocol, and `~/dev/bae` pins and passes its full workspace suite
against the verified Coven revision.

## Part 1 — removed-architecture sweep

The removed-architecture searches from `plans/circles.md` were run across
production code, tests, fixtures, plans, and website documentation:

```sh
rg -i "serial|writepolicy|write_policy|coordination|conditional head|provisional branch" \
  crates site README.md plans
rg "StoreEngine|CycleEngine|AuthorizedCycleEngine|store_engine" crates
```

The remaining matches are explicit design prohibitions or unrelated uses such
as serialization. The stale implementation names listed by the original audit
are absent.

## Part 2 — verification-checklist audit

The schema and routing, Circle lifecycle, package and data, and
snapshot/bootstrap/acknowledgement/reclaim checklists were matched to their
tests. The audit covered:

- durable and remote failure boundaries for push and pull;
- forged, partial, duplicate, updated, and deleted routing metadata;
- column, delete/edit, foreign-key, and uniqueness conflicts in both arrival
  orders; and
- access-leaf replay across recipients, Stores, Circles, epochs, controls, and
  bootstraps.

## Part 3 — open-ledger closure

The control-resolution and member-re-add findings are covered by the completed
control and recovery paths. Accepted Store membership-grant revocation is an
implemented discard proof. Owner visibility in public control state is stated
as a protocol privacy property in the website documentation.

## Part 4 — public documentation (`site/docs/`)

The website documents Store/Circle/Local audiences, the Circle lifecycle,
effective membership, rotation requirements, the `coven.circles()` API,
`CircleState`, `CircleError`, offline and blocked writes, snapshots, restore,
reclamation, and the protocol's privacy limits. It contains no
coordinated-protocol description.

## Part 5 — `~/dev/bae`

`~/dev/bae` pins the verified Coven revision. Its full workspace suite passes
with all features, including runtime, remote-storage, playback, CPU,
integration, and documentation tests.
