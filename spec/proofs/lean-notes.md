# Lean model boundaries

The current proof packages are [sequence](sequence.md), [merge](merge.md),
[queue](queue.md), [storage](storage.md) and [io](io.md). Each package's root
imports its retained modules; its `Axioms.lean` names results for the audit.
`scripts/check.sh` builds them and permits only Lean's `propext`,
`Classical.choice` and `Quot.sound`. It rejects `sorry`, `admit` and custom
axioms in the proof libraries.

The sequence package models the shared store sequence: ordered application,
claim settlement, device-copy detection, authority, cut-off, first-slot claims,
reader progress, snapshots, the floor, retention and operations. Its write-up
states each theorem's assumptions and the runner's JSON cases.

The merge package proves cell and generation convergence, removal-rule
agreement, per-change data dependence, owner permissions and value accounting.
It accepts sequence admission, creator identity and unique-claim ranks as
inputs. A timestamp choosing a cell is distinct from the slot order choosing
an owner or unique claimant.

The queue package preserves original queued records while deriving received
interpretations and schema losses. Its read and coverage sets are expanded
inputs to these proofs, not stored per-device frontiers. The sequence package
owns publication and reload scheduling.

Storage proofs assume the provider primitives they compose, and cryptographic
proofs assume their hash and signature properties. IO proofs assume truthful
read outcomes, complete successful listings and durable local
transactions. Neither those assumptions nor the binary byte format is proved
by Lean. SQLite capture and provider certification require their own tests.

A Rust differential calls a model runner with the same inputs as the real
Rust function and compares the result. The check script names each ignored
test exactly and rejects an absent test. Lean builds and Rust comparisons
are separate evidence: neither substitutes for the other or proves behavior
outside the stated model boundary.
