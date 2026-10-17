# Old spec sections → new spec sections

How every section of the old `spec/coven.md` (origin/main before the swap) and `spec/sync-pass.md`
maps onto the rewritten `spec/coven.md`, by content. Where an old section's rules went to several
places, each row names what moved where; references were renumbered by reading the citing sentence
and choosing the part that holds the cited rule. `spec/format.md` keeps its D1–D14 numbers and
anchors; `spec/api.md` keeps E1–E13.

## coven.md

| Old | Old title | New |
|---|---|---|
| §1 | Overview | §1 The problem; encryption bullets → §2.3, §12 |
| §2 | Threat model | §3 What coven assumes |
| §3 | Guarantees | §2 What you can rely on: local writes §2.1, convergence and atomicity §2.2, confidentiality §2.3, authorization §2.4, revocation §2.5, durability §2.6, storage bounds §2.7, visible failures §2.8, proofs §2.9 |
| §3.1 | IO bounds | §21 Costs: request counts §21.1, other bounds (idle work, custody, writer never held, retry delays) §21.2, tests §21.3 |
| §4 | Storage providers and access | §20 and Appendix E. Requirements (create once, conditional replace, reads, status, listing, delete, sharing) §20.1; providers, least privilege, Dropbox access levels, sign-in and tokens, CloudKit summary §20.2; paths and where a store lives §20.3; listings and the margin M §20.4; status, times, revisions, checksums §20.5 (per-provider table E.11); large-object publication, temporaries, copy limits §20.6; S3 settings and profiles, member-key scope, own bucket and `coven-bucket`, detection, versioned buckets, certification, clock skew §20.7; setup safety check and provider check §20.8; sharing with accounts §20.9; S3 member keys §20.10. Reference detail: CloudKit E.1, Dropbox/OneDrive E.2, S3 settings/hosts/validation E.3, certified profiles E.4, certification history E.5, running the suite E.6, detection probes E.7, safety-check `Behavior` cases E.8, versioning E.9, S3 publication mechanics E.10, times/revisions/checksums E.11, minters/policy/naming/listing/member-side check E.12 |
| §5 | Local database | §4 Writing locally; coven's tables → Appendix C.1 |
| §6 | Syncing writes | §5 The sequence (slots, claims, applying in order); writes-slot rules (one per pass, no size cap, 1 GB value limit, fixed format and keys, leaving the queue) §6.3; `read_through` and causality → §7.1; parts outside a member's audiences → §5.4, §15.6 |
| §6 "Stuck devices" (`#stuck-devices`) | | §5.4 A stuck device stops |
| §6 "Settling a claim" (`#settling-a-claim`) | | §5.1 Claiming a slot; floor check §5.2; settling after the slot is gone §5.3 |
| §7 | Order | §7 |
| §7.1 | Causality | §7.1 |
| §7.2 | Timestamps | §7.2 |
| §7.3 | Example | §7.3 |
| §8 | Merge | §8 Merging. Cells §8.1; lost values §8.2; removal rules, taking rows out, dismissing, inserting the same key, rule order §8.4; dependence by data (dropped sources) §9.5; internal tables Appendix B.1; note 42 tables B.2; lost-value record B.3; constraint naming B.5 |
| §8.1 | Example | §8.1 Cells (the example); internal-table view B.2 |
| §8.2 | Concurrent writes to one row | §8.1 Cells (overlap rule); lost-value table §8.2 |
| §8.3 | Deletes | §8.3 |
| §8.4 | Foreign keys | §9.1 |
| §8.5 | Keys and uniqueness (`#unique-constraints`) | §9.2 Keys and unique values |
| §8.6 | CHECK constraints | §9.3 |
| §8.7 | Triggers | §9.4 |
| §8.8 | Owner tables | §9.6 |
| §9 | Members and roles | §10 Members, roles and devices (10.1–10.4); "every admin action uses §18" → §11 |
| §10 | Device identity | split: device id defined §10; another copy, custody, `DeviceIdInUse`, backups, occupied immutable paths, `reset_device` and the reset notice §19.3; removal/key-replacement slot cut-off, `WritesRefused`, `MemberKeysReplaced`, sending by slot order §14.1; self-replacement pause §12.3; restoring makes a fresh device §13.1; bootstrap send rules §13.4; content-bound nonces Appendix D.4 |
| §11 | Keys | §12 Keys and encryption: key copies and introductions §12.1; exposure, choosing a key, rotation, late copies §12.2; replacing a member's keys §12.3; custody and app secrets §12.4 (mechanics D.6) |
| §11.1 | Cryptography | Appendix D (member keys D.1, algorithms D.2, what authentication binds D.3, nonces D.4, routing D.5) |
| §12 | Joining and restore | §13 Joining and restoring; installation edge cases §13.4 |
| §12.1 | A person's new device | §13.1 |
| §12.2 | Adding a person | §13.2 |
| §12.3 | Losing everything | §13.3 |
| §13 | Removing members and devices | §14: cut-off §14.1; provider access, `AccessRemains`, `StoreOwner` §14.2; S3 key replacement, `DeleteAccessKey`, reclaim of unrecorded keys §14.3 |
| §14 | Audiences | §15 Circles |
| §14.1 | Roots and descendants | §15.1 Declaring audiences; unique values across audiences §15.2 |
| §14.2 | Moving rows | §15.4 |
| §14.3 | Circles | §15.5 Running a circle |
| §14.4 | Writes | §15.6 Writes into circles; reload on joining a circle §15.7; one signature over all parts → Appendix D.3 |
| §14.5 | References | §15.3 |
| §14.6 | Leaving a circle | §15.8 |
| §14.7 | Deleting a circle | §15.9 |
| §15 | Snapshots | §16: what a snapshot holds §16.1; taking and announcing §16.2; raise/reset preparations §16.3 |
| §15 "The floor and reloading" | | §16.4 The floor; §16.5 Coming back after a long absence |
| §15 "Deletion" | | §16.6 Deleting history |
| §16 | Files | §17 |
| §16.1 | Kinds and where files are | §17.1 Two kinds of file; attaching, fixed reference, four columns, key in the row §17.2; status and read agreement §17.4 |
| §16.2 | Storage and naming | §17.3 |
| §16.3 | Reading ranges | §17.5 |
| §16.4 | Cache | §17.6 |
| §16.5 | Uploads and deletion | §17.7 Uploads; §17.8 Deleting stored files; shared retry delays and persisted deadlines §6.7 |
| §16.6 | What a device keeps about files | §17.9; owned-bytes crash rules Appendix C.3 |
| §17 | Schema changes | §18 |
| §17.1 | Host application | §18.1 classification; §18.2 additions; §18.3 declarations, running migrations, the raise, older apps; §18.4 migration write; §18.5 the migrating device's queue; §18.6 late old-shape writes; §18.7 lost values across a migration; §18.8 two devices raising |
| §17.2 | Coven's schema | §18.9; tables Appendix C.1; storage format versions Appendix C.2 |
| §18 | Operations | §11 Admin operations |
| §18.1 | Operations | §11.5 How each operation prepares |
| §18.2 | Example | §11.1 (Ben's rotation example) |
| §19 | Recovery | §19 When things go wrong |
| §19.1 | Noticing | §19.1 What the app sees |
| §19.2 | Recovering one device | reload §19.2; damaged database reset §19.3 |
| §19.3 | Resetting a store | §19.4 Resetting an audience |
| §20 | Crates and conventions | Appendix F |
| §20.1 | Crates | F.1 |
| §20.2 | Capabilities, owners and lifetimes | capabilities F.2; owners, tasks, composition roots F.3; opening, closing and sync F.4 |
| §20.3 | Code conventions | F.5 |
| §20.4 | Checks | F.6 |
| A.1 | Integer primary keys | A.1 |
| A.2 | Tables with no primary key | A.2 |
| A.3 | Unique constraints besides the primary key | A.4 |
| A.4 | Restrict and no-action foreign keys | A.5 |
| A.5 | CHECK constraints | A.6 |
| A.6 | Triggers that write synced tables | A.7 |
| A.7 | Schema changes | A.11 |
| A.8 | SET DEFAULT foreign keys | A.8 |
| A.9 | References to non-key columns | A.9 |
| A.10 | Collations on keys | A.10 |
| (new) | | A.3 Key columns that can be empty, and independent keys without a UUID column |
| "Appendix B" (proofs/merge.md) | Proof of convergence | `proofs/merge.md` (its B1–B20 unchanged); the new Appendix B is coven's merge records |
| "Appendix D" (format.md) | Storage format | `format.md` (D1–D14 unchanged); the new Appendix D is the cryptography reference |
| "Appendix E" (api.md) | API | `api.md` (E1–E13 unchanged); the new Appendix E is the provider reference |

## sync-pass.md

| Old | Old anchor | New |
|---|---|---|
| intro | | §6 A sync pass |
| 1. Read forward | `#requests-in-order` | §6.1 Read forward |
| 2. Apply in order | `#apply-in-order` | §6.2 Apply in order |
| 3. Claim | | §6.3 Claim |
| 4. Effects after applying | `#effects-after-applying` | §6.4 Effects |
| 5. Counting requests | `#counting-a-pass` | §21.1 Counting the requests of a pass |
| Object reads | `#one-stream-checked-as-it-arrives` | §6.5 Reading an object |
| 6. Files and waiting | | §6.6, §6.7 |
| File uploads outside the pass | `#file-uploads-outside-the-pass` | §6.6 Files travel outside the pass; upload rules §17.7 |
| Waiting without repeated work | `#waiting-without-repeated-work` | §6.7 Waiting and retrying |

Code comments cited sync-pass.md as "SP" ("SP waiting", "SP, Files and waiting", "SP step 5"); those
now cite §6.6, §6.7, §17.7 and §21.1.

## Anchors other files linked to

Every `coven.md#…` link outside the spec was rewritten to the anchor of its new section (GitHub
slugs: `#51-claiming-a-slot`, `#54-a-stuck-device-stops`, `#e6-running-the-certification-suite`, …).
format.md anchors linked from elsewhere (`#d7-snapshots`, `#d9-sealed-objects`, and those coven.md
uses) are unchanged; `#d8-small-frames` is kept as an explicit anchor.
