# Prior art

Other projects that sync an app's data between devices, measured against what
coven requires, and the case for running a server instead. Section numbers
point into [spec/coven.md](../spec/coven.md). Project status gives each
project's latest release or commit as its repository showed it when surveyed.

## What coven requires

| # | Requirement | Where |
|---|---|---|
| R1 | The app's data is SQLite. The app keeps its own schema and writes ordinary SQL; coven captures each committed change. | §1, §4 |
| R2 | Synced tables keep real constraints: foreign keys with their actions, unique values, CHECK constraints, triggers. After a merge every device shows only rows that satisfy them; rows that lose are taken out and recorded, never dropped unsaid. | §8.4, §9 |
| R3 | A proven merge: every device that applied the same writes holds the same database, whatever order they arrived in, and a concurrently replaced value is kept as a lost value. The merge and the sequence are modelled and proved in Lean. | §2, §7, §8, Appendix G |
| R4 | End-to-end encryption: the provider sees only encrypted bytes and a few routing facts, never names or values. | §2, §3, §12 |
| R5 | Integrity: every slot is signed by its member; no member can write as another, and nobody without the store key can forge or alter data. | §2, §10 |
| R6 | Members and roles: several admins, several devices per member, membership changes as admin entries every device judges the same way. | §10, §11 |
| R7 | Circles and removal: groups inside a store with keys of their own; after a removal, new data uses a key the removed person never held. | §12.2, §14, §15 |
| R8 | Breaking schema changes: renaming, retyping or dropping columns and tables, with late writes in the old shape converted or kept, alongside devices still on older versions. | §18 |
| R9 | No server. Sync goes through storage the members already have (S3, Dropbox, OneDrive, iCloud through CloudKit), used only to create, read, list and delete objects. | §1, §20 |
| R10 | Local writes never wait on the network. | §2 |
| R11 | Files attached to rows (audio, images) sync with them, encrypted, readable by byte range. | §17 |
| R12 | Committed work survives crashes; each write applies in one transaction; operations with several steps resume and finish. | §2, §19 |
| R13 | Bounded storage: old history is replaced by snapshots and deleted. | §16 |
| R14 | Joining and restoring with a restore code, with no account system of coven's own. | §13 |
| R15 | A Rust library: bae's core is Rust called from Swift and Kotlin, so a JavaScript-only library doesn't fit. | |

## Sync engines and databases

"Meets" and "lacks" use the R numbers above. "For bae" says whether it could
replace coven as it is or with a thin layer on top.

| Project | Status | Meets | Lacks | For bae |
|---|---|---|---|---|
| [cr-sqlite](https://github.com/vlcn-io/cr-sqlite) (vlcn) | Last release v0.16.3, 2024-01-17; commits in August 2026 are build fixes only. | R1 (a SQLite extension; per-column last-writer-wins and a counter for deletes and re-adds), R10. It only merges change sets, so any transport works, plain storage included (R9). | R2: [forbids](https://vlcn.io/docs/cr-sqlite/constraints) checked foreign keys, unique constraints other than the primary key, and CHECKs across columns. R3: no published proof. R4–R7, R11–R14: none; it is a merge function, not a sync system. R8: column additions only. | No. It would be the merge under everything else coven does, and it drops the constraints bae's schema relies on. |
| [Evolu](https://www.evolu.dev) | Evolu 8 released [2026-09-01](https://github.com/evoluhq/evolu). | R1 (SQLite, queried through Kysely), R4 (end-to-end encrypted by default), R10, part of R6 (an "owner" can be shared). | R2: [rejects user-defined unique indexes](https://www.evolu.dev/docs/schema); no foreign-key or CHECK handling across merges. R3: per-column last-writer-wins by timestamp, no proof. R5–R7: a shared owner is one secret; the write token can be rotated at the relay (`Owner.ts`), but there are no per-member signatures and no new key on removal. R8: forbidden; the schema is append-only, and unknown columns are set aside. R9: needs [Evolu Relay](https://www.evolu.dev/docs/relay), a server that stores the encrypted messages. R15: TypeScript only. | No. The nearest in spirit (SQLite, encrypted, private), but it lacks R2, R5–R9 and R15. |
| [ElectricSQL](https://electric-sql.com) | [1.0, 2025-03-17](https://electric-sql.com/blog/2025/03/17/electricsql-1.0-released). In [July 2024](https://electric-sql.com/blog/2024/07/17/electric-next) it dropped its two-way SQLite sync with constraint-keeping CRDTs and rebuilt as a read-only sync out of Postgres. | R10 for reads. | Writes go through your own API to Postgres. No R4–R9. | No. |
| [PowerSync](https://www.powersync.com) | Service [v1.27.0, 2026-10-06](https://github.com/powersync-ja/powersync-service); commercial, with a self-hostable edition. | R1 (SQLite on the client), R10, R15 (native Swift, Kotlin and other SDKs). R2 holds in the sense that the server database enforces constraints and rejects bad writes. | Needs a backend database (Postgres, MongoDB, MySQL, SQL Server), the PowerSync service, and your API for writes. No R4 (the service reads rows to decide who gets them), no R5–R7, R9. | No: it needs a server bae would run, and it can't be end-to-end encrypted. |
| [Replicache](https://replicache.dev) / [Zero](https://zero.rocicorp.dev) (Rocicorp) | Replicache in maintenance mode. Zero 1.0, [June 2026](https://www.infoq.com/news/2026/06/zero-version-1/). | R10. Constraints hold because mutations run again on the server against Postgres. | The server decides: Postgres plus `zero-cache`. Web and React Native. No R4–R9, R15. | No. |
| [Triplit](https://github.com/aspen-cloud/triplit) | The company ended; a co-founder [joined Supabase on 2025-10-08](https://supabase.com/blog/triplit-joins-supabase). Repository last pushed 2026-01-19. | R10. | The server decides; TypeScript, its own query model. No R1, R4–R9, R15. | No. |
| [Instant](https://github.com/instantdb/instant) | Open source; commits in September 2026. | R10 (local cache, offline reads, optimistic writes). | A hosted or self-hosted server (Clojure, Postgres) enforces permissions; a triple store, not your SQLite schema. JavaScript, React Native and Python clients. No R1, R4–R9, R15. | No. |
| [LiveStore](https://github.com/livestorejs/livestore) | v0.4.0, 2026-06-02. | R1 in part (events are replayed into SQLite tables). Replaying events gives one way to change schemas (R8). R10. | The sync backend must give a [single total order of events](https://docs.livestore.dev/reference/syncing/); clients pull and rebase before they push. Encryption and permissions are listed as to do. TypeScript only. No R4–R7, R9, R15. | No. |
| [Jazz](https://github.com/garden-co/jazz) | "Classic Jazz" (JSON-like CoValues) is still documented; Jazz 2.0 is in alpha (2.0.0-alpha.59, 2026-10-04) with a new API. | Classic Jazz is the closest on R4–R7: signed transactions, groups with admin, writer and reader roles, a group read key issued again to the remaining members when someone is removed ([docs](https://classic.jazz.tools/docs/react/reference/encryption)). Jazz 2 is relational (like R1) with per-column merging. | Classic needs a sync server (Jazz Cloud or self-hosted) and isn't SQLite. Jazz 2 dropped that design: its spec says "One sync protocol; one authority. ... Core alone authorizes reads and writes and assigns final transaction fates" (`crates/jazz/SPEC/1_intro.md`, ch. 9); authorization is server-side row policies, not end-to-end encryption. Neither meets R2, R3, R8, R9. JavaScript first. | No. |
| CloudKit ([CKSyncEngine](https://developer.apple.com/videos/play/wwdc2023/10188/), CKShare) | Apple's; CKSyncEngine since 2023. | R9 in part (no server of yours), R10 with your own local store, sharing through CKShare (part of R6), encrypted fields (`encryptedValues`). | Apple only, so no Android. Apple's server orders writes and rejects stale saves; the app writes its own merge; no relational constraints. Encrypted fields use keys in the user's iCloud Keychain, but record names, metadata and other fields are visible to Apple. No R2, R3, R5, R7, R8. | No. Coven uses it as one of its storage providers. |
| [Ditto](https://docs.ditto.live) | SDK v5; commercial. | R10; device-to-device sync over Bluetooth and local Wi-Fi. | Documents with a CRDT per field, not SQLite. Internet sync goes through Ditto Server (hosted, on-premises or in your cloud). No R1, R2, R3, R8, R9; R4–R7 not checked. | No. |
| [Couchbase Lite](https://docs.couchbase.com/couchbase-lite/3.3/cbl-whatsnew.html) | 3.3, October 2025, added device-to-device sync with no gateway (Enterprise Edition). | R10; access control through Sync Gateway channels. | JSON documents with revision-tree conflict handling, not SQLite. Internet sync needs Sync Gateway and Couchbase Server; no end-to-end encryption. No R2–R5, R7–R9. | No. |
| [Automerge](https://automerge.org) | [3.0, July 2025](https://automerge.org/blog/automerge-3/); JS 3.5.0, 2026-09-16. Rust core. | R3 in part (the algorithms come from peer-reviewed papers), R10, R15 (Rust core). | JSON-like documents, not SQLite tables; no relational constraints. Sync through `automerge-repo` peers or a sync server. Access control and encryption are research (Keyhive, below). No R1, R2, R4–R9. | No. bae's library is relational tables with joins and constraints. |
| [Yjs](https://github.com/yjs/yjs) | 13.6.33, 2026-09-23; 14 in release candidates. | R10. | Shared types for collaborative editing (text, lists, maps), JavaScript. Transport through providers (WebSocket, WebRTC). No encryption or permissions built in. No R1–R9, R15. | No. |
| [Graft](https://github.com/orbitinghail/graft) | v0.2.1, 2025-12-04; its README says alpha, and to contact the author before production use. | R1 (a SQLite extension that replicates pages), lazy partial replication through object storage. | Serializable isolation over pages: it keeps one order of commits instead of merging concurrent offline writes. The README lists no encryption, signatures or membership. No R2 as a merge, R3–R8. | No. |
| [sqlite-sync](https://github.com/sqliteai/sqlite-sync) (SQLite AI) | 1.2.0, 2026-09-28. | R1, R10 (CRDT merge inside SQLite). | Syncs through SQLite Cloud, Postgres or Supabase servers. No R4–R9. | No. |
| [Actual Budget](https://jlongster.com/using-crdts-in-the-wild) | An app, not a library. | R1 (SQLite), per-cell last-writer-wins with hybrid logical clocks, optional end-to-end encryption (R4). | Needs `actual-server` to relay changes. No R2, R3, R5–R9. | No; it shows the per-cell approach in production. |
| [any-sync](https://github.com/anyproto/any-sync) (Anytype) | Commits in October 2026. | R4, R5 (signed changes), R6–R7 in part (access lists). | Needs sync nodes, file nodes and a [consensus node](https://github.com/anyproto/any-sync-consensusnode) "responsible for ACL changes monitoring and validation". Its README: the CRDT layer is "paired with a decentralized, strictly ordered list". Objects, not SQLite; Go. No R1–R3, R8, R9. | No. A team doing encrypted collaborative sync chose a service that orders membership changes. |

## Apps that sync encrypted data over consumer storage

These show that coven's transport (each device writes encrypted objects into
storage the user already has, and every device merges) has worked for years,
for one person's devices:

- [Joplin](https://github.com/laurent22/joplin): notes, end-to-end encrypted,
  over Dropbox, OneDrive, S3, WebDAV. Concurrent edits go to a "Conflicts"
  notebook for the user. Users report slow syncs on OneDrive and with large
  libraries ([ctrl.blog](https://www.ctrl.blog/entry/joplin-notes-sync/)).
- [OmniFocus](https://discourse.omnigroup.com/t/omnifocus-sync-encryption-gory-technical-details/24611):
  a directory of encrypted transaction files on any WebDAV server, read and
  written by clients that don't otherwise talk to each other.
- [Moneydance](https://infinitekind.tenderapp.com/kb/syncing-and-sharing-data/syncing-with-other-computers):
  encrypted incremental sync through Dropbox since 2017.
- Nikita Prokopov, ["Local, first, forever"](https://tonsky.me/blog/crdt-filesync/)
  (2024-06-24): one file per device on Dropbox and CRDT merging, so sync
  outlives the app's company. It doesn't address encryption, sharing or
  permissions.

None is a library, none has members with removal, and none keeps relational
constraints.

Relying on a vendor's sync service carries its own risk: MongoDB
[deprecated Atlas Device Sync](https://www.mongodb.com/docs/realm/sync/)
(Realm) in September 2024 and shut it down on 2025-09-30.

## Research

- Ink & Switch, ["Local-first software"](https://www.inkandswitch.com/local-first/)
  (Kleppmann et al., Onward! 2019): the ideals coven follows (works offline,
  the user owns the data, it outlives the vendor).
- Ink & Switch [Keyhive](https://github.com/inkandswitch/keyhive): access
  control and end-to-end encryption for Automerge, pre-alpha ("DO NOT use this
  release in production"; `keyhive-wasm` 0.3.0-alpha.1, 2026-09-25). Group keys
  come from BeeKEM ([ePrint 2026/1434](https://eprint.iacr.org/2026/1434)), a
  version of TreeKEM that tolerates concurrent changes. Sync runs over
  [Subduction](https://github.com/inkandswitch/subduction), a protocol between
  running peers over WebSocket, HTTP or QUIC, not plain storage. It addresses
  R4–R7 for JSON documents, not R1, R2, R8 or R9.
  [Notebook 06](https://www.inkandswitch.com/keyhive/notebook/06/) states the
  central problem: "If Alice adds Carol to the group while Bob simultaneously
  removes Alice from the group, the order in which the central server
  processes these messages affects the outcome." Without that server they rely
  on causal order, and every member keeps several old secrets, which gives up
  forward secrecy.
- Kleppmann, [Making CRDTs Byzantine fault tolerant](https://martin.kleppmann.com/papers/bft-crdt-papoc22.pdf)
  (PaPoC 2022), and Kleppmann & Howard,
  [Byzantine eventual consistency and the fundamental limits of peer-to-peer databases](https://martin.kleppmann.com/2020/12/01/byzantine-eventual.html)
  (2020): how to converge when some members misbehave, and which invariants
  can't be kept without coordination, uniqueness among them. Coven assumes
  members aren't hostile (§3), so it doesn't need the first; the second is the
  theory behind coven taking unique-value losers out after the merge.
- Weidner, Kleppmann, Hugenroth, Beresford,
  [Key agreement for decentralized secure group messaging](https://martin.kleppmann.com/2021/11/17/decentralized-key-agreement.html)
  (CCS 2021): group keys with forward secrecy and recovery after compromise,
  with no server. Coven's design is coarser (a new key sealed to each
  remaining member), which fits a household.
- Kleppmann et al., [Kintsugi](https://martin.kleppmann.com/2025/03/26/kintsugi-spw.html)
  (2025): recovering end-to-end encryption keys without a server; the problem
  of a member who loses every device and their restore code (§13).
- Gomes, Kleppmann et al., [Verifying strong eventual consistency in distributed systems](https://martin.kleppmann.com/2017/10/25/verifying-crdt-isabelle.html)
  (OOPSLA 2017): machine-checked convergence proofs for CRDTs in Isabelle, the
  same kind of work as coven's Lean proofs.
- Bailis et al., [Coordination avoidance in database systems](https://arxiv.org/abs/1402.2237)
  (VLDB 2015): which constraints can be kept without coordination. Inserting a
  chosen value under a unique constraint can't; inserting rows that point at a
  parent through a foreign key can. Coven's removal rules keep the constraint
  true on what the app sees by taking losers out after the merge.

## What comes closest

- **On SQLite and merging:** cr-sqlite (no constraints, no recent releases, no
  encryption or members) and Evolu (no unique constraints, no breaking
  changes, a shared secret instead of members, needs a relay, TypeScript).
- **On encryption and members:** Classic Jazz (needs a server, not SQLite,
  replaced by a Jazz 2 whose server decides) and Keyhive (pre-alpha, JSON
  documents, needs running peers).
- **On transport through the user's own storage:** Joplin, OmniFocus,
  Moneydance (one person, one app each, no constraints).

## The case for a server

The strongest arguments that sync over consumer storage is the wrong design,
and where coven stands on each:

1. **A server's main job is ordering.** With one place that accepts writes in
   turn, concurrent membership changes, resets, breaking schema changes and
   deleting history are ordinary steps in sequence. Others who faced this chose
   ordering: any-sync runs a consensus node for access changes; Keyhive's
   notebook names the missing server as the source of its hardest trade-off;
   Matrix, which settles room membership without a central server, spent years
   on "state resets" and shipped new resolution rules in
   [room version 12 (Matrix 1.16, 2025-09-17)](https://matrix.org/blog/2025/09/17/matrix-v1.16-release/).
   Coven gets one order without a server: every slot of the store's sequence
   is claimed by creating its path only if it is empty, so the provider's
   create-once picks exactly one winner (§5, §20.1). That makes create-once
   under contention the one guarantee everything rests on, which is why every
   provider is checked at setup and each S3 service coven knows by name is
   certified against racing writers (§20.7, §20.8). Offline writes are still concurrent, and are merged by their
   timestamps and causes, not by the slot they land in (§7).
2. **Constraints can't be enforced when a write is made.** Without
   coordination a unique value can't be checked against writes nobody has seen
   yet (Bailis 2015). Coven keeps constraints true by taking losing rows out
   later and recording them (§8.4). That converges, but a row a person saw saved
   can be taken out an hour later. A server refuses it at commit, while the
   person is still there.
3. **Device clocks decide.** Last-writer-wins needs timestamps, and there is
   no trusted clock. A device never stamps a write earlier than anything it has
   seen, so a slow clock loses nothing, but one clock a year ahead drags every
   device's timestamps forward until real time catches up (§7.2).
4. **Nobody validates.** Every device checks everything itself, and any member
   with access to the storage can delete the store (§3). With end-to-end
   encryption a server couldn't check content either, so what is missing is
   ordering and refusal by metadata (who, which path, which version), not
   content checks.
5. **Consumer storage is a poor base for a database.** There are no push
   notifications, so devices poll; listings are slow on some providers
   (Joplin's OneDrive reports); each provider keeps its own rules, such as
   whether an S3 service honours create-once at all (§20.7); and each has
   quotas, rate limits and app review (§20.2).
6. **The field moved toward servers.** Electric dropped two-way SQLite merging
   with constraints (2024); Jazz 2 moved from end-to-end-encrypted groups to
   "one authority"; Zero, PowerSync, Instant and LiveStore all put a server in
   charge of order.

What the case leaves open: for bae the alternative to consumer storage is a
relay that bae, or each household, runs and pays for for the life of the app.
A relay that only stores encrypted bytes (Evolu's, Actual's) orders nothing;
only one that orders writes answers the points above. The group is a
household that trusts itself (§3), it already keeps its audio files in that
storage, and sync that outlives bae's maintainer is the point Prokopov makes.
