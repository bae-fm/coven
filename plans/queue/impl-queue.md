This queue began with the Rust audit at `c90cf2ea` against the spec at `d414449c`.
The snapshot tasks below incorporate §15's proved publication, deletion, common-point reload and per-audience retention rules, plus §17.1's upload-before-raise decision. Implementation follows `impl-common.md`; this spec update changes no Rust and adds no Rust/Lean differential runner.

**Notation.** Paths below are relative to `crates/`: `F` = `coven-format/src`, `K` = `coven-crypto/src`, `D` = `coven-database/src`, `S` = `coven-sync/src`, `P` = `coven-storage/src`, `C` = `coven/src`, `B` = `coven-foundation/src`, `M` = `coven-merge/src`. A listed implementation file includes its existing sibling `_tests.rs`; additional test files are named explicitly. `+` means a proposed new file. Tasks sharing a wave have disjoint planned edit sets; singleton waves reserve shared files such as `internal_schema.rs`.

**Decision references.** `A#` means the Decisions section of [audit.md](/Users/dima/.claude/jobs/73253d57/tmp/integrity/audit.md); `S1#` means [simplifications.md](/Users/dima/.claude/jobs/73253d57/tmp/integrity/simplifications.md); `S2#` means [simplifications-2.md](/Users/dima/.claude/jobs/73253d57/tmp/integrity/simplifications-2.md); `I#` means the final, revised Decisions in [io-audit.md](/Users/dima/.claude/jobs/73253d57/tmp/integrity/io-audit.md), while `IF#` means its findings; `E#` means [error-audit.md](/Users/dima/.claude/jobs/73253d57/tmp/integrity/error-audit.md); `O` means the final decision in [store-log-order.md](/Users/dima/.claude/jobs/73253d57/tmp/integrity/store-log-order.md); `Clock#` means [clock-sweep.md](/Users/dima/.claude/jobs/73253d57/tmp/integrity/clock-sweep.md). `§` refers to `coven.md`, `D` to format sections, `E` to API sections, and `SP` to `sync-pass.md`.

**Spec issues that gate implementation**

- **G1 resolved — Snapshot coverage.** §15 retains greatest covered-write count with smaller-path ties, but admits only snapshots landing by signed `T_c + one day`, requires completed catch-up and preparation-time coverage of the current selection, confirms every covered own upload before sending, and deletes only with mature historical and current all-part coverage. `SnapshotHistory.deleted_stays_covered`, `SnapshotWrites.snapshot_covers_published` and `CommonReload.committed_number` supply the proved invariants. Implement T94–T98 and the revised T69; no selection-order decision remains open.
- **G2 resolved — Check each write miss.** §15/SP require `T + elapsed < S + 29 days` at every use of a miss, counting sleep; equality takes snapshot discovery. §4 supplies the required clock-advance bound. IO `accepted_miss_is_recent`, `miss_before_retention` and `recent_return_complete` cover it; T43 implements it.
- **G3 resolved — Observe through the preceding provider unit.** §4/§9/SP use inclusive endpoint `T′ = T - δ` with a positive provider unit, so equal-time later publications are outside the finality observation. IO `old_immutable_present`, `through_time` and `Refinement.finality_precondition` provide the connection to the actual C10 horizon. T21 must consume that evidence; provider conformance remains G5.
- **G4 — Format-1 compatibility lacks a discovery and semantic transition rule.** §17.2 requires every older reader and byte-identical attempted retries. D10 moves paths, D9 removes creation’s special prefix, D6 removes key-bearing removals, and D11 changes nonce derivation. Specify how existing root-layout stores and their old removals are discovered/interpreted. Do not delete v1 readers or overwrite v1 fixtures.
- **G5 — Provider evidence needs conformance verification.** §4 requires first *complete-publication* time and revisions changing on every replacement. The existing `StoredObject` documentation explicitly says S3 multipart time does not establish publication age. The new retirement drain also assumes remote requests cannot publish after one day; a client timeout does not prove that. Time-dependent deletion and drain activation require those assumptions to be established.

The IO proofs also cover retired-write admission and the post-finality drain's timing-to-closure argument under their stated provider and duration hypotheses; G5 remains an implementation verification obligation.

**Differential-test notation**

- **SL:** existing `spec/proofs/storelog/StorelogRunner.lean`, executable `storelogRunner`, `COVEN_STORELOG_LEAN`, Rust `S/lib_tests.rs`. It currently calls historical `CovenStorelog.resolve`; T16 must change it to `CurrentReplay`.
- **MG:** existing `spec/proofs/merge/MergeRunner.lean`, `mergeRunner`, `COVEN_MERGE_LEAN`, Rust `M/lib_tests.rs`. Its current differential does not cover the new boundary/migration behavior.
- **SD, IO, ST, Q:** proposed runners, respectively `StorelogDataRunner.lean`, `IORunner.lean`, `StorageRunner.lean`, `QueueRunner.lean` in their proof packages, with corresponding Rust differential tests. **These runners do not currently exist.** Their introduction includes the package’s `lakefile.toml` and `scripts/check.sh`.
- Where “no differential” is stated, use Rust regression, wire-vector, provider-fake, or crash-injection tests. A symbolic Lean crypto model is not an oracle for encrypted bytes.

**Wave 01**

**T01 — Rename blocked-operation vocabulary to pending.**
Spec: E5–E6; note: S2#10a, blocked-sweep. Files: `S/{operation_types,operations,operation_calls,error,lib}.rs`; `C/{handle,lib}.rs`; `S/{schema_sync,operation_steps,operation_invites,snapshots}_tests.rs`; `C/{builder,bootstrap,handle,lib}_tests.rs`. Remove the old `BlockedOperation`, `NotBlocked`, and `*_blocked_operation` spellings through the existing operation surface. Tests: update existing API assertions and search all crates/docs/examples for stale names; no behavioral test addition or differential. Dependencies: none.

**Wave 02**

**T02 — Use one refusal vocabulary with optional native causes.**
Spec: §19.1, E5; note: S2 gap 1, E11–E12. Files: `S/{error,write_object,store_log_object,store_log_sync,snapshot_catalog,posted_positions,lib}.rs`; `C/lib.rs`. Remove `ObjectCheckFailure` and category conversions that erase semantic distinctions; introduce the specified `Refusal` variants and cause-independent equality. Tests: immediate errors retain causes; persisted-equivalent values compare by variant; authorization, causality, identity and content-hash failures remain distinct. Differential: none. Dependencies: T01.

**Wave 03**

**T03 — Preserve format versions through decoding and attempted retries.**
Spec: §17.2, D1/D9/D14; note: O’s fixed-attempt bytes, A7. Files: `F/{lib,wire,sealed,sealed_single,sealed_write,sealed_snapshot,codes,key,file}.rs`; `K/wire.rs`; `D/{internal_schema,upload,store_log_upload}.rs`; `S/{write_seal,store_log_object}.rs`; format fixtures and `F/test_utils.rs`. Replace single-version rejection and retries implicitly using the current encoder; retain v1 readers and vectors. Tests: old objects remain readable, newer versions request updates, reopening after an upgrade reproduces attempted bytes. Differential: none. Dependencies: G4 for legacy semantics.

**Wave 04**

**T04 — Implement D6’s key-free removals and committed introductions.**
Spec: §9/§11, D6; note: S2#1. Files: `F/store_log.rs`; current-format fixtures; `F/test_utils.rs`; affected constructors in `S/{effects,conflicts,store_log_keys,operation_steps,bootstrap}.rs`. Remove current-format `CircleKeyId`, removal `key`/`circle_keys` payloads and `WrongCircleKeys`; add creation commitments and tag-15 rotation. Tests: exact tag layouts, hash lengths, reserved tag 12, truncation and malformed payloads. Differential: `CurrentModel` through SL after T16. Dependencies: T03.

**Wave 05**

**T05 — Encode replacement registration without log ends.**
Spec: §10, D6 tag 4; note: S2#9. Files: `F/store_log.rs`, `F/test_utils.rs`, current-format fixtures; registration constructors in `S/{bootstrap,operation_steps}.rs`. Add `replaces: Option<DeviceId>`; introduce no counter/end fields. Remove old tag-4 layout assertions from current-format tests. Tests: absent/present replacement, identical old/new IDs rejected, old-format decoding preserved. Differential: recorded-read behavior belongs to T48, not the codec. Dependencies: T04.

**Wave 06**

**T06 — Replace D8 stuck reports with pending reports.**
Spec: §19.1, D8, E5; note: blocked-sweep, S2#10a/gap 1. Files: `F/{stuck,objects,lib,test_utils}.rs`, `F/+pending.rs`, D8 fixtures. Remove current-format `StuckRecord`, `StuckFailure`, and the write/entry-only report grammar. Encode the permitted subject/reason subset, deterministic ordering and duplicate rejection; local operation/provider/drop details must not leak into posts. Tests: every wire tag, forbidden local-only cases, report limits, canonical ordering. Differential: none. Dependencies: T02, T03.

**Wave 07**

**T07 — Give creation the ordinary sealed-entry envelope.**
Spec: D9, §4; note: S1#5. Files: `F/{sealed_single,sealed,test_utils}.rs`; `S/store_log_object.rs`; sealed-store-log fixtures. Remove creation’s cleartext race/origin prefix and its alternate signature/layout path from v2. Tests: uniform 19-byte prefix, creation authenticated through its plaintext, wrong path/key/signature rejection; preserve v1 decoding. Differential: none. Dependencies: T04.

**Wave 08**

**T08 — Bind numbered key-copy routing into its sealed box.**
Spec: §11, D9/D11; note: I12/I14. Files: `F/{key,sealed_single,sealed,test_utils}.rs`; `K/{keys,member}.rs`; sealed-key fixtures. Replace v2 routing inferred from audience/key/member paths with the clear audience/key/recipient prefix authenticated alongside the exact path. Tests: swapping each routing field or writer/number fails; foreign-recipient routing remains inspectable; own payload identity/hash is checked. Differential: byte checks none; exposure semantics in T31. Dependencies: T03–T04.

**Wave 09**

**T09 — Scope all storage paths by store ID and remove relocation.**
Spec: §4, D10, E5; note: S1#5. Files: `F/path.rs`; `P/{path,provider_check,connection,storage,conformance,test_utils}.rs`; provider path consumers; `S/{storage_setup,storage_copy,store_log_object,store_log_keys,device_log_sync,write_seal,write_object,snapshots,snapshot_data,snapshot_boundaries,snapshot_catalog,snapshot_reload,snapshot_retention,snapshot_file_retention,file_read,file_upload,joining_identity,operation_invites}.rs`; `D/{write_commit,upload}.rs`; `C/{builder,storage}.rs`; affected fixture tests. Remove `LocationTaken`, cross-store creation races, occupied-root rejection and relocation copying. Tests: two stores share one location without collision; all authenticated paths include the store. Differential: none. Dependencies: T07–T08, G4.

**Wave 10**

**T10 — Make key copies one durable, gap-free writer queue.**
Spec: §9/§11/§18, D10; note: S2#8, I14. Files: `D/{internal_schema,key_upload,store_log_upload,database_operations,lib}.rs`; `S/{store_log_keys,store_log_object,store_log_sync,operation_steps}.rs`. Remove `_coven_store_log_key_uploads` and recipient-path queue identity; extend `_coven_key_uploads` with writer/number, fixed bytes and optional `before_entry`. Reserved obsolete copies still settle. Tests: crash after reservation/publication, duplicate suppliers, cancellation, prerequisite-before-entry ordering and no number gaps. Differential: `CovenIO.Publication` via IO after T41. Dependencies: T08–T09.

**Wave 11**

**T11 — Add the per-device clock object.**
Spec: §9, D4/D8/D9/D10; note: I3. Files: `F/{objects,sealed_single,path,lib,test_utils}.rs`, `F/+clock.rs`; clock fixtures; `P/provider_check.rs`. Add frame 12 and sealed kind 39 with device/path checks; allow replacement only for positions and clock objects. Remove no other object types. Tests: wire vectors, signature/path mismatch, replacement eligibility, absent versus malformed clock. Differential: none. Dependencies: T09.

**Wave 12**

**T12 — Put initial-key and writer-cursor evidence in invites.**
Spec: §12.2, D13, E10; note: I7/I14. Files: `F/codes.rs`, invite fixtures; `S/{operation_invites,joining_identity,codes}.rs`; `C/{bootstrap,bootstrap_authentication}.rs`. Replace invite construction lacking `initial_key`, inviting writer and published key-copy number. Tests: round-trip, malformed cursor, store mismatch and fixed invite bytes; retain v1 decoding. Differential: none. Dependencies: T10.

**Wave 13**

**T13 — Make deterministic nonces content-bound.**
Spec: §6/§11.1, D11; note: O nonce decision, A7. Files: `K/derivation.rs`; `S/{write_seal,store_log_object}.rs`; deterministic encrypted fixtures. Remove the v1 path/section/index-only derivation from v2 sealing. Tests: independent known-answer vectors; changed plaintext or clear prefix changes nonce; identical input reproduces bytes; section/path/index separation. Differential: none for cryptographic bytes; `CovenStorage.Objects` supplies the symbolic contract. Dependencies: T03, T07–T09.

**T14 — Implement the fixed file-reference grammar.**
Spec: §16.1, D12; note: A9/S1#4. Files: `F/file_reference.rs`, `F/file_reference_tests.rs`, `fixtures/uploaded-file.txt`. Add the `file <device> <id> <key>` form and retain legacy decoding where required. Remove current-format `on …`/`uploaded …` encoding assertions. Tests: canonical UUID/key encoding, malformed and trailing input, no local path disclosure. Differential: none. Dependencies: T03.
*Disjoint from T13.*

**Wave 14**

**T15 — Persist one first-unmet-condition record per subject/reporter.**
Spec: §18/§19.1, E5–E6; note: blocked-sweep, S2#10a. Files: `D/{internal_schema,stuck,operation,database_operations,database_sync,read,lib}.rs`, `D/+pending.rs`; `S/{operation_types,operations,operation_calls,operation_steps,error}.rs`; `C/{handle,lib}.rs`. Remove `_coven_stuck_logs`, `judged_at`, operation `failure TEXT` and the operation-only pending carrier. Add indexed `_coven_pending`, typed local persistence and `pending()/subscribe_pending()`. Tests: reporter isolation, atomic replacement/clearing, restart, database failure propagation, update-gated retry. Differential: `CovenStorelogData.Pending` via SD in T25. Dependencies: T01–T02, T06.

**Wave 15**

**T16 — Point the store-log differential at CurrentReplay.**
Spec: §9, proof C10–C12; note: O, S2#1. Files: `spec/proofs/storelog/StorelogRunner.lean`, `S/lib_tests.rs`, `scripts/check.sh`. Replace the historical action/parser projection and `CovenStorelog.resolve` call with `CurrentReplay.History`, current actions, storage times and received subsets. Tests: the current checked examples plus generated causal histories/arrival permutations. Compare modeled state and fates; Rust-only reasons and cryptographic bytes need separate assertions. Dependencies: T04–T05.

**Wave 16**

**T17 — Conflict only on contradictory effects.**
Spec: §9, C11; note: A1. Files: `S/{conflicts,replay}.rs`, their tests, `S/lib_tests.rs`. Remove broad shared-target/shared-key conflicts, including independent device additions and different-member removals. Tests: same-member device additions both survive; independent additions/removals commute; true add/remove, role, rename and boundary contradictions retain their winner rules. Differential: SL `CurrentReplay.contradiction`. Dependencies: T16.

**Wave 17**

**T18 — Apply the current priority tiers and finish empty circles after replay.**
Spec: §9/§14.7, C11; note: A6, S2#1. Files: `S/{conflicts,effects,replay}.rs`; `D/{store_log,store_log_check,store_log_tables}.rs`; `S/lib_tests.rs`. Remove the combined removal tier and eager intermediate empty-circle deletion. Record the responsible kept deletion/removal entry after replay, including collectively emptied circles. Tests: all tier boundaries, empty-then-refilled replay, latest responsible removal, dropped deletion restoration. Differential: SL `ReplayPolicy`/`CurrentReplay.circleCause`. Dependencies: T17.

**Wave 18**

**T19 — Validate online administrative calls through replay.**
Spec: §9/§18, E5/E9/E12; note: S2#3, O. Files: `S/{operation_calls,operation_steps,operation_invites,store_log_sync,error}.rs`; `D/{store_log,store_log_tables}.rs`; `C/{handle,circles}.rs`. Remove `PermissionDenied`, `LastAdmin`, `CircleNotMember`, `CircleDeleted`, `NotStoreMember` and duplicated prevalidation; make `TargetGone` carry `EntryTarget`. Tests: failed catch-up reserves nothing; sole-admin demotion and absent targets match replay rejection; retry revalidates. Differential: SL for modeled authority, Rust for typed target/API behavior. Dependencies: T15, T18.

**Wave 19**

**T20 — Permanently consume entries that landed too late.**
Spec: §9, D6, C10; note: O. Files: `D/{internal_schema,store_log,store_log_tables,store_log_upload}.rs`; `S/{store_log_sync,store_log_object,replay,operation_steps}.rs`; SL runner/tests. Retain first publication time and permanent late verdict; compare against **every unread stored entry**, including dropped entries and implicit own past. Remove any keep-only comparison or retry that changes publication time. Tests: equality at 30 days, later arrival of the witness, unknown device, dropped witness, restart and new-number operation retry. Differential: SL `Finality` through `CurrentReplay`. Dependencies: T19.

**Wave 20**

**T21 — Persist the single strict finality horizon.**
Spec: §9, C10; note: S2#10d, O, I21. Files: `D/{internal_schema,store_log,store_log_tables}.rs`; `S/{replay,store_log_sync}.rs`, `S/+finality.rs`; SL runner/tests. Add optional monotonic `H`; do not add per-entry final flags. Tests: no horizon versus zero, strict old boundary, inclusive quiet interval, tied publication times, late-window reset, incomplete scans and reopening. Differential: SL `CovenStorelog.Horizon`. Dependencies: T20, T36, G5. Use `T′ = T - δ`, not `T`, as the completed old-entry endpoint; test tied timestamps and underflow. Extend the IO differential with `observationTime`/`through_time` and compare the resulting SL horizon input.

**Wave 21**

**T22 — Commit a ready entry batch and its replay atomically.**
Spec: §9, SP steps 2–4; note: IF23, O. Files: `S/{store_log_sync,replay,replay_cache}.rs`; `D/{database_sync,store_log,store_log_tables}.rs`. Remove per-arrival full replay/table replacement and independently stale decoded copies. Tests: several ready entries yield one committed replay; failed transaction preserves old decoded/durable state; newly discovered prerequisites extend the batch; no observer sees intermediate membership. Differential: SL across batch partitions. Dependencies: T18–T21.

**Wave 22**

**T23 — Make circle deletion entry-only.**
Spec: §14.7, E12; note: A6, entry-effects-review #1. Files: `D/{circle_deletion,database_operations,lib}.rs`; `S/{operation_data,operation_steps,operations,operation_calls}.rs`. Delete `circle_deletion.rs`, `CircleRows`, its step offset, row-delete write and upload wait. Tests: existing and arriving rows keep generation/setter identities, are hidden by the named entry, and return when it drops. Rust's deleted-circle loss names its entry: add `entry_differential_histories` to `scripts/check.sh` and replace `entry_differential_counterexample` (spec/proofs/merge.md B16) with an agreement check. Differential: MG `Circles` and `EntryInputs`; SD `CurrentData` after T25. Dependencies: T18, T22.

**Wave 23**

**T24 — Store boundaries once, keyed by their checked entries.**
Spec: §9/§15/§17/§19.3; note: S2#7. Files: `D/{internal_schema,store_log,store_log_tables,write_boundary,snapshot_state,snapshot_load}.rs`; `S/{snapshot_boundaries,snapshot_reload}.rs`. Replace `_coven_versions`, `_coven_resets`, `_coven_applied_boundaries` with `_coven_boundaries`; derive selected boundaries from kept entries. Tests: repeated no-op raises/resets, concurrent winners, winner reversal, restart and atomic application. Differential: SD `ReplayEffects`/`Snapshots`; SQL persistence remains Rust-tested. Dependencies: T22.

**Wave 24**

**T25 — Retain inputs and rebuild entry-dependent data in the replay transaction.**
Spec: §9/§15/§19.1; note: O, entry-effects-review. Files: `D/{internal_schema,store_log,download,snapshot_load,snapshot_state,write_apply}.rs`; `S/{snapshot_reload,store_log_sync}.rs`; new SD runner and `D/+entry_effects_tests.rs`. Remove irreversible replacement of provisional rows, boundary inputs and refusal decisions. Tests: reset/raise/delete flips restore the appropriate rows, losses and pending judgments; crash cannot commit membership without its data projection. Differential: SD `CurrentData`, `EntryEffects`, `ReplayEffects`, `EntryFateLive`. Dependencies: T15, T23–T24.

**Wave 25**

**T26 — Preserve local children through reversible SQLite cascades.**
Spec: §8.4/§9; note: entry-effects-review, local-cascade finding. Files: `D/{internal_schema,removal_sql,removal_view,write_apply,snapshot_load}.rs`, `D/+local_effects.rs`, corresponding tests. Add retained local-row/cascade inputs tied to provisional causes; remove destructive-only treatment of local children when a synced parent is provisionally hidden. Tests: cascade/set-null chains, reversal, intervening local edits, repeated replay and final release. Differential: SD `EntryEffects`/`Durability` at the abstract boundary; actual SQLite actions need Rust tests. Dependencies: T25.

**T27 — Derive store identity from creation.**
Spec: §9; note: S2#10c. Files: `D/{internal_schema,store_log,store_log_tables}.rs`; `S/bootstrap.rs`; `C/builder.rs`. Delete `_coven_store` and its singleton index; read ID/name from checked creation, with matching directory settings during bootstrap. Tests: incomplete creation, reopened creation, mismatched ID/name fails, no duplicate persisted identity. Differential: none. Dependencies: T22.
*This shares `internal_schema.rs` with T26, so it must be its own wave: Wave 26.*

**Wave 27**

**T28 — Derive key provenance from authorized received introductions.**
Spec: §11, C12; note: S2#1, O. Files: `S/{store_log_keys,write_seal,snapshot_catalog,device_log_sync}.rs`; `D/{store_log,store_log_tables,internal_schema}.rs`; `K/keys.rs`; SL runner/tests. Remove `StoreIdentity.key`, `StoreCircle.key`, `_coven_circles.key`, kept-only historical-key selection and dropped-removal special cases. Tests: authorized dropped introductions stay usable for reading; conflicting commitments reject acquisition; unauthorized introductions supply no key. Differential: SL `CurrentKeys.receivedIntroductions/hashFor`. Dependencies: T04, T20, T27.

**Wave 28**

**T29 — Keep unlocked custody for the handle session.**
Spec: §11/§20.2, SP step 1; note: I10. Files: `K/custody/{mod,passphrase,store_custody}.rs`; `C/{builder,authentication,bootstrap_commit}.rs`; `S/restore_codes.rs`. Remove per-use unlock/Argon2 derivation and save paths that reread/rederive existing material. Tests: one unlock per session, additions persist without another derivation, failure leaves held state unchanged, close/forget releases secrets. Differential: none. Dependencies: none beyond the format foundation.

**T30 — Reject an individual bad key copy without failing store unlock.**
Spec: §11/§19.1, E5; note: E9. Files: `S/{storage_setup,store_log_sync}.rs`; `C/storage_tests.rs`. Remove “first damaged historical/circle copy fails unlock” behavior. Tests: usable store key plus bad unrelated copy succeeds with a pending record; later valid copy supplies the key; genuinely absent usable store key stays pending. Differential: SL `CurrentKeys.acceptedCopy`, with Rust unlock-path tests. Dependencies: T15, T28.
*Disjoint from T29.*

**Wave 29**

**T31 — Select keys from complete copy observations and exposure.**
Spec: §11, C12, SP phase 2; note: I12/I14, O. Files: `S/{store_log_keys,write_seal,device_log_sync,snapshot_catalog}.rs`; `D/{key_scope,internal_schema}.rs`; SL runner/tests. Remove shared-current-key selection and former-member fallback sealing. Tests: foreign-recipient forged copy counts as exposure, own bad hash does not supply a key, incomplete observation blocks first attempts, newest usable introduction wins, old keys still decrypt. Differential: SL `CurrentKeys.select/firstAttempt`. Dependencies: T10, T28, T30.

**Wave 30**

**T32 — Publish rotation as a durable operation.**
Spec: §11/§18, D6 tag 15; note: S2#1, O. Files: `S/{operation_data,operation_types,operation_steps,operation_calls,store_log_keys,operations}.rs`; `D/store_log_upload.rs`; SD runner/tests. Remove removal-driven replacement-key generation and any use before both copy and rotation-entry publication are confirmed. Tests: restart at every step, concurrent rotations, unusable replacement, ordinary member store rotation, circle-member authority, dropped operation revalidation. Differential: SD `KeyPhases`, `Operations`, `KeySelection`. Dependencies: T19, T28, T31.

**Wave 31**

**T33 — Make intended provider access one serialized phase.**
Spec: §4/§13/§18, E9; note: S2#2, O. Files: `S/{operation_data,operation_types,operation_calls,operation_steps,operation_invites,operations}.rs`; `P/{storage,connection,provider_ops}.rs`, access adapters; `D/{internal_schema,database_operations}.rs`; `C/handle.rs`. Remove public `MemberRemoval`, `Output::Removal`, duplicate removal-time revocation and `_coven_access_keys_to_delete`; use pending subjects for owner/manual/grant outcomes. Tests: shared accounts, open invites, superseded access, obsolete request correction and retained grants. Differential: SD `Access`. Dependencies: T15, T32.

**Wave 32**

**T34 — Reload every newly readable circle, including one returning from deletion.**
Spec: §9/§14.3/§15; note: O rejoin decision. Files: `S/{store_log_sync,snapshot_reload,snapshot_boundaries}.rs`; `D/snapshot_state.rs`; SD tests. Remove the `!previous.deleted` restriction and reliance on already-passed write headers to recover skipped parts. Tests: leave/rejoin, absent/deleted circle returning, passed positions, other audiences unchanged and failed reload atomicity. Differential: SD `CurrentExamples.deleted_return_reloads`. Dependencies: T25, T28.

**T35 — Reject local writes outside current circle membership at commit.**
Spec: §14, E3/E12; note: E7. Files: `D/{write_commit,write_schema,key_scope,error}.rs`; `D/{write_tests,write_commit_tests}.rs`. Remove acceptance that reaches upload-time key failure. Tests: direct SQL, shared-trigger writes, moves and file attachment all return `NotInCircle` atomically; membership changing before commit is rechecked. Differential: none; this is SQL authorization. Dependencies: T18.
*Disjoint from T34.*

**Wave 33**

**T36 — Add exact status, publication metadata and replacement revisions.**
Spec: §4, E1, SP; note: I2, E12. Files: `P/{storage,provider_ops,connection,object_listing,test_utils,conformance}.rs`, all five provider adapters; `C/test_utils.rs`. Add exact-object status and metadata on reads; replace size/time-only replacement identity. Tests: absent versus failed lookup, complete-only multipart visibility, stable immutable time, changed revision for equal-size/equal-time replacement and checksum semantics. Differential: none for native guarantees; IO `Storage` models the required contract. Dependencies: T09, T11, G5.

**Wave 34**

**T37 — Scope Drive storage to real folders and exact names.**
Spec: §4/D10/§3.1; note: I1, IF1/IF24. Files: `P/providers/google_drive.rs` and its tests. Remove flat full-path filenames, root enumeration/filtering and repeated known-ID resolution. Tests: nested folders, cached IDs, one exact-query miss, paginated hit, native duplicate handling and incomplete search never becoming absence. Differential: none. Dependencies: T36.

**T38 — Scope Dropbox listing to the requested folder.**
Spec: §4/D10/§3.1; note: I1, IF1. Files: `P/providers/dropbox.rs` and its tests. Remove recursive root scans for unrelated prefixes. Tests: immediate writer folders, pagination, absent folder, namespace roots and no file-history enumeration during discovery. Differential: none. Dependencies: T36.

**T39 — Cache OneDrive folders and scope its listings.**
Spec: §4/D10/§3.1; note: I1, IF1/IF24. Files: `P/providers/onedrive.rs` and its tests. Remove whole-location filtering and repeated parent creation/lookups for known paths. Tests: native request trace for warm read/create, pagination, folder disappearance and exact-name misses. Differential: none. Dependencies: T36.

**Wave 35**

**T40 — Expose CloudKit writer discovery and scoped metadata.**
Spec: §4/E1/§3.1; note: I1/I2/I14. Files: `P/providers/cloudkit.rs` and its tests; its fake bridge implementations. Require indexed entry-1 writer discovery and scoped prefix queries; remove history enumeration as a way to derive writer IDs. Tests: selected zone, pending assets excluded, pagination and native-request accounting. Differential: none for the native bridge. Dependencies: T36.

**Wave 36**

**T41 — Discover store-log entries and key copies by next number.**
Spec: §6/§9/§11, SP step 2; note: I5 revision/I13/I14. Files: `S/{store_log_sync,store_log_keys,joining_identity,bootstrap}.rs`; `D/{internal_schema,database_sync}.rs`; `S/+discovery.rs`; new IO runner and `S/+discovery_tests.rs`. Remove full store-log/key history listings and positions-derived discovery bounds. Persist received bytes separately from applied positions. Tests: fetch-ahead gaps, crashes before cursor commit, unknown writers, registrations discovered during scanning, malformed copies followed by valid copies, invite cursor behavior. Differential: IO `Publication`, `Discovery`, `Execution`. Dependencies: T10, T12, T22, T37–T40.

**Wave 37**

**T42 — Select the current counting snapshot.**
Spec: §15, D9, IO snapshot coverage rule; note: I19 revision 4. Files: `S/{snapshot_catalog,snapshot_boundaries,snapshot_reload}.rs`; `D/{snapshot_coverage,snapshot_state}.rs`; IO runner/tests. Keep greatest summed normalized write positions and smaller-path ties, filtering late snapshots before selection or coverage. Preserve schema/reset/reader usability checks. Publication, deletion, loading and refresh are T95–T98; do not implement another ordering or treat arrival-time dominance as required. Tests: equal counts and paths, incomparable timely snapshots, late high-count arrival, per-audience selection and boundary changes. Differential: IO `catalogCurrent` with `.writes`, `late_snapshot_ignored` and `RepairedExamples.old_races_preserved`. Dependencies: T24, T36, T94.

**Wave 38**

**T43 — Use next-number write discovery with safe return checkpoints.**
Spec: §6/§15, SP steps 3/6; note: I11/I20. Files: `S/{device_log_sync,snapshot_reload,snapshot_catalog,operations}.rs`; `D/{internal_schema,database_sync,snapshot_coverage}.rs`; IO runner/tests. Remove device-log history listings and snapshot-number probing. Save qualifying `S` only after committed catch-up and confirmed posts. Tests: new/30-day return, no-advance snapshot, required gaps, crossing the deadline during a pass, failed clock observation and suspension. Differential: IO `needsSnapshots`/`readRecentLog`, `accepted_miss_is_recent` and `recent_return_complete`, including `pass_crosses_deadline` and the clock-jump counterexample outside the required assumption. Check the guard on every cached or parallel miss at use, pair the storage sample with its original sleep-inclusive monotonic start, and take discovery at equality. Dependencies: T41–T42.

**Wave 39**

**T44 — Retain checked object facts across passes and reopen.**
Spec: §3.1, SP “What survives a pass”; note: I4, IF3/5/19/20. Files: `S/{pass_reads,write_object,snapshot_catalog,snapshot_file_retention}.rs`; `D/{internal_schema,download,snapshot_metadata}.rs`; IO runner/tests. Replace pass-end-expiring metadata/reference maps with indexed durable observations keyed by immutable identity or replacement revision. Tests: repeated wait causes no redownload; key arrival reuses bytes; changed positions revision invalidates only that observation; eviction is explicit and accounted. Differential: IO `Trace`/`Downloads`. Dependencies: T36, T41, T43.

**Wave 40**

**T45 — Stream each required body once with bounded prefix reads.**
Spec: §4/§3.1, SP streaming section; note: I4, IF2/20. Files: `P/{storage,provider_ops,connection}.rs`, HTTP/S3/CloudKit read adapters; `S/{write_object,snapshot_catalog,snapshot_io,stream_input,object_range}.rs`. Remove per-chunk pair-of-range-GET loops and unbounded complete-body collection. Tests: at most two prefix/header ranges, one remainder stream, chunk-boundary splits, truncation/extra bytes, whole-signature failure and bounded buffering. Differential: IO `Downloads` for dispositions; Rust traces for bytes/requests. Dependencies: T36, T44.

**Wave 41**

**T46 — Gate every send on a recent completed catch-up.**
Spec: §10, SP steps 1–4 and upload worker; note: I18, commit `9851cc67`. Files: `B/clock.rs`; `S/{operations,sync_loop,store_log_sync,recorded_upload}.rs`; `P/{connection,providers/http,providers/s3}.rs`; `S/+send_gate.rs`. Add injected monotonic elapsed time including sleep; remove send routes and SDK retries that bypass shared freshness. Tests: just-before/exactly-five-minutes, slow catch-up, suspension, reconnect/reset, request waiting for a permit and clock-object bootstrap ordering. Differential: existing IO gate covers identity ordering only; elapsed-time/closure needs a model extension and Rust tests. Dependencies: T41, T45.

**Wave 42**

**T47 — Check identity before any publication and reset stale copies.**
Spec: §10/§19.2, E5; note: A7, S1#3. Files: `S/{operations,store_log_sync,device_log_sync,joining_identity}.rs`; `C/{builder,bootstrap,bootstrap_commit,handle}.rs`; `D/{internal_schema,database_sync}.rs`; `B/files/{bootstrap,recovery,layout}.rs`; new ST runner/tests. Add shared counter/custody/post/snapshot evidence and durable `_coven_device_reset`; discard stale local queues rather than renumbering them. Tests: restored backup, second live copy, missing custody, preserved bootstrap identity after failure and no sends before evidence. Differential: ST `Identity`/`ReplacementRead`. Dependencies: T15, T43, T46.

**Wave 43**

**T48 — Apply replacement by recorded reads and retain device states.**
Spec: §10, D6, E9; note: S2#9. Files: `S/{effects,store_log_object,write_object,write_seal,bootstrap}.rs`; `D/{store_log,store_log_tables,internal_schema}.rs`; ST runner/tests. Replace boolean-only device state with active/removed/replaced; add no log-end counters. Tests: concurrent replacements keep both new IDs, before-read old objects pass this check, after-read objects fail, replacement reversal rebuilds admission, removed local identity remains stopped. Differential: ST `ReplacementRead`; other admission rules remain separate. Dependencies: T05, T25, T47.

**Wave 44**

**T49 — Recompute retired-write landing verdicts with replay.**
Spec: §10/§19.1, D5; note: I16 corrected, commit `3d9d8250`. Files: `D/{internal_schema,download,store_log,write_apply}.rs`; `S/{write_object,device_log_sync}.rs`; SD/ST tests and a new Lean admission module. Retain write publication time and original inputs; consume too-late positions without effects. Do not reuse permanent entry lateness as the write rule. Tests: member/device removal and replacement, equality at 30 days, multiple retirements, arrival permutations, reversal restores the write but never a permanently late entry. Differential: requires extending SD/ST; existing proofs assume this verdict. Dependencies: T20, T25, T48.

**Wave 45**

**T50 — Drain retired writers only after the finality wait.**
Spec: §6/§9/§10/§13, SP steps 2/6; note: I15/I18 corrected, commit `d414449c`. Files: `D/{internal_schema,store_log,database_sync}.rs`; `S/{store_log_sync,device_log_sync,discovery,finality}.rs`; IO runner/tests. Add `_coven_drained_devices` and qualifying finality observation `F`; remove unconditional forever-probing only after successful three-log drainage. Tests: strict `T > F + 1 day + 5 minutes`, restart, delayed finality, pre-T misses, in-flight reads, partial failure, retained waits and atomic basis invalidation. Differential: IO `Retired`; extend timing-to-closure proof. Dependencies: T21, T41, T43, T46, T49, G5.

**Wave 46**

**T51 — Make the pass follow catch-up → keys/access → data.**
Spec: §6, SP phases 1–3; note: O, S2#2. Files: `S/{operations,store_log_sync,operation_steps,device_log_sync,sync_loop}.rs`. Remove publish-before-discovery, repeated independent catalog reads and repeated operation passes used to manufacture ordering. Tests: incomplete first phase prevents administrative work/first attempts; rotation receipts precede new sealing; provider-access waits do not block unrelated protected data; reload gates affect only required work. Differential: SD `KeyPhases` plus IO `Execution`. Dependencies: T19, T32–T34, T46–T50.

**Wave 47**

**T52 — Share durable retry deadlines and provider cooldowns.**
Spec: §3.1/§16.5/§19.1, E5/E7, SP waiting; note: I9 supersedes Clock#2, E4. Files: `P/{error,connection,providers/http,providers/cloudkit}.rs`; `D/internal_schema.rs`, `D/+retry.rs`; `S/{operations,sync_loop,file_upload,joining_identity}.rs`. Replace one-second polling and wall-clock-only upload backoff with durable `T,W`, monotonic live waits and category policy. Tests: restart clamp, backward/forward clock changes, Retry-After, explicit retry cannot bypass cooldown, shared consumers and SDK retry accounting. Differential: IO `Waiting`, extended adapter. Dependencies: T15, T46, T51.

**Wave 48**

**T53 — Read peers once and post only changed publishable positions.**
Spec: §6/§19.1, D8, SP steps 3/4/9; note: IF4/7/18, S1#1 rejected simplification. Files: `S/{device_log_sync,posted_positions,snapshot_retention,pass_reads}.rs`; `D/{database_sync,pending}.rs`. Remove duplicate peer reads, unconditional replacement and log-only fingerprint disagreement. Preserve confirmed positions while posting updated pending reports. Tests: equal revision reused, same-time replacement detected, comparable disagreement recorded, own uploads prevent false advancement, unchanged idle pass sends no post. Differential: IO `Trace`; fingerprint comparison also needs Rust tests. Dependencies: T06, T15, T36, T44.

**Wave 49**

**T54 — Coalesce wakes without generating another pass for own bookkeeping.**
Spec: §3.1, SP step 9/waiting; note: IF6/9/18. Files: `S/{sync_loop,operations}.rs`; `D/database_changes.rs`; `C/handle_tests.rs`. Remove unconditional operation polling and internal-commit feedback that schedules redundant passes. Tests: racing app write is not lost, own upload/positions commits do not trigger another full pass, one retained wake, earliest due retry, stopped/closed behavior. Differential: IO `Execution`/`Waiting`. Dependencies: T51–T53.

**Wave 50**

**T55 — Route object waits/refusals to their subjects without failing the pass.**
Spec: §19.1, E5, SP; note: E11–E12, blocked-sweep. Files: `S/{write_object,write_download,store_log_object,snapshot_catalog,posted_positions,bootstrap}.rs`; `D/{download,stuck,pending}.rs`. Remove unknown-author-as-damage, semantic-failure-as-parse and repeated full reads to establish an already observed failure. Tests: unknown registration waits; complete invalid object refuses; transport truncation does not; independent logs progress; immutable refusal stops that log until update/reset rules permit it. Differential: IO `Downloads`, SD `Pending`. Dependencies: T02, T15, T44–T45.

**Wave 51**

**T56 — Serve member and circle read/subscription pairs from committed queries.**
Spec: E9/E12, §20.2; note: O paired queries, S2#10a. Files: `D/{read,database_operations,observation}.rs`; `S/{operations,operation_calls,operation_types}.rs`; `C/{handle,circles,read_handle}.rs`. Remove worker-command reads and mirrored state where the same committed query supplies both forms. Tests: immediate subscription value, replay transaction coherence, removed/replaced device details, disconnected reads and read failure propagation. Differential: none. Dependencies: T18, T33, T48.

**Wave 52**

**T57 — Fix file identity at attachment and remove upload-marking writes.**
Spec: §16.1/§16.5, E3/E7/E8, D12; note: A9/S1#4. Files: `D/{file_staging,file_write,file_row,file_ref,file_location,file_database,file_queue,write_commit}.rs`; `S/{file_upload,file_read,files}.rs`; `C/file.rs`. Remove upload-time `fix_identity`, `OnDevice` versus `Uploaded` row encoding, and `finish_upload`’s app-row rewrite. Tests: retarget/delete/move during upload cannot be undone by completion; `FileRef` remains stable; original upload bytes retain their captured identity. Differential: ST `Files`. Dependencies: T14, T47.

**Wave 53**

**T58 — Own chunk hashes once in the upload queue.**
Spec: §16.6; note: S2#4. Files: `D/{internal_schema,file_hashes,file_queue,file_staging,file_write,file_database}.rs`; `S/file_upload.rs`. Delete `_coven_file_upload_chunks`, row-keyed duplicate hash ownership and hash-copy/delete triggers; key `_coven_file_chunks` by immutable upload/file identity. Tests: attachment transaction, row retargeting, queue completion, crash recovery, exact chunk count and missing hash fails before encryption. Differential: none; SQL ownership and source verification need Rust tests. Dependencies: T57.

**Wave 54**

**T59 — Run file uploads outside the sync pass.**
Spec: §6/§16.5, SP upload-worker section; note: I17, commit `fa82bf87`. Files: `S/{files,file_upload,operations,sync_loop}.rs`. Remove awaited upload drainage from `sync_pass` and upload-completion-induced full passes. Tests: held upload does not delay write sync/posting; pause/stop/close, shared transfer limits, reconnect, freshness expiry between parts and cooldown sharing. Differential: IO `Execution` needs independent-worker traces. Dependencies: T46, T52, T54, T57.

**Wave 55**

**T60 — Keep provisional-circle file sources and upload them normally.**
Spec: §9/§14.7/§16.5; note: S2#5 supersedes entry-effects-review’s “pause while hidden”. Files: `D/{file_write,file_removals,database_file_retention,local_file}.rs`; `S/file_upload.rs`. Remove source release and any upload suppression caused solely by provisional circle hiding. Tests: deletion/reversal preserves user paths and app bytes; hidden rows upload; real deletion/final release still reclaims eligible sources. Differential: SD `EntryEffects`/`RetentionSafety`, ST `Files`. Dependencies: T23, T25, T58–T59.

**Wave 56**

**T61 — Derive file availability from exact status and uploader evidence.**
Spec: §16.1, E8; note: A9, I2, blocked-sweep. Files: `S/{file_read,file_error,files}.rs`; `D/{file_ref,file_location}.rs`; `C/file.rs`. Remove row-location-based availability and directory-listing status probes. Tests: complete object wins, uploading uploader, signed unavailable report, removed uploader, malformed/authenticated wrong-content object and provider error versus absence. Differential: ST `Files` for abstract status; provider/Rust tests for actual evidence. Dependencies: T15, T36, T48, T57.

**Wave 57**

**T62 — Expose upload progress solely from committed queue/session state.**
Spec: §16.5–§16.6, E7; note: S2#10a. Files: `D/{internal_schema,file_queue,file_database,read}.rs`; `S/{file_upload,files,file_error}.rs`; `C/{file,handle,lib}.rs`. Remove upload `failure` column, public attempt/failure fields, `Preparing`/`Stored`, `AllInBackoff`, failure-bearing drain results and `UploadsLiveQuery`’s mirrored phase map. Tests: confirmed bytes advance only after persistence, reopen works offline, one-request completion atomically retires queue, failures appear in pending. Differential: IO `Downloads`/`Waiting` where modeled; session persistence Rust-only. Dependencies: T15, T52, T58–T59.

**Wave 58**

**T63 — Continue eager fill after an individual file fails.**
Spec: §16.4, E8; note: E8, S2#10a. Files: `S/{file_cache,files}.rs`, `S/files_tests.rs`. Remove `EagerCacheFillStatus::Failed` and first-failure termination/requeue of unrelated files. Tests: bad first/middle file leaves a file-subject pending record while later files complete; cancellation and retry eligibility remain correct. Differential: none. Dependencies: T15, T61.

**Wave 59**

**T64 — Make pin/eager progress committed coverage queries.**
Spec: §16.4, E8; note: S2#10a. Files: `D/{file_cache,file_database,read,observation}.rs`; `S/{file_cache,files}.rs`; `C/file.rs`. Remove callback-accumulated downloaded-byte totals, eager watch mirrors and custom `RowsPinnedLiveQuery`; use specified read/live-query pairs with whole requested byte totals. Tests: already-cached bytes, concurrent readers, eviction, reopened progress, ordered absent rows and failed reads. Differential: none. Dependencies: T62–T63.

**Wave 60**

**T65 — Publish verified cache ranges together and share overlapping fetches.**
Spec: §3.1/§16.3/§16.6; note: IF15. Files: `S/{file_read,files}.rs`; `D/{file_cache,file_database}.rs`; `B/files/atomic_file.rs`. Remove per-chunk durable publication when one verified range can commit together and duplicate concurrent range transfers. Tests: crash before/after file/database publication, overlapping requests, authentication failure, cancellation and bounded memory. Differential: ST `Files` only for publication invariants; real disk behavior Rust-tested. Dependencies: T61, T64.

**Wave 61**

**T66 — Maintain indexed cache totals and changed-file work.**
Spec: §3.1/§16.4/§16.6; note: IF16/18 and IO SQLite-index findings. Files: `D/{internal_schema,file_cache,file_database,database_file_retention}.rs`; `S/file_cache.rs`. Remove full namespace `SUM` per budget check and repeated all-eager-row/O(n²) comparison. Maintain totals and changed-reference work in the same transaction. Tests: eviction/rollback/pin accounting, changed-row selection, `EXPLAIN QUERY PLAN`, idle work independent of cached chunk count. Differential: none. Dependencies: T58, T64–T65.

**Wave 62**

**T67 — Delete files from retained reference facts, with one cutoff scan.**
Spec: §15/§16.5, SP step 8; note: I6 revised/I14, IF5. Files: `S/{snapshot_file_retention,snapshot_catalog,write_object}.rs`; `D/{database_file_retention,file_removals,snapshot_writes,internal_schema}.rs`; IO/SD tests. Remove idle `files/` enumeration and repeated body reads to rediscover references. Include rows, all losses, queued work, snapshots and reversible inputs; persist removed-uploader orphan-scan completion only after confirmed cutoff. Tests: frozen-loss reference, unreadable audience, late upload, failed scan/reopen and restored access epoch. Differential: SD `RetentionSafety`, IO `Retired`. Dependencies: T25, T44, T50, T60, T66.

**Wave 63**

**T68 — Make reset discard uncovered pre-reset effects without losses.**
Spec: §19.3/§15, D7, E4; note: A2. Files: `D/{write_boundary,download,snapshot_load,snapshot_state,snapshot_writes,write_apply,lost,loss_record}.rs`; `F/{snapshot_rows,loss,merge_fields}.rs`; `S/snapshot_reload.rs`; MG runner/tests. Remove reset-as-loss `LostWriteCause::Reset` and corresponding replacement/loss encoding in v2. Tests: first and later late writes both ignored, positions consumed, author reloads too, post-reset write reads discarded numbers without inheriting their effects, other audiences unchanged. Also: Rust's reset predicate asks whether a write read the snapshot's write frontier; §19.3 asks whether it read the kept reset entry (store_log_read). Use the entry. Add `reset_differential_histories` to scripts/check.sh and replace the fixed reset counterexample check (spec/proofs/merge.md B16, ResetCounterexample) with an agreement check. Differential: MG `Reset`/`Boundary`. Dependencies: T24–T25, T49.

**Wave 64**

**T69 — Preserve original queued writes and upload the captured prefix before a raise.**
Spec: §6/§15/§17.1/§18.1/§19.2, D5/D7, E13; note: A3 and the queue/io upload-before-raise proofs. Files: `D/{migration_writes,migration_snapshot,snapshot_writes,snapshot_load,upload,download,write_boundary,error}.rs`; `S/{write_upload,schema_sync,snapshot_data}.rs`; `F/write.rs` and affected write fixtures; new Q runner and `D/+queue_differential_tests.rs`. Remove conversion of every original queued record, including untried records, covered-means-stored queue deletion, transmitted lost disposition and `MigrationUploadMissing`. Keep received interpretations and losses separate from authored bytes. Append the empty migration marker after the old queue and capture through it; permit old-schema heads and that marker before the kept raise, confirm the captured prefix before requesting the snapshot, then publish the raise entry. Ordinary new-schema app writes wait for a kept raise and do not extend the captured cut. T95 supplies the shared snapshot request gate and §15's catch-up/expiry conditions.

Tests: attempted and untried original records survive successive migrations; old write 1 and marker 2 publish the raise with new-schema 3 still queued; crash/lost reply preserves the prefix confirmation; competing raise, callback/no callback, callback failure without partial commit, Ben 9/10 including attempted 9, post-adoption 11, excluded implicit own input, reset precedence, losing markers without losses, discarded-input facts surviving dismissal, and unchanged upload bytes after author reload. Differential: Q `Upload`, `Conversion`, `Verdicts`, `RaiseUpload`, `RaiseProgress`, `RaiseExamples`; compare original queue, attempts, interpreted records, verdicts/losses and raise stages after each event. IO `SnapshotWrites` compares request admission and covered published numbers, including `SnapshotWriteExamples.pending_run` and `two_audience_raise_keeps_circle_part`; MG `Boundary` checks row effects. Dependencies: T03, T24, T68.

**Wave 65**

**T70 — Turn independently hidden migration rows into ordinary generation deletes.**
Spec: §17.1, D7; note: A5. Files: `D/{migration_state,migration_run,migration_change,migration_writes,removal_view,write_apply}.rs`; MG runner/tests. Remove deletion of merge identities from `_coven_migration_retired` and blanket freezing based on current `DeletedCircle` losses. Rerun all removal passes without non-final circle causes and their downstream effects. Tests: provisional parent/child untouched; independent CHECK/unique failure deleted; late eligible edit loses to the retained generation delete; one frozen whole-row loss. Also: add the strict migration comparison from spec/proofs/merge.md B16 (MigrationCounterexample: a row hidden solely by a non-final circle deletion) to scripts/check.sh and replace its fixed counterexample check with an agreement check. Differential: MG `Migration`/`Circles`. Dependencies: T21, T23, T25, T69.

**Wave 66**

**T71 — Preserve losses under dropped/renamed schema using frozen names.**
Spec: §17.1/§15/§19.1, D7, E4; note: A5. Files: `D/{internal_schema,migration_state,loss_record,lost,dismissal,fingerprint,snapshot_write,snapshot_load}.rs`; `F/{loss,merge_fields,snapshot_rows}.rs`; MG runner/tests. Remove the old retired-loss representation and schema-dependent loss deletion; frozen records retain old names, setters and causes without live parent links. Tests: dropped table/column, name reuse, whole-row loss, dismissal, snapshot round-trip and fingerprint preservation. Differential: MG `Migration.freezeDropped`, `Accounting`. Dependencies: T68, T70.

**Wave 67**

**T72 — Verify the sealed snapshot before and after publication.**
Spec: §15, SP step 8; note: A8. Files: `S/{snapshot_seal,snapshot_io,snapshots,snapshot_catalog,snapshot_data}.rs`; `D/{snapshot_write,snapshot_metadata,snapshot_load}.rs`; provider checksum adapters. Add local reopen through the real reader and fingerprint/schema checks, then complete-object provider checksum or one full readback. Remove prefix-only confidence as publication confirmation. Tests: damaged temp file, wrong fingerprint, multipart checksum distinction, lost reply and changed stored bytes. Differential: none for cryptography/IO. Dependencies: T36, T45, T71.

**Wave 68**

**T73 — End snapshot operations at confirmed upload; derive retention separately.**
Spec: §18/§15, SP step 8; note: S2#6. Files: `S/{snapshot_data,snapshots,snapshot_retention,operation_data,operation_steps,operations}.rs`. Delete `SnapshotJob::Retain`, retention journal rows and retention stages embedded in snapshot completion. Tests: confirmed snapshot finishes despite retention wait/failure; next due retention uses catalog evidence; crash never repeats publication under new bytes. Differential: SD `Operations`/`Retention`. Dependencies: T52, T72.

**Wave 69**

**T74 — Require final usable coverage before physical history release.**
Spec: §9/§15/§16.5; note: A8, O, entry-effects-review retention findings. Files: `S/{snapshot_retention,snapshot_file_retention,snapshot_boundaries}.rs`; `D/{snapshot_coverage,database_file_retention,pending}.rs`; IO/SD tests. Remove wall-clock aging and deletion based on provisional key/boundary effects. Tests: every relevant entry final, all-part coverage, absent/invalid active-reader positions, first lagging reader, tied storage times, provisional reversal and permanent refusal blocking absence proofs. Differential: SD `StorageFinality`/`RetentionSafety`, IO `Retention`. Dependencies: T21, T42, T50, T67, T72–T73, G5.

**Wave 70**

**T75 — Replace damaged-database salvage with the shared fresh-device reset.**
Spec: §19.2/§10, E1/E5; note: S1#3. Files: `D/{database_recovery,database,database_builder,error,lib}.rs`; `C/{builder,error,bootstrap}.rs`; `B/files/recovery.rs`. Delete `database_recovery.rs`, salvage tests, old recovery journal/marker, `open_reloading` and `RecoveryError`; retain only bootstrap state needed to retry the chosen fresh identity. Tests: damaged archive retained, no unsent queue salvage, bootstrap failure/reopen, reset notice committed before publishing the handle. Differential: ST `Identity` reset boundary. Dependencies: T47.

**T76 — Remove every future-timestamp hold.**
Spec: §7.2; note: A4, Clock#1. Files: `D/{download,snapshot_load}.rs`; `S/{store_log_sync,snapshot_reload}.rs`; corresponding clock-jump tests. Remove `now + 300_000` acceptance gates and tests expecting future-stamped work to wait. Keep causal ordering and timestamp advancement. Tests: far-future write, entry and snapshot apply once actual prerequisites exist; later timestamps remain ordered after the wall clock moves back. Differential: MG timestamp/order behavior; acceptance gates Rust-tested. Dependencies: none.
*Disjoint from T75.*

**Wave 71**

**T77 — Reserve DamagedDatabase for stored damage.**
Spec: §19.1/§20.3, E1/E5; note: E13, S2#10b. Files: `D/{error,database_operations,operation,upload,file_queue,snapshot_error}.rs`; `S/{error,operation_data,operations,file_error}.rs`; `C/error.rs`. Replace invariant/worker/remote-input paths mislabeled as database corruption with typed failures; preserve sources across internal handoffs. Tests: corrupted persisted bytes versus invariant failure versus remote rejection, worker death reaches `Failed(Database)`, no failure becomes an empty query. Differential: none. Dependencies: T15, T62, T75.

**Wave 72**

**T78 — Classify sync status from the latest attempt.**
Spec: E5/§19.1; note: E10 final decision. Files: `S/sync_loop.rs`; `P/connection.rs`; `C/handle_tests.rs`. Remove sticky “ever reached storage” as the Offline criterion. Tests: successful pass then unreachable network is Offline; provider error response is Failed; individual pending subjects do not fail the pass; stop/reconnect clears only the appropriate session evidence. Differential: none. Dependencies: T52, T55, T77.

**Wave 73**

**T79 — Refresh OAuth once on provider rejection for every consumer.**
Spec: §4, E5/E10; note: E2, Clock#1. Files: `P/{credentials,connection,providers/http,providers/oauth,providers/connector}.rs`; `S/{restore_codes,sync_loop}.rs`; `C/authentication.rs`. Remove `expires_at` scheduling, `refresh_if_expired`, local-expiry rejection and `OAuthError::Expired/InvalidExpiry` where obsolete. Persist rotated credentials before installing them. Tests: concurrent rejected requests share refresh, app operation between passes, retry once, refresh persistence failure, scope errors do not loop. Differential: none. Dependencies: T29, T52.

**Wave 74**

**T80 — Classify token-endpoint and redirect failures correctly.**
Spec: E10/§20.3; note: E5. Files: `P/providers/{oauth,http}.rs`, their tests. Remove treating invalid client/configuration as reauthentication and every redirect error as user denial. Tests: invalid grant, invalid client, invalid scope, interaction/consent requirement, transient response, cancellation and typed original response retention. Differential: none. Dependencies: T79.

**T81 — Retry S3 clock skew once using server evidence.**
Spec: §4, E5; note: E6, Clock#4. Files: `P/providers/s3.rs` and its tests. Remove `RequestTimeTooSkewed → PermissionDenied`; perform one offset-corrected retry, then `ClockSkew`. Tests: corrected retry succeeds, second skew preserves cause, no credential replacement, retry observes freshness/cooldown policy. Differential: none. Dependencies: T46, T52.
*Disjoint from T80.*

**Wave 75**

**T82 — Correct Drive throttling and quota classifications.**
Spec: §4/E5; note: E3. Files: `P/providers/http.rs`, `P/providers/http_tests.rs`. Replace generic permission/refusal fallthroughs for documented sharing rate, daily and item/storage limit responses. Tests: provider response fixtures distinguish retryable throttling, quota, authentication and permission; Retry-After survives classification. Differential: none. Dependencies: T52, T80.

**T83 — Correct S3 transport, protocol and quota classifications.**
Spec: §4/E5; note: E6. Files: `P/providers/s3.rs` and its tests. Remove timeout-as-refusal, quota-as-generic-failure and SDK construction/response failures mislabeled Network; retain archive-state/provider causes. Tests: each E6 mapping, malformed SDK response, request construction, 400 timeout and 507/storage-full fixtures. Differential: none. Dependencies: T81.
*Disjoint from T82.*

**Wave 76**

**T84 — Correct Dropbox native error classifications.**
Spec: §4/E5; note: E3. Files: `P/providers/http.rs`, `P/providers/http_tests.rs`. Remove generic 409/refusal fallthrough for write permission, traffic/space, suspension and scope failures. Tests: nested tags and summary prefixes, category precedence and preserved provider wait. Differential: none. Dependencies: T82.

**T85 — Restart S3 multipart publication after conditional conflict.**
Spec: §4/§16.5/§18; note: E6. Files: `P/providers/s3.rs`, `P/session.rs`, their tests. Remove conditional-409-as-rate-limit behavior and retries of an invalid completion state. Tests: new session with identical fixed bytes, occupied-path comparison, lost completion reply and abort failure preserving both causes. Differential: none. Dependencies: T83.
*Disjoint from T84.*

**Wave 77**

**T86 — Handle OneDrive temporary locks and overlapping parts.**
Spec: §4/§16.5, E5; note: E3. Files: `P/providers/{http,onedrive}.rs`, their tests. Remove permanent refusal for temporary lock and generic range-error treatment of fragment overlap. Tests: locked response respects retry policy; overlap queries/resumes confirmed progress rather than corrupting or abandoning it; invalid app ranges remain errors. Differential: none. Dependencies: T52, T84.

**T87 — Carry CloudKit retry timing and response evidence.**
Spec: E1/E5/§4; note: E14/E12. Files: `P/providers/cloudkit.rs`, bridge fake/tests. Remove opaque bridge failures that cannot supply typed cooldown/reached-provider facts and empty-account `AccountIdUnavailable`. Tests: native retry delay, authentication, quota, permission, temporary service failure, partial failure and empty-account authentication. Differential: none; app-native mapping requires bridge conformance. Dependencies: T40, T52, T78.
*Disjoint from T86.*

**Wave 78**

**T88 — Ignore unrelated listing entries without hiding provider failures.**
Spec: §4/D10/§19.1; note: E1/E12. Files: `P/{object_listing,provider_check}.rs`, all provider listing parsers and tests; `S/storage_setup.rs`. Remove foreign-item-as-InvalidPath aborts and malformed/missing root misclassification. Tests: stray files/folders, another store, malformed coven-shaped objects, explicit invalid caller path, missing container, incomplete search and failed page. Differential: none. Dependencies: T09, T37–T40, T82–T87.

**Wave 79**

**T89 — Use the provider’s actual single-request upload limit.**
Spec: §3.1/§16.5, SP upload worker; note: IF13. Files: `S/{file_upload,write_upload,recorded_upload}.rs`; `P/{transfer,session}.rs`. Remove the arbitrary 1-MiB file cap and encryption-chunk-size-driven session decisions; retain bounded source reads and the required whole-source verification. Tests: below/at/above each injected limit, multichunk single request, multipart restart, confirmed progress and source changed before retry. Differential: none. Dependencies: T57–T59, T62, T85–T86.

**Wave 80**

**T90 — Enforce native-request and local-query bounds as acceptance tests.**
Spec: §3.1/§20.4, SP counting; note: I8, IF17/18 and SQLite-index findings. Files: `P/{conformance,test_utils}.rs`, provider HTTP/native test harnesses; `S/+sync_pass_cost_tests.rs`; database query-plan tests; IO runner; `scripts/check.sh`. Remove historical assertions that normalize full-history listing, repeated body reads or idle durable writes. Tests: warm idle `2 + 3N` with actual pages counted, retirement reducing probes, bounded fetch-ahead, new-object streams, reopened retained waits, independent upload accounting, zero unchanged durable commits and indexed scheduling/coverage queries. Differential: IO `Trace`/`Execution`/`Refinement`; native requests and SQLite plans remain Rust measurements. Dependencies: T41–T89, T94–T98.

All behavioral bug tasks should begin with the failing regression named above. Each commit should run its affected crate checks and relevant differential; the final integrated state should run `scripts/check.sh`. Existing v1 compatibility tests remain obligations, even where current-policy tests and implementations are removed.
**Added after the hostile-member audit (honest-path conformance bugs)**

**T91 — Compare bytes on an occupied upload path.**
Spec: §6 ("A retry finding an occupied path reads and compares the complete bytes. Equal bytes count as stored; different bytes reset this stale device (§10). A failed comparison read leaves the upload pending."), §10. Files: `S/write_upload.rs` (every `AlreadyExists => return Ok(())` arm: single PUT, begin/resume/restart session), the same arms for store-log entries, key copies, snapshots and files if they share the shortcut (`S/store_log_sync.rs`, `S/file_upload.rs`, snapshot publication). Read the stored object, compare with the attempt's exact bytes; equal settles, different triggers the §10 stale-device reset, a failed read leaves the work pending. Tests: equal occupant settles; unequal occupant resets this device and never settles the queued write; failed comparison read stays pending; resumed and restarted sessions too. Differential: none. Dependencies: T08–T10 (key-copy paths), T47 (device reset). A draft exists at coven-worktrees/next-checker/plans/t91-comparison.patch (code +386/−132), uncommitted. Source: tmp/integrity/hostile-member-audit.md §1.

**T92 — Check a store-log folder against its device's registered member.**
Spec: §9 (entries live at `store-log/<device>/<n>`; whose entry it is comes from its signature; a device belongs to one member), §10. Files: `S/store_log_object.rs` (the device-owner check compares authors of earlier entries in the folder instead of the device's registered member). An entry in `<device>`'s folder must be signed by the member that device is registered to in the entry's recorded view; the first entry of a device adds that device (or creates the store). Tests: member X's entry in Y's empty registered folder is refused as WrongIdentity; a device's own add-device first entry is accepted; replacement registration. Differential: storelog after T16 if the model covers folder ownership. Dependencies: T04. Source: tmp/integrity/hostile-member-audit.md §2.

**T93 — Keep storage credentials outside restore codes.**
Spec: §2, §12.1, §13; E9/E10 and their E5 error definitions; D13. User decision: a restore code is the member identity, store id and name, and storage location only. It never carries an S3 key or OAuth tokens. The person keeps the S3 key separately and enters it on a new device, as account providers require sign-in. A QR transfer from one of their own devices may carry the code and S3 key as separate values; iCloud Keychain may keep the key alongside the code. Theft recovery is outside coven's threat model; do not add stolen-code recovery. Stable member ids and key replacement are a separate decided item (lean-member-keys), not part of this task.

Files: `crates/coven-sync/src/restore_codes.rs`; its callers in `C/{handle,bootstrap,bootstrap_commit,builder,storage}.rs` and `S/sync_loop.rs`; `S/{codes,error,lib}.rs`; `P/{restore_storage,credentials,lib}.rs`; `F/{codes,test_utils}.rs`; `K/custody/{keychain,platform,keychain_test_utils}.rs`; facade exports in `C/lib.rs`. Include their existing sibling tests, `C/{bootstrap_authentication,authentication,storage}_tests.rs`, `crates/coven/tests/fixtures/bootstrap.rs`, and `crates/coven-format/fixtures/{restore-code.hex,codes.txt}`. Check every restore-code constructor and restore/credential call for the changed contract; keep invite credentials as specified in §12.2.

Remove S3 credentials from the restore storage payload, its encoding and decoding, code generation and bootstrap output. Reuse the existing location and S3 credential types. `replace_access_key` returns `Result<(), SyncError>` and leaves the code unchanged; `update_credentials` takes `access_key_id` and `secret_access_key` directly. Remove the code-decoding, member/store/location comparisons that only extracted a replacement key, plus obsolete `SyncError::Code` and `WrongMember`; retain `WrongStore` for create-store validation. `restore_from_code` accepts the separate optional S3 credentials specified in E10; `restore_from_keychain` accepts an explicit key or reads the separately stored key. Require an S3 key before bootstrap; refuse one for other providers. Keep the existing account sign-in and invite paths.

Keep S3 custody and transfer separate from the code: replacement updates the held key, retained devices receive it directly or alongside an own-device QR scan, and the written code stays valid. Update synced S3 custody alongside device custody without rewriting the code. Setup, bootstrap, credential changes and rollback must preserve whole-operation failure reporting, including rollback errors; publication failures retain the replacement credentials for an idempotent retry. Store deletion removes the separate synced key too. Do not retain a credential-bearing restore-code reader or add a migration/compatibility path.

Tests: first reproduce S3 key disclosure through the existing restore-code API, then assert that binary and text codes contain only member identity, store identity/name and location for every provider. Pin the new restore payload fixtures. Replacing and updating S3 credentials must leave code bytes unchanged, install the supplied key on connected and disconnected handles, preserve location and member identity, and reject non-S3 updates. Restore succeeds with a separately entered key, an own-device QR's separately supplied key, or a key held alongside the iCloud code; missing keys, wrong-provider keys and keychain read failures fail visibly before bootstrap. Cover keychain write/rollback failures, cancellation, reopen, store deletion, publication failure and retry; OAuth refresh must touch neither the code nor the separately held S3 key. Invites still carry the recipient's S3 key and account providers still require sign-in. Differential: none for the code/custody contract; run `scripts/check.sh` in full under the shared lock. Dependencies: T29; coordinate shared `restore_codes.rs` and caller edits with T79.


**Snapshot publication, deletion and loading**

Execute these tasks serially where their files overlap; each is one concern and one commit. All five extend the IO runner introduced by T41 (`spec/proofs/io/IORunner.lean`, its package executable and `scripts/check.sh`) and compare the actual Rust decision path with executable Lean transitions. Do not reconstruct Rust logic inside the differential harness. Use `.writes` for normative selection; retain the Lean examples for all three proved orders as proof checks. No runtime implementation is included in this spec change.

**T94 — Record the catch-up storage time in the signed snapshot prefix.**
Spec: §15 publication, D2/D9/D14, SP retained prefixes. Files: `F/{sealed_snapshot,wire,test_utils}.rs`, snapshot wire fixtures and `F/sealed_snapshot_tests.rs`; `S/{snapshot_catalog,snapshot_seal,snapshot_write_input}.rs`; `D/{snapshot_metadata,snapshot_state}.rs`; IO runner/tests. Add `caught_at:StorageTime` (u64 Unix seconds, u32 nanoseconds) to the single snapshot prefix and its signature/length accounting. Capture the provider sample from before the publication catch-up, preserve its precision and fixed value on retry, and retain the first complete landing time separately. Remove prefix constructors that omit the sample; do not substitute device time or upload start/completion. Tests: exact fixture bytes, signature tampering, truncated field, invalid nanoseconds, overflow-safe expiry comparison, subsecond round-trip and equality/one-unit-late boundary without rounding forward. Differential: IO `Snapshot.counts` and `late_snapshot_ignored`, projecting exact representable integer-second samples for the model; codec, signature and subsecond checks remain Rust tests. Dependencies: T36, T41.

**T95 — Gate every snapshot request on completed catch-up and confirmed coverage.**
Spec: §15 publication, §17.1/§18.1 raise ordering, SP steps 5/6/8. Files: `S/{snapshots,snapshot_data,snapshot_catalog,snapshot_seal,schema_sync,device_log_sync}.rs`; `D/{snapshot_write,snapshot_writes,snapshot_metadata,upload}.rs`; IO runner/tests. Require a completed all-writer catch-up through next-number misses, no unresolved history, immediate preparation and dominance of the current counting selection at preparation. A reload invalidates earlier terminal misses. Confirm the captured own prefix before sending and derive remote positions from checked publication evidence; apply this to ordinary, raise and reset snapshots. Keep later app writes outside the captured cut. Preserve `T_c`, abandon expired attempts and prepare a new path after another catch-up; readers ignore late landings even from abandoned requests. Do not add a remote lock or assume bounded request duration. Tests: failed/partial catch-up, unseen writer, pending history, prepared-but-unconfirmed own cut, later queued tail, crash/retry, concurrent incomparable arrivals, exact one-day admission, late remote completion after cancellation and raise marker ordering. Differential: IO `SnapshotPublication.step/execute`, `completed_catchup_covers_old`, `SnapshotWrites` and its publication examples; compare captured/requested/landed states, counting selection and publication evidence. Dependencies: T42–T43, T69, T72, T94.

**T96 — Require mature historical coverage and current coverage for every part before deletion.**
Spec: §15 deletion, SP retained evidence and step 8. Files: `S/{snapshot_retention,snapshot_catalog,snapshot_boundaries}.rs`; `D/{snapshot_coverage,snapshot_metadata,internal_schema}.rs`; IO runner/tests. Replace any-snapshot coverage with current newest counting coverage for every part plus a historical counting witness for each part whose landing is strictly more than a day old. Preserve finality, active-reader positions or 30-day age, ownership and refusal gates. Retain checked historical witness evidence across supersession, body deletion and reopen; without evidence wait for a known witness. Do not restart its age when a fresh snapshot wins, or advance local deletion/reference state before provider confirmation. Tests: exact-day refusal, one-unit-later acceptance, different part witnesses, late noncounting snapshots, newer uncovered selection, witness removal/reopen, mixed finality, failed delete and idempotent retry. Differential: IO `Eligible`, `maturity_survives_new_snapshots`, `SnapshotHistory.deleted_stays_covered` and `retainWrites`, comparing candidates and confirmed deletions on concurrent histories. Durable evidence and provider failures also need Rust crash tests. Dependencies: T74, T95.

**T97 — Reload all readable audiences to one proved common point.**
Spec: §15 loading, §18.1, SP step 5. Files: `S/{snapshot_reload,snapshot_catalog,device_log_sync}.rs`; `D/{snapshot_reload,snapshot_load,snapshot_state,snapshot_writes,database_snapshots}.rs`; IO runner/tests. Freeze each writer's requirement from waiting remote past, uploaded own history and unchanged audience positions. Keep selections fixed until a certified deleted number forces that audience's replacement; compute the target from the frozen requirement and all selected positions. After an absent GET refresh the complete catalog, establish publication/deletion even through another audience's prefix, and reload if this audience's current snapshot reaches the number or skip it otherwise. Recompute the common target after replacement and fill every audience with retained real log bytes. Remove moving-tail completion, stale-catalog absence, independent audience commits, assumed suffix availability and unpublished reservations treated as deleted. Commit rows, merge records, losses, positions and eligible waiting writes together; the app reads the old view until commit, with existing boundary write gates preserved. Tests: the 2/1 replacement fills the other part through 2; another audience alone proves a deleted hole; empty/unchanged audience; concurrent app writes; failed/partial catalog, unavailable body/key, permanent refusal, transaction failure and reopen; new tails do not move selections. Differential: IO `FixedReload.scan`, `CommonReload.round/replace`, `committed_number`, `committed_safe`, `AtomicReload`, `replacement_fills_other_audience`; compare selections, targets, actual receipts, skips/retries and committed positions. Generate deletion cessation histories and assert at most A replacements and A+1 finite rounds afterward, without treating this as an elapsed-time guarantee. Dependencies: T45, T68–T69, T96.

**T98 — Request first and replacement coverage during every retention pass.**
Spec: §3 bounded storage, §15 retention refresh, SP steps 1/8. Files: `S/{snapshots,snapshot_retention,sync_loop}.rs`; `D/{snapshot_coverage,snapshot_metadata}.rs`; IO runner/tests and sync pass request traces. For each final write satisfying positions-or-30-day age, request a fresh snapshot in that or the next pass for every readable part audience whose current counting snapshot lacks coverage. Remove growth-only and historical-witness-only triggers. Use indexed due work and retained catalogs; unchanged waits obey shared backoff and idle passes do not list snapshots. Schedule the strict witness-age threshold using storage time. Tests: one old write below the growth threshold with no snapshot; superseded coverage; busy snapshots do not restart historical age; late attempts do not qualify; nonfinal/gated writes wait; offline circle retains a shared write while store coverage progresses. Differential: IO `RetentionDeadline.refreshDue`, `WitnessService`/`Service` executed schedules, `AudienceRetention.first_witness`, `audience_deadline`, `bounded_storage`, `first_snapshot_deleted` and `first_snapshot_deadline`. With explicit successful-service bounds compare `H_a`, `B_a`, `R_a` and `R + 3P + U + D`; exercise the historical-only trigger to reproduce `no_first_witness`, and retain `offlinePart` as the no-bound case. No bound is asserted without bounded successful catch-up/publication and deletion for every part. Dependencies: T44, T52, T73, T95–T96.
