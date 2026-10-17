# Audit 1, part A: coven.md rewrite against today's old spec

Sources: `spec/coven.md` and `spec/sync-pass.md` at HEAD of `spec-swap` (origin/main, d856e095c).
Target: `plans/spec-rewrite/coven.md`. Section numbers below are the rewrite's.

## Step 1: commits ported

`git log c07538c90..HEAD -- spec/coven.md` gives 12 commits; `git log b61e822d1..HEAD -- spec/sync-pass.md`
gives 2 (one shared). 13 distinct commits.

| Commit | What changed in the old spec | Where it now lives |
|---|---|---|
| d53b86355 Prevent deleted reload selections from moving to older snapshots | Floor's rules alone don't imply its snapshot supersedes a reload's selected announcement; `unchecked_reselection_goes_backwards` | §16.5 "The target is fixed…" paragraph: a reload switches only to a snapshot that replaces its selection, never an older one, with the reason and body citation; G.5 row |
| e3a9cfee2 Keep content-derived integers distinct from SQLite-chosen keys | A.1: SQLite-chosen keys (integer rowid aliases) refused; content-derived integer keys allowed | A.1 rewritten; Appendix A table row; §9.2 refusal list bullet |
| 84398a190 Schema additions must preserve compatible writes | No breaking migration in a write's range: no converter needed, changes apply unchanged; `newest_breaking_write_admitted`, `retained_step_required`; exclusion only if a migration in the range lacks a converter | §18.6 new paragraph after "What the raise's snapshot supplies" (with the §18.5 version-2/version-3 example); G.7 row. The "lacks a converter" wording was superseded by d856e095c (below) |
| 902a1ad7f Describe the shared sequence without obsolete log terminology | "store log" replaced by applied admin entries / applied membership; `NotInCircle` covers a circle absent from applied membership; join step "publishes its add device entry" | §8 two-layer list and §8.4 reason list now say "admin entries the device has applied"; §15.6 and H.1 `NotInCircle` wording; §13.2 step 5. The rewrite had no "store log" text; `grep 'store log\|admin history'` now finds nothing |
| 58399dfcd Keep key revocation covered by sequence proofs | `revocation` theorem; cut-off phrased as writes claimed after removal or signing-credential retirement; store-key exposure reveals headers, store data and admin contents but not circle parts or recipient boxes; usable key must be introduced, authorized, held and unexposed in the applied prefix; device removal alone doesn't retire the member's shared keys | §2.5 first and third bullets; §12 "Who can read what" paragraph; §12.2 "Choosing a key" list (judged on applied slots, introduced); §12.2 new paragraph on device removal; G.1 row |
| 844df5760 Distinguish device claim numbers from shared slots | Floor settlement compares journaled claim number c (not slot n) with L; snapshot keeps the greatest claim number, and on a tie (only with cloned ids) the later slot's digest | §5.3: claim number renamed c, sentence saying it's claim numbers not slot numbers; §16.1 tie rule. (sync-pass.md's same change is the same rule.) |
| b5a458491 Describe SeaweedFS publication by conditional copy | SeaweedFS dropped from the "refused occupied names sequentially, so conditional completion" list | E.4 "Why each profile…": separate bullets for RustFS/Ceph RGW, SeaweedFS 4.48, and the no-checksum fact |
| d3322e20c Keep device resets from stranding invite grants | Reset carries `DeleteAccessKey`, owed account revocations and open invites (as owed cleanup) into the new database with its notice; inviting device's reset ends its invites like expiry, keeping active members' access | §19.3 carry-over bullet ("only owed provider cleanup carries over"); §13.2 "Ending an invite" new paragraph |
| b64119466 Prevent concurrent stores from sharing a bucket | `coven-bucket` root marker: listing allows it, then first setup creates it create-once with the 36-byte store id; equal bytes = retry, others refuse; reconnect/restore/join read it exactly and refuse missing/malformed/other; never replaced or deleted, survives `delete_store`/`discard_store`; `at_most_one_store`; path exception; setup's wider reads | §20.7 own-bucket list and a new "Why both checks?" paragraph (removed the old "two stores within the margin can both pass" sentence and "reconnect checks nothing outside the folder"); §20.3 path exception and wider-access list; H.4 `OwnBucketRequired { reason: Occupied }` row; G.8 row. Links to format.md `#the-bucket-marker` |
| c5fb89879 Avoid leaving failed mint principals after device retirement | Reclaim listing includes keyless IAM users and GCS service accounts with coven naming; removed without a deletion record; failed removal fails the call, absence succeeds on retry | §14.3 "Keys nobody recorded" new paragraph; E.12 "Listing a store's keys" note |
| 0954b0062 Queue preservation does not imply commit equivalence | §3: checks never wait for a reload or check its future state, and a preserved original doesn't keep its verdict; `reloading_blocks_commit` citation removed; self-replacement counterexamples; app commits start against installed state; reload counterexamples replace `ordinary_commit_allowed`/`affected_commit_refused`; self-replacement releases writes after custody and code | §2.1 second paragraph; §12.3 (pause lifts after step 5; new paragraph with body citation); §13.4 "Unfinished installations"; §16.5 new "Keeping a write's bytes isn't keeping its outcome" list with body citation; §11.5 table row; G.1 row for `reloading_blocks_commit` removed; G.5 row for `ordinary_commit_allowed`/`affected_commit_refused` replaced by the counterexample row; G.4 row for self-replacement |
| d856e095c Make the old-write choice part of the migration contract | App declares `addition`/`breaking` (api.md E13); breaking requires `OldWrites::Convert(f)` or `OldWrites::Drop`; final-schema and declared-kind check, `DbError::MigrationKindMismatch { version }`; local-only accepts `addition`; reopen checks historical declarations; three theorems; "without a converter" becomes "declares `Drop`"; theorem renamed to `explicit_drop_preserves_values` | §18.3 "What the app declares" and "Running migrations" (with reason and body citation); §18.1 closing sentence; §18.6 opening, "When a migration declares `Drop`", example bullets; §9.5 list; H.1 row; G.7 rows (renamed theorem, new body row) |
| 8dd191f27 Keep pending conditions local to their subject | sync-pass.md anchors renamed only (`apply-in-order`, `effects-after-applying`) | nothing to port: the rewrite replaces sync-pass.md and links none of its anchors |

## Step 2: completeness against today's old spec

Read both old files completely and compared by hand; then mechanical checks (scripts outside the repo).

**Inline code spans.** 540 distinct spans in the old files. Before fixes, 44 were missing as code spans from
the rewrite (32 missing even as text). All content misses came from the commits above and are fixed. Also
fixed: `first - 1` and `max(0, min(T - now, W))` used a Unicode minus (now ASCII); `δ`, `δ = 1 second`,
`δ = 10^(-p) seconds`, `δ = 1 millisecond` added to §20.5 and E.11; `notLanded` added to the G.2
`lands_once_never_lost` row; `M` added as the listing margin's symbol in §20.4. Remaining 15 are wording or
formatting equivalents, not misses: `(device, number)` (§4 "identity, device and number"),
`<store>/seq/<applied+1>` (§6 `seq/<last applied + 1>`), `read_through = 0/20/21` (written "`read_through` 0"
etc.), the certification command (present, wrapped across a line inside one span), `(slot, write number)`,
`<store>/.coven-check/`, `reset_device` (as `CovenBuilder::reset_device`), `todo_id`/`todo_labels` (in a code
block), `member <member or invite id>` (inside a longer span), `and` (regex artifact), `M` (see contradictions).

**Identifiers, acronyms, CamelCase, snake_case.** After fixes the only old token absent is "UX" (§13.1 says
"up to the app").

**Numbers.** Every number in the old files appears, except renumbered § numbers and "300 seconds" (written
"then 300" in §6.7).

**Proof files.** Old files cited four files the rewrite lacked (`Keys.lean`, `QueueReloadExamples.lean`,
`ReloadExamples.lean`, `BucketClaim.lean`); all now cited. Every theorem name cited in the old files exists in
`spec/proofs` and appears in the rewrite. `missing_converter_preserves_values` no longer exists in the proofs
and is gone from the rewrite.

**Hand-found misses fixed (not tied to a commit):**
- §20.7: how each prefix-scoped service limits a key (AWS IAM `s3:prefix` conditions, GCS managed folder per
  store, the others policy `s3:prefix` conditions).
- §9.1 "Parents apply before children": the old text says the parent's insert is *consumed* first and both
  are applied in order only when both count; the rewrite said "applies" unconditionally.
- §9.2 `UuidText` link pointed at `api.md` with no anchor; now `#e3-writing`.

**Citations in the rewrite that the old spec doesn't make.** These came from the earlier rewrite (proof
write-ups) and name real theorems: `conclude_correct`, `exactly_one_claimant`, `first_slot_wins`,
`owner_stays`, `merge_converges`, `device_converges`, `rule_order_converges`, `retained_iff`,
`derived_root_authorized`, `no_forged_effect`, `member_without_active_device_can_register`. Left in place.

## Contradictions in the old text

- **`M` has two meanings.** `spec/coven.md` uses M for the listing margin (lines 402, 645, 1016) but line 874
  says "Count metadata requests and redirects in `M`". The rewrite defines M as the listing margin (§20.4) and
  keeps the counting fact in §21.1 ("Extra requests are counted too", including OneDrive's metadata fetches and
  redirects) without the symbol.
- **SeaweedFS's sequential refusal.** After b5a458491, old line 798 no longer says SeaweedFS refuses occupied
  names sequentially, but the next sentence still says "That sequential refusal does not hold under concurrency
  on SeaweedFS 4.48", and line 965 still lists SeaweedFS among services whose timestamps passed. The rewrite
  keeps both: E.4 says SeaweedFS refuses one request at a time but not under concurrency (as §20.7 already
  did).
- **Dropped citations.** 0954b0062 removed the old spec's citations of `reloading_blocks_commit`,
  `ordinary_commit_allowed` and `affected_commit_refused`; the theorems still exist in the proofs. The rewrite
  follows the old spec and no longer cites them. The coordinator may want them back in Appendix G as plain
  model facts.

## Step 3: consistency

- Every `§n`, `§n.m`, range, `Appendix X`, `Appendix X.n` and bare `X.n` reference resolves to a heading
  (script check: 0 unresolved).
- Every `api.md#…` link resolves in `spec/api.md` (e1, e2, e3, e4, e5, e8, e9, e10, e11, e13).
- Every `format.md#…` link resolves in `plans/spec-rewrite/format.md` as it stands now: d1, d3, d6, d7,
  d8-small-frames, d9, d10, d11, d12, the-bucket-marker. Watch these two, which depend on the other agent:
  `#d8-small-frames` resolves only through an explicit `<a id>` (the heading is now "D8 Floor and join
  frames"), and `#the-bucket-marker` is a sub-heading of D10.
- Appendix G rows are kept in section order (moved the new G.1, G.4 and G.8 rows into place).

## Couldn't place / open

- E.4 "SeaweedFS refuses one request at a time" rests on the old spec's wording, not a stated
  measurement; see the contradiction above.
- The A.1 wording "an integer key derived from the row's content is allowed" repeats the old one line. The old
  spec doesn't say how coven tells such a key apart from a rowid alias; I didn't add a mechanism.
- Not checked: whether `plans/spec-rewrite/format.md` itself agrees with the rewrite's new text (bucket
  marker bytes, claim-number tie rule); that's the other agent's file.
