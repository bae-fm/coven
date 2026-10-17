# Audit 1B: format.md rewrite against spec/format.md at HEAD

Branch `spec-swap` (at origin/main, d856e095c). Edited only `plans/spec-rewrite/format.md`.

## Step 1: commits since 7f4a3750d that changed spec/format.md

`git log --oneline 7f4a3750d..HEAD -- spec/format.md` lists four commits. All four landed after or around the
rewrite's base (9badadebb, 13:33); none was fully reflected.

| Commit | What changed in spec/format.md | Where it lives in the rewrite |
| --- | --- | --- |
| 902a1ad7f (15:15) Describe the shared sequence without obsolete log terminology | Removed the `d6-store-log-entries` anchor; dropped "it has no log ends" from device replacement; dropped "No per-device log or key-copy path exists." | Anchor and "log ends" were already absent. Removed the D10 Rules bullet "No per-device log or key-copy path exists." No other "log" wording remains in the rewrite. Nothing in the repo links to `d6-store-log-entries`. |
| d895c1f6b (13:41) Keep frozen display values independent of live parents | An excluded row's inserts and updates keep their new *scalar* values (no parents); "Original parent evidence is retained separately"; "All frozen values have empty parent maps" (was "Migration-frozen"). | D7.2 Generations: "new scalar values", plus "The original parent evidence is retained separately: the parents the change named survive in its original row change, which section 5 keeps (D7.3)." D7.2 Frozen: "All frozen values have empty parent maps, whatever froze them." (The rewrite had already half-adopted this via the code audit; the "Migration-frozen" bullet still had the old meaning.) |
| 844df5760 (15:10) Distinguish device claim numbers from shared slots | Section 11 records the *greatest* claim number; a tie is possible only with cloned device ids, and keeps the later slot's digest. (Its coven.md/api.md/sync-pass.md side renames the compared number to c.) | D7.7: "greatest" was already there; added "that can happen only when two copies share a device id (§19.3)". Added an opening sentence saying how a device compares its recorded claim number with section 11's (lower / equal with its digest / higher or other digest), in claim-number terms, so the section's reason is stated. |
| b64119466 (17:27) Prevent concurrent stores from sharing a bucket | New unsealed bucket-root marker `coven-bucket`: D1 exception to the `kind | version` rule; D10 table row; its bytes (36 ASCII bytes of the owning store id, no header, newline, encryption or signature), no store prefix, never replaced or deleted (also not by local store deletion), only first setup creates it, reconnect/restore/join need an exact match; "bound into its *encrypted* object's authentication"; "reserves ownership only: bootstrap discovers store creation at slot 1 or in the floor's store snapshot" (replacing "there is no separate creation marker"). | Tour table: new "Bucket marker" row. Three layers: files, codes and the bucket marker are the three exceptions. Kind numbers: "except the bucket marker". D1: exception stated. D10: intro now says only encrypted objects bind their path; table row; Spellings bullet says "encrypted object's"; Rules bullet rewritten; new subsection "The bucket marker" (problem: listing margin lets two setups pass on a bucket-scoped service; example: Household's marker bytes in hex, Garden refused with `OwnBucketRequired { reason: Occupied }`, Household's retry proceeds; rules). |

Format fixes that touched code only (no spec/format.md diff), checked against the rewrite:

| Commit | Change | Rewrite |
| --- | --- | --- |
| 32a989ae1 Keep floor ciphertext independent across sealings (F-D-3) | Floors are sealed with fresh random nonces, not the slot recipe | Already said (D9 Nonces, D11 "does not apply to … floors"). Added to D9 Nonces: each new sealing of a floor draws fresh nonces, so two sealings differ and both authenticate. |
| f33705229 Distinguish creation order from value timestamps (F-R-3) | Creation selected by lowest slot, then write number | Already in D7.4. |
| f38388a67 Hide overwritten cells after snapshot reloads (F-D-1) | Overwritten values never opened | Already in D7.5. |
| 29c5a8d71, d3cb01a29 Bound reads before allocating | Lengths checked before allocation | Already in D1, D5.11, D9 Chunking. |
| f859b09f4 Pin deliveries and settlement across every slot kind | Fixtures | Already in D14. |

## Step 2: completeness against spec/format.md at HEAD

Read the whole old file and compared it rule by rule with the rewrite. Every rule, layout, field, kind number,
tag, limit, example and proof citation is present with the same meaning after the Step 1 ports.

Mechanical check (script: every inline code span, every code-block line, every number, and every identifier with
`_`, CamelCase or `::` of the old file, searched in the rewrite with `\|` unescaped and whitespace collapsed):
the only misses are `161`, `162`, `171`, `201`, `85`. They are old coven.md section numbers inside link slugs
(`#161-…`, `#162-…`, `#171-…`, `#201-crates`, `#85-…`), renumbered in the rewrite (§17.4, §17.2, §18.x, F.1, §9.2).

Proof citations: `conversions_preserve_uploads`, `receive_losses_converge`, `conversions_preserve_untried`,
`receive_agreement`, `action_iff`, `follows_cause`, merge write-up B2 and B17 are all in "What's proved". The new
`at_most_one_store` proof (b64119466) is cited from coven.md, not format.md, and stays there.

Differences that are not contradictions (the rewrite is more precise, from the fafe66303 code audit):

- D2 `S3Principal` tag 1 lists Versity Gateway beside AWS, Ceph RGW and SeaweedFS; the old text omits it. The code
  mints Versity keys as IAM users (`crates/coven-storage/src/providers/s3_iam.rs`), so the rewrite is right and the
  old text is incomplete.
- D7.3: old says section 5 holds "the D5 header of its part"; the rewrite gives the exact 45-byte `WriteHeader`
  (D5.1 fields without `parts`).
- D5.8: the rewrite adds that a receiver doesn't check that positions start at 0 or run without gaps.

## Contradictions

None between the old spec and the rewrite. Two between the rewrite's format.md and the rewrite's coven.md (not my
file; for the coven.md agent):

- coven.md §20.3 says "Every object path starts with the store's id". D10 now has `coven-bucket`, with no store
  prefix (old spec/coven.md now says "Apart from the own-bucket marker `coven-bucket`, every storage path starts with
  the store's id").
- coven.md §20.7 ("A member key's reach, and a bucket of its own") says reconnecting, restoring and joining don't
  check for objects outside the store's folder, and that two stores created within the listing margin can both
  pass. D10 (and old spec/coven.md after b64119466) say they require an exact read of `coven-bucket`, and that the
  marker lets at most one store's setup pass. coven.md §20.7 has no `coven-bucket` text yet.

## Step 3: references

- Every § reference in the rewrite (§4 … §20.7) names a heading in `plans/spec-rewrite/coven.md`, and every
  `coven.md#…` link resolves by GitHub slug: `51-claiming-a-slot`, `53-settling-a-claim-after-its-slot-is-gone`,
  `72-timestamps`, `92-keys-and-unique-values`, `164-the-floor`, `172-attaching-a-file`, `174-a-files-status`,
  `189-covens-own-schema-and-format`, `20-what-coven-needs-from-a-provider`, `f1-crates`. Appendix B.1, B.3, B.5,
  C.1, C.2 and F.1 exist. Internal `#…` links, `proofs/merge.md#b2-terms`, `#b17-…`, `api.md#e4-reading` and every
  cited `.lean` file resolve. Checked against coven.md as it stood during this audit (unmodified in the worktree);
  the other agent's later edits can still break these.
- Anchors other files link to: `spec/coven.md`, `spec/proofs/merge.md`, `spec/proofs/sequence.md` and the rewrite's
  coven.md link `d1-rules-for-every-object`, `d3-ordered-keys`, `d6-admin-entries`, `d7-snapshots`,
  `d8-small-frames` (kept as an explicit anchor), `d9-sealed-objects`, `d10-paths`, `d11-keys-and-contexts`,
  `d12-files`. All exist in the rewrite. None needs renaming.

## Open items

- The coven.md agent should add the bucket marker to §20.3 and §20.7 (see Contradictions); D10 links there by
  §20.7.
- coven.md §5.3's table calls the recorded claim number `n`, which reads like a slot number; the old spec now says
  `c` (844df5760). D7.7 avoids the letter.
- Name clash once the rewrite replaces the old files: this document's sections are D1 to D14, and the rewrite's
  coven.md has its own "Appendix D. Cryptography and keys reference" with D.1 to D.6. In the old layout, format.md
  *was* coven's Appendix D. References to "Appendix D" elsewhere (code comments, api.md) should be checked when the
  swap lands.
- D14 lists no fixture for the bucket marker; neither does the old spec.
