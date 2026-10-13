# Format conformance audit

Decisions: preserve the existing handling of version zero, the invite storage bound, and the exhausted invite cursor because the specification does not settle those cases. Do not invent encodings for queued member identity, source evidence, losses or snapshot time changes. Cross-store report paths return the typed Invalid/Store error at the authenticated reader; this rejects the bad context without changing bytes or the queued refusal-routing policy. The Lean storage model treats object bytes symbolically; the byte-level fixes use Rust regressions.

Source baseline: `d1932a1e21d320998aef4fe1a14ef61a79567801`. Read all of `spec/format.md` and every source file in `crates/coven-format/src`.

Paths below abbreviate `crates/coven-format/src` as F, `crates/coven-crypto/src` as K, `crates/coven-database/src` as D, and `crates/coven-sync/src` as S. Tests beside a differing row demonstrate the existing representation, not conformance. “Untested” on a policy row means the codec has no such decision and this audit does not certify the end-to-end policy; the named queue task owns that work. Repeated field layouts share a row where the same encoder and fixture check the whole layout.

| Statement (section + short quote) | Code (file:line) | Test (file:line) | Verdict |
| --- | --- | --- | --- |
| D intro “format 1”; no older format | F/version.rs:8 | F/version_tests.rs:5 | matches for version selection; member layouts differ below (T104) |
| D intro local tables “own encodings” | F/merge_fields.rs:1 | F/merge_fields_tests.rs:31 | matches; local metadata is outside object framing |
| D1 “Integers are big-endian”, fixed width, signed two's complement | F/wire.rs:85 | F/wire_tests.rs:74 | matches |
| D1 clear “kind:u8 … version:u16”; unknown kind refused, newer format requires update | F/lib.rs:119; F/version.rs:25 | F/lib_tests.rs:192; F/version_tests.rs:33 | matches |
| D1 newer-version rule: version zero is unspecified | F/version.rs:25 | F/version_tests.rs:33 | differs: version `0` returns `UnsupportedVersion(0)`; preserved |
| D1 “length … checked … before anything is allocated”; no trailing bytes | F/wire.rs:58; F/wire.rs:185; F/wire.rs:254 | F/lib_tests.rs:219; F/wire_tests.rs:97; F/lib_tests.rs:192 | matches |
| D1 “Every decoded value is checked”; canonical re-encoding | F/lib.rs:88; F/wire.rs:206 | F/lib_tests.rs:163 | matches for implemented layouts; differing layouts itemized below |
| D1 maps/sets increasing, “no duplicates” | F/wire.rs:154; F/wire.rs:206 | F/wire_tests.rs:31 | matches |
| D1 frame prefix and “at most 16 MiB in all” | F/lib.rs:119 | F/wire_tests.rs:150 | matches |
| D2 bytes: u32 length, “At most 8 MiB” | F/wire.rs:248 | F/wire_tests.rs:97 | matches |
| D2 text: UTF-8, “Not normalized”, empty/NUL allowed | F/wire.rs:267 | F/wire_tests.rs:136; F/lib_tests.rs:243 | matches |
| D2 name: “1 to 1,024 bytes, no NUL” | F/value.rs:293; F/wire.rs:235 | F/store_log_tests.rs:52; F/wire_tests.rs:65 | matches |
| D2 list/map/set u32 counts; 65,536 per collection and per frame | F/wire.rs:28; F/wire.rs:64; F/wire.rs:185 | F/wire_tests.rs:4 | matches |
| D2 Option: “0, or 1 … T” | F/wire.rs:105 | F/wire_tests.rs:74; F/store_log_tests.rs:87 | matches |
| D2 UUID 16-byte order; DeviceId u64 | F/value.rs:18 | F/value_tests.rs:10; F/clock_tests.rs:192 | matches for store/circle/invite/key/file/device |
| D2 MemberId “uuid … independent of keys” | F/value.rs:31; K/member.rs:12 | F/value_tests.rs:24 | differs — T104: one member occupies 32 signing-key bytes, not 16 UUID bytes |
| D2 SigningKey 32 bytes; weak/invalid points refused; SealingKey 32 bytes | F/value.rs:31; K/member.rs:16; F/value.rs:40 | F/value_tests.rs:182; K/member_tests.rs:132 | matches for key bytes and validation; separate identity type belongs to T104 |
| D2 Timestamp u48/u16/u64, byte order equals timestamp order | F/value.rs:55 | F/value_tests.rs:43 | matches |
| D2 StorageTime u64/u32, nanos <=999,999,999, no rounding forward | F/sealed_snapshot.rs:16 | — | unimplemented — T94: no storage-time field |
| D2 WriteId/EntryId positive number; positions sorted by device, omit zero | F/value.rs:100; F/value.rs:118 | F/value_tests.rs:68; F/write_tests.rs:169; F/store_log_tests.rs:372 | matches |
| D2 Audience tags 0/1, store before UUID circles | F/value.rs:68 | F/objects_tests.rs:28; F/write_tests.rs:147 | matches |
| D2 Value tags 0–4; integer and integral real distinct; NaN/negative zero refused; infinities valid | F/value.rs:237 | F/value_tests.rs:143; F/value_tests.rs:211 | matches |
| D2 RowId table/key/audience, logical order; ordered key checked | F/value.rs:274 | F/merge_fields_tests.rs:31; F/write_tests.rs:147; F/lib_tests.rs:243 | matches |
| D2 Parent row/generation; present odd generation | F/merge_wire.rs:11; F/write.rs:153; F/snapshot_rows.rs:19 | F/write_tests.rs:169; F/snapshot_tests.rs:202 | matches in storage objects; local metadata codecs are outside D |
| D2 Columns declaration order; ForeignKey nonempty/equal lists and full identity | F/merge_wire.rs:9; F/merge_wire.rs:121 | F/merge_fields_tests.rs:272 | matches |
| D2 Unique nonempty text terms, optional text predicate; full logical order, absent first | F/merge_wire.rs:10; F/merge_wire.rs:148 | F/merge_fields_tests.rs:336; F/merge_fields_tests.rs:401; F/merge_fields_tests.rs:439 | matches |
| D2 ColumnValue value/parent map; MemberKeys signing/sealing; MemberAccess tags 0/1 text | F/merge_wire.rs:12; F/store_log.rs:16; F/member_access.rs:19 | F/merge_fields_tests.rs:31; F/lib_tests.rs:29 | matches for field bytes; identity separation differs under T104 |
| D3 numeric/text/blob order; binary text; prefix sorts first | F/key.rs:12 | F/key_tests.rs:18; F/key_tests.rs:91 | matches |
| D3 “1 to 65,536 non-NULL components”; independent from frame count | F/key.rs:12; F/key.rs:89 | F/key_tests.rs:126; F/key_tests.rs:192 | matches |
| D3 “at most 8 MiB of encoded bytes” including escapes/terminators | F/key.rs:12; F/key.rs:89 | F/key_tests.rs:215 | matches |
| D3 numeric tags 10–14, complemented negatives, normalized exact integers, canonical finite doubles | F/key.rs:62; F/key.rs:145 | F/key_tests.rs:70; F/key_tests.rs:126; F/key_tests.rs:151 | matches |
| D3 text/blob 20/30, 00→00FF and 0000 terminator; `(7,"a")` example | F/key.rs:75 | F/key_tests.rs:180 | matches |
| D4 frame kinds 1–12; kind 7 empty | F/lib.rs:88; F/snapshot.rs:292 | F/lib_tests.rs:29; F/snapshot_tests.rs:154 | matches for kind assignments; payload differences below |
| D5 header position/timestamp/read sets/schema/parts field order | F/write.rs:14; F/write_stream.rs:40 | F/lib_tests.rs:29; F/write_tests.rs:169 | matches except disposition below |
| D5 schema zero representable; own store-log entries allowed; own earlier writes implicit | F/write.rs:29; F/value.rs:158 | F/write_stream_tests.rs:227; F/value_tests.rs:115 | matches |
| D5 “0 apply · 2 migration”; exclusion never transmitted | F/write.rs:56 | F/write_tests.rs:193 | differs — T69: `Lost(1)` writes tag 1 plus a version |
| D5 originals survive conversion; no inherited exclusion; mixed converted/frozen changes | F/write.rs:56; F/write.rs:179 | — | unimplemented — T69/T100/T102: a part has rows and dismissals, no received per-change interpretation |
| D5 parts increasing audience, nonempty, counts include dismissals; migration alone has no parts | F/write_stream.rs:64; F/write.rs:239 | F/write_tests.rs:147; F/write_tests.rs:211; F/dismissal_tests.rs:19 | matches |
| D5 first publication retained; inclusive 30-day retirement window; too-late consumed; atomic replay/finality | F/write.rs:14 (metadata boundary) | — | untested here — T49/T103; provider time is correctly absent from the frame |
| D5 recorded-view signer; no extra author/key; unread key replacements impose inclusive deadline | F/write.rs:14; K/member.rs:40 | — | unimplemented — T104/T105 for stable-owner/key-history authentication |
| D5 records ordered by RowId and part audience; length includes seven-byte prefixes | F/write_stream.rs:90; F/write_stream.rs:206 | F/write_stream_tests.rs:112; F/dismissal_tests.rs:19 | matches |
| D5 counts/stream lengths only u64 bound; no surrounding count; frame bounds retained | F/write_stream.rs:13; F/write_stream.rs:206 | F/write_stream_tests.rs:25; F/write_stream_tests.rs:51; F/write_stream_tests.rs:227 | matches |
| D5 RowChange fields; insert/update/delete tags; delete omits columns; old keys follow operation | F/write.rs:97; F/write.rs:126 | F/write_tests.rs:51; F/lib_tests.rs:29 | matches |
| D5 generation parity/overflow and parent generation/audience checks | F/write.rs:153 | F/write_tests.rs:51; F/write_tests.rs:111; F/write_tests.rs:169 | matches |
| D5 authenticated generation/setter sources, read/time/incarnation checks, wait/refusal | F/write.rs:126 | — | unimplemented — T99/T100: same old scalar and generation cannot distinguish two setters |
| D5 preserve original identities/operation/names; failed roles generation-first, repeated IDs; empty delete retained | F/write.rs:126; F/loss.rs:100 | — | unimplemented — T100/T101/T102: no dependency-source cause encoding |
| D5 each whole row change is loss unit; frozen originals skip callbacks/rules; evidence independent of dismissal/positions | F/write.rs:179; F/loss.rs:13 | — | unimplemented — T100/T101/T102/T103 |
| D5 dismissal row/name/write bytes; removed-row dismissal is ordinary delete | F/dismissal.rs:14; F/write.rs:97 | F/dismissal_tests.rs:19; F/write_tests.rs:51 | matches for encoded forms; app routing not audited |
| D5 one change per row, then column/write ordered unique dismissals; dismissed write was read | F/write.rs:188; F/dismissal.rs:38; F/dismissal.rs:87 | F/dismissal_tests.rs:49; F/dismissal_tests.rs:76; F/write_tests.rs:147 | matches |
| D5 counts/lengths exact; frame beyond part refused before allocation | F/write_stream.rs:206; F/chunks.rs:95 | F/write_stream_tests.rs:112; F/chunks_tests.rs:28 | matches |
| D6 EntryId/timestamp/author/other-device reads/change | F/store_log.rs:262 | F/lib_tests.rs:29; F/store_log_tests.rs:323; F/store_log_tests.rs:372 | differs — T104: author is 32 bytes; other fields match |
| D6 first complete publication, fixed retry bytes/time; permanent any-unread-entry lateness; consume dropped IDs | F/store_log.rs:262 (metadata boundary) | — | untested here — T21/T22; provider time is absent from the frame |
| D6 tags 0–11,13–15 retain assignments; 12 unused | F/store_log.rs:63 | F/store_log_tests.rs:234; F/store_log_tests.rs:303 | matches for assignments |
| D6 tag 0 store/name/admin pair/access/device name/key/hash, no duplicated member id | F/store_log.rs:63 | F/store_log_tests.rs:161; F/store_log_tests.rs:279 | differs — T104: header author duplicates the admin signing key |
| D6 tag 1 explicit member plus keys/role/access | F/store_log.rs:63 | F/lib_tests.rs:29 | differs — T104: tag 1 omits the stable member UUID |
| D6 tags 2/3/9/10 member targets; tag 16 replaces only member keys | F/store_log.rs:63 | F/store_log_tests.rs:124; F/store_log_tests.rs:234 | differs — T104: 32-byte member targets; tag 16 absent |
| D6 tags 4/5 device/name/optional replacement and removal target | F/store_log.rs:63 | F/store_log_tests.rs:5; F/store_log_tests.rs:26; F/store_log_tests.rs:87 | matches |
| D6 tags 6/7/8 circle creation/rename/delete; no member list | F/store_log.rs:63 | F/store_log_tests.rs:124; F/store_log_tests.rs:161; F/store_log_tests.rs:234 | matches |
| D6 tags 11/13 version/snapshot and reset snapshot; raised version >=1 | F/store_log.rs:183 | F/store_log_tests.rs:347 | matches |
| D6 tags 14/15 access and audience/key/hash; no membership payload on rotation | F/store_log.rs:63 | F/store_log_tests.rs:161; F/store_log_tests.rs:234 | matches |
| D6 role 0/1; SnapshotId audience/device/positive number; timestamp writer matches | F/store_log.rs:26; F/store_log.rs:51; F/store_log.rs:276 | F/lib_tests.rs:29; F/store_log_tests.rs:347; F/store_log_tests.rs:372 | matches |
| D6 creation number 1, no reads, device is writer | F/store_log.rs:276 | F/store_log_tests.rs:279; F/store_log_tests.rs:372 | matches |
| D6 creation supplied signer/path store; later entry signer from recorded past | F/store_log.rs:276; K/member.rs:40 | — | unimplemented — T104/T105 for specified stable-id/key binding; path policy not certified here |
| D6 new keys only creation/addition/replacement; no reuse; target in both views; admin/self/time/removal conflict order | F/store_log.rs:63 | — | unimplemented — T104/T105: no replacement variant |
| D6 replacement older/same member/distinct; concurrent replacements keep both new IDs; entry/write admission | F/store_log.rs:183 | F/store_log_tests.rs:26 | untested here — T48/T49/T105; distinct-ID codec check matches |
| D6 set-access author; hash encrypted and creation/rotation only; UUID key IDs have no succession; key-free removals | F/store_log.rs:63; F/sealed_single.rs:31 | F/store_log_tests.rs:124; F/store_log_tests.rs:161 | matches for fields; authority/provenance policies T28/T32 |
| D6 rotation recorded audience membership, outside admin refused, concurrent rotations coexist | S/effects.rs:54; S/effects.rs:296 | S/effects_tests.rs:250; S/effects_tests.rs:278; S/effects_tests.rs:298 | matches for replay authority/coexistence; durable publication remains T32 |
| D7 header/id/schema/five u64 counts; positions only in sealed prefix; ordered header/records/end | F/snapshot.rs:28; F/snapshot.rs:190 | F/snapshot_tests.rs:39; F/snapshot_tests.rs:154 | matches |
| D7 section 0 RowId/columns, nonempty; section 1 write/timestamp/past; section 2 table/column | F/snapshot_rows.rs:11; F/snapshot.rs:54 | F/snapshot_tests.rs:39; F/snapshot_tests.rs:202; F/snapshot_tests.rs:420 | matches |
| D7 section 3 row/generation map/cell map; Cell setter/value; logical section order | F/snapshot_rows.rs:71; F/merge_wire.rs:13 | F/merge_fields_tests.rs:31; F/snapshot_tests.rs:252 | matches |
| D7 all losses in section 4; row/generation/bool/values/cause | F/loss.rs:26; F/snapshot.rs:102 | F/loss_tests.rs:25 | matches for outer layout |
| D7 values tags 0 cell/1 row; retain written values; dismissed cells absent, empty dismissed row removed | F/loss.rs:58 | F/loss_tests.rs:25; F/loss_tests.rs:53 | matches for cell/row codec; dismissal persistence T71/T101 not certified here |
| D7 cause 0 replacing/1 rules/2 write+version; “Resets create no loss records” | F/loss.rs:114; F/snapshot_rows.rs:105 | F/merge_fields_tests.rs:378 | differs — T68: schema cause adds nested tag 0; nested tag 1 represents reset loss |
| D7 cause/value pairing; cell/removed generations positive odd; nonempty removed cells/rules | F/loss.rs:210 | F/loss_tests.rs:53 | matches |
| D7 excluded values setter equals excluded write; migration-deleted values keep original setters | F/loss.rs:210 | F/loss_tests.rs:53 | differs — T70/T71: migration write 2 with a cell set by write 1 is rejected |
| D7 excluded generation/new values/parents, deleted old scalars; empty deletion identity retained | F/loss.rs:210; F/loss.rs:182 | F/loss_tests.rs:25; F/loss_tests.rs:53 | matches for representable excluded rows; original operation/source evidence T101 |
| D7 frozen schema loss; migration-frozen values have no live parents; frozen losses need no live schema | F/loss.rs:210 | F/loss_tests.rs:53; F/snapshot_tests.rs:39 | matches for codec constraints; schema freeze lifecycle T71 |
| D7 schema version positive and <= snapshot version | F/snapshot_rows.rs:112; F/snapshot.rs:120 | F/merge_fields_tests.rs:378; F/loss_tests.rs:53 | matches |
| D7 identity row/generation/values identity/frozen/cause/schema write; duplicates refused | F/loss.rs:182; F/snapshot.rs:190 | F/loss_tests.rs:163 | matches for existing cause representation |
| D7 Rule 0 FK/1 CHECK/3 other audience/4 Unique; logical ordering; circle-only rules | F/merge_wire.rs:16; F/merge_wire.rs:97 | F/merge_fields_tests.rs:136; F/merge_fields_tests.rs:439; F/loss_tests.rs:193 | matches for these tags |
| D7 deleted-circle Rule “2 … entry:EntryId”, kept and covered, no companion row-delete write | F/merge_wire.rs:16 | F/loss_tests.rs:193 | differs — T23: tag 2 has no entry ID |
| D7 merge contiguous generations, transitions/timestamps, deleted cells absent, parent checks | F/snapshot_rows.rs:81 | F/snapshot_tests.rs:252 | matches |
| D7 active losses match live row/history; concurrent replacer; uniqueness; synced iff present/unremoved | D/snapshot_state.rs:189; D/snapshot_state.rs:251 | D/snapshot_state_tests.rs:22 | matches for checked active-loss relationships |
| D7 migration-deleted row retains advanced even generation | F/snapshot_rows.rs:71 | — | untested here — T70; even deleted state is representable |
| D7 all rows/header audience matches prefix; all named writes and reads covered | F/snapshot.rs:120; F/loss.rs:281 | F/snapshot_tests.rs:102; F/snapshot_tests.rs:202 | matches |
| D7 consumed includes excluded/reset-ignored; reset uses store_log_read; rejoin loads skipped parts | F/snapshot.rs:15 (policy boundary) | — | untested here — T34/T68/T97/T101 |
| D7 incorrect order, duplicate/count/audience/coverage refused; no records after marker | F/snapshot.rs:190; F/snapshot.rs:292 | F/snapshot_tests.rs:70; F/snapshot_tests.rs:102; F/snapshot_tests.rs:154 | matches |
| D7 “EOF without the end marker is truncation” | F/snapshot.rs:311 | F/snapshot_tests.rs:175 | matches after error-mapping fix |
| D7 no local IDs/uploads/operations/paths or pending_entries in snapshot | F/snapshot.rs:54; F/loss.rs:26 | F/loss_tests.rs:25; F/lib_tests.rs:29 | matches for encoded fields |
| D7 retained undo inputs survive snapshot; original sources/admission facts independent of losses/positions; repeated dependency roles | F/snapshot.rs:54; F/loss.rs:100 | — | unimplemented — T25/T101/T103: five-section records carry no such evidence |
| D8 positions device/writes/store_log/schema/fingerprints/pending; ordered audiences store first | F/objects.rs:51; F/objects.rs:77 | F/objects_tests.rs:7; F/objects_tests.rs:28 | matches |
| D8 causally closed uploaded past; include dropped entries; pending can change without past/fingerprints | F/objects.rs:51 (policy boundary) | F/objects_tests.rs:7 | untested here — T53; omission of fingerprints is encoded |
| D8 not finality/discovery acknowledgements; changed-post T, T−one hour−δ; underflow; unchanged posts not probes | F/objects.rs:51 (policy boundary) | — | untested here — T21/T41/T53 |
| D8 drop reasons stay local; reports exclude local times/paths/attempts/operations/package versions | F/pending.rs:158 | F/pending_tests.rs:256; F/pending_tests.rs:338 | matches for wire grammar |
| D8 subject tags 0 Write/1 Entry/3 File/4 Snapshot/5 Positions | F/pending.rs:111 | F/pending_tests.rs:70 | matches |
| D8 KeyCopy subject includes audience/key/member/sealing and orders by exact credential | F/pending.rs:29 | F/pending_tests.rs:70 | differs — T104: no sealing field and member is 32 bytes |
| D8 reason tags 0–6 and exact payloads | F/pending.rs:352 | F/pending_tests.rs:102 | matches |
| D8 refusal tags 0–7; content hash only File; Refused immutable; InvalidPositions only Positions | F/pending.rs:221; F/pending.rs:407 | F/pending_tests.rs:183 | matches |
| D8 prerequisite 0 object path/1 device; other waits local | F/pending.rs:295 | F/pending_tests.rs:102; F/pending_tests.rs:256 | matches |
| D8 update 0 app/1 format; u32 format fits u16; file failure 0/1/2, only own File source | F/pending.rs:320; F/pending.rs:407 | F/pending_tests.rs:183; F/pending_tests.rs:315 | matches |
| D8 “Every path is canonical D10 text for this store” | F/pending.rs:283; F/objects.rs:109; S/posted_positions.rs:69 | F/pending_tests.rs:338; S/posted_positions_tests.rs:78 | matches after authenticated-store check |
| D8 ordered unique subjects; one first condition; reported_by is enclosing device | F/objects.rs:77; F/pending.rs:70 | F/pending_tests.rs:382 | matches |
| D8 only poster observations, never forwarded; exact credential reports do not satisfy replacement | F/objects.rs:109; F/pending.rs:29 | F/pending_tests.rs:466 | differs — T104/T106: old/new credentials share one report identity; forwarding policy T53 not audited |
| D8 complete signed report list; oversized list fails without truncation | F/wire.rs:28; F/objects.rs:77 | F/pending_tests.rs:486; F/sealed_tests.rs:402 | matches for codec bounds/signature; provider-limit pending publication T53/T55 |
| D8 clock “device … store_log_read”; no author time, positions/fingerprints/reports | F/objects.rs:14; F/clock.rs:17 | F/lib_tests.rs:6; F/clock_tests.rs:124 | differs — T104: clock contains only device |
| D8 clock provider T and T′ finality/return-time policies | F/clock.rs:17 (policy boundary) | — | untested here — T21/T43/T46; no device time is encoded |
| D8 join invite/member/keys/device name, supplied-key signature and invite-fixed member | F/objects.rs:22 | F/sealed_tests.rs:253 | differs — T104/T107: request omits stable member ID |
| D9 kinds 32 write/33 entry/35 positions/36 join/39 clock and prefixes | F/sealed_write.rs:53; F/sealed_single.rs:31 | F/sealed_write_tests.rs:14; F/sealed_single_tests.rs:7; F/clock_tests.rs:33 | matches |
| D9 kind 34 snapshot audience/key/caught_at/writes/store_log | F/sealed_snapshot.rs:16 | F/sealed_snapshot_tests.rs:88 | differs — T94: prefix omits the 12-byte caught_at |
| D9 kind 37 anonymous box member/sealing/ephemeral/nonce/ciphertext/tag; public routing only | F/sealed_key.rs:12; F/sealed_key.rs:71 | F/sealed_key_tests.rs:57; F/sealed_key_tests.rs:292 | differs — T104: member is 32 bytes and sealing credential absent |
| D9 entry prefix “same 19-byte prefix”, including creation | F/sealed_single.rs:31 | F/sealed_tests.rs:253; F/sealed_single_tests.rs:7 | matches for layout; creation authority T105 |
| D9 snapshot “two 64-byte” signatures; prefix signs exact clear bytes/path with separate label | F/sealed_snapshot.rs:92; K/member.rs:297 | F/sealed_tests.rs:342 | matches for implemented prefix bytes |
| D9 caught_at captured before catch-up, fixed; inclusive 86,400-second publication deadline | F/sealed_snapshot.rs:16 | — | unimplemented — T94/T95: no caught_at; `(100,0)` deadline cannot be checked |
| D9 bounded prefix reads without keys; verify before selecting/coverage/history/retention | F/sealed_snapshot.rs:31; K/member.rs:52 | F/sealed_snapshot_tests.rs:51; F/sealed_tests.rs:342 | untested — bounded prefix/signature primitives match; policy ordering belongs to T94/T95/T96 |
| D9 final signature hashes prefix signature too; verify before applying records | K/member.rs:289; D/snapshot_load.rs:19 | F/sealed_tests.rs:342 | matches for complete-object authentication; replay rebuilding T25/T97 |
| D9 pre-upload reopen, both signatures/records/fingerprint; post-upload complete checksum | F/sealed_snapshot.rs:153 (consumer boundary) | — | untested here — T72 |
| D9 snapshot signer from signed store_log; unknown author waits, wrong/missing signature permanent refusal | F/sealed_snapshot.rs:16; K/member.rs:52 | — | unimplemented — T104/T105 for recorded-view key history; refusal routing T55 |
| D9 positions exactly one chunk + author signature; recorded-view key; InvalidPositions failure | F/sealed_single.rs:111; S/posted_positions.rs:14 | F/sealed_tests.rs:402 | differs — T55/T105: framing/signature match, but reader uses supplied current replay and damaged-object errors |
| D9 clock same single-chunk signature, recorded store_log_read; status supplies time | F/clock.rs:17; F/objects.rs:14 | F/clock_tests.rs:60; F/clock_tests.rs:74 | differs — T104/T105: no recorded view in clock frame |
| D9 write/entry own-view signers; retain historical bindings; unverified fields hints; join/creation supplied keys | K/member.rs:40; F/write.rs:14; F/store_log.rs:262 | F/sealed_tests.rs:253 | unimplemented — T104/T105 for historical bindings; primitive signatures present |
| D9 historical snapshot/positions/clock remain valid; no 30-day key window; current-credential send gate | K/member.rs:40 (policy boundary) | — | untested here — T105/T106 |
| D9 chunk u32 length/24 nonce/ciphertext/16 tag; XChaCha20-Poly1305, length+44 | F/sealed.rs:31; K/derivation.rs:95 | F/sealed_write_tests.rs:348; F/sealed_single_tests.rs:7 | matches |
| D9 write/entry deterministic nonces; other object random nonces; read stored nonce | K/derivation.rs:95; K/derivation.rs:113; K/derivation.rs:145 | K/derivation_tests.rs:83; F/sealed_tests.rs:89; F/sealed_tests.rs:253 | matches |
| D9 no empty chunks; frame 7–16 MiB; stream 1–64 KiB, only last short | F/sealed_single.rs:205; F/sealed_snapshot.rs:211; F/sealed_write.rs:236 | F/sealed_single_tests.rs:142; F/sealed_snapshot_tests.rs:166; F/sealed_write_tests.rs:179 | matches |
| D9 section 0 header, part i+1 with named key; snapshot one stream; other objects one frame | F/sealed_write.rs:201; F/sealed_snapshot.rs:153; F/sealed_single.rs:111 | F/sealed_tests.rs:89; F/sealed_snapshot_tests.rs:105; F/sealed_single_tests.rs:7 | matches |
| D9 part-key count exact; migration no part keys; unreadable parts still delimit and authenticate | F/sealed_write.rs:128; F/sealed_write.rs:172 | F/sealed_write_tests.rs:170; F/sealed_write_tests.rs:179; F/sealed_write_tests.rs:264 | matches |
| D9 listed snapshot size: subtract prefix/two signatures, ceil chunks, final sealed length 45..65580 | F/sealed_snapshot.rs:49 | F/sealed_snapshot_tests.rs:10 | matches for existing prefix layout |
| D9 AAD context label/path/whole prefix/section/index; excludes snapshot prefix signature | K/derivation.rs:169 | F/sealed_tests.rs:89; F/sealed_tests.rs:195; K/derivation_tests.rs:83 | matches |
| D9 prefix/path/chunk swap/drop/reorder detected; missing snapshot end refused | F/sealed_write.rs:250; F/snapshot.rs:311; K/derivation.rs:145 | F/sealed_write_tests.rs:179; F/sealed_tests.rs:195; F/snapshot_tests.rs:175 | matches |
| D9 signature 64-byte Ed25519 over label/path/SHA256(all preceding bytes); join supplied key | K/member.rs:162; K/member.rs:289 | K/member_tests.rs:78; F/sealed_tests.rs:253; F/sealed_tests.rs:342 | matches for cryptographic message |
| D10 all eight exact path layouts, snapshot audience, numbered writer copies, device-qualified files | F/path.rs:19; F/path.rs:177 | F/path_tests.rs:168; F/path_tests.rs:257 | matches |
| D10 decimal IDs/n, no leading zero, n>=1; canonical lowercase hyphenated UUIDs | F/path.rs:314; F/path.rs:320 | F/path_tests.rs:37; F/path_tests.rs:168 | matches for paths |
| D10 member spelling UUID, never public key | K/member.rs:64 | K/member_tests.rs:7 | differs — T104: member text is 64 hexadecimal key digits |
| D10 separate gap-free sequences; copies share across recipients; entries/copies never deleted; list snapshots | F/path.rs:19; F/path.rs:238 (policy boundary) | F/path_tests.rs:122 | untested here — T41/T43/T50/T74; constructors alone do not enforce publication/retention |
| D10 replacement never starts second log/overwrites copies; exact path bound to authentication | K/derivation.rs:169; K/member.rs:305 | F/sealed_key_tests.rs:57; F/clock_tests.rs:74 | matches for path authentication; replacement lifecycle T105/T106 |
| D11 context u64 BE lengths, numbers separate 8-byte fields | K/cipher.rs:20; K/derivation.rs:169 | K/cipher_tests.rs:5; K/derivation_tests.rs:162 | matches |
| D11 no-salt HKDF-SHA256 labels encryption/fingerprints and invite join key | K/derivation.rs:15; K/derivation.rs:86 | K/derivation_tests.rs:6; K/derivation_tests.rs:40; K/derivation_tests.rs:51 | matches |
| D11 HMAC-SHA256 nonce v2, exact path/prefix/u64 section/index/SHA256(plaintext), first 24 bytes | K/derivation.rs:113 | K/derivation_tests.rs:83; K/derivation_tests.rs:162 | matches |
| D11 identical retry bytes; changed plaintext/prefix changes nonce; hash/plaintext not published | K/derivation.rs:113; F/sealed_write.rs:236 | F/sealed_tests.rs:89; K/derivation_tests.rs:136; K/derivation_tests.rs:149 | matches for deterministic primitives; persisted attempt policy outside codec |
| D11 deterministic recipe excludes snapshot/key/positions/clock/join; retain originally sealed random bytes | K/derivation.rs:95; F/sealed_key.rs:90 | F/sealed_key_tests.rs:317; F/sealed_tests.rs:195; F/clock_tests.rs:33 | matches for encode/decode; durable retention policy not certified |
| D11 X25519 fresh ephemeral, reject all-zero contribution; box HKDF/AAD exact label/audience/path/prefix/keys | K/member.rs:228; K/member.rs:305; K/member.rs:322 | K/member_tests.rs:145; K/member_tests.rs:274; F/sealed_key_tests.rs:57 | matches for implemented box context; missing recipient credential below |
| D11 box plaintext store key UUID+32 bytes or circle UUID+key UUID+32 bytes; verify prefix IDs/hash | F/sealed_key.rs:97; F/sealed_key.rs:113 | F/sealed_key_tests.rs:159; F/sealed_key_tests.rs:217 | matches for payload/hash checks; authorized-introduction selection T28 |
| D11 recipient exact member and sealing credential, historical private key, no latest-key search | F/sealed_key.rs:130 | F/sealed_key_tests.rs:190 | differs — T104/T106: prefix cannot name the historical sealing credential |
| D11 fingerprint outer raw label/audience/32-byte sum, HMAC; no context lengths; empty zero | D/fingerprint.rs:160 | D/fingerprint_tests.rs:223; K/derivation_tests.rs:40 | matches |
| D11 SHA256 context leaf/identity/value; raw UTF-8/D3 keys; typed unframed values, u64 counts | D/fingerprint.rs:16; D/fingerprint.rs:189; F/merge_fields.rs:1 | F/merge_fields_tests.rs:31; D/fingerprint_tests.rs:6 | matches for implemented fields |
| D11 one row leaf, exact generation/setter/written-parent/visible-value order; absent visible row empty map | D/fingerprint.rs:25 | D/fingerprint_tests.rs:6; D/fingerprint_tests.rs:117 | matches by code and row/sum tests |
| D11 one loss leaf; D7 identity and complete loss value, dismissed cells absent | D/fingerprint.rs:94; F/merge_fields.rs:1 | F/merge_fields_tests.rs:93; D/fingerprint_tests.rs:117 | differs — T68/T71/T101: inherits obsolete D7 cause bytes and lacks source evidence |
| D11 replace subtracts/adds modulo 2^256 in same transaction; audiences count independently | D/fingerprint.rs:179; D/fingerprint.rs:189 | D/fingerprint_tests.rs:6; D/fingerprint_tests.rs:102 | matches for sum/update behavior |
| D12 file kind 38/version/u64 size; 64-KiB ciphertext chunks plus 16 tag, empty file none | F/file.rs:25; F/file.rs:53 | F/file_tests.rs:19; F/file_tests.rs:151 | matches |
| D12 file XChaCha key, 24-byte BE index nonce, label/path/header/index AAD | K/file_key.rs:31; K/file_key.rs:54 | F/file_tests.rs:86; F/file_tests.rs:178 | matches |
| D12 chunk offset “11 + i × (65,536 + 16)”; independent ranges | F/file.rs:67; F/file.rs:119 | F/file_tests.rs:19; F/file_tests.rs:86 | matches |
| D12 fixed reference text device/UUID/64 lowercase key hex, exact ASCII spaces | F/file_reference.rs:26; F/file_reference.rs:52 | F/file_reference_tests.rs:8; F/file_reference_tests.rs:29; F/file_reference_tests.rs:69 | matches |
| D12 reference row's store supplies path; key encrypted; completion/cache/pin never rewrite; no-file NULL/hash NULL | F/file_reference.rs:11; F/path.rs:106; D/file_location.rs:25 | F/file_reference_tests.rs:136; D/file_ref_tests.rs:94 | differs — T57: pending files also encode decimal device text, e.g. `0`; fixed reference grammar matches; clear/hash lifecycle T61 not audited |
| D13 restore store/name/member/private pair/storage | F/codes.rs:17 | F/codes_tests.rs:64 | differs — T104/T107: member UUID absent |
| D13 CVMK 01+32 seed+32 sealing secret; storage <=16 KiB | K/member.rs:174; F/codes.rs:156 | K/member_tests.rs:78; F/codes_tests.rs:154; F/codes_tests.rs:227 | matches for private-pair blob/bound |
| D13 restore current pair for stable member; old code cannot rebind it | F/codes.rs:17 | — | unimplemented — T104/T107: stable member ID is absent |
| D13 restore storage is location only, never S3/OAuth credentials; credentials travel separately | S/restore_codes.rs:253; S/codes.rs:101 | S/codes_tests.rs:65 | matches for the checked bootstrap payload contract (T93); raw format storage remains opaque |
| D13 invite store/name/invite/member/secret/initial key/writer/cursor/storage | F/codes.rs:73 | F/codes_tests.rs:216 | differs — T104: member UUID absent; remaining fields encoded |
| D13 cursor zero or last copy, begin next; u64 exhausted case unspecified | F/codes.rs:148 | F/codes_tests.rs:186 | differs: `u64::MAX` refused because no next number; preserved |
| D13 invite “storage:bytes”; separate 16-KiB invite bound not stated | F/codes.rs:156 | F/codes_tests.rs:154; F/codes_tests.rs:227 | differs: 16,385-byte storage refused although frame can fit 18 KiB; preserved |
| D13 admin-fixed fresh/existing member; exact signed pair; prerequisite copies and approving writer kept binding | F/codes.rs:73; F/objects.rs:22 | — | unimplemented — T104/T107 |
| D13 CVR1-/CVI1- uppercase unpadded base32(frame+BE CRC32C), canonical spare bits | F/codes.rs:217; F/codes.rs:240 | F/codes_tests.rs:64; F/codes_tests.rs:109; F/codes_tests.rs:154 | matches |
| D13 CRC polynomial/init/final/check E3069283; max 18 KiB before encoding; checksum not authentication | F/codes.rs:206; F/codes.rs:168 | F/codes_tests.rs:59; F/codes_tests.rs:154 | matches |
| D14 one fixture per frame/object/box/file/code, store+circle write with three first-part chunks | F/test_utils.rs:1; F/lib_tests.rs:29 | F/lib_tests.rs:29; F/lib_tests.rs:42; F/sealed_tests.rs:89 | matches for implemented layouts; omitted new layouts below |
| D14 every entry tag, UUID authors/members, tag 16, both access forms, key-free removals/introductions | F/store_log.rs:63 | F/store_log_tests.rs:124; F/store_log_tests.rs:161; F/lib_tests.rs:29 | differs — T104: tag 16 and stable UUID layouts absent |
| D14 dismissal, migration, all snapshot sections and active/frozen/excluded losses | F/test_utils.rs:1 | F/lib_tests.rs:29; F/dismissal_tests.rs:19; F/loss_tests.rs:25 | matches for current records; new loss/evidence fixtures T68/T101 |
| D14 signed caught_at snapshot; clock store_log_read; rebinding codes/requests; old/new key copies/reports | F/sealed_snapshot.rs:16; F/objects.rs:14; F/codes.rs:17 | F/sealed_tests.rs:195; F/clock_tests.rs:33; F/codes_tests.rs:64 | differs — T94/T104: these fields and credential variants absent |
| D14 successful decode exact re-encode; every fixture truncation/bit mutation, including payload paths | F/lib_tests.rs:118 | F/lib_tests.rs:163; F/sealed_tests.rs:89; F/sealed_tests.rs:342; F/sealed_tests.rs:402; F/sealed_key_tests.rs:317; F/file_tests.rs:178; F/codes_tests.rs:12; F/clock_tests.rs:60 | untested — frame/object mutation suites exist; independent raw v1 stream-chunk mutation coverage is not checked (T104 regeneration) |
| D14 listed fixture names, file 64 KiB+29 bytes and device-qualified reference | F/file_tests.rs:178; crates/coven-format/fixtures/file-reference.txt:1 | F/file_reference_tests.rs:136; F/path_tests.rs:70 | matches after fixture rename |
| D14 independent Python hashlib/hmac/libsodium ciphertext/HKDF/signatures | crates/coven-format/fixtures/generate-deterministic.py:17 | F/sealed_tests.rs:89; F/sealed_tests.rs:253 | untested — write/entry generator is independent; other fixture-generation provenance not rerun (T104 regeneration) |
| D14 both snapshot signatures, positions signature; truncation/bit/path/stable-author/recorded-view binding | K/member.rs:40; K/member.rs:52 | F/sealed_tests.rs:342; F/sealed_tests.rs:402 | differs — cryptographic mutations match; stable-author/recorded-view fixtures are absent (T104/T105) |
| D14 mutate either key-copy recipient field; pin historical/current credentials; regenerate format 1 crypto | F/sealed_key.rs:12 | F/sealed_key_tests.rs:57 | unimplemented — T104: no separate sealing recipient field |
| D14 identical retry/changed plaintext/changed prefix nonce examples | K/derivation.rs:113 | K/derivation_tests.rs:83; K/derivation_tests.rs:136; K/derivation_tests.rs:149; F/sealed_tests.rs:89 | matches |

Verification: `PATH=$HOME/.elan/bin:$PATH lockf -k "$CARGO_TARGET_DIR/.full-check.lock" scripts/check.sh changed` passed. This included formatting, the ownership checker, clippy, documentation, production builds, both feature configurations, the release replay benchmark, and the selected Rust/Lean comparisons (including existing counterexample tests). The snapshot EOF and authenticated cross-store regressions were each observed failing before their fixes.

Not checked: live-provider integration tests that remain ignored, other operating systems, full proof-package/axiom audits (no proof packages were selected), and independent regeneration of the existing cryptographic fixtures. Queued representation and policy changes are identified in the table and were not implemented.
