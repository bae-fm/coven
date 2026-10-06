# Coven plaintext format 1

This crate encodes the storage objects of `plans/coven-from-scratch.md`, §5,
§7–§9, §11–§12, §14–§17 and §19. It depends on foundation, crypto and merge.
Foundation owns store, device, circle and invite identities; crypto owns member
identities, public keys, fingerprints and secrets; merge owns timestamps,
audiences, write identities, row identities, changes, row state and removal rules.
Format performs no I/O, clock reads, randomness, signing or sealing.

## Frames and bounds

Every frame is `kind:u8 | version:u16 | payload_length:u32 | payload`.
Version is 1. Numbers are big-endian, except inside crypto's opaque member-key
encoding. A signed SQL integer uses two's complement. `frame_length` validates
the seven-byte prefix before allocation or fetching. A complete frame is at
most 16 MiB. Unknown kinds, versions and trailing bytes are refused.

Notation:

- `bytes`: `length:u32 | raw bytes`, at most 8 MiB.
- `text`: UTF-8 `bytes`, without Unicode normalization. SQL text can be empty
  or contain NUL. Names contain 1–1,024 bytes and no NUL.
- `[T]`: `count:u32 | T...`. Each collection and the sum of length-prefixed
  collection counts in a frame are at most 65,536. String/blob lengths are not
  collection counts. Maps encode a count followed by key/value pairs; sets a
  count followed by members. Keys/members must be strictly increasing in their
  type's order, including on decode; duplicates never silently overwrite.
- Fixed byte arrays have no prefix. UUIDs are 16 bytes in UUID byte order.
  Store, circle and invite ids wrap foundation's UUID types. Device ids are u64.
  Member ids are 32 Ed25519 bytes from crypto's `MemberId::to_bytes`; decoding calls
  `MemberId::from_bytes`, which rejects invalid and weak points. A sealing public
  key is crypto's `SealingPublicKey`, encoded as its 32 public bytes.
- `Timestamp`: `milliseconds:u48 | counter:u16 | device:u64`, exactly 16 bytes.
  Byte order is timestamp order. Decode calls merge's `Timestamp::new`.
- `WriteId` and `EntryId` both encode `device:u64 | number:u64`, with positive
  numbers. They are different Rust types: `WriteId` names a write; format's
  `EntryId` names a store-log entry. `WritePositions` and `EntryPositions` are
  separate lists, strictly ordered by device. Zero positions are omitted.
- `Audience`: `0` for the store, `1 | circle_uuid:16` for a circle. Store sorts
  first, then circles in UUID order, as merge defines.
- `Value`: `0` NULL; `1 | i64` integer; `2 | u64` real's IEEE 754 bits;
  `3 | text`; `4 | bytes` blob. NaNs and negative zero are refused; infinities
  and positive zero are valid. Outside keys, integer and real storage classes
  remain distinct.
- `RowId`: `table:text | key:bytes | audience:Audience`. The key bytes have the
  ordered encoding below. Every row and parent key is decoded for validation.
- `Parent`: `row:RowId | generation:u64`. Written references name odd
  incarnations. A child can refer to its own audience or the store.
- `ConstraintColumns`: `[text]`, the column names in declaration order. List
  boundaries are encoded; `(a, bc)` and `(ab, c)` are different identities.
  Column lists order lexicographically, without sorting their columns. A foreign
  key has at least one column.
- `ForeignKey`: `columns:ConstraintColumns | parent:text |
  parent_columns:ConstraintColumns`. Both column lists are nonempty and have
  equal lengths. Implicit targets name the parent's primary-key columns. Order
  compares the source columns, then the parent table, then the target columns.
  Two keys on one column into different tables or target columns stay distinct.
- `UniqueConstraint`: `terms:[text] | partial`. Terms are nonempty and ordered,
  each a column name or an expression's text as written. `partial` is `0` for a
  full constraint or `1 | text` for the WHERE expression of a partial index.
  Terms and predicates use the name bounds. Order compares terms, then the
  optional predicate (absent first). `title`, `lower(title)` and `title` with
  `WHERE active=1` identify three different constraints. An explicit COLLATE
  stays in an expression term; ASC/DESC ordering is not part of the term.
- `ColumnValue<Value>`: `value:Value | parents:map<ForeignKey, Parent>`.
  This is merge's type, also used inside winning cells and lost values, without
  copying its fields into another type.
- `MemberPublicKeys`: `signing:32 | sealing:32`.
- `SnapshotId`: `device:u64 | number:u64 | audience:Audience`. Its positive
  number is in the device's snapshot sequence, not either log.

Bounds are checked before decoding length-prefixed allocations. Collection
counts must fit at least one byte per member, or two per map entry, in the
remaining input. Encoder and decoder share the collection budget. Nesting depth
is fixed by these types. No arbitrary recursively nested value is supported.

## Ordered primary keys

`encode_key` accepts a nonempty list of non-null `Value`s, in schema key-column
order. Components concatenate without a list length or terminator. Thus a
shorter composite prefix sorts first. Encoded keys are at most 8 MiB and contain
at most 65,536 components. `decode_key` rejects noncanonical bytes.

SQLite orders integers and reals together numerically, then text under its
collation, then blobs by bytes ([SQLite sorting](https://www.sqlite.org/datatype3.html#sorting_grouping_and_compound_selects)).
This encoding uses BINARY text collation. A caller using another collation must
normalize its key components accordingly. Numerically equal integer and real
keys have the same bytes: `1` and `1.0` identify the same key. Decode chooses an
integer for an exactly integral number in i64 range; other numbers decode as
reals. This does not change the values stored in write records or cells.

Numeric component encodings:

| Tag | Meaning | Following bytes |
| --- | --- | --- |
| 0x10 | Negative infinity | None |
| 0x11 | Negative finite nonzero | Complemented exponent:u16 and significand:u64 |
| 0x12 | Zero | None |
| 0x13 | Positive finite nonzero | Exponent:u16 and significand:u64 |
| 0x14 | Positive infinity | None |

The absolute value is `significand * 2^(exponent - 1074 - 63)`. The significand's
highest bit is one; the encoded exponent is 0–2097. Integer magnitudes are
normalized exactly, without converting through f64, so adjacent i64 values
above 2^53 remain distinct. Real values include subnormals. Negative finite
values complement both fields to reverse magnitude order. Decode rejects any
number not exactly representable as an i64 or permitted f64, and verifies that
re-encoding produces identical bytes.

Text components start with 0x20; blobs with 0x30. Each zero data byte is escaped
as `00 FF`; other bytes are literal. `00 00` terminates a component. Text bytes
must be UTF-8. The escaping preserves byte order, including embedded zeroes,
empty strings/blobs and prefixes, without a length field affecting comparison.

## Object kinds

Fields appear in exactly the order shown. All enum tags are u8.

| Kind | Object | Payload |
| --- | --- | --- |
| 1 | Write | `header:WriteHeader, parts:[WritePart]` |
| 2 | Store-log entry | `position:EntryId, timestamp:Timestamp, author:MemberId, had_read:EntryPositions, change:StoreChange` |
| 3 | Snapshot header | `id:SnapshotId, schema_version:u32, writes:WritePositions, store_log:EntryPositions, counts:5*u64` |
| 4 | Snapshot record | `section:u8, record` |
| 5 | Snapshot end | Empty |
| 7 | File header | `chunk_size:u32, total_size:u64` |
| 8 | Restore code | `store_uuid:16, name:text, member_keys:bytes, storage:bytes` |
| 9 | Invite code | `store_uuid:16, name:text, invite_uuid:16, secret:32, storage:bytes` |
| 10 | Join request | `invite_uuid:16, keys:MemberPublicKeys, device_name:text` |
| 11 | Posted positions | `device:u64, writes:WritePositions, store_log:EntryPositions, fingerprints:[Fingerprint]` |
| 12 | File chunk | `index:u64, plaintext:bytes` |

Kind 6 is not defined. Sealed keys, their plaintext, recipient binding and path
binding belong entirely to crypto's `seal_store_key` and `seal_circle_key`.
There is no format-owned sealed-key envelope or reader for one.

`Object` encodes and decodes kinds 1, 2, 7, 10, 11 and 12. Snapshots use their
streaming encoder/decoder, with merge's oracle supplied on decode. Restore and
invite codes use their own zeroizing binary and text APIs; they cannot be
encoded through the ordinary `Vec<u8>` object API.

A file's chunk size is 1–8 MiB in bytes (65,536 is the product default).
Total size may be zero, meaning no chunks. Chunk data is nonempty.
`FileHeader::validate_chunk` checks index, overflow and exact length, including
the final partial chunk. It does not authenticate.

A fingerprint is `audience:Audience | key_number:u64 | fingerprint:32`.
Fingerprints are strictly ordered by audience and include the store first.
Key numbers are positive. The fingerprint value is crypto's `Fingerprint`,
encoded through its `as_bytes` and decoded through `from_bytes`.

### Write records

`WriteHeader` is `position:WriteId | timestamp:Timestamp |
had_read:WritePositions | schema_version:u32 | disposition`. The timestamp's
device matches the write. Had-read contains other devices only; own earlier
writes are implicit. Schema version zero is representable. Disposition is `0`
to apply or `1 | breaking_version:u32` to upload the write marked lost.
The breaking version is positive and names the version the change raised the
store to.

`WritePart` is `audience:Audience | rows:[RowChange]`. Parts and their row lists
are nonempty and strictly ordered by audience and `RowId`, respectively.
Every row has its part's audience. Row identity order is merge's order: table,
encoded key bytes, then audience. Table and column names sort by UTF-8 bytes.

`RowChange` is `row:RowId | change:Change<Value> | old:map<text, Value>`.
`Change<Value>` is `generation:u64 | operation:Operation<Value>`.
Operations are merge's own enum:

- Insert: `0 | columns:map<text, ColumnValue<Value>>`.
- Update: `1 | columns:map<text, ColumnValue<Value>>`.
- Delete: `2`.

The generation precedes the change. Merge's `Change::validate` checks parity,
generation advancement without overflow, and parent generations/audiences.
Insert/update columns are nonempty. Inserts have no old values; updates have
exactly the same column names in old and new maps; deletes may retain old values
or omit them.
Parent metadata belongs to each new `ColumnValue`, keyed by the full `ForeignKey` identity.

The local upload queue holds the canonical kind-1 frame, unencrypted and
unsigned. Its uploader encrypts and signs it on its first upload (§6).

### Store-log entries

Had-read lists store-log entries, not writes. Own-device positions, when
included, precede the entry. A create-store entry is number 1, has no had-read
entries, and is authored by the first admin. `MemberRole` is admin 0 or member 1.

| Tag | Change | Fields after tag |
| --- | --- | --- |
| 0 | Create store | `store_uuid:16, name:text, admin:MemberPublicKeys` |
| 1 | Add member | `keys:MemberPublicKeys, role:u8` |
| 2 | Remove member | `member:32, replacement_key_number:u64, circle_keys:[CircleKeyNumber], deleted_circles:[CircleId]` |
| 3 | Change role | `member:32, role:u8` |
| 4 | Add device | `member:32, device:u64, name:text` |
| 5 | Remove device | `device:u64` |
| 6 | Create circle | `circle:16, name:text, creator:32` |
| 7 | Rename circle | `circle:16, name:text` |
| 8 | Delete circle | `circle:16` |
| 9 | Add circle member | `circle:16, member:32` |
| 10 | Remove circle member | `circle:16, member:32, replacement_key_number:u64` |
| 11 | Raise schema | `version:u32, snapshot:SnapshotId` |
| 12 | Raise format | `version:u16, snapshot:SnapshotId` |
| 13 | Reset | `snapshot:SnapshotId` |

`CircleKeyNumber` is `circle:16 | key_number:u64`. A member removal records the
replacement store key, the replacement keys of circles the member shared with
others, and circles deleted because the member was alone in them (§13).
Both circle lists are strictly increasing by circle UUID, and no circle occurs
in both. Either list may be empty; both count fields are always encoded.

All replacement key numbers and raised versions are positive. Schema/format
snapshots name the store audience; reset snapshots name the affected audience.
Authority, conflicts, causal closure and monotonic version/key changes require
other entries and are checked by their owners.

### Snapshot streams

A snapshot is one kind-3 header, its kind-4 records, then a kind-5 end marker.
Counts declare records in five sections, in order. Empty sections emit nothing.

| Section | Record | Order within section |
| --- | --- | --- |
| 0 | `SyncedRow { row:RowId, columns:map<text, ColumnValue<Value>> }` | RowId |
| 1 | `AppliedWrite { id:WriteId, timestamp:Timestamp, had_read:WritePositions }` | WriteId |
| 2 | `SyncedColumn { table:text, column:text }` | Table, column |
| 3 | `MergeRow`, described below | RowId |
| 4 | `LostWrite`, described below | Header's WriteId |

`MergeRow` is `row:RowId | generations:map<u64, WriteId> |
cells:map<text, Cell<Value>> | lost:map<LostKey, LostValue<Value>> |
removed:set<Rule>`. Each nested shape belongs to merge:

- `Cell<Value>`: `write:WriteId | value:ColumnValue<Value>`.
- `LostKey`: `column:text | write:WriteId`, ordered by column then write.
- `LostValue<Value>`: `incarnation:u64 | value:ColumnValue<Value> |
  replaced_by:WriteId`.
- `Rule`: `0 | foreign_key:ForeignKey`, `1 | check_name:text`,
  `2` DeletedCircle, `3` OtherAudience, or `4 | unique:UniqueConstraint`.
  A CHECK uses its declared name, or its SQL expression when unnamed.
  Ordering is merge's variant order, then the foreign-key identity, unique terms and predicate,
  or CHECK name. OtherAudience names no winner and imposes
  no invented restriction on which circle can win.

Synced-row references are checked by merge's `Parent::validate_written`.
Merge-row decode constructs `RowState::from_parts` using the caller's `WriteOracle`.
Merge checks contiguous generations, transition timestamps, deleted-row cell
absence, cell timestamps, parent generations/audiences, lost incarnations and
canonical replacing writes and whether those writes had read the lost values.
Format checks names, keys, positive ids and encodings. Removed rows must be
present in merge state; DeletedCircle and OtherAudience rules require a circle.
Which removal rules actually hold is established by the removal computation.

`LostWrite` is `write:WriteRecord | cause:LostWriteCause`. Cause is
`0 | schema_version:u32` or `1 | reset_entry:EntryId`. These writes were never
applied; nothing in the merged state replaced them. The record contains only
the snapshot audience's part, exactly one part. The producer filters parts;
the codec refuses a mismatched or multi-audience record. If the write header
contains `WriteDisposition::Lost(version)`, its cause must be
`SchemaChange(version)`. A schema-change cause is positive and no greater than
the snapshot schema version.
Merge's lost values and these lost writes remain separate; the database owns
presenting both through its `coven_lost` API.

The streaming decoder retains its header, remaining counts and previous identity,
not rows or write history. The consumer stages each AppliedWrite and supplies
an oracle for the snapshot's causally closed applied set when decoding merge
rows. Own earlier writes are implicit in each AppliedWrite's read positions.
Lost writes are excluded from that oracle. This lets a database supply indexed
metadata without collecting the snapshot in memory.

The stream checks ordering, duplicates, audience, counts, and coverage of all
referenced write ids, reset entries and breaking schema versions. Header write
positions describe consumed writes, including lost writes; a lost write's excluded dependencies
need not be covered. Applied write read positions must be covered. Every failed
call leaves the cursor unchanged. EOF without the end marker is truncation.
The consumer commits staged records only after `finish` succeeds. Cross-section
completeness, SQL schema rules and app-row visibility are database concerns.
No local row ids, uploads, operations or storage paths occur in a snapshot.

### Fields stored in SQLite

`merge_fields` exposes the snapshot field encodings without a frame prefix:
`Timestamp`, `WriteId`, `WritePositions`, `ForeignKey`, `UniqueConstraint`,
parent maps, `ColumnValue<Value>`,
column maps, setter maps (`map<text, WriteId>`), removal-rule sets, and
`LostWriteCause`. The database's schema version identifies their format.
These functions use the same binary primitives and validation as snapshots;
they do not define another encoding. Each call has the same collection and
byte bounds and rejects trailing bytes. Cross-field relationships still need
merge's validation.

Present values live only in the app's tables. `coven_cells` retains setters.
`coven_foreign_keys` interns reference identities, and `coven_references` holds
one parent table, key, audience and generation per row, column and foreign key.
`coven_constraints` interns unique identities, and `coven_claims` indexes each
removed row's audience and equality-encoded claim. `coven_lost` retains lost
values and removed rows; lost cells keep their written parents in their value.
The database round-trip tests use these codecs with the actual tables. Local
writes load only touched rows’ merge state and persist `coven-merge::apply` updates.

### Restore and invite codes

Restore codes hold crypto's `MemberKeys`. Their bytes come only from
`MemberKeys::to_secret_bytes` and are restored by `from_secret_bytes`; format
treats this blob as crypto's encoding. Invite codes hold `InviteSecret`.
Both store storage settings, including credentials, as `SecretBytes`, at most
16 KiB. Debug redacts keys, invite secrets and storage settings. These code
types do not implement Clone.

Binary frames and text results use `Zeroizing<Vec<u8>>` and
`Zeroizing<String>`. Every buffer receiving secret data is allocated to its
complete capacity before copying any secrets and is never grown. Parsing
borrows the caller's input; owned frame, key-material, credential and base32
buffers erase on drop, including error paths. The caller owns erasure of its
input and of any copies it chooses to retain.

Text is `CVR1-` (restore) or `CVI1-` (invite), followed by uppercase unpadded
base32 of `complete_frame | checksum:u32`. The alphabet is
`ABCDEFGHIJKLMNOPQRSTUVWXYZ234567`. Unused trailing bits must be zero.
CRC-32C covers the complete frame: reflected polynomial 0x82F63B78, initial and
final XOR 0xFFFFFFFF, checksum big-endian. The check value for `123456789` is
E3069283. CRC input is borrowed from the zeroizing frame, without another copy.

Binary contents plus checksum are capped at 18 KiB; text length is bounded
before allocation. There are no whitespace, case or spelling aliases.
The checksum detects typing mistakes, not hostile changes. Text may be typed
or supplied to a QR encoder; QR capacity limits what fits in one symbol.

## Verification and boundaries

`fixtures/v1.hex` pins ordinary object kinds, including a member removal with
replacement circle keys and deleted circles, a complete snapshot of all five
sections, and both secret frame kinds. `fixtures/codes.txt` pins both text
codes. Seeds and credentials in these fixtures are public test data.
Tests exercise every store change and removal rule, merge invariant rejection,
schema/reset lost writes, SQLite numeric/text/blob/composite key ordering and
streaming snapshots exceeding the frame bound. A decoded RowState is fed back
into merge's actual `apply` function.

Deterministic generated inputs exercise 25,000 binary inputs, all fixture
truncations and bit flips, 10,000 real keys and 5,000 code strings. Every
successful decode must re-encode identically. Both codes reject single ASCII
substitutions, alphabet insertions and deletions at every position. Tests also
check maximum credentials, crypto-owned key restoration and weak public-key
rejection. A compile-fail example checks that write and store-log positions
cannot be interchanged.

Authentication, signatures, key sealing, path/index binding, nonces, fingerprint
computation and custody belong to crypto and its callers. A changed plaintext
integer can still be a valid integer; this codec cannot authenticate it. There
are no signature/ciphertext placeholders or obsolete format readers.
