# Plaintext frame codecs

This describes coven-format's write, store-log, snapshot, join-request and
posted-position frame codecs. [Appendix D](../../../plans/coven-format.md)
defines the storage format, its shared primitives, sealed envelopes, files
and codes.

## Object kinds

Fields appear in exactly the order shown. All enum tags are u8.
These codecs identify their plaintext frames with kinds 1–5, 10, 11 and 13.

| Kind | Object | Payload |
| --- | --- | --- |
| 1 | Write header | `header:WriteHeader, parts:[PartHeader]` |
| 2 | Store-log entry | `position:EntryId, timestamp:Timestamp, author:MemberId, had_read:EntryPositions, change:StoreChange` |
| 3 | Snapshot header | `id:SnapshotId, schema_version:u32, writes:WritePositions, store_log:EntryPositions, counts:5*u64` |
| 4 | Snapshot record | `section:u8, record` |
| 5 | Snapshot end | Empty |
| 10 | Join request | `invite_uuid:16, keys:MemberPublicKeys, device_name:text` |
| 11 | Posted positions | `device:u64, writes:WritePositions, store_log:EntryPositions, fingerprints:[Fingerprint]` |
| 13 | Write row | `row:RowChange` |

Kinds 6–9 and 12 are not defined by these plaintext codecs.

`Object` encodes and decodes kinds 2, 10 and 11. Writes and snapshots use their
streaming encoder/decoder, with merge's oracle supplied on decode.

`SnapshotId` in these plaintext frames is
`audience:Audience | device:u64 | number:u64`; its positive number belongs to
the device's snapshot sequence. `UniqueConstraint` terms and predicates use
the name bounds in these codecs.

A fingerprint is `audience:Audience | key:KeyId | fingerprint:32`.
Fingerprints are strictly ordered by audience and include the store first.
The fingerprint value is crypto's `Fingerprint`,
encoded through its `as_bytes` and decoded through `from_bytes`.

### Write records

`WriteHeader` is `position:WriteId | timestamp:Timestamp |
had_read:WritePositions | schema_version:u32 | disposition`. The timestamp's
device matches the write. Had-read contains other devices only; own earlier
writes are implicit. Schema version zero is representable. Disposition is `0`
to apply, `1 | breaking_version:u32` to upload the write marked lost, or `2`
for a migration write. A migration write has no parts: only the breaking
change's snapshot carries its changes (§17.1). Applying its log object consumes
the position without changing rows.
The breaking version is positive and names the version the change raised the
store to.

`PartHeader` is `audience:Audience | row_count:u64 | plaintext_length:u64`.
The header's descriptors are strictly ordered by audience, empty for a migration
write and nonempty for every other disposition.
Each part is a stream of kind-13 frames, one `RowChange` per frame, strictly
increasing by `RowId`. Every row has its part's audience. Each part has at least
one row. `plaintext_length` includes every row frame's seven-byte
prefix. There is no collection count around the stream, and no bound on the
write's aggregate size or row count beyond the lengths' u64 representation.
Every individual frame retains Appendix D's frame and collection bounds.

`WriteRecord` and `WritePart` are in-memory values, not wire encodings.
`WriteEncoder` measures each row frame to produce the header, then emits each
part's frame stream. `PartDecoder` retains at most one unfinished frame and
the previous row identity, yields complete rows from each chunk, and checks
row count and plaintext length at `finish`. A frame announced past the stream's
end is refused before allocating its payload. Row identity order is merge's order: table,
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

The local upload queue holds one plaintext value: the complete kind-1 header
frame, then every part's frame stream in descriptor order, with no chunk-length
fields between them. `WriteEncoder::encode_plaintext` fills a buffer of exactly
`plaintext_length` bytes; a different buffer length is refused before writing.
The database checks that length against its connection's SQLite length limit
before allocating the buffer. `decode_plaintext` reconstructs the database's
in-memory write using the same header and part decoders. The uploader encrypts
and signs it on its first upload (§6).

### Store-log entries

Had-read lists store-log entries, not writes. Own-device positions, when
included, precede the entry. A create-store entry is number 1, has no had-read
entries, and is authored by the first admin: decoding refuses an admin signing
key different from the author. It registers the writing device with its supplied
name. Added devices belong to the author; a new circle's first member is the
author. These identities are derived during replay. `MemberRole` is admin 0 or member 1.

| Tag | Change | Fields after tag |
| --- | --- | --- |
| 0 | Create store | `store_uuid:16, name:text, admin:MemberPublicKeys, key:KeyId, device_name:text` |
| 1 | Add member | `keys:MemberPublicKeys, role:u8` |
| 2 | Remove member | `member:32, key:KeyId, circle_keys:[CircleKeyId]` |
| 3 | Change role | `member:32, role:u8` |
| 4 | Add device | `device:u64, name:text` |
| 5 | Remove device | `device:u64` |
| 6 | Create circle | `circle:16, name:text, key:KeyId` |
| 7 | Rename circle | `circle:16, name:text` |
| 8 | Delete circle | `circle:16` |
| 9 | Add circle member | `circle:16, member:32` |
| 10 | Remove circle member | `circle:16, member:32, key:KeyId` |
| 11 | Raise schema | `version:u32, snapshot:SnapshotId` |
| 12 | Raise format | `version:u16, snapshot:SnapshotId` |
| 13 | Reset | `snapshot:SnapshotId` |

`CircleKeyId` is `circle:16 | key:KeyId`. A member removal records the
replacement store key and the replacement keys of circles the member shared
with others in the author's view (§13). The replacement list is strictly
increasing by circle UUID; its count is encoded even when empty. Replay checks
that it names exactly those shared circles and derives which circles the
removal deletes from the author's view; the entry carries no deletion list.

Each create-store or create-circle entry names its first key; removals name
the replacement keys. Key ids carry no numerical order. Raised versions are
positive. A schema/format raise names a snapshot whose audience is the store
or circle it raises; a reset snapshot names the audience it resets. Authority,
conflicts, causal closure, monotonic version changes and the current key require
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

`LostWrite` is `header:WriteHeader | audience:Audience | row_count:u64 |
cause:LostWriteCause`. Cause is `0 | schema_version:u32` or
`1 | reset_entry:EntryId`. A lost-write header (record tag 4) is followed by
exactly `row_count` kind-4 records with tag 5, each
`change:RowChange`. The count is positive and counts only the snapshot
audience's rows. These rows belong to the preceding lost-write header by their
position, without repeating its WriteId, and have its audience in strictly
increasing RowId order. No other record or end marker can interrupt them; an
extra row after the count is refused. The snapshot header's fifth count counts
lost-write headers, not their rows.
No frame grows with the number of rows in a lost write.

These writes were never applied; nothing in the merged state replaced them.
If the write header contains `WriteDisposition::Lost(version)`, its cause must
be `SchemaChange(version)`. A schema-change cause is positive and no greater
than the snapshot schema version. The lost-write header's audience must match
the snapshot, as must every following row.
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
