## Appendix D. Storage format

- This appendix gives every byte coven writes to storage, and every code a
  person carries: what [§4](coven.md#4-storage-providers-and-access)
  to [§19](coven.md#19-recovery) store, laid out exactly.
- `coven-format` implements it, with crypto's ciphers for the sealed layers
  ([§20.1](coven.md#201-crates)).
- It is format 1 ([§17.2](coven.md#172-covens-schema)). A
  change to any layout here is a new format version, and older versions'
  readers stay ([§17.2](coven.md#172-covens-schema)).
- Coven's local `_coven_` tables keep their own encodings, versioned by the
  database's schema; they reuse the primitives of D2, and appear here only
  where they must agree across devices: the fingerprint (D11). Their SQL
  names are local schema details, not part of this storage format.

### D1 Rules for every object

- Integers are big-endian, signed ones two's complement; none has a
  variable length.
- Every stored object starts in the clear with `kind:u8 | version:u16`. A
  reader refuses an unknown kind, and asks for an update on a newer version
  instead of calling the object damaged.
- Within an object, every length is checked against what remains before
  anything is allocated, and trailing bytes are refused.
- Every decoded value is checked, and re-encoding it gives the same bytes:
  there is one encoding of each value.
- Maps and sets are written in increasing key order, with no duplicates; a
  reader refuses any other order.
- A *frame* is a bounded plaintext unit: `kind:u8 | version:u16 |
  length:u32 | payload`, at most 16 MiB in all. Sealed objects carry frames
  inside their chunks (D9).

### D2 Primitives

| Name | Bytes | Notes |
| --- | --- | --- |
| `bytes` | `length:u32 \| bytes` | At most 8 MiB. |
| `text` | `bytes`, UTF-8 | Not normalized; may be empty or hold NUL. |
| `name` | `text` | 1 to 1,024 bytes, no NUL: table, column, store, device and circle names. |
| `[T]` | `count:u32 \| T…` | At most 65,536 items; all collections in one frame together at most 65,536. |
| `map<K, V>` | `count:u32 \| (K V)…` | Keys strictly increasing. |
| `set<T>` | `count:u32 \| T…` | Strictly increasing. |
| `Option<T>` | `0`, or `1 \| T` | |
| `uuid` | 16 bytes | UUID byte order. Store, circle, invite, key and file ids. |
| `DeviceId` | `u64` | |
| `MemberId` | 32 bytes | An Ed25519 public key; weak and invalid points refused. |
| `SealingKey` | 32 bytes | An X25519 public key. |
| `Timestamp` | `ms:u48 \| counter:u16 \| device:u64` | 16 bytes; byte order is timestamp order ([§7.2](coven.md#72-timestamps)). |
| `WriteId`, `EntryId` | `device:u64 \| number:u64` | Number at least 1. |
| `WritePositions`, `EntryPositions` | `[device:u64 \| number:u64]` | Strictly increasing by device; zero positions left out. |
| `Audience` | `0`, or `1 \| circle:uuid` | The store sorts first, then circles by id. |
| `Value` | `0` NULL, `1 \| i64`, `2 \| u64` (IEEE 754 bits), `3 \| text`, `4 \| bytes` | NaN and negative zero refused. |
| `RowId` | `table:name \| key:bytes \| audience:Audience` | `key` is an ordered key (D3). |
| `Parent` | `row:RowId \| generation:u64` | The generation is odd: a present row. |
| `Columns` | `[name]` | In declaration order. |
| `ForeignKey` | `columns:Columns \| parent:name \| parent_columns:Columns` | Both lists nonempty and equally long. |
| `Unique` | `terms:[text] \| predicate:Option<text>` | Nonempty terms; each is a column or an expression as written. Terms and predicates use `text`, not `name`. |
| `ColumnValue` | `value:Value \| parents:map<ForeignKey, Parent>` | |
| `MemberKeys` | `signing:MemberId \| sealing:SealingKey` | |
| `MemberAccess` | `0 \| account:text`, or `1 \| access_key_id:text` | A provider account, or an S3 key's public id. |

- Identifiers order as their bytes, except where a row says otherwise. Names
  order by UTF-8 bytes; `RowId` orders by table, encoded key, then audience.
  A `Value` retains its SQLite storage class: an integer and an integral real
  have distinct encodings outside ordered keys. Infinities are valid reals.
- `ForeignKey` orders by its columns, then parent, then parent columns;
  `Unique` by its terms, then its predicate, absent first.
- E.g. `title`, `lower(title)`, and `title WHERE active = 1` are three
  different unique constraints.

### D3 Ordered keys

- A row's key is its key columns' values, encoded so that byte order is
  SQLite's order ([§8.5](coven.md#85-keys-and-uniqueness)):
  numbers, then text by bytes, then blobs by bytes.
- Keys have 1 to 65,536 non-NULL components and at most 8 MiB of encoded
  bytes. These components are inside `bytes`, not a counted collection in
  the containing frame. Text uses binary collation.
- Components follow each other with no count or end; a shorter key that is
  a prefix of a longer one sorts first.
- Numbers, integer or real, share one encoding, so `1` and `1.0` are one key:

  | Tag | Number | Then |
  | --- | --- | --- |
  | `0x10` | −∞ | nothing |
  | `0x11` | negative | `!exponent:u16 \| !significand:u64` |
  | `0x12` | zero | nothing |
  | `0x13` | positive | `exponent:u16 \| significand:u64` |
  | `0x14` | +∞ | nothing |

  - The magnitude is `significand × 2^(exponent − 1137)`, with the
    significand's top bit set; integers are normalized exactly, without
    passing through a 64-bit float.
  - A decoded number is an integer when it is whole and fits in i64.
    Otherwise it must be exactly representable as a nonzero finite IEEE 754
    double; other exponent/significand pairs are refused. NaN and negative
    zero are refused as inputs; zero has only tag `0x12`.
- Text is `0x20`, blobs `0x30`, then their bytes with each `00` written
  `00 FF`, then `00 00`.
- E.g. the key `(7, "a")` is `13 0434 E000000000000000 20 61 00 00`:
  7 is `0xE000000000000000 × 2^(1076 − 1137)`, and 1076 is `0x0434`.

### D4 Plaintext frames

| Kind | Frame | Payload |
| --- | --- | --- |
| 1 | Write header | D5 |
| 2 | Row change | D5 |
| 3 | Dismissal | D5 |
| 4 | Store log entry | D6 |
| 5 | Snapshot header | D7 |
| 6 | Snapshot record | D7 |
| 7 | Snapshot end | empty |
| 8 | Posted positions | D8 |
| 9 | Join request | D8 |
| 10 | Restore code | D13 |
| 11 | Invite code | D13 |

### D5 Writes

- A write's plaintext is a header frame, then one stream per part.
- The header (kind 1):

  ```
  position        WriteId
  timestamp       Timestamp          its device is the write's
  had_read        WritePositions     other devices' logs
  store_log_read  EntryPositions     the store log, own entries included
  schema_version  u32
  disposition     u8                 0 apply · 1 lost, then u32 version · 2 migration
  parts           [audience:Audience | rows:u64 | length:u64]
  ```

  - Schema version zero is representable. A lost disposition's version is
    positive and names the breaking version reached by the store.
  - Parts are in increasing audience order, each with at least one record.
    `rows` counts both row changes and dismissals. Own earlier writes are
    implicit in `had_read`; an explicit own-device position is refused.
  - A migration write has no parts; every other write has at least one
    ([§17.1](coven.md#171-host-application)).
- A part's stream is its records, one frame each, in increasing `RowId`
  order, each of the part's audience; `length` counts every byte of them,
  including each seven-byte frame prefix. Record counts and total stream
  lengths have only their u64 bound; individual frames keep D1/D2's bounds.
  There is no surrounding collection count.
  - A row change (kind 2):

    ```
    row         RowId
    generation  u64                    the row's generation before the change
    operation   u8                     0 insert · 1 update · 2 delete
    columns     map<name, ColumnValue> new values; insert and update only, nonempty
    old         map<name, Value>       insert none; update the same columns; delete any
    ```

  - A delete omits the `columns` field completely; every operation carries
    the `old` map's count, including an insert's zero count. Generations and
    references obey merge's parity, advancement without overflow, and
    parent-generation/audience checks (§8).
  - A dismissal (kind 3) names a lost cell the app dismissed
    ([E4](api.md#e4-reading)):

    ```
    row     RowId
    column  name
    write   WriteId
    ```

  - Dismissing a removed row is a delete of it, an ordinary row change.

  - A row has at most one change in a write, and changes come before
    dismissals of the same row. Those dismissals are ordered by column,
    then write, with no duplicates; each names a write the author had read.
- A part's record count and byte length must match its header. A frame
  announced beyond the part's end is refused before allocation.

### D6 Store log entries

- An entry (kind 4):

  ```
  position   EntryId
  timestamp  Timestamp
  author     MemberId
  had_read   EntryPositions   other devices' entries; its own device's are implicit
  change     u8, then its fields
  ```

| Tag | Change | Fields |
| --- | --- | --- |
| 0 | Create store | `store:uuid \| name:name \| admin:MemberKeys \| access:MemberAccess \| device_name:name \| key:uuid` |
| 1 | Add member | `keys:MemberKeys \| role:u8 \| access:MemberAccess` |
| 2 | Remove member | `member:MemberId \| key:uuid \| circle_keys:[circle:uuid \| key:uuid]` |
| 3 | Change role | `member:MemberId \| role:u8` |
| 4 | Add device | `device:DeviceId \| name:name` |
| 5 | Remove device | `device:DeviceId` |
| 6 | Create circle | `circle:uuid \| name:name \| key:uuid` |
| 7 | Rename circle | `circle:uuid \| name:name` |
| 8 | Delete circle | `circle:uuid` |
| 9 | Add circle member | `circle:uuid \| member:MemberId` |
| 10 | Remove circle member | `circle:uuid \| member:MemberId \| key:uuid` |
| 11 | Raise schema | `version:u32 \| snapshot:SnapshotId` |
| 12 | Raise format | `version:u16 \| snapshot:SnapshotId` |
| 13 | Reset | `snapshot:SnapshotId` |
| 14 | Set access | `access:MemberAccess` |

- A role is `0` admin or `1` member. `SnapshotId` is
  `audience:Audience | device:DeviceId | number:u64`, its path's parts (D10).
  Its number is positive and belongs to the device's snapshot sequence.
  The entry's timestamp names its writing device.
- The create-store entry is number 1 of its device, reads nothing, and its
  `admin` is its author; the device it adds is the one writing it.
- Add-device carries no member id; create-circle carries no member list.
- Set-access is about its author, the member whose access it records.
- `key` names the key the entry brings in ([§11](coven.md#11-keys));
  a removal's `circle_keys` are strictly increasing by circle. Their count
  is present even when zero. Key ids have no numerical ordering or succession.
  A removal carries no list of deleted circles.
- Raised versions are at least 1, and a raise names a snapshot of the
  audience it raises; a reset names the audience it resets. The sealed
  creation identity must agree with the plaintext creation (D9).

### D7 Snapshots

- A snapshot's plaintext is a header frame, its record frames in order,
  then an end frame (kind 7).
- The header (kind 5): `id:SnapshotId | schema_version:u32 | counts:6×u64`,
  one count per section below. Its positions are in its sealed prefix (D9).
- Records (kind 6) are `section:u8 | record`, sections in order, each
  ordered as shown:

  | Section | Record | Order |
  | --- | --- | --- |
  | 0 | Synced row: `row:RowId \| columns:map<name, ColumnValue>` | `RowId` |
  | 1 | Applied write: `id:WriteId \| timestamp:Timestamp \| had_read:WritePositions` | `WriteId` |
  | 2 | Synced column: `table:name \| column:name` | table, column |
  | 3 | Merge row (below) | `RowId` |
  | 4 | Lost write: `header` (D5's header fields through `disposition`, inclusive) `\| audience:Audience \| rows:u64 \| cause` | `WriteId` |
  | 5 | Kept loss: `row:RowId \| values` (below) | row, incarnation, loss identity |

  - Empty sections emit nothing. A synced row's columns are nonempty. An
    applied write's timestamp names its device; its had-read positions name
    other devices only, with own earlier writes implicit.
  - A lost write is followed at once by `rows` records of tag `6`, each a
    row change (D5) of its write, in increasing `RowId` order; the count
    in the header counts lost writes, not their rows. Dismissed cells are
    absent from these changes (including an update's or delete's old values);
    rows and lost-write headers emptied by dismissal are omitted. `rows`
    is positive, covers only this audience, and its row records cannot be
    interrupted by another record or the end marker. Extra rows are refused.
    A lost-write header cannot have the migration disposition.
  - `cause` is `0 | version:u32`, lost to a schema change, or
    `1 | entry:EntryId`, lost to a reset. A schema-change version is positive
    and at most the snapshot's schema version; a reset entry is covered by
    its store-log positions. A header with lost disposition `v` requires
    schema-change cause `v`. These writes were excluded from merge, so they
    are distinct from concurrent lost cells; neither replaces the other.
  - A kept loss is a removed row's loss whose merge records a breaking
    change forgot ([§17.1](coven.md#171-host-application)).
    Its values are `0 | key:LostKey | value:LostValue` for a displaced cell,
    or `1 | generation:u64 | cells:map<name, write:WriteId | value:ColumnValue>
    | replaced_by:set<Rule>` for a removed row. Written values are frozen
    at the migration, with empty parent maps. Within a row and
    incarnation, cells precede rows; cell losses order by `LostKey`, removed
    rows by their column-to-setter maps. Duplicate identities are refused.
    Incarnations are positive and odd; a removed row has nonempty cells and
    removal rules. Names, values, write identities and circle-only rules
    have the same checks as merged rows.
- A merge row is merge's state of one row ([§8](coven.md#8-merge)):

  ```
  row          RowId
  generations  map<u64, WriteId>              the write that started each
  cells        map<name, write:WriteId | value:ColumnValue>
  lost         map<LostKey, LostValue>
  removed      set<Rule>
  ```

  - `LostKey` is `column:name | write:WriteId`; `LostValue` is
    `incarnation:u64 | value:ColumnValue | replaced_by:WriteId`.
    Lost cells and removed rows carry their values as written, without
    foreign-key null substitution.
  - `Rule` is `0 | ForeignKey`, `1 | check:text` (its name, or its
    expression when unnamed), `2` deleted circle, `3` another audience's
    row, or `4 | Unique`. Rules order by tag, then the foreign-key identity,
    CHECK text or unique identity. Another audience's row names no winner
    and imposes no ordering on which circle can win.
- Synced-row references obey merge's written-parent checks. A merge row
  must have contiguous generations, valid transition and cell timestamps,
  no cells when deleted, valid parent generations/audiences, and valid lost
  incarnations and replacing writes that had not read the lost values (§8).
  A removed row is present in merge state; deleted-circle and other-audience
  rules require a circle.
- All row records, lost-write headers and the plaintext header have the
  sealed prefix's audience. Every write id named by applied/merge/kept-loss
  records or lost-write headers is covered by its write positions. Applied
  writes' read positions are covered too; excluded writes' dependencies need
  not be. Positions describe consumed writes, including excluded ones.
- The decoder refuses incorrect section/record order, duplicate identities,
  mismatched counts, audiences and coverage. EOF without the end marker is
  truncation; no records may follow the marker.
- No local row ids, uploads, operations or storage paths occur in a snapshot.

### D8 Small frames

- Posted positions (kind 8):

  ```
  device          DeviceId
  writes          WritePositions
  store_log       EntryPositions
  schema_version  u32
  fingerprints    [audience:Audience | key:uuid | fingerprint:32 bytes]   store first, increasing
  ```

- A join request (kind 9):
  `invite:uuid | keys:MemberKeys | device_name:name`.

### D9 Sealed objects

- Every object in storage but a file is sealed:

  ```
  kind:u8 | version:u16 | prefix | chunks | signature
  ```

  | Kind | Object | Prefix |
  | --- | --- | --- |
  | 32 | Write | `header_key:uuid \| part_keys:[uuid]` |
  | 33 | Store log entry | `key:uuid \| origin:option<store:uuid, timestamp:Timestamp, author:MemberId>` |
  | 34 | Snapshot | `audience:Audience \| key:uuid \| writes:WritePositions \| store_log:EntryPositions` |
  | 35 | Posted positions | `key:uuid` |
  | 36 | Join request | nothing |

- A store-log `origin` is `0` for an ordinary entry, or `1` followed by
  the store id, timestamp and author's public signing key for a create-store
  entry. No other tag is valid. The complete store-log prefix is 20 or 84
  bytes including kind and version. The creation signature is checked with
  this public key before comparing stores in the setup race (§4), without
  decrypting either store. Its timestamp's device must match the path, whose
  number is 1. After opening, the origin must equal the creation frame's
  fields; it is required exactly on create-store entries.
- A snapshot has two 64-byte Ed25519 signatures:

  ```
  kind:u8 (34) | version:u16 | prefix | prefix_signature:64 bytes | chunks | signature:64 bytes
  ```

  - `prefix_signature` signs the D11 context of `coven/prefix-signature/v1`,
    the path, and the exact cleartext `kind | version | prefix` bytes. The
    separate label distinguishes this message from a whole-object digest.
  - Verify it before using any prefix field to choose a snapshot, establish
    required history, or decide retention coverage, including reset and
    version-raise boundaries. It can be checked from one bounded ranged read;
    encrypted chunks and their keys are unnecessary.
  - The final signature uses `coven/object-signature/v1` as below, hashing
    every preceding byte, including `prefix_signature`. Verify it when loading,
    before applying any snapshot data.
  - Both signatures must verify with the member the store log names for the
    device in the path, as of the entries the reader has applied. An unknown
    device, missing signature or wrong signer makes the object damaged (§19.1).
- Posted positions have exactly one chunk followed by the author's 64-byte
  `coven/object-signature/v1` signature. Every read verifies it against the
  member the applied store log names for the device in the path, before using
  positions or fingerprints. An unknown device or missing or wrong signature
  is damaged and counts as not posted (§19.1).
- A chunk is `length:u32 | nonce:24 bytes | ciphertext | tag:16 bytes`:
  XChaCha20-Poly1305 under the encryption key derived from the named key.
  Writes and store log entries derive the nonce with HMAC-SHA256 from that
  encryption key, path, section and chunk index (D11); other objects use
  random nonces. Readers use the nonce field as stored. `length` is the
  ciphertext's, which is the plaintext's, so the chunk takes `length + 44` bytes. Empty chunks are
  refused. A chunk holding one frame has 7 bytes to 16 MiB of plaintext; a
  stream chunk has 1 byte to 64 KiB, and only its section's last may be shorter
  than 64 KiB.
- Chunks are grouped in *sections*, each sealed with one key:
  - a write: section 0 is the header frame, one chunk with the prefix's
    `header_key`; section `i + 1` is part `i`'s stream, with
    `part_keys[i]`, cut into 64 KiB chunks, the last shorter;
  - a snapshot: one section, its frames cut into 64 KiB chunks, to the end
    of the encrypted data, before the final signature;
  - an entry, positions or a join request: one section of one chunk
    holding its frame.
  - The write prefix has exactly one key per declared part, including no
    part keys for a migration write.
  - A write's parts' chunk counts come from its header; a device that
    can't open a part still finds its end.
- Each chunk's associated data binds, as in D11's context encoding, the
  label `coven/object-chunk/v1`, the object's path, its whole cleartext
  `kind | version | prefix`, the section and the chunk's index in it.
  A snapshot's prefix signature is not part of this associated data.
  - So a provider can't change the prefix, swap, drop or reorder chunks, or
    move an object to another path, unnoticed.
  - A truncated snapshot lacks its end frame.
- The signature is 64 bytes of Ed25519, by the author's member key, over
  the context of the label `coven/object-signature/v1`, the path, and the
  SHA-256 of every byte of the object before the signature.
  - A join request is signed by the requester's new member key, the one
    its frame carries.
- E.g. Ana's write 12, with a store part and a Gifts part:

  ```
  20 0001                       kind 32, version 1
  <store key id>                header_key
  00000002 <store key id> <Gifts key id>    part_keys
  <length><nonce><header frame sealed><tag>                 section 0
  <length><nonce><notes row 47 sealed><tag>                 section 1, chunk 0
  <length><nonce><pins row 4 sealed><tag>                   section 2, chunk 0
  <64-byte signature>
  ```

### D10 Paths

| Path | Holds |
| --- | --- |
| `devices/<device>/<n>` | A device's write `n` |
| `store-log/<device>/<n>` | A device's store log entry `n` |
| `snapshots/<audience>/<device>/<n>` | A device's snapshot `n` of an audience: `store`, or a circle's id |
| `positions/<device>` | A device's posted positions, replaced as they advance |
| `keys/store/<key>/<member>` | A store key sealed to a member |
| `keys/circles/<circle>/<key>/<member>` | A circle key sealed to a member |
| `files/<device>/<file>` | An uploaded file |
| `join-requests/<invite>` | A join request |

- A device id and `n` are decimal, with no leading zeros; `n` is at least 1.
- Ids are lowercase hyphenated UUIDs; a member is its public key in
  lowercase hex.
- A path is used exactly as written here, with no other spelling, and is
  bound into its object's authentication.

### D11 Keys, contexts and fingerprints

- A *context* encodes a list of byte strings as each one's
  `length:u64 | bytes`; it is used as associated data, as HKDF's info, and
  for nonce derivation and D9's `coven/object-signature/v1` and
  `coven/prefix-signature/v1` messages.
  A number in a context, such as a section or a chunk's index, is one
  string of its 8 bytes, as a `u64`.
- From a store or circle key, HKDF-SHA256 with no salt derives 32-byte keys
  by label: `coven/encryption/v1` for sealing objects,
  `coven/fingerprints/v1` for fingerprints, and `coven/app-data/v1` for the
  app's own data ([§11](coven.md#11-keys)).
- For each write or store-log chunk, derive its 24-byte nonce as:

  ```
  HMAC-SHA256(encryption_key,
    context(UTF8("coven/object-nonce/v1"), UTF8(path), u64(section), u64(index)))[0..24]
  ```

  Here `encryption_key` is the 32-byte key derived with `coven/encryption/v1`,
  `path` is the exact D10 path, and the numbers are big-endian eight-byte
  strings, each context field length-prefixed as above. Sections and indices
  start at zero as in D9; an entry uses section 0, index 0. The label separates
  nonce derivation from other contexts. Every path is used once, and its
  plaintext, prefix and sealing key ids are immutable before the first nonce
  is used. §17.1 converts only untried writes. Re-sealing and deterministic
  Ed25519 signing therefore reproduce the complete object byte for byte.
  This derivation does not apply to snapshots, sealed keys, positions or join
  requests; snapshots and sealed keys retain their originally sealed bytes.
- An invite's secret derives its join request's key with
  `coven/join-request/v1`.
- A sealed key at `keys/…` is:

  ```
  kind:u8 (37) | version:u16 | ephemeral:32 bytes | nonce:24 bytes | ciphertext | tag:16 bytes
  ```

  - The shared secret is X25519 of a fresh ephemeral key and the member's
    sealing key; a contribution of all zeros is refused.
  - Its key is HKDF-SHA256 of the shared secret with the context of
    `coven/sealed-box/v1`, `store` or `circle`, the path, the ephemeral key
    and the member's sealing key; that context is also the associated data.
  - The plaintext is `key:uuid | key bytes:32` for a store key, and
    `circle:uuid | key:uuid | key bytes:32` for a circle key.
- A fingerprint ([§19.1](coven.md#191-noticing)) is
  HMAC-SHA256, under the audience's fingerprint key, of the raw concatenation
  `coven/agreement/root/v1 | audience | sum`. This outer concatenation has no
  context length fields. `audience` is UTF-8 `store` or the lowercase hyphenated
  circle UUID; `sum` is exactly 32 bytes, a big-endian sum modulo 2^256.
  The empty set has sum zero.
  - Each leaf is SHA-256 over the context of `coven/agreement/leaf/v1`, an
    identity hash, and a value hash. Every hash below is SHA-256 over a
    context, with each listed field separately length-prefixed as above.
    Labels, table names and column names used as context fields are raw UTF-8,
    without D2's `text` length prefix. Keys are raw D3 bytes. Typed values,
    maps, sets and write ids use D2/D7 encodings without frame envelopes;
    counts, generations and incarnations used as context fields are `u64`.
  - There is one leaf per row with generations. Its identity hash has fields
    `row`, table, key. Its value hash has these fields, in this order:
    1. `coven/agreement/row/v1`, table, key, generation count;
    2. for each generation in increasing order, the generation and the
       `WriteId` that started it, as two separate fields;
    3. the cells' setters as `map<name, WriteId>`;
    4. only the cells with nonempty parent maps, as
       `map<name, ColumnValue>`, retaining their values and references as written;
    5. the app-visible values as `map<name, ColumnValue>`, with empty parent
       maps; an encoded empty map if the row is deleted or removed;
    6. if a rule removed the row, its cells as `map<name, ColumnValue>` with
       written values and parent maps, then its `set<Rule>`;
       otherwise two zero-length context fields, not encoded empty collections;
    7. the lost-cell count, then, in `LostKey` order, each loss's column,
       setting `WriteId`, incarnation, written `ColumnValue`, and replacing
       `WriteId`, each a separate field.
  - Each row of an excluded write part has its own leaf. Its identity hash
    has fields `excluded`, table, key, and the write's `WriteId`. Its value
    hash has fields generation, values as `map<name, ColumnValue>`, setters
    as `map<name, WriteId>`, and cause (`0 | version:u32` or `1 | EntryId`).
    A delete uses its old scalar values with empty parent maps; every setter
    names the excluded write. Values and setters omit dismissed cells.
  - Each kept loss from D7 section 5 has its own leaf. Its identity hash has
    fields `retired`, table, key, incarnation, column, setter. A cell uses
    its column and setting `WriteId`; a removed row uses an empty column
    field and its `map<name, WriteId>` of setters. Its value hash has fields
    value, setter, replacement kind, replacement. For a cell these are its
    frozen `ColumnValue`, setting `WriteId`, raw UTF-8 `write`, and replacing
    `WriteId`. For a removed row they are its frozen
    `map<name, ColumnValue>`, setters map, raw UTF-8 `rules`, and `set<Rule>`.
    All these frozen values have empty parent maps, as in D7.
  - Replacing a leaf subtracts its previous hash and adds its new hash in the
    same transaction as the state change. Rows are counted as if a key in
    two audiences were shown in both
    ([§14.2](coven.md#142-moving-rows)).

### D12 Files

- An uploaded file at `files/<device>/<file>` is:

  ```
  kind:u8 (38) | version:u16 | chunk_size:u32 | size:u64 | chunks
  ```

  - Chunks are `ciphertext | tag:16 bytes`, each `chunk_size` bytes of the
    file except the last; a file of size 0 has none. `chunk_size` is 64 KiB
    unless the app chose another, from 4 KiB to 8 MiB, inclusive; every
    integer byte size in that range is valid.
  - Each is XChaCha20-Poly1305 under the file's own key
    ([§16.2](coven.md#162-storage-and-naming)), with its index
    as the nonce, a 24-byte big-endian number, and associated data binding
    `coven/file-chunk/v1`, the path, the cleartext header and the index.
  - Chunk `i` starts at `15 + i × (chunk_size + 16)`, so any range is read
    without the rest.
- The file's key and id are in its row's where-column, which coven writes
  as the text `uploaded <device id> <file id> <key in lowercase hex>`, or the decimal
  id of the device that attached it while it waits to upload
  ([§16.1](coven.md#161-kinds-and-where-files-are)). A file moves only from
  that device id to `uploaded`; pinning and caching do not change this value.

### D13 Codes

- A restore code (kind 10): `store:uuid | name:name | member_keys:bytes |
  storage:bytes`.
  - `member_keys` is `CVMK 01 | signing seed:32 bytes | sealing secret:32
    bytes`; `storage` is the provider settings and credentials, at most
    16 KiB.
- An invite code (kind 11): `store:uuid | name:name | invite:uuid |
  secret:32 bytes | storage:bytes`.
- As text, a code is `CVR1-` (restore) or `CVI1-` (invite), then unpadded
  uppercase base32 (`A`–`Z`, `2`–`7`) of its frame followed by a CRC-32C
  of the frame, big-endian.
  - CRC-32C: reflected polynomial `0x82F63B78`, initial and final XOR
    `0xFFFFFFFF`; the check value of `123456789` is `E3069283`.
  - Unused trailing bits are zero; there is no other spelling, and the
    whole is at most 18 KiB before encoding.
  - The checksum catches typing mistakes, not tampering.

### D14 What the fixtures pin

- `coven-format`'s fixtures hold, as hex, one of each frame, sealed object,
  sealed key, file and code, including:
  - a write with a store part and a circle part, the first spanning three
    chunks;
  - all fourteen store-log change tags, including both member-access variants
    across creation and addition, and a removal replacing two circles' keys;
  - a dismissal frame;
  - a snapshot with every section, a lost write and a kept loss;
  - a migration write.
- Every successful decode re-encodes to the same bytes; tests decode every
  truncation and single-bit change of every fixture without panicking.

- `v1.hex` pins the plaintext frames, then a write prefix/header/part chunks
  and a snapshot prefix/plaintext chunks. `migration.hex` also pins the
  migration header separately. Frame mutations exercise the payload decoder,
  including dismissal and migration frames; a successful mutation must
  re-encode to exactly the mutated bytes.
- The sealed fixtures are `sealed-write.hex`, `sealed-store-log.hex`,
  `sealed-snapshot.hex`, `sealed-positions.hex` and `sealed-join-request.hex`;
  `sealed-store-key.hex` and `sealed-circle-key.hex` hold the two key boxes.
  `file.hex` has a full 4-KiB chunk and a 29-byte last chunk; `uploaded-file.txt`
  pins its device-qualified path and uploaded row reference. The code frames
  are `restore-code.hex` and `invite-code.hex`, with their text in `codes.txt`.
  All key material and fixed nonces in these fixtures are public test data.
  Ciphertext, HKDF and signatures were calculated independently using
  Python hashlib/hmac and libsodium; tests open them through the Rust APIs.
  Snapshot fixtures pin both signatures and the positions fixture pins its
  author signature. Tests reject every truncation and every single-bit change
  of those signatures, and verify their path and author bindings.
