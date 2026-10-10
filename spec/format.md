## Appendix D. Storage format

- This appendix gives every byte coven writes to storage, and every code a
  person carries: what [§4](coven.md#4-storage-providers-and-access)
  to [§19](coven.md#19-recovery) store, laid out exactly.
- `coven-format` implements it, with crypto's ciphers for the sealed layers
  ([§20.1](coven.md#201-crates)).
- It is format 2 ([§17.2](coven.md#172-covens-schema)). A
  change to any layout here is a new format version, and older versions'
  readers stay. Coven writes the newest format and reads every older one
  without a store-wide version raise ([§17.2](coven.md#172-covens-schema)).
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
| 13 | Reset | `snapshot:SnapshotId` |
| 14 | Set access | `access:MemberAccess` |

- Tag 12 is unused. The other tags retain their numbers.
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
  audience it raises; a reset names the audience it resets. A creation's
  store id must agree with its path (D9).

### D7 Snapshots

- A snapshot's plaintext is a header frame, its record frames in order,
  then an end frame (kind 7).
- The header (kind 5): `id:SnapshotId | schema_version:u32 | counts:5×u64`,
  one count per section below. Its positions are in its sealed prefix (D9).
- Records (kind 6) are `section:u8 | record`, sections in order, each
  ordered as shown:

  | Section | Record | Order |
  | --- | --- | --- |
  | 0 | Synced row: `row:RowId \| columns:map<name, ColumnValue>` | `RowId` |
  | 1 | Applied write: `id:WriteId \| timestamp:Timestamp \| had_read:WritePositions` | `WriteId` |
  | 2 | Synced column: `table:name \| column:name` | table, column |
  | 3 | Merge row: `row:RowId \| generations:map<u64, WriteId> \| cells:map<name, Cell>` | `RowId` |
  | 4 | Loss: `row:RowId \| generation:u64 \| frozen:bool \| values \| cause` | identity below |

  Empty sections emit nothing. `Cell` is `write:WriteId | value:ColumnValue`.
  Synced-row columns are nonempty. Applied-write timestamps name their
  devices; had-read positions name other devices only, with own earlier
  writes implicit.
- Every loss uses section 4, whether a cell lost concurrently, removal rules
  hid a row, a schema change or reset excluded a write, or a breaking
  migration froze a removed row's losses ([§17.1](coven.md#171-host-application)).
  - `values` is `0 | column:name | cell:Cell` for a cell or
    `1 | cells:map<name, Cell>` for a whole row. All values are as written,
    without foreign-key null substitution. Dismissed cells are absent;
    dismissal that empties a row removes its record.
  - `cause` is `0 | replacing:WriteId`, `1 | rules:set<Rule>`, or
    `2 | excluded:WriteId | boundary`, where `boundary` is
    `0 | version:u32` for a schema change or `1 | entry:EntryId` for a reset.
    A replacing write requires cell values; rules and boundaries require
    row values. Each excluded cell's setter is the excluded write. Its ID
    remains even for a deletion with no old values.
  - A cell's generation is its incarnation; a removed row's is its generation
    when captured. Both are positive and odd. A removed row's
    cells and rules are nonempty. An excluded row keeps the generation of
    its row change; inserts and updates keep their new values and parents,
    deletes their old scalar values with empty parent maps.
  - `frozen` is true for excluded writes and for losses frozen by a breaking
    migration. Migration-frozen values have empty parent maps, keeping their
    written scalar values and names independently of the current schema.
    Active losses follow merge and removal rules; frozen ones do not.
  - A schema-change version is positive and at most the snapshot's schema
    version; a reset entry is covered by its store-log positions.
  - Loss identity orders by row, generation, values identity, frozen flag,
    cause tag, then excluded write ID when present. Values identity is
    `0 | column:name | setter:WriteId` for a cell or
    `1 | setters:map<name, WriteId>` for a row. Columns and maps compare
    logically as in D2. Duplicate identities are refused.
- `Rule` is `0 | ForeignKey`, `1 | check:text` (its name, or its expression
  when unnamed), `2 | entry:EntryId` deleted circle, `3` another audience's row, or
  `4 | Unique`. Rules order by tag, then the foreign-key identity, CHECK
  text, entry id or unique identity. Another audience's row names no winner and
  imposes no ordering on which circle can win. Deleted-circle and
  other-audience rules require a circle. A deleted-circle rule names the
  kept entry that deleted it, covered by the snapshot's store-log positions.
  A delete-circle entry has no companion write or row list (D6).
- Synced-row references obey merge's written-parent checks. A merge row must
  have contiguous generations, valid transition and cell timestamps, no
  cells when deleted, and valid parent generations/audiences. Active cell
  losses must have valid incarnations and replacing writes that had not read
  their setters (§8), with at most one per row, column and setter. An active
  removed-row loss must match its present
  merge row's generation and cells. A present merge row has a synced row
  exactly when it has no active removed-row loss. Frozen losses need no
  merge row or current schema columns.
- All rows and the plaintext header have the sealed prefix's audience.
  Every named write ID and applied write's read positions are covered by
  the prefix's write positions. Positions describe consumed writes,
  including excluded ones; excluded writes' headers and dependencies are
  not retained in loss records.
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
  stuck           [log:u8 | device:DeviceId | number:u64 | failure:u8]
  ```

  `log` is 0 for a device write log, 1 for a store log. `number` is positive
  and identifies the refused write or entry in that device's log. `failure`
  is 0 for decryption/authentication, 1 for signature, 2 for parsing, 3 for
  an invalid write. Records are strictly ordered by `(log, device)`, at most
  one per log. Only judgments made by the posting device appear here; peer
  reports, local judgment times and coven versions do not travel. The D9
  signature authenticates the complete list as that device's report (§19.1).
  Store-log positions also acknowledge received entries, kept or dropped.
  Posting requires every earlier reserved own entry and write to be settled;
  a reader fetches entries through those positions before using the post to
  establish finality (§9). Storage age alone does not establish finality.

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
  | 33 | Store log entry | `key:uuid` |
  | 34 | Snapshot | `audience:Audience \| key:uuid \| writes:WritePositions \| store_log:EntryPositions` |
  | 35 | Posted positions | `key:uuid` |
  | 36 | Join request | nothing |

- Every store-log entry has the same 19-byte prefix including kind and
  version. The create-store frame supplies the first admin's signing key;
  decrypt it, verify the signature with that key, and require its store id
  to match the path. Creation is entry 1 of its authoring device (D6).
- A snapshot has two 64-byte Ed25519 signatures:

  ```
  kind:u8 (34) | version:u16 | prefix | prefix_signature:64 bytes | chunks | signature:64 bytes
  ```

  - `prefix_signature` signs the D11 context of `coven/prefix-signature/v1`,
    the path, and the exact cleartext `kind | version | prefix` bytes. The
    separate label distinguishes this message from a whole-object digest.
  - Verify it before using any prefix field to choose a snapshot, establish
    required history, or decide retention coverage, including reset and
    version-raise boundaries. Bounded ranged reads fetch the routing bytes,
    counted positions and prefix signature without fetching encrypted chunks;
    their keys are unnecessary. Unverified counts may delimit these reads,
    subject to D2's bounds, but no prefix field has authority until verified.
  - The final signature uses `coven/object-signature/v1` as below, hashing
    every preceding byte, including `prefix_signature`. Verify it when loading,
    before applying any snapshot data.
  - Before upload, the writer verifies both signatures, decrypts and checks
    every record, and compares with the fingerprint of the captured database
    state (§15). After upload it checks the complete stored bytes' checksum.
    These checks add no receipt or field to the object.
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
- A snapshot's plaintext length is determined by its listed object length.
  Subtract the cleartext prefix length and both 64-byte signatures to get
  `s`, the sealed section length. With `c = 65536 + 44`, it has
  `n = ceil(s / c)` chunks and `s - 44*n` plaintext bytes. Require `s > 0`
  and a final chunk of 45 through `c` bytes. This computes a size without
  opening the section; loading still checks every chunk and the end frame.
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
  20 0002                       kind 32, version 2
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
| `<store>/devices/<device>/<n>` | A device's write `n` |
| `<store>/store-log/<device>/<n>` | A device's store log entry `n` |
| `<store>/snapshots/<audience>/<device>/<n>` | A device's snapshot `n` of an audience: `store`, or a circle's id |
| `<store>/positions/<device>` | A device's posted positions and stuck records, replaced as either changes |
| `<store>/keys/store/<key>/<member>` | A store key sealed to a member |
| `<store>/keys/circles/<circle>/<key>/<member>` | A circle key sealed to a member |
| `<store>/files/<device>/<file>` | An uploaded file |
| `<store>/join-requests/<invite>` | A join request |

- A device id and `n` are decimal, with no leading zeros; `n` is at least 1.
- Store, circle, key, invite and file ids are lowercase hyphenated UUIDs.
  A member is its public key in lowercase hex.
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
  by label: `coven/encryption/v1` for sealing objects and
  `coven/fingerprints/v1` for fingerprints.
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
- A sealed key at `<store>/keys/…` is:

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
       maps; an encoded empty map if the row is deleted or removed.
  - Every loss has one leaf. Its identity hash has fields `loss` and its
    D7 identity bytes: row, generation, values identity, frozen flag, cause
    tag, and excluded write ID when present, concatenated using D2/D7
    encodings. Its value hash has one field: the complete D7 loss record,
    without the section tag or frame envelope. Thus every retained value,
    setter and cause participates, with dismissed cells absent.
  - Replacing a leaf subtracts its previous hash and adds its new hash in the
    same transaction as the state change. Rows are counted as if a key in
    two audiences were shown in both
    ([§14.2](coven.md#142-moving-rows)).

### D12 Files

- An uploaded file at `<store>/files/<device>/<file>` is:

  ```
  kind:u8 (38) | version:u16 | size:u64 | chunks
  ```

  - Chunks are `ciphertext | tag:16 bytes`, each 65,536 bytes (64 KiB) of
    the file except the last, which may be shorter; a file of size 0 has none.
  - Each is XChaCha20-Poly1305 under the file's own key
    ([§16.2](coven.md#162-storage-and-naming)), with its index
    as the nonce, a 24-byte big-endian number, and associated data binding
    `coven/file-chunk/v1`, the path, the cleartext header and the index.
  - Chunk `i` starts at `11 + i × (65,536 + 16)`, so any range is read
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
    bytes`; `storage` is the provider location and, only for S3, the
    member's access key, at most 16 KiB. OAuth tokens are device-only and
    never appear in a restore code.
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

- Each supported format keeps its own fixtures. The current format's
  `coven-format` fixtures must hold, as hex, one of each frame, sealed object,
  sealed key, file and code, including:
  - a write with a store part and a circle part, the first spanning three
    chunks;
  - all fourteen store-log change tags, including both member-access variants
    across creation and addition, and a removal replacing two circles' keys;
  - a dismissal frame;
  - a snapshot with every section and active, frozen and excluded losses;
  - a migration write.
- Every successful decode re-encodes to the same bytes; tests decode every
  truncation and single-bit change of every fixture without panicking.

- `v2.hex` pins the plaintext frames, then a write prefix/header/part chunks
  and a snapshot prefix/plaintext chunks. Frame mutations exercise the payload decoder,
  including dismissal and migration frames; a successful mutation must
  re-encode to exactly the mutated bytes.
- The sealed fixtures are `sealed-write.hex`, `sealed-store-log.hex`,
  `sealed-snapshot.hex`, `sealed-positions.hex` and `sealed-join-request.hex`;
  `sealed-store-key.hex` and `sealed-circle-key.hex` hold the two key boxes.
  `file.hex` has a full 64-KiB chunk and a 29-byte last chunk; `uploaded-file.txt`
  pins its device-qualified path and uploaded row reference. The code frames
  are `restore-code.hex` and `invite-code.hex`, with their text in `codes.txt`.
  All key material and fixed nonces in these fixtures are public test data.
  Ciphertext, HKDF and signatures must be calculated independently using
  Python hashlib/hmac and libsodium; tests open them through the Rust APIs.
  Snapshot fixtures pin both signatures and the positions fixture pins its
  author signature. Tests reject every truncation and every single-bit change
  of those signatures, and verify their path and author bindings.
