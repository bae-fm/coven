## Appendix D. Storage format

- This appendix gives every byte coven writes to storage, and every code a
  person carries: what [§4](coven.md#4-storage-providers-and-access)
  to [§19](coven.md#19-recovery) store, laid out exactly.
- `coven-format` implements it, with crypto's ciphers for the sealed layers
  ([§20.1](coven.md#201-crates)).
- This appendix specifies format **1**, the first, including stable member
  ids and replaceable member keys. Nothing has shipped and no older format
  exists; fixtures must encode these layouts as format 1. After release, a
  layout change gets a new version and released
  versions' readers stay.
  Coven writes the newest format and reads every released older one without a
  store-wide version raise ([§17.2](coven.md#172-covens-schema)).
- Local `_coven_` tables use the database's schema version.
  Their names and encodings are not part of the storage format.

### D1 Rules for every object

- Integers are big-endian, signed ones two's complement; none has a
  variable length.
- Every stored object starts in the clear with `kind:u8 | version:u16`. A
  reader refuses an unknown kind, and asks for an update on a newer version
  instead of calling the object damaged.
  The version for every layout in this appendix is 1, including inner frames
  and code frames. Cryptographic context labels retain their D11 versions;
  they do not identify a storage layout.
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
| `uuid` | 16 bytes | UUID byte order. Store, member, circle, invite, key, attempt and file ids. |
| `DeviceId` | `u64` | |
| `MemberId` | `uuid` | Random stable member identity, independent of keys. |
| `SigningKey` | 32 bytes | An Ed25519 public key; weak and invalid points refused. |
| `SealingKey` | 32 bytes | An X25519 public key. |
| `Timestamp` | `ms:u48 \| counter:u16 \| device:u64` | 16 bytes; byte order is timestamp order ([§7.2](coven.md#72-timestamps)). |
| `StorageTime` | `seconds:u64 \| nanos:u32` | Unix seconds plus a nanosecond fraction from 0 through 999,999,999; retain the provider sample's precision without rounding forward. |
| `WriteId` | `device:u64 \| number:u64` | Number at least 1; assigned at local commit. |
| `Slot`, `EntryId` | `u64` | Slot at least 1; a consumed cursor may be zero. |
| `Audience` | `0`, or `1 \| circle:uuid` | The store sorts first, then circles by id. |
| `Value` | `0` NULL, `1 \| i64`, `2 \| u64` (IEEE 754 bits), `3 \| text`, `4 \| bytes` | NaN and negative zero refused. |
| `RowId` | `table:name \| key:bytes \| audience:Audience` | `key` is an ordered key (D3). |
| `Parent` | `row:RowId \| generation:u64` | The generation is odd: a present row. |
| `Columns` | `[name]` | In declaration order. |
| `ForeignKey` | `columns:Columns \| parent:name \| parent_columns:Columns` | Both lists nonempty and equally long. |
| `Unique` | `terms:[text] \| predicate:Option<text>` | Nonempty terms; each is a column or an expression as written. Terms and predicates use `text`, not `name`. |
| `ColumnValue` | `value:Value \| parents:map<ForeignKey, Parent>` | |
| `MemberKeys` | `signing:SigningKey \| sealing:SealingKey` | Public pair, independent of `MemberId`. |
| `MemberAccess` | `0 \| account:text`, or `1 \| access_key_id:text \| principal:S3Principal` | A provider account, or an S3 key's public id and who holds it; each text at most 1,024 UTF-8 bytes. |
| `S3Principal` | `0` console; `1 \| user:text \| policy`, policy `0 \| name:text` inline or `1 \| arn:text` managed; `2 \| project:text \| email:text \| managed_folder:text`; `3 \| account:text`; `4 \| parent:text` | The non-secret handles deleting the key needs (§13): none for a key made in the console; an IAM user and its prefix policy (AWS, Ceph RGW, SeaweedFS); a GCS service account and its managed folder, the HMAC access id being the key id; a Cloudflare account, the token id being the key id; RustFS's parent user, the service account's access key being the key id. Each text at most 1,024 UTF-8 bytes. |

- A complete admin, key-copy or announcement slot is at most 4 MiB,
  including its prefix, framing, authentication and signature. Reject an
  oversized operation before preparing artifacts. A writes slot has no
  format-wide size limit; its individual writes and frames retain their
  bounds. Provider transfer limits still apply (§4).

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
  the containing frame. Text uses binary collation: every key, unique and
  foreign-key column of a synced table compares by bytes
  ([§8.5](coven.md#85-keys-and-uniqueness)), so equal keys are equal bytes,
  as rows are in the merge model ([Appendix B2](proofs/merge.md#b2-terms)).
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
| 4 | Admin entry | D6 |
| 5 | Snapshot header | D7 |
| 6 | Snapshot record | D7 |
| 7 | Snapshot end | empty |
| 9 | Join request | D8 |
| 10 | Restore code | D13 |
| 11 | Invite code | D13 |
| 12 | Floor | D8 |
| 13 | Execution record | D5 |
| 14 | Slot header | D9 |
| 15 | Snapshot announcement | D9 |
| 16 | Key-copy acknowledgement | empty; the box is in D9’s prefix |

### D5 Writes

- A write's plaintext is a header frame, then one stream per part.
- The header (kind 1):

  ```
  position        WriteId
  timestamp       Timestamp          its device is the write's
  read_through    u64                last applied slot when authored
  schema_version  u32
  disposition     u8                 0 apply · 2 migration
  parts           [audience:Audience | rows:u64 | length:u64]
  ```

  - Schema version zero is representable. Schema exclusion is a received
    interpretation, never a transmitted disposition: tried and untried
    uploads keep their original schema, disposition and plaintext, and
    readers derive conversion or loss from the current schema and the
    row changes' actual data sources (§8, §17.1;
    [`conversions_preserve_uploads`, `receive_losses_converge`](proofs/queue/CovenQueue/Conversion.lean)).
    Without a converter, the received interpretation retains each original
    change as a frozen schema loss. Exclusion is never inherited through
    `read_through`, including implicit own reads. With a converter, one received
    part can contain converted changes and frozen dependent originals;
    the original inner write retains the layout below.
  - E.g. Ben's untried old-schema `title` edit uploads unchanged after he
    migrates; the receiving callback can interpret it as a `name` edit
    without rewriting that object
    ([`conversions_preserve_untried`, `receive_agreement`](proofs/queue/CovenQueue/Conversion.lean)).
  - Parts are in increasing audience order, each with at least one record.
    `rows` counts row changes, execution records and dismissals. Own earlier writes are implicit reads. The containing slot follows
    `read_through`; an earlier own write is in an earlier slot or this batch.
  - A migration write has no parts; every other write has at least one
    ([§17.1](coven.md#171-host-application)).
- The containing slot supplies slot order. A lower-slot device or
  member removal, or replacement of this author's signing credential,
  consumes the writes without effects or losses and records the refusal
  (§10). No storage-time deadline applies.
- Authenticate the original write with its device owner's signing key at
  `read_through`, with §7.1’s creation-binding exception for the first
  device’s pre-setup writes at zero. Retain historical bindings and source
  evidence. Claim
  retries, conversion and snapshot reload never alter this authored past.

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
    old         map<name, Value>       insert none; update the same columns; delete each replaced cell
    generation_source option<ChangeId> absent exactly at generation zero
    replaced_setters  map<name, CellSource>
    creator     Option<ChangeId>       an audience copy's original creation
    execution   Execution              the SQL operations on this row

    ChangeId   = write:WriteId | row:RowId
    CellSource = change:ChangeId | column:name
    Execution  = steps:[Step] | generation_source:Option<ChangeId> |
                 replaced_setters:map<name, CellSource>
    Step       = sequence:u32 | change | key_update:u8
    ```

  - A delete omits the `columns` field completely; every operation carries
    the `old` map's count, including an insert's zero count. Generations and
    references obey merge's parity, advancement without overflow, and
    parent-generation/audience checks (§8).
  - Setting any component of a multi-column foreign key supplies all its
    components in `columns` (and in `old` for an update), even unchanged
    ones, with the same recorded parent on every component. A tuple containing
    NULL records no parent for that key. Overlapping keys and the inputs of generated
    components expand together (§8.4); incomplete tuples are refused.
  - Each row change requires authenticated evidence of the actual
    generation-source write and the setter of every cell it replaced on the
    authoring device (§8). Sources name changes at the same table, key and
    audience; generation zero has no source. Validate that sources were
    read, precede the change's timestamp and establish the stated generation
    or cell in that incarnation. Unavailable evidence waits; invalid evidence
    is refused. Numeric generations, old scalar values, read frontiers and
    the current canonical generation winner cannot substitute for it.
    `ChangeId.row` and `CellSource.column` use the source's original authored
    names, before conversion. Setter map keys use this change's authored column
    names, in strictly increasing name order. An insert has no replaced setters;
    an update includes each previously set cell it replaces, and a delete includes
    every current cell setter. A delete's `old` map holds exactly those cells,
    each with the value its setter wrote, never a displayed substitution such
    as a stale reference's NULL; a receiver refuses a foreign-key action's
    delete whose `old` map differs (§8.8). A cell never
    set in the author's merge state has no setter (the model's `St.cell = none`).
    Generation is the first source role, followed by setter map order; repeated
    source identities in distinct roles are retained. Schema name matching tracks
    the original table and column identities across explicit renames, never scalar
    equality or the receiver's current generation winner. The authenticated original
    header and row frame, including parent links, and the row's own admission are
    retained independently of received interpretations and dismissible loss values.
  - Source evidence and received frozen-change representations must preserve
    original write, row and column identities through conversion and renaming.
    A frozen dependent change keeps its generation, operation, old names and
    scalar values, with failed source identities in authored role order:
    generation first, then replaced setters. Keep repeated identities when
    one write supplied several roles. Frozen display values have no live
    parent links; their original source evidence is retained separately.
    A delete with no scalar values still retains its write/row identity and cause.
  - One row change, including all its columns, is the unit of loss. Converted
    changes and frozen originals can share a received part; frozen originals
    never enter conversion callbacks or current removal rules. Their values
    and original causes survive later raises and snapshot reloads.
    Source identities and per-change verdicts remain separate from dismissible
    losses and consumed slots. Snapshot records and the loss
    API must preserve that evidence and dependency causes (§8, §17.1);
    D7 carries this evidence separately from losses.
  - `creator` is present only on a generation-zero insert that copies a row
    from another audience (§14.2): it names the copied row's original
    creation, at the same table and key in another audience, which the
    author must have read. The copy's owner is that creation's author
    (§8.8). Every other change has no creator; its own write is the
    creation of a generation-zero insert.
  - `execution` lists every SQLite operation the write's SQL made on this
    row: app statements, shared triggers and foreign-key actions, as
    SQLite ran them. A step's `sequence` is its position among all of the
    write's operations, across its parts; steps are in increasing sequence
    order, and only the two halves of one primary-key update share a
    position. That update is a delete of the old key, its `key_update` 1,
    and an insert of the new one: the old key's foreign keys follow their
    ON UPDATE actions, not ON DELETE (§8.4). `key_update` is 0 everywhere else.
  - A step's `change` is `0` when it is exactly the row change's own
    `generation | operation | columns`, which is then written only once, and
    otherwise `1 | generation:u64 | operation:u8 | columns`. An execution
    record's steps always use `1`. Writing the own change again with `1` is
    refused, so every step list has one encoding.
  - A step's generation is the row's before that operation; each step starts
    at the generation the previous one left. An insert step sets every stored
    column, an update step the columns that operation changed, expanded to
    complete reference tuples and generated inputs like a row change, with
    the parents its references named at that moment; a delete sets none.
    Inserting a removed row's key is an update step setting every column.
    Generated columns are never set. Operations on a row after a move into
    this audience in the same write start at the generation the move began.
  - No step says whether a statement, a trigger or a foreign-key action
    made it. A receiver reconstructs that from the original schema and the
    steps themselves, never from a sender's label or cause list (§8.8):
    comparing whole steps, with their columns' parents, against consequences
    of earlier steps through the references replayed up to each step
    ([`action_iff`, `follows_cause`](proofs/merge/CovenMerge/ForeignKeyClassification.lean)).
  - `Execution`'s sources are the generation source and replaced setters the
    steps used beyond those the net change already names, in the same role
    order. A change coven made without SQL has no steps: an audience move,
    a dismissal's delete of a removed row, or a migration's difference.
  - An execution record (kind 13) is `row:RowId | execution:Execution`, for a
    row the write's SQL changed with no net change: inserted and deleted
    again, or left with the values it began with. It has at least one step.
    A row has a row change or an execution record, never both; both kinds
    share the part's `RowId` order and count in its `rows`. Its steps can
    still cause foreign-key actions on other rows, and its sources are data
    sources like a row change's (§8).
  - Every row change and execution record also commits to its part's
    authorship root (D9, D11).
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

<a id="d6-store-log-entries"></a>

### D6 Admin entries

- An admin frame (kind 4) is `change:u8 | fields`. Its containing slot gives
  its `EntryId` and author (D9). Validate against the
  state immediately before that slot. Every claimed entry is final; there
  is no timestamp selection or landing-time admission.

| Tag | Change | Fields |
| --- | --- | --- |
| 0 | Create store | `store:uuid \| name:name \| admin:MemberKeys \| access:MemberAccess \| device_name:name \| key:uuid \| key_hash:32 bytes` |
| 1 | Add member | `member:MemberId \| keys:MemberKeys \| role:u8 \| access:MemberAccess` |
| 2 | Remove member | `member:MemberId` |
| 3 | Change role | `member:MemberId \| role:u8` |
| 4 | Add device | `name:name \| replaces:Option<DeviceId>`; registers the writing device, which `replaces` must not name |
| 5 | Remove device | `device:DeviceId` |
| 6 | Create circle | `circle:uuid \| name:name \| key:uuid \| key_hash:32 bytes` |
| 7 | Rename circle | `circle:uuid \| name:name` |
| 8 | Delete circle | `circle:uuid` |
| 9 | Add circle member | `circle:uuid \| member:MemberId` |
| 10 | Remove circle member | `circle:uuid \| member:MemberId` |
| 11 | Raise schema | `version:u32 \| announcement:Slot` |
| 13 | Reset | `announcement:Slot` |
| 14 | Set access | `member:MemberId \| access:MemberAccess` |
| 15 | Rotate key | `audience:Audience \| key:uuid \| key_hash:32 bytes` |
| 16 | Replace member keys | `member:MemberId \| expected:MemberKeys \| keys:MemberKeys` |

- Tag 12 is unused. A role is `0` admin or `1` member. `SnapshotId` is
  `audience:Audience | device:DeviceId | number:u64`, naming D10's path;
  its number is positive and only identifies an object.
- Creation occupies slot 1. Its slot header's author is the first admin's
  stable id, its device is the first device, and its payload supplies the
  signing key. Check that signature and the path's store id.
- Other entries use the signing binding immediately before their slot.
  Add-device registers the slot's writing device. A replacement retires a
  distinct older device of the same member; it has no log ends.
- Replace-member-keys requires the target's current binding to equal
  `expected`; both new public keys must be unused. Only this change alters
  an existing member's binding. A competing completed replacement requires
  a new call, not an automatic overwrite (§9).
- Set-access changes its author's access, or an admin's target member's S3
  key. The named member must be active. Remove and circle changes use the
  immediately preceding state, including last-member circle deletion.
- `key` and `key_hash` name the introduced key and SHA-256 of its exact 32
  bytes, inside encryption. Rotation changes no membership; the author must
  belong to that audience. Successive rotations take effect in slot order.
- A raise names an earlier announcement for its audience and positive schema
  version; a reset names its reset announcement. Verify purpose, coverage,
  schema and authorization under §15 and §17 before claiming. A later
  reader checks the same facts.

### D7 Snapshots

- A snapshot's plaintext is a header frame, its record frames in order,
  then an end frame (kind 7).
- The header (kind 5): `id:SnapshotId | schema_version:u32 | counts:12×u64`,
  one count per section below. Its covered slot is in its sealed prefix (D9).
- Records (kind 6) are `section:u8 | record`, sections in order, each
  ordered as shown:

  | Section | Record | Order |
  | --- | --- | --- |
  | 0 | Synced row: `row:RowId \| columns:map<name, ColumnValue>` | `RowId` |
  | 1 | Applied write: `id:WriteId \| slot:Slot \| timestamp:Timestamp \| read_through:u64` | `WriteId` |
  | 2 | Synced column: `table:name \| column:name` | table, column |
  | 3 | Merge row: `row:RowId \| generations:map<u64, WriteId> \| cells:map<name, Cell>` | `RowId` |
  | 4 | Loss: `row:RowId \| generation:u64 \| frozen:bool \| values \| cause` | identity below |
  | 5 | Authored change: `header:WriteHeader \| complete:bool \| change:record \| verdict` | `ChangeId` |
  | 6 | Creation: `row:RowId \| write:WriteId \| creator:ChangeId` | `RowId` |
  | 7 | Write evidence: `write:WriteId \| prefix:bytes \| header:bytes \| roots:[32 bytes] \| header_signature:64 bytes \| proof:Multiproof` | `WriteId` |
  | 8 | Original admin slot: `slot:Slot \| object:bytes` | slot |
  | 9 | Original key-copy slot: `slot:Slot \| object:bytes` | slot |
  | 10 | Original announcement slot: `slot:Slot \| object:bytes` | slot |
  | 11 | Last included claim: `device:DeviceId \| number:u64` | device |

  Empty sections emit nothing. `Cell` is `write:WriteId | value:ColumnValue`.
  Synced-row columns are nonempty. Applied-write timestamps name their
  devices; `read_through` is below the containing slot, with own earlier
  writes implicit.
- Every loss uses section 4, whether a cell lost concurrently, removal rules
  hid a row, a schema change excluded a write, or a breaking
  migration froze a removed row's losses ([§17.1](coven.md#171-host-application)).
  - `values` is `0 | column:name | cell:Cell` for a cell or
    `1 | cells:map<name, Cell>` for a whole row. All values are as written,
    without foreign-key null substitution. Dismissed cells are absent;
    dismissal that empties a row removes its record.
  - `cause` is `0 | replacing:WriteId`, `1 | rules:set<Rule>`,
    `2 | write:WriteId | version:u32` for a schema-excluded loss, or
    `3 | write:WriteId | sources:[ChangeId]` for a dependent row change (§8):
    its write and the failed sources in role order, repeats retained:
    generation, replaced setters and additional execution sources, each
    naming the loss's key and audience, then foreign-key causes, which are
    changes of the loss's own write (§8.8), or
    `4 | write:WriteId | reason` for an unauthorized row change (§8.8), where
    `reason` is `0 | owner:MemberId` for a change of that member's owned row
    or `1` for a foreign-key effect not derived exactly from authorized
    changes.
    Resets create no loss records (§19.3).
    A replacing write requires cell values; rules and schema changes require
    row values. The schema loss's `write` is the excluded write, or the
    migration write that deleted a hidden row through its generation.
    Excluded-write values name that write as setter; a migration-deleted
    row keeps each original setter. The identity remains even for an
    excluded deletion with no old values.
  - A cell's generation is its incarnation; a removed row's is its generation
    when captured. Both are positive and odd. A removed row's
    cells and rules are nonempty. An excluded row keeps the generation of
    its row change; inserts and updates keep their new values and parents,
    deletes their old scalar values with empty parent maps.
  - Dependent and unauthorized values name their own write as setter and
    have empty parent maps; the identity remains for a delete with no old
    values.
  - `frozen` is true for schema-excluded, dependent and unauthorized rows, and for losses frozen by a breaking
    migration. Migration-frozen values have empty parent maps, keeping their
    written scalar values and names independently of the current schema.
    Active losses follow merge and removal rules; frozen ones do not.
    Dropping a table or column freezes its affected pending losses with
    their original cause, names and setters; a row loss keeps its whole
    value map. Freezing does not require current schema columns (§17.1).
  - A schema-change version is positive and at most the snapshot's schema
    version.
  - Loss identity orders by row, generation, values identity, frozen flag,
    cause tag, then the schema, dependency or unauthorized loss's write ID when present. Values identity is
    `0 | column:name | setter:WriteId` for a cell or
    `1 | setters:map<name, WriteId>` for a row. Columns and maps compare
    logically as in D2. Duplicate identities are refused.
- `Rule` is `0 | ForeignKey`, `1 | check:text` (its declared name, or its full
  schema expression with outer ASCII whitespace removed when unnamed),
  `2 | entry:EntryId` deleted circle, `3` another audience's row, or
  `4 | Unique`. Rules order by tag, then the foreign-key identity, CHECK
  text, entry id or unique identity. CHECK expression text preserves quoting,
  comments and internal whitespace; it is never SQLite's abbreviated failure
  token. For example, `"n">0` and `"n"<10` remain distinct rule names in a loss.
  Rules use this one representation in SQLite metadata and snapshot losses;
  there is no abbreviated-name decoding form. Another audience's row names
  no winner and imposes no ordering on which circle can win. Deleted-circle and
  other-audience rules require a circle. A deleted-circle rule names the
  entry that deleted it, at or below the snapshot's covered slot.
  A delete-circle entry has no companion write or row list (D6).
- Synced-row references obey merge's written-parent checks. A merge row must
  have contiguous generations, valid transition and cell timestamps, no
  cells when deleted, and valid parent generations/audiences. Active cell
  losses must have valid incarnations and replacing writes that had not read
  their setters (§8), with at most one per row, column and setter. An active
  removed-row loss must match its present
  merge row's generation and cells. A present merge row has a synced row
  exactly when it has no active removed-row loss. Frozen losses need no
  live cells or current schema columns. A migration-deleted row still
  carries its advanced even generation in section 3 (§17.1).
- Rows and the plaintext header have the prefix's audience. Every named
  published write and source has a containing slot at or below coverage.
  The consumed prefix includes cut-off and reset-ignored writes; their
  verdicts remain. Reset eligibility uses `read_through`. Passing unreadable
  circle parts does not prove they were loaded; rejoining reloads them.
- Sections 8–10 occur only in the store snapshot. Keep original signed admin,
  key-copy and announcement slots through coverage, including creation.
  Verify each object using its original slot path.
- These records preserve historical bindings, key introductions, exposure and announcements.
  Derive admin state from them in order; retained later slots supply newer bindings.
- Section 11 occurs in every snapshot, once per device known through its coverage.
  It records that device's last claim number in the covered slots and the SHA-256
  of that slot's complete bytes, or zero and no digest without one.
- Include every slot kind, valid or refused, including writes whose parts were
  unreadable or reset-ignored. This number settles every claim after slot deletion
  ([§6](coven.md#settling-a-claim)).
- Authenticate the snapshot before using these numbers. They establish that claims
  were included, not that their effects survived.
- The decoder refuses incorrect order, duplicate identities, mismatched counts,
  audiences or coverage. EOF without the end frame is truncation; nothing may follow it.
- No local row ids, upload queues, operation journals or local paths occur
  in a snapshot. Loading leaves these local obligations intact.

- Section 5 holds every original row change of the audience whose write
  the snapshot's covered slot includes and which this device holds: the D5 header
  of its part, whether that part's every change is known (so a named row's
  absence is proof), the original D5 row change with its generation source
  and replaced setters, and the author's verdict. `verdict` is
  `0` unjudged (its effects arrived in an earlier snapshot), `1` applied,
  `2 | version:u32` excluded by the breaking change to that version, `3`
  refused by the earlier retirement or credential-replacement slot, `4 | sources:[ChangeId]` dependent,
  `5 | entry:EntryId` ignored by that reset (§19.3), which it had not
  read, `6 | reason` unauthorized, with loss cause 4's `reason`, or
  `7 | causes:[ChangeId]` applied as the exact foreign-key effect of those
  changes of its own write: each action's root, then its intermediate
  causes, in execution order (§8.8). Only unjudged, applied and derived
  changes survive as sources. Its write and every source it names must be
  covered by its consumed prefix; dismissing a loss never changes a verdict.
  A loader keeps the same original bytes for one identity and adopts the
  snapshot's verdicts with its data.
- Snapshot evidence must preserve original change identities, actual
  generation/setter sources and their own admission facts independently of
  dismissible losses and consumed slots. Old-credential cut-offs feed
  the same per-change rule as schema and device-retirement exclusions (D5).
  Dependent changes retain their original generation, operation, column names
  and values, even an empty-valued delete. Their dependency cause names failed
  sources in generation-then-setter role order, retaining repeated ids.
  A later reader, conversion, loss dismissal or snapshot reload cannot turn
  that excluded source into retained data. Preserve originals in successor snapshots while they support source
  verdicts or current authorship; physical deletion follows §15.
  Section 5 and loss cause 3 supply that evidence for
  [B17](proofs/merge.md#b17-receive-order-progress-and-dropping-by-data).
- A section 5 `record` is `0 | row change` or `1 | execution record`,
  each the D5 frame body without its envelope: an execution record is
  source evidence too, and its steps can cause foreign-key actions (§8.8).
- Section 6 holds each row's selected creation (§8.8): `write` is the
  generation-zero insert with the lowest slot, then lowest write number, and `creator` the
  creation it names, `write` and `row` themselves unless that insert copied
  the row from another audience. `creator` has the row's key, and names
  its table as its own write did, before any later rename; it is in another
  audience exactly when `write` copied it.
  It holds a row whenever any write has created it, through deletion,
  removal and dismissal; a loader replaces the audience's creations with
  these.
- Section 7 lets the snapshot carry another member's authorship without
  that write's parts. For every creation of an `owner_only()` row, and every
  cell of a present owned row, set by a write whose device's member is not
  the snapshot's signer, it holds that write's evidence. Writes of the
  snapshot's own device are its signer's.
  - `prefix` is the write object's exact cleartext `kind | version | prefix`,
    `header` its sealed header chunk with its length field, and `roots` and
    `header_signature` the D9 header tail. `proof` opens, in this
    audience's part, each needed creation leaf and each leaf of a cell the
    row still shows from that write, and nothing else (D11):

    ```
    Multiproof = leaves:u64 | opened:[index:u64 | leaf:bytes] | hidden:[32 bytes]
    ```

    `opened` is in increasing index order; `hidden` has one root for each
    maximal subtree with no opened leaf, left to right. An overwritten value
    is never opened; its subtree's hash stands in for it.
  - A migration write sets no leaves: its evidence has an empty proof, and
    its authenticated header's disposition shows it is one.
  - The loader opens the header with the store key its prefix names,
    verifies the header signature with the signing key of the write
    device's owner in the header's `read_through`, and checks `proof`
    against the root of this audience's part. A key not yet held waits; any
    other failure refuses the snapshot. Then every creation and present cell
    it attributes to another member must be among the opened leaves, at the
    row, incarnation, column and value it claims. A snapshot cannot name
    another member as an owned row's creator or setter without them.
  - A loader keeps the openings: its own later snapshot reuses them for the
    same writes, so it can carry evidence for parts it never held.

<a id="d8-small-frames"></a>

### D8 Floor and join frames

- The floor (kind 12) names the first kept slot and one snapshot per required
  audience ([§15](coven.md#the-floor-and-reloading)):

  ```
  author     DeviceId
  at         u64
  first      Slot
  snapshots  [announcement:Slot]
  ```

- `at` is the signer's applied slot and covers all named announcements.
  Verify the signer against the membership at that slot (D9).
- References follow audience order, store first. Every named snapshot covers
  at least `first - 1`; different audiences may have different coverages.
- The encrypted body starts with this frame. It then carries each named
  announcement as a kind-6 frame: `slot:Slot | object:bytes`.
- Each announcement occurs once, in header order, followed by the kind-7 end frame.
  There is no total-size bound; individual frames retain D1's bounds.
- Retention replaces this entire record conditionally before deleting slots below
  `first`. Conflicts reread the floor and cannot move it backward (§15).
- A join request (kind 9) is
  `invite:uuid | member:MemberId | keys:MemberKeys | device_name:name`.
  Verify its supplied signing key and require the invite's fixed member id.

### D9 Sealed objects

- Stored objects use `kind:u8 | version:u16 | prefix | chunks | signature`,
  except D12 files. Inner writes use the same framing but are not separate
  storage objects.

  | Kind | Object | Prefix |
  | --- | --- | --- |
  | 32 | Inner write | `header_key:uuid \| part_keys:[uuid]` |
  | 33 | Sequence slot | `slot_kind:u8 \| key:uuid \| copy:Option<CopyBox>` |
  | 34 | Snapshot | `audience:Audience \| key:uuid \| covers:u64 \| purpose:Purpose` |
  | 36 | Join request | empty |
  | 39 | Floor | `key:uuid \| copies:[slot:Slot \| object:bytes]` |

- `Purpose` is `0` ordinary, `1 | version:u32` raise, or `2`
  reset. A raise's version is positive and equals its snapshot schema.
  The admin entry names the exact announcement activating its prepared state.
- Slot kinds are 0 admin, 1 writes, 2 key copy, 3 snapshot announcement.
  `copy` is present exactly for kind 2. `key` names the store key that seals
  the slot's body; a prerequisite copy uses an accepted store key (§11).
- Every slot starts its encrypted body with a kind-14 header:
  `device:DeviceId | claim:u64 | author:MemberId | after:u64`.
  `claim` is positive, from the device's one counter for all slot kinds (§6).
  Retries of that claim retain its number; it is independent of inner write numbers.
  The device and claim number are readable by every member, not by the provider.
  `after` is exactly its slot minus one.
  Admin, copy and announcement slots use the device owner's preceding current key;
  creation supplies it in D6. Registration uses the new device's existing member key.
  A writes slot uses the signing credential shared by its inner writes,
  resolved from their authored views. Check that historical signature before
  consuming a batch cut off by an earlier retirement or replacement (§10).
- After this header, the plaintext body is:
  - admin: one D6 frame;
  - writes: `count:u64`, then `length:u64 | inner write` for each queued
    write, in strictly increasing write-number order, all by the header's
    device, with no omitted unconfirmed predecessor;
  - key copy: one empty kind-16 frame;
  - announcement: one kind-15 frame:
    `snapshot:SnapshotId | covers:u64 | purpose:Purpose | digest:32 bytes`.
    Its coverage is below this slot. The digest is SHA-256 of the complete
    snapshot. Applying the announcement uses only these signed fields; a reader
    loading the body checks it against the digest and its prefix against
    the announced coverage and purpose.
- A writes slot's body streams across 64-KiB chunks; there is no enclosing
  bounded collection or frame. Check every length before allocation and
  stream each inner write. Every other slot holds its two frames in one
  chunk, subject to D2's complete-slot limit. A completed create includes
  all uploaded parts; a partial slot is never visible (§4).
- `CopyBox` is `audience:Audience | key:uuid | member:MemberId |
  sealing:SealingKey | ephemeral:32 bytes | nonce:24 bytes |
  ciphertext:bytes | tag:16 bytes`. Its ciphertext is 48 bytes for a store
  key or 64 for a circle key (D11). Kind and routing remain clear; the
  anonymous box and the slot's final signature authenticate them. The box
  may be opened tentatively before the store key is held, but membership,
  introduction and slot signatures must verify before applying anything.
  Its box context uses the slot prefix through `sealing`, excluding the
  ephemeral key and ciphertext. D11 adds the ephemeral key separately.
- A floor's `copies` retain the exact complete original key-copy slots
  needed to open its key, in slot order. They are evidence, not newly sealed
  copies. Verify each with its original slot path and historical binding
  from the authenticated store snapshot. The floor's signer is `author` in the
  state at `at`; `at` covers all referenced announcements. The encrypted
  author and `at` fields have no authority until verification. Verify the
  floor and the snapshots' historical evidence before committing a reload.
- Snapshots have a 64-byte prefix signature immediately after their prefix,
  before chunks. It signs `context("coven/prefix-signature/v1", path,
  kind | version | prefix)`. Check it before using coverage; verify the final
  signature before loading. Resolve the snapshot device's key at `covers`
  from local history or its store snapshot. An unknown author waits.
- An inner write carries its authorship roots and header signature between
  its header chunk and part chunks:

  ```
  kind (32) | version | prefix | header chunk | roots:(32 bytes)×parts |
  header_signature:64 bytes | part chunks | signature:64 bytes
  ```

  Its context identity is UTF-8 `coven/write/<store>/<device>/<number>`, with
  D10's canonical id spellings. This is not a storage path. Both signatures,
  chunk authentication and nonce derivation use this identity where other
  objects use their path. Claiming another slot changes only the outer
  envelope; the inner bytes and authored `read_through` stay fixed.
- Each root is D11's keyed authorship root for that part. The header signature
  signs the context `coven/write-header-signature/v1`, identity, exact clear
  prefix, complete sealed header chunk and concatenated roots. Resolve its
  signing key at `read_through`; check each opened part against its root.
  The final signature covers the roots and header signature too.
- A chunk is `length:u32 | nonce:24 bytes | ciphertext | tag:16 bytes` under
  the named key's derived XChaCha20-Poly1305 key. `length` is plaintext size.
  A frame chunk holds 7 bytes through 16 MiB; stream chunks hold 1 through
  65,536 bytes, with only the last in a section shorter. Empty chunks fail.
- Sections are: inner write header at 0 under `header_key`, each part at
  `i+1` under `part_keys[i]`; all other objects have section 0. Snapshots,
  floors and writes slots stream; admin/copy/announcement slots and
  join requests use one chunk. Inner part counts and lengths come from D5.
  Unreadable parts are hashed and passed over without opening them.
- `header_key` names a store key, so every member can read D5's part audiences
  before opening parts with their audience keys. The provider sees neither
  these audiences nor the slot's device or claim number; the slot's kind,
  key-copy routing and object framing remain clear.
- Chunk associated data is D11's context of `coven/object-chunk/v1`, path or
  inner identity, complete clear prefix, section and chunk index. Snapshot
  prefix signatures are outside this context. Derive deterministic nonces
  for inner writes and slots as D11 specifies; other objects retain random
  nonces and their sealed bytes across retries.
- The final signature is Ed25519 over the context of
  `coven/object-signature/v1`, path or inner identity, and SHA-256 of every
  preceding byte. It authenticates lengths, headers and skipped ciphertext.
  Check the complete signature before committing a slot or snapshot.
- A snapshot or floor stream's encrypted length follows from the complete
  object's length minus its prefix and signatures. With `c = 65536 + 44`,
  `n = ceil(sealed_length / c)` and plaintext size is `sealed_length - 44*n`.
  Require a nonempty stream, a final chunk of 45 through c bytes, and the
  plaintext end frame. Length alone never authenticates its body.
- Claim retries follow [§6](coven.md#settling-a-claim).
  The journal fixes the slot path and bytes before each create.

- A delivered S3 key keeps its signed anonymous box:

  ```
  kind:u8 (40) | version:u16 | member:MemberId | sealing:SealingKey | author:MemberId |
  box_length:u32 | ephemeral:32 bytes | nonce:24 bytes | ciphertext | tag:16 bytes |
  signature:64 bytes
  ```

  Its prefix ends at `author`; plaintext is `access_key_id:text |
  secret_access_key:text | principal:S3Principal`, at most 8 KiB, each text
  at most 1,024 bytes. `box_length` includes 72 bytes of box overhead.
  Verify the author's signature, open only the matching recipient box and
  use it only after that author's claimed SetAccess makes this key current.
  D11 binds its path and prefix into the box.

### D10 Paths

| Path | Holds |
| --- | --- |
| `<store>/seq/<n>` | One create-once sequence slot |
| `<store>/floor` | Signed, encrypted snapshot references and first retained slot; conditional replacement |
| `<store>/snapshots/<audience>/<device>/<n>` | One snapshot, discovered by its announcement |
| `<store>/access-keys/<member>/<attempt>` | S3 key delivery owned by its operation |
| `<store>/files/<device>/<file>` | Uploaded file |
| `<store>/join-requests/<invite>` | Join request |

- Device ids and numbers are decimal without leading zeros; n starts at 1.
  The sequence has no gaps before retention. Snapshot numbers identify
  objects, never discovery cursors. No per-device log or key-copy path exists.
- Audience is `store` or a circle UUID. Store, member, circle, invite,
  attempt and file ids are canonical lowercase hyphenated UUIDs.
- Every immutable path is created once and retries compare exact bytes.
  Only the floor is replaceable. A path has only this spelling and is
  bound into its object's authentication. Bootstrap discovers creation at
  slot 1 or in the floor's store snapshot; there is no separate creation marker.

### D11 Keys and contexts

- A *context* encodes a list of byte strings as each one's
  `length:u64 | bytes`; it is used as associated data, as HKDF's info, and
  for nonce derivation and D9's `coven/object-signature/v1`,
  `coven/prefix-signature/v1` and `coven/write-header-signature/v1` messages.
  A number in a context, such as a section or a chunk's index, is one
  string of its 8 bytes, as a `u64`.
- From a store or circle key, HKDF-SHA256 with no salt derives 32-byte keys
  by label: `coven/encryption/v1` for sealing objects and
  `coven/row-authorship/v1` for authorship leaves.
- A write part's authorship root is a binary hash tree over its leaves,
  split as RFC 6962 splits a Merkle tree: a tree of `n > 1` leaves has the
  largest power of two below `n` on its left.
  - A leaf is `HMAC-SHA256(authorship_key, 0x00 | leaf bytes)`, with the key
    derived from the part's key; a branch is `SHA-256(0x01 | left | right)`;
    an empty part's root is `SHA-256(0x02)`. Keying the leaves keeps a reader
    of another audience from testing a guessed row against a published
    root. Branches are unkeyed, so a device that kept a part's leaf hashes
    can open its leaves again without the key.
  - Leaves follow the part's record order. A row change has its value
    leaves, then one per step; an execution record one per step;
    dismissals have none:

    ```
    0 | row:RowId | generation:u64 | creator:Option<ChangeId>     an insert's creation
    1 | row:RowId | generation:u64 | kind:u8 | column:name | value:ColumnValue
                                     each column an insert (kind 0) or update (1) set
    2 | row:RowId | generation:u64                                a delete
    3 | row:RowId | sequence:u32 | generation:u64 | operation:u8 | columns | key_update:u8
                                                                  each SQL operation
    ```

    An insert has its creation leaf even when it sets no other column, and
    it carries no values, so ownership stays checkable after every inserted
    value is overwritten. Generations are the row's before the change.
- For each inner-write or slot chunk, derive its 24-byte nonce as:

  ```
  HMAC-SHA256(encryption_key,
    context(UTF8("coven/object-nonce/v2"), UTF8(path), prefix,
      u64(section), u64(index), SHA256(plaintext)))[0..24]
  ```

  Here `encryption_key` is the 32-byte key derived with `coven/encryption/v1`,
  `path` is the exact D10 path or D9 inner-write identity, `prefix` is the exact cleartext
  `kind | version | prefix` from D9, and `plaintext` is this chunk's complete
  plaintext, including any frame bytes. The numbers are big-endian eight-byte
  strings, each context field length-prefixed as above. Sections and indices
  start at zero as in D9. The label separates
  nonce derivation from other contexts. Hashing bounds the content field at
  32 bytes and uses the existing SHA-256 primitive. Neither plaintext nor its
  unkeyed hash is published; only the keyed nonce reaches storage.

  A different chunk or prefix changes these inputs even after rollback or
  copying a device. Collision resistance, rather than mathematical injectivity
  of a 24-byte value, is the cryptographic boundary. The plaintext, prefix,
  format and sealing key ids remain fixed for retries; deterministic sealing
  and Ed25519 signing reproduce the complete object byte for byte.

  This derivation does not apply to snapshots, anonymous boxes,
  floors or join requests; snapshots and sealed keys retain
  their originally sealed bytes.
- An invite's secret derives its join request's key with
  `coven/join-request/v1`.
- A sealed audience key in a sequence slot has D9's clear routing prefix and box:
  - The shared secret is X25519 of a fresh ephemeral key and the member's
    exact sealing key named in D9's prefix; a contribution of all zeros is
    refused. The prefix must match the recipient key actually used by the box.
  - Its key is HKDF-SHA256 of the shared secret with the context of
    `coven/sealed-box/v1`, `store`, `circle` or `s3-key`, the path, the exact D9
    clear prefix, the ephemeral key and the member's sealing key, in that
    order; that context is also the associated data.
  - The plaintext is `key:uuid | key bytes:32` for a store key, and
    `circle:uuid | key:uuid | key bytes:32` for a circle key. The recipient
    must match the prefix's member and sealing credential, opening with that
    historical private key rather than trying the latest binding, and check the opened audience and key id
    against its prefix. It checks SHA-256 of the 32 key bytes against the
    authorized D6 introduction; a mismatch is invalid, not another value
    under that key id.
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
- The attaching write sets the where-column to the text
  `file <device id> <file id> <key in lowercase hex>`.
  - The device id is decimal without leading zeros; the file id is its
    canonical UUID. The key is 32 bytes, encoded as 64 lowercase hex digits.
    Components have one ASCII space between them and no surrounding spaces.
  - The store id is the row's store, giving the fixed path
    `<store>/files/<device>/<file>`. The key stays inside encrypted row data.
  - Upload completion, pinning and caching never change this reference.
    File status follows [§16.1](coven.md#161-kinds-and-where-files-are);
    a write does not record availability. A row without a file has NULL here and in its hash column.

### D13 Codes

- A restore code (kind 10): `store:uuid | name:name | member:MemberId | member_keys:bytes |
  storage:bytes`.
  - `member_keys` is `CVMK 01 | signing seed:32 bytes | sealing secret:32
    bytes`; `storage` is the provider location only, at most 16 KiB.
    The private-pair blob is unchanged and explicitly belongs to `member`.
    Restore requires that id's current pair; an old code cannot rebind it.
    A restore code never carries storage credentials: neither an S3 access
    key nor OAuth tokens. A device-to-device QR transfer or iCloud Keychain
    may carry the S3 key alongside the code (§12.1), outside this frame.
- An invite code (kind 11): `store:uuid | name:name | invite:uuid | member:MemberId |
  secret:32 bytes | initial_key:uuid | inviting_writer:DeviceId |
  read_through:u64 | storage:bytes`.
  The key is the one introduced by creation. `read_through` is the inviting
  device's applied slot when issuing the invite. A joiner checks subsequent
  slot routing for its copy, or follows the floor when that prefix is gone
  (§12.2). Routing scans establish no applied progress or authority.

  The admin fixes `member`: a fresh id for addition, or an existing active id
  for replacement. The signed request must repeat that id. Approval publishes
  its exact pair, after prerequisite copies; bootstrap requires a claimed matching
  id-and-pair binding from the approving writer named in the invite, never
  merely an active id, another writer's equal pair or an available key copy.
- As text, a code is `CVR1-` (restore) or `CVI1-` (invite), then unpadded
  uppercase base32 (`A`–`Z`, `2`–`7`) of its frame followed by a CRC-32C
  of the frame, big-endian.
  These text envelopes and `CVMK 01` identify their own encoding; the inner
  frame carries storage format 1.
  - CRC-32C: reflected polynomial `0x82F63B78`, initial and final XOR
    `0xFFFFFFFF`; the check value of `123456789` is `E3069283`.
  - Unused trailing bits are zero; there is no other spelling, and the
    whole is at most 18 KiB before encoding.
  - The checksum catches typing mistakes, not tampering.

### D14 What the fixtures pin

- Each supported format requires fixtures of every frame, slot kind, sealed
  object, file and code. Format 1 includes:
  - a writes slot containing store and circle parts, streaming chunks,
    authorship roots, SQL execution records and a migration write;
  - every admin tag and first-slot creation, stable member ids, expected and
    replacement member keys, access principals and key introductions;
  - key copies for old and current credentials, with signed clear routing;
  - snapshot announcements and snapshots with every evidence section,
    active/frozen losses, first-slot creations and per-device last-claim numbers;
  - a floor with original boxes, announcements and the first kept slot;
  - restore and invite codes, signed join requests and delivered S3 keys.

- Every successful decode re-encodes to the same bytes; tests decode every
  truncation and single-bit change of every fixture without panicking.

- Fixture keys and nonces are public test data. Independently calculate
  ciphertext, derivation and signatures using Python hashlib/hmac and
  libsodium, then open through Rust. Mutations must reject changed paths,
  routing credentials, slot order, authored views, roots and signatures.
  A retry at the same slot repeats every byte; a different slot changes its
  outer authentication while retaining each inner write exactly.
- Fixtures cover an advanced floor with deletion unfinished, and last-claim
  numbers settling lost-reply claims of every slot kind. Refused slots and
  writes with reset-ignored parts count too.
- Fixtures cover snapshots for audiences at different slots and the schema
  rule in §15. Regenerate format-1 ciphertext and signatures for these layouts.

- Fixtures must pin D11's content-bound nonce recipe:
  Ana retries an identical chunk, changes its plaintext at the same path,
  and changes only the cleartext prefix. The first case repeats every byte;
  the latter two use different nonce inputs.
