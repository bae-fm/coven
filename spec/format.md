# Coven storage format

Every device in a store reads the same shared sequence of slots, and there is no
server in between to translate. Not every device can open everything in it: a
circle's parts open only for that circle's members (§15), and other devices pass
over them unopened. But what two devices both read, they must read alike. So
every device must turn the same bytes into the same values, and the same values
back into the same bytes. Three parts of coven depend on that:

- **Settling a claim.** A device that lost the reply to a create compares the
  slot's bytes with the bytes it sent (§5.1). If one write had two encodings,
  Ana's own slot would look like someone else's.
- **Signatures.** Every signature and authentication tag covers exact bytes. A
  reader that re-encoded a value differently couldn't check them.
- **Retries.** A retry must reproduce its object byte for byte, so a lost reply
  never leaves two versions of one slot (§6.3).

This document gives every byte coven writes to storage, and every code a person
carries. [coven.md](coven.md) says what each object is for; this document says
exactly how it is laid out.

**How to read references.** A section sign (§) refers to coven.md. D1 to D14 are
this document's sections; the code, api.md and coven.md cite them by those
numbers, so the numbers never change. They are not coven.md's Appendix D, whose
parts are written with a dot (D.1 to D.6) and always as "Appendix D".

**Who implements it.** The `coven-format` crate is the byte codec. It encodes
and decodes everything here, using `coven-crypto`'s ciphers for the sealed
layers, and checks the rules an object's own bytes can show: bounds, order, tags
and the fields' agreement with each other. Rules that need the store's history
are checked by the crates that hold it
([coven.md Appendix F.1](coven.md#f1-crates)):

- `coven-database` checks a write's data sources against authenticated evidence
  (D5.6);
- `coven-sync` checks who may sign each slot and object, as of its place in the
  sequence (D5.3, D9).

For example, `coven-format` decodes a well-formed write whose `read_through` is
zero, whichever device wrote it. Only `coven-sync`'s check of signing authority
refuses it when that device isn't the store's first (§7.1).

**Format 1.** This document specifies format **1**, the first. It includes
stable member ids and replaceable member keys. Nothing has shipped and no older
format exists, so fixtures encode these layouts as format 1. After release:

- a layout change gets a new version, and readers of released versions stay;
- coven writes the newest format and reads every released older one, without a
  store-wide version raise ([§18.9](coven.md#189-covens-own-schema-and-format),
  Appendix C.2).

**What is not here.** Coven's local `_coven_` tables use the database's schema
version (Appendix C.1). Their names and encodings are not part of the storage
format.

**Examples.** They follow coven.md's household: Ana, Ben, Carol and Dan share
Household on Dropbox, and Ana and Ben share the Gifts circle. Ids in examples are
made up: Household's store id is `5d0c8e2a-7b1f-4c3e-9a64-0f2d8b71c3e5`, and
Ana's phone is device 4242. Bytes are written in hex, two digits per byte;
spaces only separate fields for reading. Example times are UTC.

## Contents

- [A tour](#a-tour)
- [D1 Rules for every object](#d1-rules-for-every-object)
- [D2 Primitives](#d2-primitives)
- [D3 Ordered keys](#d3-ordered-keys)
- [D4 Plaintext frames](#d4-plaintext-frames)
- [D5 Writes](#d5-writes)
- [D6 Admin entries](#d6-admin-entries)
- [D7 Snapshots](#d7-snapshots)
- [D8 Floor and join frames](#d8-floor-and-join-frames)
- [D9 Sealed objects](#d9-sealed-objects)
- [D10 Paths](#d10-paths)
- [D11 Keys and contexts](#d11-keys-and-contexts)
- [D12 Files](#d12-files)
- [D13 Codes](#d13-codes)
- [D14 What the fixtures pin](#d14-what-the-fixtures-pin)
- [What's proved](#whats-proved)

---

## A tour

### What coven stores

| Object | Where it lives | What it holds | Section |
| --- | --- | --- | --- |
| Sequence slot | `<store>/seq/<n>` | one of the four slot kinds below | D9 |
| · admin entry slot | | one membership or settings change | D6, D9 |
| · writes slot | | one device's queued writes, each an inner write | D5, D9 |
| · key copy slot | | one audience key sealed to one member | D9, D11 |
| · snapshot announcement slot | | a snapshot's id, coverage, purpose and hash | D9 |
| Inner write | inside a writes slot | one write: a header, then one part per audience | D5, D9 |
| Floor | `<store>/floor` | the first kept slot, one snapshot per audience, and the original key copies a new device needs | D8, D9 |
| Snapshot | `<store>/snapshots/<audience>/<device>/<n>` | one audience's state at its coverage | D7, D9 |
| File | `<store>/files/<device>/<object>` | one stored file, encrypted in chunks | D12 |
| Join request | `<store>/join-requests/<invite>` | a would-be member's id, public keys and device name | D8, D9 |
| Restore code, invite code | on paper or a screen, never in storage | what a new device needs to find and open the store | D13 |
| Saved restore code | iCloud Keychain or Block Store, never in storage | a restore code with the store's name, for the person's next devices | D13 |
| Bucket marker | `coven-bucket`, at the bucket's root, only where member keys reach the whole bucket | the id of the one store that owns the bucket, as plain text | D10 |

### Three layers

Every stored object is built in three layers. Here they are for slot 41, the
writes slot Ana won in §5.1:

```
<store>/seq/41                          path (D10), bound into the object's authentication
└─ sealed object, kind 33               clear prefix, encrypted chunks, final signature (D9)
   └─ plaintext, carried in chunks      decrypted with the store key (D11)
      ├─ slot header frame, kind 14     device, claim number, author, previous slot (D9)
      └─ the batch: inner writes        each itself sealed, kind 32 (D9)
         └─ write header frame, kind 1, then one stream of record frames per part (D5)
            └─ values: RowId, Value, Timestamp, … (D2, D3)
```

- **Values** are built from fixed primitives (D2). Row keys use a special
  ordered encoding (D3).
- **Frames** are bounded plaintext units that carry values (D1). D4 lists them;
  D5 to D8 give their contents.
- **Sealed objects** wrap frames in encryption and signatures (D9), at paths
  from D10, using the keys and contexts of D11.

Files, codes and the bucket marker are the three exceptions. A file has its own
simpler envelope (D12); a code is a frame turned into text (D13); the bucket
marker is a store id in plain text, with no envelope at all (D10).

### Kind numbers

Every object and every frame, except the bucket marker, starts with a kind byte.
Format 1 uses these:

| Kind | What | Section |
| --- | --- | --- |
| 1 | Write header frame | D5 |
| 2 | Row change frame | D5 |
| 3 | Dismissal frame | D5 |
| 4 | Admin entry frame | D6 |
| 5 | Snapshot header frame | D7 |
| 6 | Snapshot record frame; the floor also carries its announcement copies in kind-6 frames | D7, D8 |
| 7 | Snapshot end frame; also ends the floor's body | D7, D8 |
| 9 | Join request frame | D8 |
| 10 | Restore code frame | D13 |
| 11 | Invite code frame | D13 |
| 12 | Floor frame | D8 |
| 13 | Execution record frame | D5 |
| 14 | Slot header frame | D9 |
| 15 | Snapshot announcement frame | D9 |
| 16 | Key-copy acknowledgement frame | D9 |
| 17 | Saved restore code frame | D13 |
| 32 | Inner write | D9 |
| 33 | Sequence slot | D9 |
| 34 | Snapshot | D9 |
| 36 | Join request | D9 |
| 38 | File | D12 |
| 39 | Floor | D9 |

A kind not in this table doesn't exist in format 1, and a reader refuses it (D1).

### Reading order

The sections build on each other. Read them in this order:

1. **The rules every byte follows:** D1, then the primitive values (D2) and
   ordered keys (D3).
2. **What objects say:** the frame list (D4), then writes (D5), admin entries
   (D6), snapshots (D7), and the floor and join request (D8).
3. **How objects are wrapped:** sealed objects (D9), paths (D10), and the keys and
   contexts that seal and sign them (D11).
4. **The two exceptions:** files (D12) and codes (D13).
5. **How the format is tested:** fixtures (D14).

To follow one object end to end instead, take slot 41: D9 for its envelope, D5
for the writes inside, D11 for the cryptography.

---

## D1 Rules for every object

Two devices that disagreed about one byte would disagree about everything built
on it: whose slot it is, whether a signature holds, which row a key names. So the
format allows exactly one encoding of each value, and a reader refuses anything
else rather than guessing.

**Integers** are big-endian, and signed ones are two's complement. No integer
has a variable length.

**Every stored object starts in the clear with `kind:u8 | version:u16`,** except
D10's unsealed bucket marker.

- A reader refuses an unknown kind.
- A reader that meets a newer version asks for an update instead of calling the
  object damaged (§18.9).
- The version of every layout in this document is 1, including inner frames and
  code frames.
- Cryptographic context labels keep their own versions (D11), such as
  `coven/object-nonce/v2`. A label's version doesn't identify a storage layout.

**Lengths are checked before allocation.** Within an object, every length is
checked against what remains before anything is allocated. Trailing bytes are
refused.

**One encoding per value.** Every decoded value is checked, and re-encoding it
gives the same bytes.

**Maps and sets** are written in increasing key order, with no duplicates. A
reader refuses any other order.

**A frame** is a bounded plaintext unit, `kind:u8 | version:u16 | length:u32 | payload`:

| Field | Type | Meaning |
| --- | --- | --- |
| `kind` | `u8` | which frame (D4) |
| `version` | `u16` | 1 |
| `length` | `u32` | the payload's length; the seven bytes before it aren't counted |
| `payload` | `length` bytes | the frame's fields |

A frame is at most 16 MiB in all. Sealed objects carry frames inside their
chunks (D9).

*Example.* The smallest frame is a key-copy acknowledgement (kind 16), whose
payload is empty: `10 0001 00000000`, seven bytes.

---

## D2 Primitives

Every field in this document is built from these types. Each has a single
encoding and fixed bounds, so a reader knows how much to allocate before it
reads.

### Types

| Name | Bytes | Rule |
| --- | --- | --- |
| `bytes` | `length:u32 \| bytes` | At most 8 MiB. |
| `text` | `bytes`, UTF-8 | Not normalized; may be empty or hold NUL. |
| `name` | `text` | 1 to 1,024 bytes, no NUL. Table, column, store, device and circle names. |
| `[T]` | `count:u32 \| T…` | At most 65,536 items. All collections in one frame together hold at most 65,536. |
| `map<K, V>` | `count:u32 \| (K V)…` | Keys strictly increasing. |
| `set<T>` | `count:u32 \| T…` | Strictly increasing. |
| `bool` | `u8` | `0` false, `1` true. Any other byte is refused. |
| `Option<T>` | `0`, or `1 \| T` | |
| `uuid` | 16 bytes | UUID byte order. Store, member, circle, invite, key, attempt and file ids. |
| `DeviceId` | `u64` | |
| `MemberId` | `uuid` | A random, stable member identity, independent of keys. |
| `SigningKey` | 32 bytes | An Ed25519 public key; weak and invalid points are refused. |
| `SealingKey` | 32 bytes | An X25519 public key. |
| `Timestamp` | `ms:u48 \| counter:u16 \| device:u64` | 16 bytes. Byte order is timestamp order ([§7.2](coven.md#72-timestamps)). |
| `StorageTime` | `seconds:u64 \| nanos:u32` | Unix seconds plus a nanosecond fraction from 0 through 999,999,999. Keeps the provider sample's precision, without rounding forward. |
| `WriteId` | `device:u64 \| number:u64` | The number is at least 1, assigned at local commit. |
| `Slot` | `u64` | A slot is at least 1. A consumed cursor, such as `read_through`, may be zero. |
| `SnapshotId` | `audience:Audience \| device:DeviceId \| number:u64` | Names a snapshot's D10 path. The number is positive and only identifies an object. |
| `Audience` | `0`, or `1 \| circle:uuid` | The store sorts first, then circles by id. |
| `Value` | `0` NULL, `1 \| i64`, `2 \| u64` (IEEE 754 bits), `3 \| text`, `4 \| bytes` | NaN and negative zero are refused. |
| `RowId` | `table:name \| key:bytes \| audience:Audience` | `key` is an ordered key (D3). |
| `Parent` | `row:RowId \| generation:u64` | The generation is odd: a present row. |
| `Columns` | `[name]` | In declaration order. |
| `ForeignKey` | `columns:Columns \| parent:name \| parent_columns:Columns` | Both lists nonempty and equally long. |
| `Unique` | `terms:[text] \| predicate:Option<text>` | Nonempty terms. Each term is a column or an expression, as written. Terms and predicates use `text`, not `name`. |
| `ColumnValue` | `value:Value \| parents:map<ForeignKey, Parent>` | A cell's value, and the parent each reference through it names. |
| `MemberKeys` | `signing:SigningKey \| sealing:SealingKey` | A member's public pair, independent of `MemberId`. |
| `MemberAccess` | `0 \| account:text`, or `1 \| access_key_id:text` | A provider account, or the public id of an S3 key made in the provider's console. Each text at most 1,024 UTF-8 bytes. |

### Examples

| Value | Bytes |
| --- | --- |
| the name `notes` | `00000005 6E6F746573` |
| `Value` NULL | `00` |
| `Value` integer 7 | `01 0000000000000007` |
| `Value` real 1.5 | `02 3FF8000000000000` |
| `Value` text `a` | `03 00000001 61` |
| `Audience` store | `00` |
| `Option<T>` absent | `00` |
| `WriteId` of Ana's phone's write 3 | `0000000000001092 0000000000000003` |

### Order

Maps, sets and snapshot sections are sorted, so every type needs one order.

- Identifiers order as their bytes, except where a row above says otherwise.
- Names order by their UTF-8 bytes.
- `RowId` orders by table, then encoded key, then audience.
- A `Value` keeps its SQLite storage class: an integer and an integral real have
  distinct encodings outside ordered keys. Infinities are valid reals.
- `ForeignKey` orders by its columns, then its parent, then the parent's columns.
- `Unique` orders by its terms, then its predicate, an absent predicate first.

So `title`, `lower(title)`, and `title WHERE active = 1` are three different
unique constraints.

### Size of a slot

- A complete admin, key-copy or announcement slot is at most 4 MiB, including
  its prefix, framing, authentication and signature. An operation that would
  exceed it is rejected before it prepares anything.
- A writes slot has no format-wide size limit. Its individual writes and frames
  keep their bounds.
- Provider transfer limits still apply ([§20](coven.md#20-what-coven-needs-from-a-provider)).

---

## D3 Ordered keys

Coven sorts rows by their encoded key bytes, and compares keys as bytes when it
merges. So the bytes of a key must sort exactly as SQLite sorts the key's values
([§9.2](coven.md#92-keys-and-unique-values)): numbers, then text by bytes, then
blobs by bytes. And two keys SQLite treats as one, such as `1` and `1.0`, must be
one key.

### Example

The key `(7, "a")` is `13 0434 E000000000000000 20 61 00 00`:

```
13 0434 E000000000000000      7: positive, exponent 0x0434 = 1076, significand
20 61 00 00                   "a": text tag, its byte, end
```

7 is `0xE000000000000000 × 2^(1076 − 1137)`.

| Key | Bytes |
| --- | --- |
| −1 | `11 FBCD 7FFFFFFFFFFFFFFF` |
| 0 | `12` |
| 1, or 1.0 | `13 0432 8000000000000000` |
| 1.5 | `13 0432 C000000000000000` |
| 7 | `13 0434 E000000000000000` |
| `"errands"` | `20 65 72 72 61 6E 64 73 00 00` |
| the blob `00 01` | `30 00 FF 01 00 00` |

The rows are in key order, and so are their bytes.

### Rules

**Components.** A key is its key columns' values, one component each.

- A key has 1 to 65,536 components, none NULL, and at most 8 MiB of encoded
  bytes.
- The components are inside the `RowId`'s `bytes`. They aren't a counted
  collection, so they don't count toward the frame's 65,536 collection items
  (D2).
- Components follow each other with no count and no end marker. A shorter key
  that is a prefix of a longer one sorts first.

**Binary collation.** Text compares by its bytes. Every key, unique and
foreign-key column of a synced table uses binary collation (§9.2), so equal keys
are equal bytes, as rows are in the merge model
([merge write-up, B2](proofs/merge.md#b2-terms)).

**Numbers.** Integers and reals share one encoding, so `1` and `1.0` are one key.

| Tag | Number | Then |
| --- | --- | --- |
| `0x10` | −∞ | nothing |
| `0x11` | negative | `!exponent:u16 \| !significand:u64` |
| `0x12` | zero | nothing |
| `0x13` | positive | `exponent:u16 \| significand:u64` |
| `0x14` | +∞ | nothing |

- The magnitude is `significand × 2^(exponent − 1137)`, with the significand's
  top bit set. A negative number writes both fields inverted (`!`), so larger
  magnitudes sort first.
- Integers are normalized exactly, without passing through a 64-bit float.
- On decoding, a number is an integer when it is whole and fits in i64.
  Otherwise it must be exactly representable as a nonzero finite IEEE 754 double.
  Any other exponent and significand pair is refused.
- NaN and negative zero are refused as inputs. Zero has only tag `0x12`.

**Text and blobs.** Text is tag `0x20`, a blob `0x30`. Then come the bytes, with
each `00` written `00 FF`, then `00 00`.

---

## D4 Plaintext frames

These are the frames that carry coven's content, with the section that gives each
payload. D1 gives the frame's own layout.

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
| 17 | Saved restore code | D13 |

---

## D5 Writes

A write is one SQLite transaction on synced tables (§4). Its bytes must let
every receiver do four things, without trusting the sender's own account of
them: check who wrote it and against what history, merge each row change by
generation and timestamp (§8), check owner permission and foreign-key effects
(§9.6), and decide whether the data each change built on still counts (§9.5).
Those bytes are fixed when the write is first sent, and never change afterwards.

A write's plaintext is a header frame, then one stream per part. A *part* holds
the write's records for one audience.

### Example: Ana's write 3

In §4, Ana's write 3 fixes note 42's title and deletes the tag "errands". Both
rows are in the store, so the write has one part with two records.

Its header frame, at schema version 1:

```
01 0001 00000042              frame: kind 1, version 1, 66-byte payload
0000000000001092 0000000000000003
                              position: Ana's phone (4242), write 3
01A0FC7C4E00 0000 0000000000001092
                              timestamp: 2026-10-02 12:00:00.000, counter 0, Ana's phone
0000000000000014              read_through: slot 20
00000001                      schema_version: 1
00                            disposition: apply
00000001                      parts: one
  00                            audience: the store
  0000000000000002              rows: two records
  <u64>                         length: bytes of the two record frames below
```

Then the part's stream: two row-change frames, in `RowId` order (`notes` before
`tags`). The first, for note 42:

| Field | Value |
| --- | --- |
| `row` | `notes`, note 42's key, the store |
| `generation` | 1, the row's generation before the change |
| `operation` | 1, update |
| `columns` | `title` → `"Grocery list"`, with no parents (`title` is no reference) |
| `old` | `title` → `"Grocry list"` |
| `generation_source` | present: Ana's write 1 at note 42, which created the row |
| `replaced_setters` | `title` → Ana's write 1 at note 42, column `title` |
| `creator` | absent |
| `execution` | one step, whose change is `0` (the row change's own); no further sources |

The second deletes `tags` row `"errands"`, whose key is
`20 65 72 72 61 6E 64 73 00 00` (D3). A delete has no `columns` field; its `old`
map holds each of the tag's cells with the value its setter wrote.

### D5.1 The write header

The header (kind 1):

```
position        WriteId
timestamp       Timestamp          its device is the write's
read_through    u64                last applied slot when authored
schema_version  u32
disposition     u8                 0 apply · 2 migration
parts           [audience:Audience | rows:u64 | length:u64]
```

| Field | Rule |
| --- | --- |
| `position` | the write's device and number |
| `timestamp` | its device is the write's device |
| `read_through` | the last slot the device had applied when the write was made (§7.1) |
| `schema_version` | zero is representable |
| `disposition` | `0` apply, `2` migration |
| `parts` | in increasing audience order. `rows` counts the part's row changes, execution records and dismissals; each part has at least one. `length` is the part's stream length (D5.4). |

**Migration writes.** A migration write (§18.4) has no parts. Every other write
has at least one.

**Read history.** The device's own earlier writes are implicit reads. The
containing slot comes after `read_through`, and an earlier own write is in an
earlier slot or in this batch.

### D5.2 Schema versions: what the receiver decides

A write made at an old schema can arrive after a breaking change (§18.6). The
write's bytes never record whether it was excluded: schema exclusion is a
*received interpretation*, never a transmitted disposition.

- Tried and untried uploads keep their original schema, disposition and
  plaintext. Readers derive conversion or loss from the current schema and the
  row changes' actual data sources (§9.5, §18.6).
- When a breaking migration in the write's range declares `OldWrites::Drop`
  (§18.3), the received interpretation keeps each original change as a frozen
  schema lost value.
- When every breaking migration in the range declares `OldWrites::Convert`,
  one received part can contain converted changes and frozen dependent
  originals. The original inner write keeps the layout below.
- Exclusion is never inherited through `read_through`, including implicit own
  reads.

*Example.* Ben's untried old-schema `title` edit uploads unchanged after he
migrates. The receiving callback can interpret it as a `name` edit without
rewriting that object.

### D5.3 Authenticating a write, and cut-offs

- **Slot order.** The containing slot supplies slot order.
- **Cut-off.** A device or member removal in a lower slot, or a replacement of
  this author's signing credential, consumes the writes without effects or lost
  values and records the refusal (§14.1). No storage-time deadline applies.
- **Signature.** The original write is authenticated with its device owner's
  signing key at `read_through`. The exception is §7.1's creation binding: the
  first device's pre-setup writes at zero.
- **History is kept.** Historical bindings and source evidence are retained.
  Claim retries, conversion and snapshot reload never alter this authored past.

### D5.4 The part stream

A part's stream is its records, one frame each, in increasing `RowId` order,
each in the part's audience.

- `length` counts every byte of the records, including each seven-byte frame
  prefix.
- Record counts and total stream lengths have only their `u64` bound.
  Individual frames keep D1's and D2's bounds.
- There is no surrounding collection count.

Three kinds of record can appear: row changes (D5.5), execution records (D5.9)
and dismissals (D5.10).

### D5.5 Row changes (kind 2)

A row change is one row's insert, update or delete:

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

| Field | Type | Rule |
| --- | --- | --- |
| `row` | `RowId` | the row's table, key and audience |
| `generation` | `u64` | the row's generation before the change (§8.3) |
| `operation` | `u8` | `0` insert, `1` update, `2` delete |
| `columns` | `map<name, ColumnValue>` | new values. Insert and update only, nonempty. A delete omits the field completely. |
| `old` | `map<name, Value>` | an insert's is empty; an update's has the same columns; a delete's has each replaced cell. Every operation carries the map's count, including an insert's zero. |
| `generation_source` | `Option<ChangeId>` | the change that supplied the row's generation. Absent exactly at generation zero. |
| `replaced_setters` | `map<name, CellSource>` | the change that set each cell this change replaced |
| `creator` | `Option<ChangeId>` | present only on an audience copy (D5.7) |
| `execution` | `Execution` | the SQL operations on this row (D5.8) |

A `ChangeId` names one row change: its write and its row. A `CellSource` names a
change and one of its columns.

**Merge checks.** Generations and references obey merge's parity, advancement
without overflow, and parent-generation and audience checks (§8, §9.1).

**References of several columns.** Setting any component of a multi-column
foreign key supplies all its components in `columns`, and in `old` for an update,
even unchanged ones, with the same recorded parent on every component.

- A tuple containing NULL records no parent for that key.
- Overlapping keys expand together (§9.1).
- Incomplete tuples are refused.

**One change per row.** A row has at most one change in a write.

**No generated columns.** No name in a row change is a generated column of the
write's schema: not in `columns`, `old` or `replaced_setters`, and not in a
step's columns (D5.8). SQLite computes each generated column from the row's
other columns, on every device (§8.1). A receiver refuses a write that names
one, as inconsistent, before it judges any of the write's changes, so none of
its values merges or is kept as a lost value. A part consumed without effects,
after a cut-off or a reset, is not checked: none of its values counts (§14.1,
§19.4).

**File records.** A file's record (§17.2) is a row change of coven's own synced
table `coven_files`, in the part of the file's audience, keyed by the file's id.

- An insert carries every cell: `id`, `audience`, `size`, `hash`,
  `coordinates` (D12) and `namespace`.
- An update carries exactly `coordinates` and `hash`, together. A file's id,
  size and namespace never change.
- `hash` is NULL or 32 bytes, and is set exactly when the coordinates name
  stored bytes.
- A receiver refuses a record change that breaks any of these.
- An app row's file reference names its file's record as a foreign key parent
  in the row's own audience (§9.1).

### D5.6 Data sources

Every row change carries the changes whose data it built on: the write that
supplied the row's generation, and the write that set each cell it replaced. A
receiver uses them to drop a change whose data didn't count (§9.5). They must be
the actual sources on the authoring device, so they are named exactly and
checked against authenticated evidence.

**What a change must name.**

- Sources name changes at the same table, key and audience. Generation zero has
  no source.
- An insert has no replaced setters.
- An update includes each previously set cell it replaces.
- A delete includes every current cell setter. Its `old` map holds exactly those
  cells, each with the value its setter wrote, never a displayed substitution
  such as a stale reference's NULL. A receiver refuses a foreign-key action's
  delete whose `old` map differs (§9.6).
- A cell never set in the author's merge state has no setter (the model's
  `St.cell = none`).

**Names.**

- `ChangeId.row` and `CellSource.column` use the source's original authored
  names, before any conversion.
- Setter map keys use this change's authored column names, in strictly
  increasing name order.
- Schema name matching tracks the original table and column identities across
  explicit renames. It never uses scalar equality or the receiver's current
  generation winner.

**Role order.** The generation source is the first source role, followed by the
setters in map order. A source identity repeated in distinct roles is kept once
per role.

**Checking.** Each row change requires authenticated evidence of the actual
generation-source write, and of the setter of every cell it replaced on the
authoring device (§9.5).

- Validate that each source was read, precedes the change's timestamp, and
  establishes the stated generation or cell in that incarnation.
- Unavailable evidence waits; invalid evidence is refused.
- Nothing else stands in for the evidence: not numeric generations, old scalar
  values, read frontiers, or the current canonical generation winner.

**What is kept.** The authenticated original header and row frame, including
parent links, and the row's own admission are retained independently of
received interpretations and dismissible lost values.

**Edge cases: frozen and dependent changes.** A change whose sources were
dropped is kept as a frozen lost value (§9.5). Its bytes must survive everything
that follows.

- Source evidence and received frozen-change representations preserve the
  original write, row and column identities through conversion and renaming.
- A frozen dependent loss keeps its generation, old names and scalar values,
  with the failed source identities in authored role order: generation first,
  then replaced setters. Repeated identities stay when one write supplied
  several roles. Its operation remains in the separately retained original
  change, so dismissing the display loss cannot erase it.
- Frozen display values have no live parent links. Their original source
  evidence is retained separately.
- A delete with no scalar values still retains its write and row identity and its
  cause.
- One row change, including all its columns, is the unit of loss.
- Converted changes and frozen originals can share a received part. Frozen
  originals never enter conversion callbacks or current removal rules. Their
  values and original causes survive later raises and snapshot reloads.
- Source identities and per-change verdicts stay separate from dismissible lost
  values and consumed slots. Snapshot records and the lost-value API must
  preserve that evidence and the dependency causes (§9.5, §18.6). D7 carries this
  evidence separately from lost values.

### D5.7 Copies between audiences: `creator`

When an app puts a taken-out row into another audience by inserting it again
(§8.4), the owner must stay the person who first created it (§9.6). `creator`
carries that.

- `creator` is present only on a generation-zero insert that copies a row from
  another audience. It names the copied row's original creation, at the same
  table and key in another audience, which the author must have read.
- The copy's owner is that creation's author (§9.6).
- Every other change has no creator. A generation-zero insert's own write is its
  creation.

### D5.8 Execution: the SQL operations on a row

A row change records the net effect of a write on a row. But owner checks (§9.6)
must tell an authorized direct change from a foreign-key action, and a write can
touch a row several times. So each row change also lists every SQLite operation
the write's SQL made on that row: app statements, shared triggers and foreign-key
actions, as SQLite ran them.

**Sequence.** A step's `sequence` is its position among all of the write's
operations, across all its parts. The write's first operation is at position 0,
and each later operation takes the next number, except as below.

*Example.* If Ana's write 3 updated note 42 first and then deleted the tag, note
42's step is at 0 and the tag's step at 1.

- Only the two halves of one primary-key update share a position. That update is
  a delete of the old key, with `key_update` 1, and an insert of the new one. The
  old key's foreign keys follow their ON UPDATE actions, not ON DELETE (§9.2).
- `key_update` is 0 everywhere else.
- A receiver checks that each row's steps are in strictly increasing sequence
  order. It doesn't check that the write's positions start at 0 or run without
  gaps.

**The step's change.**

- `change` is `0` when the step is exactly the row change's own
  `generation | operation | columns`, which is then written only once.
- Otherwise it is `1 | generation:u64 | operation:u8 | columns`.
- That `columns` has the row change's own shape (D5.5): an insert or update step
  carries a `map<name, ColumnValue>`, and a delete step omits the field. Despite
  the name, it is not D2's `Columns` list.
- An execution record's steps (D5.9) always use `1`.
- Writing the own change again with `1` is refused, so every step list has one
  encoding.

**What each step sets.**

- A step's generation is the row's before that operation. Each step starts at
  the generation the previous one left.
- An insert step sets every stored column.
- An update step sets the columns that operation changed, expanded to complete
  reference tuples like a row change, with the parents its references named at
  that moment. NULL assigned to a reference that already reads null is no change
  (§9.1).
- A delete step sets none.
- Inserting a removed row's key is an update step setting every column.
- Generated columns are never set (D5.5).
- Operations on a row after a move into this audience in the same write start at
  the generation the move began.

**No labels.** No step says whether a statement, a trigger or a foreign-key
action made it. A receiver reconstructs that from the original schema and the
steps themselves, never from a sender's label or cause list (§9.6). It compares
whole steps, with their columns' parents, against the consequences of earlier
steps, through the references as replayed up to each step.

**Execution sources.** `Execution`'s `generation_source` and `replaced_setters`
are the sources the steps used beyond those the net change already names, in the
same role order (D5.6).

**Changes without SQL.** Audience moves and migration differences have no steps.
A dismissal records its deletes as steps, a parent before the children whose
deletes it causes, so receivers can verify the children's permission (§9.6).

### D5.9 Execution records (kind 13)

A write's SQL can change a row and leave no net change: insert it and delete it
again, or set it back to the values it began with. Those steps still matter: they
can cause foreign-key actions on other rows, and their sources are data sources
like a row change's (§9.5). An execution record keeps them.

```
row        RowId
execution  Execution
```

That is, `row:RowId | execution:Execution`.

- It has at least one step.
- A row has a row change or an execution record, never both. Both kinds share
  the part's `RowId` order and count in its `rows`.

**Authorship.** Every row change and execution record also commits to its part's
authorship root (D9, D11).

### D5.10 Dismissals (kind 3)

A dismissal names a lost value the app dismissed
([api.md E4](api.md#e4-reading)). A frozen delete can have no old values, so it
needs a target even when there is no column to name:

```
row     RowId
target  u8: 0 cell, 1 empty frozen row
column  name, only for target 0
write   WriteId
```

- For a cell, `write` is its setter. For an empty frozen row, it is the
  original change's write; the target applies only while that loss is empty.
- Dismissing a removed row is a delete of it, an ordinary row change, not a
  dismissal.
- Changes come before dismissals of the same row. Cell dismissals come next,
  ordered by column then write; dismissals of empty rows follow, ordered by write.
- There are no duplicates. Each names a write the author had read.

### D5.11 Checking a part

- A part's record count and byte length must match its header.
- A frame announced beyond the part's end is refused before allocation.

---

## D6 Admin entries

Every change to membership or settings is an admin entry in its own slot (§10.2).
Its slot fixes everything outside the entry: the entry is identified by its
slot number, and its slot header names its author (D9). So the entry's frame
carries only what changed.

An admin frame (kind 4) is `change:u8 | fields`.

- It is validated against the state immediately before its slot.
- Every claimed entry is final. There is no timestamp selection or landing-time
  admission.

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
| 17 | Rename store | `name:name` |

- Tag 12 is unused.
- A role is `0` admin or `1` member.

### Creating the store

Creation occupies slot 1 (§10.3). Nobody is a member yet, so the slot carries
what it needs to check itself.

- Its slot header's author is the first admin's stable id, and its device is the
  first device. Its payload supplies the signing key, in `admin`.
- Check the slot's signature with that key, and check the store id against the
  path's.

### Rules for each change

**Signing.** Every other entry uses the signing binding immediately before its
slot.

**Store name.** Rename-store requires an admin in the preceding state and
replaces the current name. Creation supplies the initial name (§10.3).

**Devices.** Add-device registers the slot's writing device. A replacement
retires a distinct older device of the same member.

**Replacing member keys.**

- The target's current binding must equal `expected`.
- Both new public keys must never have been used.
- Only this change alters an existing member's binding.
- A competing completed replacement requires a new call, not an automatic
  overwrite (§10.4, §12.3).

**Access.** Set-access changes its author's own access, or an admin's target
member's S3 key. The named member must be active.

**Removals and circles.** Remove and circle changes use the immediately preceding
state, including deleting a circle whose last member leaves.

**Keys.** `key` and `key_hash` name the key being introduced, and the SHA-256 of
its exact 32 bytes. Both are inside the encryption.

- Rotation changes no membership. Its author must belong to that audience.
- Successive rotations take effect in slot order.

**Raises and resets.**

- A raise names an earlier announcement for its audience, and a positive schema
  version.
- A reset names its reset announcement.
- Before claiming, the author verifies purpose, coverage, schema and
  authorization under §16.3, §18 and §19.4. A later reader checks the same facts.

---

## D7 Snapshots

A snapshot replaces deleted history for one audience (§16). A device that loads
it must end in exactly the state it would have reached by applying every slot
through the snapshot's coverage, and must still be able to judge every late
write against that history: its generations, its data sources, who created each
owned row, and who was a member when. So a snapshot carries both the
audience's state and the evidence old writes still need.

### Layout

A snapshot's plaintext is a header frame, its record frames in order, then an
end frame (kind 7).

The header (kind 5) is `id:SnapshotId | schema_version:u32 | counts:12×u64`:

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `SnapshotId` | which snapshot (D2) |
| `schema_version` | `u32` | the audience's schema version |
| `counts` | `12×u64` | the number of records in each section below |

The covered slot is in the sealed prefix, not the header (D9).

Each record (kind 6) is `section:u8 | record`. Sections come in order, each sorted
as shown. Empty sections emit nothing.

| Section | Record | Order |
| --- | --- | --- |
| 0 | Synced row: `row:RowId \| columns:map<name, ColumnValue>` | `RowId` |
| 1 | Applied write: `id:WriteId \| slot:Slot \| timestamp:Timestamp \| read_through:u64` | `WriteId` |
| 2 | Synced column: `table:name \| column:name` | table, column |
| 3 | Merge row: `row:RowId \| generations:map<u64, WriteId> \| cells:map<name, Cell>` | `RowId` |
| 4 | Loss: `row:RowId \| generation:u64 \| frozen:bool \| values \| cause` | identity, below |
| 5 | Authored change: `header:WriteHeader \| complete:bool \| change:record \| verdict` | `ChangeId` |
| 6 | Creation: `row:RowId \| write:WriteId \| creator:ChangeId` | `RowId` |
| 7 | Write evidence: `write:WriteId \| prefix:bytes \| header:bytes \| roots:[32 bytes] \| header_signature:64 bytes \| proof:Multiproof` | `WriteId` |
| 8 | Original admin slot: `slot:Slot \| object:bytes` | slot |
| 9 | Original key-copy slot: `slot:Slot \| object:bytes` | slot |
| 10 | Original announcement slot: `slot:Slot \| object:bytes` | slot |
| 11 | Last included claim: `device:DeviceId \| number:u64 \| digest:Option<32 bytes>` | device |

`Cell` is `write:WriteId | value:ColumnValue`: the write that set a cell, and the
value it set.

### D7.1 Rows, writes and merge records (sections 0 to 3)

These sections are the audience's state, in the shape of coven's merge records
(Appendix B.1).

- **Synced rows** (section 0) are the audience's present rows that removal rules
  haven't taken out, each with the values its merge records hold as written.
  Their `columns` maps are nonempty, and hold every column but the generated
  ones. These values can differ from what the app's tables show: when a
  nullable reference's parent is deleted, the app reads NULL, but section 0
  keeps the written parent key.
- **No generated columns.** No section names a generated column, as no write
  does (D5.5). A device loading the snapshot computes them, and refuses a
  snapshot that names one.
- **Applied writes** (section 1) name every write applied. Their timestamps name
  their devices. `read_through` is below the containing slot, with own earlier
  writes implicit.
- **Synced columns** (section 2) name each synced column.
- **Merge rows** (section 3) hold each row's generations, with the write that
  moved it to each, and each cell's setting write and value.

**Checks.**

- Each applied write is in a slot from 1 to the snapshot's coverage, and is
  stamped after every write it had read: each in a slot up to its
  `read_through`, and its own device's earlier writes (§7.2).
- Synced-row references obey merge's written-parent checks.
- A merge row must have contiguous generations, valid transition and cell
  timestamps, no cells when deleted, and valid parent generations and audiences.
- A present merge row has a synced row exactly when it has no active removed-row
  lost value.
- A row deleted by a migration still carries its advanced even generation in
  section 3 (§18.7).

### D7.2 Lost values (section 4)

Every lost value uses section 4, whatever caused it: a cell lost concurrently,
removal rules that took a row out, a schema change that excluded a write, or a
breaking migration that froze a removed row's lost values (§18.7). Appendix B.3
describes the same record as coven keeps it locally.

| Field | Type | Meaning |
| --- | --- | --- |
| `row` | `RowId` | the row |
| `generation` | `u64` | which generation the value belongs to (below) |
| `frozen` | `bool` | whether the values are kept outside the merge (below) |
| `values` | `0 \| column:name \| cell:Cell` or `1 \| cells:map<name, Cell>` | one cell, or a whole row |
| `cause` | below | what replaced or dropped the values |

**Values.** All values are as written, without foreign-key null substitution.
Dismissed cells are absent, and a dismissal that empties a row removes its
record.

**Causes.**

| Bytes | Cause | Values |
| --- | --- | --- |
| `0 \| replacing:WriteId` | replaced by a write that hadn't read it (§8.2) | a cell |
| `1 \| rules:set<Rule>` | taken out by removal rules (§8.4) | a row |
| `2 \| write:WriteId \| version:u32` | excluded by a schema change (§18.6) | a row |
| `3 \| write:WriteId \| sources:[ChangeId]` | a dependent row change (§9.5) | |
| `4 \| write:WriteId \| reason` | an unauthorized row change (§9.6) | |

- **Schema-excluded (2).** `write` is the excluded write, or the migration write
  that deleted a hidden row through its generation. The version is positive and
  at most the snapshot's schema version. Excluded-write values name that write
  as setter; a migration-deleted row keeps each original setter. The identity
  remains even for an excluded deletion with no old values.
- **Dependent (3).** `sources` lists the write's failed sources in role order,
  repeats retained: the generation source, the replaced setters, and additional
  execution sources, each naming the lost value's key and audience. Then come
  the foreign-key causes, which are changes of the lost value's own write (§9.6).
- **Unauthorized (4).** `reason` is `0 | owner:MemberId` for a change of that
  member's owned row, or `1` for a foreign-key effect not derived exactly from
  authorized changes.
- Resets create no lost-value records (§19.4).

**Generations.**

- A cell's generation is its incarnation. A removed row's is its generation when
  captured. Both are positive and odd.
- A removed row's cells and rules are nonempty.
- An excluded row keeps the generation of its row change. Inserts and updates
  keep their new scalar values; deletes keep their old scalar values. Like every
  frozen value (D5.6), they have empty parent maps. The original parent evidence
  is retained separately: the parents the change named survive in its original
  row change, which section 5 keeps (D7.3).
- Dependent and unauthorized values name their own write as setter and have
  empty parent maps. The identity remains for a delete with no old values.

**Frozen.** `frozen` is true for schema-excluded, dependent and unauthorized
rows, and for lost values frozen by a breaking migration.

- Active lost values follow merge and removal rules; frozen ones don't.
- All frozen values have empty parent maps, whatever froze them. They keep their
  written scalar values and names independently of the current schema.
- Dropping a table or column freezes its affected pending lost values, with their
  original cause, names and setters. A row's lost value keeps its whole value
  map. Freezing doesn't require current schema columns (§18.7).

**Checks.**

- Active cell lost values must have valid incarnations, and replacing writes
  that hadn't read their setters (§8.2). There is at most one per row, column and
  setter.
- An active removed-row lost value must match its present merge row's generation
  and cells.
- Frozen lost values need no live cells or current schema columns.

**Identity and order.** Section 4 is sorted by each record's identity: its row,
generation, values identity, frozen flag, cause tag, then the write id of a
schema, dependency or unauthorized lost value when present.

- A values identity is `0 | column:name | setter:WriteId` for a cell, or
  `1 | setters:map<name, WriteId>` for a row.
- Columns and maps compare logically, as in D2: by their values, not their
  encoded bytes.
- Duplicate identities are refused.

**Rules.** A `Rule` names one removal rule:

| Bytes | Rule |
| --- | --- |
| `0 \| ForeignKey` | a foreign key |
| `1 \| check:text` | a CHECK: `check` is its declared name, or, when unnamed, its full schema expression with outer ASCII whitespace removed |
| `2 \| entry:Slot` | a deleted circle, with the entry that deleted it |
| `3` | another audience's row |
| `4 \| Unique` | a unique constraint |

- Rules order by tag, then by the foreign-key identity, CHECK text, entry id or
  unique identity.
- CHECK expression text keeps quoting, comments and internal whitespace. It is
  never SQLite's abbreviated failure token. For example, `"n">0` and `"n"<10`
  stay distinct rule names in a lost value (Appendix B.5).
- Rules use this one representation in SQLite metadata and in snapshot lost
  values. There is no abbreviated-name decoding form.
- Another audience's row names no winner, and imposes no order on which circle
  can win.
- Deleted-circle and other-audience rules require a circle.
- A deleted-circle rule names the entry that deleted it, at or below the
  snapshot's covered slot. A delete-circle entry has no companion write or row
  list (D6).

### D7.3 Authored changes (section 5)

A late write can depend on a change the snapshot already judged (§9.5). So the
snapshot keeps every original change and its verdict, separately from the lost
values the app may dismiss.

Section 5 holds every original row change of the audience whose write the
snapshot's covered slot includes and which this device holds.

| Field | Meaning |
| --- | --- |
| `header` | its write's header fields, without `parts` (below) |
| `complete` | a `bool`: whether every change of that part is known, so a named row's absence is proof |
| `change` | `0 \| row change` or `1 \| execution record`: the D5 frame body without its envelope, with its generation source and replaced setters. An execution record is source evidence too, and its steps can cause foreign-key actions (§9.6). |
| `verdict` | the author's verdict, below |

| Verdict | Meaning |
| --- | --- |
| `0` | unjudged: its effects arrived in an earlier snapshot |
| `1` | applied |
| `2 \| version:u32` | excluded by the breaking change to that version |
| `3` | refused by the earlier retirement or credential-replacement slot |
| `4 \| sources:[ChangeId]` | dependent |
| `5 \| entry:Slot` | ignored by that reset (§19.4), which it had not read |
| `6 \| reason` | unauthorized, with lost-value cause 4's `reason` |
| `7 \| causes:[ChangeId]` | applied as the exact foreign-key effect of those changes of its own write: each action's root, then its intermediate causes, in execution order (§9.6) |

**The header.** Each change must name its write's identity, time, history and
schema. It doesn't need the write's list of parts: its own part is its row's
audience. So `header` is a `WriteHeader`: D5.1's header fields up to
`disposition`, with no frame envelope and no `parts` list. It is always 45 bytes:

```
position        WriteId      16 bytes
timestamp       Timestamp    16 bytes
read_through    u64           8 bytes
schema_version  u32           4 bytes
disposition     u8            1 byte
```

`complete` follows it directly.

**Rules.**

- Only unjudged, applied and derived changes survive as sources.
- A change's write, and every source it names, must be covered by its consumed
  prefix.
- Dismissing a lost value never changes a verdict.
- A loader keeps the same original bytes for one identity, and adopts the
  snapshot's verdicts with its data.

**Evidence that must survive.**

- Snapshot evidence preserves original change identities, actual generation and
  setter sources, and their own admission facts, independently of dismissible
  lost values and consumed slots.
- Old-credential cut-offs feed the same per-change rule as schema and
  device-retirement exclusions (D5.3).
- Dependent changes keep their original generation, operation, column names and
  values, even an empty-valued delete. Their dependency cause names failed
  sources in generation-then-setter role order, keeping repeated ids.
- A later reader, conversion, lost-value dismissal or snapshot reload can't turn
  an excluded source into retained data.
- Successor snapshots preserve originals while they support source verdicts or
  current authorship. Physical deletion follows §16.6.

Section 5 and lost-value cause 3 supply the evidence for the merge model's data
rule (see What's proved).

### D7.4 Creations (section 6)

An owned row's owner is the member whose write created it first (§9.6), and that
must survive deletion and reloads. Section 6 holds each row's selected creation.

- `write` is the generation-zero insert with the lowest slot, then the lowest
  write number.
- `creator` is the creation it names: `write` and `row` themselves, unless that
  insert copied the row from another audience (D5.7).
- `creator` has the row's key, and names its table as its own write did, before
  any later rename. It is in another audience exactly when `write` copied it.
- Section 6 holds a row whenever any write has created it, through deletion,
  removal and dismissal.
- A loader replaces the audience's creations with these.

### D7.5 Write evidence (section 7)

A snapshot carries only one audience's rows, but owner checks need proof of who
wrote them. Carol's snapshot can't simply claim that Ana created an owned row:
it must carry Ana's signed evidence. Section 7 lets the snapshot carry another
member's authorship without that write's parts.

**What it holds.** For every creation of an `owner_only()` row, and every cell of
a present owned row, set by a write whose device's member is not the snapshot's
signer, section 7 holds that write's evidence. Writes of the snapshot's own
device are its signer's.

| Field | Meaning |
| --- | --- |
| `write` | the write |
| `prefix` | the write object's exact cleartext `kind \| version \| prefix` |
| `header` | its sealed header chunk, with its length field |
| `roots`, `header_signature` | the D9 header tail |
| `proof` | a multiproof, below |

**The proof** opens, in this audience's part, each needed creation leaf and each
leaf of a cell the row still shows from that write, and nothing else (D11):

```
Multiproof = leaves:u64 | opened:[index:u64 | leaf:bytes] | hidden:[32 bytes]
```

- `opened` is in increasing index order.
- `hidden` has one root for each maximal subtree with no opened leaf, left to
  right.
- An overwritten value is never opened. Its subtree's hash stands in for it.
- A migration write sets no leaves. Its evidence has an empty proof, and its
  authenticated header's disposition shows it is one.

*Example.* Ana's write 3's part has four leaves (D11): note 42's title, its step,
the tag's delete, and its step. To prove the title alone: `leaves` is 4,
`opened` holds leaf 0, and `hidden` holds leaf 1's hash and the hash of the
subtree over leaves 2 and 3.

**Loading.**

1. Open the header with the store key its prefix names.
2. Verify the header signature with the signing key of the write device's owner,
   as of the header's `read_through`.
3. Check `proof` against the root of this audience's part. A key not yet held
   waits; any other failure refuses the snapshot.
4. Every creation and present cell the snapshot attributes to another member
   must be among the opened leaves, at the row, incarnation, column and value it
   claims.

So a snapshot can't name another member as an owned row's creator or setter
without their evidence.

A loader keeps the openings. Its own later snapshot reuses them for the same
writes, so it can carry evidence for parts it never held.

### D7.6 Original slots (sections 8 to 10)

A device that loads the store snapshot must still check old signatures and know
which keys reached whom. So the store snapshot keeps the signed slots
themselves, not a summary.

- Sections 8 to 10 occur only in the store snapshot.
- They keep the original signed admin, key-copy and announcement slots through
  coverage, including creation. Verify each object using its original slot path.
- These records preserve historical bindings, key introductions, exposure and
  announcements. Admin state is derived from them in order; retained later slots
  supply newer bindings.

### D7.7 Last included claims (section 11)

A device whose claim's slot was deleted settles it from the floor's snapshot
(§5.3). Section 11 makes that possible. The device compares the claim number it
recorded with the number section 11 holds for it: a lower number means the claim
didn't land; an equal number with the digest of its recorded bytes means it
landed; a higher number, or an equal one with another digest, means another copy
uses its device id.

- It occurs in every snapshot, once per device known through its coverage.
- It records that device's last claim: the greatest claim number among the
  covered slots, and the SHA-256 of that slot's complete bytes. When two covered
  slots hold the same number, the later slot's digest is kept; that can happen
  only when two copies share a device id (§19.3). A device with no such claim
  has number zero and no digest.
- A greater number in an earlier slot still wins. A restored copy of Ana's phone
  can land claim 8 in slot 41 after claim 9 landed in slot 40; section 11 records
  claim 9, with slot 40's digest.
- Every slot kind counts, valid or refused, including writes whose parts were
  unreadable or ignored by a reset. So this number settles every claim after
  slot deletion ([§5.3](coven.md#53-settling-a-claim-after-its-slot-is-gone)).
- Authenticate the snapshot before using these numbers. They establish that
  claims were included, not that their effects survived.

### D7.8 Edge cases

**Audiences and coverage.**

- Rows and the plaintext header have the prefix's audience.
- Every named published write and source has a containing slot at or below
  coverage.
- The consumed prefix includes cut-off and reset-ignored writes; their verdicts
  remain.
- Reset eligibility uses `read_through`.
- Passing unreadable circle parts doesn't prove they were loaded. Rejoining
  reloads them (§15.7).

**What the decoder refuses.** Incorrect order, duplicate identities, and
mismatched counts, audiences or coverage. End of file without the end frame is
truncation, and nothing may follow the end frame.

**Nothing local.** No local row ids, upload queues, operation journals or local
paths occur in a snapshot. Loading leaves these local obligations intact.

---

<a id="d8-small-frames"></a>

## D8 Floor and join frames

### The floor (kind 12)

Old slots are deleted once snapshots cover them (§16.6). The floor records what
was deleted and where to start instead: the first kept slot, and each audience's
newest applicable snapshot ([§16.4](coven.md#164-the-floor)).

```
author     DeviceId
at         u64
first      Slot
snapshots  [announcement:Slot]
```

| Field | Rule |
| --- | --- |
| `author` | the device that signed the floor |
| `at` | the signer's applied slot. It covers all named announcements. Verify the signer against the membership at that slot (D9). |
| `first` | the first kept slot; must equal the clear prefix's `first` (D9) |
| `snapshots` | the slot of each named snapshot's announcement, in audience order, store first |

- Every named snapshot covers at least `first - 1`. Different audiences may have
  different coverages.

**The body.** The floor's encrypted body starts with this frame. Then it carries
each named announcement slot as a kind-6 frame, `slot:Slot | object:bytes`.

- Each announcement occurs once, in header order, followed by the kind-7 end
  frame.
- There is no total-size bound. Individual frames keep D1's bounds.

*Example.* From §16.4: the store's snapshot covers 560 and Gifts' covers 520,
with `first` at 501. Say they were announced at slots 562 and 523, and Carol's
laptop writes the floor having applied through 570. Then `author` is Carol's
laptop, `at` is 570, `first` is 501, and `snapshots` is `[562, 523]`: store
first. Both snapshots cover at least 500. The body carries slot 562's object,
then slot 523's, then the end frame.

**Replacing it.** Retention replaces this entire record conditionally before
deleting slots below `first`. Conflicts reread the floor, and can't move it
backward (§16.6).

D9 gives the floor's sealed envelope, which also carries the original key copies
a new device needs.

### The join request (kind 9)

Dan's phone asks to join by writing a join request (§13.2), which is
`invite:uuid | member:MemberId | keys:MemberKeys | device_name:name`:

```
invite       uuid
member       MemberId
keys         MemberKeys
device_name  name
```

- Verify its supplied signing key: the request is signed with the key it
  carries.
- Require the invite's fixed member id.

---

## D9 Sealed objects

Everything coven stores, except files, is wrapped the same way. The provider
must see only what it needs to route and frame an object. Members must be able to
check who wrote it before trusting any of it. And a retry must produce the same
bytes. This section gives that wrapping, the *envelope*, and the slot, inner
write, floor and key formats built on it.

### The envelope

Stored objects use `kind:u8 | version:u16 | prefix | chunks | signature`, except
D12 files. Inner writes use the same framing but are not separate storage
objects.

| Kind | Object | Prefix |
| --- | --- | --- |
| 32 | Inner write | `header_key:uuid \| part_keys:[uuid]` |
| 33 | Sequence slot | `slot_kind:u8 \| key:uuid \| copy:Option<CopyBox>` |
| 34 | Snapshot | `audience:Audience \| key:uuid \| covers:u64 \| purpose:Purpose` |
| 36 | Join request | empty |
| 39 | Floor | `key:uuid \| first:Slot \| copies:[slot:Slot \| object:bytes]` |

- The kind, version and prefix are in the clear.
- The chunks hold the encrypted plaintext (below).
- The signature authenticates everything before it (below).

**`Purpose`** says why a snapshot was taken (§16.3):

| Value | Purpose |
| --- | --- |
| `0` | ordinary |
| `1 \| version:u32` | a schema raise. The version is positive and equals the snapshot's schema. |
| `2` | a reset |

The raise or reset admin entry names the exact announcement whose prepared state
it activates (D6).

### Sequence slots (kind 33)

Every slot has the same envelope, whatever it holds.

**The prefix.**

| Field | Rule |
| --- | --- |
| `slot_kind` | `0` admin, `1` writes, `2` key copy, `3` snapshot announcement |
| `key` | names the store key that seals the slot's body. A prerequisite copy's body uses an accepted store key (§12.1). |
| `copy` | present exactly for kind 2 (below) |

**The slot header.** Every slot starts its encrypted body with a kind-14 header,
`device:DeviceId | claim:u64 | author:MemberId | after:u64`:

| Field | Type | Rule |
| --- | --- | --- |
| `device` | `DeviceId` | the claiming device |
| `claim` | `u64` | positive; from the device's one counter for all slot kinds (§5.1). Retries of that claim keep its number. It is independent of inner write numbers. |
| `author` | `MemberId` | the device's member |
| `after` | `u64` | exactly the slot's number minus one |

Every member can read the device and claim number; the provider can't.

**Signing.**

- Admin, copy and announcement slots use the device owner's preceding current
  key. Creation supplies it in its D6 payload.
- Registration uses the new device's existing member key.
- A writes slot uses the signing credential its inner writes share, resolved from
  their authored views. Check that historical signature before consuming a batch
  cut off by an earlier retirement or replacement (§14.1).

**The body.** After the header, the plaintext body is:

| Slot kind | Body |
| --- | --- |
| admin | one D6 frame |
| writes | `count:u64`, then `length:u64 \| inner write` for each queued write: in strictly increasing write-number order, all by the header's device, with no omitted unconfirmed predecessor |
| key copy | one empty kind-16 frame |
| announcement | one kind-15 frame: `snapshot:SnapshotId \| covers:u64 \| purpose:Purpose \| digest:32 bytes` |

**Announcements.**

- The coverage is below the announcement's own slot.
- The digest is the SHA-256 of the complete snapshot.
- Applying the announcement uses only these signed fields. A reader that loads
  the body checks it against the digest, and checks the snapshot's prefix against
  the announced coverage and purpose.

**Chunking.**

- A writes slot's body streams across 64-KiB chunks. There is no enclosing
  bounded collection or frame. Check every length before allocation, and stream
  each inner write.
- Every other slot holds its two frames in one chunk, subject to D2's
  complete-slot limit.
- A completed create includes all uploaded parts. A partial slot is never visible
  (§20.6).

#### Example: slot 41

Slot 41 is the writes slot Ana won in §5.1. Say it carries two of her queued
writes, 9 and 10, under her phone's claim number 15. At
`5d0c8e2a-7b1f-4c3e-9a64-0f2d8b71c3e5/seq/41`:

```
Clear prefix
  21                        kind 33: sequence slot
  0001                      version 1
  01                        slot_kind: writes
  <16 bytes>                key: the id of the store key sealing the body
  00                        copy: absent

Chunks, streamed; their plaintext:
  0E 0001 00000028          slot header frame: kind 14, 40-byte payload
    0000000000001092        device: Ana's phone, 4242
    000000000000000F        claim: 15
    <16 bytes>              author: Ana's MemberId
    0000000000000028        after: 40
  0000000000000002          count: two inner writes
  <u64> <inner write 9>     length, then the sealed inner write (kind 32)
  <u64> <inner write 10>

Signature
  <64 bytes>                Ed25519 by Ana's signing key, over the path and every byte above
```

Ben's attempt at slot 41 has his own header and writes, so its bytes differ; the
provider stores only one. If Ana's reply is lost, her journal holds these exact
bytes, and the content-bound nonces (D11) let a retry reproduce them byte for
byte. If she had lost and claimed slot 42 instead, only this outer envelope
would change: inner writes 9 and 10 keep every byte.

### Key copies

A key copy seals an audience key to one member (§12.1). Its routing must be
readable before the recipient holds any key, so Dan can find his copy. So the
box sits in the slot's clear prefix, and the slot's body is only an
acknowledgement.

`CopyBox` is:

```
audience:Audience | key:uuid | member:MemberId | sealing:SealingKey |
ephemeral:32 bytes | nonce:24 bytes | ciphertext:bytes | tag:16 bytes
```

| Field | Meaning |
| --- | --- |
| `audience` | whose key is copied |
| `key` | the copied key's id (not the slot prefix's `key`, which names the store key sealing the body) |
| `member` | the recipient |
| `sealing` | the exact sealing key it is sealed to |
| `ephemeral`, `nonce`, `ciphertext`, `tag` | the anonymous box (D11). The ciphertext is 48 bytes for a store key, or 64 for a circle key. |

- The box is the only encryption around the copied key. It is sealed to its
  recipient alone; no store key wraps it. The slot's `key` seals only the body,
  the acknowledgement.
- The kind and routing stay in the clear. The anonymous box and the slot's final
  signature authenticate them.
- The box may be opened tentatively before the store key is held. But
  membership, introduction and slot signatures must verify before anything is
  applied.
- The box's context uses the slot prefix through `sealing`, leaving out the
  ephemeral key and ciphertext. D11 adds the ephemeral key separately.

### The floor (kind 39)

The floor's prefix carries the first kept slot and the key copies a new device
needs before it holds any key (§16.4).

- `key` names the store key that seals the floor's body.
- `first` guides a keyless joiner's scan past deleted slots (§13.2). It is
  positive, like every `Slot`, and is authenticated with the complete prefix.
  Until then it grants no authority. After opening the body, require its `first`
  to equal the prefix's; refuse a mismatch as damaged.
- `copies` keeps the exact, complete original key-copy slots needed to open that
  key, in slot order, from history applied through `at`. This includes copies
  for joining credentials before admission. They are evidence, not newly sealed
  copies.
- Verify each copy with its original slot path and historical binding, from the
  authenticated store snapshot.
- The floor's signer is `author`, in the state at `at`; `at` covers all
  referenced announcements. The encrypted `author` and `at` fields have no
  authority until verified.
- Verify the floor and the snapshots' historical evidence before committing a
  reload.

### Snapshots (kind 34)

A device reads a snapshot's prefix to learn its coverage before it downloads the
rest (§6.5). So snapshots carry a second signature, over the prefix alone.

- A 64-byte prefix signature comes immediately after the prefix, before the
  chunks.
- It signs `context("coven/prefix-signature/v1", path,
  kind | version | prefix)`.
- Check it before using the coverage. Verify the final signature before loading.
- Resolve the snapshot device's key at `covers`, from local history or its store
  snapshot. An unknown author waits.
- A reader takes the body in one stream after the prefix signature, in order:
  each chunk adds to the digest, then opens, and its frames decode. It uses no
  decoded record until the stream ends. Then it checks, in order, the complete
  object's SHA-256 against the announced digest, the final signature, and the
  prefix signature. A chunk or frame that fails does not stop the digest: the
  rest of the object is still read, so an object that differs from its
  announcement is refused for its digest first (§16.2).

### Inner writes (kind 32)

A write keeps its bytes from its first attempt, whichever slot it finally lands
in (§5.1, §6.3). So an inner write is sealed and signed against its own stable
identity, not a slot path.

```
kind (32) | version | prefix | header chunk | roots:(32 bytes)×parts |
header_signature:64 bytes | part chunks | signature:64 bytes
```

**Identity.** Its context identity is the UTF-8 text
`coven/write/<store>/<device>/<number>`, with D10's canonical id spellings. Ana's
write 9 is `coven/write/5d0c8e2a-7b1f-4c3e-9a64-0f2d8b71c3e5/4242/9`.

- This is not a storage path.
- Both signatures, chunk authentication and nonce derivation use this identity
  where other objects use their path.
- Claiming another slot changes only the outer envelope. The inner bytes and the
  authored `read_through` stay fixed.

**Roots and the header signature.** The header signature lets a reader check who
wrote the write, and the authorship root of every part, without opening parts
it can't read.

- Each root is D11's keyed authorship root for that part.
- The header signature signs the context `coven/write-header-signature/v1`, the
  identity, the exact clear prefix, the complete sealed header chunk, and the
  concatenated roots.
- Resolve its signing key at `read_through`. Check each opened part against its
  root.
- The final signature covers the roots and header signature too.

**Who sees the audiences.** `header_key` names a store key, so every member can
read D5's part audiences before opening parts with their audience keys. The
provider sees neither these audiences nor the slot's device or claim number. The
slot's kind, key-copy routing and object framing stay in the clear.

### Chunks

A chunk is:

```
length:u32 | nonce:24 bytes | ciphertext | tag:16 bytes
```

- It is sealed with XChaCha20-Poly1305 under the named key's derived key (D11).
- `length` is the plaintext size.
- A frame chunk holds 7 bytes through 16 MiB.
- Stream chunks hold 1 through 65,536 bytes. Only the last in a section may be
  shorter.
- Empty chunks fail.

**Sections.**

- An inner write's header is section 0, under `header_key`. Each part is section
  `i+1`, under `part_keys[i]`.
- All other objects have section 0.
- Snapshots, floors and writes slots stream. Admin, copy and announcement slots,
  and join requests, use one chunk.
- Inner part counts and lengths come from D5.
- Unreadable parts are hashed and passed over without opening them.

**Associated data.** A chunk's associated data is D11's context of
`coven/object-chunk/v1`, the path or inner identity, the complete clear prefix,
the section and the chunk index. Snapshot prefix signatures are outside this
context.

**Nonces.** Inner writes and slots derive deterministic nonces, as D11 specifies.
Other objects keep random nonces, and keep their sealed bytes across retries.
Each new sealing of a floor draws fresh nonces, so two sealings of the same floor
differ in their chunks' bytes, and both authenticate.

### The final signature

- It is Ed25519 over the context of `coven/object-signature/v1`, the path or
  inner identity, and the SHA-256 of every preceding byte.
- It authenticates lengths, headers and skipped ciphertext.
- Check the complete signature before committing a slot or snapshot.

### Stream lengths

A snapshot or floor stream's encrypted length follows from the complete object's
length minus its prefix and signatures. With `c = 65536 + 44`:

- `n = ceil(sealed_length / c)` chunks;
- the plaintext size is `sealed_length - 44*n`.

44 is each chunk's overhead: the length field, the nonce and the tag. Require a
nonempty stream, a final chunk of 45 through c bytes, and the plaintext end
frame. Length alone never authenticates the body.

*Example.* A snapshot with 100,000 bytes of plaintext has two chunks, of 65,536
and 34,464 plaintext bytes. Sealed, they are 65,580 and 34,508 bytes: 100,088 in
all. A reader computes `n = ceil(100,088 / 65,580) = 2`, and a plaintext size of
100,088 − 88 = 100,000.

### Retries

Claim retries follow [§5.1](coven.md#51-claiming-a-slot) and §5.3. The journal
fixes the slot path and bytes before each create.

---

## D10 Paths

Every object has exactly one path, so two devices never spell one object two
ways. Every encrypted object's path is also bound into its authentication, so the
provider can't move one object into another's place.

| Path | Holds |
| --- | --- |
| `coven-bucket` | Bucket-root create-once ownership marker, only where member keys reach the whole bucket (§20.7) |
| `<store>/seq/<n>` | One create-once sequence slot |
| `<store>/floor` | Signed, encrypted snapshot references and the first retained slot; conditional replacement |
| `<store>/snapshots/<audience>/<device>/<n>` | One snapshot, discovered by its announcement |
| `<store>/files/<device>/<file>` | An uploaded file |
| `<store>/join-requests/<invite>` | A join request |

*Example.* Household's slot 41 is
`5d0c8e2a-7b1f-4c3e-9a64-0f2d8b71c3e5/seq/41`, and a store snapshot Ana's phone
numbered 3 is `5d0c8e2a-7b1f-4c3e-9a64-0f2d8b71c3e5/snapshots/store/4242/3`.

**Spellings.**

- Device ids and numbers are decimal, without leading zeros. `n` starts at 1.
- An audience is `store` or a circle's UUID.
- Store, member, circle, invite, attempt and file ids are canonical lowercase
  hyphenated UUIDs.
- A path has only this spelling, and is bound into its encrypted object's
  authentication.

**Rules.**

- The sequence has no gaps before retention.
- Snapshot numbers identify objects; they are never discovery cursors.
- Every immutable path is created once, and retries compare exact bytes. Only
  the floor is replaceable.
- Bootstrap discovers store creation at slot 1, or in the floor's store
  snapshot. The bucket marker (below) reserves ownership only; it doesn't mark
  creation.

### The bucket marker

On a service whose member keys reach a whole bucket, such as R2, a member of one
store could delete another store's objects in the same bucket. So there, every
store has its own bucket (§20.7). A listing of the bucket's root can't enforce
that alone: an object stored within the listing margin may be missing from it,
so two stores set up in one bucket at nearly the same time could both pass. The
bucket marker closes that gap. It is one object at the bucket's root, created
once, naming the one store that owns the bucket.

*Example.* Ana sets up Household in the R2 bucket `household`. Its first setup
creates the object `coven-bucket` at the bucket's root, whose complete bytes are
Household's store id as text:

```
35 64 30 63 38 65 32 61 2D 37 62 31 66 2D 34 63 33 65 2D 39 61 36 34 2D 30 66 32 64 38 62 37 31 63 33 65 35
                              "5d0c8e2a-7b1f-4c3e-9a64-0f2d8b71c3e5": 36 bytes
```

If someone tries to set up Garden in the same bucket, its create of
`coven-bucket` finds the name taken, reads these bytes, sees another store's id,
and is refused with `OwnBucketRequired { reason: Occupied }`. A retry of
Household's own setup reads equal bytes and proceeds.

**Rules.**

- The marker's complete bytes are the owning store id's canonical lowercase
  hyphenated UUID: 36 ASCII bytes, with no header, newline, encryption or
  signature. It has no `kind | version` prefix (D1).
- Its path has no store prefix.
- It exists only where member keys reach the whole bucket.
- First setup alone creates it. Reconnect, restore and join require an exact
  match of its bytes (§20.7).
- It is never replaced or deleted, including by local store deletion.

---

## D11 Keys and contexts

Coven derives several keys from one audience key, and signs and encrypts many
different things. If two of those could ever produce the same input bytes, a
signature or tag made for one could be accepted for the other. So coven builds
those inputs as *contexts*: lists of length-prefixed byte strings, each starting
with a label that says what it is for.

### Contexts

A context encodes a list of byte strings as each one's `length:u64 | bytes`.

- It is used as associated data, as HKDF's info, for nonce derivation, for
  confirmation numbers, and for D9's `coven/object-signature/v1`, `coven/prefix-signature/v1` and
  `coven/write-header-signature/v1` messages.
- A number in a context, such as a section or a chunk's index, is one string of
  its 8 bytes, as a `u64`.

*Example.* Section 1 enters a context as `0000000000000008 0000000000000001`:
the string's length, 8, then the number.

**Labels.**

| Label | Used for | Where |
| --- | --- | --- |
| `coven/encryption/v1` | the key that seals objects | below |
| `coven/row-authorship/v1` | the key for authorship leaves | below |
| `coven/object-chunk/v1` | a chunk's associated data | D9 |
| `coven/object-signature/v1` | an object's final signature | D9 |
| `coven/prefix-signature/v1` | a snapshot's prefix signature | D9 |
| `coven/write-header-signature/v1` | an inner write's header signature | D9 |
| `coven/object-nonce/v2` | deterministic nonces | below |
| `coven/join-request/v1` | a join request's key | below |
| `coven/confirmation-number/v1` | a join request's confirmation number | below |
| `coven/sealed-box/v1` | a sealed key's box | below |
| `coven/file-chunk/v1` | a file chunk's associated data | D12 |

### Derived keys

From a store or circle key, HKDF-SHA256 with no salt derives 32-byte keys by
label:

- `coven/encryption/v1` for sealing objects;
- `coven/row-authorship/v1` for authorship leaves.

Every HKDF in this document works the same way: the input is a 32-byte secret,
there is no salt, the info is a context (above), and the output is 32 bytes. Here the
context holds the label alone.

### Authorship roots

A write's signatures must cover its row changes, including parts some readers
can't open (§12). The authorship root does that: a hash tree over a part's
records, whose leaves a snapshot can later open one by one (D7.5).

**The tree.** A write part's authorship root is a binary hash tree over its
leaves, split as RFC 6962 splits a Merkle tree: a tree of `n > 1` leaves has the
largest power of two below `n` on its left.

- A leaf is `HMAC-SHA256(authorship_key, 0x00 | leaf bytes)`, with the key
  derived from the part's key.
- A branch is `SHA-256(0x01 | left | right)`.
- An empty part's root is `SHA-256(0x02)`.
- Keying the leaves keeps a reader of another audience from testing a guessed row
  against a published root.
- Branches are unkeyed, so a device that kept a part's leaf hashes can open its
  leaves again without the key.

**Leaves** follow the part's record order. A row change has its value leaves,
then one leaf per step. An execution record has one per step. Dismissals have
none.

```
0 | row:RowId | generation:u64 | creator:Option<ChangeId>     an insert's creation
1 | row:RowId | generation:u64 | kind:u8 | column:name | value:ColumnValue
                                 each column an insert (kind 0) or update (1) set
2 | row:RowId | generation:u64                                a delete
3 | row:RowId | sequence:u32 | generation:u64 | operation:u8 | columns | key_update:u8
                                                              each SQL operation
```

- An insert has its creation leaf even when it sets no other column. That leaf
  carries no values, so ownership stays checkable after every inserted value is
  overwritten.
- Generations are the row's before the change.
- A step leaf's `columns` is the step's, as in D5.8: a `map<name, ColumnValue>`
  for an insert or update, absent for a delete. It is not D2's `Columns`.

*Example.* Ana's write 3's one part has four leaves: a value leaf for note 42's
title (kind 1), note 42's step, the tag's delete leaf, and the tag's step. With
four leaves, the left subtree holds two:

```
root = SHA-256(01 | SHA-256(01 | L0 | L1) | SHA-256(01 | L2 | L3))
```

where each `Li` is the HMAC of `00` and that leaf's bytes.

### Nonces for inner writes and slots

A retried write or slot must reproduce its bytes exactly (§5.1), including its
ciphertext, which a random nonce can't do. And a restored backup that forgot an
attempt must never seal different plaintext under a nonce it already used
(§19.3). So the nonce is computed from the chunk itself.

For each inner-write or slot chunk, its 24-byte nonce is:

```
HMAC-SHA256(encryption_key,
  context(UTF8("coven/object-nonce/v2"), UTF8(path), prefix,
    u64(section), u64(index), SHA256(plaintext)))[0..24]
```

| Input | Meaning |
| --- | --- |
| `encryption_key` | the 32-byte key derived with `coven/encryption/v1` |
| `path` | the exact D10 path, or the D9 inner-write identity |
| `prefix` | the exact cleartext `kind \| version \| prefix` from D9 |
| `section`, `index` | big-endian eight-byte strings; both start at zero, as in D9 |
| `plaintext` | this chunk's complete plaintext, including any frame bytes |

Each context field is length-prefixed as above.

**Why these inputs.**

- The label separates nonce derivation from other contexts.
- Hashing bounds the content field at 32 bytes, and uses the existing SHA-256
  primitive.
- Neither the plaintext nor its unkeyed hash is published. Only the keyed nonce
  reaches storage.
- A different chunk or prefix changes these inputs, even after rollback or
  copying a device.
- Collision resistance, rather than mathematical injectivity of a 24-byte value,
  is the cryptographic boundary.
- The plaintext, prefix, format and sealing key ids stay fixed for retries, so
  deterministic sealing and Ed25519 signing reproduce the complete object byte
  for byte.

This derivation does not apply to snapshots, anonymous boxes, floors or join
requests. Snapshots and sealed keys keep their originally sealed bytes.

### Join request keys

An invite's secret derives its join request's key with `coven/join-request/v1`:
HKDF-SHA256 of the 32-byte secret, with no salt, the context of that label alone
as its info, and a 32-byte output.

### Confirmation numbers

A sent invite's request shows a six-digit confirmation number on both phones
(§13.2). It is SHA-256 of a context of three strings:
`coven/confirmation-number/v1`, the invite id's 16 bytes, and the 64 bytes of
the request's `keys:MemberKeys` encoding. The first 8 bytes of the hash, as a
big-endian `u64`, modulo 1,000,000, give the number. It is shown with leading
zeros as two groups of three digits: `041 778`.

*Example.* Ana's pair from signing and sealing seeds `0x33`, answering the
invite `0x55` repeated, has the number `181 205`.

- The invite secret is not an input. Anyone who saw the code has it, so it adds
  nothing a stranger lacks.
- The number names only the keys, never the device name, which the requester
  chooses freely.

### Sealed keys

A sealed audience key in a sequence slot has D9's clear routing prefix and box.

- **The shared secret** is X25519 of a fresh ephemeral key and the member's exact
  sealing key named in D9's prefix. A contribution of all zeros is refused. The
  prefix must match the recipient key the box actually uses.
- **The box key** is HKDF-SHA256 of the shared secret, with no salt and a
  32-byte output. Its info is the context of `coven/sealed-box/v1`; `store` or
  `circle`; the path; the exact D9 clear prefix; the ephemeral key;
  and the member's sealing key, in that order. That context is also the
  associated data.
- **The plaintext** is `key:uuid | key bytes:32` for a store key, and
  `circle:uuid | key:uuid | key bytes:32` for a circle key.

**Opening.**

- The recipient must match the prefix's member and sealing credential. It opens
  with that historical private key, rather than trying the latest binding.
- It checks the opened audience and key id against its prefix.
- It checks the SHA-256 of the 32 key bytes against the authorized D6
  introduction. A mismatch is invalid, not another value under that key id.

---

## D12 Files

A file can be hundreds of megabytes, and a song must play from any point
without downloading the rest (§17.7). So a file isn't a sealed object of D9: it
is fixed-size chunks at computable offsets, each authenticated on its own.

### The object

A stored file at `<store>/files/<device>/<object>` is:

```
kind:u8 (38) | version:u16 | size:u64 | chunks
```

- `size` is the file's plaintext size.
- Chunks are `ciphertext | tag:16 bytes`, each 65,536 bytes (64 KiB) of the file
  except the last, which may be shorter. A file of size 0 has none.
- Each chunk is XChaCha20-Poly1305 under the file's own key, chosen when it
  was staged ([§17.3](coven.md#173-staging-a-file)). Its nonce is its index, as a 24-byte
  big-endian number. Its associated data is D11's context of
  `coven/file-chunk/v1`, the path, the 11-byte cleartext header
  `kind | version | size`, and the index as an eight-byte big-endian string.
- Chunk `i` starts at `11 + i × (65,536 + 16)`, so any range is read without the
  rest.

*Example.* A 150,000-byte photo has chunks of 65,536, 65,536 and 18,928 bytes.
Its object is 11 + 150,000 + 3 × 16 = 150,059 bytes, its complete encrypted
size (§17.6). Chunk 2 starts at 11 + 2 × 65,552 = 131,115.

### Coordinates

A file's record (§17.2) holds where the file is in its `coordinates` cell, one
of two texts, never both places and never neither
([§17.3](coven.md#173-staging-a-file)):

```
file <device id> <object id> <key in lowercase hex>
device <device id>
```

- `file` names stored bytes: the uploader's device, the stored object's id and
  the file's key. The store id is the record's store, which gives the fixed
  path `<store>/files/<device>/<object>`. The key stays inside encrypted write
  data.
- `device` names the one device the file is on until its upload completes.
- The device id is decimal, without leading zeros. The object id is its
  canonical UUID. The key is 32 bytes, written as 64 lowercase hex digits.
- Components have one ASCII space between them, and no surrounding spaces. Any
  other text is invalid.

*Example.* A photo Ana's phone stages has `device 4242` until its upload
completes, then `file 4242 0b7e5c1a-3d2f-4e8b-9c6a-1f0e2d3c4b5a ` followed by 64
hex digits of its key.

**Edge cases.**

- No write carries `file` coordinates before their object is stored. The
  staging write carries `device` coordinates, and the write completing the
  upload switches them.
- The content hash is set exactly when the coordinates are `file`: a record
  change carries both together (D5.5).
- Pinning and caching never change the coordinates.
- File status follows [§17.6](coven.md#176-a-files-status); a write doesn't
  record availability.

---

## D13 Codes

A new device has nothing yet: no key, no store, often not even the storage
location. A code carries what it needs, on paper or a screen (§13). It must be
short enough to type or scan, and must catch typing mistakes. Every character
adds to the QR code a phone's camera has to read, so every field is packed,
without names.

### Restore code (kind 10)

A restore code lets a member's new device stand in for them (§13.1). It is
`store:uuid | member:MemberId | member_keys:bytes | storage:bytes`:

```
store        uuid
member       MemberId
member_keys  bytes
storage      bytes
```

- `member_keys` is `CVMK 01 | signing seed:32 bytes | sealing secret:32 bytes`:
  the member's private pair. `CVMK 01` is the four ASCII bytes `CVMK`, then the
  byte `01`.
- `storage` is the code's storage part (below): the store's location and, on
  S3, whether the member's access key follows, then the key. Dropbox and
  OneDrive locations include the member's display-only account label, so saved
  codes can distinguish accounts without carrying credentials.
- The private-pair blob is unchanged, and explicitly belongs to `member`.
  Restore requires that id's current pair; an old code can't rebind it.
- No store name is encoded. A code written down can't be corrected after a
  rename, so a restoring device learns the name after loading. The copy coven
  saves for the person carries a name beside the code instead (below).
- A restore code never carries OAuth tokens. It carries an S3 key only when
  the person's own device shows it to their new device, with the key that
  device holds (§13.1). The code coven returns to write down, and the copy it
  saves for the person, carry none.

### Saved restore code (kind 17)

Coven saves a member's restore code with the store's name, in iCloud Keychain
or Block Store, so the person's next devices can list it by name (§13.1). The entry's bytes
are this frame, `name:name | named_at:Slot | code:bytes`:

```
name      name
named_at  Slot
code      bytes
```

- `name` is the store's name as the saving device last had it. In Block Store
  the frame and its entry name together take at most 1,024 bytes, so a name too
  long to fit is saved shortened, cut between characters (§13.1).
- `named_at` is the slot that set that name: 1 for the name chosen at creation,
  or the slot of the rename. A device rewrites the name only with one from a
  later slot, so a device that is behind never puts back an older name.
- `code` holds a whole restore code frame (kind 10), as above. Its store id
  names the entry's store. It carries no S3 key: the key is its own saved
  entry beside it (§13.1), and a reader refuses a saved code that carries one.
- It has no text form: it is never typed, scanned or shown.

### Invite code (kind 11)

An invite code lets a new person ask to join (§13.2). It is
`store:uuid | name:name | invitee:Option<name> | delivery:u8 | invite:uuid |
member:MemberId | secret:32 bytes | initial_key:uuid | inviting_writer:DeviceId |
read_through:u64 | storage:bytes`:

```
store            uuid
name             name
invitee          Option<name>
delivery         u8
invite           uuid
member           MemberId
secret           32 bytes
initial_key      uuid
inviting_writer  DeviceId
read_through     u64
storage          bytes
```

| Field | Rule |
| --- | --- |
| `name` | the current store name copied when the invite is made; loading the store supplies its current name (§13.2) |
| `invitee` | the new person's name, as the admin typed it; `None` for a replacement, whose member already has one |
| `delivery` | how the invite reaches the person, which fixes how its request is approved. `0` in person: the inviting device approves on its own. `1` sent: the admin approves after comparing the confirmation number. Any other byte is refused. |
| `invite` | the join request's path |
| `member` | fixed by the admin: a fresh id for an addition, or an existing active id for a replacement. The signed request must repeat that id. |
| `secret` | the invite secret, which derives the join request's key (D11) |
| `initial_key` | the key introduced by creation |
| `inviting_writer` | the inviting device, whose approval the joiner trusts |
| `read_through` | the inviting device's applied slot when it issued the invite |
| `storage` | the code's storage part (below): the provider location, what the new person needs to accept it, and on S3 the joining member's own access key |

**Joining.**

- A joiner checks the routing of subsequent slots for its copy, or follows the
  floor when that prefix is gone (§13.2).
- Routing scans establish no applied progress or authority.
- Approval publishes the request's exact pair, after the prerequisite copies.
- Bootstrap requires a claimed, matching id-and-pair binding from the approving
  writer named in the invite. Never merely an active id, another writer's equal
  pair, or an available key copy.

### Storage parts

A code's `storage` field tells the new device where the store lives and how it
reaches it. `coven-storage` writes and checks it; the format's codecs carry it
as bytes, at most 16 KiB. It is packed from D2's primitives, with no field
names. As JSON with field names it was most of a code. A restore code with its
S3 key, for a bucket on a service whose settings setup detected, took 930
characters with the key as JSON beside it; packed it takes 450, a QR code of
version 13 instead of 25.

*Example.* Household on Dropbox, in the shared folder with namespace id
`4815162342`, which Ana signed in to as `ana@example.com`. Her restore code's
storage part is:

```
01                              Dropbox
01 0000000A 34383135313632333432  the shared folder 4815162342
01 0000000F 616E61406578616D706C652E636F6D  account label ana@example.com
```

A Dropbox location is the end of a restore code's storage part. On S3, one
more byte says whether a key follows.

**A location** is the provider's tag, then that provider's own fields:

| Tag | Provider | Fields |
| --- | --- | --- |
| 0 | S3 | `bucket:text \| region:text \| endpoint:Option<text> \| profile:Option<S3Profile> \| prefix:text` |
| 1 | Dropbox | `0` for the App folder, or `1 \| namespace_id:text` for a shared folder; then `account_label:Option<text>` |
| 2 | OneDrive | `drive_id:text \| folder_id:text \| account_label:Option<text>` |
| 3 | CloudKit | `container:text \| owner:text \| zone:text` |

- `endpoint` is the URL as it serializes itself: `https://s3.example.net/`, with
  its slash. `prefix` is the base prefix in its one spelling, with no slash at
  either end ([§20.3](coven.md#203-where-a-store-lives)). A reader refuses any
  other spelling, so a location has one encoding.
- `S3Profile` is `0 | name:u8` for a catalogued service, or `1 | settings:u16`
  for custom settings (Appendix E.3). The names are 0 AWS, 1 GCS, 2 R2,
  3 RustFS, 4 SeaweedFS, 5 Ceph RGW and 6 Versity Gateway.
- The account label only tells the person which account it is. It never
  routes or authorizes.
- A reader checks the location as setup checks one: an empty bucket, a
  catalogued host without https, or a profile that contradicts its host is
  refused (Appendix E.3).

**Custom settings** take one bit field each, from the lowest bit. The top four
bits are zero.

| Bits | Setting | Values |
| --- | --- | --- |
| 0 | signing | 0 AWS SigV4, 1 Google HMAC |
| 1 | single-PUT condition | 0 `If-None-Match: *`, 1 `x-goog-if-generation-match: 0` |
| 2–4 | copy publication | 0 never; 1, 2, 3 multipart uploads; 4, 5, 6 every object. Within each, the destination condition is `If-None-Match: *`, `x-goog-if-generation-match: 0`, then `cf-copy-destination-if-none-match: *`. 7 is refused. |
| 5 | native multipart timestamp | 0 completion, 1 start |
| 6–7 | checksum policy | 0 scope stated, 1 unstated scope ignored, 2 none returned; 3 is refused |
| 8 | listing | 0 V2, 1 V1 |
| 9 | revision | 0 ETag, 1 generation |
| 10 | response headers | 0 S3, 1 Google |
| 11 | reads request checksums | 0 no, 1 yes |

**After the location** comes what the code's provider needs, and nothing else:

| Code | S3 | Dropbox | OneDrive | CloudKit |
| --- | --- | --- | --- | --- |
| restore | `s3_key:Option<S3Key>` | nothing | nothing | nothing |
| invite | `s3_key:S3Key` | nothing | `share_token:Option<text>` | `share_url:text` |

`S3Key` is `access_key_id:text | secret_access_key:text`, and neither may be
empty.

**Edge cases.**

- An invite's Dropbox location must be a shared folder, which the new person
  mounts; an App folder can't be shared, and a reader refuses it.
- A OneDrive invite carries no share token when the folder is already shared
  with the new person's account. A token, when present, is not empty.
- A CloudKit invite's share URL uses https.
- An unknown tag, a text that isn't UTF-8, a length past what remains, and
  trailing bytes are all refused.

### As text

A code's text is its prefix, then unpadded uppercase base32 (`A`–`Z`, `2`–`7`)
of its frame followed by a CRC-32C of the frame, big-endian.

| Code | Prefix |
| --- | --- |
| restore | `CVR1-` |
| invite | `CVI1-` |

- These text envelopes and `CVMK 01` identify their own encoding; the inner frame
  carries storage format 1.
- CRC-32C uses the reflected polynomial `0x82F63B78`, with initial and final XOR
  `0xFFFFFFFF`. The check value of `123456789` is `E3069283`.
- Unused trailing bits are zero. There is no other spelling.
- The whole is at most 19 KiB before encoding: room for an invite with two
  1,024-byte names and 16 KiB of storage.
- The checksum catches typing mistakes, not tampering.

---

## D14 What the fixtures pin

The format is a contract between devices running different builds. Fixtures
pin it: fixed bytes that every build must decode and reproduce.

**What exists.** Each supported format requires fixtures of every frame, slot
kind, sealed object, file and code. Format 1 includes:

- a writes slot containing store and circle parts, streaming chunks, authorship
  roots, SQL execution records and a migration write;
- every admin tag and first-slot creation, stable member ids, expected and
  replacement member keys, access key ids and key introductions;
- key copies for old and current credentials, with signed clear routing;
- snapshot announcements, and snapshots with every evidence section,
  active and frozen lost values, first-slot creations and per-device last-claim
  numbers;
- a floor with original boxes, announcements and the first kept slot;
- restore and invite codes with S3 storage parts, which `coven-storage`'s
  tests decode and reproduce, a saved restore code, signed join requests and
  delivered S3 keys.

**Decoding.**

- Every successful decode re-encodes to the same bytes.
- Tests decode every truncation and every single-bit change of every fixture
  without panicking.

**Cryptography.**

- Fixture keys and nonces are public test data.
- Ciphertext, derivations, confirmation numbers and signatures are calculated
  independently with Python's hashlib and hmac and libsodium, then opened
  through Rust.
- Mutations must reject changed paths, routing credentials, slot order, authored
  views, roots and signatures.
- A retry at the same slot repeats every byte. A different slot changes its outer
  authentication, while keeping each inner write exactly.

**Retention and claims.**

- Fixtures cover an advanced floor with deletion unfinished.
- They cover last-claim numbers settling lost-reply claims of every slot kind.
  Refused slots, and writes with reset-ignored parts, count too.
- They cover snapshots for audiences at different slots, and the schema rule of
  §16.3. Format-1 ciphertext and signatures are regenerated for these layouts.

**Nonces.** Fixtures must pin D11's content-bound nonce recipe. Ana retries an
identical chunk, changes its plaintext at the same path, and changes only the
cleartext prefix. The first case repeats every byte; the other two use different
nonce inputs.

---

## What's proved

The Lean models under [proofs/](proofs/) check the rules this format serves, not
the encodings themselves: no proof covers the byte layouts. Each row names the
claim in plain words, its theorems, their file, and the section here it
supports. coven.md Appendix G lists every other proof.

| Claim | Theorems | File | Section |
| --- | --- | --- | --- |
| Conversions leave the queued uploads unchanged. Two receptions of the same write, under the same conversion, schema view and history, each meeting the receipt's prerequisites, reach the same record and verdict, and the same lost value | `conversions_preserve_uploads`, `receive_losses_converge` | `proofs/queue/CovenQueue/Conversion.lean` | D5.2 |
| Ben's example: an untried upload stays untried with its original record through conversions, and two receptions of a write under the same conversion, schema view and history, each meeting the receipt's prerequisites, agree | `conversions_preserve_untried`, `receive_agreement` | `proofs/queue/CovenQueue/Conversion.lean` | D5.2 |
| A step counts as a foreign-key action exactly when it equals the schema's consequence of an earlier step, through a reference its row holds at that moment; the classification reads only the steps, the replayed references and the schema, never a sender's label. In the model's construction of steps and their parents, a classified step names, in every reference it writes, the row and incarnation whose step caused it; this isn't proved for every overlapping reference a real schema can declare | `action_iff`, `follows_cause` | `proofs/merge/CovenMerge/ForeignKeyClassification.lean` | D5.8 |
| The merge model's row is one table, key and audience, so equal keys must be one row; binary collation and D3 make equal keys equal bytes. The model abstracts keys, so B2 doesn't prove the byte order: Rust compares `RowId` keys as bytes, and `coven-format`'s key tests show D3's bytes sort as SQLite does | write-up B2 | [`proofs/merge.md`](proofs/merge.md#b2-terms) | D3 |
| The data rule: receive order, progress, and dropping a change by its data, whose evidence section 5 and lost-value cause 3 supply | write-up B17 | [`proofs/merge.md`](proofs/merge.md#b17-receive-order-progress-and-dropping-by-data) | D7.3 |
