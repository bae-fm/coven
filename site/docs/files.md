# Files

Rows can reference files: songs, photos, documents. A file is far too large to
travel in a slot, yet it has to sync with the rows that reference it, be
readable only by their readers, be readable a piece at a time while a song
plays, and never put the user's own original at risk. This page covers
declaring files, staging them, uploads, status and reading. The device's copies
of downloaded files are the [Cache](/docs/cache) page.

## Files and references

Coven owns files. Each file has a *record* in coven's own synced table, which
the app's SQL reads as `coven_files`:

| Column | Holds |
| --- | --- |
| `id` | the file's id, which coven picks when the file is staged |
| `audience` | the store or the circle the record is in |
| `size` | its size in bytes |
| `hash` | the SHA-256 of its bytes; NULL until it is stored |
| `coordinates` | where it is: stored at a fixed location with its key, or on one device only |
| `namespace` | the namespace it belongs to, which fixes its kind |

An app row holds only a file's id, in a column the app declares as a *file
reference*, and joins `coven_files` for the rest:

```sql
SELECT a.title, f.size, coven_file_device(f.coordinates)
FROM attachments AS a JOIN coven_files AS f ON f.id = a.file
WHERE a.note_id = ?1
```

`coven_file_device` gives the device a file is on, as `coven_devices.device`
holds it, while the file is on one device only, and NULL once it is stored.

A file's record lives in one audience, and travels in that audience's
encrypted writes, so its readers are exactly the audience's members. A
reference names a file in its own row's audience, or is NULL; a write setting
one to anything else fails with `DbError::FileUnknown`. Several rows may
reference one file, and a file never changes its bytes: a new image is a new
file with a new id.

To use a file's bytes in another audience, stage a new file over them with
`batch.existing_file(file)` and put its id in the row there. The new file has
the same namespace, size, place and key, nothing is uploaded again, and each
audience's record changes on its own: making one device-only leaves the
other's stored. The stored bytes stay until neither record names them. Only
the device holding a file that is on one device only can do this
(`NotThisDevice`).

## Declaring files

The app declares its namespaces when it opens the store, each a kind of file
with its kind and [cache](/docs/cache) budget, and each synced table's file
reference columns:

```rust
fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        // The user's own file, never changed once set.
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .file_column(FileColumn::new("file").write_once()),
        // A thumbnail the app makes, which reaches other devices with its row.
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .file_column(FileColumn::new("file").bound()),
    ]
}

let handle = Coven::builder(layout)
    .synced_tables(tables())
    .namespaces(vec![
        Namespace::new("attachments", Provenance::UserProvided, 2 << 30),
        Namespace::new("thumbnails", Provenance::AppProvided, 256 << 20),
    ])
    .migrations(migrations())
    .open(store)
    .await?;
```

A file reference column holds a file's id as text, or NULL. It can't be a
generated column or an SQL foreign key: coven reads it as a reference to the
file's record, and writes it when its row moves to another audience. For the
same reason, no table of the app's may hold an SQL foreign key into
`coven_files`: opening refuses one, and a file column is how a row references
a file.

## Bound and trailing references

A file reference is *trailing* unless declared otherwise: the write setting it
goes out at once, and other devices see the file on the staging device until
its upload completes. That suits an album's tracks, which other devices should
list while they upload. A *bound* reference, `FileColumn::new("file").bound()`,
holds the write setting it until its file is stored, so other devices get the
row and the stored file in one slot. That suits a cover or a thumbnail, which
the app never wants to show missing.

Writes go out in order, so a held write holds every later write of the device
too, until the file is stored; then the next pass sends them all, with the
write switching the file to the cloud. On the staging device the write commits
at once, and reads use coven's copy. So keep bound files small: a bound
reference names files of at most 1.5 MiB, `BOUND_FILE_LIMIT`, or the limit
`bound_with_limit(bytes)` sets, and a larger file fails the write with
`DbError::BoundFileTooLarge`. It names only app files, never the user's
(`DbError::NamespaceKind`), and a file it names must be stored already, staged
by the same write for the cloud, or uploading for another bound reference;
anything else on one device only fails the write with `DbError::DeviceOnly`.
Bound uploads run beside trailing ones, up to the same upload limit, so a
cover never waits behind a queue of tracks, and pausing uploads leaves them
running. A binding can change without a migration: it decides only when this
device sends its own writes.
`write_once()` refuses a write that changes the reference once it names a file
(`DbError::FileWriteOnce`). Which columns are file references changes only with
a breaking migration, since every device must read the same cells as
references.

## Two kinds of file

A **user-provided** file is the user's own file at a path on their device, such
as a song in their music folder. Coven records its path, size and modification
time. It never copies, moves, changes or deletes it. If the file moves or
changes before it is uploaded, coven reports it missing or changed before
reading any of it. After a successful upload, coven never reads the original
again; reads use the cache or storage.

An **app-provided** file is bytes the app hands to coven, which keeps and owns
them while the file is on this device only: until its upload succeeds, or for
as long as it stays here. The app can hand them over as a stream, so a
large file never has to fit in memory.

A namespace holds files of one kind. Staging into an undeclared namespace fails
with `DbError::UndeclaredNamespace`, and into one of the other kind with
`DbError::NamespaceKind`.

For both kinds, the first read records the whole-file hash and a SHA-256 of
each 64 KiB chunk. Every upload attempt checks against those, so a retry can
never encrypt different bytes than the first attempt did.

## Staging a file

A write stages its files first, then runs its SQL with what staging returned,
as one write. Staging gives each file a new id, which the SQL puts in the rows
that reference it. A user's file is read once before the write, with
`prepare_user_file`:

```rust
let note = UuidText(Uuid::now_v7());
let attachment = UuidText(Uuid::now_v7());
let prepared = prepare_user_file(&path, |read| show_progress(read)).await?;

let file = handle.write_with_files(
    move |batch| Ok(batch.put_user_file("attachments", UserSource::Cloud(prepared))),
    move |sql, file| {
        sql.execute(
            "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, ?3)",
            (&note, "Paint colors", Audience::Store),
        )?;
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, file) VALUES (?1, ?2, ?3, ?4)",
            (&attachment, &note, "Swatches", file),
        )?;
        Ok(file)
    },
).await?;
```

App-provided bytes go through `put_file`, which takes a byte buffer or a
stream, read once before the write takes the database writer, and a `Place`. A
staged file no row references fails the write with `DbError::FileUnreferenced`.

## On this device only

A file can stay on the device that staged it, with no upload: `Place::DeviceOnly`
for app bytes, kept in coven's copy, or `UserSource::DeviceOnly` for a user's
original, observed with `observe_user_file`, which records its path, size and
modification time without reading it. Every device that reads the row sees the
file on that device only. That device reads it from its bytes there; others get
`FileReadError::DeviceOnly`, and nothing waits for it.

```rust
let observed = observe_user_file(&path).await?;
handle.write_with_files(
    move |batch| Ok(batch.put_user_file("attachments", UserSource::DeviceOnly(observed))),
    move |sql, file| {
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, file) VALUES (?1, ?2, ?3, ?4)",
            (&attachment, &note, "Swatches", file),
        )?;
        Ok(())
    },
).await?;
```

Before the write, `observed.size()` and `observed.modified_at()` give the size
and modification time coven will record, and `size()` and `modified_at()` on a
`PreparedUserFile` do the same for the cloud. Compare them with what your own
scan saw, so a file changed in between isn't recorded under rows that describe
its old contents. The time is exact, as the operating system gives it.

App SQL reads this device's originals through `coven_user_files`: each file's
`path`, `size`, and its exact modification time as `modified_seconds`, whole
seconds since the Unix epoch rounded down, and `modified_nanos`, the
nanoseconds past that second. Two saves within one millisecond show different
times, and a time before 1970 counts its nanoseconds forward from the second
before it: half a second before the epoch is -1 and 500,000,000.
`unix_seconds_and_nanos(time)` gives a time on disk as the same pair, to
compare with the view's. The path is the operating system's own bytes, so a file whose name isn't valid
Unicode, such as a Latin-1 `Café.flac` on Linux or a name with an unpaired
surrogate on Windows, is staged, read and uploaded like any other. Show it
with `coven_path_text(path)`, and make a name to sync from a path with
`path_text(&path)`, which gives the same text: valid UTF-8 as it is, and each
byte that isn't as `\xNN`.

```sql
SELECT a.title, coven_path_text(u.path)
FROM attachments AS a JOIN coven_user_files AS u ON u.file = a.file
```

The text is for people to read: `Caf\xE9.flac`. A backslash already in a name
stays as it is, so the text can't be turned back into the path, and coven never
opens a file by it.

To keep paths of your own, such as the folder an album was imported from, keep
`path_bytes(&path)`: the bytes `coven_user_files.path` holds for the same path.
`path_from_bytes(bytes)` gives the path back to open. It fails with
`PathBytesError` for bytes that are no path on this operating system, such as
Latin-1 bytes on Windows; Unix takes any bytes.

## Moving to the cloud

The files one write stages for the cloud are one *move*: none of them switches
to the cloud until every one is stored, and then one write switches them all.
A write can also move files that are on this device only, with
`sql.move_to_cloud(files)`, which returns the move's `MoveId`. Each
`MoveFile::User` comes with its original read whole again by
`prepare_user_file`, and must still be as recorded; `MoveFile::App` uploads
from coven's copy. A move survives restarts, and its files stay on this device
only until they switch together, with no row changing.

- An original that's missing, on an unplugged drive say, makes the move wait and
  retry. Its record, with subject `PendingSubject::Move`, names the original.
- An original that changed fails the move: no file of it switches, each stays on
  this device only, and its record names the original until the app cancels the
  move with `sql.cancel_move(id)`.
- Cancelling a move still under way also switches nothing. A write staging
  files for the cloud returns only their ids: read its move, as a `MoveId`,
  from the `move` column of `coven_uploads`, joined with your rows. A bound
  file's upload shows NULL there, since its move can't be cancelled.
- Moving a file on another device fails with `DbError::NotThisDevice`, and one
  in the cloud or already moving with `DbError::NotDeviceOnlyHere`.

The staging write picks the file's random id, a random storage name and a
random key, records this device as its uploader, and fixes its storage path,
`<store>/files/<device>/<name>`. In the same transaction it makes the file's
record, on this device only, and queues the upload, even with no storage
connected. The rows reference the file at once: other devices receive them
with the record, and see the file as on the staging device until its upload
completes. On the staging device, reads use the original or coven's copy.

When the upload completes, one more write switches the file's record to its
stored location and sets its hash. No app row changes, and other devices can
read the file as soon as they receive that write.

A file goes with its last reference. The write that removes the last reference
this device can see, by pointing the row at another file, clearing it or
deleting the row, also deletes the file's record. If that happens before the
upload completes, the upload stores nothing: an attempt not started is retired,
and one in flight finishes and switches no record. A write from another device
naming a file whose record was deleted is taken out on every device, like a
child of a deleted row, and its lost value can't be restored.

## Making files device-only

`handle.make_device_only(folder, files, &cancel)` brings cloud files back to
this device, into the folder the person picked. Each file comes with its path
under that folder, a `RelativePath`: the names the app gives, outermost
first, each one name whatever it holds. Coven downloads each file, checking its
content hash, writes it under a temporary name in its final folder and renames
it there once it is whole and synced. Then one write switches every file to
this device only, and `coven_user_files` lists each copy as the file's
original here. No app row changes, and other devices see the files on this
device when the write reaches them.

```rust
let (_stop, cancel) = watch::channel(false);
let files = vec![(track, RelativePath::new(["Abbey Road", "01 Come Together.flac"])?)];
handle.make_device_only(&music_folder, files, &cancel).await?;
```

- Coven writes each name through one rule, the same on every platform, so it
  is valid on any drive: `" * : < > ? \ | /` become `＂ ＊ ： ＜ ＞ ？ ＼ ｜ ／`,
  control characters `␀`–`␟` and DEL `␡`; a last `.` or space becomes `．` or
  `␠`; a Windows device name such as `CON.flac` becomes `ＣＯＮ.flac`; and a
  name past 255 bytes keeps its extension and is cut before it, with `…`. So
  `AC/DC` is the folder `AC／DC`, and the `Caf\xE9.flac` that `path_text` made
  from a Latin-1 name is `Caf＼xE9.flac`. Other names stay as given. A path
  with no names, or an empty one, is `RelativePathError::Empty`, and the
  folder's path must be valid Unicode (`CopyPathError::NotUnicode`).
- A path through a symbolic link below the folder fails with
  `CopyPathError::Link`, and one where something is already with
  `CopyPathError::Exists`: coven never replaces a file. Two paths written as
  one, such as `Why?.flac` and `Why？.flac`, or differing only in case or
  Unicode form, such as `Album/Track.flac` and `album/track.flac`, fail with
  `CopyPathError::Clash` on every volume, since the default macOS and Windows
  volumes hold them as one name; it names both paths as the app gave them.
  All are refused before any byte downloads.
- The member must be in each file's audience, and may change every row
  referencing it: another member's row of an `owner_only` table refuses the
  call with `DbError::NotOwner`, an admin's call included. A member removed
  from a file's circle keeps its record and is refused with
  `DbError::NotInCircle`; one never in the circle has no record of the file,
  so it is `DbError::FileAbsent`. A file on one device only fails with
  `DbError::NotInCloud`, and an app file with `DbError::NamespaceKind`.
- Until the write commits, a failure or a cancel removes every copy and
  folder the call made, and no file switches; a folder that was there, or
  that someone made meanwhile, stays. If the app drops the call, the store's
  next write removes them; after a crash, the next open.
- Other devices drop their pins and cached bytes of the files when the
  switch arrives, and the stored bytes become deletion candidates. A file
  kept here on another device stops being kept there, and a download for it
  still under way keeps nothing.
- If two devices make one file device-only at once, the later write wins. The
  losing device's copies go when the winning write reaches it, each only while
  it is as coven left it, with the folders coven made for them once empty; a
  copy the person changed meanwhile stays. The losing switch shows as lost
  values of the files' records in `lost_values()`.

## Uploads

A separate upload worker sends queued files, outside sync passes, so a pass
never waits for a file and a file never holds up the write that staged it. An
upload no file waits for any more is retired without sending more.

Each attempt, retries included, checks the source's size, and a user
original's modification time, before sending any bytes. As it streams, it
checks each 64 KiB chunk against its recorded hash before encrypting it. A
changed or missing source fails the upload, reported to this device's app as
`FileUnavailable` in `pending()`; a chunk that differs is never encrypted or
sent.

A file up to the provider's single-request limit, or 32 MiB, whichever is
less, uploads in one request; a larger one in parts, through the provider's
resumable upload. Each part the provider confirms is recorded before it counts
as progress, so after a crash the upload resumes from its confirmed parts.

```rust
let mut outbox = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT p.title, u.sending, u.bytes_confirmed, u.bytes_total
         FROM coven_uploads AS u JOIN photos AS p ON u.file = p.file
         ORDER BY u.position",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?,
                  row.get::<_, Option<i64>>(2)?, row.get::<_, Option<i64>>(3)?)),
    )?)
});
while let Some(rows) = outbox.next().await {
    let rows = rows?;
    show_outbox(&rows);
    if rows.is_empty() {
        break; // every queued photo is stored
    }
}
```

`uploads()` reads the queue once, oldest first, including while
disconnected. The app's SQL reads the same queue, live, from the `coven_uploads`
view, by file id, with what each file waits for and the bytes the provider
confirmed, to order or filter its own rows by it or draw their progress
([Coven's state in SQL](/docs/local-data#coven-s-state-in-sql)).
Its `sending` column, and the `coven_sending` view, say which files this
device is sending right now.
`set_uploads_paused(true)` pauses its trailing uploads, keeping each upload's
place; bound ones run on, since writes wait for them. The `coven_upload_pause`
view says whether uploads are paused, with nothing queued too. Coven retries
failures on its own; `retry_uploads_now()` lets the app try early.
Uploads follow the same stops as writes: a device stuck at a slot, or one that
has been removed, sends nothing.

## File status

A file's record says it is stored only once storage holds it, so a stored file
can be read at once. `handle.file_status(&file)` still says whether storage
holds it, reading the object's metadata at its fixed path. A file on one device
only is judged with no storage request:

| `FileStatus` | When |
| --- | --- |
| `Available` | storage holds an object of the file's complete encrypted size |
| `Uploading` | the file is on this device only, while its upload is queued here |
| `DeviceOnly { device }` | the file is on one device only, with no upload: another device, and nothing here can read it, or this one, which reads it from its bytes here |
| `Missing { device, reason: Lost }` | storage confirms nothing at the path the record names: the provider lost it |
| `Missing { device, reason: Source(..) }` | the file is on this device only, uploading, and its original is missing or coven's copy fails its check; the upload retries. A changed original fails its move, and the file is then `DeviceOnly` |

An object of any other size at the path is damage
(`FileReadError::Integrity`), never available and never absent. A network or
permission failure is an error, never proof of absence; with no storage set up,
the call returns `FileReadError::NoStorage`. The app can stage a missing file
again.

## Reading a file

The app reads a file through a [`FileRef`](rustdoc:struct:coven::FileRef),
taken by the file's id, the id its row holds. A read uses the file's record as
it stands when the file opens, so a reference taken while the file uploaded
reads it from storage once it is stored.

```rust
let recording = handle.file_ref(file).await?;
let stream = handle.open_file_stream(&recording).await?;

// A voice memo: read its header, then seek to where listening resumes.
let header = stream.read_at(0, 64 * 1024).await?;
let resume_at = position_for(&header, saved_seconds);
match stream.read_at(resume_at, 256 * 1024).await {
    Ok(bytes) => play(bytes),
    Err(FileReadError::Storage(error)) if error.failure() == StorageFailure::Network => show_not_downloaded(),
    Err(error) => return Err(error.into()),
}
```

`read_file` returns the whole file. `open_file_stream` opens it for ranges:
`read_at(offset, len)` returns exactly that range, and a range past the end is
an error, never a short read; `read_range` yields the same range as bounded
buffers.

Coven reads a file from wherever it is: the user's original before upload,
coven's own copy of an app-provided file, the cache, or storage. A stored file
not in the cache is read by fetching only the encrypted chunks that cover the
range, neighbouring chunks together up to 1 MiB per request. Each chunk is
checked as it arrives, and fetched chunks go into the cache. While a file is
read in order, coven fetches ahead of the reader so playback doesn't wait on
the network; seeking costs only the chunks under the new position. A complete
read in order checks the whole file against its record's content hash before
returning its last bytes.

Offline, a range whose chunks aren't cached fails with the storage network
error. [Pinning](/docs/cache#pinning) a stored file ahead of time keeps it
readable offline.

A file's key travels inside its record, so a member who can't read the record
can't read the file. If the key needed to read the record hasn't arrived yet,
the write holding it waits with `KeyUnavailable`, and no `FileRef` exists for
it.

| `FileReadError` | When |
| --- | --- |
| `NoStorage` | the read needs storage and none is connected |
| `Storage(..)` | a network or provider failure, with its native cause |
| `DeviceOnly { file, device }` | the file is on another device only |
| `Missing { id, device }` | storage confirms nothing at the path the record names |
| `UserFileMissing`, `UserFileChanged` | the user's original moved or changed |
| `Integrity { id }` | a chunk, a local copy or the stored object failed its check |
| `RangeOutOfBounds` | the range lies outside the file |
| `UpdateRequired` | the file uses a newer coven format |

## How a file is stored

The uploaded object at `<store>/files/<device>/<name>` is encrypted with the
file's own random key, in 64 KiB chunks, each authenticated on its own with its
index bound in ([Encryption](/docs/encryption#files)). The provider sees a
random name and a size, never a hash of the content. Each staged file is a copy
of its own: identical bytes staged twice are stored twice, and deleting one
never touches the other. A file several rows reference is stored once.

Moving a row to another audience, into a circle or out of one, gives it a new
file in its new audience over the same stored bytes and key, with no upload:
the new audience's devices get the key with the new record. Only the device
holding a file that is on one device only can move its row (`NotThisDevice`).

## Deleting stored files

Stored bytes are deleted only when nothing can still point at them: no file
record in a row, lost value, snapshot, queued or published write, or upload
queue. Deleting too early would leave a record naming bytes that are gone for
good. A file's record goes with its last reference, and its bytes stay until
the write deleting it is published. They also stay until every other active
device has published a write that saw the delete, or the switch making the file
device-only, so a new file another device made over them meanwhile, with
`existing_file` or by moving its row, still reads them. An idle device holds
them until it writes or is removed. A device deletes stored bytes only after
it has read and checked everything needed to prove nothing names them, and
only with provider permission. Data it can't read, such as a circle it isn't
in, means it leaves the bytes alone. A quiet pass never lists files.

A removed device can leave files that no surviving record names, and until its
provider access ends it can store more that nothing will ever refer to. When
its removal applies, the device responsible for the cleanup lists that device's
file folder once and deletes what nothing refers to. The app can run that
cleanup again at any time, for example after the person deletes a removed S3
member's key, to delete files the removed device stored later. Deleting a
file's record forgets coven's record of a user's original; the original itself
is never deleted by any of this.
