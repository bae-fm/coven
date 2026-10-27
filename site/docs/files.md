# Files

Rows can carry files: songs, photos, documents. A file is far too large to
travel in a slot, yet it has to sync with its row, be readable only by the
row's readers, be readable a piece at a time while a song plays, and never put
the user's own original at risk. This page covers declaring files, attaching
them, uploads, status and reading. The device's copies of downloaded files are
the [Cache](/docs/cache) page.

## Declaring files

A synced table whose rows carry a file says so with `carries_files` and a
[`FileDecl`](rustdoc:struct:coven::FileDecl):

```rust
fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        // The user's own file.
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("attachments", 2 << 30, Provenance::UserProvided)),
        // A thumbnail the app makes.
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("thumbnails", 256 << 20, Provenance::AppProvided)),
    ]
}
```

`FileDecl::new` takes:

- a **namespace**, which groups files for the [cache](/docs/cache), and the
  namespace's cache budget in bytes;
- the file's **kind**: `Provenance::UserProvided` or `Provenance::AppProvided`
  ([below](#two-kinds-of-file)).

Other devices download a file when they read it, into their
[cache](/docs/cache), or keep it whole when the app pins it.

It also names four columns of the table, with these defaults:

| Column | Default | Holds |
| --- | --- | --- |
| id | `id` | which file the row refers to |
| size | `size` | its size in bytes; a user-provided file is checked against it |
| hash | `hash` | the SHA-256 of its bytes, which coven fills in |
| where | `location` | its fixed storage reference and key, which coven fills in |

`with_id_column`, `with_size_column`, `with_hash_column` and
`with_location_column` change them. A row with no file has NULL in its hash and
where columns, so both must allow NULL; app SQL can't assign either
(`DbError::FileColumnWrite`). `write_once()` refuses a write that points an
existing row at a different file.

**The four columns move together.** A write that changes any of a row's file
columns writes all four, unchanged ones included, so a row's file always comes
whole from one write. Ana's phone and Ben's laptop each attach a different file
to row 7: the later write wins all four columns, never one file's size with the
other's hash. For the same reason coven refuses a foreign key with set null on
any of the four.

## Two kinds of file

A **user-provided** file is the user's own file at a path on their device, such
as a song in their music folder. Coven records its path, size and modification
time. It never copies, moves, changes or deletes it. If the file moves or
changes before it is uploaded, coven reports it missing or changed before
reading any of it. After a successful upload, coven never reads the original
again; reads use the cache or storage.

An **app-provided** file is bytes the app hands to coven, which keeps and owns
them. The app can hand them over as a stream, so a large file never has to fit
in memory.

For both kinds, the first read records the whole-file hash and a SHA-256 of
each 64 KiB chunk. Every upload attempt checks against those, so a retry can
never encrypt different bytes than the first attempt did.

## Attaching a file

Attaching a file is part of the write that changes its row. A user's file is
read once before the write, with `prepare_user_file`, then recorded on the row
inside the write:

```rust
let note = UuidText(Uuid::now_v7());
let attachment = UuidText(Uuid::now_v7());
let size = std::fs::metadata(&path)?.len() as i64;
let prepared = prepare_user_file(&path, |read| show_progress(read)).await?;

handle.write(move |sql| {
    sql.execute(
        "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, ?3)",
        (note, "Paint colors", Audience::Store),
    )?;
    sql.execute(
        "INSERT INTO attachments (id, note_id, title, size) VALUES (?1, ?2, ?3, ?4)",
        (attachment, note, "Swatches", size),
    )?;
    sql.register_user_file("attachments", attachment, prepared)?;
    Ok(())
}).await?;
```

`insert_user_file` runs the app's `INSERT` and records the file in one call.
`clear_user_file` forgets a row's user file and clears its hash and where
columns; the original is untouched.

App-provided bytes go through `write_with_files`, whose first closure hands
coven the files and whose second runs the write that refers to them:

```rust
let thumbnail = Uuid::now_v7().to_string();
let staged = thumbnail.clone();
handle.write_with_files(
    move |batch| {
        batch.put_file("thumbnails", staged, png_bytes);
        Ok(())
    },
    move |sql| {
        sql.execute(
            "INSERT INTO thumbnails (id, note_id, size) VALUES (?1, ?2, ?3)",
            (&thumbnail, note, png_len),
        )?;
        Ok(())
    },
).await?;
```

`put_file` takes a byte buffer or a stream, read once before the write takes
the database writer. A staged file no row names, or a row naming an id nothing
staged, fails the write.

The attaching write picks the file's random id and key, records this device as
its uploader, and fixes its storage path, `<store>/files/<device>/<file>`. It
queues the upload in the same transaction, even with no storage connected, and
records a *pending attachment* on this device. It doesn't change the row's
file: other devices see the row as it was, with no file or with the one it had
before. On this device, the pending attachment is the row's file:
`file_ref` returns it, and reads use the original or coven's copy.

When the upload completes, one more write names the file in the row, and other
devices can read it as soon as they receive that write. No device ever holds a
reference to bytes that aren't stored yet.

A newer change wins. Ana attaches photo A to row 7, then Ben replaces it with
photo B. If Ana's phone applies Ben's write before A's upload finishes, A is
abandoned: its completion writes nothing, and B stays. The same goes for a
later write of Ana's own that clears or replaces the file, deletes the row or
moves it to another audience. Each abandonment is listed by
`abandoned_attachments()`, and in the app's SQL through the `coven_abandoned`
view, until the app dismisses it by its file id with
`dismiss_abandoned_attachments`, so the app can tell the person and offer to
attach the file again. The view's `key` reads as a `RowKey`, so a row that is
gone is still named. If Ben's write reaches Ana's phone only after her upload
completed, the two writes are concurrent: the later one wins, and the other
file stays visible as a lost value.

## Uploads

A separate upload worker sends queued files, outside sync passes, so a pass
never waits for a file and a file never holds up the write that attached it.
An upload whose attachment was abandoned is retired without sending more.

Each attempt, retries included, checks the source's size and whole-file hash,
and a user original's modification time, before sending any bytes. As it
streams, it checks each 64 KiB chunk against its recorded hash before
encrypting it. A changed or missing source fails the upload, reported to this
device's app as `FileUnavailable` in `pending()`; a chunk that differs is never
encrypted or sent.

A file up to the provider's single-request limit, or 32 MiB, whichever is
less, uploads in one request; a larger one in parts, through the provider's
resumable upload. Each part the provider confirms is recorded before it counts
as progress, so after a crash the upload resumes from its confirmed parts.

```rust
let mut uploads = handle.subscribe_uploads();
while let Some(state) = uploads.next().await {
    let state = state?;
    for upload in &state.files {
        let sending = upload.phase == UploadPhase::Uploading;
        if let Some(UploadProgress { bytes_confirmed, bytes_total }) = upload.progress {
            show_progress(upload.file.key(), sending, bytes_confirmed, bytes_total);
        }
    }
    if state.files.is_empty() {
        break; // every queued file is stored
    }
}
```

`uploads()` and `subscribe_uploads()` read the queue, oldest first, including
while disconnected. The app's SQL reads the same queue from the `coven_uploads`
view, with what each file waits for and the bytes the provider confirmed, to
order or filter its own rows by it or draw their progress
([Coven's state in SQL](/docs/local-data#coven-s-state-in-sql)).
Its `sending` column, and the `coven_sending` view, say which files this
device is sending right now.
`set_uploads_paused(true)` pauses it, keeping each upload's place; the
`coven_upload_pause` view says whether it is paused, with nothing queued too. Coven
retries failures on its own; `retry_uploads_now()` lets the app try early.
Uploads follow the same stops as writes: a device stuck at a slot, or one that
has been removed, sends nothing.

## File status

A row names a file only once it is stored, so a file can be read as soon as
its row names it. `handle.file_status(&file)` still says whether storage holds
it, reading the object's metadata at its fixed path. On the attaching device, a
pending attachment is judged locally, with no storage request:

| `FileStatus` | When |
| --- | --- |
| `Available` | storage holds an object of the file's complete encrypted size |
| `Uploading` | this device's own pending attachment, still queued here |
| `Missing { device, reason: Lost }` | storage confirms nothing at the path a row names: the provider lost it |
| `Missing { device, reason: Source(..) }` | this device's pending attachment, whose original moved or changed; the upload retries |

An object of any other size at the path is damage
(`FileReadError::Integrity`), never available and never absent. A network or
permission failure is an error, never proof of absence; with no storage set up,
the call returns `FileReadError::NoStorage`. The app can ask for a missing file
to be attached again.

## Reading a file

The app reads a file through a [`FileRef`](rustdoc:struct:coven::FileRef),
taken from its row and naming the file the row had then. A read checks the
reference against the row first, so a row changed since can't redirect it to
another file. A write can check a reference the same way with
`sql.validate_file_ref(&file)` before changing the row.

```rust
let recording = handle.file_ref("attachments", attachment).await?;
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
coven's own copy of an app-provided file, the cache, or storage. An uploaded
file not in the cache is read by fetching only the encrypted chunks that cover
the range, neighbouring chunks together up to 1 MiB per request. Each chunk is
checked as it arrives, and fetched chunks go into the cache. While a file is
read in order, coven fetches ahead of the reader so playback doesn't wait on
the network; seeking costs only the chunks under the new position. A complete
read in order checks the whole file against the row's content hash before
returning its last bytes.

Offline, a range whose chunks aren't cached fails with the storage network
error. [Pinning](/docs/cache#pinning) a file ahead of time keeps it readable
offline.

A file's key travels inside its row, so a member who can't read the row can't
read the file. If the key needed to read the row hasn't arrived yet, the row
itself waits with `KeyUnavailable`, and no `FileRef` exists for it.

| `FileReadError` | When |
| --- | --- |
| `NoStorage` | the read needs storage and none is connected |
| `Storage(..)` | a network or provider failure, with its native cause |
| `Missing { id, device }` | storage confirms nothing at the path a row names |
| `UserFileMissing`, `UserFileChanged` | the user's original moved or changed |
| `Integrity { id }` | a chunk, a local copy or the stored object failed its check |
| `RangeOutOfBounds` | the range lies outside the file |
| `UpdateRequired` | the file uses a newer coven format |

## How a file is stored

The uploaded object at `<store>/files/<device>/<file>` is encrypted with the
file's own random key, in 64 KiB chunks, each authenticated on its own with its
index bound in ([Encryption](/docs/encryption#files)). The provider sees a
random name and a size, never a hash of the content. Each upload is a copy of
its own: identical files attached to two rows are stored twice, and deleting
one never touches the other.

Moving a row between the store and a circle changes nothing about its file:
the new audience's devices get the key with the row.

## Deleting stored files

A stored file is deleted only when nothing can still point at it: no row, lost
value, snapshot, queued or published write, or upload queue. Deleting too
early would leave a row naming a file that's gone for good. So a device deletes
a stored file only after it has read and checked everything needed to prove
that, and only with provider permission. Data it can't read, such as a circle
it isn't in, means it leaves the file alone. A quiet pass never lists files.

A removed device can leave files whose rows didn't survive, and until its
provider access ends it can store more that nothing will ever refer to. When
its removal applies, the device responsible for the cleanup lists that device's
file folder once and deletes what nothing refers to. The app can run that
cleanup again at any time, for example after the person deletes a removed S3
member's key, to delete files the removed device stored later. Deleting a row deletes only coven's copies of its file;
a user's original is never deleted by any of this.
