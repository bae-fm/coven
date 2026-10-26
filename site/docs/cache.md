# Cache

An uploaded file lives in storage, encrypted. To show a photo or play a song, a
device needs its bytes on local disk. The cache is where those bytes live: each
device keeps uploaded files, and pieces of them, within a size budget the app
sets. This page covers the budget, pinning and progress. How a
file is attached, uploaded and read is the [Files](/docs/files) page.

The cache holds only uploaded files, so everything in it can be fetched again.
A file still waiting to upload is read from its source on this device: the
user's original, or coven's own copy of app-provided bytes. The cache keeps the
encrypted chunks as storage holds them, checked as they arrived; reads decrypt
them.

## The budget

Each file declaration names a *namespace* and that namespace's cache budget in
bytes ([Files](/docs/files#declaring-files)). Each namespace evicts on its own,
so a big namespace can't push out a small one:

```rust
vec![
    SyncedTable::new("tracks", RowIdentity::IndependentUuid)
        .carries_files(FileDecl::new("audio", 2 << 30, Provenance::UserProvided)),
    SyncedTable::new("covers", RowIdentity::IndependentUuid)
        .carries_files(FileDecl::new("covers", 64 << 20, Provenance::AppProvided)),
]
```

The declared budget applies when the namespace is first declared. After that,
`handle.set_cache_budget("audio", bytes)` changes it, and
`handle.get_cache_budget("audio")` reads it. Tables that share a namespace must
declare the same budget (`SchemaError::CacheBudgetDisagrees`).

When a namespace is over budget, the least recently used files and chunks go
first. Freeing space never fails a read.

`handle.evict_file(&file)` removes an uploaded file's copies from the cache,
pinned or not. It never touches storage or a file waiting to upload; a later
read downloads it again.

## Reads fill the cache

A device caches a file when it reads it; a row arriving downloads nothing.
Reading an uploaded file that isn't cached fetches only the chunks the read
needs and keeps them, so reading the same range again costs nothing. Ben plays
the first minute of a song on his laptop: those chunks are cached. He seeks to
the chorus: only the chunks under the new position are fetched.

## Pinning

The cache is evictable. Pinning keeps a file whole on the device whatever the
budget, until the app unpins it: the gesture for "keep this for the flight".

```rust
let files = vec![handle.file_ref("tracks", track).await?];
let mut progress = handle.subscribe_pin_progress(files.clone());
handle.pin(&files).await?; // returns once every file is whole on this device
```

Pinning assembles a complete cache file from the chunks already cached and the
ones it fetches, checks it against the row's content hash, and syncs it to disk
before the pin counts. A cancelled or crashed pin leaves no half-downloaded file
exempt from eviction. A file still queued for upload on this device is pinned at
once, and stays pinned through its upload. An app-provided file keeps the copy
it uploaded from; a user's original is never read again after its upload, so
coven keeps the stored file whole from storage in the background.

`handle.unpin(&files)` stops keeping them; they stay in the cache until the
budget evicts them. To show which rows are kept offline, `rows_pinned(table,
keys)` answers with one `Option<bool>` per key, `None` for a row with no file,
and `subscribe_rows_pinned` keeps the answer current. The app's SQL reads the
same answer from the `coven_pins` view, which lists every pinned row, to count
or filter its own rows by it
([Coven's state in SQL](/docs/local-data#coven-s-state-in-sql)).

## Progress and failures

Pin progress is a [`PinProgress`](rustdoc:struct:coven::PinProgress):
files completed and requested, and bytes cached and total. It counts only
checked bytes committed to the cache, never bytes merely received, so after a
restart the first reads show the same progress, even before storage
reconnects.

Reads and pins of one file share its cached ranges and one pending record. A
call that fails returns its typed error at once; the pending list records the
first unmet condition, and progress stays at what was committed. Ben's laptop
is pinning a song and has two checked ranges when Wi-Fi drops. The pending list reports the network failure, progress stays
at those two ranges, and the retry fetches the rest. A final content hash that
doesn't match instead leaves a permanent refusal (`Refused(ContentHash)`) and
publishes no completed file or pin.

| Pending reason | When | Next |
| --- | --- | --- |
| `NoStorage` | no storage connected | |
| `Storage { .. }` | a network or provider failure | retried by its failure class |
| `FileUnavailable(Lost)` | storage confirms nothing at the path a row names | needs the app to act |
| `FileUnavailable(Source(..))` | this device's pending attachment's original moved or changed | the upload retries |
| `Refused(Decryption)`, `Refused(ContentHash)` | a complete chunk or the whole file failed its check | not retried on the same bytes |
| `Failed(Disk)` | writing the local cache failed | |
