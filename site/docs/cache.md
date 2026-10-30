# Cache

An uploaded file lives in storage, encrypted. To show a photo or play a song, a
device needs its bytes on local disk. The cache is where those bytes live: each
device keeps uploaded files, and pieces of them, within a size budget the app
sets. This page covers the budget, pinning and progress. How a
file is staged, uploaded and read is the [Files](/docs/files) page.

The cache holds only uploaded files, so everything in it can be fetched again.
A file still waiting to upload is read from its source on this device: the
user's original, or coven's own copy of app-provided bytes. The cache keeps the
encrypted chunks as storage holds them, checked as they arrived; reads decrypt
them.

## The budget

Each namespace the app declares when it opens the store has a cache budget in
bytes ([Files](/docs/files#declaring-files)). Each namespace evicts on its own,
so a big namespace can't push out a small one:

```rust
vec![
    Namespace::new("audio", Provenance::UserProvided, 2 << 30),
    Namespace::new("covers", Provenance::AppProvided, 64 << 20),
]
```

The declared budget applies when the namespace is first declared. After that,
`handle.set_cache_budget("audio", bytes)` changes it, and
`handle.get_cache_budget("audio")` reads it. Declaring a namespace twice fails
opening (`SchemaError::DuplicateNamespace`).

When a namespace is over budget, the least recently used files and chunks go
first. Freeing space never fails a read.

`handle.evict_file(&file)` removes a stored file's copies from the cache,
pinned or not. It never touches storage or a file waiting to upload; a later
read downloads it again.

## Reads fill the cache

A device caches a file when it reads it; a record arriving downloads nothing.
Reading a stored file that isn't cached fetches only the chunks the read
needs and keeps them, so reading the same range again costs nothing. Ben plays
the first minute of a song on his laptop: those chunks are cached. He seeks to
the chorus: only the chunks under the new position are fetched.

## Pinning

The cache is evictable. Pinning keeps a file whole on the device whatever the
budget, until the app unpins it: the gesture for "keep this for the flight".

```rust
let files = vec![handle.file_ref(track_file).await?];
let mut progress = handle.subscribe_pin_progress(files.clone());
handle.pin(&files).await?; // returns once every file is whole on this device
```

Pinning assembles a complete cache file from the chunks already cached and the
ones it fetches, checks it against the record's content hash, and syncs it to
disk before the pin counts. A cancelled or crashed pin leaves no half-downloaded
file exempt from eviction. Only stored files are pinned: pinning a file still on
one device only, this one included while it uploads, fails with
`FileReadError::DeviceOnly`, and the app pins it once it is stored.

`handle.unpin(&files)` stops keeping them; they stay in the cache until the
budget evicts them. To show which files are kept offline, `files_pinned(files)`
answers with one `Option<bool>` per file id, `None` for a file this device has
no record of, and `subscribe_files_pinned` keeps the answer current. The app's
SQL reads the same answer from the `coven_pins` table, which lists every pinned
file by id, to count or filter its own rows by it, and its local triggers can
follow it
([Coven's state in SQL](/docs/local-data#coven-s-state-in-sql)).

## Progress and failures

Pin progress is a [`PinProgress`](rustdoc:struct:coven::PinProgress):
files completed and requested, and bytes cached and total. It counts only
checked bytes committed to the cache, never bytes merely received, so after a
restart the first reads show the same progress, even before storage
reconnects.

Reads and pins of one file share its cached ranges. A call that fails
returns its typed error at once, and progress stays at what was committed.
Only a pin's failure goes on the pending list, as the file's one record of its
first unmet condition: a pin is a lasting wish for the bytes. A read's failure
goes to its caller only, since nothing is left to finish once the caller has
it; a player shows a song it couldn't fetch offline, and the pending list
stays empty. Ben's laptop
is pinning a song and has two checked ranges when Wi-Fi drops. The pending list reports the network failure, progress stays
at those two ranges, and pinning again fetches the rest. Nothing retries a
download on its own. A final content hash that
doesn't match instead leaves a permanent refusal (`Refused(ContentHash)`) and
publishes no completed file or pin.

| Pending reason | When | Next |
| --- | --- | --- |
| `NoStorage` | no storage connected | |
| `Storage { .. }` | a network or provider failure | the next read or pin tries again |
| `FileUnavailable(Lost)` | storage confirms nothing at the path the record names | needs the app to act |
| `FileUnavailable(Source(..))` | the original of a file this device uploads moved or changed | the upload retries |
| `Refused(Decryption)`, `Refused(ContentHash)` | a complete chunk or the whole file failed its check | not retried on the same bytes |
| `Failed(Disk)` | writing the local cache failed | |
