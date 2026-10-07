# coven

Sync normally means a backend: a server you run and pay for, and a database
that holds every user's data in the clear.

coven syncs without the server. Devices exchange end-to-end-encrypted changes
through storage the user already has (Google Drive, Dropbox, OneDrive, iCloud,
or any S3-compatible endpoint), and merge them locally.

The data is SQLite. You keep your schema; coven owns the connections and runs
your queries through them, so it can capture each change with SQLite's session
extension, encrypt and sign it, move it through the user's storage, and apply
other devices' changes back.

This Cargo workspace is the implementation. Its design is
[`spec/coven.md`](spec/coven.md), and every byte it
writes to storage is in [`spec/format.md`](spec/format.md).

Docs: <https://coven.bae.fm>
