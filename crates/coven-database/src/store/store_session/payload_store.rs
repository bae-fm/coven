//! Content-addressed payloads owned by Store database rows.
//!
//! Every payload is compressed. Compressed values through 64 KiB live in SQLite;
//! larger values live in files beside it. The catalog on the same connection is
//! the sole authority for which representation a hash uses.
//!
//! Deletion rides row deletion, counted by owner. Rows of different kinds can
//! name the same payload — a Circle operation and the remote object it prepared
//! both need one object's bytes — so a row does not delete the storage it is done
//! with; it drops its claim with [`set_payload_owner_claims_on`], and the
//! transaction that drops the last claim records the deletion obligation.
//!
//! [`pay_owed_payload_deletions_on`] is the other half. Every call this store
//! makes runs it once the call's own work has returned, so the flow that
//! committed an obligation is the flow that discharges it and no sweeper exists
//! to be kept correct. A failure between removing file-backed bytes and clearing
//! the obligation leaves the obligation durable, so the caller's retry finishes
//! the deletion rather than losing it.

use std::collections::BTreeSet;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use coven_foundation::atomic_file::AtomicFileStage;
use coven_foundation::store_dir::StoreDir;
use rusqlite::{Connection, OptionalExtension};
use tracing::debug;

use super::StoreTransaction;
#[cfg(any(test, feature = "test-utils"))]
use super::{StoreDatabase, StoreSession};
use crate::DbError;
use coven_protocol::store_commit::ObjectHash;

#[derive(Debug, thiserror::Error)]
pub enum PayloadStoreError {
    /// A payload a caller holds the hash of is not in the spool. The row naming
    /// it and the file it names have gone out of step, which no flow may
    /// recover from by reading something else.
    #[error("payload {hash} is absent from the spool at {}", path.display())]
    Missing { hash: ObjectHash, path: PathBuf },
    #[error(
        "payload at {} hashes to {actual}, but its row names {expected}",
        path.display()
    )]
    ContentMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
        path: PathBuf,
    },
    #[error("payload spool {}: {error}", path.display())]
    File { path: PathBuf, error: String },
    #[error("payload {hash} has invalid storage metadata: {error}")]
    Storage { hash: ObjectHash, error: String },
    #[error("inline payload {expected} contains bytes hashing to {actual}")]
    InlineContentMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("payload {hash} has invalid compressed bytes: {error}")]
    Compression { hash: ObjectHash, error: String },
}

const INLINE_PAYLOAD_LIMIT: usize = 64 * 1024;

enum StoredPayload {
    Inline {
        compressed: Vec<u8>,
        payload_size: u64,
    },
    File {
        compressed_size: u64,
        payload_size: u64,
    },
}

/// One database connection's closed access to the payload bytes its rows own.
/// The catalog on that connection selects inline SQLite bytes or the Store
/// directory's file spool; callers never receive either dependency.
#[derive(Clone, Copy)]
pub(crate) struct PayloadStore<'store> {
    conn: &'store Connection,
    store_dir: &'store StoreDir,
}

impl<'store> PayloadStore<'store> {
    pub(crate) fn new(conn: &'store Connection, store_dir: &'store StoreDir) -> Self {
        Self { conn, store_dir }
    }

    pub(crate) fn install(self, bytes: &[u8]) -> Result<ObjectHash, PayloadStoreError> {
        let hash = ObjectHash::digest(bytes);
        self.require_transaction(hash)?;
        let compressed = compress_payload(hash, bytes)?;
        match self.stored(hash)? {
            Some(StoredPayload::Inline {
                compressed: installed,
                payload_size,
            }) => {
                if installed == compressed && payload_size == bytes.len() as u64 {
                    Ok(hash)
                } else {
                    Err(PayloadStoreError::Storage {
                        hash,
                        error: "installed inline representation differs from the payload"
                            .to_string(),
                    })
                }
            }
            Some(StoredPayload::File { .. }) => {
                self.record_file(hash, bytes.len() as u64, compressed.len() as u64)?;
                write_payload_file_bytes_blocking(self.store_dir, hash, &compressed)?;
                Ok(hash)
            }
            None if compressed.len() <= INLINE_PAYLOAD_LIMIT => {
                self.record_inline(hash, bytes.len() as u64, &compressed)?;
                Ok(hash)
            }
            None => {
                self.record_file(hash, bytes.len() as u64, compressed.len() as u64)?;
                write_payload_file_bytes_blocking(self.store_dir, hash, &compressed)?;
                Ok(hash)
            }
        }
    }

    pub(crate) fn read(self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        let stored = self
            .stored(hash)?
            .ok_or_else(|| PayloadStoreError::Storage {
                hash,
                error: "no catalog row".to_string(),
            })?;
        self.decode_stored(hash, stored)
    }

    fn decode_stored(
        self,
        hash: ObjectHash,
        stored: StoredPayload,
    ) -> Result<Vec<u8>, PayloadStoreError> {
        let (compressed, payload_size) = match stored {
            StoredPayload::Inline {
                compressed,
                payload_size,
            } => (compressed, payload_size),
            StoredPayload::File {
                compressed_size,
                payload_size,
            } => {
                let compressed = read_payload_file_blocking(self.store_dir, hash)?;
                if compressed.len() as u64 != compressed_size {
                    return Err(PayloadStoreError::Storage {
                        hash,
                        error: format!(
                            "catalog records {compressed_size} compressed file bytes, but the spool contains {}",
                            compressed.len()
                        ),
                    });
                }
                (compressed, payload_size)
            }
        };
        let bytes = decompress_payload(hash, &compressed, payload_size)?;
        if bytes.len() as u64 != payload_size {
            return Err(PayloadStoreError::Storage {
                hash,
                error: format!(
                    "catalog records {payload_size} payload bytes, but decompression produced {}",
                    bytes.len()
                ),
            });
        }
        Ok(bytes)
    }

    pub(crate) fn read_verified(self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        let stored = self
            .stored(hash)?
            .ok_or_else(|| PayloadStoreError::Storage {
                hash,
                error: "no catalog row".to_string(),
            })?;
        let inline = matches!(stored, StoredPayload::Inline { .. });
        let bytes = self.decode_stored(hash, stored)?;
        let actual = ObjectHash::digest(&bytes);
        if actual == hash {
            return Ok(bytes);
        }
        if inline {
            Err(PayloadStoreError::InlineContentMismatch {
                expected: hash,
                actual,
            })
        } else {
            Err(PayloadStoreError::ContentMismatch {
                expected: hash,
                actual,
                path: self.store_dir.payload_spool_path(hash),
            })
        }
    }

    fn writer(self) -> PayloadWriter<'store> {
        PayloadWriter {
            payloads: self,
            encoder: lz4_flex::frame::FrameEncoder::new(CompressedPayloadTarget::new(
                self.store_dir,
            )),
            hasher: coven_protocol::blob::ContentHasher::new(),
            size: 0,
        }
    }

    fn require_transaction(self, hash: ObjectHash) -> Result<(), PayloadStoreError> {
        if self.conn.is_autocommit() {
            return Err(PayloadStoreError::Storage {
                hash,
                error: "installation requires the owning database transaction".to_string(),
            });
        }
        Ok(())
    }

    fn stored(self, hash: ObjectHash) -> Result<Option<StoredPayload>, PayloadStoreError> {
        let row = self
            .conn
            .query_row(
                "SELECT storage, payload_size, compressed_bytes, compressed_size
                 FROM payload_storage WHERE payload_hash = ?1",
                [hash.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| PayloadStoreError::Storage {
                hash,
                error: error.to_string(),
            })?;
        match row {
            None => Ok(None),
            Some((storage, payload_size, Some(compressed), compressed_size))
                if storage == "inline"
                    && payload_size >= 0
                    && compressed_size == compressed.len() as i64 =>
            {
                Ok(Some(StoredPayload::Inline {
                    compressed,
                    payload_size: payload_size as u64,
                }))
            }
            Some((storage, payload_size, None, compressed_size))
                if storage == "file" && payload_size >= 0 && compressed_size > 0 =>
            {
                Ok(Some(StoredPayload::File {
                    compressed_size: compressed_size as u64,
                    payload_size: payload_size as u64,
                }))
            }
            Some((storage, payload_size, compressed, compressed_size)) => Err(PayloadStoreError::Storage {
                hash,
                error: format!(
                    "tag {storage:?}, payload size {payload_size}, compressed bytes {}, compressed size {compressed_size}",
                    compressed
                        .as_ref()
                        .map_or("absent".to_string(), |bytes| format!(
                            "{} bytes",
                            bytes.len()
                        ))
                ),
            }),
        }
    }

    fn record_inline(
        self,
        hash: ObjectHash,
        payload_size: u64,
        compressed: &[u8],
    ) -> Result<(), PayloadStoreError> {
        let payload_size =
            i64::try_from(payload_size).map_err(|error| PayloadStoreError::Storage {
                hash,
                error: error.to_string(),
            })?;
        let compressed_size =
            i64::try_from(compressed.len()).map_err(|error| PayloadStoreError::Storage {
                hash,
                error: error.to_string(),
            })?;
        match self.stored(hash)? {
            None => self
                .conn
                .execute(
                    "INSERT INTO payload_storage
                     (payload_hash, payload_size, storage, compressed_bytes, compressed_size)
                     VALUES (?1, ?2, 'inline', ?3, ?4)",
                    rusqlite::params![hash.to_string(), payload_size, compressed, compressed_size],
                )
                .map(|_| ())
                .map_err(|error| PayloadStoreError::Storage {
                    hash,
                    error: error.to_string(),
                }),
            Some(StoredPayload::Inline {
                compressed: stored,
                payload_size: stored_payload_size,
            }) if stored == compressed && stored_payload_size == payload_size as u64 => Ok(()),
            Some(StoredPayload::Inline { .. }) => Err(PayloadStoreError::Storage {
                hash,
                error: "installed inline representation differs from the payload".to_string(),
            }),
            Some(StoredPayload::File { .. }) => Err(PayloadStoreError::Storage {
                hash,
                error: "an inline installation conflicts with file storage".to_string(),
            }),
        }
    }

    fn record_file(
        self,
        hash: ObjectHash,
        payload_size: u64,
        compressed_size: u64,
    ) -> Result<(), PayloadStoreError> {
        let payload_size =
            i64::try_from(payload_size).map_err(|error| PayloadStoreError::Storage {
                hash,
                error: error.to_string(),
            })?;
        let compressed_size =
            i64::try_from(compressed_size).map_err(|error| PayloadStoreError::Storage {
                hash,
                error: error.to_string(),
            })?;
        match self.stored(hash)? {
            None => self
                .conn
                .execute(
                    "INSERT INTO payload_storage
                     (payload_hash, payload_size, storage, compressed_bytes, compressed_size)
                     VALUES (?1, ?2, 'file', NULL, ?3)",
                    rusqlite::params![hash.to_string(), payload_size, compressed_size],
                )
                .map(|_| ())
                .map_err(|error| PayloadStoreError::Storage {
                    hash,
                    error: error.to_string(),
                }),
            Some(StoredPayload::File {
                compressed_size: stored_compressed_size,
                payload_size: stored_payload_size,
            }) if stored_compressed_size == compressed_size as u64
                && stored_payload_size == payload_size as u64 => Ok(()),
            Some(StoredPayload::File {
                compressed_size: stored_compressed_size,
                payload_size: stored_payload_size,
            }) => Err(PayloadStoreError::Storage {
                hash,
                error: format!(
                    "catalog sizes ({stored_payload_size} payload, {stored_compressed_size} compressed) differ from installed sizes ({payload_size} payload, {compressed_size} compressed)"
                ),
            }),
            Some(StoredPayload::Inline { .. }) => Err(PayloadStoreError::Storage {
                hash,
                error: "a file installation conflicts with inline storage".to_string(),
            }),
        }
    }
}

/// One payload being streamed into an unpublished file while its content hash
/// is computed from the bytes the file accepted.
pub(crate) struct PayloadWriter<'store> {
    payloads: PayloadStore<'store>,
    encoder: lz4_flex::frame::FrameEncoder<CompressedPayloadTarget<'store>>,
    hasher: coven_protocol::blob::ContentHasher,
    size: u64,
}

enum PayloadWriterTarget {
    Inline(Vec<u8>),
    File(AtomicFileStage),
}

struct CompressedPayloadTarget<'store> {
    store_dir: &'store StoreDir,
    target: PayloadWriterTarget,
    size: u64,
}

impl<'store> CompressedPayloadTarget<'store> {
    fn new(store_dir: &'store StoreDir) -> Self {
        Self {
            store_dir,
            target: PayloadWriterTarget::Inline(Vec::new()),
            size: 0,
        }
    }
}

impl<'store> PayloadWriter<'store> {
    pub(crate) fn commit(self) -> Result<(ObjectHash, u64), PayloadStoreError> {
        let hash = self
            .hasher
            .finish()
            .parse::<ObjectHash>()
            .expect("SHA-256 hex is an ObjectHash");
        self.payloads.require_transaction(hash)?;
        let compressed = self
            .encoder
            .finish()
            .map_err(|error| PayloadStoreError::Compression {
                hash,
                error: error.to_string(),
            })?;
        match compressed.target {
            PayloadWriterTarget::Inline(bytes) => {
                self.payloads.record_inline(hash, self.size, &bytes)?;
            }
            PayloadWriterTarget::File(staged) => {
                let path = self.payloads.store_dir.payload_spool_path(hash);
                self.payloads
                    .record_file(hash, self.size, compressed.size)?;
                staged
                    .commit(&path)
                    .map_err(|error| PayloadStoreError::File {
                        path,
                        error: error.to_string(),
                    })?;
            }
        }
        Ok((hash, self.size))
    }
}

impl<'store, 'connection> StoreTransaction<'store, 'connection> {
    pub(crate) fn payload_writer(self) -> PayloadWriter<'store> {
        PayloadStore::new(self.transaction, self.store_dir).writer()
    }
}

impl std::io::Write for PayloadWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.encoder.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.size = self
            .size
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("payload size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.encoder.flush()
    }
}

impl std::io::Write for CompressedPayloadTarget<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = match &mut self.target {
            PayloadWriterTarget::Inline(buffer)
                if buffer.len().saturating_add(bytes.len()) <= INLINE_PAYLOAD_LIMIT =>
            {
                buffer.extend_from_slice(bytes);
                bytes.len()
            }
            PayloadWriterTarget::Inline(buffer) => {
                let directory = self.store_dir.payload_spool_dir();
                let mut staged = self
                    .store_dir
                    .create_payload_spool_stage()
                    .map_err(|error| {
                        std::io::Error::new(
                            error.kind(),
                            format!("create payload stage {}: {error}", directory.display()),
                        )
                    })?;
                staged.write_all(buffer)?;
                let written = staged.write(bytes)?;
                self.target = PayloadWriterTarget::File(staged);
                written
            }
            PayloadWriterTarget::File(staged) => staged.write(bytes)?,
        };
        self.size = self
            .size
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("compressed payload size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.target {
            PayloadWriterTarget::Inline(_) => Ok(()),
            PayloadWriterTarget::File(staged) => staged.flush(),
        }
    }
}

fn compress_payload(hash: ObjectHash, bytes: &[u8]) -> Result<Vec<u8>, PayloadStoreError> {
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder
        .write_all(bytes)
        .map_err(|error| PayloadStoreError::Compression {
            hash,
            error: error.to_string(),
        })?;
    encoder
        .finish()
        .map_err(|error| PayloadStoreError::Compression {
            hash,
            error: error.to_string(),
        })
}

fn decompress_payload(
    hash: ObjectHash,
    compressed: &[u8],
    payload_size: u64,
) -> Result<Vec<u8>, PayloadStoreError> {
    let mut bytes = Vec::new();
    lz4_flex::frame::FrameDecoder::new(compressed)
        .take(payload_size.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| PayloadStoreError::Compression {
            hash,
            error: error.to_string(),
        })?;
    Ok(bytes)
}

/// Delete the payload behind every committed cleanup obligation, clearing each
/// obligation once its file is gone.
///
/// Runs on the database's connection thread, where the transactions that record
/// obligations also run, so a payload cannot be re-claimed and rewritten between
/// this reading its obligation and removing its file. A failure between the two
/// leaves the obligation durable and fails the caller, so the caller's retry
/// finishes the deletion rather than losing it.
pub(crate) fn pay_owed_payload_deletions_on(
    conn: &Connection,
    store_dir: &StoreDir,
) -> Result<(), DbError> {
    for hash in payload_cleanup_hashes_on(conn)? {
        let payloads = PayloadStore::new(conn, store_dir);
        match payloads.stored(hash).map_err(DbError::from)? {
            Some(StoredPayload::Inline { .. }) => {}
            Some(StoredPayload::File { .. }) => {
                delete_payload_file_blocking(store_dir, hash).map_err(DbError::from)?;
            }
            None => {
                return Err(DbError::Message(format!(
                    "payload deletion obligation {hash} has no storage row"
                )));
            }
        }
        let transaction = conn.unchecked_transaction().map_err(DbError::from)?;
        transaction
            .execute(
                "DELETE FROM payload_cleanup WHERE payload_hash = ?1",
                [hash.to_string()],
            )
            .map_err(DbError::from)?;
        let removed = transaction
            .execute(
                "DELETE FROM payload_storage
                 WHERE payload_hash = ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM payload_owners
                       WHERE payload_hash = ?1
                   )",
                [hash.to_string()],
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(format!(
                "payload deletion obligation {hash} is still claimed"
            )));
        }
        transaction.commit().map_err(DbError::from)?;
    }
    Ok(())
}

/// Install `bytes` as one payload from a caller that owns its thread.
///
/// The rows that name payloads are written on the database's own connection
/// thread, and payload storage has to exist before the row naming it commits, so
/// installation runs there too — the same blocking-IO position SQLite's own
/// writes occupy.
pub(crate) fn write_payload_blocking(
    conn: &Connection,
    store_dir: &StoreDir,
    bytes: &[u8],
) -> Result<ObjectHash, PayloadStoreError> {
    PayloadStore::new(conn, store_dir).install(bytes)
}

fn write_payload_file_bytes_blocking(
    store_dir: &StoreDir,
    hash: ObjectHash,
    bytes: &[u8],
) -> Result<(), PayloadStoreError> {
    let path = store_dir.payload_spool_path(hash);
    match std::fs::read(&path) {
        Ok(installed) if installed == bytes => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(PayloadStoreError::File {
                path,
                error: error.to_string(),
            });
        }
    }

    let directory = store_dir.payload_spool_dir();
    let mut staged =
        store_dir
            .create_payload_spool_stage()
            .map_err(|error| PayloadStoreError::File {
                path: directory.clone(),
                error: error.to_string(),
            })?;
    staged
        .write_all(bytes)
        .map_err(|error| PayloadStoreError::File {
            path: directory,
            error: error.to_string(),
        })?;
    staged
        .commit(&path)
        .map_err(|error| PayloadStoreError::File {
            path,
            error: error.to_string(),
        })?;
    Ok(())
}

/// Copy an existing file into the payload spool without reading it into one
/// contiguous buffer. Returns the content hash and byte length naming the
/// installed payload.
pub(crate) fn write_payload_file_blocking(
    conn: &Connection,
    store_dir: &StoreDir,
    source: &Path,
) -> Result<(ObjectHash, u64), PayloadStoreError> {
    let mut input = std::fs::File::open(source).map_err(|error| PayloadStoreError::File {
        path: source.to_path_buf(),
        error: error.to_string(),
    })?;
    let mut writer = PayloadStore::new(conn, store_dir).writer();
    std::io::copy(&mut input, &mut writer).map_err(|error| PayloadStoreError::File {
        path: source.to_path_buf(),
        error: error.to_string(),
    })?;
    writer.commit()
}

/// Read a payload on the database's connection thread.
pub(crate) fn read_payload_blocking(
    conn: &Connection,
    store_dir: &StoreDir,
    hash: ObjectHash,
) -> Result<Vec<u8>, PayloadStoreError> {
    PayloadStore::new(conn, store_dir).read(hash)
}

fn read_payload_file_blocking(
    store_dir: &StoreDir,
    hash: ObjectHash,
) -> Result<Vec<u8>, PayloadStoreError> {
    let path = store_dir.payload_spool_path(hash);
    std::fs::read(&path).map_err(|error| read_error(hash, path, error))
}

pub(super) fn read_verified_payload_blocking(
    conn: &Connection,
    store_dir: &StoreDir,
    hash: ObjectHash,
) -> Result<Vec<u8>, PayloadStoreError> {
    PayloadStore::new(conn, store_dir).read_verified(hash)
}

fn read_error(hash: ObjectHash, path: PathBuf, error: std::io::Error) -> PayloadStoreError {
    if error.kind() == std::io::ErrorKind::NotFound {
        return PayloadStoreError::Missing { hash, path };
    }
    PayloadStoreError::File {
        path,
        error: error.to_string(),
    }
}

/// Remove the payload stored under `hash`. An absent file is success: the
/// obligation this discharges says the payload must not be there, and a drain
/// that failed after the removal retries the whole deletion.
fn delete_payload_file_blocking(
    store_dir: &StoreDir,
    hash: ObjectHash,
) -> Result<(), PayloadStoreError> {
    let path = store_dir.payload_spool_path(hash);
    match std::fs::remove_file(&path) {
        Ok(()) => sync_parent(store_dir, &path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            debug!(payload = %hash, "payload spool file is already absent");
            Ok(())
        }
        Err(error) => Err(PayloadStoreError::File {
            path,
            error: error.to_string(),
        }),
    }
}

fn sync_parent(store_dir: &StoreDir, path: &Path) -> Result<(), PayloadStoreError> {
    store_dir
        .sync_parent_dir_blocking(path)
        .map_err(|error| PayloadStoreError::File {
            path: path.to_path_buf(),
            error,
        })
}

/// Claim `payloads` for `owner_key`, replacing whatever that owner claimed
/// before. Called in the transaction that writes the row holding the claim, so
/// the row and its claims commit together.
///
/// The whole set is replaced rather than one hash added or dropped, because the
/// flows that rewrite a journal in place — a Circle operation reaching its
/// finalization, a membership mutation advancing — carry one owner key across
/// both the payloads they drop and the payloads they take on, and a payload
/// named by both must not pass through a moment of being owed a deletion.
///
/// A payload leaving the set with no other claimant is owed a deletion,
/// recorded here. A payload entering it discharges any deletion it was owed:
/// the obligation says no row names the payload, and this claim is a row that
/// does.
pub(crate) fn set_payload_owner_claims_on(
    conn: &Connection,
    owner_key: &str,
    payloads: &BTreeSet<ObjectHash>,
) -> Result<(), DbError> {
    let held = crate::query_mapped_rows(
        conn,
        "SELECT payload_hash FROM payload_owners WHERE owner_key = ?1",
        [owner_key],
        |row| row.get::<_, String>(0),
    )
    .map_err(DbError::from)?
    .into_iter()
    .map(|hash| hash.parse::<ObjectHash>().map_err(DbError::from))
    .collect::<Result<BTreeSet<_>, _>>()?;

    for hash in held.difference(payloads) {
        conn.execute(
            "DELETE FROM payload_owners WHERE payload_hash = ?1 AND owner_key = ?2",
            rusqlite::params![hash.to_string(), owner_key],
        )
        .map_err(DbError::from)?;
        let claimed: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM payload_owners WHERE payload_hash = ?1)",
                [hash.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if !claimed {
            conn.execute(
                "INSERT OR IGNORE INTO payload_cleanup (payload_hash) VALUES (?1)",
                [hash.to_string()],
            )
            .map_err(DbError::from)?;
        }
    }
    for hash in payloads.difference(&held) {
        conn.execute(
            "INSERT INTO payload_owners (payload_hash, owner_key) VALUES (?1, ?2)",
            rusqlite::params![hash.to_string(), owner_key],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "DELETE FROM payload_cleanup WHERE payload_hash = ?1",
            [hash.to_string()],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

/// Drop every claim `owner_key` holds, owing a deletion for each payload it was
/// the last claimant of. Called in the transaction that drops the row.
pub(crate) fn release_payload_owner_on(conn: &Connection, owner_key: &str) -> Result<(), DbError> {
    set_payload_owner_claims_on(conn, owner_key, &BTreeSet::new())
}

pub(crate) fn payload_owner_claims_on(
    conn: &Connection,
    owner_key: &str,
) -> Result<BTreeSet<ObjectHash>, DbError> {
    crate::query_mapped_rows(
        conn,
        "SELECT payload_hash FROM payload_owners
         WHERE owner_key = ?1 ORDER BY payload_hash",
        [owner_key],
        |row| row.get::<_, String>(0),
    )?
    .into_iter()
    .map(|hash| hash.parse::<ObjectHash>().map_err(DbError::from))
    .collect()
}

/// The owner key naming the single retained replay baseline row's claim on the
/// two payloads it names: its database image and its canonical authority bytes.
pub(crate) const RETAINED_REPLAY_BASELINE_OWNER_KEY: &str = "retained-replay-baseline";

/// The singleton outbound Store snapshot row's plaintext and ciphertext image
/// payloads.
pub(crate) const OUTBOUND_STORE_SNAPSHOT_OWNER_KEY: &str = "outbound-store-snapshot";

/// One outbound Circle snapshot row's plaintext and ciphertext image payloads.
pub(crate) fn outbound_circle_snapshot_owner_key(
    circle_id: coven_protocol::circle::CircleId,
) -> String {
    format!("outbound-circle-snapshot:{circle_id}")
}

/// One queued Store write's captured SQLite changeset.
pub(crate) fn store_write_owner_key(write_id: &coven_protocol::write::WriteId) -> String {
    format!("store-write:{write_id}")
}

/// The owner key naming one Circle operation's claim on its prepared objects.
pub(crate) fn circle_operation_owner_key(operation_id: &str) -> String {
    format!("circle-operation:{operation_id}")
}

/// One retained Circle bootstrap coverage row's database image.
pub(crate) fn circle_bootstrap_coverage_owner_key(
    circle_id: coven_protocol::circle::CircleId,
) -> String {
    format!("circle-bootstrap-coverage:{circle_id}")
}

/// The owner key naming one remote object record's claim on its payloads.
pub(crate) fn remote_object_owner_key(object_id: ObjectHash) -> String {
    format!("remote-object:{object_id}")
}

pub(crate) fn payload_cleanup_hashes_on(conn: &Connection) -> Result<Vec<ObjectHash>, DbError> {
    crate::query_mapped_rows(
        conn,
        "SELECT payload_hash FROM payload_cleanup ORDER BY payload_hash",
        [],
        |row| row.get::<_, String>(0),
    )
    .map_err(DbError::from)?
    .into_iter()
    .map(|hash| hash.parse::<ObjectHash>().map_err(DbError::from))
    .collect()
}

#[cfg(any(test, feature = "test-utils"))]
#[path = "payload_store_test_support.rs"]
mod test_support;

#[cfg(test)]
#[path = "payload_store_tests.rs"]
mod tests;
