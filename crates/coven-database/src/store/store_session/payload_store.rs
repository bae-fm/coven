//! Content-addressed payloads owned by Store database rows.
//!
//! A payload is its `payload_storage` row and the `payload_chunks` rows that
//! row's `chunk_count` names — nothing else, nowhere else. Every payload is
//! compressed, and its compressed frame is written one bounded row at a time as
//! it is produced, so a payload larger than memory never needs to be in memory.
//!
//! Rows of different kinds can name the same payload — a Circle operation and
//! the remote object it prepared both need one object's bytes — so a row does
//! not delete the storage it is done with; it drops its claim with
//! [`set_payload_owner_claims_on`], and the transaction that drops the last
//! claim deletes the payload. Storage, claims and the row holding them are one
//! commit: a payload exists exactly while a durable owner claims it, and there
//! is no moment in between for anything to repair.

use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

#[cfg(any(test, feature = "test-utils"))]
use super::{StoreDatabase, StoreSession};
use crate::DbError;
use coven_protocol::store_commit::ObjectHash;

#[derive(Debug, thiserror::Error)]
pub enum PayloadStoreError {
    #[error("{operation} payload source {}: {source}", path.display())]
    FileIo {
        operation: &'static str,
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("payload {hash} database operation: {source}")]
    Database {
        hash: ObjectHash,
        #[source]
        source: rusqlite::Error,
    },
    #[error("payload {hash} size does not fit SQLite: {source}")]
    SizeConversion {
        hash: ObjectHash,
        #[source]
        source: std::num::TryFromIntError,
    },
    #[error("payload {hash} has invalid storage metadata: {error}")]
    Storage { hash: ObjectHash, error: String },
    #[error("payload {expected} contains bytes hashing to {actual}")]
    ContentMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error(
        "payload {hash} was written with {actual} bytes, but its catalog row records {stored}"
    )]
    SizeMismatch {
        hash: ObjectHash,
        actual: u64,
        stored: u64,
    },
    #[error("payload {hash} compression I/O failed: {source}")]
    CompressionIo {
        hash: ObjectHash,
        #[source]
        source: std::io::Error,
    },
    #[error("payload {hash} compression framing failed: {source}")]
    CompressionFrame {
        hash: ObjectHash,
        #[source]
        source: lz4_flex::frame::Error,
    },
}

/// One `payload_chunks` row's byte length. Chunks are bounded so a payload
/// larger than memory is still one bounded SQL parameter at a time, and this
/// matches the buffer the decoder reads back through.
const PAYLOAD_CHUNK_BYTES: usize = 64 * 1024;

/// The compressed frame's block size, pinned rather than chosen from the first
/// write's length. The encoder holds one block while it fills, so pinning it is
/// what keeps a streamed payload's memory bounded by a block and a chunk instead
/// of by the size of the first slice its caller hands over.
fn payload_frame_info() -> lz4_flex::frame::FrameInfo {
    lz4_flex::frame::FrameInfo::new().block_size(lz4_flex::frame::BlockSize::Max64KB)
}

/// What one payload's catalog row states its chunks must add up to.
struct StoredPayload {
    payload_size: u64,
    compressed_size: u64,
    chunk_count: u64,
}

/// One database connection's closed access to the payload bytes its rows own.
#[derive(Clone, Copy)]
pub(crate) struct PayloadStore<'store> {
    conn: &'store Connection,
}

impl<'store> PayloadStore<'store> {
    pub(crate) fn new(conn: &'store Connection) -> Self {
        Self { conn }
    }

    pub(crate) fn install(self, bytes: &[u8]) -> Result<ObjectHash, PayloadStoreError> {
        let hash = ObjectHash::digest(bytes);
        let mut writer = self.writer(hash)?;
        writer
            .write_all(bytes)
            .map_err(|source| PayloadStoreError::CompressionIo { hash, source })?;
        writer.commit()?;
        Ok(hash)
    }

    pub(super) fn copy_verified(
        self,
        hash: ObjectHash,
        output: &mut impl std::io::Write,
    ) -> Result<u64, PayloadStoreError> {
        let stored = self.require_stored(hash)?;
        let size = stored.payload_size;
        let mut output = reading::HashedPayloadOutput::new(output);
        self.copy_stored(hash, stored, &mut output)?;
        self.verify_hash(hash, output.finish())?;
        Ok(size)
    }

    /// Stream `source` into the payload `expected` names, verifying as it
    /// reads, and report the payload's length.
    ///
    /// The caller already holds the payload's identity — a blob fact's
    /// plaintext hash, a snapshot's signed image hash — so the chunks can be
    /// written under it while the digest is still being computed. That is not
    /// permission to skip the check: a source whose content differs from what
    /// the caller named fails here, and the enclosing transaction is the only
    /// place the chunks exist.
    pub(super) fn write_file(
        self,
        expected: ObjectHash,
        source: &Path,
    ) -> Result<u64, PayloadStoreError> {
        let mut input = std::fs::File::open(source).map_err(|error| PayloadStoreError::FileIo {
            operation: "open",
            path: source.to_path_buf(),
            source: error,
        })?;
        let mut writer = self.writer(expected)?;
        std::io::copy(&mut input, &mut writer).map_err(|error| PayloadStoreError::FileIo {
            operation: "copy",
            path: source.to_path_buf(),
            source: error,
        })?;
        writer.commit()
    }

    /// Open a writer for the payload `expected` names.
    ///
    /// A payload already in the catalog is still read and verified — its bytes
    /// are hashed and dropped rather than rewritten, so re-installing neither
    /// replaces committed chunks with a differently framed compression of the
    /// same content nor stops checking what it was handed.
    fn writer(self, expected: ObjectHash) -> Result<PayloadWriter<'store>, PayloadStoreError> {
        self.require_transaction(expected)?;
        let target = match self.stored(expected)? {
            Some(stored) => {
                self.verify_chunk_set(expected, &stored)?;
                PayloadWriterTarget::Installed {
                    payload_size: stored.payload_size,
                }
            }
            None => PayloadWriterTarget::Chunks(lz4_flex::frame::FrameEncoder::with_frame_info(
                payload_frame_info(),
                PayloadChunkSink::new(self.conn, expected),
            )),
        };
        Ok(PayloadWriter {
            payloads: self,
            hash: expected,
            target,
            hasher: coven_protocol::blob::ContentHasher::new(),
            size: 0,
        })
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
                "SELECT payload_size, compressed_size, chunk_count
                 FROM payload_storage WHERE payload_hash = ?1",
                [hash.to_string()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| PayloadStoreError::Database { hash, source })?;
        let Some((payload_size, compressed_size, chunk_count)) = row else {
            return Ok(None);
        };
        if payload_size < 0 || compressed_size <= 0 || chunk_count <= 0 {
            return Err(PayloadStoreError::Storage {
                hash,
                error: format!(
                    "payload size {payload_size}, compressed size {compressed_size}, \
                     chunk count {chunk_count}"
                ),
            });
        }
        Ok(Some(StoredPayload {
            payload_size: payload_size as u64,
            compressed_size: compressed_size as u64,
            chunk_count: chunk_count as u64,
        }))
    }

    /// Check the chunk rows against what the catalog row says they are, without
    /// decompressing them: a lost, extra or resized chunk is a disagreement
    /// between two rows of the same commit, and reading past it would decode
    /// bytes no writer produced.
    fn verify_chunk_set(
        self,
        hash: ObjectHash,
        stored: &StoredPayload,
    ) -> Result<(), PayloadStoreError> {
        let (count, total): (i64, i64) = self
            .conn
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(length(bytes)), 0)
                 FROM payload_chunks WHERE payload_hash = ?1",
                [hash.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|source| PayloadStoreError::Database { hash, source })?;
        if count as u64 != stored.chunk_count || total as u64 != stored.compressed_size {
            return Err(PayloadStoreError::Storage {
                hash,
                error: format!(
                    "catalog records {} chunks totalling {} bytes, but storage holds \
                     {count} chunks totalling {total}",
                    stored.chunk_count, stored.compressed_size
                ),
            });
        }
        Ok(())
    }

    fn record(
        self,
        hash: ObjectHash,
        payload_size: u64,
        compressed_size: u64,
        chunk_count: u64,
    ) -> Result<(), PayloadStoreError> {
        let size = |value: u64| {
            i64::try_from(value)
                .map_err(|source| PayloadStoreError::SizeConversion { hash, source })
        };
        self.conn
            .execute(
                "INSERT INTO payload_storage
                 (payload_hash, payload_size, compressed_size, chunk_count)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    hash.to_string(),
                    size(payload_size)?,
                    size(compressed_size)?,
                    size(chunk_count)?
                ],
            )
            .map(drop)
            .map_err(|source| PayloadStoreError::Database { hash, source })
    }
}

/// Where a compressed frame's bytes go while it is being produced: bounded rows
/// under the payload's own address, written by the transaction that will commit
/// the row claiming them.
struct PayloadChunkSink<'store> {
    conn: &'store Connection,
    hash: ObjectHash,
    buffer: Vec<u8>,
    ordinal: u64,
    size: u64,
}

impl<'store> PayloadChunkSink<'store> {
    fn new(conn: &'store Connection, hash: ObjectHash) -> Self {
        Self {
            conn,
            hash,
            buffer: Vec::with_capacity(PAYLOAD_CHUNK_BYTES),
            ordinal: 0,
            size: 0,
        }
    }

    fn flush_chunk(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let ordinal = i64::try_from(self.ordinal)
            .map_err(|_| std::io::Error::other("payload chunk ordinal overflow"))?;
        self.conn
            .execute(
                "INSERT INTO payload_chunks (payload_hash, ordinal, bytes)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![self.hash.to_string(), ordinal, self.buffer],
            )
            .map_err(std::io::Error::other)?;
        self.size = self
            .size
            .checked_add(self.buffer.len() as u64)
            .ok_or_else(|| std::io::Error::other("compressed payload size overflow"))?;
        self.ordinal += 1;
        self.buffer.clear();
        Ok(())
    }

    /// Write the frame's trailing bytes and report what the catalog row must
    /// state about the chunks now in the database.
    fn finish(mut self) -> std::io::Result<(u64, u64)> {
        self.flush_chunk()?;
        Ok((self.size, self.ordinal))
    }
}

impl std::io::Write for PayloadChunkSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut written = 0;
        while written < bytes.len() {
            let room = PAYLOAD_CHUNK_BYTES - self.buffer.len();
            let taken = room.min(bytes.len() - written);
            self.buffer
                .extend_from_slice(&bytes[written..written + taken]);
            written += taken;
            if self.buffer.len() == PAYLOAD_CHUNK_BYTES {
                self.flush_chunk()?;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

enum PayloadWriterTarget<'store> {
    /// The payload is not in the catalog: compress into chunk rows.
    Chunks(lz4_flex::frame::FrameEncoder<PayloadChunkSink<'store>>),
    /// The payload is already in the catalog: verify what we are handed against
    /// what the catalog records and leave the committed chunks alone.
    Installed { payload_size: u64 },
}

/// One payload being written under the identity its owner already holds, while
/// the digest of what it is actually given is computed alongside.
pub(crate) struct PayloadWriter<'store> {
    payloads: PayloadStore<'store>,
    hash: ObjectHash,
    target: PayloadWriterTarget<'store>,
    hasher: coven_protocol::blob::ContentHasher,
    size: u64,
}

impl PayloadWriter<'_> {
    /// Settle the payload and report its length: what was written must hash to
    /// the identity the caller opened this writer with.
    ///
    /// A writer that never reaches here leaves nothing: its chunk rows live in
    /// the caller's transaction, which fails with it.
    pub(crate) fn commit(self) -> Result<u64, PayloadStoreError> {
        let Self {
            payloads,
            hash,
            target,
            hasher,
            size,
        } = self;
        let actual = hasher
            .finish()
            .parse::<ObjectHash>()
            .expect("SHA-256 hex is an ObjectHash");
        if actual != hash {
            return Err(PayloadStoreError::ContentMismatch {
                expected: hash,
                actual,
            });
        }
        match target {
            PayloadWriterTarget::Installed { payload_size } => {
                if payload_size != size {
                    return Err(PayloadStoreError::SizeMismatch {
                        hash,
                        actual: size,
                        stored: payload_size,
                    });
                }
                Ok(size)
            }
            PayloadWriterTarget::Chunks(encoder) => {
                let sink = encoder
                    .finish()
                    .map_err(|source| PayloadStoreError::CompressionFrame { hash, source })?;
                let (compressed_size, chunk_count) = sink
                    .finish()
                    .map_err(|source| PayloadStoreError::CompressionIo { hash, source })?;
                payloads.record(hash, size, compressed_size, chunk_count)?;
                Ok(size)
            }
        }
    }
}

impl std::io::Write for PayloadWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = match &mut self.target {
            PayloadWriterTarget::Chunks(encoder) => encoder.write(bytes)?,
            PayloadWriterTarget::Installed { .. } => bytes.len(),
        };
        self.hasher.update(&bytes[..written]);
        self.size = self
            .size
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("payload size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.target {
            PayloadWriterTarget::Chunks(encoder) => encoder.flush(),
            PayloadWriterTarget::Installed { .. } => Ok(()),
        }
    }
}

/// Install `bytes` as one payload from a caller that owns its thread.
///
/// The rows that name payloads are written on the database's own connection
/// thread, and payload storage has to exist before the row naming it commits, so
/// installation runs there too — the same blocking-IO position SQLite's own
/// writes occupy.
pub(crate) fn write_payload_blocking(
    conn: &Connection,
    bytes: &[u8],
) -> Result<ObjectHash, PayloadStoreError> {
    PayloadStore::new(conn).install(bytes)
}

/// Copy an existing file into the payload the caller's identity names, without
/// reading it into one contiguous buffer.
pub(crate) fn write_payload_file_blocking(
    conn: &Connection,
    expected: ObjectHash,
    source: &Path,
) -> Result<u64, PayloadStoreError> {
    PayloadStore::new(conn).write_file(expected, source)
}

/// Read a payload on the database's connection thread.
pub(crate) fn read_payload_blocking(
    conn: &Connection,
    hash: ObjectHash,
) -> Result<Vec<u8>, PayloadStoreError> {
    PayloadStore::new(conn).read(hash)
}

pub(super) fn read_verified_payload_blocking(
    conn: &Connection,
    hash: ObjectHash,
) -> Result<Vec<u8>, PayloadStoreError> {
    PayloadStore::new(conn).read_verified(hash)
}

/// Claim `payloads` for `owner_key`, replacing whatever that owner claimed
/// before. Called in the transaction that writes the row holding the claim, so
/// the row, its claims and the storage they name commit together.
///
/// The whole set is replaced rather than one hash added or dropped, because the
/// flows that rewrite a journal in place — a Circle operation reaching its
/// finalization, a membership mutation advancing — carry one owner key across
/// both the payloads they drop and the payloads they take on, and a payload
/// named by both must not pass through a moment of being deleted.
///
/// A payload leaving the set with no other claimant is deleted here, chunks and
/// all, by this same transaction. There is no obligation to discharge later and
/// no window in which a row names storage that is gone. Claims are taken before
/// they are dropped so one owner's set replacement never deletes a payload it
/// is about to hold again; a transaction moving a payload between two owners
/// has to claim it for the new one before releasing the old, and fails loudly
/// on the storage reference if it does not.
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

    for hash in payloads.difference(&held) {
        conn.execute(
            "INSERT INTO payload_owners (payload_hash, owner_key) VALUES (?1, ?2)",
            rusqlite::params![hash.to_string(), owner_key],
        )
        .map_err(DbError::from)?;
    }
    for hash in held.difference(payloads) {
        conn.execute(
            "DELETE FROM payload_owners WHERE payload_hash = ?1 AND owner_key = ?2",
            rusqlite::params![hash.to_string(), owner_key],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "DELETE FROM payload_storage
             WHERE payload_hash = ?1
               AND NOT EXISTS (
                   SELECT 1 FROM payload_owners WHERE payload_hash = ?1
               )",
            [hash.to_string()],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

/// Drop every claim `owner_key` holds, deleting each payload it was the last
/// claimant of. Called in the transaction that drops the row.
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

/// One queued Store write's captured edits, partitions and immutable blob sources.
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

/// Every table a payload's bytes, metadata or claims live in, ordered so that
/// deleting them front to back never leaves a dangling reference.
///
/// A serialized database image that travels — a published snapshot, a private
/// replay baseline — carries the rows that name payloads but never the payloads
/// themselves: the device reading it consults its own catalog, and an image that
/// embedded its own enclosing payload history would nest every predecessor
/// image inside its successor.
pub(crate) const PAYLOAD_TABLES: &[&str] = &["payload_owners", "payload_chunks", "payload_storage"];

/// Remove every payload row from a database image being prepared for travel.
pub(crate) fn clear_payload_tables_on(conn: &Connection) -> Result<(), DbError> {
    for table in PAYLOAD_TABLES {
        conn.execute_batch(&format!("DELETE FROM {}", crate::quote_ident(table)))
            .map_err(DbError::from)?;
    }
    Ok(())
}

/// The payload rows an image carries, by table. Empty is the only shape a
/// travelling image may have.
pub(crate) fn payload_rows_in_image(
    conn: &Connection,
) -> Result<Vec<(&'static str, i64)>, DbError> {
    let mut counts = Vec::new();
    for table in PAYLOAD_TABLES {
        let count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {}", crate::quote_ident(table)),
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if count != 0 {
            counts.push((*table, count));
        }
    }
    Ok(counts)
}

#[cfg(any(test, feature = "test-utils"))]
#[path = "payload_store_test_support.rs"]
mod test_support;

#[cfg(test)]
#[path = "payload_store_tests.rs"]
mod tests;

#[path = "payload_store_reading.rs"]
mod reading;
