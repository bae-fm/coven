//! Content-addressed payloads owned by Store database rows.
//!
//! Protocol-sized payloads live in SQLite. Larger and streamed payloads live in
//! files beside it. The catalog on the same connection is the sole authority
//! for which representation a hash uses.
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
use std::io::Write as _;
use std::path::{Path, PathBuf};

use coven_foundation::atomic_file::AtomicFileStage;
use coven_foundation::store_dir::StoreDir;
use rusqlite::{Connection, OptionalExtension};
use tracing::debug;

use super::{StoreDatabase, StoreTransaction};
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
}

const INLINE_PAYLOAD_LIMIT: usize = 64 * 1024;

enum StoredPayload {
    Inline(Vec<u8>),
    File { size: u64 },
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
        match self.stored(hash)? {
            Some(StoredPayload::Inline(installed)) => {
                if installed == bytes {
                    Ok(hash)
                } else {
                    Err(PayloadStoreError::InlineContentMismatch {
                        expected: hash,
                        actual: ObjectHash::digest(&installed),
                    })
                }
            }
            Some(StoredPayload::File { .. }) => {
                write_payload_file_bytes_blocking(self.store_dir, hash, bytes)?;
                Ok(hash)
            }
            None if bytes.len() <= INLINE_PAYLOAD_LIMIT => {
                self.conn
                    .execute(
                        "INSERT INTO payload_storage
                         (payload_hash, storage, inline_bytes, file_size)
                         VALUES (?1, 'inline', ?2, NULL)",
                        rusqlite::params![hash.to_string(), bytes],
                    )
                    .map_err(|error| PayloadStoreError::Storage {
                        hash,
                        error: error.to_string(),
                    })?;
                Ok(hash)
            }
            None => {
                write_payload_file_bytes_blocking(self.store_dir, hash, bytes)?;
                self.record_file(hash, bytes.len() as u64)?;
                Ok(hash)
            }
        }
    }

    pub(crate) fn read(self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        match self.stored(hash)? {
            Some(StoredPayload::Inline(bytes)) => Ok(bytes),
            Some(StoredPayload::File { size }) => {
                let bytes = read_payload_file_blocking(self.store_dir, hash)?;
                if bytes.len() as u64 != size {
                    return Err(PayloadStoreError::Storage {
                        hash,
                        error: format!(
                            "catalog records {size} file bytes, but the spool contains {}",
                            bytes.len()
                        ),
                    });
                }
                Ok(bytes)
            }
            None => Err(PayloadStoreError::Storage {
                hash,
                error: "no catalog row".to_string(),
            }),
        }
    }

    pub(crate) fn read_verified(self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        let bytes = self.read(hash)?;
        let actual = ObjectHash::digest(&bytes);
        if actual == hash {
            return Ok(bytes);
        }
        match self.stored(hash)? {
            Some(StoredPayload::Inline(_)) => Err(PayloadStoreError::InlineContentMismatch {
                expected: hash,
                actual,
            }),
            Some(StoredPayload::File { .. }) | None => Err(PayloadStoreError::ContentMismatch {
                expected: hash,
                actual,
                path: self.store_dir.payload_spool_path(hash),
            }),
        }
    }

    fn writer(self) -> PayloadWriter<'store> {
        PayloadWriter {
            payloads: self,
            target: PayloadWriterTarget::Inline(Vec::new()),
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
                "SELECT storage, inline_bytes, file_size
                 FROM payload_storage WHERE payload_hash = ?1",
                [hash.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
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
            Some((storage, Some(bytes), None)) if storage == "inline" => {
                Ok(Some(StoredPayload::Inline(bytes)))
            }
            Some((storage, None, Some(size))) if storage == "file" && size >= 0 => {
                Ok(Some(StoredPayload::File { size: size as u64 }))
            }
            Some((storage, inline, size)) => Err(PayloadStoreError::Storage {
                hash,
                error: format!(
                    "tag {storage:?}, inline bytes {}, file size {size:?}",
                    inline
                        .as_ref()
                        .map_or("absent".to_string(), |bytes| format!(
                            "{} bytes",
                            bytes.len()
                        ))
                ),
            }),
        }
    }

    fn record_file(self, hash: ObjectHash, size: u64) -> Result<(), PayloadStoreError> {
        let size = i64::try_from(size).map_err(|error| PayloadStoreError::Storage {
            hash,
            error: error.to_string(),
        })?;
        match self.stored(hash)? {
            None => self
                .conn
                .execute(
                    "INSERT INTO payload_storage
                     (payload_hash, storage, inline_bytes, file_size)
                     VALUES (?1, 'file', NULL, ?2)",
                    rusqlite::params![hash.to_string(), size],
                )
                .map(|_| ())
                .map_err(|error| PayloadStoreError::Storage {
                    hash,
                    error: error.to_string(),
                }),
            Some(StoredPayload::File { size: stored }) if stored == size as u64 => Ok(()),
            Some(StoredPayload::File { size: stored }) => Err(PayloadStoreError::Storage {
                hash,
                error: format!("catalog file size {stored} differs from installed size {size}"),
            }),
            Some(StoredPayload::Inline(_)) => Err(PayloadStoreError::Storage {
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
    target: PayloadWriterTarget,
    hasher: coven_protocol::blob::ContentHasher,
    size: u64,
}

enum PayloadWriterTarget {
    Inline(Vec<u8>),
    File(AtomicFileStage),
}

impl<'store> PayloadWriter<'store> {
    pub(crate) fn commit(self) -> Result<(ObjectHash, u64), PayloadStoreError> {
        let hash = self
            .hasher
            .finish()
            .parse::<ObjectHash>()
            .expect("SHA-256 hex is an ObjectHash");
        self.payloads.require_transaction(hash)?;
        match self.target {
            PayloadWriterTarget::Inline(bytes) => {
                let installed = self.payloads.install(&bytes)?;
                if installed != hash {
                    return Err(PayloadStoreError::Storage {
                        hash,
                        error: format!("streamed bytes installed as {installed}"),
                    });
                }
            }
            PayloadWriterTarget::File(staged) => {
                let path = self.payloads.store_dir.payload_spool_path(hash);
                staged
                    .commit(&path)
                    .map_err(|error| PayloadStoreError::File {
                        path,
                        error: error.to_string(),
                    })?;
                self.payloads.record_file(hash, self.size)?;
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
        let written = match &mut self.target {
            PayloadWriterTarget::Inline(buffer)
                if buffer.len().saturating_add(bytes.len()) <= INLINE_PAYLOAD_LIMIT =>
            {
                buffer.extend_from_slice(bytes);
                bytes.len()
            }
            PayloadWriterTarget::Inline(buffer) => {
                let directory = self.payloads.store_dir.payload_spool_dir();
                let mut staged = self
                    .payloads
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
        self.hasher.update(&bytes[..written]);
        self.size = self
            .size
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("payload size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.target {
            PayloadWriterTarget::Inline(_) => Ok(()),
            PayloadWriterTarget::File(staged) => staged.flush(),
        }
    }
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
            Some(StoredPayload::Inline(_)) => {}
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

impl StoreDatabase {
    /// The payloads still owed a deletion. Empty once every obligation this
    /// store committed has been discharged.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn owed_payload_cleanup(&self) -> Result<Vec<ObjectHash>, DbError> {
        self.call_store(|session| session.owed_payload_cleanup())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn store_write_payload_claims_for_test(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<Vec<ObjectHash>, DbError> {
        let owner_key = store_write_owner_key(write_id);
        self.call_store(move |session| session.payload_owner_claims(&owner_key))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn circle_operation_payload_claims_for_test(
        &self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<Vec<ObjectHash>, DbError> {
        let owner_key = circle_operation_owner_key(operation_id.as_str());
        self.call_store(move |session| session.payload_owner_claims(&owner_key))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn retained_replay_payload_claims_for_test(
        &self,
    ) -> Result<Vec<ObjectHash>, DbError> {
        self.call_store(|session| session.payload_owner_claims(RETAINED_REPLAY_BASELINE_OWNER_KEY))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn outbound_store_snapshot_payload_claims_for_test(
        &self,
    ) -> Result<Vec<ObjectHash>, DbError> {
        self.call_store(|session| session.payload_owner_claims(OUTBOUND_STORE_SNAPSHOT_OWNER_KEY))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn install_payload_for_test(&self, bytes: Vec<u8>) -> Result<ObjectHash, DbError> {
        self.call_store(move |session| session.install_payload_for_test(&bytes))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn payload_for_test(&self, hash: ObjectHash) -> Result<Vec<u8>, DbError> {
        self.call_store(move |session| session.payload_for_test(hash))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn has_payload_for_test(&self, hash: ObjectHash) -> Result<bool, DbError> {
        self.call_store(move |session| session.has_payload_for_test(hash))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn corrupt_payload_for_test(
        &self,
        hash: ObjectHash,
        bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.corrupt_payload_for_test(hash, &bytes))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn remove_payload_bytes_for_test(&self, hash: ObjectHash) -> Result<(), DbError> {
        self.call_store(move |session| session.remove_payload_bytes_for_test(hash))
            .await
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl super::StoreSession<'_> {
    fn owed_payload_cleanup(&self) -> Result<Vec<ObjectHash>, DbError> {
        payload_cleanup_hashes_on(self.conn)
    }

    fn payload_owner_claims(&self, owner_key: &str) -> Result<Vec<ObjectHash>, DbError> {
        Ok(payload_owner_claims_on(self.conn, owner_key)?
            .into_iter()
            .collect())
    }

    fn install_payload_for_test(&self, bytes: &[u8]) -> Result<ObjectHash, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let hash = PayloadStore::new(&transaction, self.store_dir)
            .install(bytes)
            .map_err(DbError::from)?;
        transaction.commit().map_err(DbError::from)?;
        Ok(hash)
    }

    fn payload_for_test(&self, hash: ObjectHash) -> Result<Vec<u8>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .payload(hash)
            .map_err(DbError::from)
    }

    fn has_payload_for_test(&self, hash: ObjectHash) -> Result<bool, DbError> {
        Ok(PayloadStore::new(self.conn, self.store_dir)
            .stored(hash)
            .map_err(DbError::from)?
            .is_some())
    }

    fn corrupt_payload_for_test(&self, hash: ObjectHash, bytes: &[u8]) -> Result<(), DbError> {
        match PayloadStore::new(self.conn, self.store_dir)
            .stored(hash)
            .map_err(DbError::from)?
        {
            Some(StoredPayload::Inline(_)) => {
                self.conn
                    .execute(
                        "UPDATE payload_storage SET inline_bytes = ?2 WHERE payload_hash = ?1",
                        rusqlite::params![hash.to_string(), bytes],
                    )
                    .map_err(DbError::from)?;
            }
            Some(StoredPayload::File { .. }) => {
                std::fs::write(self.store_dir.payload_spool_path(hash), bytes)
                    .map_err(|error| DbError::context("corrupt test payload file", error))?;
            }
            None => {
                return Err(DbError::Message(format!(
                    "cannot corrupt absent test payload {hash}"
                )));
            }
        }
        Ok(())
    }

    fn remove_payload_bytes_for_test(&self, hash: ObjectHash) -> Result<(), DbError> {
        match PayloadStore::new(self.conn, self.store_dir)
            .stored(hash)
            .map_err(DbError::from)?
        {
            Some(StoredPayload::Inline(bytes)) => {
                self.conn
                    .execute(
                        "UPDATE payload_storage
                         SET storage = 'file', inline_bytes = NULL, file_size = ?2
                         WHERE payload_hash = ?1",
                        rusqlite::params![hash.to_string(), bytes.len() as i64],
                    )
                    .map_err(DbError::from)?;
                match std::fs::remove_file(self.store_dir.payload_spool_path(hash)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(DbError::context("remove test payload file", error));
                    }
                }
            }
            Some(StoredPayload::File { .. }) => {
                std::fs::remove_file(self.store_dir.payload_spool_path(hash))
                    .map_err(|error| DbError::context("remove test payload file", error))?;
            }
            None => {
                return Err(DbError::Message(format!(
                    "cannot remove absent test payload {hash}"
                )));
            }
        }
        Ok(())
    }
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

#[cfg(test)]
#[path = "payload_store_tests.rs"]
mod tests;
