//! Payload files: bytes a row owns, stored beside the database instead of
//! inside it.
//!
//! A payload is written before the row that references it commits, and deleted
//! by the flow that deletes that row. The file is named for the digest of the
//! bytes it holds, so a retry of a failed insert rewrites the same path with
//! the same contents and a file whose insert never committed is inert garbage
//! bounded by that one operation's content.
//!
//! Deletion rides row deletion, counted by owner. Rows of different kinds can
//! name the same payload — a Circle operation and the remote object it prepared
//! both need one object's bytes — so a row does not delete the file it is done
//! with; it drops its claim with [`set_payload_owner_claims_on`], and the
//! transaction that drops the last claim records the deletion obligation. Once
//! that transaction commits, [`PayloadSpool::drain_cleanup`] removes each file
//! and clears its obligation. A failure between the two leaves the obligation
//! durable, so the next drain finishes the deletion rather than losing it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use coven_foundation::local_file::AtomicStagedFile;
use coven_foundation::store_dir::StoreDir;
use rusqlite::Connection;
use tracing::debug;

use super::StoreDatabase;
use crate::DbError;
use coven_protocol::store_commit::ObjectHash;

#[derive(Debug, thiserror::Error)]
pub enum PayloadSpoolError {
    /// A payload a caller holds the hash of is not in the spool. The row naming
    /// it and the file it names have gone out of step, which no flow may
    /// recover from by reading something else.
    #[error("payload {hash} is absent from the spool at {}", path.display())]
    Missing { hash: ObjectHash, path: PathBuf },
    #[error("payload spool {}: {error}", path.display())]
    File { path: PathBuf, error: String },
    #[error("{0}")]
    Database(#[from] DbError),
}

/// One store's payload files, under `spool/payloads` in its store directory.
pub struct PayloadSpool<'store> {
    store_dir: &'store StoreDir,
}

impl<'store> PayloadSpool<'store> {
    pub fn new(store_dir: &'store StoreDir) -> Self {
        Self { store_dir }
    }

    /// Install `bytes` as one payload and return the hash naming the file they
    /// were installed as. The bytes go to a temporary sibling that is flushed
    /// to disk and then renamed onto the hash-named path, so the only thing a
    /// reader can ever find under that name is the whole payload.
    pub async fn write(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        let hash = ObjectHash::digest(bytes);
        let path = self.store_dir.payload_spool_path(hash);
        let mut staged =
            AtomicStagedFile::create(&path)
                .await
                .map_err(|error| PayloadSpoolError::File {
                    path: path.clone(),
                    error,
                })?;
        staged
            .write_bytes(bytes)
            .await
            .map_err(|error| PayloadSpoolError::File {
                path: path.clone(),
                error,
            })?;
        staged
            .commit()
            .await
            .map_err(|error| PayloadSpoolError::File {
                path: path.clone(),
                error,
            })?;
        Ok(hash)
    }

    /// The payload stored under `hash`. The spool hands back exactly the bytes
    /// it holds; a caller that needs the file to still match the hash its row
    /// carries checks that itself, under whatever rule its own flow applies.
    pub async fn read(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        let path = self.store_dir.payload_spool_path(hash);
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(bytes),
            Err(error) => Err(read_error(hash, path, error)),
        }
    }

    /// Delete the payload behind every committed cleanup obligation, clearing
    /// each obligation once its file is gone. A filesystem or database failure
    /// leaves the obligation durable and fails the drain.
    ///
    /// The deletions run on the database's connection thread, alongside the
    /// transactions that register claims, so a payload cannot be re-claimed and
    /// rewritten between this drain reading its obligation and removing its
    /// file.
    pub async fn drain_cleanup(&self, database: &StoreDatabase) -> Result<(), PayloadSpoolError> {
        let store_dir = self.store_dir.clone();
        database
            .connection
            .call(move |conn| {
                for hash in payload_spool_cleanup_hashes_on(conn)? {
                    delete_payload_blocking(&store_dir, hash)
                        .map_err(|error| DbError::Message(error.to_string()))?;
                    conn.execute(
                        "DELETE FROM payload_spool_cleanup WHERE payload_hash = ?1",
                        [hash.to_string()],
                    )
                    .map_err(DbError::from)?;
                }
                Ok(())
            })
            .await
            .map_err(PayloadSpoolError::Database)
    }
}

/// A connection and the payload files the rows on it name.
///
/// A record whose bytes live in the spool is half a row and half a file, so
/// everything that handles whole records carries both halves as one value. A
/// function that only touches rows keeps taking the connection alone, and its
/// signature says so.
#[derive(Clone, Copy)]
pub struct StoreRecords<'store> {
    conn: &'store Connection,
    store_dir: &'store StoreDir,
}

impl<'store> StoreRecords<'store> {
    pub fn new(conn: &'store Connection, store_dir: &'store StoreDir) -> Self {
        Self { conn, store_dir }
    }

    pub fn conn(&self) -> &'store Connection {
        self.conn
    }

    pub fn store_dir(&self) -> &'store StoreDir {
        self.store_dir
    }

    /// The payload stored under `hash`, read on this thread.
    pub fn payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        read_payload_blocking(self.store_dir, hash)
    }

    /// Install `bytes` as a payload and return the hash naming the file.
    pub fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        write_payload_blocking(self.store_dir, bytes)
    }
}

/// The same pair inside one write transaction.
///
/// A row and the payload file it names commit together, so a flow that installs
/// payloads takes this rather than a bare connection: on a connection in
/// autocommit each statement would land on its own, and a failure between them
/// would leave a row naming a file that never arrived — or the reverse.
#[derive(Clone, Copy)]
pub struct StoreRecordTransaction<'store, 'connection> {
    transaction: &'store rusqlite::Transaction<'connection>,
    records: StoreRecords<'store>,
}

impl<'store, 'connection> StoreRecordTransaction<'store, 'connection> {
    pub fn new(
        transaction: &'store rusqlite::Transaction<'connection>,
        store_dir: &'store StoreDir,
    ) -> Self {
        Self {
            transaction,
            records: StoreRecords::new(transaction, store_dir),
        }
    }

    /// The transaction itself, for the statements that need it named.
    pub fn transaction(&self) -> &'store rusqlite::Transaction<'connection> {
        self.transaction
    }
}

impl<'store> std::ops::Deref for StoreRecordTransaction<'store, '_> {
    type Target = StoreRecords<'store>;

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

/// Install `bytes` as one payload from a caller that owns its thread, and
/// return the hash naming the file they were installed as.
///
/// The rows that name payloads are written on the database's own connection
/// thread, and a payload has to be on disk before the row naming it commits, so
/// the write that installs it runs there too — the same blocking-IO position
/// SQLite's own writes occupy. The async [`PayloadSpool::write`] is for callers
/// that reach the spool from a task instead.
pub fn write_payload_blocking(
    store_dir: &StoreDir,
    bytes: &[u8],
) -> Result<ObjectHash, PayloadSpoolError> {
    let hash = ObjectHash::digest(bytes);
    let path = store_dir.payload_spool_path(hash);
    coven_foundation::atomic_file::AtomicFile::new(path.clone())
        .replace(bytes)
        .map_err(|error| PayloadSpoolError::File { path, error })?;
    Ok(hash)
}

/// [`PayloadSpool::read`] for callers on the database's connection thread.
pub fn read_payload_blocking(
    store_dir: &StoreDir,
    hash: ObjectHash,
) -> Result<Vec<u8>, PayloadSpoolError> {
    let path = store_dir.payload_spool_path(hash);
    std::fs::read(&path).map_err(|error| read_error(hash, path, error))
}

fn read_error(hash: ObjectHash, path: PathBuf, error: std::io::Error) -> PayloadSpoolError {
    if error.kind() == std::io::ErrorKind::NotFound {
        return PayloadSpoolError::Missing { hash, path };
    }
    PayloadSpoolError::File {
        path,
        error: error.to_string(),
    }
}

/// Remove the payload stored under `hash`. An absent file is success: the
/// obligation this discharges says the payload must not be there, and a drain
/// that failed after the removal retries the whole deletion.
fn delete_payload_blocking(
    store_dir: &StoreDir,
    hash: ObjectHash,
) -> Result<(), PayloadSpoolError> {
    let path = store_dir.payload_spool_path(hash);
    match std::fs::remove_file(&path) {
        Ok(()) => sync_parent(&path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            debug!(payload = %hash, "payload spool file is already absent");
            Ok(())
        }
        Err(error) => Err(PayloadSpoolError::File {
            path,
            error: error.to_string(),
        }),
    }
}

fn sync_parent(path: &Path) -> Result<(), PayloadSpoolError> {
    coven_foundation::atomic_file::sync_parent_dir_blocking(path).map_err(|error| {
        PayloadSpoolError::File {
            path: path.to_path_buf(),
            error,
        }
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
pub fn set_payload_owner_claims_on(
    conn: &Connection,
    owner_key: &str,
    payloads: &BTreeSet<ObjectHash>,
) -> Result<(), DbError> {
    let held = crate::query_mapped_rows(
        conn,
        "SELECT payload_hash FROM payload_spool_owners WHERE owner_key = ?1",
        [owner_key],
        |row| row.get::<_, String>(0),
    )
    .map_err(DbError::from)?
    .into_iter()
    .map(|hash| hash.parse::<ObjectHash>().map_err(DbError::from))
    .collect::<Result<BTreeSet<_>, _>>()?;

    for hash in held.difference(payloads) {
        conn.execute(
            "DELETE FROM payload_spool_owners WHERE payload_hash = ?1 AND owner_key = ?2",
            rusqlite::params![hash.to_string(), owner_key],
        )
        .map_err(DbError::from)?;
        let claimed: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM payload_spool_owners WHERE payload_hash = ?1)",
                [hash.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if !claimed {
            conn.execute(
                "INSERT OR IGNORE INTO payload_spool_cleanup (payload_hash) VALUES (?1)",
                [hash.to_string()],
            )
            .map_err(DbError::from)?;
        }
    }
    for hash in payloads.difference(&held) {
        conn.execute(
            "INSERT INTO payload_spool_owners (payload_hash, owner_key) VALUES (?1, ?2)",
            rusqlite::params![hash.to_string(), owner_key],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "DELETE FROM payload_spool_cleanup WHERE payload_hash = ?1",
            [hash.to_string()],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

/// Drop every claim `owner_key` holds, owing a deletion for each payload it was
/// the last claimant of. Called in the transaction that drops the row.
pub fn release_payload_owner_on(conn: &Connection, owner_key: &str) -> Result<(), DbError> {
    set_payload_owner_claims_on(conn, owner_key, &BTreeSet::new())
}

/// The owner key naming one Circle operation's claim on its prepared objects.
pub fn circle_operation_owner_key(operation_id: &str) -> String {
    format!("circle-operation:{operation_id}")
}

/// The owner key naming one remote object record's claim on its payloads.
pub fn remote_object_owner_key(object_id: ObjectHash) -> String {
    format!("remote-object:{object_id}")
}

impl StoreDatabase {
    /// The payloads still owed a deletion. Empty once every obligation this
    /// store committed has been discharged.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn owed_payload_spool_cleanup(&self) -> Result<Vec<ObjectHash>, DbError> {
        self.connection.call(payload_spool_cleanup_hashes_on).await
    }

    /// The payloads `owner_key` claims. Empty when it holds none.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn payload_owner_claims(&self, owner_key: &str) -> Result<Vec<ObjectHash>, DbError> {
        let owner_key = owner_key.to_string();
        self.connection
            .call(move |conn| {
                crate::query_mapped_rows(
                    conn,
                    "SELECT payload_hash FROM payload_spool_owners
                     WHERE owner_key = ?1 ORDER BY payload_hash",
                    [owner_key],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)?
                .into_iter()
                .map(|hash| hash.parse::<ObjectHash>().map_err(DbError::from))
                .collect()
            })
            .await
    }
}

fn payload_spool_cleanup_hashes_on(conn: &Connection) -> Result<Vec<ObjectHash>, DbError> {
    crate::query_mapped_rows(
        conn,
        "SELECT payload_hash FROM payload_spool_cleanup ORDER BY payload_hash",
        [],
        |row| row.get::<_, String>(0),
    )
    .map_err(DbError::from)?
    .into_iter()
    .map(|hash| hash.parse::<ObjectHash>().map_err(DbError::from))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_foundation::store_dir::temp_store_dir;

    fn payload(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    fn spool_entries(store_dir: &StoreDir) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(store_dir.payload_spool_dir())
            .expect("read payload spool directory")
            .map(|entry| {
                entry
                    .expect("payload spool entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn a_payload_round_trips_and_is_absent_once_deleted() {
        let (_directory, store_dir) = temp_store_dir();
        let spool = PayloadSpool::new(&store_dir);
        let bytes = payload(3, 4096);

        let hash = spool.write(&bytes).await.expect("write payload");

        assert_eq!(hash, ObjectHash::digest(&bytes));
        assert_eq!(spool.read(hash).await.expect("read payload"), bytes);
        assert_eq!(
            read_payload_blocking(&store_dir, hash).expect("read payload"),
            bytes
        );

        delete_payload_blocking(&store_dir, hash).expect("delete payload");

        let error = spool.read(hash).await.expect_err("deleted payload");
        assert!(
            matches!(error, PayloadSpoolError::Missing { hash: missing, .. } if missing == hash),
            "{error}"
        );
    }

    #[tokio::test]
    async fn deleting_a_payload_that_is_already_gone_is_success() {
        let (_directory, store_dir) = temp_store_dir();
        let spool = PayloadSpool::new(&store_dir);
        let hash = spool.write(&payload(9, 16)).await.expect("write payload");

        delete_payload_blocking(&store_dir, hash).expect("delete payload");
        delete_payload_blocking(&store_dir, hash).expect("delete absent payload");
    }

    /// Writing the same content twice is the shape a retried insert takes, so
    /// the second write has to land on the first one's file and leave nothing
    /// else behind — no second copy, and no temporary sibling from either write.
    /// Both write paths install the same file, so the blocking one a database
    /// transaction uses agrees with the async one a task uses.
    #[tokio::test]
    async fn rewriting_the_same_payload_installs_the_same_single_file() {
        let (_directory, store_dir) = temp_store_dir();
        let spool = PayloadSpool::new(&store_dir);
        let bytes = payload(5, 8192);

        let first = spool.write(&bytes).await.expect("write payload");
        let second = spool.write(&bytes).await.expect("rewrite payload");
        let third = write_payload_blocking(&store_dir, &bytes).expect("rewrite payload");

        assert_eq!(first, second);
        assert_eq!(first, third);
        assert_eq!(spool_entries(&store_dir), vec![first.to_string()]);
        assert_eq!(spool.read(first).await.expect("read payload"), bytes);
    }

    /// A payload's file is named for its own content, so a reader that finds
    /// the name must find the whole payload under it — never the prefix of a
    /// write still in flight. Reading throughout a write asserts exactly that
    /// invariant against the bytes each read returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reader_never_observes_a_partly_written_payload() {
        let (_directory, store_dir) = temp_store_dir();
        let bytes = payload(1, 16 << 20);
        let hash = ObjectHash::digest(&bytes);
        let reading = store_dir.clone();

        let reader = tokio::spawn(async move {
            let spool = PayloadSpool::new(&reading);
            for _ in 0..100_000 {
                match spool.read(hash).await {
                    Ok(found) => {
                        assert_eq!(
                            ObjectHash::digest(&found),
                            hash,
                            "read {} bytes that do not hash to the name they were under",
                            found.len()
                        );
                        return;
                    }
                    Err(PayloadSpoolError::Missing { .. }) => tokio::task::yield_now().await,
                    Err(error) => panic!("read during write: {error}"),
                }
            }
            panic!("payload never became readable");
        });

        PayloadSpool::new(&store_dir)
            .write(&bytes)
            .await
            .expect("write payload");

        reader.await.expect("reader task");
    }

    /// Claim `payloads` for `owner_key` the way an owning flow does: inside the
    /// transaction that writes the row holding the claim.
    async fn commit_claims(db: &crate::Database, owner_key: &str, payloads: &[ObjectHash]) {
        let owner_key = owner_key.to_string();
        let payloads = payloads.iter().copied().collect::<BTreeSet<_>>();
        db.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            set_payload_owner_claims_on(&tx, &owner_key, &payloads)?;
            tx.commit().map_err(DbError::from)
        })
        .await
        .expect("record payload claims");
    }

    #[tokio::test]
    async fn the_last_claim_to_go_owes_its_payload_a_deletion_and_the_drain_pays_it() {
        let (_directory, store_dir) = temp_store_dir();
        let db = crate::synthetic_store::open_test_db();
        let database = crate::synthetic_store::store_database(&db);
        let spool = PayloadSpool::new(&store_dir);
        let kept = spool.write(&payload(1, 64)).await.expect("write kept");
        let dropped = spool.write(&payload(2, 64)).await.expect("write dropped");

        commit_claims(&db, "owner-a", &[kept, dropped]).await;
        commit_claims(&db, "owner-a", &[kept]).await;

        spool
            .drain_cleanup(&database)
            .await
            .expect("drain obligations");

        assert_eq!(spool_entries(&store_dir), vec![kept.to_string()]);
        assert_eq!(
            database
                .owed_payload_spool_cleanup()
                .await
                .expect("remaining obligations"),
            Vec::new()
        );
    }

    /// Two owners naming one payload is the collision the claim table exists
    /// for: a Circle operation finishing with an object whose remote record
    /// still needs its bytes must not take the file with it.
    #[tokio::test]
    async fn a_payload_a_second_owner_still_claims_is_not_owed_a_deletion() {
        let (_directory, store_dir) = temp_store_dir();
        let db = crate::synthetic_store::open_test_db();
        let database = crate::synthetic_store::store_database(&db);
        let spool = PayloadSpool::new(&store_dir);
        let shared = spool.write(&payload(3, 64)).await.expect("write shared");

        commit_claims(&db, "owner-a", &[shared]).await;
        commit_claims(&db, "owner-b", &[shared]).await;
        commit_claims(&db, "owner-a", &[]).await;

        assert_eq!(
            database
                .owed_payload_spool_cleanup()
                .await
                .expect("obligations"),
            Vec::new()
        );

        spool
            .drain_cleanup(&database)
            .await
            .expect("drain obligations");
        assert_eq!(spool_entries(&store_dir), vec![shared.to_string()]);

        commit_claims(&db, "owner-b", &[]).await;
        assert_eq!(
            database
                .owed_payload_spool_cleanup()
                .await
                .expect("obligations"),
            vec![shared]
        );
    }

    /// A payload an owner takes on is a payload some row names, so whatever
    /// deletion it was owed is void. Re-preparing an object whose earlier
    /// record was deleted lands exactly here.
    #[tokio::test]
    async fn claiming_a_payload_that_is_owed_a_deletion_discharges_the_obligation() {
        let (_directory, store_dir) = temp_store_dir();
        let db = crate::synthetic_store::open_test_db();
        let database = crate::synthetic_store::store_database(&db);
        let spool = PayloadSpool::new(&store_dir);
        let bytes = payload(4, 64);
        let hash = spool.write(&bytes).await.expect("write payload");

        commit_claims(&db, "owner-a", &[hash]).await;
        commit_claims(&db, "owner-a", &[]).await;
        commit_claims(&db, "owner-b", &[hash]).await;

        assert_eq!(
            database
                .owed_payload_spool_cleanup()
                .await
                .expect("obligations"),
            Vec::new()
        );
        spool
            .drain_cleanup(&database)
            .await
            .expect("drain obligations");
        assert_eq!(spool.read(hash).await.expect("read payload"), bytes);
    }

    /// One owner replacing its claim set — the shape a rewritten journal takes
    /// — must not put a payload it keeps through a moment of being unowned.
    #[tokio::test]
    async fn replacing_a_claim_set_keeps_the_payloads_that_stay_in_it() {
        let (_directory, store_dir) = temp_store_dir();
        let db = crate::synthetic_store::open_test_db();
        let database = crate::synthetic_store::store_database(&db);
        let spool = PayloadSpool::new(&store_dir);
        let carried = spool.write(&payload(5, 64)).await.expect("write carried");
        let superseded = spool.write(&payload(6, 64)).await.expect("write old");
        let fresh = spool.write(&payload(7, 64)).await.expect("write new");

        commit_claims(&db, "owner-a", &[carried, superseded]).await;
        commit_claims(&db, "owner-a", &[carried, fresh]).await;

        assert_eq!(
            database
                .owed_payload_spool_cleanup()
                .await
                .expect("obligations"),
            vec![superseded]
        );
        spool
            .drain_cleanup(&database)
            .await
            .expect("drain obligations");

        let mut surviving = vec![carried.to_string(), fresh.to_string()];
        surviving.sort();
        assert_eq!(spool_entries(&store_dir), surviving);
    }

    /// A drain that fails between removing the file and clearing the row leaves
    /// the obligation durable, so the retry finds the file already gone. It
    /// still has to clear the obligation rather than fail on the absence.
    #[tokio::test]
    async fn a_drain_whose_payload_is_already_gone_still_clears_the_obligation() {
        let (_directory, store_dir) = temp_store_dir();
        let db = crate::synthetic_store::open_test_db();
        let database = crate::synthetic_store::store_database(&db);
        let spool = PayloadSpool::new(&store_dir);
        let hash = spool.write(&payload(8, 64)).await.expect("write payload");
        delete_payload_blocking(&store_dir, hash).expect("delete payload");

        commit_claims(&db, "owner-a", &[hash]).await;
        commit_claims(&db, "owner-a", &[]).await;
        spool
            .drain_cleanup(&database)
            .await
            .expect("drain obligations");

        assert_eq!(
            database
                .owed_payload_spool_cleanup()
                .await
                .expect("remaining obligations"),
            Vec::new()
        );
    }
}
