//! Payload files: bytes a row owns, stored beside the database instead of
//! inside it.
//!
//! A payload is written before the row that references it commits, and deleted
//! by the flow that deletes that row. The file is named for the digest of the
//! bytes it holds, so a retry of a failed insert rewrites the same path with
//! the same contents and a file whose insert never committed is inert garbage
//! bounded by that one operation's content.
//!
//! Deletion rides row deletion. The transaction that drops a referencing row
//! records the obligation with [`enqueue_payload_spool_cleanup_on`]; once that
//! transaction commits, [`PayloadSpool::drain_cleanup`] removes each file and
//! clears its obligation. A failure between the two leaves the obligation
//! durable, so the next drain finishes the deletion rather than losing it.

use std::path::PathBuf;

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
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(PayloadSpoolError::Missing { hash, path })
            }
            Err(error) => Err(PayloadSpoolError::File {
                path,
                error: error.to_string(),
            }),
        }
    }

    /// Remove the payload stored under `hash`. An absent file is success: the
    /// obligation this discharges says the payload must not be there, and a
    /// drain that failed after the removal retries the whole deletion.
    pub async fn delete(&self, hash: ObjectHash) -> Result<(), PayloadSpoolError> {
        let path = self.store_dir.payload_spool_path(hash);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => coven_foundation::atomic_file::sync_parent_dir(&path)
                .await
                .map_err(|error| PayloadSpoolError::File {
                    path: path.clone(),
                    error,
                }),
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

    /// Delete the payload behind every committed cleanup obligation, clearing
    /// each obligation once its file is gone. A filesystem or database failure
    /// leaves the obligation durable and fails the drain.
    pub async fn drain_cleanup(&self, database: &StoreDatabase) -> Result<(), PayloadSpoolError> {
        for hash in database.payload_spool_cleanup_hashes().await? {
            self.delete(hash).await?;
            database.complete_payload_spool_cleanup(hash).await?;
        }
        Ok(())
    }
}

/// Record that the payload stored under `hash` is owed a deletion. Called in
/// the transaction that drops the row referencing it, so the row's absence and
/// the file's deletion obligation commit together.
pub fn enqueue_payload_spool_cleanup_on(
    conn: &Connection,
    hash: ObjectHash,
) -> Result<(), DbError> {
    let recorded = conn
        .execute(
            "INSERT OR IGNORE INTO payload_spool_cleanup (payload_hash) VALUES (?1)",
            [hash.to_string()],
        )
        .map_err(DbError::from)?;
    if recorded == 0 {
        debug!(payload = %hash, "payload spool cleanup obligation already recorded");
    }
    Ok(())
}

impl StoreDatabase {
    async fn payload_spool_cleanup_hashes(&self) -> Result<Vec<ObjectHash>, DbError> {
        self.connection
            .call(|conn| {
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
            })
            .await
    }

    async fn complete_payload_spool_cleanup(&self, hash: ObjectHash) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM payload_spool_cleanup WHERE payload_hash = ?1",
                    [hash.to_string()],
                )
                .map(|_| ())
                .map_err(DbError::from)
            })
            .await
    }
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

        spool.delete(hash).await.expect("delete payload");

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

        spool.delete(hash).await.expect("delete payload");
        spool.delete(hash).await.expect("delete absent payload");
    }

    /// Writing the same content twice is the shape a retried insert takes, so
    /// the second write has to land on the first one's file and leave nothing
    /// else behind — no second copy, and no temporary sibling from either write.
    #[tokio::test]
    async fn rewriting_the_same_payload_installs_the_same_single_file() {
        let (_directory, store_dir) = temp_store_dir();
        let spool = PayloadSpool::new(&store_dir);
        let bytes = payload(5, 8192);

        let first = spool.write(&bytes).await.expect("write payload");
        let second = spool.write(&bytes).await.expect("rewrite payload");

        assert_eq!(first, second);
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

    /// Record an obligation the way an owning flow does: inside the
    /// transaction that drops the row referencing the payload.
    async fn commit_obligation(db: &crate::Database, hash: ObjectHash) {
        db.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            enqueue_payload_spool_cleanup_on(&tx, hash)?;
            tx.commit().map_err(DbError::from)
        })
        .await
        .expect("record cleanup obligation");
    }

    #[tokio::test]
    async fn a_committed_obligation_deletes_its_payload_and_clears_itself() {
        let (_directory, store_dir) = temp_store_dir();
        let db = crate::synthetic_store::open_test_db();
        let database = crate::synthetic_store::store_database(&db);
        let spool = PayloadSpool::new(&store_dir);
        let kept = spool.write(&payload(1, 64)).await.expect("write kept");
        let dropped = spool.write(&payload(2, 64)).await.expect("write dropped");

        commit_obligation(&db, dropped).await;
        spool
            .drain_cleanup(&database)
            .await
            .expect("drain obligations");

        assert_eq!(spool_entries(&store_dir), vec![kept.to_string()]);
        assert_eq!(
            database
                .payload_spool_cleanup_hashes()
                .await
                .expect("remaining obligations"),
            Vec::new()
        );
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
        let hash = spool.write(&payload(4, 64)).await.expect("write payload");
        spool.delete(hash).await.expect("delete payload");

        commit_obligation(&db, hash).await;
        spool
            .drain_cleanup(&database)
            .await
            .expect("drain obligations");

        assert_eq!(
            database
                .payload_spool_cleanup_hashes()
                .await
                .expect("remaining obligations"),
            Vec::new()
        );
    }
}
