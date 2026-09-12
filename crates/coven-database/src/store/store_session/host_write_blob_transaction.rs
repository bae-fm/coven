use super::*;

pub struct HostWriteBlobTransaction<'transaction, 'connection> {
    store: StoreTransaction<'transaction, 'connection>,
    verified_authority: &'transaction mut VerifiedStoreAuthority,
    created_payload_files: &'transaction std::cell::RefCell<Vec<PathBuf>>,
}

impl<'transaction, 'connection> HostWriteBlobTransaction<'transaction, 'connection> {
    pub(super) fn new(
        store: StoreTransaction<'transaction, 'connection>,
        verified_authority: &'transaction mut VerifiedStoreAuthority,
        created_payload_files: &'transaction std::cell::RefCell<Vec<PathBuf>>,
    ) -> Self {
        Self {
            store,
            verified_authority,
            created_payload_files,
        }
    }

    /// Retain the verified plaintext source in the same transaction as its
    /// captured write. Files created before SQL commit join capture rollback.
    pub fn retain_source_plaintext(
        &mut self,
        fact: &StoreWriteBlobFact,
        source: &std::path::Path,
    ) -> Result<(), DbError> {
        let (hash, size) = PayloadStore::new(self.store.transaction, self.store.store_dir)
            .file_writer(source)?
            .commit(super::payload_store::CreatedPayloadFiles::tracked(
                self.created_payload_files,
            ))?;
        if hash != fact.plaintext_hash || size != fact.plaintext_size {
            return Err(DbError::Message(format!(
                "captured blob {}/{}/{} source differs from its declared plaintext",
                fact.table, fact.row_id, fact.column,
            )));
        }
        Ok(())
    }

    pub fn circle_blob_opening_protection(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        self.store.circle_blob_opening_protection(
            self.verified_authority,
            root,
            circle_id,
            expected_control,
            expected_key_fingerprint,
        )
    }

    pub fn external_local_path(
        &self,
        fact: &StoreWriteBlobFact,
    ) -> Result<Option<PathBuf>, DbError> {
        let stored = self
            .store
            .transaction
            .query_row(
                "SELECT path, plaintext_size, plaintext_hash
                 FROM local_blob_refs
                 WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                   AND namespace = ?4 AND blob_id = ?5
                 ORDER BY row_stamp DESC LIMIT 1",
                rusqlite::params![
                    fact.table,
                    fact.row_id,
                    fact.column,
                    fact.blob.namespace,
                    fact.blob.id,
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((path, size, hash)) = stored else {
            return Ok(None);
        };
        let size = u64::try_from(size).map_err(|_| {
            DbError::Message("registered external blob has a negative size".to_string())
        })?;
        if size != fact.plaintext_size || hash != fact.plaintext_hash.to_string() {
            return Err(DbError::Message(
                "registered external blob identity differs from the moved row".to_string(),
            ));
        }
        Ok(Some(PathBuf::from(path)))
    }
}

/// Remove payload spool files an unfinished operation created, newest first.
/// Reports every file it could not remove rather than any one of them.
pub(crate) fn remove_created_payload_files(
    directory: &coven_foundation::store_dir::StoreDir,
    files: Vec<PathBuf>,
) -> Result<(), crate::StagedBlobRollbackFailures> {
    let mut failures = Vec::new();
    for path in files.into_iter().rev() {
        let cleanup = std::fs::remove_file(&path)
            .map_err(|source| {
                coven_foundation::atomic_file::FileError::at(
                    "remove captured payload",
                    &path,
                    source,
                )
            })
            .and_then(|()| directory.sync_parent_dir_blocking(&path));
        if let Err(error) = cleanup {
            failures.push(crate::StagedBlobRollbackFailure {
                path,
                reason: error.into(),
            });
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(crate::StagedBlobRollbackFailures(failures))
    }
}

pub(super) fn rollback_captured_payload_files(
    directory: &coven_foundation::store_dir::StoreDir,
    files: Vec<PathBuf>,
    operation: DbError,
) -> DbError {
    match remove_created_payload_files(directory, files) {
        Ok(()) => operation,
        Err(rollback) => DbError::AudienceBlobRollbackFailed {
            operation: Box::new(operation),
            rollback,
        },
    }
}

impl StoreSession<'_> {
    fn stage_captured_blob_source(
        &self,
        fact: StoreWriteBlobFact,
        staged: coven_foundation::local_file::AtomicStagedFile,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, DbError> {
        let materialize = (|| {
            if fact.audience_move != Some(StoreWriteBlobMoveMaterialization::Payload) {
                return Err(DbError::Message(
                    "blob fact does not retain a captured payload".into(),
                ));
            }
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(staged.path())
                .map_err(|error| {
                    coven_foundation::atomic_file::FileError::at(
                        "open captured blob stage",
                        staged.path(),
                        error,
                    )
                })?;
            let size = PayloadStore::new(self.conn, self.store_dir)
                .copy_verified(fact.plaintext_hash, &mut output)?;
            if size != fact.plaintext_size {
                return Err(DbError::Message(
                    "captured blob payload size differs from its fact".into(),
                ));
            }
            Ok(())
        })();
        match materialize {
            Ok(()) => Ok(staged),
            Err(operation) => {
                let path = staged.path().to_path_buf();
                match staged.discard_blocking() {
                    Ok(()) => Err(operation),
                    Err(cleanup) => Err(DbError::AudienceBlobRollbackFailed {
                        operation: Box::new(operation),
                        rollback: crate::StagedBlobRollbackFailures(vec![
                            crate::StagedBlobRollbackFailure {
                                path,
                                reason: cleanup.into(),
                            },
                        ]),
                    }),
                }
            }
        }
    }
}

impl StoreDatabase {
    /// Fill an unpublished file from an audience move's exact captured source.
    pub async fn stage_captured_blob_source(
        &self,
        fact: StoreWriteBlobFact,
        staged: coven_foundation::local_file::AtomicStagedFile,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, DbError> {
        self.call_store(move |session| session.stage_captured_blob_source(fact, staged))
            .await
    }
}
