use std::path::{Path, PathBuf};

use coven_database::AudienceMove;
use coven_database::{
    DbError, HostWriteBlobTransaction, StoreWriteBlobFact, StoreWriteBlobFacts,
    StoreWriteBlobMoveMaterialization,
};
use coven_foundation::store_dir::StoreDir;
use coven_protocol::blob::locator::RemoteAudience;
use coven_protocol::blob::{Provenance, RowBlobAuthority};
use coven_protocol::objects::BlobSpoolProtection;
use coven_storage::CloudSyncObjectStorage;

enum BlobMoveOpening {
    Store,
    Circle(BlobSpoolProtection),
}

#[doc(hidden)]
pub struct HostWriteBlobStaging {
    runtime: tokio::runtime::Handle,
    storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
    store_root: coven_protocol::store_commit::StoreRootRef,
    store_dir: StoreDir,
}

impl HostWriteBlobStaging {
    pub(super) fn new(
        runtime: tokio::runtime::Handle,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
        store_root: coven_protocol::store_commit::StoreRootRef,
        store_dir: StoreDir,
    ) -> Self {
        Self {
            runtime,
            storage,
            store_root,
            store_dir,
        }
    }

    pub(crate) fn stage_audience_move_blobs_on(
        &self,
        transaction: &mut HostWriteBlobTransaction<'_, '_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
    ) -> Result<StagedAudienceBlobFiles, DbError> {
        self.runtime.block_on(async {
            let mut files = StagedAudienceBlobFiles::new(self.store_dir.clone());
            let result = self
                .stage_audience_move_blobs_inner(transaction, facts, moves, &mut files)
                .await;
            match result {
                Ok(()) => Ok(files),
                Err(error) => match files.rollback().await {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(DbError::AudienceBlobRollbackFailed {
                        operation: Box::new(error),
                        rollback,
                    }),
                },
            }
        })
    }

    async fn stage_audience_move_blobs_inner(
        &self,
        transaction: &mut HostWriteBlobTransaction<'_, '_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
        files: &mut StagedAudienceBlobFiles,
    ) -> Result<(), DbError> {
        let moved_rows = coven_database::audience_moves_by_row(moves)?;
        if moved_rows.is_empty() {
            return Ok(());
        }

        for fact in &mut facts.blobs {
            let Some(audience_move) = moved_rows.get(&(fact.table.clone(), fact.row_id.clone()))
            else {
                continue;
            };
            let source = source_authority(fact, &audience_move.source)?;
            match &audience_move.destination {
                coven_protocol::circle::Audience::Local => {
                    self.stage_local_destination(transaction, fact, &source, files)
                        .await?;
                    fact.audience_move = Some(StoreWriteBlobMoveMaterialization::Local);
                }
                _ => {
                    let source_stage = self.store_dir.payload_spool_path(fact.plaintext_hash);
                    let source_path = self
                        .move_source_plaintext(transaction, fact, &source, &source_stage)
                        .await?;
                    transaction
                        .retain_source_plaintext(fact, source_path.path())
                        .map_err(|error| move_materialization_error(fact, error))?;
                    fact.audience_move = Some(StoreWriteBlobMoveMaterialization::Payload);
                }
            }
        }
        Ok(())
    }

    fn opening_protection(
        &self,
        transaction: &mut HostWriteBlobTransaction<'_, '_>,
        fact: &StoreWriteBlobFact,
        source: &RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<BlobMoveOpening, DbError> {
        match source
            .opening_authority(stored)
            .map_err(|error| move_materialization_error(fact, error))?
        {
            coven_protocol::blob::BlobOpeningAuthority::Store => Ok(BlobMoveOpening::Store),
            coven_protocol::blob::BlobOpeningAuthority::Circle {
                circle_id,
                control,
                key_fingerprint,
            } => transaction
                .circle_blob_opening_protection(
                    &self.store_root,
                    circle_id,
                    control,
                    key_fingerprint,
                )
                .map(BlobMoveOpening::Circle)
                .map_err(|error| move_materialization_error(fact, error)),
        }
    }

    async fn move_source_plaintext(
        &self,
        transaction: &mut HostWriteBlobTransaction<'_, '_>,
        fact: &StoreWriteBlobFact,
        source: &RowBlobAuthority,
        spool_path: &Path,
    ) -> Result<MoveSourcePlaintext, DbError> {
        if let Some(path) = self.local_source_path(transaction, fact, source)? {
            match tokio::fs::metadata(&path).await {
                Ok(metadata) if metadata.is_file() => {
                    ExactMovePlaintext::new(fact, &path).verify().await?;
                    return Ok(MoveSourcePlaintext::Existing(path));
                }
                Ok(_) => {
                    return Err(move_materialization_error(
                        fact,
                        DbError::Message(format!("blob source is not a file: {}", path.display())),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(move_materialization_error(
                        fact,
                        DbError::context(format!("inspect blob source {}", path.display()), error),
                    ));
                }
            }
        }
        if source == &RowBlobAuthority::Local {
            return Err(move_materialization_error(
                fact,
                DbError::Message("Local source plaintext is unavailable".to_string()),
            ));
        }
        let previous = fact.previous.as_ref().ok_or_else(|| {
            move_materialization_error(
                fact,
                DbError::Message("remote source has no exact prior blob locator".to_string()),
            )
        })?;
        let opening = self.opening_protection(transaction, fact, source, &previous.stored)?;
        let destination = spool_path.with_extension("move-plaintext");
        let stage = self
            .store_dir
            .stage_atomic_file(&destination)
            .await
            .map_err(|error| move_materialization_error(fact, DbError::File(error)))?;
        let staged = match opening {
            BlobMoveOpening::Store => {
                self.storage
                    .stage_verified_store_blob_plaintext(
                        &previous.stored,
                        stage,
                        coven_storage::cloud::no_download_progress(),
                    )
                    .await
            }
            BlobMoveOpening::Circle(protection) => {
                self.storage
                    .stage_verified_blob_plaintext(
                        &previous.stored,
                        protection,
                        stage,
                        coven_storage::cloud::no_download_progress(),
                    )
                    .await
            }
        }
        .map_err(|error| move_materialization_error(fact, error))?;
        Ok(MoveSourcePlaintext::Downloaded(staged))
    }

    async fn stage_local_destination(
        &self,
        transaction: &mut HostWriteBlobTransaction<'_, '_>,
        fact: &StoreWriteBlobFact,
        source: &RowBlobAuthority,
        files: &mut StagedAudienceBlobFiles,
    ) -> Result<(), DbError> {
        let destination = match fact.blob.provenance {
            Provenance::HostProvided => self
                .store_dir
                .local_blob_path(&fact.blob.namespace, &fact.blob.id)
                .map_err(|error| move_materialization_error(fact, error))?,
            Provenance::UserProvided => transaction
                .external_local_path(fact)
                .map_err(|error| move_materialization_error(fact, error))?
                .ok_or_else(|| {
                    move_materialization_error(
                        fact,
                        DbError::Message(
                            "UserProvided Local destination has no registered file path"
                                .to_string(),
                        ),
                    )
                })?,
        };
        match tokio::fs::metadata(&destination).await {
            Ok(metadata) if metadata.is_file() => {
                ExactMovePlaintext::new(fact, &destination).verify().await?;
                return Ok(());
            }
            Ok(_) => {
                return Err(move_materialization_error(
                    fact,
                    DbError::Message(format!(
                        "Local destination is not a file: {}",
                        destination.display()
                    )),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(move_materialization_error(
                    fact,
                    DbError::context(
                        format!("inspect Local destination {}", destination.display()),
                        error,
                    ),
                ));
            }
        }
        if source == &RowBlobAuthority::Local {
            return Err(move_materialization_error(
                fact,
                DbError::Message("Local source plaintext is unavailable".to_string()),
            ));
        }
        let previous = fact.previous.as_ref().ok_or_else(|| {
            move_materialization_error(
                fact,
                DbError::Message("remote source has no exact prior blob locator".to_string()),
            )
        })?;
        let opening = self.opening_protection(transaction, fact, source, &previous.stored)?;
        let stage = self
            .store_dir
            .stage_atomic_file(&destination)
            .await
            .map_err(|error| move_materialization_error(fact, DbError::File(error)))?;
        let staged = match opening {
            BlobMoveOpening::Store => {
                self.storage
                    .stage_verified_store_blob_plaintext(
                        &previous.stored,
                        stage,
                        coven_storage::cloud::no_download_progress(),
                    )
                    .await
            }
            BlobMoveOpening::Circle(protection) => {
                self.storage
                    .stage_verified_blob_plaintext(
                        &previous.stored,
                        protection,
                        stage,
                        coven_storage::cloud::no_download_progress(),
                    )
                    .await
            }
        }
        .map_err(|error| move_materialization_error(fact, error))?;
        match fact.blob.provenance {
            Provenance::HostProvided => staged
                .commit()
                .await
                .map_err(|error| move_materialization_error(fact, DbError::File(error)))?,
            Provenance::UserProvided => staged
                .commit_new()
                .await
                .map_err(|error| move_materialization_error(fact, error))?,
        }
        files.created.push(StagedAudienceBlobFile::new(destination));
        Ok(())
    }

    fn local_source_path(
        &self,
        transaction: &HostWriteBlobTransaction<'_, '_>,
        fact: &StoreWriteBlobFact,
        source: &RowBlobAuthority,
    ) -> Result<Option<PathBuf>, DbError> {
        match fact.blob.provenance {
            Provenance::HostProvided => self
                .store_dir
                .local_blob_path(&fact.blob.namespace, &fact.blob.id)
                .map(Some)
                .map_err(|error| move_materialization_error(fact, error)),
            Provenance::UserProvided if source == &RowBlobAuthority::Local => transaction
                .external_local_path(fact)
                .map_err(|error| move_materialization_error(fact, error)),
            Provenance::UserProvided => Ok(fact.external_path.clone()),
        }
    }
}

pub(crate) struct StagedAudienceBlobFiles {
    store_dir: StoreDir,
    created: Vec<StagedAudienceBlobFile>,
}

impl StagedAudienceBlobFiles {
    fn new(store_dir: StoreDir) -> Self {
        Self {
            store_dir,
            created: Vec::new(),
        }
    }

    async fn rollback(self) -> Result<(), coven_database::StagedBlobRollbackFailures> {
        let mut failures = Vec::new();
        for file in self.created.into_iter().rev() {
            let path = file.path.clone();
            if let Err(reason) = file.rollback(&self.store_dir).await {
                failures.push(coven_database::StagedBlobRollbackFailure { path, reason });
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(coven_database::StagedBlobRollbackFailures(failures))
        }
    }
}

struct StagedAudienceBlobFile {
    path: PathBuf,
}

impl StagedAudienceBlobFile {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    async fn rollback(
        self,
        store_dir: &StoreDir,
    ) -> Result<(), coven_database::StagedBlobRollbackReason> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(coven_database::StagedBlobRollbackReason::Missing);
            }
            Err(source) => {
                return Err(coven_foundation::atomic_file::FileError::at(
                    "remove staged audience blob",
                    &self.path,
                    source,
                )
                .into());
            }
        }
        store_dir
            .sync_parent_dir(&self.path)
            .await
            .map_err(Into::into)
    }
}

fn source_authority(
    fact: &StoreWriteBlobFact,
    source: &coven_protocol::circle::Audience,
) -> Result<RowBlobAuthority, DbError> {
    match source {
        coven_protocol::circle::Audience::Local => Ok(RowBlobAuthority::Local),
        source => {
            let expected = RemoteAudience::try_from(source.clone())
                .map_err(|error| move_materialization_error(fact, error))?;
            let previous = fact.previous.as_ref().ok_or_else(|| {
                move_materialization_error(
                    fact,
                    DbError::Message(format!(
                        "source audience {source:?} has no exact prior blob locator"
                    )),
                )
            })?;
            if previous.authority.remote_audience() != expected {
                return Err(move_materialization_error(
                    fact,
                    DbError::Message(format!(
                        "source audience {source:?} differs from exact prior blob authority {:?}",
                        previous.authority
                    )),
                ));
            }
            Ok(RowBlobAuthority::Remote(previous.authority.clone()))
        }
    }
}

enum MoveSourcePlaintext {
    Existing(PathBuf),
    Downloaded(coven_foundation::local_file::AtomicStagedFile),
}

impl MoveSourcePlaintext {
    fn path(&self) -> &Path {
        match self {
            Self::Existing(path) => path,
            Self::Downloaded(staged) => staged.path(),
        }
    }
}

struct ExactMovePlaintext<'a> {
    fact: &'a StoreWriteBlobFact,
    path: &'a Path,
}

impl<'a> ExactMovePlaintext<'a> {
    fn new(fact: &'a StoreWriteBlobFact, path: &'a Path) -> Self {
        Self { fact, path }
    }

    async fn verify(&self) -> Result<(), DbError> {
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncReadExt;

        let mut file = tokio::fs::File::open(self.path)
            .await
            .map_err(|error| move_materialization_error(self.fact, error))?;
        let mut size = 0_u64;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1 << 20];
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .map_err(|error| move_materialization_error(self.fact, error))?;
            if read == 0 {
                break;
            }
            size = size.checked_add(read as u64).ok_or_else(|| {
                move_materialization_error(
                    self.fact,
                    DbError::Message("plaintext size overflow".to_string()),
                )
            })?;
            hasher.update(&buffer[..read]);
        }
        let hash = coven_protocol::store_commit::ObjectHash::from_digest(hasher.finalize().into());
        if size != self.fact.plaintext_size || hash != self.fact.plaintext_hash {
            return Err(move_materialization_error(
                self.fact,
                DbError::Message(format!(
                    "plaintext {} differs from declared size/hash",
                    self.path.display()
                )),
            ));
        }
        Ok(())
    }
}

fn move_materialization_error(fact: &StoreWriteBlobFact, reason: impl Into<DbError>) -> DbError {
    DbError::BlobMoveRequiresMaterialization {
        table: fact.table.clone(),
        row_id: fact.row_id.clone(),
        column: fact.column.clone(),
        row_stamp: fact.row_stamp.clone(),
        reason: Box::new(reason.into()),
    }
}

impl coven_database::AudienceBlobMoveStaging for HostWriteBlobStaging {
    fn stage_audience_move_blobs_on(
        &self,
        transaction: &mut HostWriteBlobTransaction<'_, '_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
    ) -> Result<coven_database::StagedAudienceBlobRollback, DbError> {
        let files =
            HostWriteBlobStaging::stage_audience_move_blobs_on(self, transaction, facts, moves)?;
        let runtime = self.runtime.clone();
        Ok(Box::new(move |operation: DbError| {
            match runtime.block_on(files.rollback()) {
                Ok(()) => operation,
                Err(rollback) => DbError::AudienceBlobRollbackFailed {
                    operation: Box::new(operation),
                    rollback,
                },
            }
        }))
    }
}
