use std::path::{Path, PathBuf};

use crate::database::{AudienceMove, AudiencePartition};
use crate::database::{
    DbError, HostWriteBlobTransaction, StoreWriteBlobFact, StoreWriteBlobFacts,
    StoreWriteBlobMoveDestination,
};
use crate::protocol::blob::locator::RemoteAudience;
use crate::protocol::blob::{Provenance, RowBlobAuthority};
use crate::protocol::objects::{BlobSpoolProtection, BlobWriteAuthority};
use crate::storage::SyncStorage;
use crate::store_dir::StoreDir;

#[doc(hidden)]
pub(crate) struct HostWriteBlobStaging {
    runtime: tokio::runtime::Handle,
    storage: std::sync::Arc<dyn SyncStorage>,
    store_root: crate::protocol::store_commit::StoreRootRef,
    store_dir: StoreDir,
}

impl HostWriteBlobStaging {
    pub(super) fn new(
        runtime: tokio::runtime::Handle,
        storage: std::sync::Arc<dyn SyncStorage>,
        store_root: crate::protocol::store_commit::StoreRootRef,
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
        transaction: &HostWriteBlobTransaction<'_, '_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
        partitions: &[AudiencePartition],
    ) -> Result<StagedAudienceBlobFiles, DbError> {
        self.runtime.block_on(async {
            let mut files = StagedAudienceBlobFiles::new();
            let result = self
                .stage_audience_move_blobs_inner(transaction, facts, moves, partitions, &mut files)
                .await;
            match result {
                Ok(()) => Ok(files),
                Err(error) => match files.rollback().await {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(DbError::Message(format!(
                        "{error}; audience blob rollback failed: {cleanup}"
                    ))),
                },
            }
        })
    }

    async fn stage_audience_move_blobs_inner(
        &self,
        transaction: &HostWriteBlobTransaction<'_, '_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
        partitions: &[AudiencePartition],
        files: &mut StagedAudienceBlobFiles,
    ) -> Result<(), DbError> {
        let moved_rows = crate::database::audience_moves_by_row(moves)?;
        if moved_rows.is_empty() {
            return Ok(());
        }

        let remote_destination_exists = facts.blobs.iter().any(|fact| {
            moved_rows
                .get(&(fact.table.clone(), fact.row_id.clone()))
                .is_some_and(|audience_move| {
                    audience_move.destination != crate::protocol::circle::Audience::Local
                })
        });
        let registration = if remote_destination_exists {
            Some(transaction.local_activated_registration(&self.store_root)?)
        } else {
            None
        };

        for fact in &mut facts.blobs {
            let Some(audience_move) = moved_rows.get(&(fact.table.clone(), fact.row_id.clone()))
            else {
                continue;
            };
            let source = source_authority(fact, &audience_move.source)?;
            match &audience_move.destination {
                crate::protocol::circle::Audience::Local => {
                    self.stage_local_destination(transaction, fact, &source, files)
                        .await?;
                    fact.audience_move = Some(StoreWriteBlobMoveDestination::Local);
                }
                destination => {
                    let registration = registration
                        .as_ref()
                        .expect("remote destination loads authority");
                    let authority = BlobWriteAuthority::new(registration);
                    let (audience, protection) =
                        self.destination_protection(transaction, destination, partitions, fact)?;
                    let locator = super::writer::prepare_partition_blob_locator(
                        fact,
                        audience.clone(),
                        &protection,
                        &authority,
                    )
                    .map_err(|error| move_materialization_error(fact, error.to_string()))?;
                    let spool_path = self
                        .store_dir
                        .outbound_blob_spool_path(locator.locator_hash());
                    let source_path = self
                        .move_source_plaintext(transaction, fact, &source, &spool_path)
                        .await?;
                    let spool_write = self
                        .storage
                        .seal_blob_to_spool(
                            &locator,
                            &authority,
                            protection,
                            source_path.path(),
                            &spool_path,
                        )
                        .await
                        .map_err(|error| move_materialization_error(fact, error.to_string()))?;
                    if spool_write == crate::protocol::objects::BlobSpoolWrite::Created {
                        files
                            .created
                            .push(StagedAudienceBlobFile::new(spool_path.clone()));
                    }
                    fact.audience_move = Some(StoreWriteBlobMoveDestination::Remote {
                        audience,
                        locator,
                        spool_path,
                    });
                }
            }
        }
        Ok(())
    }

    fn destination_protection(
        &self,
        transaction: &HostWriteBlobTransaction<'_, '_>,
        destination: &crate::protocol::circle::Audience,
        partitions: &[AudiencePartition],
        fact: &StoreWriteBlobFact,
    ) -> Result<(RemoteAudience, BlobSpoolProtection), DbError> {
        match destination {
            crate::protocol::circle::Audience::Store => self
                .storage
                .store_blob_protection()
                .map(|protection| (RemoteAudience::Store, protection))
                .map_err(|error| move_materialization_error(fact, error.to_string())),
            crate::protocol::circle::Audience::Circle(circle_id) => {
                let partition = partitions
                    .iter()
                    .find(|partition| partition.audience == *destination)
                    .ok_or_else(|| {
                        move_materialization_error(
                            fact,
                            format!("destination Circle {circle_id} has no audience partition"),
                        )
                    })?;
                let control = partition.control.as_ref().ok_or_else(|| {
                    move_materialization_error(
                        fact,
                        format!("destination Circle {circle_id} has no exact control"),
                    )
                })?;
                let access = transaction
                    .circle_publication_context(*circle_id, control.coordinate())
                    .map_err(|error| move_materialization_error(fact, error))?;
                Ok((RemoteAudience::Circle(*circle_id), access.blob_protection()))
            }
            crate::protocol::circle::Audience::Local => {
                unreachable!("Local handled before protection")
            }
        }
    }

    fn opening_protection(
        &self,
        transaction: &HostWriteBlobTransaction<'_, '_>,
        fact: &StoreWriteBlobFact,
        source: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<BlobSpoolProtection, DbError> {
        match source
            .opening_authority(stored)
            .map_err(|error| move_materialization_error(fact, error))?
        {
            crate::protocol::blob::BlobOpeningAuthority::Store => self
                .storage
                .store_blob_protection()
                .map_err(|error| move_materialization_error(fact, error)),
            crate::protocol::blob::BlobOpeningAuthority::Circle {
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
                .map_err(|error| move_materialization_error(fact, error)),
        }
    }

    async fn move_source_plaintext(
        &self,
        transaction: &HostWriteBlobTransaction<'_, '_>,
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
                        format!("blob source is not a file: {}", path.display()),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(move_materialization_error(
                        fact,
                        format!("inspect blob source {}: {error}", path.display()),
                    ));
                }
            }
        }
        if source == &RowBlobAuthority::Local {
            return Err(move_materialization_error(
                fact,
                "Local source plaintext is unavailable",
            ));
        }
        let previous = fact.previous.as_ref().ok_or_else(|| {
            move_materialization_error(fact, "remote source has no exact prior blob locator")
        })?;
        let protection = self.opening_protection(transaction, fact, source, &previous.stored)?;
        let destination = spool_path.with_extension("move-plaintext");
        let staged = self
            .storage
            .stage_verified_blob_plaintext(&previous.stored, protection, &destination)
            .await
            .map_err(|error| move_materialization_error(fact, error.to_string()))?;
        Ok(MoveSourcePlaintext::Downloaded(staged))
    }

    async fn stage_local_destination(
        &self,
        transaction: &HostWriteBlobTransaction<'_, '_>,
        fact: &StoreWriteBlobFact,
        source: &RowBlobAuthority,
        files: &mut StagedAudienceBlobFiles,
    ) -> Result<(), DbError> {
        let destination = match fact.blob.provenance {
            Provenance::HostProvided => self
                .store_dir
                .local_blob_path(&fact.blob.namespace, &fact.blob.id)
                .map_err(|error| move_materialization_error(fact, error.to_string()))?,
            Provenance::UserProvided => transaction
                .external_local_path(fact)
                .map_err(|error| move_materialization_error(fact, error))?
                .ok_or_else(|| {
                    move_materialization_error(
                        fact,
                        "UserProvided Local destination has no registered file path",
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
                    format!("Local destination is not a file: {}", destination.display()),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(move_materialization_error(
                    fact,
                    format!(
                        "inspect Local destination {}: {error}",
                        destination.display()
                    ),
                ));
            }
        }
        if source == &RowBlobAuthority::Local {
            return Err(move_materialization_error(
                fact,
                "Local source plaintext is unavailable",
            ));
        }
        let previous = fact.previous.as_ref().ok_or_else(|| {
            move_materialization_error(fact, "remote source has no exact prior blob locator")
        })?;
        let protection = self.opening_protection(transaction, fact, source, &previous.stored)?;
        let staged = self
            .storage
            .stage_verified_blob_plaintext(&previous.stored, protection, &destination)
            .await
            .map_err(|error| move_materialization_error(fact, error.to_string()))?;
        match fact.blob.provenance {
            Provenance::HostProvided => staged
                .commit()
                .await
                .map_err(|error| move_materialization_error(fact, error))?,
            Provenance::UserProvided => staged
                .commit_new()
                .await
                .map_err(|error| move_materialization_error(fact, error.to_string()))?,
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
                .map_err(|error| move_materialization_error(fact, error.to_string())),
            Provenance::UserProvided if source == &RowBlobAuthority::Local => transaction
                .external_local_path(fact)
                .map_err(|error| move_materialization_error(fact, error)),
            Provenance::UserProvided => Ok(fact.external_path.clone()),
        }
    }
}

pub(crate) struct StagedAudienceBlobFiles {
    created: Vec<StagedAudienceBlobFile>,
}

impl StagedAudienceBlobFiles {
    fn new() -> Self {
        Self {
            created: Vec::new(),
        }
    }

    async fn rollback(self) -> Result<(), DbError> {
        let mut failures = Vec::new();
        for file in self.created.into_iter().rev() {
            if let Err(error) = file.rollback().await {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Message(failures.join("; ")))
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

    async fn rollback(self) -> Result<(), String> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(format!(
                    "staged audience blob disappeared before rollback: {}",
                    self.path.display()
                ));
            }
            Err(error) => {
                return Err(format!(
                    "remove staged audience blob {}: {error}",
                    self.path.display()
                ));
            }
        }
        crate::atomic_file::sync_parent_dir(&self.path).await
    }
}

fn source_authority(
    fact: &StoreWriteBlobFact,
    source: &crate::protocol::circle::Audience,
) -> Result<RowBlobAuthority, DbError> {
    match source {
        crate::protocol::circle::Audience::Local => Ok(RowBlobAuthority::Local),
        source => {
            let expected = RemoteAudience::try_from(source.clone())
                .map_err(|error| move_materialization_error(fact, error.to_string()))?;
            let previous = fact.previous.as_ref().ok_or_else(|| {
                move_materialization_error(
                    fact,
                    format!("source audience {source:?} has no exact prior blob locator"),
                )
            })?;
            if previous.authority.remote_audience() != expected {
                return Err(move_materialization_error(
                    fact,
                    format!(
                        "source audience {source:?} differs from exact prior blob authority {:?}",
                        previous.authority
                    ),
                ));
            }
            Ok(RowBlobAuthority::Remote(previous.authority.clone()))
        }
    }
}

enum MoveSourcePlaintext {
    Existing(PathBuf),
    Downloaded(crate::local_file::AtomicStagedFile),
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
            size = size
                .checked_add(read as u64)
                .ok_or_else(|| move_materialization_error(self.fact, "plaintext size overflow"))?;
            hasher.update(&buffer[..read]);
        }
        let hash = crate::protocol::store_commit::ObjectHash::from_digest(hasher.finalize().into());
        if size != self.fact.plaintext_size || hash != self.fact.plaintext_hash {
            return Err(move_materialization_error(
                self.fact,
                format!(
                    "plaintext {} differs from declared size/hash",
                    self.path.display()
                ),
            ));
        }
        Ok(())
    }
}

fn move_materialization_error(
    fact: &StoreWriteBlobFact,
    reason: impl std::fmt::Display,
) -> DbError {
    DbError::Message(format!(
        "BlobMoveRequiresMaterialization: {}/{}/{} at {}: {reason}",
        fact.table, fact.row_id, fact.column, fact.row_stamp
    ))
}

impl crate::database::AudienceBlobMoveStaging for HostWriteBlobStaging {
    fn stage_audience_move_blobs_on(
        &self,
        transaction: &HostWriteBlobTransaction<'_, '_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
        partitions: &[AudiencePartition],
    ) -> Result<crate::database::StagedAudienceBlobRollback, DbError> {
        let files = HostWriteBlobStaging::stage_audience_move_blobs_on(
            self,
            transaction,
            facts,
            moves,
            partitions,
        )?;
        let runtime = self.runtime.clone();
        Ok(Box::new(move |operation: DbError| {
            match runtime.block_on(files.rollback()) {
                Ok(()) => operation,
                Err(cleanup) => DbError::Message(format!(
                    "{operation}; audience blob rollback failed: {cleanup}"
                )),
            }
        }))
    }
}
