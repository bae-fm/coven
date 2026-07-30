use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rusqlite::OptionalExtension;

use crate::blob::locator::RemoteAudience;
use crate::blob::{Provenance, RowBlobAuthority};
use crate::database::StoreDatabase;
use crate::database::{
    load_activated_registration_on, local_activated_registration_ref_on, DbError,
    StoreWriteBlobFact, StoreWriteBlobFacts, StoreWriteBlobMoveDestination,
};
use crate::storage::{BlobSpoolProtection, BlobWriteAuthority, SyncStorage};
use crate::store_dir::StoreDir;
use crate::sync::gate::{AudienceMove, AudiencePartition};

#[doc(hidden)]
pub(crate) struct HostWriteBlobStaging {
    runtime: tokio::runtime::Handle,
    store: std::sync::Arc<crate::sync::store::Store>,
    store_dir: StoreDir,
}

impl HostWriteBlobStaging {
    pub(super) fn new(
        runtime: tokio::runtime::Handle,
        store: std::sync::Arc<crate::sync::store::Store>,
        store_dir: StoreDir,
    ) -> Self {
        Self {
            runtime,
            store,
            store_dir,
        }
    }

    pub(crate) fn stage_audience_move_blobs_on(
        &self,
        tx: &rusqlite::Transaction<'_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
        partitions: &[AudiencePartition],
    ) -> Result<StagedAudienceBlobFiles, DbError> {
        self.runtime.block_on(async {
            let mut files = StagedAudienceBlobFiles::new();
            let result = self
                .stage_audience_move_blobs_inner(tx, facts, moves, partitions, &mut files)
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

    pub(crate) fn rollback_staged_audience_blobs(
        &self,
        files: StagedAudienceBlobFiles,
        operation: DbError,
    ) -> DbError {
        match self.runtime.block_on(files.rollback()) {
            Ok(()) => operation,
            Err(cleanup) => DbError::Message(format!(
                "{operation}; audience blob rollback failed: {cleanup}"
            )),
        }
    }

    pub(crate) fn record_prepared_transition_local_blob_moves(
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
    ) -> Result<(), DbError> {
        let moved_rows = audience_moves_by_row(moves)?;
        for fact in &mut facts.blobs {
            let Some(audience_move) = moved_rows.get(&(fact.table.clone(), fact.row_id.clone()))
            else {
                continue;
            };
            if audience_move.destination == crate::protocol::circle::Audience::Local {
                fact.audience_move = Some(StoreWriteBlobMoveDestination::Local);
            }
        }
        Ok(())
    }

    async fn stage_audience_move_blobs_inner(
        &self,
        tx: &rusqlite::Transaction<'_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
        partitions: &[AudiencePartition],
        files: &mut StagedAudienceBlobFiles,
    ) -> Result<(), DbError> {
        let storage = self.store.storage().as_ref();
        let store_root = self.store.store_root();
        let store_dir = &self.store_dir;
        let moved_rows = audience_moves_by_row(moves)?;
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
            let reference = local_activated_registration_ref_on(tx)?.ok_or_else(|| {
                DbError::Message(
                    "audience blob move has no activated local Store registration".to_string(),
                )
            })?;
            let registration = load_activated_registration_on(tx, store_root, &reference)?;
            Some((reference, registration))
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
                    stage_local_destination(
                        tx, storage, store_root, store_dir, fact, &source, files,
                    )
                    .await?;
                    fact.audience_move = Some(StoreWriteBlobMoveDestination::Local);
                }
                destination => {
                    let (registration_ref, registration) = registration
                        .as_ref()
                        .expect("remote destination loads authority");
                    let authority = BlobWriteAuthority::new(registration_ref, registration)
                        .map_err(|error| move_materialization_error(fact, error.to_string()))?;
                    let (audience, protection) =
                        destination_protection(tx, storage, destination, partitions, fact)?;
                    let locator = super::writer::blob_preparation::prepare_partition_blob_locator(
                        fact,
                        audience.clone(),
                        &protection,
                        &authority,
                    )
                    .map_err(|error| move_materialization_error(fact, error.to_string()))?;
                    let spool_path = store_dir.outbound_blob_spool_path(locator.locator_hash());
                    let source_path = move_source_plaintext(
                        tx,
                        storage,
                        store_root,
                        store_dir,
                        fact,
                        &source,
                        &spool_path,
                    )
                    .await?;
                    let spool_write = storage
                        .seal_blob_to_spool(
                            &locator,
                            &authority,
                            protection,
                            source_path.path(),
                            &spool_path,
                        )
                        .await
                        .map_err(|error| move_materialization_error(fact, error.to_string()))?;
                    if spool_write == crate::storage::BlobSpoolWrite::Created {
                        files.created.push(spool_path.clone());
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
}

pub(crate) struct StagedAudienceBlobFiles {
    created: Vec<PathBuf>,
}

impl StagedAudienceBlobFiles {
    fn new() -> Self {
        Self {
            created: Vec::new(),
        }
    }

    async fn rollback(self) -> Result<(), DbError> {
        let mut failures = Vec::new();
        for path in self.created.into_iter().rev() {
            match crate::local_blob::remove_file(&path).await {
                Ok(true) => {
                    if let Err(error) = crate::local_blob::sync_parent_dir(&path).await {
                        failures.push(format!(
                            "sync parent after removing staged audience blob {}: {error}",
                            path.display()
                        ));
                    }
                }
                Ok(false) => failures.push(format!(
                    "staged audience blob disappeared before rollback: {}",
                    path.display()
                )),
                Err(error) => failures.push(format!(
                    "remove staged audience blob {}: {error}",
                    path.display()
                )),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Message(failures.join("; ")))
        }
    }
}

fn audience_moves_by_row(
    moves: &[AudienceMove],
) -> Result<BTreeMap<(String, String), &AudienceMove>, DbError> {
    let mut moved_rows = BTreeMap::new();
    for audience_move in moves {
        for row in &audience_move.rows {
            if let Some(prior) = moved_rows.insert(row.clone(), audience_move) {
                if prior.source != audience_move.source
                    || prior.destination != audience_move.destination
                {
                    return Err(DbError::Message(format!(
                        "row {}/{} belongs to conflicting audience moves",
                        row.0, row.1
                    )));
                }
            }
        }
    }
    Ok(moved_rows)
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

fn destination_protection(
    tx: &rusqlite::Transaction<'_>,
    storage: &dyn SyncStorage,
    destination: &crate::protocol::circle::Audience,
    partitions: &[AudiencePartition],
    fact: &StoreWriteBlobFact,
) -> Result<(RemoteAudience, BlobSpoolProtection), DbError> {
    match destination {
        crate::protocol::circle::Audience::Store => storage
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
            let (encryption, _) =
                StoreDatabase::circle_publication_context_on(tx, *circle_id, control.coordinate())?;
            Ok((
                RemoteAudience::Circle(*circle_id),
                BlobSpoolProtection::Opaque(encryption),
            ))
        }
        crate::protocol::circle::Audience::Local => unreachable!("Local handled before protection"),
    }
}

enum MoveSourcePlaintext {
    Existing(PathBuf),
    Downloaded(crate::local_blob::AtomicStagedFile),
}

impl MoveSourcePlaintext {
    fn path(&self) -> &Path {
        match self {
            Self::Existing(path) => path,
            Self::Downloaded(staged) => staged.path(),
        }
    }
}

async fn move_source_plaintext(
    tx: &rusqlite::Transaction<'_>,
    storage: &dyn SyncStorage,
    store_root: &crate::protocol::store_commit::StoreRootRef,
    store_dir: &StoreDir,
    fact: &StoreWriteBlobFact,
    source: &RowBlobAuthority,
    spool_path: &Path,
) -> Result<MoveSourcePlaintext, DbError> {
    if let Some(path) = local_source_path(tx, store_dir, fact, source)? {
        match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => {
                verify_plaintext_file(fact, &path).await?;
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
    let protection = crate::sync::store::blob::opening_protection_on(
        tx,
        storage,
        store_root,
        source,
        &previous.stored,
    )
    .map_err(|error| move_materialization_error(fact, error.to_string()))?;
    let destination = spool_path.with_extension("move-plaintext");
    let staged = storage
        .stage_verified_blob_plaintext(&previous.stored, protection, &destination)
        .await
        .map_err(|error| move_materialization_error(fact, error.to_string()))?;
    Ok(MoveSourcePlaintext::Downloaded(staged))
}

async fn stage_local_destination(
    tx: &rusqlite::Transaction<'_>,
    storage: &dyn SyncStorage,
    store_root: &crate::protocol::store_commit::StoreRootRef,
    store_dir: &StoreDir,
    fact: &StoreWriteBlobFact,
    source: &RowBlobAuthority,
    files: &mut StagedAudienceBlobFiles,
) -> Result<(), DbError> {
    let destination = match fact.blob.provenance {
        Provenance::HostProvided => store_dir
            .local_blob_path(&fact.blob.namespace, &fact.blob.id)
            .map_err(|error| move_materialization_error(fact, error.to_string()))?,
        Provenance::UserProvided => external_local_path_on(tx, fact)?.ok_or_else(|| {
            move_materialization_error(
                fact,
                "UserProvided Local destination has no registered file path",
            )
        })?,
    };
    match tokio::fs::metadata(&destination).await {
        Ok(metadata) if metadata.is_file() => {
            verify_plaintext_file(fact, &destination).await?;
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
    let protection = crate::sync::store::blob::opening_protection_on(
        tx,
        storage,
        store_root,
        source,
        &previous.stored,
    )
    .map_err(|error| move_materialization_error(fact, error.to_string()))?;
    let staged = storage
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
    files.created.push(destination.clone());
    Ok(())
}

fn local_source_path(
    tx: &rusqlite::Transaction<'_>,
    store_dir: &StoreDir,
    fact: &StoreWriteBlobFact,
    source: &RowBlobAuthority,
) -> Result<Option<PathBuf>, DbError> {
    match fact.blob.provenance {
        Provenance::HostProvided => store_dir
            .local_blob_path(&fact.blob.namespace, &fact.blob.id)
            .map(Some)
            .map_err(|error| move_materialization_error(fact, error.to_string())),
        Provenance::UserProvided if source == &RowBlobAuthority::Local => {
            external_local_path_on(tx, fact)
        }
        Provenance::UserProvided => Ok(fact.external_path.clone()),
    }
}

fn external_local_path_on(
    tx: &rusqlite::Transaction<'_>,
    fact: &StoreWriteBlobFact,
) -> Result<Option<PathBuf>, DbError> {
    let stored = tx
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
        move_materialization_error(fact, "registered external blob has a negative size")
    })?;
    if size != fact.plaintext_size || hash != fact.plaintext_hash.to_string() {
        return Err(move_materialization_error(
            fact,
            "registered external blob identity differs from the moved row",
        ));
    }
    Ok(Some(PathBuf::from(path)))
}

async fn verify_plaintext_file(fact: &StoreWriteBlobFact, path: &Path) -> Result<(), DbError> {
    let (size, hash) = crate::local_blob::exact_file_facts(path)
        .await
        .map_err(|error| move_materialization_error(fact, error))?;
    if size != fact.plaintext_size || hash != fact.plaintext_hash {
        return Err(move_materialization_error(
            fact,
            format!(
                "plaintext {} differs from declared size/hash",
                path.display()
            ),
        ));
    }
    Ok(())
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
