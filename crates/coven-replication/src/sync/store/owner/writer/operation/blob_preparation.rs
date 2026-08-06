use super::*;
use crate::sync::store::package_preparation::{PreparedPartitionBlob, PreparedPartitionPackage};
use coven_database::AudiencePartition;
use coven_database::{
    PreparedAudienceBlob, PreparedAudienceObjects, PreparedAudiencePackage, StoreWriteBlobFact,
    StoreWriteBlobFacts,
};
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{BlobWriteAuthority, ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    circle_package_semantic_prefix, package_semantic_prefix, CandidateFamilyId, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
};
use coven_protocol::{audience_package, circle, remote_object};

impl AuthorizedWriterOperation<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_partition_package(
        &self,
        candidate_family: CandidateFamilyId,
        write_id: &coven_protocol::write::WriteId,
        coord: &StoreCommitCoord,
        schema_version: u32,
        stream_id: String,
        seq: u64,
        partition: AudiencePartition,
        blob_facts: &StoreWriteBlobFacts,
        active_store_members: &std::collections::BTreeSet<String>,
    ) -> Result<PreparedPartitionPackage, StoreError> {
        let database = &self.database;
        let storage = self.storage.as_ref();
        let store_root_hash = self.store_root().store_root_hash;
        let authority = self.writer.blob_write_authority();
        let partition = if let circle::Audience::Circle(circle_id) = partition.audience {
            if let Some(blocked) = database
                .circle_publication_rotation_block(circle_id, active_store_members.clone())
                .await?
            {
                return Err(StoreError::CirclePublicationBlocked(blocked));
            }
            // Publish under the Circle's current active control. An epoch close
            // between capture and publication retires the control the write was
            // captured under; its rows belong to the successor epoch that is live now.
            let control = database.current_circle_partition_control(circle_id).await?;
            AudiencePartition {
                control: Some(control),
                ..partition
            }
        } else {
            partition
        };
        let blob_facts = partition_blob_facts(&partition.changeset, blob_facts)?;
        let (remote_audience, protection) = match partition.audience {
            circle::Audience::Store => (
                coven_protocol::blob::locator::RemoteAudience::Store,
                storage
                    .store_blob_protection()
                    .map_err(|source| StoreError::BlobStorage {
                        namespace: "store".to_string(),
                        id: "protection".to_string(),
                        source,
                    })?,
            ),
            circle::Audience::Circle(circle_id) => {
                let control = partition.control.as_ref().ok_or_else(|| {
                    StoreError::InvalidOutbound(format!(
                        "Circle partition {circle_id} has no exact control"
                    ))
                })?;
                let access = database
                    .circle_publication_context(circle_id, control.coordinate().clone())
                    .await?;
                (
                    coven_protocol::blob::locator::RemoteAudience::Circle(circle_id),
                    access.blob_protection(),
                )
            }
            circle::Audience::Local => {
                return Err(StoreError::InvalidOutbound(
                    "Local partition reached Store publication".to_string(),
                ));
            }
        };
        let mut prepared_blobs = Vec::with_capacity(blob_facts.len());
        let mut blob_bindings = Vec::with_capacity(blob_facts.len());
        for fact in blob_facts {
            let (binding, blob) = self
                .prepare_partition_blob(
                    fact,
                    remote_audience.clone(),
                    protection.clone(),
                    &authority,
                )
                .await?;
            blob_bindings.push(binding);
            prepared_blobs.push(blob);
        }
        let (package, context, semantic_prefix, key_fingerprint) = match partition.audience {
            circle::Audience::Store => {
                if partition.control.is_some() {
                    return Err(StoreError::InvalidOutbound(
                        "Store partition carries Circle control".to_string(),
                    ));
                }
                let package = audience_package::AudiencePackage::store(
                    store_root_hash,
                    candidate_family,
                    write_id.clone(),
                    coord.clone(),
                    schema_version,
                    partition.changeset.clone(),
                    blob_bindings,
                )
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                let bytes = package.to_bytes();
                let prefix = package_semantic_prefix(
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(&bytes),
                );
                (
                    package,
                    ProtocolObjectContext::store_encrypted(
                        store_root_hash,
                        ProtocolObjectDomain::StorePackage,
                    ),
                    prefix,
                    None,
                )
            }
            circle::Audience::Circle(circle_id) => {
                let control = partition.control.as_ref().ok_or_else(|| {
                    StoreError::InvalidOutbound(format!(
                        "Circle partition {circle_id} has no exact control"
                    ))
                })?;
                let access = database
                    .circle_publication_context(circle_id, control.coordinate().clone())
                    .await?;
                let key_fingerprint = access.key_fingerprint();
                let package = audience_package::AudiencePackage::circle(
                    store_root_hash,
                    candidate_family,
                    write_id.clone(),
                    coord.clone(),
                    schema_version,
                    circle_id,
                    control.coordinate().clone(),
                    key_fingerprint,
                    partition.changeset.clone(),
                    blob_bindings,
                )
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                let bytes = package.to_bytes();
                let prefix = circle_package_semantic_prefix(
                    circle_id,
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(&bytes),
                );
                (
                    package,
                    access.protocol_context(store_root_hash, ProtocolObjectDomain::CirclePackage),
                    prefix,
                    Some(key_fingerprint),
                )
            }
            circle::Audience::Local => {
                return Err(StoreError::InvalidOutbound(
                    "Local partition reached Store publication".to_string(),
                ));
            }
        };
        let semantic_bytes = package.to_bytes();
        let slot = storage
            .allocate_protocol_slot(&context, &semantic_prefix, ".pkg")
            .await
            .map_err(StoreObjectError::from)?;
        let prepared = storage
            .prepare_protocol_object(&context, slot, &semantic_prefix, semantic_bytes.clone())
            .map_err(StoreObjectError::from)?;
        Ok(PreparedPartitionPackage {
            audience: partition.audience,
            control: partition.control,
            key_fingerprint,
            semantic_bytes,
            prepared,
            blobs: prepared_blobs,
        })
    }

    pub(super) async fn prepare_partition_blob(
        &self,
        fact: &StoreWriteBlobFact,
        audience: coven_protocol::blob::locator::RemoteAudience,
        protection: coven_protocol::objects::BlobSpoolProtection,
        authority: &BlobWriteAuthority<'_>,
    ) -> Result<
        (
            audience_package::RowBlobLocatorBinding,
            PreparedPartitionBlob,
        ),
        StoreError,
    > {
        let storage = self.storage.as_ref();
        let locator =
            prepare_partition_blob_locator(fact, audience.clone(), &protection, authority)?;
        if let Some(audience_move) = &fact.audience_move {
            let coven_database::StoreWriteBlobMoveDestination::Remote {
                audience: staged_audience,
                locator: staged_locator,
                spool_path,
            } = audience_move
            else {
                return Err(StoreError::InvalidOutbound(format!(
                    "Local audience-move blob {}/{}/{} reached remote package preparation",
                    fact.table, fact.row_id, fact.column
                )));
            };
            if staged_audience != &audience || staged_locator != &locator {
                return Err(StoreError::InvalidOutbound(format!(
                    "audience-move blob {}/{}/{} destination differs from its durable spool",
                    fact.table, fact.row_id, fact.column
                )));
            }
            let expected_path = self
                .store_dir
                .outbound_blob_spool_path(locator.locator_hash());
            if spool_path != &expected_path {
                return Err(StoreError::InvalidOutbound(format!(
                    "audience-move blob {}/{}/{} durable spool has the wrong path",
                    fact.table, fact.row_id, fact.column
                )));
            }
            let slot = storage
                .allocate_blob_slot(&locator, authority)
                .await
                .map_err(|source| StoreError::BlobStorage {
                    namespace: fact.blob.namespace.clone(),
                    id: fact.blob.id.clone(),
                    source,
                })?;
            let stored = storage
                .prepare_blob_object(&locator, authority, slot, spool_path)
                .await
                .map_err(|source| StoreError::BlobStorage {
                    namespace: fact.blob.namespace.clone(),
                    id: fact.blob.id.clone(),
                    source,
                })?;
            let binding = audience_package::RowBlobLocatorBinding::new(
                fact.table.clone(),
                fact.row_id.clone(),
                fact.row_stamp.clone(),
                fact.column.clone(),
                stored.clone(),
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
            return Ok((
                binding,
                PreparedPartitionBlob {
                    audience,
                    stored,
                    spool_path: Some(spool_path.clone()),
                    uploaded_verified: false,
                },
            ));
        }
        let spool_path = self
            .store_dir
            .outbound_blob_spool_path(locator.locator_hash());
        if let Some(previous) = &fact.previous {
            if previous.stored.locator() == &locator {
                let binding = audience_package::RowBlobLocatorBinding::new(
                    fact.table.clone(),
                    fact.row_id.clone(),
                    fact.row_stamp.clone(),
                    fact.column.clone(),
                    previous.stored.clone(),
                )
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                return Ok((
                    binding,
                    PreparedPartitionBlob {
                        audience,
                        stored: previous.stored.clone(),
                        spool_path: None,
                        uploaded_verified: true,
                    },
                ));
            }
        }
        let host_path = match fact.blob.provenance {
            coven_protocol::blob::Provenance::HostProvided => Some(
                self.store_dir
                    .local_blob_path(&fact.blob.namespace, &fact.blob.id)
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            ),
            coven_protocol::blob::Provenance::UserProvided => None,
        };
        let source = if let Some(path) = &fact.external_path {
            if fact.blob.provenance != coven_protocol::blob::Provenance::UserProvided {
                return Err(StoreError::InvalidOutbound(format!(
                    "host-provided blob {}/{} carries an external path",
                    fact.blob.namespace, fact.blob.id
                )));
            }
            PartitionBlobSource::existing(path.clone())
        } else if let Some(path) = host_path {
            match tokio::fs::metadata(&path).await {
                Ok(metadata) if metadata.is_file() => PartitionBlobSource::existing(path),
                Ok(_) => {
                    return Err(StoreError::InvalidOutbound(format!(
                        "host blob source is not a file: {}",
                        path.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    PartitionBlobSource::temporary(
                        self.materialize_previous_blob(fact, locator.locator_hash())
                            .await?,
                    )
                }
                Err(error) => {
                    return Err(StoreError::InvalidOutbound(format!(
                        "inspect host blob source {}: {error}",
                        path.display()
                    )));
                }
            }
        } else {
            PartitionBlobSource::temporary(
                self.materialize_previous_blob(fact, locator.locator_hash())
                    .await?,
            )
        };
        let spool_write = match storage
            .seal_blob_to_spool(&locator, authority, protection, source.path(), &spool_path)
            .await
            .map_err(|source| StoreError::BlobStorage {
                namespace: fact.blob.namespace.clone(),
                id: fact.blob.id.clone(),
                source,
            }) {
            Ok(spool_write) => spool_write,
            Err(error) => {
                return Err(source.cleanup_failure(None, error).await);
            }
        };
        let prepared = async {
            source.retire_temporary().await?;
            let slot = storage
                .allocate_blob_slot(&locator, authority)
                .await
                .map_err(|source| StoreError::BlobStorage {
                    namespace: fact.blob.namespace.clone(),
                    id: fact.blob.id.clone(),
                    source,
                })?;
            let stored = storage
                .prepare_blob_object(&locator, authority, slot, &spool_path)
                .await
                .map_err(|source| StoreError::BlobStorage {
                    namespace: fact.blob.namespace.clone(),
                    id: fact.blob.id.clone(),
                    source,
                })?;
            let binding = audience_package::RowBlobLocatorBinding::new(
                fact.table.clone(),
                fact.row_id.clone(),
                fact.row_stamp.clone(),
                fact.column.clone(),
                stored.clone(),
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
            Ok::<_, StoreError>((binding, stored))
        }
        .await;
        let (binding, stored) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let created_spool = (spool_write
                    == coven_protocol::objects::BlobSpoolWrite::Created)
                    .then(|| PreparedBlobSpool::new(&spool_path));
                return Err(source.cleanup_failure(created_spool, error).await);
            }
        };
        Ok((
            binding,
            PreparedPartitionBlob {
                audience,
                stored,
                spool_path: Some(spool_path),
                uploaded_verified: false,
            },
        ))
    }

    async fn materialize_previous_blob(
        &self,
        fact: &StoreWriteBlobFact,
        destination_locator: ObjectHash,
    ) -> Result<std::path::PathBuf, StoreError> {
        let previous = fact
            .previous
            .as_ref()
            .ok_or_else(|| StoreError::MissingBlob {
                namespace: fact.blob.namespace.clone(),
                id: fact.blob.id.clone(),
            })?;
        let authority = coven_protocol::blob::RowBlobAuthority::Remote(previous.authority.clone());
        let destination = self
            .store_dir
            .storage_dir()
            .join("outbound-blobs")
            .join(format!(".plaintext-{destination_locator}"));
        let staged = self
            .stage_verified_blob_plaintext(&authority, &previous.stored, &destination)
            .await
            .map_err(|error| match error {
                crate::sync::BlobCacheError::Storage(source) => StoreError::BlobStorage {
                    namespace: fact.blob.namespace.clone(),
                    id: fact.blob.id.clone(),
                    source,
                },
                error => StoreError::InvalidOutbound(error.to_string()),
            })?;
        staged.commit().await.map_err(StoreError::InvalidOutbound)?;
        Ok(destination)
    }
}

fn partition_blob_facts<'a>(
    changeset: &[u8],
    facts: &'a StoreWriteBlobFacts,
) -> Result<Vec<&'a StoreWriteBlobFact>, StoreError> {
    let rows = coven_database::walk_changeset(changeset)
        .map_err(|error| {
            StoreError::InvalidOutbound(format!("read audience package blob rows: {error}"))
        })?
        .into_iter()
        .filter(|change| {
            matches!(
                change.op,
                coven_foundation::changeset::ChangeOp::Insert
                    | coven_foundation::changeset::ChangeOp::Update
            )
        })
        .map(|change| {
            change
                .pk()
                .map(|row_id| (change.table.clone(), row_id.to_string()))
                .ok_or_else(|| {
                    StoreError::InvalidOutbound(format!(
                        "audience package row in {:?} has no primary key",
                        change.table
                    ))
                })
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    Ok(facts
        .blobs
        .iter()
        .filter(|fact| rows.contains(&(fact.table.clone(), fact.row_id.clone())))
        .collect())
}

struct PartitionBlobSource {
    path: std::path::PathBuf,
    temporary: bool,
}

impl PartitionBlobSource {
    fn existing(path: std::path::PathBuf) -> Self {
        Self {
            path,
            temporary: false,
        }
    }

    fn temporary(path: std::path::PathBuf) -> Self {
        Self {
            path,
            temporary: true,
        }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    async fn retire_temporary(&self) -> Result<(), StoreError> {
        if !self.temporary {
            return Ok(());
        }
        remove_durable_file(&self.path, false)
            .await
            .map_err(StoreError::InvalidOutbound)
    }

    async fn cleanup_failure(
        &self,
        created_spool: Option<PreparedBlobSpool<'_>>,
        error: StoreError,
    ) -> StoreError {
        let mut failures = Vec::new();
        if let Err(cleanup_error) = self.retire_temporary().await {
            failures.push(cleanup_error.to_string());
        }
        if let Some(spool) = created_spool {
            if let Err(cleanup_error) = spool.rollback().await {
                failures.push(cleanup_error);
            }
        }
        if failures.is_empty() {
            error
        } else {
            StoreError::InvalidOutbound(format!(
                "blob preparation failed: {error}; cleanup failed: {}",
                failures.join("; ")
            ))
        }
    }
}

struct PreparedBlobSpool<'a> {
    path: &'a std::path::Path,
}

impl<'a> PreparedBlobSpool<'a> {
    fn new(path: &'a std::path::Path) -> Self {
        Self { path }
    }

    async fn rollback(self) -> Result<(), String> {
        remove_durable_file(self.path, true).await
    }
}

async fn remove_durable_file(path: &std::path::Path, require_present: bool) -> Result<(), String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !require_present => {
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!("prepared blob spool {} is absent", path.display()));
        }
        Err(error) => return Err(format!("remove prepared blob {}: {error}", path.display())),
    }
    coven_foundation::atomic_file::sync_parent_dir(path).await
}

pub(crate) fn prepare_partition_blob_locator(
    fact: &StoreWriteBlobFact,
    audience: coven_protocol::blob::locator::RemoteAudience,
    protection: &coven_protocol::objects::BlobSpoolProtection,
    authority: &BlobWriteAuthority<'_>,
) -> Result<coven_protocol::blob::locator::BlobLocator, StoreError> {
    match protection {
        coven_protocol::objects::BlobSpoolProtection::Opaque(encryption) => {
            coven_protocol::blob::locator::BlobLocator::opaque(
                fact.blob.namespace.clone(),
                fact.blob.id.clone(),
                authority.reference.clone(),
                audience,
                fact.blob.scope.clone(),
                encryption.seal_key_fingerprint(),
                fact.plaintext_size,
                fact.plaintext_hash,
            )
        }
        coven_protocol::objects::BlobSpoolProtection::Browsable => {
            if audience != coven_protocol::blob::locator::RemoteAudience::Store {
                return Err(StoreError::InvalidOutbound(
                    "Circle blob cannot use Browsable storage".to_string(),
                ));
            }
            coven_protocol::blob::locator::BlobLocator::browsable(
                fact.blob.namespace.clone(),
                fact.blob.id.clone(),
                authority.reference.clone(),
                fact.blob.cloud_path.clone().ok_or_else(|| {
                    StoreError::InvalidOutbound(format!(
                        "Browsable blob {}/{} has no readable path",
                        fact.blob.namespace, fact.blob.id
                    ))
                })?,
                fact.plaintext_size,
                fact.plaintext_hash,
            )
        }
    }
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
}

pub(crate) fn close_prepared_packages(
    packages: Vec<PreparedPartitionPackage>,
    commit: &StoreBatchCommit,
    owner: &StoreBatchCommitRef,
) -> Result<
    (
        Vec<remote_object::RemoteObjectRecord>,
        PreparedAudienceObjects,
    ),
    StoreError,
> {
    let mut materials = Vec::with_capacity(packages.len());
    let mut indexed_packages = Vec::with_capacity(packages.len());
    let mut prepared_blobs = Vec::new();
    for package in packages {
        let object = package.prepared.reference().clone();
        let remote_object_id = remote_object::remote_object_id(&object);
        indexed_packages.push(
            PreparedAudiencePackage::new(
                remote_object_id,
                package.semantic_bytes.clone(),
                package.prepared.stored_bytes().to_vec(),
                object.clone(),
            )
            .map_err(StoreError::from)?,
        );
        materials.push(remote_object::CandidateObjectMaterial {
            object,
            canonical_semantic_bytes: package.semantic_bytes,
            stored_bytes: package.prepared.stored_bytes().to_vec(),
        });
        prepared_blobs.extend(package.blobs);
    }
    let mut remote_objects = remote_object::CandidateObjectGraph::from_commit(commit)
        .and_then(|graph| graph.close(commit, owner, materials))
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let (blob_remotes, indexed_blobs) = close_prepared_blobs(prepared_blobs, owner)?;
    remote_objects.extend(blob_remotes);
    Ok((
        remote_objects,
        PreparedAudienceObjects {
            packages: indexed_packages,
            blobs: indexed_blobs,
        },
    ))
}

pub(super) fn close_prepared_blobs(
    blobs: Vec<PreparedPartitionBlob>,
    owner: &StoreBatchCommitRef,
) -> Result<
    (
        Vec<remote_object::RemoteObjectRecord>,
        Vec<PreparedAudienceBlob>,
    ),
    StoreError,
> {
    let mut exact_blobs = std::collections::BTreeMap::<
        (
            coven_protocol::blob::locator::RemoteAudience,
            coven_protocol::store_commit::ObjectHash,
        ),
        PreparedPartitionBlob,
    >::new();
    for blob in blobs {
        let object_id = remote_object::remote_object_id(blob.stored.object());
        let key = (blob.audience.clone(), object_id);
        if let Some(existing) = exact_blobs.get_mut(&key) {
            existing.merge_exact_duplicate(blob)?;
        } else {
            exact_blobs.insert(key, blob);
        }
    }
    let mut remote_objects = Vec::with_capacity(exact_blobs.len());
    let mut indexed_blobs = Vec::with_capacity(exact_blobs.len());
    for blob in exact_blobs.into_values() {
        let locator_hash = blob.stored.locator().locator_hash();
        let remote = remote_object::RemoteObjectRecord::candidate_owned_blob(
            &blob.stored,
            owner.clone(),
            blob.uploaded_verified,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let prepared = PreparedAudienceBlob::from_remote(
            blob.audience,
            &locator_hash.to_string(),
            remote.clone(),
            blob.spool_path,
        )?;
        indexed_blobs.push(prepared);
        remote_objects.push(remote);
    }
    Ok((remote_objects, indexed_blobs))
}

#[cfg(test)]
mod tests;
