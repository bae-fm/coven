use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_partition_package(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    candidate_family: CandidateFamilyId,
    write_id: &crate::WriteId,
    coord: &StoreCommitCoord,
    schema_version: u32,
    stream_id: String,
    seq: u64,
    partition: super::gate::AudiencePartition,
    blob_facts: &StoreWriteBlobFacts,
    authority: &BlobWriteAuthority<'_>,
    store_dir: &StoreDir,
) -> Result<PreparedPartitionPackage, StoreError> {
    let blob_facts = partition_blob_facts(&partition.changeset, blob_facts)?;
    let (remote_audience, protection) = match partition.audience {
        super::circle::Audience::Store => (
            crate::blob::locator::RemoteAudience::Store,
            storage
                .store_blob_protection()
                .map_err(|source| StoreError::BlobStorage {
                    namespace: "store".to_string(),
                    id: "protection".to_string(),
                    source,
                })?,
        ),
        super::circle::Audience::Circle(circle_id) => {
            let control = partition.control.as_ref().ok_or_else(|| {
                StoreError::InvalidOutbound(format!(
                    "Circle partition {circle_id} has no exact control"
                ))
            })?;
            let (encryption, _) = database
                .circle_publication_context(circle_id, control.coordinate().clone())
                .await?;
            (
                crate::blob::locator::RemoteAudience::Circle(circle_id),
                super::storage::BlobSpoolProtection::Opaque(encryption),
            )
        }
        super::circle::Audience::Local => {
            return Err(StoreError::InvalidOutbound(
                "Local partition reached Store publication".to_string(),
            ));
        }
    };
    let mut prepared_blobs = Vec::with_capacity(blob_facts.len());
    let mut blob_bindings = Vec::with_capacity(blob_facts.len());
    for fact in blob_facts {
        let (binding, blob) = prepare_partition_blob(
            database,
            storage,
            fact,
            remote_audience.clone(),
            protection.clone(),
            authority,
            store_dir,
        )
        .await?;
        blob_bindings.push(binding);
        prepared_blobs.push(blob);
    }
    let (package, context, semantic_prefix, key_fingerprint) = match partition.audience {
        super::circle::Audience::Store => {
            if partition.control.is_some() {
                return Err(StoreError::InvalidOutbound(
                    "Store partition carries Circle control".to_string(),
                ));
            }
            let package = super::audience_package::AudiencePackage::store(
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
        super::circle::Audience::Circle(circle_id) => {
            let control = partition.control.as_ref().ok_or_else(|| {
                StoreError::InvalidOutbound(format!(
                    "Circle partition {circle_id} has no exact control"
                ))
            })?;
            let (encryption, key_fingerprint) = database
                .circle_publication_context(circle_id, control.coordinate().clone())
                .await?;
            let package = super::audience_package::AudiencePackage::circle(
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
                ProtocolObjectContext::circle(
                    store_root_hash,
                    ProtocolObjectDomain::CirclePackage,
                    encryption,
                ),
                prefix,
                Some(key_fingerprint),
            )
        }
        super::circle::Audience::Local => {
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

fn partition_blob_facts<'a>(
    changeset: &[u8],
    facts: &'a StoreWriteBlobFacts,
) -> Result<Vec<&'a StoreWriteBlobFact>, StoreError> {
    let rows = crate::changeset::walk(changeset)
        .map_err(|error| {
            StoreError::InvalidOutbound(format!("read audience package blob rows: {error}"))
        })?
        .into_iter()
        .filter(|change| {
            matches!(
                change.op,
                crate::changeset::ChangeOp::Insert | crate::changeset::ChangeOp::Update
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

pub(crate) async fn prepare_partition_blob(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    fact: &StoreWriteBlobFact,
    audience: crate::blob::locator::RemoteAudience,
    protection: super::storage::BlobSpoolProtection,
    authority: &BlobWriteAuthority<'_>,
    store_dir: &StoreDir,
) -> Result<
    (
        super::audience_package::RowBlobLocatorBinding,
        PreparedPartitionBlob,
    ),
    StoreError,
> {
    let locator = prepare_partition_blob_locator(fact, audience.clone(), &protection, authority)?;
    if let Some(audience_move) = &fact.audience_move {
        let crate::database::StoreWriteBlobMoveDestination::Remote {
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
        let expected_path = store_dir.outbound_blob_spool_path(locator.locator_hash());
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
        let binding = super::audience_package::RowBlobLocatorBinding::new(
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
    let spool_path = store_dir.outbound_blob_spool_path(locator.locator_hash());
    if let Some(previous) = &fact.previous {
        if previous.stored.locator() == &locator {
            let binding = super::audience_package::RowBlobLocatorBinding::new(
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
        crate::blob::Provenance::HostProvided => Some(
            store_dir
                .local_blob_path(&fact.blob.namespace, &fact.blob.id)
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
        ),
        crate::blob::Provenance::UserProvided => None,
    };
    let mut temporary_plaintext = false;
    let source_path = if let Some(path) = &fact.external_path {
        if fact.blob.provenance != crate::blob::Provenance::UserProvided {
            return Err(StoreError::InvalidOutbound(format!(
                "host-provided blob {}/{} carries an external path",
                fact.blob.namespace, fact.blob.id
            )));
        }
        path.clone()
    } else if let Some(path) = host_path {
        match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => path,
            Ok(_) => {
                return Err(StoreError::InvalidOutbound(format!(
                    "host blob source is not a file: {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                temporary_plaintext = true;
                materialize_previous_blob(
                    database,
                    storage,
                    fact,
                    store_dir,
                    locator.locator_hash(),
                )
                .await?
            }
            Err(error) => {
                return Err(StoreError::InvalidOutbound(format!(
                    "inspect host blob source {}: {error}",
                    path.display()
                )));
            }
        }
    } else {
        temporary_plaintext = true;
        materialize_previous_blob(database, storage, fact, store_dir, locator.locator_hash())
            .await?
    };
    let spool_write = match storage
        .seal_blob_to_spool(&locator, authority, protection, &source_path, &spool_path)
        .await
        .map_err(|source| StoreError::BlobStorage {
            namespace: fact.blob.namespace.clone(),
            id: fact.blob.id.clone(),
            source,
        }) {
        Ok(spool_write) => spool_write,
        Err(error) => {
            return Err(cleanup_failed_partition_blob(
                &spool_path,
                temporary_plaintext.then_some(source_path.as_path()),
                false,
                error,
            )
            .await);
        }
    };
    let prepared = async {
        if temporary_plaintext {
            tokio::fs::remove_file(&source_path)
                .await
                .map_err(|error| {
                    StoreError::InvalidOutbound(format!(
                        "remove prepared plaintext {}: {error}",
                        source_path.display()
                    ))
                })?;
            crate::local_blob::sync_parent_dir(&source_path)
                .await
                .map_err(StoreError::InvalidOutbound)?;
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
            .prepare_blob_object(&locator, authority, slot, &spool_path)
            .await
            .map_err(|source| StoreError::BlobStorage {
                namespace: fact.blob.namespace.clone(),
                id: fact.blob.id.clone(),
                source,
            })?;
        let binding = super::audience_package::RowBlobLocatorBinding::new(
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
            return Err(cleanup_failed_partition_blob(
                &spool_path,
                temporary_plaintext.then_some(source_path.as_path()),
                spool_write == crate::sync::storage::BlobSpoolWrite::Created,
                error,
            )
            .await);
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

async fn cleanup_failed_partition_blob(
    spool_path: &std::path::Path,
    temporary_plaintext: Option<&std::path::Path>,
    spool_created_by_attempt: bool,
    error: StoreError,
) -> StoreError {
    let mut failures = Vec::new();
    if let Some(path) = temporary_plaintext {
        match crate::local_blob::remove_file(path).await {
            Ok(removed) => {
                if removed {
                    if let Err(sync_error) = crate::local_blob::sync_parent_dir(path).await {
                        failures.push(format!(
                            "sync removed temporary plaintext {}: {sync_error}",
                            path.display()
                        ));
                    }
                }
            }
            Err(cleanup_error) => failures.push(format!(
                "remove temporary plaintext {}: {cleanup_error}",
                path.display()
            )),
        }
    }
    if spool_created_by_attempt {
        match crate::local_blob::remove_file(spool_path).await {
            Ok(true) => {
                if let Err(sync_error) = crate::local_blob::sync_parent_dir(spool_path).await {
                    failures.push(format!(
                        "sync removed prepared blob spool {}: {sync_error}",
                        spool_path.display()
                    ));
                }
            }
            Ok(false) => failures.push(format!(
                "prepared blob spool {} is absent",
                spool_path.display()
            )),
            Err(cleanup_error) => failures.push(format!(
                "remove prepared blob spool {}: {cleanup_error}",
                spool_path.display()
            )),
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

pub(crate) fn prepare_partition_blob_locator(
    fact: &StoreWriteBlobFact,
    audience: crate::blob::locator::RemoteAudience,
    protection: &super::storage::BlobSpoolProtection,
    authority: &BlobWriteAuthority<'_>,
) -> Result<crate::blob::locator::BlobLocator, StoreError> {
    match protection {
        super::storage::BlobSpoolProtection::Opaque(encryption) => {
            crate::blob::locator::BlobLocator::opaque(
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
        super::storage::BlobSpoolProtection::Browsable => {
            if audience != crate::blob::locator::RemoteAudience::Store {
                return Err(StoreError::InvalidOutbound(
                    "Circle blob cannot use Browsable storage".to_string(),
                ));
            }
            crate::blob::locator::BlobLocator::browsable(
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

async fn materialize_previous_blob(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    fact: &StoreWriteBlobFact,
    store_dir: &StoreDir,
    destination_locator: ObjectHash,
) -> Result<std::path::PathBuf, StoreError> {
    let previous = fact
        .previous
        .as_ref()
        .ok_or_else(|| StoreError::MissingBlob {
            namespace: fact.blob.namespace.clone(),
            id: fact.blob.id.clone(),
        })?;
    let authority = crate::blob::RowBlobAuthority::Remote(previous.authority.clone());
    let protection = crate::sync::store::blob::opening_protection(
        database,
        storage,
        &authority,
        &previous.stored,
    )
    .await
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let destination = store_dir
        .storage_dir()
        .join("outbound-blobs")
        .join(format!(".plaintext-{destination_locator}"));
    let staged = storage
        .stage_verified_blob_plaintext(&previous.stored, protection, &destination)
        .await
        .map_err(|source| StoreError::BlobStorage {
            namespace: fact.blob.namespace.clone(),
            id: fact.blob.id.clone(),
            source,
        })?;
    staged.commit().await.map_err(StoreError::InvalidOutbound)?;
    Ok(destination)
}

pub(crate) fn close_prepared_packages(
    packages: Vec<PreparedPartitionPackage>,
    commit: &StoreBatchCommit,
    owner: &StoreBatchCommitRef,
) -> Result<
    (
        Vec<super::remote_object::RemoteObjectRecord>,
        PreparedAudienceObjects,
    ),
    StoreError,
> {
    let mut materials = Vec::with_capacity(packages.len());
    let mut indexed_packages = Vec::with_capacity(packages.len());
    let mut prepared_blobs = Vec::new();
    for package in packages {
        let object = package.prepared.reference().clone();
        let remote_object_id = super::remote_object::remote_object_id(&object);
        indexed_packages.push(
            PreparedAudiencePackage::new(
                remote_object_id,
                package.semantic_bytes.clone(),
                package.prepared.stored_bytes().to_vec(),
                object.clone(),
            )
            .map_err(StoreError::from)?,
        );
        materials.push(super::remote_object::CandidateObjectMaterial {
            object,
            canonical_semantic_bytes: package.semantic_bytes,
            stored_bytes: package.prepared.stored_bytes().to_vec(),
        });
        prepared_blobs.extend(package.blobs);
    }
    let mut remote_objects = super::remote_object::CandidateObjectGraph::from_commit(commit)
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
        Vec<super::remote_object::RemoteObjectRecord>,
        Vec<PreparedAudienceBlob>,
    ),
    StoreError,
> {
    let mut exact_blobs = std::collections::BTreeMap::new();
    for blob in blobs {
        let object_id = super::remote_object::remote_object_id(blob.stored.object());
        let key = (blob.audience.clone(), object_id);
        if let Some(existing) = exact_blobs.get_mut(&key) {
            merge_identical_prepared_blob(existing, blob)?;
        } else {
            exact_blobs.insert(key, blob);
        }
    }
    let mut remote_objects = Vec::with_capacity(exact_blobs.len());
    let mut indexed_blobs = Vec::with_capacity(exact_blobs.len());
    for blob in exact_blobs.into_values() {
        let locator_hash = blob.stored.locator().locator_hash();
        let remote = super::remote_object::RemoteObjectRecord::candidate_owned_blob(
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

fn merge_identical_prepared_blob(
    existing: &mut PreparedPartitionBlob,
    duplicate: PreparedPartitionBlob,
) -> Result<(), StoreError> {
    if existing.audience != duplicate.audience || existing.stored != duplicate.stored {
        return Err(StoreError::InvalidOutbound(format!(
            "prepared blob object {} has conflicting exact references",
            super::remote_object::remote_object_id(existing.stored.object())
        )));
    }
    existing.spool_path = match (&existing.spool_path, duplicate.spool_path) {
        (Some(left), Some(right)) if left != &right => {
            return Err(StoreError::InvalidOutbound(format!(
                "prepared blob object {} has conflicting spool paths",
                super::remote_object::remote_object_id(existing.stored.object())
            )));
        }
        (Some(left), _) => Some(left.clone()),
        (None, right) => right,
    };
    existing.uploaded_verified |= duplicate.uploaded_verified;
    if !existing.uploaded_verified && existing.spool_path.is_none() {
        return Err(StoreError::InvalidOutbound(format!(
            "prepared blob object {} awaiting upload has no local spool",
            super::remote_object::remote_object_id(existing.stored.object())
        )));
    }
    Ok(())
}
