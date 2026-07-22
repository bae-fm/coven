use super::*;

pub(crate) fn blocked_status(error: &StoreOutboundError) -> Option<crate::WriteBlock> {
    match error {
        StoreOutboundError::Database(_)
        | StoreOutboundError::BlobStorage { .. }
        | StoreOutboundError::Coordination(_)
        | StoreOutboundError::CandidateCleanup(_) => None,
        StoreOutboundError::MissingSerialCoordination => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::SerialControlConflict { .. }
        | StoreOutboundError::MergeAnnouncementOccupied { .. } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::SequenceExhausted { .. } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::AuthorExcluded { .. } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Object(StoreObjectError::Storage(_)) => None,
        StoreOutboundError::MissingBlob { namespace, id } => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::LocalUserBlob { namespace, id } => {
            Some(crate::WriteBlock::LocalUserBlob {
                namespace: namespace.clone(),
                id: id.clone(),
            })
        }
        StoreOutboundError::MissingState { key } => Some(crate::WriteBlock::InvalidProtocolState {
            reason: format!("Store protocol state {key:?} is absent"),
        }),
        StoreOutboundError::InvalidState { key, reason } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: format!("Store protocol state {key:?} is invalid: {reason}"),
            })
        }
        StoreOutboundError::InvalidOutbound(_) | StoreOutboundError::Object(_) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Preparation(super::service::SyncCycleError::LocalUserBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::LocalUserBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::Preparation(super::service::SyncCycleError::MissingPreparedBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::Preparation(super::service::SyncCycleError::Gate(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::AssetScan(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::Database(_)) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Preparation(super::service::SyncCycleError::AssetUpload(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::Storage { .. }) => None,
    }
}

pub(crate) async fn publish_prepared_remote_objects(
    db: &Database,
    storage: &dyn SyncStorage,
    write_id: &crate::WriteId,
    store_root_hash: ObjectHash,
) -> Result<(), StoreOutboundError> {
    for prepared in db.prepared_remote_objects(write_id).await? {
        let remote = prepared.record;
        let prepared_state = match &remote {
            super::remote_object::RemoteObjectRecord::CandidateCommit(record) => matches!(
                record.state,
                super::remote_object::CandidateCommitState::Prepared
            ),
            super::remote_object::RemoteObjectRecord::CandidateExclusive(record) => matches!(
                record.state,
                super::remote_object::CandidateObjectState::Prepared { .. }
            ),
            super::remote_object::RemoteObjectRecord::SharedLiveSet(record) => matches!(
                record.state,
                super::remote_object::OwnedObjectState::Prepared { .. }
            ),
            super::remote_object::RemoteObjectRecord::RetainedAuthority(_) => false,
        };
        match remote.bytes().stored() {
            super::remote_object::RemoteStoredRepresentation::Inline { bytes, object } => {
                let package = super::audience_package::AudiencePackage::parse(
                    remote.bytes().canonical_semantic_bytes(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let (stream_id, sequence) = match package.commit_coord() {
                    StoreCommitCoord::MergeConcurrent {
                        stream_id,
                        sequence,
                    } => (stream_id.to_string(), *sequence),
                    StoreCommitCoord::Serial { sequence } => {
                        (SERIAL_STREAM_ID.to_string(), *sequence)
                    }
                };
                let (context, prefix) = match package.audience() {
                    super::audience_package::PackageAudience::Store => (
                        ProtocolObjectContext::store_encrypted(
                            store_root_hash,
                            ProtocolObjectDomain::StorePackage,
                        ),
                        package_semantic_prefix(
                            package.candidate_family(),
                            &stream_id,
                            sequence,
                            ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                        ),
                    ),
                    super::audience_package::PackageAudience::Circle {
                        circle_id, control, ..
                    } => {
                        let (encryption, _) = db
                            .circle_publication_context(*circle_id, control.clone())
                            .await?;
                        (
                            ProtocolObjectContext::circle(
                                store_root_hash,
                                ProtocolObjectDomain::CirclePackage,
                                encryption,
                            ),
                            circle_package_semantic_prefix(
                                *circle_id,
                                package.candidate_family(),
                                &stream_id,
                                sequence,
                                ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                            ),
                        )
                    }
                };
                let prepared = PreparedExactObject::new(object.clone(), bytes.clone())
                    .map_err(StoreObjectError::from)?;
                if prepared_state {
                    storage
                        .create_protocol_object(&prepared)
                        .await
                        .map_err(StoreObjectError::from)?;
                }
                let opened = storage
                    .read_protocol_object(&context, object, &prefix)
                    .await
                    .map_err(StoreObjectError::from)?;
                if opened != remote.bytes().canonical_semantic_bytes() {
                    return Err(StoreOutboundError::InvalidOutbound(format!(
                        "remote package {} exact readback differs from its canonical bytes",
                        remote.object_id()
                    )));
                }
            }
            super::remote_object::RemoteStoredRepresentation::Blob { object } => {
                let locator = crate::blob::locator::BlobLocator::parse(
                    remote.bytes().canonical_semantic_bytes(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let uploader = locator.uploader().clone();
                let registration = db
                    .activated_store_device_registration(uploader.clone())
                    .await?;
                let authority = BlobWriteAuthority::new(&uploader, &registration)
                    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let blob = crate::blob::locator::StoredBlobRef::new(locator, object.clone())
                    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                if prepared_state {
                    let path = prepared.spool_path.as_deref().ok_or_else(|| {
                        StoreOutboundError::InvalidOutbound(format!(
                            "prepared blob {} awaiting upload has no local spool",
                            remote.object_id()
                        ))
                    })?;
                    storage
                        .create_blob_object_from_file(
                            &blob,
                            &authority,
                            path,
                            &crate::storage::cloud::no_progress(),
                        )
                        .await
                        .map_err(|source| StoreOutboundError::BlobStorage {
                            namespace: blob.locator().namespace().to_string(),
                            id: blob.locator().blob_id().to_string(),
                            source,
                        })?;
                }
                storage.verify_blob_object(&blob).await.map_err(|source| {
                    StoreOutboundError::BlobStorage {
                        namespace: blob.locator().namespace().to_string(),
                        id: blob.locator().blob_id().to_string(),
                        source,
                    }
                })?;
            }
            super::remote_object::RemoteStoredRepresentation::ExternalExact { .. } => {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "prepared outbound object {} has no locally stored representation",
                    remote.object_id()
                )));
            }
        }
        if prepared_state {
            db.mark_remote_object_uploaded(remote).await?;
        }
    }
    Ok(())
}

pub(crate) async fn required_store_root(db: &Database) -> Result<StoreRootRef, StoreOutboundError> {
    db.local_store_root_ref()
        .await?
        .ok_or(StoreOutboundError::MissingState {
            key: STORE_ROOT_AUTHORITY,
        })
}
