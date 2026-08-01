use super::blob_preparation::close_prepared_packages;
use crate::database::{PreparedProtocolObject, PreparedStoreWrite, StoreWritePreparation};
use crate::protocol::store_commit::{
    commit_semantic_prefix, head_slot_prefix, CandidateFamilyId, CirclePackageInput,
    StoreBatchCommit, StoreCommitCoord, StoreCommitOperationsInput, StoreCommitOrder,
    StoreDeviceHead, StorePackageInput, SuccessorLink,
};
use crate::storage::StoreObjectError;
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store::operations::{
    blocked_status, next_store_sequence, successor_store_sequence,
};
use crate::sync::store::StoreError;
use crate::StoreDir;

use super::AuthorizedWriterOperation;

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalBlobDropRequest {
    namespace: String,
    id: String,
    size: u64,
    plaintext_hash: crate::protocol::store_commit::ObjectHash,
    disposition: crate::sync::cycle::DeferredLocalBlobDisposition,
}

pub(super) async fn prepare_store_write(
    operation: &mut AuthorizedWriterOperation<'_>,
    store_dir: &StoreDir,
) -> Result<bool, StoreError> {
    let stream_id = operation.announcement_stream_id();
    let database = operation.database.clone();
    let membership = operation.membership.clone();
    let registration_ref = operation.writer.registration_ref.clone();
    let registration = operation.writer.registration.clone();
    let device_signer = operation.writer.device_signer.clone();
    let db = &database;
    let Some(PreparedStoreWrite {
        write_id,
        changeset,
        inverse_changeset,
        base,
        blob_facts,
        partitions,
    }) = database.prepare_store_write().await?
    else {
        return Ok(false);
    };
    if !changeset.is_empty() && inverse_changeset.is_empty() {
        return Err(StoreError::InvalidOutbound(
            "shared Store write has no inverse changeset".to_string(),
        ));
    }
    let dependencies = crate::protocol::store_commit::CommitFrontier::from_refs(base.dependencies)
        .map(|frontier| frontier.commits().clone())
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let preparation = async {
        let root = operation.store_root().clone();
        let store_root_hash = root.store_root_hash;
        let previous = database.latest_local_store_position(stream_id).await?;
        let seq = next_store_sequence(previous.as_ref())?;
        let coord = StoreCommitCoord {
            stream_id,
            sequence: seq,
        };
        let order = StoreCommitOrder {
            seq,
            predecessor: previous.clone(),
            dependencies,
        };
        let authorization = operation
            .authorize_retained_outbound(&order, membership.head_refs(), &registration_ref)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let membership_authority = operation.membership_authority(&authorization.membership)?;
        let mut local_cleanup_by_blob = std::collections::BTreeMap::new();
        for fact in &blob_facts.blobs {
            if matches!(
                fact.audience_move,
                Some(crate::database::StoreWriteBlobMoveDestination::Local)
            ) || fact.blob.provenance != crate::blob::Provenance::HostProvided
            {
                continue;
            }
            let present = store_dir
                .local_blob_path_if_present(
                    &fact.blob.namespace,
                    &fact.blob.id,
                    fact.plaintext_size,
                )
                .await
                .map_err(|error| {
                    StoreError::Preparation(crate::sync::store::StorePreparationError::AssetScan(
                        error.to_string(),
                    ))
                })?;
            if present.is_none() {
                continue;
            }
            let disposition = match fact.blob.fill {
                crate::blob::CacheFill::CacheEager => {
                    crate::sync::cycle::DeferredLocalBlobDisposition::Cache
                }
                crate::blob::CacheFill::CacheLazy => {
                    crate::sync::cycle::DeferredLocalBlobDisposition::Drop
                }
            };
            let drop = LocalBlobDropRequest {
                namespace: fact.blob.namespace.clone(),
                id: fact.blob.id.clone(),
                size: fact.plaintext_size,
                plaintext_hash: fact.plaintext_hash,
                disposition,
            };
            let key = (drop.namespace.clone(), drop.id.clone());
            if let Some(prior) = local_cleanup_by_blob.insert(key, drop.clone()) {
                if prior != drop {
                    return Err(StoreError::Preparation(
                        crate::sync::store::StorePreparationError::AssetScan(format!(
                            "captured Store write gives blob {}/{} conflicting local cleanup facts",
                            drop.namespace, drop.id,
                        )),
                    ));
                }
            }
        }
        let local_cleanup_requests = local_cleanup_by_blob.into_values().collect();
        let membership_state = authorization.membership_state;
        let device_state = authorization.device_state_ref;
        let active_store_members: std::collections::BTreeSet<String> = membership
            .current_members()
            .into_iter()
            .map(|(pubkey, _)| pubkey)
            .collect();
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &registration_ref, &write_id, &order);
        let mut prepared_packages = Vec::new();
        if let Some(partition) = partitions.store {
            prepared_packages.push(
                operation
                    .prepare_partition_package(
                        candidate_family,
                        &write_id,
                        &coord,
                        db.schema_version(),
                        stream_id.to_string(),
                        seq,
                        partition,
                        &blob_facts,
                        store_dir,
                        &active_store_members,
                    )
                    .await?,
            );
        }
        for partition in partitions.circles {
            prepared_packages.push(
                operation
                    .prepare_partition_package(
                        candidate_family,
                        &write_id,
                        &coord,
                        db.schema_version(),
                        stream_id.to_string(),
                        seq,
                        partition,
                        &blob_facts,
                        store_dir,
                        &active_store_members,
                    )
                    .await?,
            );
        }
        let storage = operation.storage.as_ref();
        let commit_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let head_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let device_id = registration_ref.device_id.to_string();
        let head_prefix = head_slot_prefix(&device_id, seq);
        let next_head_prefix = head_slot_prefix(&device_id, successor_store_sequence(seq)?);
        let next_head_slot = storage
            .allocate_protocol_slot(&head_context, &next_head_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;

        let store_package = prepared_packages
            .iter()
            .find(|package| package.audience == crate::protocol::circle::Audience::Store)
            .map(|package| StorePackageInput {
                candidate_family,
                schema_version: db.schema_version(),
                bytes: package.semantic_bytes.as_slice(),
                object: package.prepared.reference().clone(),
            });
        let circle_packages = prepared_packages
            .iter()
            .filter_map(|package| {
                let crate::protocol::circle::Audience::Circle(circle_id) = package.audience else {
                    return None;
                };
                let control = package
                    .control
                    .as_ref()
                    .expect("Circle partition carries exact control");
                Some(CirclePackageInput {
                    circle_id,
                    control: control.coordinate().clone(),
                    key_fingerprint: package
                        .key_fingerprint
                        .expect("Circle partition carries exact key fingerprint"),
                    package: StorePackageInput {
                        candidate_family,
                        schema_version: db.schema_version(),
                        bytes: package.semantic_bytes.as_slice(),
                        object: package.prepared.reference().clone(),
                    },
                })
            })
            .collect::<Vec<_>>();
        let commit = StoreBatchCommit::signed_operations(
            store_root_hash,
            write_id.clone(),
            coord.clone(),
            registration_ref.clone(),
            &registration,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package,
                circle_packages: &circle_packages,
            },
            &device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let commit_prefix = commit_semantic_prefix(
            commit.candidate_family(),
            &stream_id.to_string(),
            seq,
            commit.commit_hash(),
        );
        let commit_slot = storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let commit_prepared = storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                commit.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let commit = crate::protocol::store_commit::VerifiedStoreBatchCommit::parse_prepared(
            &commit.to_bytes(),
            store_root_hash,
            coord,
            commit_prepared.reference().clone(),
            &registration,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let commit_ref = commit.reference().clone();
        let successor = operation
            .prepare_merge_history_successor(
                &commit,
                &authorization.membership,
                None,
                authorization.device_state.clone(),
                crate::sync::store::owner::verified_history::MergeHistorySuccessorEvidence::none(),
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let storage = operation.storage.as_ref();
        let activation = registration
            .store_announcement_activation(&registration_ref)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            .activation_id();
        let head = StoreDeviceHead::signed(
            store_root_hash,
            registration_ref.clone(),
            commit_ref.clone(),
            successor.summary.digest(),
            SuccessorLink {
                activation,
                predecessor: successor.predecessor_head.map(|reference| reference.object),
                next_slot: next_head_slot,
            },
            &device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let head_prepared = storage
            .prepare_protocol_object(
                &head_context,
                successor.head_slot,
                &head_prefix,
                head.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let (remote_objects, audience_objects) =
            close_prepared_packages(prepared_packages, commit.value(), &commit_ref)?;
        let local_cleanup = bind_local_cleanup(local_cleanup_requests, &audience_objects.blobs)
            .map_err(StoreError::Preparation)?;
        Ok::<_, StoreError>(StoreWritePreparation {
            root,
            write_id: write_id.clone(),
            remote_objects,
            audiences: audience_objects,
            commit: PreparedProtocolObject {
                value: commit,
                prepared: commit_prepared,
            },
            head: PreparedProtocolObject {
                value: head,
                prepared: head_prepared,
            },
            history_summary: successor.summary,
            local_cleanup,
            completion: crate::database::StoreBatchCompletion {},
        })
    }
    .await;
    let preparation = match preparation {
        Ok(preparation) => preparation,
        Err(error) => {
            if let Some(block) = blocked_status(&error) {
                database
                    .block_write_if_unresolved(&write_id, block)
                    .await
                    .map_err(|status_error| {
                        StoreError::Database(format!(
                            "record blocked status for write {write_id} after {error}: {status_error}"
                        ))
                    })?;
            }
            return Err(error);
        }
    };
    database.prepare_store_write_commit(preparation).await?;
    Ok(true)
}

fn bind_local_cleanup(
    requests: Vec<LocalBlobDropRequest>,
    blobs: &[crate::database::PreparedAudienceBlob],
) -> Result<crate::database::StoreBatchLocalCleanup, crate::sync::store::StorePreparationError> {
    let mut drops = Vec::with_capacity(requests.len());
    for request in requests {
        let matching = blobs
            .iter()
            .filter(|prepared| {
                let locator = prepared.blob().locator();
                locator.namespace() == request.namespace
                    && locator.blob_id() == request.id
                    && locator.plaintext_size() == request.size
                    && locator.plaintext_hash() == request.plaintext_hash
            })
            .map(|prepared| prepared.blob().locator().locator_hash())
            .collect::<std::collections::BTreeSet<_>>();
        let Some(locator_hash) = matching.iter().copied().next() else {
            return Err(crate::sync::store::StorePreparationError::AssetScan(
                format!(
                    "published blob {}/{} has {} exact cleanup locator candidates",
                    request.namespace,
                    request.id,
                    matching.len()
                ),
            ));
        };
        if matching.len() != 1 {
            return Err(crate::sync::store::StorePreparationError::AssetScan(
                format!(
                    "published blob {}/{} has {} exact cleanup locator candidates",
                    request.namespace,
                    request.id,
                    matching.len()
                ),
            ));
        }
        drops.push(crate::sync::cycle::DeferredLocalBlobDrop {
            namespace: request.namespace,
            id: request.id,
            size: request.size,
            plaintext_hash: request.plaintext_hash,
            locator_hash,
            disposition: request.disposition,
        });
    }
    Ok(crate::database::StoreBatchLocalCleanup { drops })
}
