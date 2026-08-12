use super::close_prepared_packages;
use crate::sync::store::commit_publication::operation::commit_plan::{
    next_store_sequence, successor_store_sequence,
};
use crate::sync::store::StoreError;
use coven_database::{PreparedProtocolObject, PreparedStoreWrite, StoreWritePreparation};
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    commit_semantic_prefix, head_slot_prefix, CirclePackageInput, StoreCommitCoord,
    StoreCommitOperationsInput, StoreCommitOrder, StorePackageInput, SuccessorLink,
};

use super::AuthorizedWriterOperation;

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalBlobDropRequest {
    namespace: String,
    id: String,
    size: u64,
    plaintext_hash: coven_protocol::store_commit::ObjectHash,
    disposition: coven_protocol::blob::DeferredLocalBlobDisposition,
}

impl AuthorizedWriterOperation<'_> {
    pub(super) async fn prepare_store_write(&mut self) -> Result<bool, StoreError> {
        let database = self.database.clone();
        let Some(pending) = database.prepare_store_write().await? else {
            return Ok(false);
        };
        let stream_id = self.announcement_stream_id();
        let membership = self.membership.clone();
        let db = &database;
        let PreparedStoreWrite {
            write_id,
            base,
            blob_facts,
            partitions,
        } = pending;
        let dependencies =
            coven_protocol::store_commit::CommitFrontier::from_refs(base.dependencies)
                .map(|frontier| frontier.commits().clone())
                .map_err(StoreError::from)?;
        let preparation = async {
            let root = self.store_root().clone();
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
            let authorization = self
                .authorize_retained_outbound(&order, membership.head_refs())
                .await
                .map_err(StoreError::from)?;
            let membership_authority = self.membership_authority(&authorization.membership)?;
            let mut local_cleanup_by_blob = std::collections::BTreeMap::new();
            for fact in &blob_facts.blobs {
                if matches!(
                    fact.audience_move,
                    Some(coven_database::StoreWriteBlobMoveDestination::Local)
                ) || fact.blob.provenance != coven_protocol::blob::Provenance::HostProvided
                {
                    continue;
                }
                let present = self
                    .store_dir
                    .local_blob_path_if_present(
                        &fact.blob.namespace,
                        &fact.blob.id,
                        fact.plaintext_size,
                    )
                    .await
                    .map_err(|error| {
                        StoreError::Preparation(
                            crate::sync::store::StorePreparationError::AssetScanFile(error),
                        )
                    })?;
                if present.is_none() {
                    continue;
                }
                let disposition = match fact.blob.fill {
                    coven_protocol::blob::CacheFill::CacheEager => {
                        coven_protocol::blob::DeferredLocalBlobDisposition::Cache
                    }
                    coven_protocol::blob::CacheFill::CacheLazy => {
                        coven_protocol::blob::DeferredLocalBlobDisposition::Drop
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
                self.writer
                    .candidate_family_id(store_root_hash, &write_id, &order);
            let mut prepared_packages = Vec::new();
            if let Some(partition) = partitions.store {
                prepared_packages.push(
                    self.prepare_partition_package(
                        candidate_family,
                        &write_id,
                        &coord,
                        db.schema_version(),
                        stream_id.to_string(),
                        seq,
                        partition,
                        &blob_facts,
                        &active_store_members,
                    )
                    .await?,
                );
            }
            for partition in partitions.circles {
                prepared_packages.push(
                    self.prepare_partition_package(
                        candidate_family,
                        &write_id,
                        &coord,
                        db.schema_version(),
                        stream_id.to_string(),
                        seq,
                        partition,
                        &blob_facts,
                        &active_store_members,
                    )
                    .await?,
                );
            }
            let storage = self.storage.as_ref();
            let commit_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            );
            let head_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let device_id = self.local_device_id().to_string();
            let head_prefix = head_slot_prefix(&device_id, seq);
            let next_head_prefix = head_slot_prefix(&device_id, successor_store_sequence(seq)?);
            let next_head_slot = storage
                .allocate_protocol_slot(&head_context, &next_head_prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;

            let store_package = prepared_packages
                .iter()
                .find(|package| package.audience == coven_protocol::circle::Audience::Store)
                .map(|package| StorePackageInput {
                    candidate_family,
                    schema_version: db.schema_version(),
                    bytes: package.semantic_bytes.as_slice(),
                    object: package.prepared.reference().clone(),
                });
            let circle_packages = prepared_packages
                .iter()
                .filter_map(|package| {
                    let coven_protocol::circle::Audience::Circle(circle_id) = package.audience
                    else {
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
            let commit = self
                .writer
                .sign_store_write_commit(
                    store_root_hash,
                    write_id.clone(),
                    coord.clone(),
                    order,
                    membership_state,
                    device_state,
                    membership_authority,
                    StoreCommitOperationsInput {
                        store_package,
                        circle_packages: &circle_packages,
                        ..StoreCommitOperationsInput::empty()
                    },
                )
                .map_err(StoreError::from)?;
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
            let commit = self
                .writer
                .verify_prepared_commit(
                    &commit.to_bytes(),
                    store_root_hash,
                    coord,
                    commit_prepared.reference().clone(),
                )
                .map_err(StoreError::from)?;
            let commit_ref = commit.reference().clone();
            let successor = self
            .prepare_merge_history_successor(
                &commit,
                &authorization.membership,
                None,
                authorization.device_state.clone(),
                crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence::none(),
            )
            .await
            .map_err(StoreError::from)?;
            let storage = self.storage.as_ref();
            let activation = self
                .writer
                .announcement_activation_id()
                .map_err(StoreError::from)?;
            let head = self.writer.sign_device_head(
                store_root_hash,
                commit_ref.clone(),
                SuccessorLink {
                    activation,
                    predecessor: successor.predecessor_head.map(|reference| reference.object),
                    next_slot: next_head_slot,
                },
            )?;
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
                history_evidence: successor.history_evidence,
                local_cleanup,
                completion: coven_database::StoreBatchCompletion {},
            })
        }
        .await;
        let preparation = match preparation {
            Ok(preparation) => preparation,
            Err(error) => {
                if let Some(block) = error.write_block() {
                    if let Err(status) = database.block_write_if_unresolved(&write_id, block).await
                    {
                        return Err(StoreError::WriteBlockNotRecorded {
                            write_id: write_id.clone(),
                            preparation: Box::new(error),
                            status,
                        });
                    }
                }
                return Err(error);
            }
        };
        database.prepare_store_write_commit(preparation).await?;
        Ok(true)
    }
}

fn bind_local_cleanup(
    requests: Vec<LocalBlobDropRequest>,
    blobs: &[coven_database::PreparedAudienceBlob],
) -> Result<coven_database::StoreBatchLocalCleanup, crate::sync::store::StorePreparationError> {
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
        drops.push(coven_protocol::blob::DeferredLocalBlobDrop {
            namespace: request.namespace,
            id: request.id,
            size: request.size,
            plaintext_hash: request.plaintext_hash,
            locator_hash,
            disposition: request.disposition,
        });
    }
    Ok(coven_database::StoreBatchLocalCleanup { drops })
}
