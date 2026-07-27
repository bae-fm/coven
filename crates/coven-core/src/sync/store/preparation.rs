use super::database::StoreDatabase;
use super::*;
use crate::database::{PreparedProtocolObject, PreparedStoreWrite};
use crate::sync::membership::MembershipChain;
use crate::sync::storage::{BlobWriteAuthority, ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store::database::publication_state::StoreWritePreparation;
use crate::sync::store::package_preparation::{close_prepared_packages, prepare_partition_package};
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, CandidateFamilyId, CirclePackageInput,
    StoreBatchCommit, StoreCommitCoord, StoreCommitOperationsInput, StoreCommitOrder,
    StoreDeviceHead, StorePackageInput, SuccessorLink,
};
use crate::sync::store_objects::StoreObjectError;

use super::StoreError;

use super::operations::{
    blocked_status, load_local_store_authority, next_store_sequence, successor_store_sequence,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_store_write(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    _timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: &MembershipChain,
) -> Result<bool, StoreError> {
    let db = database.sqlite();
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
    let dependencies = crate::sync::store_commit::CommitFrontier::from_refs(base.dependencies)
        .map(|frontier| frontier.commits().clone())
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let preparation = async {
        let (root, registration_ref, registration, device_signer) =
            load_local_store_authority(database, device_id, keypair).await?;
        let blob_write_authority = BlobWriteAuthority::new(&registration_ref, &registration)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let store_root_hash = root.store_root_hash;
        let stream_id = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &registration_ref,
            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let previous = database.latest_local_store_position().await?;
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
        let candidate_membership = membership;
        let authorization = super::pull::load_retained_merge_outbound_authorization(
            database,
            storage,
            &root,
            &order,
            candidate_membership.head_refs(),
            &registration_ref,
        )
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let payload = crate::sync::service::prepare_store_payload(
            &blob_facts,
            keypair,
            store_dir,
            &authorization.membership,
        )
        .await
        .map_err(StoreError::Preparation)?;
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
                prepare_partition_package(
                    database,
                    storage,
                    store_root_hash,
                    candidate_family,
                    &write_id,
                    &coord,
                    db.schema_version(),
                    stream_id.to_string(),
                    seq,
                    partition,
                    &blob_facts,
                    &blob_write_authority,
                    store_dir,
                    &active_store_members,
                )
                .await?,
            );
        }
        for partition in partitions.circles {
            prepared_packages.push(
                prepare_partition_package(
                    database,
                    storage,
                    store_root_hash,
                    candidate_family,
                    &write_id,
                    &coord,
                    db.schema_version(),
                    stream_id.to_string(),
                    seq,
                    partition,
                    &blob_facts,
                    &blob_write_authority,
                    store_dir,
                    &active_store_members,
                )
                .await?,
            );
        }
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
            .find(|package| package.audience == crate::sync::circle::Audience::Store)
            .map(|package| StorePackageInput {
                candidate_family,
                schema_version: db.schema_version(),
                bytes: package.semantic_bytes.as_slice(),
                object: package.prepared.reference().clone(),
            });
        let circle_packages = prepared_packages
            .iter()
            .filter_map(|package| {
                let crate::sync::circle::Audience::Circle(circle_id) = package.audience else {
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
            payload.membership_authority,
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
        let commit = crate::sync::store_commit::VerifiedStoreBatchCommit::parse_prepared(
            &commit.to_bytes(),
            store_root_hash,
            coord,
            commit_prepared.reference().clone(),
            &registration,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let commit_ref = commit.reference().clone();
        let successor = super::pull::prepare_merge_history_successor(
            database,
            &root,
            commit.value(),
            &commit_ref,
            &authorization.membership,
            &registration,
            None,
            authorization.device_state.clone(),
            super::pull::MergeHistorySuccessorEvidence::none(),
        )
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let activation = registration
            .store_announcement_activation(&registration_ref)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            .activation_id();
        let head = StoreDeviceHead::signed(
            store_root_hash,
            registration_ref,
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
        let local_cleanup = crate::sync::service::bind_local_cleanup(
            payload.local_cleanup,
            &audience_objects.blobs,
        )
        .map_err(StoreError::Preparation)?;
        Ok::<_, StoreError>(StoreWritePreparation {
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
            completion: payload.completion,
        })
    }
    .await;
    let preparation = match preparation {
        Ok(preparation) => preparation,
        Err(error) => {
            record_preparation_failure(database, &write_id, &error).await?;
            return Err(error);
        }
    };
    database.prepare_store_write_commit(preparation).await?;
    Ok(true)
}

async fn record_preparation_failure(
    database: &StoreDatabase,
    write_id: &crate::WriteId,
    error: &StoreError,
) -> Result<(), StoreError> {
    let Some(block) = blocked_status(error) else {
        return Ok(());
    };
    database
        .block_write_if_unresolved(write_id, block)
        .await
        .map(|_| ())
        .map_err(|status_error| {
            StoreError::Database(format!(
                "record blocked status for write {write_id} after {error}: {status_error}"
            ))
        })
}
