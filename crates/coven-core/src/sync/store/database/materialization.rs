use std::collections::BTreeSet;

use super::{MergeMaterializationTransaction, StoreDatabase};
use crate::database::{
    install_store_founder_state_on, required_store_root_authority_on, DbError,
    VerifiedMergeMaterialization,
};
use crate::sync::storage::ExactObjectRef;
use crate::sync::store::circle_controls::activation::VerifiedCircleActivations;
use crate::sync::store_commit::{
    StoreDeviceHead, StoreDeviceRegistration, VerifiedStoreBatchCommit,
};

impl StoreDatabase {
    pub(crate) async fn install_device_join_bootstrap(
        &self,
        root: crate::sync::store_commit::StoreRootRef,
        plan: crate::sync::store::owner::pull::DeviceJoinBootstrapPlan,
    ) -> Result<(), DbError> {
        self.database.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let installed_root = required_store_root_authority_on(&tx)?;
            if installed_root != root || plan.founder.store_root != root {
                return Err(DbError::Message(
                    "device join bootstrap root differs from the installed exact root".to_string(),
                ));
            }
            install_store_founder_state_on(
                &tx,
                &root,
                &plan.founder_reference,
                &plan.founder,
                &plan.founder_bytes,
                &plan.genesis,
            )?;
            crate::database::set_protocol_state_on(
                &tx,
                crate::sync::store::membership::OWNER_PUBKEY_STATE_KEY,
                &plan.founder.author_pubkey,
            )?;
            plan.membership.install_on(&tx)?;

            let frontier = crate::sync::store::database::StoreDatabase::materialized_frontier_on(&tx, None)?;
            let mut represented = BTreeSet::new();
            for prepared in &plan.commits {
                let stream_id = prepared.reference.coord.stream_id.to_string();
                let sequence = prepared.reference.coord.sequence();
                if let Some(existing) =
                    crate::sync::store::database::StoreDatabase::materialized_commit_ref_on(&tx, &stream_id, sequence)?
                {
                    if existing != prepared.reference {
                        return Err(DbError::Message(format!(
                            "device join bootstrap conflicts at {stream_id}/{sequence}"
                        )));
                    }
                    represented.insert(prepared.reference.clone());
                    continue;
                }
                let encoded = serde_json::to_string(&prepared.reference).map_err(|error| {
                    DbError::Message(format!(
                        "serialize device join bootstrap commit ref: {error}"
                    ))
                })?;
                let has_snapshot_state = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM store_device_state_snapshots
                         WHERE commit_ref = ?1)",
                        [&encoded],
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(DbError::from)?;
                let covered = frontier
                    .get(&stream_id)
                    .is_some_and(|tip| sequence <= tip.coord.sequence());
                if has_snapshot_state && covered {
                    represented.insert(prepared.reference.clone());
                }
            }

            for prepared in &plan.commits {
                let commit = prepared.commit.value();
                if !represented.contains(&prepared.reference)
                    && (commit.store_package().is_some()
                        || !commit.circle_packages().is_empty())
                {
                    return Err(DbError::Message(format!(
                        "device join bootstrap cannot advance over unmaterialized row data at {}/{}",
                        prepared.reference.coord.stream_id,
                        prepared.reference.coord.sequence()
                    )));
                }
            }

            for prepared in plan.commits {
                if represented.contains(&prepared.reference) {
                    continue;
                }
                let stream_id = prepared.reference.coord.stream_id.to_string();
                if let Some(existing) = crate::sync::store::database::StoreDatabase::materialized_commit_ref_on(
                    &tx,
                    &stream_id,
                    prepared.reference.coord.sequence(),
                )? {
                    if existing != prepared.reference {
                        return Err(DbError::Message(format!(
                            "device join bootstrap conflicts at {stream_id}/{}",
                            prepared.reference.coord.sequence()
                        )));
                    }
                    continue;
                }
                let commit = prepared.commit.value();
                crate::sync::store::database::StoreDatabase::record_activated_store_device_registrations_on(
                    &tx,
                    commit,
                    &prepared.registrations,
                )?;
                let circle_activations =
                    VerifiedCircleActivations::none(commit, &prepared.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?;
                let activation = &prepared.activation;
                let materialization = VerifiedMergeMaterialization::verify(
                    &root,
                    &prepared.commit,
                    &prepared.registrations,
                    &prepared.device_operations,
                    &circle_activations,
                    &activation.head,
                    &activation.object,
                    &activation.history_summary,
                    None,
                    &[],
                    None,
                )?;
                MergeMaterializationTransaction::new(&tx)
                    .record_verified_merge_materialization(materialization)?;
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn complete_owner_recovery(
        &self,
        verified_commit: VerifiedStoreBatchCommit,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
        registration: StoreDeviceRegistration,
        authority: crate::sync::store_commit::StoreDeviceRegistrationActivation,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let root = required_store_root_authority_on(&tx)?;
                let registrations = vec![(registration, authority)];
                let commit = verified_commit.value();
                crate::sync::store::database::StoreDatabase::record_activated_store_device_registrations_on(
                    &tx,
                    commit,
                    &registrations,
                )?;
                MergeMaterializationTransaction::new(&tx).record_materialized_merge_commit(
                    &root,
                    &verified_commit,
                    &registrations,
                    &activation_head,
                    &activation_head_object,
                    &history_summary,
                    &[],
                    None,
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}
