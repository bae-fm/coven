use std::collections::BTreeSet;

use rusqlite::OptionalExtension;

use super::store_device_state::{
    apply_store_device_exclusion_freezes_on, load_declared_store_device_state_on,
};
use super::{StoreDatabase, StoreDatabaseTransaction};
use crate::database::{
    install_store_founder_state_on, load_activated_registration_on, load_remote_object_on,
    record_activated_circle_acks_on, record_activated_store_ack_on,
    record_store_reclaim_activation_on, record_verified_stream_activations_on,
    required_store_root_authority_on, store_reclaim_journal_error, update_remote_object_on,
    Database, DbError, OwnedVerifiedMergeMaterialization, RetainedMergeMaterializationKey,
    RetainedPackageApplication, VerifiedMergeMaterialization,
};
use crate::sync::audience_package::AudiencePackage;
use crate::sync::remote_object::RemoteObjectRecord;
use crate::sync::storage::ExactObjectRef;
use crate::sync::store::circle_controls::activation::{
    VerifiedCircleActivations, VerifiedStreamActivations,
};
use crate::sync::store::ReclaimCommitActivation;
use crate::sync::store_commit::{
    CommitFrontier, ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreHistoryCut, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
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
                StoreDatabaseTransaction::new(&tx)
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
                StoreDatabaseTransaction::new(&tx).record_materialized_merge_commit(
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

impl StoreDatabaseTransaction<'_, '_> {
    pub(crate) fn record_materialized_merge_commit(
        &self,
        root: &crate::sync::store_commit::StoreRootRef,
        verified_commit: &VerifiedStoreBatchCommit,
        registrations: &[(
            StoreDeviceRegistration,
            crate::sync::store_commit::StoreDeviceRegistrationActivation,
        )],
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
        history_summary: &crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
        packages: &[AudiencePackage],
        package_application: Option<RetainedPackageApplication>,
    ) -> Result<(), DbError> {
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let device_operations = VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let circle_activations = VerifiedCircleActivations::none(commit, commit_ref)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let materialization = VerifiedMergeMaterialization::verify(
            root,
            verified_commit,
            registrations,
            &device_operations,
            &circle_activations,
            activation_head,
            activation_head_object,
            history_summary,
            None,
            packages,
            package_application,
        )?;
        self.record_verified_merge_materialization(materialization)?;
        Ok(())
    }

    pub(crate) fn record_verified_merge_materialization(
        &self,
        materialization: VerifiedMergeMaterialization<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let conn = self.transaction;
        self.record_author_exclusion_activations(
            materialization.verified_commit(),
            materialization.device_operations(),
            materialization.activation_head(),
            materialization.activation_head_object(),
        )?;
        let root = required_store_root_authority_on(conn)?;
        let state_after = self.derive_materialized_store_device_state(
            &root,
            materialization.commit(),
            materialization.device_operations(),
        )?;
        let expected_post_state = crate::sync::store_commit::StoreDeviceStateRef::from_resolved(
            CommitFrontier(
                materialization
                    .history_summary()
                    .frontier()
                    .map_err(|error| DbError::Message(error.to_string()))?,
            ),
            &state_after,
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        if materialization.history_summary().post_state != expected_post_state {
            return Err(DbError::Message(
                "retained Merge history summary differs from the derived post-commit device state"
                    .to_string(),
            ));
        }
        let (retained_commit_ref, retained) =
            crate::sync::store::database::StoreDatabase::retain_merge_materialization_on(
                conn,
                &root,
                &materialization,
            )?;
        StoreDatabase::record_circle_bootstrap_coverage_on(
            conn,
            &root,
            materialization.commit_ref(),
            materialization.circle_activations(),
        )?;
        let activation = ReclaimCommitActivation::new(
            materialization.commit_ref().clone(),
            crate::sync::store_commit::StoreDeviceHeadRef {
                head_hash: materialization.activation_head().head_hash(),
                object: materialization.activation_head_object().clone(),
            },
        )
        .map_err(store_reclaim_journal_error)?;
        self.record_materialized_commit_with_device_operations(
            materialization.verified_commit(),
            materialization.device_operations(),
            materialization.circle_activations().stream_activations(),
            &retained_commit_ref,
            &activation,
        )?;
        Ok(retained)
    }

    fn derive_materialized_store_device_state(
        &self,
        root: &crate::sync::store_commit::StoreRootRef,
        commit: &StoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
    ) -> Result<crate::sync::store_commit::ResolvedStoreDeviceState, DbError> {
        let conn = self.transaction;
        let mut device_state = load_declared_store_device_state_on(conn, &commit.device_state)?;
        let recovery_author = commit
            .device_registrations()
            .iter()
            .find_map(|activation| {
                if activation.registration != commit.author_registration {
                    return None;
                }
                let crate::sync::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                    node,
                    ..
                } = &activation.authority
                else {
                    return None;
                };
                Some((&activation.registration, node))
            })
            .map(|(registration_ref, node)| {
                let registration = load_activated_registration_on(conn, root, registration_ref)?;
                let crate::sync::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                    owner_grant,
                    ..
                } = registration.origin
                else {
                    return Err(DbError::Message(
                        "recovery activation author has a non-recovery registration origin"
                            .to_string(),
                    ));
                };
                Ok((
                    registration_ref.clone(),
                    crate::sync::store_commit::OwnerRecoveryCursor {
                        owner_grant,
                        position: crate::sync::store_commit::OwnerRecoveryPosition::At {
                            node: node.clone(),
                        },
                    },
                ))
            })
            .transpose()?;
        if let Some((registration, recovery)) = &recovery_author {
            device_state = device_state
                .activate_registration(registration.clone(), Some(recovery.clone()))
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        let active_author = device_state
            .devices
            .get(&commit.author_registration.device_id)
            .is_some_and(|record| {
                record.registration == commit.author_registration
                    && matches!(
                        record.status,
                        crate::sync::store_commit::StoreDeviceStatus::Active
                    )
            });
        if !active_author {
            return Err(DbError::Message(
                "materialized commit author is not active at its exact predecessor state".into(),
            ));
        }
        device_state = device_operations
            .apply_to(device_state, &commit.device_state)
            .map_err(|error| DbError::Message(error.to_string()))?;
        for activation in commit.device_registrations() {
            if recovery_author
                .as_ref()
                .is_some_and(|(registration, _)| registration == &activation.registration)
            {
                continue;
            }
            device_state = device_state
                .activate_registration(activation.registration.clone(), None)
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        let mut owner_recoveries = commit.stream_activations().iter().filter_map(|activation| {
            let crate::sync::store_commit::StreamActivation::GrantAuthorized {
                author_registration,
                grant_id,
                anchor: anchor @ crate::sync::store_commit::GrantStreamAnchor::OwnerRecovery { .. },
                ..
            } = activation
            else {
                return None;
            };
            Some((author_registration, grant_id, anchor))
        });
        let owner_recovery = owner_recoveries.next();
        if owner_recoveries.next().is_some() {
            return Err(DbError::Message(
                "materialized commit activates more than one Owner recovery stream".to_string(),
            ));
        }
        let owner_recovery = match owner_recovery {
            Some((registration, grant_id, anchor)) => {
                let registration = load_activated_registration_on(conn, root, registration)?;
                Some((
                    grant_id.clone(),
                    crate::sync::store_commit::OwnerRecoveryActivationId::derive(
                        root,
                        &registration.author_pubkey,
                        grant_id,
                        anchor,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?,
                ))
            }
            None => None,
        };
        if let Some((grant_id, activation)) = owner_recovery {
            device_state = device_state
                .activate_owner_recovery(grant_id, activation)
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        Ok(device_state)
    }

    fn record_materialized_commit_with_device_operations(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
        stream_activations: &VerifiedStreamActivations,
        retention: &RetainedMergeMaterializationKey,
        activation: &ReclaimCommitActivation,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let stored_registration: String = conn
            .query_row(
                "SELECT registration_object FROM store_device_registration_activations \
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (
                    commit.author_registration.device_id.to_string(),
                    commit.author_registration.registration_hash.to_string(),
                ),
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let stored_registration: StoreDeviceRegistrationRef =
            serde_json::from_str(&stored_registration).map_err(|error| {
                DbError::Message(format!("materialized author registration ref: {error}"))
            })?;
        if stored_registration != commit.author_registration {
            return Err(DbError::Message(
                "materialized commit author registration differs from its activation".to_string(),
            ));
        }
        let root = required_store_root_authority_on(conn)?;
        if root.store_root_hash != commit.store_root_hash {
            return Err(DbError::Message(
                "materialized commit belongs to a different Store root".to_string(),
            ));
        }
        let expected_stream =
            crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &commit.author_registration,
                crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        if commit_ref.coord.stream_id != expected_stream {
            return Err(DbError::Message(
                "materialization stream differs from its exact author registration".to_string(),
            ));
        }
        let stream_id = commit_ref.coord.stream_id.to_string();
        let sequence = commit_ref.coord.sequence;
        if sequence != commit.seq() {
            return Err(DbError::Message(
                "materialization coordinate differs from its signed commit".to_string(),
            ));
        }
        let predecessor = if commit.seq() == 1 {
            None
        } else if let Some(reference) =
            crate::sync::store::database::StoreDatabase::materialized_commit_ref_on(
                conn,
                &stream_id,
                commit.seq() - 1,
            )?
        {
            Some(reference)
        } else {
            conn.query_row(
                "SELECT commit_ref FROM snapshot_coverage \
                 WHERE device_id = ?1 AND seq = ?2",
                (
                    &stream_id,
                    Database::sequence_to_sqlite(&stream_id, commit.seq() - 1)?,
                ),
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|reference| {
                serde_json::from_str(&reference).map_err(|error| {
                    DbError::Message(format!("snapshot coverage exact commit ref: {error}"))
                })
            })
            .transpose()?
        };
        if predecessor.as_ref() != commit.order.predecessor() {
            return Err(DbError::Message(format!(
                "Store commit {}/{} names predecessor {:?}, durable predecessor is {:?}",
                stream_id,
                commit.seq(),
                commit.order.predecessor(),
                predecessor
            )));
        }
        let device_state =
            self.derive_materialized_store_device_state(&root, commit, device_operations)?;
        record_activated_store_ack_on(conn, commit, commit_ref)?;
        record_activated_circle_acks_on(conn, commit, commit_ref)?;
        let seq = Database::sequence_to_sqlite(&stream_id, commit.seq())?;
        let commit_ref_json = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::Message(format!("serialize exact Store commit ref: {error}"))
        })?;
        if retention.commit_ref != commit_ref_json {
            return Err(DbError::Message(
                "retained input names another exact commit".to_string(),
            ));
        }
        let retained_commit_ref = retention.commit_ref.as_str();
        let retained_input_hash = retention.input_hash.to_string();
        conn.execute(
            "INSERT INTO store_device_state_snapshots (commit_ref, state) VALUES (?1, ?2)",
            rusqlite::params![
                &commit_ref_json,
                serde_json::to_string(&device_state).map_err(|error| {
                    DbError::Message(format!(
                        "serialize materialized Store device state: {error}"
                    ))
                })?,
            ],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO materialized_commits
             (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                &stream_id,
                seq,
                &commit_ref_json,
                retained_commit_ref,
                retained_input_hash
            ],
        )
        .map_err(DbError::from)?;
        if stream_activations.as_slice() != commit.stream_activations() {
            return Err(DbError::Message(
                "verified stream activations differ from the materialized Store commit".to_string(),
            ));
        }
        if stream_activations.activating_commit() != commit_ref {
            return Err(DbError::Message(
                "verified stream activation commit differs from the materialized Store commit"
                    .to_string(),
            ));
        }
        record_verified_stream_activations_on(conn, stream_activations, &commit_ref_json)?;
        apply_store_device_exclusion_freezes_on(conn, &root, &device_state, device_operations)?;
        record_store_reclaim_activation_on(conn, commit, commit_ref, activation)
    }

    fn record_author_exclusion_activations(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let root = required_store_root_authority_on(conn)?;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        if commit.store_root_hash != root.store_root_hash
            || activation_head.author_registration != commit.author_registration
            || activation_head.commit != *commit_ref
        {
            return Err(DbError::Message(
                "author exclusion activation head differs from its exact commit authority"
                    .to_string(),
            ));
        }
        let author =
            load_activated_registration_on(conn, &root, &activation_head.author_registration)?;
        StoreDeviceHead::parse_at(
            &activation_head.to_bytes(),
            root.store_root_hash,
            &author,
            commit_ref,
        )
        .map_err(|error| {
            DbError::Message(format!("verify author exclusion activation head: {error}"))
        })?;
        activation_head_object
            .verify(&activation_head.to_bytes())
            .map_err(|error| {
                DbError::Message(format!(
                    "verify author exclusion activation head object: {error}"
                ))
            })?;
        let expected_head_key = format!(
            "{}.json",
            crate::sync::store_commit::head_slot_prefix(
                &activation_head.author_registration.device_id.to_string(),
                commit_ref.coord.sequence(),
            )
        );
        if activation_head_object.slot().logical_key() != expected_head_key {
            return Err(DbError::Message(
                "author exclusion activation head object occupies another protocol slot"
                    .to_string(),
            ));
        }
        let activation_head = crate::sync::store_commit::StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        };
        let activation_commit = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::Message(format!(
                "serialize author exclusion activation commit: {error}"
            ))
        })?;
        for (exclusion, accepted_cut) in device_operations.exclusions() {
            let StoreHistoryCut(accepted_cut) = accepted_cut;
            let exclusion_json = serde_json::to_string(exclusion).map_err(|error| {
                DbError::Message(format!("serialize author exclusion reference: {error}"))
            })?;
            let accepted_cut_json = serde_json::to_string(accepted_cut).map_err(|error| {
                DbError::Message(format!("serialize author exclusion accepted cut: {error}"))
            })?;
            let activation_head_json =
                serde_json::to_string(&activation_head).map_err(|error| {
                    DbError::Message(format!(
                        "serialize author exclusion activation head: {error}"
                    ))
                })?;
            let inserted = conn
                .execute(
                    "INSERT INTO store_author_exclusion_activations (
                         exclusion_ref, accepted_cut, activation_commit, activation_head
                     ) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(exclusion_ref) DO NOTHING",
                    (
                        &exclusion_json,
                        &accepted_cut_json,
                        &activation_commit,
                        &activation_head_json,
                    ),
                )
                .map_err(DbError::from)?;
            if inserted == 0 {
                let stored: (String, String, String) = conn
                    .query_row(
                        "SELECT accepted_cut, activation_commit, activation_head
                         FROM store_author_exclusion_activations
                         WHERE exclusion_ref = ?1",
                        [&exclusion_json],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(DbError::from)?;
                if stored
                    != (
                        accepted_cut_json,
                        activation_commit.clone(),
                        activation_head_json,
                    )
                {
                    return Err(DbError::Message(
                        "author exclusion already names different activation evidence".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn record_verified_circle_activations(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        if activations.len() != commit.circle_controls().len() {
            return Err(DbError::Message(
                "verified circle activations do not cover every control reference".to_string(),
            ));
        }
        let stream_id = commit_ref.coord.stream_id.to_string();
        let seq = Database::sequence_to_sqlite(&stream_id, commit_ref.coord.sequence())?;
        for activation in activations {
            if !commit.circle_controls().contains(&activation.reference)
                || activation.reference.circle_id() != activation.circle_id
                || activation.reference.control() != &activation.control.coord
                || !activation.control.verify()
            {
                return Err(DbError::Message(
                    "verified circle activation differs from Store control reference".to_string(),
                ));
            }
            let circle_id = activation.circle_id.to_string();
            if let Some(access) = &activation.local_access {
                let leaf = &access.leaf.value;
                if activation.control.value.author_pubkey != leaf.owner_pubkey {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} local access signer differs from its control author"
                    )));
                }
                match (&leaf.disposition, &access.active) {
                    (crate::sync::circle::CircleAccessDisposition::Active { .. }, Some(_))
                    | (crate::sync::circle::CircleAccessDisposition::Inactive, None) => {}
                    _ => {
                        return Err(DbError::Message(format!(
                            "circle {circle_id} access state differs from its disposition"
                        )));
                    }
                }
            }
            let mut statement = conn
                .prepare(
                    "SELECT control_bytes FROM circle_control_activations
                     WHERE circle_id = ?1",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([&circle_id], |row| row.get::<_, Vec<u8>>(0))
                .map_err(DbError::from)?;
            let mut existing_controls = Vec::new();
            for bytes in rows {
                let bytes = bytes.map_err(DbError::from)?;
                let control: crate::sync::circle::CircleControl = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        DbError::Message(format!("parse activated circle control: {error}"))
                    })?;
                existing_controls.push(control);
            }
            drop(statement);
            if activation.control.value.is_founder() {
                if !existing_controls.is_empty() {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} already has a founder control"
                    )));
                }
            } else {
                let covered = existing_controls
                    .iter()
                    .filter(|control| activation.control.value.causally_covers(control))
                    .collect::<Vec<_>>();
                let order = &activation.control.value.value.order;
                let expected_covered =
                    order.dependencies.len() + usize::from(order.previous_control_hash.is_some());
                if covered.len() != expected_covered
                    || covered.iter().any(|control| {
                        control
                            .owners()
                            .binary_search(&activation.control.value.author_pubkey)
                            .is_err()
                    })
                {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} control does not cover every authorized predecessor"
                    )));
                }
            }
            let current_state_payload = StoreDatabase::reduce_circle_current_state_on(
                conn,
                commit.candidate_family(),
                activation,
            )?;
            let control_coord =
                serde_json::to_string(&activation.control.coord).map_err(|error| {
                    DbError::Message(format!("serialize circle control coordinate: {error}"))
                })?;
            conn.execute(
                "INSERT INTO circle_control_activations
                 (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    &circle_id,
                    &control_coord,
                    stream_id,
                    seq,
                    commit.commit_hash().to_string(),
                    &activation.control.bytes,
                ],
            )
            .map_err(DbError::from)?;
            if let Some(access) = &activation.local_access {
                let disposition = match access.leaf.value.disposition {
                    crate::sync::circle::CircleAccessDisposition::Active { .. } => "active",
                    crate::sync::circle::CircleAccessDisposition::Inactive => "inactive",
                };
                conn.execute(
                    "INSERT INTO circle_access_cache
                     (circle_id, control_coord, owner_pubkey, disposition)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        &circle_id,
                        &control_coord,
                        &access.leaf.value.owner_pubkey,
                        disposition,
                    ],
                )
                .map_err(DbError::from)?;
            }
            conn.execute(
                "INSERT INTO circle_current_state (circle_id, state) VALUES (?1, ?2)
                 ON CONFLICT(circle_id) DO UPDATE SET state = excluded.state",
                rusqlite::params![&circle_id, current_state_payload],
            )
            .map_err(DbError::from)?;
            if StoreDatabase::circle_current_state_is_deleted_on(conn, activation.circle_id)? {
                conn.execute(
                    "DELETE FROM circle_access_cache WHERE circle_id = ?1",
                    [&circle_id],
                )
                .map_err(DbError::from)?;
            }
        }
        Ok(())
    }

    pub(crate) fn activate_store_operation_remote_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        object_ids: &[ObjectHash],
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let mut unique = std::collections::BTreeSet::new();
        for object_id in object_ids {
            if !unique.insert(*object_id) {
                return Err(DbError::Message(
                    "Store operation names a duplicate remote object".to_string(),
                ));
            }
            let remote = load_remote_object_on(conn, *object_id).map_err(|error| {
                DbError::Message(format!(
                    "load Store operation remote object {object_id} for activation: {error}"
                ))
            })?;
            let kind = match &remote {
                RemoteObjectRecord::CandidateCommit(_) => "candidate commit",
                RemoteObjectRecord::CandidateExclusive(_) => "candidate-exclusive object",
                RemoteObjectRecord::RetainedAuthority(_) => "retained authority",
                RemoteObjectRecord::SharedLiveSet(_) => "shared live-set object",
            };
            let remote = remote.into_activated(commit_ref).map_err(|error| {
                DbError::Message(format!(
                    "activate Store operation {kind} {object_id}: {error}"
                ))
            })?;
            update_remote_object_on(conn, *object_id, &remote)?;
        }
        Ok(())
    }
}
