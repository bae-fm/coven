use crate::database::store_device_state::apply_store_device_exclusion_freezes_on;
use crate::database::store_device_state::load_declared_store_device_state_on;

use crate::database::blob_records::load_activated_registration_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::update_remote_object_on;
use crate::database::store_ack_records::record_activated_store_ack_on;
use crate::database::store_reclaim_records::record_store_reclaim_activation_on;
use crate::database::store_reclaim_records::store_reclaim_journal_error;
use crate::database::stream_activation_records::record_verified_stream_activations_on;

use super::*;

impl Database {
    pub(crate) fn record_materialized_merge_commit_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::sync::store_commit::StoreRootRef,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
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
        let device_operations = VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let circle_activations = VerifiedCircleActivations::none(commit, commit_ref)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let materialization = VerifiedMergeMaterialization::verify(
            root,
            commit,
            commit_ref,
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
        Self::record_verified_merge_materialization_on(conn, materialization)
    }

    pub(crate) fn record_verified_merge_materialization_on(
        conn: &rusqlite::Transaction<'_>,
        materialization: VerifiedMergeMaterialization<'_>,
    ) -> Result<(), DbError> {
        Self::record_author_exclusion_activations_on(
            conn,
            materialization.commit(),
            materialization.commit_ref(),
            materialization.device_operations(),
            materialization.activation_head(),
            materialization.activation_head_object(),
        )?;
        let root = required_store_root_authority_on(conn)?;
        let state_after = Self::derive_materialized_store_device_state_on(
            conn,
            &root,
            materialization.commit(),
            materialization.device_operations(),
        )?;
        let expected_post_state = crate::sync::store_commit::StoreDeviceStateRef::merge_concurrent(
            CommitFrontier::MergeConcurrent(
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
        let retained_commit_ref = Self::retain_merge_materialization_on(conn, &materialization)?;
        let activation = ReclaimCommitActivation::merge_concurrent(
            materialization.commit_ref().clone(),
            crate::sync::store_commit::StoreDeviceHeadRef {
                head_hash: materialization.activation_head().head_hash(),
                object: materialization.activation_head_object().clone(),
            },
        )
        .map_err(store_reclaim_journal_error)?;
        Self::record_materialized_commit_with_device_operations_on(
            conn,
            materialization.commit(),
            materialization.commit_ref(),
            materialization.device_operations(),
            materialization.circle_activations().stream_activations(),
            MaterializedCommitRetention::MergeConcurrent(&retained_commit_ref),
            &activation,
        )
    }

    fn derive_materialized_store_device_state_on(
        conn: &Connection,
        root: &crate::sync::store_commit::StoreRootRef,
        commit: &StoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
    ) -> Result<crate::sync::store_commit::ResolvedStoreDeviceState, DbError> {
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
        for retirement in commit.device_retirements() {
            device_state = device_state
                .self_retire(retirement.clone())
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        enum OwnerRecoveryAuthority<'a> {
            Merge {
                registration: &'a StoreDeviceRegistrationRef,
                grant_id: &'a crate::sync::membership::MembershipGrantId,
                anchor: &'a crate::sync::store_commit::GrantStreamAnchor,
            },
            Serial {
                grant_id: &'a crate::sync::membership::MembershipGrantId,
                acceptance: &'a crate::sync::store_commit::OwnerPromotionAcceptance,
            },
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
            Some(OwnerRecoveryAuthority::Merge {
                registration: author_registration,
                grant_id,
                anchor,
            })
        });
        let mut owner_recovery = owner_recoveries.next();
        if owner_recoveries.next().is_some() {
            return Err(DbError::Message(
                "materialized commit activates more than one Owner recovery stream".to_string(),
            ));
        }
        if let Some(serial) =
            commit.control().and_then(|control| {
                let crate::sync::store_commit::StoreControl::SerialMembership { entry } = control
                else {
                    return None;
                };
                let crate::sync::membership::SerialMembershipChange::SetMember {
                    role:
                        crate::sync::membership::StoreMembershipRoleGrant::Owner {
                            recovery:
                                crate::sync::membership::OwnerRecoveryAnchorRef::Promotion {
                                    acceptance,
                                },
                        },
                    grant_id,
                    ..
                } = &entry.change
                else {
                    return None;
                };
                Some(OwnerRecoveryAuthority::Serial {
                    grant_id,
                    acceptance,
                })
            })
        {
            if owner_recovery.replace(serial).is_some() {
                return Err(DbError::Message(
                    "materialized commit mixes Merge and Serial Owner recovery activation"
                        .to_string(),
                ));
            }
        }
        let owner_recovery = match owner_recovery {
            Some(OwnerRecoveryAuthority::Merge {
                registration,
                grant_id,
                anchor,
            }) => {
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
            Some(OwnerRecoveryAuthority::Serial {
                grant_id,
                acceptance,
            }) => Some((
                grant_id.clone(),
                crate::sync::store_commit::OwnerRecoveryActivationId::derive(
                    root,
                    &acceptance.request.member_pubkey,
                    grant_id,
                    acceptance.anchors.recovery(),
                )
                .map_err(|error| DbError::Message(error.to_string()))?,
            )),
            None => None,
        };
        if let Some((grant_id, activation)) = owner_recovery {
            device_state = device_state
                .activate_owner_recovery(grant_id, activation)
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        Ok(device_state)
    }

    pub(super) fn record_materialized_commit_with_device_operations_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        device_operations: &VerifiedStoreDeviceOperations,
        stream_activations: &VerifiedStreamActivations,
        retention: MaterializedCommitRetention<'_>,
        activation: &ReclaimCommitActivation,
    ) -> Result<(), DbError> {
        commit_ref
            .verify_commit(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
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
        let (stream_id, sequence) = match commit_ref.coord {
            StoreCommitCoord::MergeConcurrent {
                stream_id,
                sequence,
            } if stream_id == expected_stream => (stream_id.to_string(), sequence),
            StoreCommitCoord::MergeConcurrent { .. } => {
                return Err(DbError::Message(
                    "Merge materialization stream differs from its exact author registration"
                        .to_string(),
                ));
            }
            StoreCommitCoord::Serial { sequence } => (SERIAL_STREAM_ID.to_string(), sequence),
        };
        if sequence != commit.seq() || commit_ref.coord.policy() != commit.policy() {
            return Err(DbError::Message(
                "materialization coordinate differs from its signed commit".to_string(),
            ));
        }
        let predecessor = if commit.seq() == 1 {
            None
        } else if let Some(reference) =
            Self::materialized_commit_ref_on(conn, &stream_id, commit.seq() - 1)?
        {
            Some(reference)
        } else {
            conn.query_row(
                "SELECT commit_ref FROM snapshot_coverage \
                 WHERE device_id = ?1 AND seq = ?2",
                (
                    &stream_id,
                    Self::sequence_to_sqlite(&stream_id, commit.seq() - 1)?,
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
        let device_state = Self::derive_materialized_store_device_state_on(
            conn,
            &root,
            commit,
            device_operations,
        )?;
        record_activated_store_ack_on(conn, commit, commit_ref)?;
        let seq = Self::sequence_to_sqlite(&stream_id, commit.seq())?;
        let commit_ref_json = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::Message(format!("serialize exact Store commit ref: {error}"))
        })?;
        let (retained_commit_ref, retained_input_hash) = match (commit.policy(), retention) {
            (
                WritePolicy::MergeConcurrent,
                MaterializedCommitRetention::MergeConcurrent(retained),
            ) if retained.commit_ref == commit_ref_json => (
                Some(retained.commit_ref.as_str()),
                Some(retained.input_hash.to_string()),
            ),
            (WritePolicy::Serial, MaterializedCommitRetention::Serial) => (None, None),
            (WritePolicy::MergeConcurrent, MaterializedCommitRetention::MergeConcurrent(_)) => {
                return Err(DbError::Message(
                    "retained Merge input names another exact commit".to_string(),
                ));
            }
            _ => {
                return Err(DbError::Message(
                    "materialized commit retention differs from write policy".to_string(),
                ));
            }
        };
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
        apply_store_device_exclusion_freezes_on(
            conn,
            &root,
            commit.policy(),
            &device_state,
            device_operations,
        )?;
        record_store_reclaim_activation_on(conn, commit, commit_ref, activation)
    }

    fn record_author_exclusion_activations_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        device_operations: &VerifiedStoreDeviceOperations,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
    ) -> Result<(), DbError> {
        let root = required_store_root_authority_on(conn)?;
        commit_ref
            .verify_commit(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
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
            let StoreHistoryCut::MergeConcurrent(accepted_cut) = accepted_cut else {
                return Err(DbError::Message(
                    "Merge exclusion carries a Serial accepted cut".to_string(),
                ));
            };
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

    pub(crate) fn record_verified_circle_activations_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        activations: &[crate::sync::circle_ops::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        if activations.len() != commit.circle_controls().len() {
            return Err(DbError::Message(
                "verified circle activations do not cover every control reference".to_string(),
            ));
        }
        commit_ref
            .verify_commit(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let stream_id = match &commit_ref.coord {
            StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
            StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
        };
        let seq = Self::sequence_to_sqlite(&stream_id, commit_ref.coord.sequence())?;
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
                let expected_covered = match &activation.control.value.value {
                    crate::sync::circle::CircleControlValue::MergeConcurrent { order, .. } => {
                        order.dependencies.len()
                            + usize::from(order.previous_control_hash.is_some())
                    }
                    crate::sync::circle::CircleControlValue::Serial { .. } => 1,
                };
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
                if matches!(
                    &activation.control.value.value,
                    crate::sync::circle::CircleControlValue::Serial { .. }
                ) && activation.control.value.ordinal()
                    != covered[0].ordinal().checked_add(1).ok_or_else(|| {
                        DbError::Message("circle control ordinal overflow".to_string())
                    })?
                {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} Serial control does not advance its predecessor generation"
                    )));
                }
            }
            let current_state_payload =
                Self::reduce_circle_current_state_on(conn, commit.candidate_family(), activation)?;
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
                let access_bytes = serde_json::to_vec(&access.leaf.value).map_err(|error| {
                    DbError::Message(format!("serialize verified circle access: {error}"))
                })?;
                conn.execute(
                    "INSERT INTO circle_access_cache
                     (circle_id, control_coord, owner_pubkey, disposition, access_bytes)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        &circle_id,
                        &control_coord,
                        &access.leaf.value.owner_pubkey,
                        disposition,
                        access_bytes,
                    ],
                )
                .map_err(DbError::from)?;
                if let Some(active) = &access.active {
                    let roster_bytes = serde_json::to_vec(&active.roster).map_err(|error| {
                        DbError::Message(format!("serialize activated circle roster: {error}"))
                    })?;
                    conn.execute(
                        "INSERT INTO circle_roster_cache (circle_id, control_coord, roster_bytes)
                         VALUES (?1, ?2, ?3)",
                        rusqlite::params![&circle_id, &control_coord, roster_bytes,],
                    )
                    .map_err(DbError::from)?;
                    let metadata_bytes = serde_json::to_vec(&active.metadata).map_err(|error| {
                        DbError::Message(format!("serialize activated circle metadata: {error}"))
                    })?;
                    conn.execute(
                        "INSERT INTO circle_metadata_cache
                         (circle_id, control_coord, metadata_bytes) VALUES (?1, ?2, ?3)",
                        rusqlite::params![&circle_id, &control_coord, metadata_bytes,],
                    )
                    .map_err(DbError::from)?;
                }
            }
            conn.execute(
                "INSERT INTO circle_current_state (circle_id, state) VALUES (?1, ?2)
                 ON CONFLICT(circle_id) DO UPDATE SET state = excluded.state",
                rusqlite::params![&circle_id, current_state_payload],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(crate) fn activate_store_operation_remote_objects_on(
        conn: &Connection,
        commit_ref: &StoreBatchCommitRef,
        object_ids: &[ObjectHash],
    ) -> Result<(), DbError> {
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

    pub(crate) fn record_materialized_serial_commit_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        authorization: &SerialAuthorizationState,
    ) -> Result<(), DbError> {
        let device_operations = VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let stream_activations = VerifiedStreamActivations::none(commit, commit_ref)
            .map_err(|error| DbError::Message(error.to_string()))?;
        Self::record_materialized_serial_commit_with_device_operations_on(
            conn,
            commit,
            commit_ref,
            authorization,
            &device_operations,
            &stream_activations,
        )
    }

    pub(crate) fn record_materialized_serial_commit_with_device_operations_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        authorization: &SerialAuthorizationState,
        device_operations: &VerifiedStoreDeviceOperations,
        stream_activations: &VerifiedStreamActivations,
    ) -> Result<(), DbError> {
        if commit.policy() != WritePolicy::Serial {
            return Err(DbError::Message(
                "Serial membership state cannot accompany a MergeConcurrent commit".to_string(),
            ));
        }
        Self::record_materialized_commit_with_device_operations_on(
            conn,
            commit,
            commit_ref,
            device_operations,
            stream_activations,
            MaterializedCommitRetention::Serial,
            &ReclaimCommitActivation::serial(commit_ref.clone())
                .map_err(store_reclaim_journal_error)?,
        )?;
        let membership = serde_json::to_string(&authorization.membership).map_err(|error| {
            DbError::Message(format!("serialize Serial membership state: {error}"))
        })?;
        let provider_admin =
            serde_json::to_string(&authorization.provider_admin).map_err(|error| {
                DbError::Message(format!(
                    "serialize Serial provider administrator state: {error}"
                ))
            })?;
        let wrapped_keys = serde_json::to_string(&authorization.active_wrapped_keys)
            .map_err(|error| DbError::Message(format!("serialize Serial wrapped keys: {error}")))?;
        conn.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (SERIAL_MEMBERSHIP_STATE_KEY, membership),
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (SERIAL_WRAPPED_KEYS_STATE_KEY, wrapped_keys),
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (SERIAL_PROVIDER_ADMIN_STATE_KEY, provider_admin),
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (
                SERIAL_KEY_GENERATION_STATE_KEY,
                authorization.key_generation.to_string(),
            ),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }
}
