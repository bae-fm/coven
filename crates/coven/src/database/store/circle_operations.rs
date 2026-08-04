use crate::database::*;
use crate::encryption::EncryptionService;
use crate::protocol::store_commit::StoreBatchCommitRef;
use rusqlite::{Connection, OptionalExtension};

use super::*;

/// The three states a Circle control's activating commit can be in when resolved
/// from the retained authority: not an activation at all, a known activation whose
/// materialization has been reclaimed, or a retained activation with its commit.
enum CircleActivationCommitLookup {
    Absent,
    Reclaimed { stream_id: String, sequence: u64 },
    Retained(StoreBatchCommitRef),
}

impl StoreDatabase {
    pub(crate) async fn get_circle_operations(
        &self,
    ) -> Result<Vec<crate::protocol::circle::CircleOperationInfo>, DbError> {
        self.connection
            .call(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT operation_id, circle_id, payload
                     FROM circle_operations
                     ORDER BY rowid",
                    )
                    .map_err(DbError::from)?;
                let operations = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    })
                    .map_err(DbError::from)?
                    .map(|row| {
                        let (operation_id, circle_id, payload) = row.map_err(DbError::from)?;
                        let journal =
                            parse_circle_operation_row(&operation_id, &circle_id, &payload)?;
                        Ok(crate::protocol::circle::CircleOperationInfo {
                            operation_id: journal.operation_id.clone(),
                            circle_id: journal.circle_id(),
                            kind: journal.kind(),
                            state: journal.state(),
                        })
                    })
                    .collect();
                operations
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn get_circles(
        &self,
        identity_pubkey: &str,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::protocol::circle::CircleInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call(move |conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT circle_id, state
                     FROM circle_current_state
                     ORDER BY circle_id",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(DbError::from)?;
                let mut circles = Vec::new();
                for row in rows {
                    let (stored_circle_id, state) = row.map_err(DbError::from)?;
                    let state = Self::parse_circle_current_state(&stored_circle_id, &state)?;
                    let circle_id = state.circle_id();
                    if state.is_deleted() {
                        // A deleted Circle must remain visible to the application
                        // as deleted rather than silently disappear from its UI.
                        circles
                            .push(crate::protocol::circle::CircleInfo::Deleted { id: circle_id });
                    } else if let Some(branches) = state.conflict_branches() {
                        // A forked Circle must be visible to the application as
                        // conflicted so an Owner can resolve it; omitting it
                        // would make the Circle silently disappear.
                        circles.push(crate::protocol::circle::CircleInfo::Conflicted {
                            id: circle_id,
                            branches,
                        });
                    } else if let Some((_current, access, roster, metadata)) = state.active() {
                        if access.recipient_pubkey != identity_pubkey {
                            return Err(DbError::Message(format!(
                                "active circle {circle_id} belongs to another local identity"
                            )));
                        }
                        let role =
                            roster
                                .members()
                                .get(&identity_pubkey)
                                .copied()
                                .ok_or_else(|| {
                                    DbError::Message(format!(
                                        "activated circle {circle_id} excludes the local identity"
                                    ))
                                })?;
                        circles.push(crate::protocol::circle::CircleInfo::Active {
                            id: circle_id,
                            name: metadata.name.clone(),
                            role,
                            rotation_required: state
                                .rotation_required(&active_store_members)
                                .is_some(),
                        });
                    }
                }
                Ok(circles)
            })
            .await
    }

    /// Every Circle the local identity can see, with its public derived state. A
    /// deleted or conflicted Circle stays visible (as `Deleted`/`ControlConflict`)
    /// rather than silently disappearing; an inactive Circle the identity holds no
    /// access to is `Inactive`.
    pub(crate) async fn circle_states(
        &self,
        identity_pubkey: &str,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::protocol::circle::Circle>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call(move |conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT circle_id, state
                     FROM circle_current_state
                     ORDER BY circle_id",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(DbError::from)?;
                let mut circles = Vec::new();
                for row in rows {
                    let (stored_circle_id, state) = row.map_err(DbError::from)?;
                    let state = Self::parse_circle_current_state(&stored_circle_id, &state)?;
                    let (name, role) = state.display(&identity_pubkey);
                    circles.push(crate::protocol::circle::Circle {
                        id: state.circle_id(),
                        name,
                        role,
                        state: state.derived_state(&active_store_members),
                    });
                }
                Ok(circles)
            })
            .await
    }

    pub(crate) async fn get_circle_members(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        identity_pubkey: &str,
        store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::protocol::circle::CircleMemberInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call(move |conn| {
                let state = Self::circle_current_state_on(conn, circle_id)?.ok_or_else(|| {
                    DbError::Message(format!("Circle {circle_id} has no current state"))
                })?;
                let Some((_current, access, roster, _metadata)) = state.active() else {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} has no active local state"
                    )));
                };
                if access.recipient_pubkey != identity_pubkey {
                    return Err(DbError::Message(format!(
                        "active Circle {circle_id} belongs to another local identity"
                    )));
                }
                Ok(roster
                    .members()
                    .into_iter()
                    .filter(|(pubkey, _)| store_members.contains(pubkey))
                    .map(|(pubkey, role)| crate::protocol::circle::CircleMemberInfo {
                        is_self: pubkey == identity_pubkey,
                        pubkey,
                        role,
                    })
                    .collect())
            })
            .await
    }

    pub(crate) async fn circle_authoring_context(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            crate::protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call(move |conn| {
                let state = Self::circle_current_state_on(conn, circle_id)?.ok_or_else(|| {
                    DbError::Message(format!("Circle {circle_id} has no current state"))
                })?;
                let authoring = state.authoring_state().ok_or_else(|| {
                    DbError::Message(format!("Circle {circle_id} has no active authoring state"))
                })?;
                if authoring.access.recipient_pubkey != identity_pubkey {
                    return Err(DbError::Message(format!(
                        "active Circle {circle_id} belongs to another local identity"
                    )));
                }
                let activated_commit = Self::circle_activation_commit_ref_on(
                    conn,
                    circle_id,
                    &authoring.control.coord,
                )?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle {circle_id} current control has no materialized activation"
                    ))
                })?;
                Ok((authoring, activated_commit))
            })
            .await
    }

    /// The authoring context a terminal deletion signs from, accepting any
    /// resolved state whose local device holds owner access — `Active` or
    /// `Closing`. Unlike `circle_authoring_context` (Active-only, for commands
    /// that publish a new active epoch), deletion supersedes an in-flight close,
    /// so it authors equally from a closing control's frozen epoch.
    pub(crate) async fn circle_delete_context(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            crate::protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call(move |conn| {
                let state = Self::circle_current_state_on(conn, circle_id)?.ok_or_else(|| {
                    DbError::Message(format!("Circle {circle_id} has no current state"))
                })?;
                let authoring = state.deletable_authoring_state().ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle {circle_id} has no resolved authoring state to delete"
                    ))
                })?;
                if authoring.access.recipient_pubkey != identity_pubkey {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} belongs to another local identity"
                    )));
                }
                let activated_commit = Self::circle_activation_commit_ref_on(
                    conn,
                    circle_id,
                    &authoring.control.coord,
                )?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle {circle_id} current control has no materialized activation"
                    ))
                })?;
                Ok((authoring, activated_commit))
            })
            .await
    }

    /// The retained conflicting branch coordinates for a Circle whose control
    /// history forked, in canonical order. `None` when the Circle has no
    /// current state or its control is resolved.
    pub(crate) async fn circle_control_conflict_branches(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<Vec<crate::protocol::circle::CircleControlCoord>>, DbError> {
        self.connection
            .call(move |conn| {
                Ok(Self::circle_current_state_on(conn, circle_id)?
                    .and_then(|state| state.conflict_branches()))
            })
            .await
    }

    /// Whether the Circle's control history has terminated in a deletion.
    pub(crate) async fn circle_is_deleted(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<bool, DbError> {
        self.connection
            .call(move |conn| {
                Ok(Self::circle_current_state_on(conn, circle_id)?
                    .is_some_and(|state| state.is_deleted()))
            })
            .await
    }

    /// The Circle's currently activated authoring control, or `None` when the
    /// Circle is not in an active local state. Its retained keyring resolves
    /// acknowledgements sealed under rotated-away epochs.
    pub(crate) async fn current_circle_control(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::protocol::circle::CircleControlCoord>, DbError> {
        self.connection
            .call(move |conn| {
                Ok(
                    Self::circle_current_state_on(conn, circle_id)?.and_then(|state| {
                        state
                            .authoring_state()
                            .map(|authoring| authoring.control.coord.clone())
                    }),
                )
            })
            .await
    }

    /// Whether one activated Circle control strictly covers another in the retained
    /// control lineage — `covering` is a proper successor of `covered`. Bootstrap
    /// reclamation uses this to prove a removed recipient lost authority under a
    /// successor control that supersedes its seed's control. `false` when the
    /// controls are equal or `covering` is not retained.
    pub(crate) async fn circle_control_covers_strictly(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        covering: &crate::protocol::circle::CircleControlCoord,
        covered: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        if covering == covered {
            return Ok(false);
        }
        let covering = covering.clone();
        let covered = covered.clone();
        self.connection
            .call(move |conn| {
                let Some(covering_reference) =
                    Self::verified_circle_activation_on(conn, &root, circle_id, &covering)?
                else {
                    return Ok(false);
                };
                Self::verified_circle_control_covers_on(
                    conn,
                    &root,
                    circle_id,
                    &covering_reference.control,
                    &covered,
                )
            })
            .await
    }

    pub(crate) async fn closing_circle_controls(
        &self,
    ) -> Result<Vec<crate::protocol::circle::PreparedCircleControl>, DbError> {
        self.connection
            .call(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT circle_id, state
                         FROM circle_current_state
                         ORDER BY circle_id",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(DbError::from)?;
                let mut controls = Vec::new();
                for row in rows {
                    let (circle_id, payload) = row.map_err(DbError::from)?;
                    let state = Self::parse_circle_current_state(&circle_id, &payload)?;
                    if let Some(control) = state.closing_control() {
                        controls.push(control.clone());
                    }
                }
                Ok(controls)
            })
            .await
    }

    pub(crate) async fn circle_closing_context(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            crate::protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call(move |conn| {
                let state = Self::circle_current_state_on(conn, circle_id)?.ok_or_else(|| {
                    DbError::Message(format!("Circle {circle_id} has no current state"))
                })?;
                let closing = state.closing_authoring_state().ok_or_else(|| {
                    DbError::Message(format!("Circle {circle_id} is not closing"))
                })?;
                if closing.access.recipient_pubkey != identity_pubkey {
                    return Err(DbError::Message(format!(
                        "closing Circle {circle_id} belongs to another local identity"
                    )));
                }
                let activated_commit =
                    Self::circle_activation_commit_ref_on(conn, circle_id, &closing.control.coord)?
                        .ok_or_else(|| {
                            DbError::Message(format!(
                                "Circle {circle_id} closing control has no materialized activation"
                            ))
                        })?;
                Ok((closing, activated_commit))
            })
            .await
    }

    pub(crate) async fn circle_publication_context(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        expected_control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<crate::protocol::circle_activation::CircleEpochAccess, DbError> {
        self.connection
            .call(move |conn| circle_publication_context_on(conn, circle_id, &expected_control))
            .await
    }

    /// The Circle's current active control coordinate. A durable write captured
    /// under an earlier control publishes under this one, so an epoch close that
    /// retired the capture-time control does not strand the write: its rows
    /// belong to whichever epoch is live when it publishes. Fails when the
    /// Circle is not currently active.
    pub(crate) async fn current_circle_partition_control(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<crate::database::CirclePartitionControl, DbError> {
        self.connection
            .call(move |conn| {
                crate::database::active_circle_control(conn, circle_id)
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .await
    }

    /// Whether publishing new content into `circle_id` is blocked because the
    /// Circle's resolved roster names Store identities that hold no active
    /// membership grant at `active_store_members`. Derived from the current
    /// materialized state, so activating a successor roster without those
    /// identities clears it with no stored flag to reset.
    pub(crate) async fn circle_publication_rotation_block(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Option<crate::protocol::circle::CirclePublicationBlocked>, DbError> {
        self.connection
            .call(move |conn| {
                let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                    return Ok(None);
                };
                Ok(state
                    .rotation_required(&active_store_members)
                    .map(|rotation| {
                        crate::protocol::circle::CirclePublicationBlocked::RotationRequired {
                            circle_id,
                            removed_members: rotation.removed_members,
                        }
                    }))
            })
            .await
    }
}

pub(super) fn circle_publication_context_on(
    conn: &Connection,
    circle_id: crate::protocol::circle::CircleId,
    expected_control: &crate::protocol::circle::CircleControlCoord,
) -> Result<crate::protocol::circle_activation::CircleEpochAccess, DbError> {
    // An exclusion blocks publication until this device's bootstrap coverage
    // records the exact successor commit that excluded it. The gate derives
    // clear from that coverage; no reset flag is mutated.
    let exclusion: Option<(String, String)> = conn
        .query_row(
            "SELECT close_id, activating_commit FROM circle_close_exclusions
                 WHERE circle_id = ?1",
            [circle_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    if let Some((close_id, activating_commit)) = exclusion {
        let coverage_commit: Option<String> = conn
            .query_row(
                "SELECT activation_commit FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)?;
        if coverage_commit.as_deref() != Some(activating_commit.as_str()) {
            let close_id = serde_json::from_str(&close_id).map_err(|error| {
                DbError::Message(format!("parse pending Circle close exclusion id: {error}"))
            })?;
            return Err(DbError::ExcludedDeviceMustReset {
                circle_id,
                close_id,
            });
        }
    }
    let state = StoreDatabase::circle_current_state_on(conn, circle_id)?
        .ok_or_else(|| DbError::Message(format!("Circle {circle_id} has no current state")))?;
    if state.is_deleted() {
        return Err(DbError::Message(format!("Circle {circle_id} is deleted")));
    }
    let access = state
        .epoch_access(expected_control)
        .map_err(|error| DbError::Message(error.to_string()))?
        .ok_or_else(|| {
            DbError::Message(format!("Circle {circle_id} has no active publication key"))
        })?;
    Ok(access)
}

impl StoreDatabase {
    /// Record this device's own exclusion from a Circle epoch close, derived from
    /// the verified successor outcome at materialization. The row is keyed by
    /// Circle: a later close for the same Circle supersedes it. It is never
    /// deleted — the publication gate derives clear once the successor bootstrap's
    /// coverage records.
    pub(crate) fn record_circle_close_exclusion_on(
        conn: &Connection,
        exclusion: &crate::protocol::circle_activation::LocalCircleExclusion,
    ) -> Result<(), DbError> {
        let circle_id = exclusion.circle_id.to_string();
        let close_id = serde_json::to_string(&exclusion.close_id)
            .map_err(|error| DbError::Message(format!("serialize close exclusion id: {error}")))?;
        let excluded = serde_json::to_string(&exclusion.excluded).map_err(|error| {
            DbError::Message(format!("serialize close exclusion registration: {error}"))
        })?;
        let successor_control =
            serde_json::to_string(&exclusion.successor_control).map_err(|error| {
                DbError::Message(format!("serialize close exclusion successor: {error}"))
            })?;
        let activating_commit =
            serde_json::to_string(&exclusion.activating_commit).map_err(|error| {
                DbError::Message(format!("serialize close exclusion activation: {error}"))
            })?;
        conn.execute(
            "INSERT INTO circle_close_exclusions
             (circle_id, close_id, excluded_registration, successor_control, activating_commit)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(circle_id) DO UPDATE SET
               close_id = excluded.close_id,
               excluded_registration = excluded.excluded_registration,
               successor_control = excluded.successor_control,
               activating_commit = excluded.activating_commit",
            rusqlite::params![
                circle_id,
                close_id,
                excluded,
                successor_control,
                activating_commit,
            ],
        )
        .map_err(DbError::from)?;
        Ok(())
    }

    pub(crate) async fn record_circle_close_exclusions(
        &self,
        exclusions: Vec<crate::protocol::circle_activation::LocalCircleExclusion>,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                for exclusion in &exclusions {
                    Self::record_circle_close_exclusion_on(&tx, exclusion)?;
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn circle_epoch_access(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        expected_control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<Option<crate::protocol::circle_activation::CircleEpochAccess>, DbError> {
        self.with_retained_merge_materializations(move |conn, retained| {
            retained.replay_inputs_on(conn, &root)?;
            let Some(activation) =
                retained.verified_circle_activation_on(conn, circle_id, &expected_control)?
            else {
                return Ok(None);
            };
            activation
                .epoch_access()
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await
    }

    pub(crate) async fn circle_historical_package_keyring(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        expected_control: crate::protocol::circle::CircleControlCoord,
        expected_key_fingerprint: crate::KeyFingerprint,
    ) -> Result<Option<String>, DbError> {
        self.connection
            .call(move |conn| {
                let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                    return Ok(None);
                };
                let Some(current) = state
                    .authoring_state()
                    .or_else(|| state.closing_authoring_state())
                else {
                    return Ok(None);
                };
                let Some(historical) =
                    Self::verified_circle_activation_on(conn, &root, circle_id, &expected_control)?
                else {
                    return Ok(None);
                };
                if !Self::verified_circle_control_covers_on(
                    conn,
                    &root,
                    circle_id,
                    &current.control,
                    &expected_control,
                )? || current.control.value.epoch_id() != historical.control.value.epoch_id()
                    || current.control.value.key_fingerprint() != expected_key_fingerprint
                    || historical.control.value.key_fingerprint() != expected_key_fingerprint
                {
                    return Ok(None);
                }
                let crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } =
                    &current.access.disposition
                else {
                    return Ok(None);
                };
                let parsed = crate::encryption::MasterKeyring::from_serialized(keyring).map_err(
                    |error| {
                        DbError::Message(format!(
                            "parse Circle {circle_id} historical package keyring: {error}"
                        ))
                    },
                )?;
                let encryption = EncryptionService::from(parsed);
                if encryption
                    .service_for_fingerprint(expected_key_fingerprint.as_bytes())
                    .is_err()
                {
                    return Ok(None);
                }
                Ok(Some(keyring.clone()))
            })
            .await
    }

    pub(crate) async fn verified_circle_activation_context(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<(
            crate::protocol::circle_activation::VerifiedCircleReference,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        self.connection
            .call(move |conn| {
                let Some(commit) =
                    Self::circle_activation_commit_ref_on(conn, circle_id, &control)?
                else {
                    return Ok(None);
                };
                let activation =
                    Self::verified_circle_activation_on(conn, &root, circle_id, &control)?
                        .ok_or_else(|| {
                            DbError::Message(format!(
                                "Circle {circle_id} activation context lost control {control:?}"
                            ))
                        })?;
                Ok(Some((activation, commit)))
            })
            .await
    }

    pub(crate) async fn circle_blob_opening_protection(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        expected_control: crate::protocol::circle::CircleControlCoord,
        expected_key_fingerprint: crate::KeyFingerprint,
    ) -> Result<crate::protocol::objects::BlobSpoolProtection, DbError> {
        self.connection
            .call(move |conn| {
                circle_blob_opening_protection_on(
                    conn,
                    &root,
                    circle_id,
                    &expected_control,
                    expected_key_fingerprint,
                )
            })
            .await
    }
}

pub(super) fn circle_blob_opening_protection_on(
    conn: &Connection,
    root: &crate::protocol::store_commit::StoreRootRef,
    circle_id: crate::protocol::circle::CircleId,
    expected_control: &crate::protocol::circle::CircleControlCoord,
    expected_key_fingerprint: crate::KeyFingerprint,
) -> Result<crate::protocol::objects::BlobSpoolProtection, DbError> {
    let Some(authority) =
        StoreDatabase::verified_circle_activation_on(conn, root, circle_id, expected_control)?
    else {
        return Err(DbError::Message(format!(
            "Circle {circle_id} has no retained authority for control {expected_control:?}"
        )));
    };
    if authority.control.value.key_fingerprint() != expected_key_fingerprint {
        return Err(DbError::Message(format!(
            "Circle {circle_id} blob key {expected_key_fingerprint} differs from \
                 exact control {expected_control:?}"
        )));
    }

    let mut statement = conn
        .prepare(
            "SELECT control_coord
                 FROM circle_control_activations
                 WHERE circle_id = ?1
                 ORDER BY control_coord",
        )
        .map_err(DbError::from)?;
    let rows = statement
        .query_map([circle_id.to_string()], |row| row.get::<_, String>(0))
        .map_err(DbError::from)?;
    let mut controls = Vec::new();
    for encoded in rows {
        let encoded = encoded.map_err(DbError::from)?;
        controls.push(serde_json::from_str(&encoded).map_err(|error| {
            DbError::Message(format!(
                "parse retained Circle {circle_id} control coordinate: {error}"
            ))
        })?);
    }
    drop(statement);

    let mut retained_key = None;
    for control in controls {
        let activation =
            StoreDatabase::verified_circle_activation_on(conn, root, circle_id, &control)?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle {circle_id} activation index lost control {control:?}"
                    ))
                })?;
        let Some(keyring) = activation
            .retained_keyring()
            .map_err(|error| DbError::Message(error.to_string()))?
        else {
            continue;
        };
        for (generation, key) in keyring.keyring_entries() {
            let candidate = EncryptionService::from_key_at_generation(generation, key);
            if candidate.seal_key_fingerprint() != expected_key_fingerprint {
                continue;
            }
            if retained_key
                .as_ref()
                .is_some_and(|existing: &EncryptionService| {
                    existing.current_generation() != generation || existing.key_bytes() != key
                })
            {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} retains inconsistent key material for fingerprint \
                         {expected_key_fingerprint}"
                )));
            }
            retained_key = Some(candidate);
        }
    }
    retained_key
        .map(crate::protocol::objects::BlobSpoolProtection::Opaque)
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} retains no local key for fingerprint \
                     {expected_key_fingerprint}"
            ))
        })
}

impl StoreDatabase {
    pub(crate) async fn verified_circle_activation(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<Option<crate::protocol::circle_activation::VerifiedCircleReference>, DbError> {
        self.connection
            .call(move |conn| Self::verified_circle_activation_on(conn, &root, circle_id, &control))
            .await
    }

    pub(crate) async fn circle_restore_head(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        controls: Vec<crate::protocol::circle::CircleControlCoord>,
    ) -> Result<
        Option<(
            crate::protocol::circle::CircleControlCoord,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        self.connection
            .call(move |conn| {
                let Some(head) = Self::head_circle_control_on(conn, &root, circle_id, &controls)?
                else {
                    return Ok(None);
                };
                let commit = Self::circle_activation_commit_ref_on(conn, circle_id, &head)?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "Circle {circle_id} head control has no activating commit"
                        ))
                    })?;
                Ok(Some((head, commit)))
            })
            .await
    }

    pub(crate) async fn retained_circle_activation_commit_ref(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        self.connection
            .call(move |conn| {
                Self::retained_circle_activation_commit_ref_on(conn, circle_id, &control)
            })
            .await
    }

    pub(crate) async fn verified_circle_control_coord_covers(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        covering: crate::protocol::circle::CircleControlCoord,
        covered: crate::protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        self.connection
            .call(move |conn| {
                let Some(reference) =
                    Self::verified_circle_activation_on(conn, &root, circle_id, &covering)?
                else {
                    return Ok(false);
                };
                Self::verified_circle_control_covers_on(
                    conn,
                    &root,
                    circle_id,
                    &reference.control,
                    &covered,
                )
            })
            .await
    }

    /// The head control of a Circle: the retained control whose lineage no other
    /// retained control covers. Restore resolves the restoring identity's current
    /// access at the head control's activating commit, so a member removed by a
    /// later epoch close resolves against the successor control that excludes them
    /// — never against a stale predecessor that still lists them active. A Circle
    /// with two uncovered controls is a forked lineage and fails loud.
    pub(crate) fn head_circle_control_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        controls: &[crate::protocol::circle::CircleControlCoord],
    ) -> Result<Option<crate::protocol::circle::CircleControlCoord>, DbError> {
        // A control whose activating commit was reclaimed is superseded by a later
        // epoch and cannot be head; keep only controls whose commit is retained.
        let mut retained: Vec<(
            crate::protocol::circle::CircleControlCoord,
            crate::protocol::circle::PreparedCircleControl,
        )> = Vec::new();
        for coord in controls {
            let Some(activation_commit) =
                Self::retained_circle_activation_commit_ref_on(conn, circle_id, coord)?
            else {
                continue;
            };
            let materialization = Self::load_retained_merge_materialization_by_ref_on(
                conn,
                root,
                &activation_commit,
            )?;
            let reference = materialization.circle_activation(circle_id, coord)?;
            retained.push((coord.clone(), reference.control));
        }
        let mut head: Option<crate::protocol::circle::CircleControlCoord> = None;
        for (index, (candidate, _)) in retained.iter().enumerate() {
            let mut covered = false;
            for (other_index, (_, other_control)) in retained.iter().enumerate() {
                if other_index == index {
                    continue;
                }
                if Self::verified_circle_control_covers_on(
                    conn,
                    root,
                    circle_id,
                    other_control,
                    candidate,
                )? {
                    covered = true;
                    break;
                }
            }
            if !covered {
                if head.is_some() {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} has multiple head controls"
                    )));
                }
                head = Some(candidate.clone());
            }
        }
        Ok(head)
    }

    /// Resolve a Circle control's activating commit reference from the retained
    /// authority, not the materialized ledger. The activating commit is always a
    /// retained input (every consumer that reads it goes on to open its retained
    /// materialization), and the materialized ledger is empty on a device restored
    /// from a snapshot until the pull replays it — so a restore resolves the same
    /// reference here that a live device would.
    ///
    /// Errors when the control is a known activation whose materialization has been
    /// reclaimed: every strict caller resolves a current/authoring/closing control
    /// that must stay retained. A caller resolving a possibly-superseded control (a
    /// restore weighing an old standalone snapshot) uses
    /// [`Self::retained_circle_activation_commit_ref_on`] instead, which reads the
    /// reclaimed state as absence.
    pub(crate) fn circle_activation_commit_ref_on(
        conn: &Connection,
        circle_id: crate::protocol::circle::CircleId,
        control: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        match Self::circle_activation_commit_lookup_on(conn, circle_id, control)? {
            CircleActivationCommitLookup::Absent => Ok(None),
            CircleActivationCommitLookup::Reclaimed {
                stream_id,
                sequence,
            } => Err(DbError::Message(format!(
                "Circle {circle_id} activation commit {stream_id}/{sequence} is not retained"
            ))),
            CircleActivationCommitLookup::Retained(reference) => Ok(Some(reference)),
        }
    }

    /// Resolve a Circle control's activating commit, reading a reclaimed
    /// materialization as absence rather than an error. A control reclaimed after an
    /// epoch close leaves its standalone snapshot superseded by the successor
    /// bootstrap, so restore selection treats it as one fewer installable candidate
    /// rather than a hard failure.
    pub(crate) fn retained_circle_activation_commit_ref_on(
        conn: &Connection,
        circle_id: crate::protocol::circle::CircleId,
        control: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        Ok(
            match Self::circle_activation_commit_lookup_on(conn, circle_id, control)? {
                CircleActivationCommitLookup::Retained(reference) => Some(reference),
                CircleActivationCommitLookup::Absent
                | CircleActivationCommitLookup::Reclaimed { .. } => None,
            },
        )
    }

    fn circle_activation_commit_lookup_on(
        conn: &Connection,
        circle_id: crate::protocol::circle::CircleId,
        control: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<CircleActivationCommitLookup, DbError> {
        let control_coord = serde_json::to_string(control).map_err(|error| {
            DbError::Message(format!("serialize Circle control coordinate: {error}"))
        })?;
        let stored = conn
            .query_row(
                "SELECT stream_id, seq, commit_hash
                 FROM circle_control_activations
                 WHERE circle_id = ?1 AND control_coord = ?2",
                rusqlite::params![circle_id.to_string(), control_coord],
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
        let Some((stream_id, sequence_sql, commit_hash)) = stored else {
            return Ok(CircleActivationCommitLookup::Absent);
        };
        let sequence = Database::sequence_from_sqlite(&stream_id, sequence_sql)?;
        let stored_ref: Option<String> = conn
            .query_row(
                "SELECT commit_ref FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence_sql],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)?;
        let Some(stored_ref) = stored_ref else {
            return Ok(CircleActivationCommitLookup::Reclaimed {
                stream_id,
                sequence,
            });
        };
        let reference = Self::parse_stored_commit_ref(&stream_id, sequence, &stored_ref)?;
        if reference.commit_hash.to_string() != commit_hash {
            return Err(DbError::Message(format!(
                "Circle {circle_id} activation index differs from its retained commit"
            )));
        }
        Ok(CircleActivationCommitLookup::Retained(reference))
    }

    pub(crate) fn verified_circle_activation_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        control: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<Option<crate::protocol::circle_activation::VerifiedCircleReference>, DbError> {
        let Some(activation_commit) =
            Self::circle_activation_commit_ref_on(conn, circle_id, control)?
        else {
            return Ok(None);
        };
        let retained =
            Self::load_retained_merge_materialization_by_ref_on(conn, root, &activation_commit)?;
        retained.circle_activation(circle_id, control).map(Some)
    }

    pub(crate) async fn verified_circle_control_covers(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        current: crate::protocol::circle::PreparedCircleControl,
        prior: crate::protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        self.connection
            .call(move |connection| {
                Self::verified_circle_control_covers_on(
                    connection, &root, circle_id, &current, &prior,
                )
            })
            .await
    }

    pub(crate) fn verified_circle_control_covers_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        circle_id: crate::protocol::circle::CircleId,
        current: &crate::protocol::circle::PreparedCircleControl,
        prior: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        if current.value.circle_id != circle_id {
            return Err(DbError::Message(
                "Circle control lineage starts outside its Circle".to_string(),
            ));
        }
        if current.coord == *prior {
            return Ok(true);
        }
        let mut pending = current
            .value
            .access_epoch()
            .covered_control_heads
            .iter()
            .map(|head| (current.clone(), head.coord.clone()))
            .collect::<Vec<_>>();
        let mut visited = std::collections::BTreeSet::new();
        while let Some((successor, coordinate)) = pending.pop() {
            if !visited.insert(coordinate.clone()) {
                continue;
            }
            let predecessor =
                Self::verified_circle_activation_on(conn, root, circle_id, &coordinate)?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                        "Circle {circle_id} control lineage omits retained control {coordinate:?}"
                    ))
                    })?;
            if !successor.value.causally_covers(&predecessor.control.value) {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} control lineage contains a non-causal edge"
                )));
            }
            if predecessor.control.coord == *prior {
                return Ok(true);
            }
            pending.extend(
                predecessor
                    .control
                    .value
                    .access_epoch()
                    .covered_control_heads
                    .iter()
                    .map(|head| (predecessor.control.clone(), head.coord.clone())),
            );
        }
        Ok(false)
    }

    pub(crate) fn circle_current_state_on(
        conn: &Connection,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::protocol::circle_activation::CircleCurrentState>, DbError> {
        let stored = conn
            .query_row(
                "SELECT circle_id, state FROM circle_current_state WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        stored
            .map(|(stored_circle_id, state)| {
                Self::parse_circle_current_state(&stored_circle_id, &state)
            })
            .transpose()
    }

    fn parse_circle_current_state(
        stored_circle_id: &str,
        payload: &[u8],
    ) -> Result<crate::protocol::circle_activation::CircleCurrentState, DbError> {
        let circle_id: crate::protocol::circle::CircleId = stored_circle_id
            .parse()
            .map_err(|error| DbError::Message(format!("parse current Circle id: {error}")))?;
        let state: crate::protocol::circle_activation::CircleCurrentState =
            serde_json::from_slice(payload).map_err(|error| {
                DbError::Message(format!("parse Circle current state: {error}"))
            })?;
        if !state.verify() || state.circle_id() != circle_id {
            return Err(DbError::Message(format!(
                "Circle {circle_id} has invalid current state"
            )));
        }
        Ok(state)
    }

    pub(crate) fn remove_local_circle_access_on(conn: &Connection) -> Result<(), DbError> {
        let mut statement = conn
            .prepare(
                "SELECT circle_id, state
                 FROM circle_current_state
                 ORDER BY circle_id",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);

        for (circle_id, payload) in rows {
            let state =
                Self::parse_circle_current_state(&circle_id, &payload)?.without_local_access();
            let payload = serde_json::to_vec(&state).map_err(|error| {
                DbError::Message(format!("serialize public Circle current state: {error}"))
            })?;
            let changed = conn
                .execute(
                    "UPDATE circle_current_state
                     SET state = ?2
                     WHERE circle_id = ?1",
                    rusqlite::params![circle_id, payload],
                )
                .map_err(DbError::from)?;
            if changed != 1 {
                return Err(DbError::Message(
                    "Circle current state changed while removing local access".to_string(),
                ));
            }
        }
        conn.execute_batch("DELETE FROM circle_access_cache;")
            .map_err(DbError::from)?;
        Ok(())
    }
}
