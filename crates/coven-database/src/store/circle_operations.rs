use crate::query_mapped_rows;
use crate::*;
use coven_protocol::store_commit::StoreBatchCommitRef;
use rusqlite::{Connection, OptionalExtension};

use super::*;

impl StoreSession<'_> {
    fn circle_operations(
        &self,
    ) -> Result<Vec<coven_protocol::circle::CircleOperationInfo>, DbError> {
        let conn = self.conn;
        let rows = crate::query_mapped_rows(
            conn,
            "SELECT operation_id, circle_id, prepared, phase
             FROM circle_operations
             ORDER BY rowid",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .map_err(DbError::from)?;
        rows.into_iter()
            .map(|(operation_id, circle_id, prepared, phase)| {
                let uploaded = crate::circle_operation_uploaded_steps_on(conn, &operation_id)?;
                let journal = parse_circle_operation_row(
                    &operation_id,
                    &circle_id,
                    &prepared,
                    &phase,
                    uploaded,
                )?;
                Ok(coven_protocol::circle::CircleOperationInfo {
                    operation_id: journal.operation_id.clone(),
                    circle_id: journal.circle_id(),
                    kind: journal.kind(),
                    state: journal.state(),
                })
            })
            .collect()
    }

    fn circle_states(
        &self,
        identity_pubkey: &str,
        active_store_members: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<coven_protocol::circle::Circle>, DbError> {
        Ok(circle_current_states_on(self.conn)?
            .into_iter()
            .map(|state| {
                let (name, role) = state.display(identity_pubkey);
                coven_protocol::circle::Circle {
                    id: state.circle_id(),
                    name,
                    role,
                    state: state.derived_state(active_store_members),
                }
            })
            .collect())
    }

    fn circle_members(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        identity_pubkey: &str,
        store_members: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<coven_protocol::circle::CircleMemberInfo>, DbError> {
        let state = circle_current_state_on(self.conn, circle_id)?
            .ok_or_else(|| DbError::Message(format!("Circle {circle_id} has no current state")))?;
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
            .map(|(pubkey, role)| coven_protocol::circle::CircleMemberInfo {
                is_self: pubkey == identity_pubkey,
                pubkey,
                role,
            })
            .collect())
    }

    fn circle_signing_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        identity_pubkey: &str,
        authoring: fn(
            &coven_protocol::circle_activation::CircleCurrentState,
        ) -> Option<coven_protocol::circle_activation::CircleAuthoringState>,
        missing_authoring: fn(coven_protocol::circle::CircleId) -> String,
        foreign_identity: fn(coven_protocol::circle::CircleId) -> String,
    ) -> Result<
        (
            coven_protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let state = circle_current_state_on(self.conn, circle_id)?
            .ok_or_else(|| DbError::Message(format!("Circle {circle_id} has no current state")))?;
        let authoring =
            authoring(&state).ok_or_else(|| DbError::Message(missing_authoring(circle_id)))?;
        if authoring.access.recipient_pubkey != identity_pubkey {
            return Err(DbError::Message(foreign_identity(circle_id)));
        }
        let activated_commit = StoreDatabase::circle_activation_commit_ref_on(
            self.conn,
            circle_id,
            &authoring.control.coord,
        )?
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} current control has no materialized activation"
            ))
        })?;
        Ok((authoring, activated_commit))
    }

    fn circle_control_conflict_branches(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<Vec<coven_protocol::circle::CircleControlCoord>>, DbError> {
        Ok(circle_current_state_on(self.conn, circle_id)?
            .and_then(|state| state.conflict_branches()))
    }

    fn circle_is_deleted(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<bool, DbError> {
        Ok(circle_current_state_on(self.conn, circle_id)?.is_some_and(|state| state.is_deleted()))
    }

    fn current_circle_control(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleControlCoord>, DbError> {
        Ok(
            circle_current_state_on(self.conn, circle_id)?.and_then(|state| {
                state
                    .authoring_state()
                    .map(|authoring| authoring.control.coord.clone())
            }),
        )
    }

    fn closing_circle_controls(
        &self,
    ) -> Result<Vec<coven_protocol::circle::PreparedCircleControl>, DbError> {
        Ok(circle_current_states_on(self.conn)?
            .into_iter()
            .filter_map(|state| state.closing_control().cloned())
            .collect())
    }

    fn circle_publication_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<coven_protocol::circle_activation::CircleEpochAccess, DbError> {
        circle_publication_context_on(self.conn, circle_id, expected_control)
    }

    fn current_circle_partition_control(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<crate::CirclePartitionControl, DbError> {
        crate::active_circle_control(self.conn, circle_id)
            .map_err(|error| DbError::Message(error.to_string()))
    }

    fn circle_publication_rotation_block(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        active_store_members: &std::collections::BTreeSet<String>,
    ) -> Result<Option<coven_protocol::circle::CirclePublicationBlocked>, DbError> {
        let Some(state) = circle_current_state_on(self.conn, circle_id)? else {
            return Ok(None);
        };
        Ok(state
            .rotation_required(active_store_members)
            .map(
                |rotation| coven_protocol::circle::CirclePublicationBlocked::RotationRequired {
                    circle_id,
                    removed_members: rotation.removed_members,
                },
            ))
    }

    fn record_circle_close_exclusions(
        &self,
        exclusions: &[coven_protocol::circle_activation::LocalCircleExclusion],
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        for exclusion in exclusions {
            record_circle_close_exclusion_on(&tx, exclusion)?;
        }
        tx.commit().map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn circles(
        &self,
        identity_pubkey: &str,
        active_store_members: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<coven_protocol::circle::CircleInfo>, DbError> {
        let mut circles = Vec::new();
        for state in circle_current_states_on(self.conn)? {
            let circle_id = state.circle_id();
            if state.is_deleted() {
                circles.push(coven_protocol::circle::CircleInfo::Deleted { id: circle_id });
            } else if let Some(branches) = state.conflict_branches() {
                circles.push(coven_protocol::circle::CircleInfo::Conflicted {
                    id: circle_id,
                    branches,
                });
            } else if let Some((_current, access, roster, metadata)) = state.active() {
                if access.recipient_pubkey != identity_pubkey {
                    return Err(DbError::Message(format!(
                        "active circle {circle_id} belongs to another local identity"
                    )));
                }
                let role = roster
                    .members()
                    .get(identity_pubkey)
                    .copied()
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "activated circle {circle_id} excludes the local identity"
                        ))
                    })?;
                circles.push(coven_protocol::circle::CircleInfo::Active {
                    id: circle_id,
                    name: metadata.name.clone(),
                    role,
                    rotation_required: state.rotation_required(active_store_members).is_some(),
                });
            }
        }
        Ok(circles)
    }
}

impl StoreDatabase {
    pub async fn get_circle_operations(
        &self,
    ) -> Result<Vec<coven_protocol::circle::CircleOperationInfo>, DbError> {
        self.connection
            .call_store(|session| session.circle_operations())
            .await
    }

    /// Every Circle the local identity can see, with its public derived state. A
    /// deleted or conflicted Circle stays visible (as `Deleted`/`ControlConflict`)
    /// rather than silently disappearing; an inactive Circle the identity holds no
    /// access to is `Inactive`.
    pub async fn circle_states(
        &self,
        identity_pubkey: &str,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<coven_protocol::circle::Circle>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call_store(move |session| {
                session.circle_states(&identity_pubkey, &active_store_members)
            })
            .await
    }

    pub async fn get_circle_members(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        identity_pubkey: &str,
        store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<coven_protocol::circle::CircleMemberInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call_store(move |session| {
                session.circle_members(circle_id, &identity_pubkey, &store_members)
            })
            .await
    }

    pub async fn circle_authoring_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            coven_protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        self.circle_signing_context(
            circle_id,
            identity_pubkey,
            |state| state.authoring_state(),
            |circle_id| format!("Circle {circle_id} has no active authoring state"),
            |circle_id| format!("active Circle {circle_id} belongs to another local identity"),
        )
        .await
    }

    /// The signing context shared by Circle commands: the current state's
    /// authoring view selected by `authoring`, required to belong to the local
    /// identity, plus the commit that activated the current control.
    async fn circle_signing_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        identity_pubkey: &str,
        authoring: fn(
            &coven_protocol::circle_activation::CircleCurrentState,
        ) -> Option<coven_protocol::circle_activation::CircleAuthoringState>,
        missing_authoring: fn(coven_protocol::circle::CircleId) -> String,
        foreign_identity: fn(coven_protocol::circle::CircleId) -> String,
    ) -> Result<
        (
            coven_protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call_store(move |session| {
                session.circle_signing_context(
                    circle_id,
                    &identity_pubkey,
                    authoring,
                    missing_authoring,
                    foreign_identity,
                )
            })
            .await
    }

    /// The authoring context a terminal deletion signs from, accepting any
    /// resolved state whose local device holds owner access — `Active` or
    /// `Closing`. Unlike `circle_authoring_context` (Active-only, for commands
    /// that publish a new active epoch), deletion supersedes an in-flight close,
    /// so it authors equally from a closing control's frozen epoch.
    pub async fn circle_delete_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            coven_protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        self.circle_signing_context(
            circle_id,
            identity_pubkey,
            |state| state.deletable_authoring_state(),
            |circle_id| format!("Circle {circle_id} has no resolved authoring state to delete"),
            |circle_id| format!("Circle {circle_id} belongs to another local identity"),
        )
        .await
    }

    /// The retained conflicting branch coordinates for a Circle whose control
    /// history forked, in canonical order. `None` when the Circle has no
    /// current state or its control is resolved.
    pub async fn circle_control_conflict_branches(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<Vec<coven_protocol::circle::CircleControlCoord>>, DbError> {
        self.connection
            .call_store(move |session| session.circle_control_conflict_branches(circle_id))
            .await
    }

    /// Whether the Circle's control history has terminated in a deletion.
    pub async fn circle_is_deleted(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<bool, DbError> {
        self.connection
            .call_store(move |session| session.circle_is_deleted(circle_id))
            .await
    }

    /// The Circle's currently activated authoring control, or `None` when the
    /// Circle is not in an active local state. Its retained keyring resolves
    /// acknowledgements sealed under rotated-away epochs.
    pub async fn current_circle_control(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleControlCoord>, DbError> {
        self.connection
            .call_store(move |session| session.current_circle_control(circle_id))
            .await
    }

    pub async fn closing_circle_controls(
        &self,
    ) -> Result<Vec<coven_protocol::circle::PreparedCircleControl>, DbError> {
        self.connection
            .call_store(|session| session.closing_circle_controls())
            .await
    }

    pub async fn circle_closing_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            coven_protocol::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        self.circle_signing_context(
            circle_id,
            identity_pubkey,
            |state| state.closing_authoring_state(),
            |circle_id| format!("Circle {circle_id} is not closing"),
            |circle_id| format!("closing Circle {circle_id} belongs to another local identity"),
        )
        .await
    }

    pub async fn circle_publication_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<coven_protocol::circle_activation::CircleEpochAccess, DbError> {
        self.connection
            .call_store(move |session| {
                session.circle_publication_context(circle_id, &expected_control)
            })
            .await
    }

    /// The Circle's current active control coordinate. A durable write captured
    /// under an earlier control publishes under this one, so an epoch close that
    /// retired the capture-time control does not strand the write: its rows
    /// belong to whichever epoch is live when it publishes. Fails when the
    /// Circle is not currently active.
    pub async fn current_circle_partition_control(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<crate::CirclePartitionControl, DbError> {
        self.connection
            .call_store(move |session| session.current_circle_partition_control(circle_id))
            .await
    }

    /// Whether publishing new content into `circle_id` is blocked because the
    /// Circle's resolved roster names Store identities that hold no active
    /// membership grant at `active_store_members`. Derived from the current
    /// materialized state, so activating a successor roster without those
    /// identities clears it with no stored flag to reset.
    pub async fn circle_publication_rotation_block(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Option<coven_protocol::circle::CirclePublicationBlocked>, DbError> {
        self.connection
            .call_store(move |session| {
                session.circle_publication_rotation_block(circle_id, &active_store_members)
            })
            .await
    }

    pub async fn record_circle_close_exclusions(
        &self,
        exclusions: Vec<coven_protocol::circle_activation::LocalCircleExclusion>,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| session.record_circle_close_exclusions(&exclusions))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn get_circles(
        &self,
        identity_pubkey: &str,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<coven_protocol::circle::CircleInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call_store(move |session| session.circles(&identity_pubkey, &active_store_members))
            .await
    }
}

/// Record this device's own exclusion from a Circle epoch close, derived from
/// the verified successor outcome at materialization. The row is keyed by
/// Circle: a later close for the same Circle supersedes it. It is never
/// deleted — the publication gate derives clear once the successor bootstrap's
/// coverage records.
pub(crate) fn record_circle_close_exclusion_on(
    conn: &Connection,
    exclusion: &coven_protocol::circle_activation::LocalCircleExclusion,
) -> Result<(), DbError> {
    let circle_id = exclusion.circle_id.to_string();
    let close_id = serde_json::to_string(&exclusion.close_id)
        .map_err(|error| DbError::context("serialize close exclusion id", error))?;
    let excluded = serde_json::to_string(&exclusion.excluded)
        .map_err(|error| DbError::context("serialize close exclusion registration", error))?;
    let successor_control = serde_json::to_string(&exclusion.successor_control)
        .map_err(|error| DbError::context("serialize close exclusion successor", error))?;
    let activating_commit = serde_json::to_string(&exclusion.activating_commit)
        .map_err(|error| DbError::context("serialize close exclusion activation", error))?;
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

/// Every stored Circle current state, ordered by Circle id. The rows are read
/// and parsed up front so a caller can query or write through the same
/// connection while it walks them.
pub(crate) fn circle_current_states_on(
    conn: &Connection,
) -> Result<Vec<coven_protocol::circle_activation::CircleCurrentState>, DbError> {
    let rows = query_mapped_rows(
        conn,
        "SELECT circle_id, state FROM circle_current_state ORDER BY circle_id",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
    )?;
    rows.into_iter()
        .map(|(stored_circle_id, payload)| parse_circle_current_state(&stored_circle_id, &payload))
        .collect()
}

pub(crate) fn circle_current_state_on(
    conn: &Connection,
    circle_id: coven_protocol::circle::CircleId,
) -> Result<Option<coven_protocol::circle_activation::CircleCurrentState>, DbError> {
    let stored = conn
        .query_row(
            "SELECT circle_id, state FROM circle_current_state WHERE circle_id = ?1",
            [circle_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    stored
        .map(|(stored_circle_id, state)| parse_circle_current_state(&stored_circle_id, &state))
        .transpose()
}

fn parse_circle_current_state(
    stored_circle_id: &str,
    payload: &[u8],
) -> Result<coven_protocol::circle_activation::CircleCurrentState, DbError> {
    let circle_id: coven_protocol::circle::CircleId = stored_circle_id
        .parse()
        .map_err(|error| DbError::context("parse current Circle id", error))?;
    let state: coven_protocol::circle_activation::CircleCurrentState =
        serde_json::from_slice(payload)
            .map_err(|error| DbError::context("parse Circle current state", error))?;
    if !state.verify() || state.circle_id() != circle_id {
        return Err(DbError::Message(format!(
            "Circle {circle_id} has invalid current state"
        )));
    }
    Ok(state)
}

pub(crate) fn remove_local_circle_access_on(conn: &Connection) -> Result<(), DbError> {
    for state in circle_current_states_on(conn)? {
        let circle_id = state.circle_id().to_string();
        let state = state.without_local_access();
        let payload = serde_json::to_vec(&state)
            .map_err(|error| DbError::context("serialize public Circle current state", error))?;
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

pub(crate) fn circle_publication_context_on(
    conn: &Connection,
    circle_id: coven_protocol::circle::CircleId,
    expected_control: &coven_protocol::circle::CircleControlCoord,
) -> Result<coven_protocol::circle_activation::CircleEpochAccess, DbError> {
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
                DbError::context("parse pending Circle close exclusion id", error)
            })?;
            return Err(DbError::ExcludedDeviceMustReset {
                circle_id,
                close_id,
            });
        }
    }
    let state = circle_current_state_on(conn, circle_id)?
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

#[cfg(test)]
#[path = "circle_operations_test.rs"]
mod tests;
