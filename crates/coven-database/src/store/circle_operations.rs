use crate::payload_spool::StoreRecords;
use crate::query_mapped_rows;
use crate::*;
use coven_keys::encryption::EncryptionService;
use coven_protocol::store_commit::StoreBatchCommitRef;
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
    pub async fn get_circle_operations(
        &self,
    ) -> Result<Vec<coven_protocol::circle::CircleOperationInfo>, DbError> {
        self.connection
            .call(|conn| {
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
                        let uploaded =
                            crate::circle_operation_uploaded_steps_on(conn, &operation_id)?;
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
            })
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
            .call(move |conn| {
                Ok(Self::circle_current_states_on(conn)?
                    .into_iter()
                    .map(|state| {
                        let (name, role) = state.display(&identity_pubkey);
                        coven_protocol::circle::Circle {
                            id: state.circle_id(),
                            name,
                            role,
                            state: state.derived_state(&active_store_members),
                        }
                    })
                    .collect())
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
                    .map(|(pubkey, role)| coven_protocol::circle::CircleMemberInfo {
                        is_self: pubkey == identity_pubkey,
                        pubkey,
                        role,
                    })
                    .collect())
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
            .call(move |conn| {
                let state = Self::circle_current_state_on(conn, circle_id)?.ok_or_else(|| {
                    DbError::Message(format!("Circle {circle_id} has no current state"))
                })?;
                let authoring = authoring(&state)
                    .ok_or_else(|| DbError::Message(missing_authoring(circle_id)))?;
                if authoring.access.recipient_pubkey != identity_pubkey {
                    return Err(DbError::Message(foreign_identity(circle_id)));
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
            .call(move |conn| {
                Ok(Self::circle_current_state_on(conn, circle_id)?
                    .and_then(|state| state.conflict_branches()))
            })
            .await
    }

    /// Whether the Circle's control history has terminated in a deletion.
    pub async fn circle_is_deleted(
        &self,
        circle_id: coven_protocol::circle::CircleId,
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
    pub async fn current_circle_control(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleControlCoord>, DbError> {
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
    pub async fn circle_control_covers_strictly(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: &coven_protocol::circle::CircleControlCoord,
        covered: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        if covering == covered {
            return Ok(false);
        }
        let covering = covering.clone();
        let covered = covered.clone();
        self.call_records(move |records| {
            let Some(covering_reference) =
                Self::verified_circle_activation_on(records, &root, circle_id, &covering)?
            else {
                return Ok(false);
            };
            Self::verified_circle_control_covers_on(
                records,
                &root,
                circle_id,
                &covering_reference.control,
                &covered,
            )
        })
        .await
    }

    pub async fn closing_circle_controls(
        &self,
    ) -> Result<Vec<coven_protocol::circle::PreparedCircleControl>, DbError> {
        self.connection
            .call(|conn| {
                Ok(Self::circle_current_states_on(conn)?
                    .into_iter()
                    .filter_map(|state| state.closing_control().cloned())
                    .collect())
            })
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
            .call(move |conn| circle_publication_context_on(conn, circle_id, &expected_control))
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
            .call(move |conn| {
                crate::active_circle_control(conn, circle_id)
                    .map_err(|error| DbError::Message(error.to_string()))
            })
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
            .call(move |conn| {
                let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                    return Ok(None);
                };
                Ok(state
                    .rotation_required(&active_store_members)
                    .map(|rotation| {
                        coven_protocol::circle::CirclePublicationBlocked::RotationRequired {
                            circle_id,
                            removed_members: rotation.removed_members,
                        }
                    }))
            })
            .await
    }

    /// Record this device's own exclusion from a Circle epoch close, derived from
    /// the verified successor outcome at materialization. The row is keyed by
    /// Circle: a later close for the same Circle supersedes it. It is never
    /// deleted — the publication gate derives clear once the successor bootstrap's
    /// coverage records.
    pub fn record_circle_close_exclusion_on(
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

    pub async fn record_circle_close_exclusions(
        &self,
        exclusions: Vec<coven_protocol::circle_activation::LocalCircleExclusion>,
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

    pub async fn circle_epoch_access(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::CircleEpochAccess>, DbError> {
        self.with_retained_replay(move |records, retained| {
            retained.replay_inputs_on(records, &root)?;
            let Some(activation) = retained.verified_circle_activation_on(
                records.conn(),
                circle_id,
                &expected_control,
            )?
            else {
                return Ok(None);
            };
            activation
                .epoch_access()
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await
    }

    pub async fn circle_historical_package_keyring(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<Option<String>, DbError> {
        self.call_records(move |records| {
            let conn = records.conn();
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
                Self::verified_circle_activation_on(records, &root, circle_id, &expected_control)?
            else {
                return Ok(None);
            };
            if !Self::verified_circle_control_covers_on(
                records,
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
            let coven_protocol::circle::CircleAccessDisposition::Active { keyring, .. } =
                &current.access.disposition
            else {
                return Ok(None);
            };
            let parsed = coven_keys::encryption::MasterKeyring::from_serialized(keyring).map_err(
                |error| {
                    DbError::context(
                        format!("parse Circle {circle_id} historical package keyring"),
                        error,
                    )
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

    pub async fn verified_circle_activation_context(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<(
            coven_protocol::circle_activation::VerifiedCircleReference,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        self.call_records(move |records| {
            let Some(commit) =
                Self::circle_activation_commit_ref_on(records.conn(), circle_id, &control)?
            else {
                return Ok(None);
            };
            let activation =
                Self::verified_circle_activation_on(records, &root, circle_id, &control)?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "Circle {circle_id} activation context lost control {control:?}"
                        ))
                    })?;
            Ok(Some((activation, commit)))
        })
        .await
    }

    pub async fn circle_blob_opening_protection(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        self.call_records(move |records| {
            circle_blob_opening_protection_on(
                records,
                &root,
                circle_id,
                &expected_control,
                expected_key_fingerprint,
            )
        })
        .await
    }

    pub async fn verified_circle_activation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        self.call_records(move |records| {
            Self::verified_circle_activation_on(records, &root, circle_id, &control)
        })
        .await
    }

    pub async fn circle_restore_head(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        controls: Vec<coven_protocol::circle::CircleControlCoord>,
    ) -> Result<
        Option<(
            coven_protocol::circle::CircleControlCoord,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        self.call_records(move |records| {
            let Some(head) = Self::head_circle_control_on(records, &root, circle_id, &controls)?
            else {
                return Ok(None);
            };
            let commit = Self::circle_activation_commit_ref_on(records.conn(), circle_id, &head)?
                .ok_or_else(|| {
                DbError::Message(format!(
                    "Circle {circle_id} head control has no activating commit"
                ))
            })?;
            Ok(Some((head, commit)))
        })
        .await
    }

    pub async fn retained_circle_activation_commit_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        self.connection
            .call(move |conn| {
                Self::retained_circle_activation_commit_ref_on(conn, circle_id, &control)
            })
            .await
    }

    pub async fn verified_circle_control_coord_covers(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: coven_protocol::circle::CircleControlCoord,
        covered: coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        self.call_records(move |records| {
            let Some(reference) =
                Self::verified_circle_activation_on(records, &root, circle_id, &covering)?
            else {
                return Ok(false);
            };
            Self::verified_circle_control_covers_on(
                records,
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
    pub fn head_circle_control_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        controls: &[coven_protocol::circle::CircleControlCoord],
    ) -> Result<Option<coven_protocol::circle::CircleControlCoord>, DbError> {
        let conn = records.conn();
        // A control whose activating commit was reclaimed is superseded by a later
        // epoch and cannot be head; keep only controls whose commit is retained.
        let mut retained: Vec<(
            coven_protocol::circle::CircleControlCoord,
            coven_protocol::circle::PreparedCircleControl,
        )> = Vec::new();
        for coord in controls {
            let Some(activation_commit) =
                Self::retained_circle_activation_commit_ref_on(conn, circle_id, coord)?
            else {
                continue;
            };
            let materialization = Self::load_retained_merge_materialization_by_ref_on(
                records,
                root,
                &activation_commit,
            )?;
            let reference = materialization.circle_activation(circle_id, coord)?;
            retained.push((coord.clone(), reference.control));
        }
        let mut head: Option<coven_protocol::circle::CircleControlCoord> = None;
        for (index, (candidate, _)) in retained.iter().enumerate() {
            let mut covered = false;
            for (other_index, (_, other_control)) in retained.iter().enumerate() {
                if other_index == index {
                    continue;
                }
                if Self::verified_circle_control_covers_on(
                    records,
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
    pub fn circle_activation_commit_ref_on(
        conn: &Connection,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
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
    pub fn retained_circle_activation_commit_ref_on(
        conn: &Connection,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
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
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<CircleActivationCommitLookup, DbError> {
        let control_coord = serde_json::to_string(control)
            .map_err(|error| DbError::context("serialize Circle control coordinate", error))?;
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

    pub fn verified_circle_activation_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        let Some(activation_commit) =
            Self::circle_activation_commit_ref_on(records.conn(), circle_id, control)?
        else {
            return Ok(None);
        };
        let retained =
            Self::load_retained_merge_materialization_by_ref_on(records, root, &activation_commit)?;
        retained.circle_activation(circle_id, control).map(Some)
    }

    pub async fn verified_circle_control_covers(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: coven_protocol::circle::PreparedCircleControl,
        prior: coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        self.call_records(move |records| {
            Self::verified_circle_control_covers_on(records, &root, circle_id, &current, &prior)
        })
        .await
    }

    pub fn verified_circle_control_covers_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: &coven_protocol::circle::PreparedCircleControl,
        prior: &coven_protocol::circle::CircleControlCoord,
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
                Self::verified_circle_activation_on(records, root, circle_id, &coordinate)?
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

    /// Every stored Circle current state, ordered by Circle id. The rows are read
    /// and parsed up front so a caller can query or write through the same
    /// connection while it walks them.
    pub fn circle_current_states_on(
        conn: &Connection,
    ) -> Result<Vec<coven_protocol::circle_activation::CircleCurrentState>, DbError> {
        let rows = query_mapped_rows(
            conn,
            "SELECT circle_id, state FROM circle_current_state ORDER BY circle_id",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?;
        rows.into_iter()
            .map(|(stored_circle_id, payload)| {
                Self::parse_circle_current_state(&stored_circle_id, &payload)
            })
            .collect()
    }

    pub fn circle_current_state_on(
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
            .map(|(stored_circle_id, state)| {
                Self::parse_circle_current_state(&stored_circle_id, &state)
            })
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

    pub fn remove_local_circle_access_on(conn: &Connection) -> Result<(), DbError> {
        for state in Self::circle_current_states_on(conn)? {
            let circle_id = state.circle_id().to_string();
            let state = state.without_local_access();
            let payload = serde_json::to_vec(&state).map_err(|error| {
                DbError::context("serialize public Circle current state", error)
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

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn get_circles(
        &self,
        identity_pubkey: &str,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<coven_protocol::circle::CircleInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.connection
            .call(move |conn| {
                let mut circles = Vec::new();
                for state in Self::circle_current_states_on(conn)? {
                    let circle_id = state.circle_id();
                    if state.is_deleted() {
                        // A deleted Circle must remain visible to the application
                        // as deleted rather than silently disappear from its UI.
                        circles.push(coven_protocol::circle::CircleInfo::Deleted { id: circle_id });
                    } else if let Some(branches) = state.conflict_branches() {
                        // A forked Circle must be visible to the application as
                        // conflicted so an Owner can resolve it; omitting it
                        // would make the Circle silently disappear.
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
                        circles.push(coven_protocol::circle::CircleInfo::Active {
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

pub(crate) fn circle_blob_opening_protection_on(
    records: StoreRecords<'_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    circle_id: coven_protocol::circle::CircleId,
    expected_control: &coven_protocol::circle::CircleControlCoord,
    expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
    let conn = records.conn();
    let Some(authority) =
        StoreDatabase::verified_circle_activation_on(records, root, circle_id, expected_control)?
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
            DbError::context(
                format!("parse retained Circle {circle_id} control coordinate"),
                error,
            )
        })?);
    }

    let mut retained_key = None;
    for control in controls {
        let activation =
            StoreDatabase::verified_circle_activation_on(records, root, circle_id, &control)?
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
        .map(coven_protocol::objects::BlobSpoolProtection::Opaque)
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} retains no local key for fingerprint \
                     {expected_key_fingerprint}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coven_protocol::circle::{
        CircleInfo, CircleRole, CircleTransitionDraft, PreparedCircleControl,
    };
    use coven_protocol::circle_test_fixtures::{
        exact_logical_object, merge_device_authority, merge_membership_ref,
    };
    use coven_protocol::store_commit::ObjectHash;
    use coven_protocol::{membership, store_commit};

    #[tokio::test]
    async fn control_history_caches_the_verified_access_owner_and_rejects_second_genesis() {
        let author = coven_keys::keys::UserKeypair::generate();
        let author_pubkey = coven_keys::keys::public_key_hex(&author);
        let earlier_owner = loop {
            let candidate = coven_keys::keys::UserKeypair::generate();
            if coven_keys::keys::public_key_hex(&candidate) < author_pubkey {
                break candidate;
            }
        };
        let earlier_owner_pubkey = coven_keys::keys::public_key_hex(&earlier_owner);
        let members = vec![
            (author_pubkey.clone(), membership::MemberRole::Owner),
            (earlier_owner_pubkey.clone(), membership::MemberRole::Owner),
        ];
        let store_root_hash = ObjectHash::digest(b"multi-owner-store-root");
        let (membership, membership_authority) =
            merge_membership_ref(&author, &members, "multi-owner-control");
        let device = merge_device_authority(&author, store_root_hash, "multi-owner-device");
        let ids = coven_foundation::id_provider::SequentialIdProvider::new("multi-owner-control");
        let operation_id = coven_protocol::write::WriteId::from_generated(
            "multi-owner-control-commit".to_string(),
        );
        let order = store_commit::StoreCommitOrder {
            seq: 1,
            predecessor: None,
            dependencies: BTreeMap::new(),
        };
        let candidate_family = store_commit::CandidateFamilyId::derive(
            store_root_hash,
            &device.reference,
            &operation_id,
            &order,
        );
        let creation = CircleTransitionDraft::founder(
            store_root_hash,
            candidate_family,
            &device.reference.device_id.to_string(),
            "Household",
            "0000000001000-0000-device-a",
            membership.clone(),
            membership_authority.clone(),
            members,
            &ids,
            &author,
        )
        .expect("construct founder circle");
        let mut control = creation.control.value.clone();
        let control_author_pubkey = control.author_pubkey.clone();
        let active_epoch = control
            .body_mut()
            .value
            .state
            .active_epoch_mut()
            .expect("test control has an active epoch");
        active_epoch.common.owners = vec![earlier_owner_pubkey, author_pubkey.clone()];
        active_epoch.common.owners.sort();
        assert_ne!(active_epoch.common.owners[0], control_author_pubkey);
        control.resign(&author);
        let control = PreparedCircleControl {
            coord: control.coord(),
            bytes: serde_json::to_vec(&control).expect("serialize control"),
            value: control,
        };
        let reference = device.circle_control_reference(&control, "multi-owner");
        let first_coord = store_commit::StoreCommitCoord {
            stream_id: device.stream_id,
            sequence: 1,
        };
        let commit = store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            operation_id,
            first_coord.clone(),
            device.reference.clone(),
            &device.registration,
            order,
            membership.clone(),
            store_commit::StoreDeviceStateRef::from_resolved(
                store_commit::CommitFrontier(BTreeMap::new()),
                &store_commit::ResolvedStoreDeviceState {
                    devices: BTreeMap::new(),
                    recovery: Vec::new(),
                    state_hash: ObjectHash::digest(b"multi-owner initial device state"),
                },
            )
            .expect("bind initial device state"),
            store_commit::StoreOperationMembershipAuthority {
                predecessor: membership_authority.clone(),
            },
            store_commit::StoreCommitOperationsInput {
                circle_controls: vec![reference.clone()],
                ..store_commit::StoreCommitOperationsInput::empty()
            },
            &device.device_signer,
        )
        .expect("sign Store commit");
        let first_commit_path = format!(
            "{}.json",
            store_commit::commit_semantic_prefix(
                commit.candidate_family(),
                &device.stream_id.to_string(),
                1,
                commit.commit_hash(),
            )
        );
        let commit_ref = store_commit::StoreBatchCommitRef::from_commit(
            &commit,
            first_coord,
            exact_logical_object(first_commit_path, &commit.to_bytes()),
        )
        .expect("reference first Store commit");
        let verified_commit = store_commit::VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            store_root_hash,
            &commit_ref,
            &device.registration,
        )
        .expect("authenticate first Store commit");
        let own_access = creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == author_pubkey)
            .expect("author access");
        let verified = coven_protocol::circle_activation::VerifiedCircleReference {
            reference,
            circle_id: creation.circle_id,
            control: control.clone(),
            local_access: Some(coven_protocol::circle_activation::VerifiedCircleAccess {
                envelope: own_access.envelope.clone(),
                leaf: own_access.leaf.clone(),
                active: Some(coven_protocol::circle_activation::VerifiedCircleActive {
                    roster: creation.roster.clone(),
                    metadata: creation.metadata.clone(),
                }),
            }),
        };
        let db = crate::synthetic_store::open_test_db();
        let (_spool, store_dir) = coven_foundation::store_dir::temp_store_dir();
        let activation_store_dir = store_dir.clone();
        let store_database = crate::StoreDatabase::new(&db);
        let first_commit = verified_commit.clone();
        db.test_sql(move |database| {
            database.record_verified_circle_activations(
                &activation_store_dir,
                &first_commit,
                &[verified],
            )
        })
        .await
        .expect("record multi-Owner control");
        let cached_owner = db
            .test_sql(move |database| database.circle_access_owner(creation.circle_id))
            .await
            .expect("read cached access owner");
        assert_eq!(cached_owner, author_pubkey);
        db.test_sql(|database| {
            database.clear_table(crate::DatabaseTestTable::named("circle_access_cache"))
        })
        .await
        .expect("remove historical Circle projections");
        let circles = store_database
            .get_circles(
                &author_pubkey,
                std::collections::BTreeSet::from([author_pubkey.clone()]),
            )
            .await
            .expect("list Circle from its derived current state");
        assert_eq!(
            circles,
            vec![CircleInfo::Active {
                id: creation.circle_id,
                name: creation.metadata.name.clone(),
                role: CircleRole::Owner,
                rotation_required: false,
            }]
        );
        let publication = store_database
            .circle_publication_context(creation.circle_id, control.coord.clone())
            .await
            .expect("load publication authority from derived current state");
        let publication_fingerprint = publication.key_fingerprint();
        assert_eq!(publication_fingerprint, control.value.key_fingerprint());

        let mut second_value = control.value.clone();
        let active_epoch = second_value
            .body_mut()
            .value
            .state
            .active_epoch_mut()
            .expect("test control has an active epoch");
        active_epoch.common.access_root = ObjectHash::digest(b"different founder access root");
        second_value.resign(&author);
        let second_control = PreparedCircleControl {
            coord: second_value.coord(),
            bytes: serde_json::to_vec(&second_value).expect("serialize second founder control"),
            value: second_value,
        };
        let second_reference = device.circle_control_reference(&second_control, "second-founder");
        let second_coord = store_commit::StoreCommitCoord {
            stream_id: device.stream_id,
            sequence: 2,
        };
        let second_commit = store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            coven_protocol::write::WriteId::from_generated(
                "second-founder-control-commit".to_string(),
            ),
            second_coord.clone(),
            device.reference,
            &device.registration,
            store_commit::StoreCommitOrder {
                seq: 2,
                predecessor: Some(commit_ref.clone()),
                dependencies: BTreeMap::new(),
            },
            membership,
            store_commit::StoreDeviceStateRef::from_resolved(
                store_commit::CommitFrontier(BTreeMap::from([(
                    device.stream_id,
                    commit_ref.clone(),
                )])),
                &store_commit::ResolvedStoreDeviceState {
                    devices: BTreeMap::new(),
                    recovery: Vec::new(),
                    state_hash: ObjectHash::digest(b"multi-owner second device state"),
                },
            )
            .expect("bind second device state"),
            store_commit::StoreOperationMembershipAuthority {
                predecessor: control.value.membership_authority().clone(),
            },
            store_commit::StoreCommitOperationsInput {
                circle_controls: vec![second_reference.clone()],
                ..store_commit::StoreCommitOperationsInput::empty()
            },
            &device.device_signer,
        )
        .expect("sign second founder Store commit");
        let second_commit_path = format!(
            "{}.json",
            store_commit::commit_semantic_prefix(
                second_commit.candidate_family(),
                &device.stream_id.to_string(),
                2,
                second_commit.commit_hash(),
            )
        );
        let second_commit_ref = store_commit::StoreBatchCommitRef::from_commit(
            &second_commit,
            second_coord,
            exact_logical_object(second_commit_path, &second_commit.to_bytes()),
        )
        .expect("reference second Store commit");
        let second_commit = store_commit::VerifiedStoreBatchCommit::parse(
            &second_commit.to_bytes(),
            store_root_hash,
            &second_commit_ref,
            &device.registration,
        )
        .expect("authenticate second Store commit");
        let error = db
            .test_sql(move |database| {
                database.record_verified_circle_activations(
                    &store_dir,
                    &second_commit,
                    &[coven_protocol::circle_activation::VerifiedCircleReference {
                        reference: second_reference,
                        circle_id: creation.circle_id,
                        control: second_control,
                        local_access: None,
                    }],
                )
            })
            .await
            .expect_err("a Circle cannot accept a second founder control");
        assert!(
            error.to_string().contains("already has a founder"),
            "{error}"
        );
    }
}
