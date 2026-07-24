use crate::database::*;
use crate::encryption::EncryptionService;
use crate::sync::store_commit::StoreBatchCommitRef;
use rusqlite::{Connection, OptionalExtension};

use super::*;

impl StoreDatabase {
    pub async fn get_circle_operations(
        &self,
    ) -> Result<Vec<crate::sync::circle::CircleOperationInfo>, DbError> {
        self.database
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
                        Ok(crate::sync::circle::CircleOperationInfo {
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

    pub async fn get_circles(
        &self,
        identity_pubkey: &str,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::sync::circle::CircleInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.database
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
                    if let Some((_current, access, roster, metadata)) = state.active() {
                        let circle_id = state.circle_id();
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
                        circles.push(crate::sync::circle::CircleInfo {
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

    pub async fn get_circle_members(
        &self,
        circle_id: crate::sync::circle::CircleId,
        identity_pubkey: &str,
        store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::sync::circle::CircleMemberInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.database
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
                    .map(|(pubkey, role)| crate::sync::circle::CircleMemberInfo {
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
        circle_id: crate::sync::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            crate::sync::store::circle_controls::activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let identity_pubkey = identity_pubkey.to_string();
        self.database
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

    pub(crate) async fn closing_circle_controls(
        &self,
    ) -> Result<Vec<crate::sync::circle::PreparedCircleControl>, DbError> {
        self.database
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
        circle_id: crate::sync::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            crate::sync::store::circle_controls::activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let identity_pubkey = identity_pubkey.to_string();
        self.database
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
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
    ) -> Result<(EncryptionService, crate::KeyFingerprint), DbError> {
        self.database
            .call(move |conn| {
                Self::circle_publication_context_on(conn, circle_id, &expected_control)
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
        circle_id: crate::sync::circle::CircleId,
        active_store_members: std::collections::BTreeSet<String>,
    ) -> Result<Option<crate::sync::circle::CirclePublicationBlocked>, DbError> {
        self.database
            .call(move |conn| {
                Self::circle_publication_rotation_block_on(conn, circle_id, &active_store_members)
            })
            .await
    }

    pub(crate) fn circle_publication_rotation_block_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
        active_store_members: &std::collections::BTreeSet<String>,
    ) -> Result<Option<crate::sync::circle::CirclePublicationBlocked>, DbError> {
        let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
            return Ok(None);
        };
        Ok(state
            .rotation_required(active_store_members)
            .map(
                |rotation| crate::sync::circle::CirclePublicationBlocked::RotationRequired {
                    circle_id,
                    removed_members: rotation.removed_members,
                },
            ))
    }

    pub(crate) fn circle_publication_context_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
        expected_control: &crate::sync::circle::CircleControlCoord,
    ) -> Result<(EncryptionService, crate::KeyFingerprint), DbError> {
        let state = Self::circle_current_state_on(conn, circle_id)?
            .ok_or_else(|| DbError::Message(format!("Circle {circle_id} has no current state")))?;
        let access = state
            .package_access(expected_control)
            .map_err(|error| DbError::Message(error.to_string()))?
            .ok_or_else(|| {
                DbError::Message(format!("Circle {circle_id} has no active publication key"))
            })?;
        Ok((access.encryption, access.key_fingerprint))
    }

    pub(crate) async fn circle_package_access(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
    ) -> Result<Option<crate::sync::store::circle_controls::CirclePackageAccess>, DbError> {
        self.database
            .call(move |conn| {
                let Some(activation) =
                    Self::verified_circle_activation_on(conn, circle_id, &expected_control)?
                else {
                    return Ok(None);
                };
                activation
                    .package_access()
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .await
    }

    pub(crate) async fn circle_historical_package_keyring(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
        expected_key_fingerprint: crate::KeyFingerprint,
    ) -> Result<Option<String>, DbError> {
        self.database
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
                    Self::verified_circle_activation_on(conn, circle_id, &expected_control)?
                else {
                    return Ok(None);
                };
                if !Self::verified_circle_control_covers_on(
                    conn,
                    circle_id,
                    &current.control,
                    &expected_control,
                )? || current.control.value.epoch_id() != historical.control.value.epoch_id()
                    || current.control.value.key_fingerprint() != expected_key_fingerprint
                    || historical.control.value.key_fingerprint() != expected_key_fingerprint
                {
                    return Ok(None);
                }
                let crate::sync::circle::CircleAccessDisposition::Active { keyring, .. } =
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
        circle_id: crate::sync::circle::CircleId,
        control: crate::sync::circle::CircleControlCoord,
    ) -> Result<
        Option<(
            crate::sync::store::circle_controls::VerifiedCircleReference,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        self.database
            .call(move |conn| {
                let Some(commit) =
                    Self::circle_activation_commit_ref_on(conn, circle_id, &control)?
                else {
                    return Ok(None);
                };
                let activation = Self::verified_circle_activation_on(conn, circle_id, &control)?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "Circle {circle_id} activation context lost control {control:?}"
                        ))
                    })?;
                Ok(Some((activation, commit)))
            })
            .await
    }

    pub(crate) async fn circle_blob_opening_key(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
        expected_key_fingerprint: crate::KeyFingerprint,
    ) -> Result<EncryptionService, DbError> {
        self.database
            .call(move |conn| {
                Self::circle_blob_opening_key_on(
                    conn,
                    circle_id,
                    &expected_control,
                    expected_key_fingerprint,
                )
            })
            .await
    }

    pub(crate) fn circle_blob_opening_key_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
        expected_control: &crate::sync::circle::CircleControlCoord,
        expected_key_fingerprint: crate::KeyFingerprint,
    ) -> Result<EncryptionService, DbError> {
        let Some(authority) =
            Self::verified_circle_activation_on(conn, circle_id, expected_control)?
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
            let activation = Self::verified_circle_activation_on(conn, circle_id, &control)?
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
        retained_key.ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} retains no local key for fingerprint \
                     {expected_key_fingerprint}"
            ))
        })
    }

    pub(crate) async fn verified_circle_activation(
        &self,
        circle_id: crate::sync::circle::CircleId,
        control: crate::sync::circle::CircleControlCoord,
    ) -> Result<Option<crate::sync::store::circle_controls::VerifiedCircleReference>, DbError> {
        self.database
            .call(move |conn| Self::verified_circle_activation_on(conn, circle_id, &control))
            .await
    }

    fn circle_activation_commit_ref_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
        control: &crate::sync::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
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
        let Some((stream_id, sequence, commit_hash)) = stored else {
            return Ok(None);
        };
        let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
        let reference = Self::materialized_commit_ref_on(conn, &stream_id, sequence)?.ok_or_else(
            || {
                DbError::Message(format!(
                    "Circle {circle_id} activation commit {stream_id}/{sequence} is absent from the materialized ledger"
                ))
            },
        )?;
        if reference.commit_hash.to_string() != commit_hash {
            return Err(DbError::Message(format!(
                "Circle {circle_id} activation index differs from its materialized commit"
            )));
        }
        Ok(Some(reference))
    }

    pub(super) fn verified_circle_activation_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
        control: &crate::sync::circle::CircleControlCoord,
    ) -> Result<Option<crate::sync::store::circle_controls::VerifiedCircleReference>, DbError> {
        let Some(activation_commit) =
            Self::circle_activation_commit_ref_on(conn, circle_id, control)?
        else {
            return Ok(None);
        };
        let retained =
            Self::load_retained_merge_materialization_by_ref_on(conn, &activation_commit)?;
        let verified = retained.as_verified()?;
        let mut matches = verified
            .circle_activations()
            .circles()
            .iter()
            .filter(|activation| {
                activation.circle_id == circle_id && activation.control.coord == *control
            });
        let activation = matches.next().cloned().ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} retained activation omits control {control:?}"
            ))
        })?;
        if matches.next().is_some() {
            return Err(DbError::Message(format!(
                "Circle {circle_id} retained activation duplicates control {control:?}"
            )));
        }
        Ok(Some(activation))
    }

    pub(crate) fn verified_circle_control_covers_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
        current: &crate::sync::circle::PreparedCircleControl,
        prior: &crate::sync::circle::CircleControlCoord,
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
            let predecessor = Self::verified_circle_activation_on(conn, circle_id, &coordinate)?
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

    fn circle_current_state_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
    ) -> Result<Option<crate::sync::store::circle_controls::activation::CircleCurrentState>, DbError>
    {
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
    ) -> Result<crate::sync::store::circle_controls::activation::CircleCurrentState, DbError> {
        let circle_id: crate::sync::circle::CircleId = stored_circle_id
            .parse()
            .map_err(|error| DbError::Message(format!("parse current Circle id: {error}")))?;
        let state: crate::sync::store::circle_controls::activation::CircleCurrentState =
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
        conn.execute_batch(
            "DELETE FROM circle_access_cache;
             DELETE FROM circle_roster_cache;
             DELETE FROM circle_metadata_cache;",
        )
        .map_err(DbError::from)?;
        Ok(())
    }

    pub(crate) fn reduce_circle_current_state_on(
        conn: &Connection,
        candidate_family: crate::sync::store_commit::CandidateFamilyId,
        activation: &crate::sync::store::circle_controls::VerifiedCircleReference,
    ) -> Result<Vec<u8>, DbError> {
        let next_state =
            crate::sync::store::circle_controls::activation::CircleCurrentState::from_verified(
                candidate_family,
                activation,
            )
            .map_err(DbError::Message)?;
        let current_state = Self::circle_current_state_on(conn, activation.circle_id)?;
        let current_state = match current_state {
            Some(current) => current.advance(next_state).map_err(DbError::Message)?,
            None if activation.control.value.is_founder() => next_state,
            None => {
                return Err(DbError::Message(format!(
                    "circle {} current state is absent for a successor control",
                    activation.circle_id
                )))
            }
        };
        serde_json::to_vec(&current_state)
            .map_err(|error| DbError::Message(format!("serialize Circle current state: {error}")))
    }
}
