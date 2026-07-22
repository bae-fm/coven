use super::*;

impl Database {
    pub async fn get_circle_operations(
        &self,
    ) -> Result<Vec<crate::sync::circle::CircleOperationInfo>, DbError> {
        self.call(|conn| {
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
                    let journal = parse_circle_operation_row(&operation_id, &circle_id, &payload)?;
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
    ) -> Result<Vec<crate::sync::circle::CircleInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.call(move |conn| {
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
        self.call(move |conn| {
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
        self.call(move |conn| {
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
            let control_coord = serde_json::to_string(&authoring.control.coord).map_err(|error| {
                DbError::Message(format!("serialize current Circle control coordinate: {error}"))
            })?;
            let commit_hash = conn
                .query_row(
                    "SELECT commit_hash FROM circle_control_activations
                     WHERE circle_id = ?1 AND control_coord = ?2",
                    rusqlite::params![circle_id.to_string(), control_coord],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)?;
            let mut statement = conn
                .prepare(
                    "SELECT device_id, seq, commit_ref,
                            retained_commit_ref, retained_input_hash
                     FROM materialized_commits",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(DbError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(DbError::from)?;
            drop(statement);
            let mut activated_commit = None;
            for row in rows {
                let (
                    stream_id,
                    sequence,
                    encoded,
                    retained_commit_ref,
                    retained_input_hash,
                ) = row;
                let sequence = Self::sequence_from_sqlite(&stream_id, sequence)?;
                let reference = Self::parse_materialized_commit_row_on(
                    conn,
                    &stream_id,
                    sequence,
                    &encoded,
                    retained_commit_ref.as_deref(),
                    retained_input_hash.as_deref(),
                )?;
                if reference.commit_hash.to_string() != commit_hash {
                    continue;
                }
                if activated_commit.replace(reference).is_some() {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} activation commit is duplicated in the materialized ledger"
                    )));
                }
            }
            let activated_commit = activated_commit.ok_or_else(|| {
                DbError::Message(format!(
                    "Circle {circle_id} activation commit {commit_hash} is absent from the materialized ledger"
                ))
            })?;
            Ok((authoring, activated_commit))
        })
        .await
    }

    pub(crate) async fn circle_publication_context(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
    ) -> Result<(EncryptionService, crate::KeyFingerprint), DbError> {
        self.circle_access_context(circle_id, expected_control)
            .await?
            .ok_or_else(|| {
                DbError::Message(format!("Circle {circle_id} has no active publication key"))
            })
    }

    pub(crate) async fn circle_access_context(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
    ) -> Result<Option<(EncryptionService, crate::KeyFingerprint)>, DbError> {
        self.call(move |conn| {
            let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                return Ok(None);
            };
            let Some((current, access, _roster, _metadata)) = state.active() else {
                return Ok(None);
            };
            if current.coordinate() != &expected_control {
                return Ok(None);
            }
            let crate::sync::circle::CircleAccessDisposition::Active {
                keyring,
                key_fingerprint,
                ..
            } = &access.disposition
            else {
                return Err(DbError::Message(format!(
                    "active Circle {circle_id} has inactive access"
                )));
            };
            let encryption = EncryptionService::from(
                crate::encryption::MasterKeyring::from_serialized(keyring).map_err(|error| {
                    DbError::Message(format!("parse Circle publication keyring: {error}"))
                })?,
            );
            if encryption.seal_key_fingerprint() != *key_fingerprint {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} publication key fingerprint is invalid"
                )));
            }
            Ok(Some((encryption, *key_fingerprint)))
        })
        .await
    }

    pub(crate) async fn circle_authorizes_writer(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
        author_pubkey: String,
    ) -> Result<bool, DbError> {
        self.call(move |conn| {
            let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                return Ok(false);
            };
            let Some((current, _access, roster, _metadata)) = state.active() else {
                return Ok(false);
            };
            if current.coordinate() != &expected_control {
                return Ok(false);
            }
            Ok(roster.members().contains_key(&author_pubkey))
        })
        .await
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

    pub(super) fn reduce_circle_current_state_on(
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
