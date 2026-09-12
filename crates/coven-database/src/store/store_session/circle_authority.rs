use crate::store::store_session::StoreRecords;
use crate::*;
use coven_keys::encryption::EncryptionService;
use coven_protocol::store_commit::StoreBatchCommitRef;
use rusqlite::{Connection, OptionalExtension};

use super::*;

/// Package keys resolved against the installed state and verified prepared controls.
/// Successor keys still require the historical roster to authorize the package author.
pub enum CirclePackageAccess {
    Exact(coven_protocol::circle_activation::CircleEpochAccess),
    Historical(String),
}

/// The three states a Circle control's activating commit can be in when resolved
/// from the retained authority: not an activation at all, a known activation whose
/// materialization has been reclaimed, or a retained activation with its commit.
enum CircleActivationCommitLookup {
    Absent,
    Reclaimed { stream_id: String, sequence: u64 },
    Retained(StoreBatchCommitRef),
}

/// Resolve a Circle control's activating commit reference from retained
/// authority. A known activation whose materialization was reclaimed is an
/// error because strict callers require the current control to stay retained.
pub(crate) fn circle_activation_commit_ref_on(
    conn: &Connection,
    circle_id: coven_protocol::circle::CircleId,
    control: &coven_protocol::circle::CircleControlCoord,
) -> Result<Option<StoreBatchCommitRef>, DbError> {
    match circle_activation_commit_lookup_on(conn, circle_id, control)? {
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
/// materialization as absence because its standalone snapshot is superseded.
pub(crate) fn retained_circle_activation_commit_ref_on(
    conn: &Connection,
    circle_id: coven_protocol::circle::CircleId,
    control: &coven_protocol::circle::CircleControlCoord,
) -> Result<Option<StoreBatchCommitRef>, DbError> {
    Ok(
        match circle_activation_commit_lookup_on(conn, circle_id, control)? {
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
    let reference = crate::store::materialized_commit_index::parse_stored_commit_ref(
        &stream_id,
        sequence,
        &stored_ref,
    )?;
    if reference.commit_hash.to_string() != commit_hash {
        return Err(DbError::Message(format!(
            "Circle {circle_id} activation index differs from its retained commit"
        )));
    }
    Ok(CircleActivationCommitLookup::Retained(reference))
}

impl StoreSession<'_> {
    fn circle_control_covers_strictly(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: &coven_protocol::circle::CircleControlCoord,
        covered: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        let Some(covering_reference) = StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            covering,
        )?
        else {
            return Ok(false);
        };
        StoreDatabase::verified_circle_control_covers_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            &covering_reference.control,
            covered,
        )
    }

    fn circle_epoch_access(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::CircleEpochAccess>, DbError> {
        self.verified_store_authority.retained_replay_inputs_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            root,
        )?;
        let Some(activation) = self
            .verified_store_authority
            .verified_circle_activation_on(
                crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
                circle_id,
                expected_control,
            )?
        else {
            return Ok(None);
        };
        activation.epoch_access().map_err(DbError::from)
    }

    fn circle_package_access(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<Option<CirclePackageAccess>, DbError> {
        let Some(state) =
            self.circle_current_state_with_activations(root, circle_id, activations)?
        else {
            return Ok(None);
        };
        if state.is_deleted() {
            return Ok(None);
        }
        let exact = match activations.iter().find(|activation| {
            activation.circle_id == circle_id && &activation.control.coord == expected_control
        }) {
            Some(activation) => activation.epoch_access().map_err(DbError::from)?,
            None => self.circle_epoch_access(root, circle_id, expected_control)?,
        };
        if let Some(access) = exact {
            // Exact access remains valid for packages within the accepted epoch
            // cutoff even after a successor removes the local member.
            return Ok(Some(CirclePackageAccess::Exact(access)));
        }
        self.circle_historical_package_keyring(
            root,
            state,
            expected_control,
            expected_key_fingerprint,
            activations,
        )
        .map(|keyring| keyring.map(CirclePackageAccess::Historical))
    }

    fn circle_historical_package_keyring(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        state: coven_protocol::circle_activation::CircleCurrentState,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<Option<String>, DbError> {
        let circle_id = state.circle_id();
        if !state.verify() {
            return Err(DbError::Message(
                "invalid projected Circle package state".to_string(),
            ));
        }
        let Some(current) = state
            .authoring_state()
            .or_else(|| state.closing_authoring_state())
        else {
            return Ok(None);
        };
        let Some(historical) = verified_circle_activation_with_prefix_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            expected_control,
            activations,
        )?
        else {
            return Ok(None);
        };
        if !verified_circle_control_covers_with_prefix_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            &current.control,
            expected_control,
            activations,
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
        let parsed =
            coven_keys::encryption::MasterKeyring::from_serialized(keyring).map_err(|error| {
                DbError::context(
                    format!("parse Circle {circle_id} historical package keyring"),
                    error,
                )
            })?;
        let encryption = EncryptionService::from(parsed);
        if encryption
            .service_for_fingerprint(expected_key_fingerprint.as_bytes())
            .is_err()
        {
            return Ok(None);
        }
        Ok(Some(keyring.clone()))
    }

    fn circle_current_state_with_activations(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<Option<coven_protocol::circle_activation::CircleCurrentState>, DbError> {
        use coven_protocol::circle_activation::CircleCurrentState;

        let records = StoreRecords::new(self.conn, self.store_dir);
        let mut pending = std::collections::BTreeMap::new();
        for activation in activations
            .iter()
            .filter(|activation| activation.circle_id == circle_id)
        {
            if activation.control.value.store_root_hash != root.store_root_hash
                || activation.reference.circle_id() != circle_id
                || activation.reference.control() != &activation.control.coord
            {
                return Err(DbError::Message(
                    "prepared Circle activation differs from its Store or control reference"
                        .to_string(),
                ));
            }
            let next = CircleCurrentState::from_verified_reference(activation)?;
            let coordinate = activation.control.coord.clone();
            if let Some((prior, _)) = pending.insert(coordinate.clone(), (activation, next)) {
                if prior != activation {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} prepared history has conflicting copies of control {coordinate:?}"
                    )));
                }
            }
        }
        let dependencies = pending
            .iter()
            .map(|(coordinate, (activation, _))| {
                let dependencies = activation
                    .control
                    .value
                    .access_epoch()
                    .covered_control_heads
                    .iter()
                    .map(|head| &head.coord)
                    .filter(|coordinate| pending.contains_key(*coordinate))
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>();
                (coordinate.clone(), dependencies)
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut applied = std::collections::BTreeSet::new();
        let mut state = super::circle_operations::circle_current_state_on(self.conn, circle_id)?;
        while !pending.is_empty() {
            let coordinate = coven_protocol::causal_grants::canonical_ready_node(
                pending
                    .keys()
                    .map(|coordinate| (coordinate, &dependencies[coordinate])),
                &applied,
            )
            .ok_or_else(|| {
                DbError::Message(format!(
                    "Circle {circle_id} prepared controls contain a causal cycle"
                ))
            })?;
            let (activation, next) = pending.remove(&coordinate).ok_or_else(|| {
                DbError::Message("ready prepared Circle control is absent".to_string())
            })?;
            applied.insert(coordinate);
            let Some(current) = state.take() else {
                if !activation.control.value.is_founder() {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} current state is absent for a prepared successor"
                    )));
                }
                state = Some(next);
                continue;
            };
            if current
                .resolved_control()
                .is_some_and(|head| head.coordinate() == &activation.control.coord)
            {
                // Snapshot recipient access can enrich the installed public
                // control without publishing that control a second time.
                state = Some(if activation.local_access.is_some() {
                    next
                } else {
                    current
                });
                continue;
            }
            let heads = match &current {
                CircleCurrentState::ControlConflict { branches } => branches
                    .iter()
                    .map(|branch| branch.coordinate())
                    .collect::<Vec<_>>(),
                _ => vec![current
                    .resolved_control()
                    .ok_or_else(|| {
                        DbError::Message("resolved Circle control is absent".to_string())
                    })?
                    .coordinate()],
            };
            let mut already_covered = false;
            for head in heads {
                let covering = verified_circle_activation_with_prefix_on(
                    records,
                    self.verified_store_authority,
                    root,
                    circle_id,
                    head,
                    activations,
                )?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle {circle_id} current control has no verified activation"
                    ))
                })?;
                if verified_circle_control_covers_with_prefix_on(
                    records,
                    self.verified_store_authority,
                    root,
                    circle_id,
                    &covering.control,
                    &activation.control.coord,
                    activations,
                )? {
                    already_covered = true;
                    break;
                }
            }
            state = Some(if already_covered {
                current
            } else {
                current.advance(next)?
            });
        }
        Ok(state)
    }

    fn verified_circle_activation_context(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<(
            coven_protocol::circle_activation::VerifiedCircleReference,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        let Some(commit) = circle_activation_commit_ref_on(self.conn, circle_id, control)? else {
            return Ok(None);
        };
        let activation = StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            control,
        )?
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} activation context lost control {control:?}"
            ))
        })?;
        Ok(Some((activation, commit)))
    }

    fn circle_blob_opening_protection(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        circle_blob_opening_protection_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            expected_control,
            expected_key_fingerprint,
        )
    }

    fn verified_circle_activation(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            control,
        )
    }

    fn circle_restore_head(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        controls: &[coven_protocol::circle::CircleControlCoord],
    ) -> Result<
        Option<(
            coven_protocol::circle::CircleControlCoord,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        let Some(head) = StoreDatabase::head_circle_control_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            controls,
        )?
        else {
            return Ok(None);
        };
        let commit =
            circle_activation_commit_ref_on(self.conn, circle_id, &head)?.ok_or_else(|| {
                DbError::Message(format!(
                    "Circle {circle_id} head control has no activating commit"
                ))
            })?;
        Ok(Some((head, commit)))
    }

    fn retained_circle_activation_commit_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        retained_circle_activation_commit_ref_on(self.conn, circle_id, control)
    }

    fn verified_circle_control_coord_covers(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        covering: &coven_protocol::circle::CircleControlCoord,
        covered: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        let Some(reference) = StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            covering,
        )?
        else {
            return Ok(false);
        };
        StoreDatabase::verified_circle_control_covers_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            &reference.control,
            covered,
        )
    }

    fn verified_circle_control_covers(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: &coven_protocol::circle::PreparedCircleControl,
        prior: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        StoreDatabase::verified_circle_control_covers_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            current,
            prior,
        )
    }

    fn verify_circle_bootstrap_blob_authority(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        current: &coven_protocol::circle::PreparedCircleControl,
        blobs: &[coven_protocol::blob::RowBlobRef],
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        let records = StoreRecords::new(self.conn, self.store_dir);
        for binding in blobs {
            let coven_protocol::blob::RowBlobAuthority::Remote(
                coven_protocol::audience_package::PackageAudience::Circle {
                    circle_id,
                    control,
                    key_fingerprint,
                },
            ) = binding.authority()
            else {
                return Err(DbError::Message(
                    "Circle bootstrap row blob lacks Circle package authority".to_string(),
                ));
            };
            let activation = verified_circle_activation_with_prefix_on(
                records,
                self.verified_store_authority,
                root,
                *circle_id,
                control,
                activations,
            )?
            .ok_or_else(|| {
                DbError::Message("Circle bootstrap blob authority is not retained".to_string())
            })?;
            if *key_fingerprint != activation.control.value.key_fingerprint()
                || !verified_circle_control_covers_with_prefix_on(
                    records,
                    self.verified_store_authority,
                    root,
                    *circle_id,
                    current,
                    control,
                    activations,
                )?
            {
                return Err(DbError::Message(
                    "Circle bootstrap blob authority is outside its control history".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl StoreDatabase {
    /// Verify bootstrap blob authority against retained controls and the caller's
    /// verified candidate predecessor controls in one ancestry traversal owner.
    pub async fn verify_circle_bootstrap_blob_authority(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        current: coven_protocol::circle::PreparedCircleControl,
        blobs: Vec<coven_protocol::blob::RowBlobRef>,
        activations: Vec<coven_protocol::circle_activation::VerifiedCircleReference>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.verify_circle_bootstrap_blob_authority(&root, &current, &blobs, &activations)
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
        self.call_store(move |session| {
            session.circle_control_covers_strictly(&root, circle_id, &covering, &covered)
        })
        .await
    }

    pub async fn circle_epoch_access(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::CircleEpochAccess>, DbError> {
        self.call_store(move |session| {
            session.circle_epoch_access(&root, circle_id, &expected_control)
        })
        .await
    }

    pub async fn circle_package_access(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
        activations: Vec<coven_protocol::circle_activation::VerifiedCircleReference>,
    ) -> Result<Option<CirclePackageAccess>, DbError> {
        self.call_store(move |session| {
            session.circle_package_access(
                &root,
                circle_id,
                &expected_control,
                expected_key_fingerprint,
                &activations,
            )
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
        self.call_store(move |session| {
            session.verified_circle_activation_context(&root, circle_id, &control)
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
        self.call_store(move |session| {
            session.circle_blob_opening_protection(
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
        self.call_store(move |session| {
            session.verified_circle_activation(&root, circle_id, &control)
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
        self.call_store(move |session| session.circle_restore_head(&root, circle_id, &controls))
            .await
    }

    pub async fn retained_circle_activation_commit_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        self.call_store(move |session| {
            session.retained_circle_activation_commit_ref(circle_id, &control)
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
        self.call_store(move |session| {
            session.verified_circle_control_coord_covers(&root, circle_id, &covering, &covered)
        })
        .await
    }

    /// The head control of a Circle: the retained control whose lineage no other
    /// retained control covers. Restore resolves the restoring identity's current
    /// access at the head control's activating commit, so a member removed by a
    /// later epoch close resolves against the successor control that excludes them
    /// — never against a stale predecessor that still lists them active. A Circle
    /// with two uncovered controls is a forked lineage and fails loud.
    pub(super) fn head_circle_control_on(
        records: StoreRecords<'_>,
        authority: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        controls: &[coven_protocol::circle::CircleControlCoord],
    ) -> Result<Option<coven_protocol::circle::CircleControlCoord>, DbError> {
        // A control whose activating commit was reclaimed is superseded by a later
        // epoch and cannot be head; keep only controls whose commit is retained.
        let mut retained: Vec<(
            coven_protocol::circle::CircleControlCoord,
            coven_protocol::circle::PreparedCircleControl,
        )> = Vec::new();
        for coord in controls {
            let Some(activation_commit) =
                records.retained_circle_activation_commit_ref(circle_id, coord)?
            else {
                continue;
            };
            let materialization =
                authority.retained_materialization_by_ref_on(records, &activation_commit)?;
            if materialization.root() != root {
                return Err(DbError::Message(
                    "Circle activation belongs to another Store root".to_string(),
                ));
            }
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
                    authority,
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

    pub(super) fn verified_circle_activation_on(
        records: StoreRecords<'_>,
        authority: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        let Some(activation_commit) = records.circle_activation_commit_ref(circle_id, control)?
        else {
            return Ok(None);
        };
        let retained = authority.retained_materialization_by_ref_on(records, &activation_commit)?;
        if retained.root() != root {
            return Err(DbError::Message(
                "Circle activation belongs to another Store root".to_string(),
            ));
        }
        retained.circle_activation(circle_id, control).map(Some)
    }

    pub async fn verified_circle_control_covers(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: coven_protocol::circle::PreparedCircleControl,
        prior: coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.verified_circle_control_covers(&root, circle_id, &current, &prior)
        })
        .await
    }

    pub(super) fn verified_circle_control_covers_on(
        records: StoreRecords<'_>,
        authority: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: &coven_protocol::circle::PreparedCircleControl,
        prior: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        verified_circle_control_covers_with_prefix_on(
            records,
            authority,
            root,
            circle_id,
            current,
            prior,
            &[],
        )
    }
}

fn verified_circle_activation_with_prefix_on(
    records: StoreRecords<'_>,
    authority: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    circle_id: coven_protocol::circle::CircleId,
    coordinate: &coven_protocol::circle::CircleControlCoord,
    activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
    let mut matching = activations.iter().filter(|activation| {
        activation.circle_id == circle_id && &activation.control.coord == coordinate
    });
    if let Some(activation) = matching.next() {
        if activation.control.value.store_root_hash != root.store_root_hash
            || activation.reference.circle_id() != circle_id
            || activation.reference.control() != coordinate
            || !activation.control.verify()
        {
            return Err(DbError::Message(
                "prepared Circle lineage differs from its Store or control reference".to_string(),
            ));
        }
        if matching.any(|other| other != activation) {
            return Err(DbError::Message(format!(
                "Circle {circle_id} prepared history has conflicting copies of control {coordinate:?}"
            )));
        }
        return Ok(Some(activation.clone()));
    }
    StoreDatabase::verified_circle_activation_on(records, authority, root, circle_id, coordinate)
}

fn verified_circle_control_covers_with_prefix_on(
    records: StoreRecords<'_>,
    authority: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    circle_id: coven_protocol::circle::CircleId,
    current: &coven_protocol::circle::PreparedCircleControl,
    prior: &coven_protocol::circle::CircleControlCoord,
    activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
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
        let predecessor = verified_circle_activation_with_prefix_on(
            records,
            authority,
            root,
            circle_id,
            &coordinate,
            activations,
        )?
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

pub(crate) fn circle_blob_opening_protection_on(
    records: StoreRecords<'_>,
    verified_store: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    circle_id: coven_protocol::circle::CircleId,
    expected_control: &coven_protocol::circle::CircleControlCoord,
    expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
    let Some(authority) = StoreDatabase::verified_circle_activation_on(
        records,
        verified_store,
        root,
        circle_id,
        expected_control,
    )?
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

    let controls = records.circle_controls(circle_id)?;

    let mut retained_key = None;
    for control in controls {
        let activation = StoreDatabase::verified_circle_activation_on(
            records,
            verified_store,
            root,
            circle_id,
            &control,
        )?
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} activation index lost control {control:?}"
            ))
        })?;
        let Some((generation, key)) = activation
            .retained_key_entry(expected_key_fingerprint)
            .map_err(DbError::from)?
        else {
            continue;
        };
        let candidate = EncryptionService::from_key_at_generation(generation, key);
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
    retained_key
        .map(coven_protocol::objects::BlobSpoolProtection::Opaque)
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} retains no local key for fingerprint \
                     {expected_key_fingerprint}"
            ))
        })
}

impl crate::store::store_session::StoreTransaction<'_, '_> {
    pub(super) fn circle_blob_opening_protection(
        self,
        verified_store: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        circle_blob_opening_protection_on(
            self.records(),
            verified_store,
            root,
            circle_id,
            expected_control,
            expected_key_fingerprint,
        )
    }
}
