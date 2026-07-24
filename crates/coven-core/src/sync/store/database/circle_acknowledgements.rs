use crate::database::*;
use crate::sync::circle::{
    CircleBootstrapCoverageRef, CircleControlCoord, CircleEpochId, CircleId,
};
use crate::sync::storage::PreparedExactObject;
use crate::sync::store::circle_controls::activation::CircleCurrentState;
use crate::sync::store_commit::{
    CircleAck, CircleAckRef, CommitFrontier, StoreDeviceId, StoreDeviceStatus, StoreHistoryCut,
};
use crate::KeyFingerprint;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

use super::StoreDatabase;

/// Everything one active Circle contributes to staging its device's next Circle
/// acknowledgement: the exact control and epoch the live projection derives from,
/// the epoch key that seals the acknowledgement, and the retained bootstrap
/// coverage the projection was seeded from (`None` for a founder/source device).
pub(crate) struct CircleAckPublicationInput {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    pub encryption: crate::encryption::EncryptionService,
    pub seeded_from: Option<CircleBootstrapCoverageRef>,
}

/// The last Circle acknowledgement this device published for one Circle: its
/// exact reference, the successor slot its next acknowledgement occupies, and the
/// coverage it named (used to skip re-staging an unchanged acknowledgement).
pub(crate) struct PublishedCircleAck {
    pub reference: CircleAckRef,
    pub successor_slot: crate::storage::cloud::ObjectSlot,
    pub store_cut: CommitFrontier,
    pub control: CircleControlCoord,
}

impl StoreDatabase {
    pub(crate) async fn circle_acknowledgement_publication_inputs(
        &self,
    ) -> Result<Vec<CircleAckPublicationInput>, DbError> {
        self.sqlite()
            .call(Self::circle_acknowledgement_publication_inputs_on)
            .await
    }

    fn circle_acknowledgement_publication_inputs_on(
        conn: &Connection,
    ) -> Result<Vec<CircleAckPublicationInput>, DbError> {
        let mut statement = conn
            .prepare("SELECT circle_id, state FROM circle_current_state ORDER BY circle_id")
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut inputs = Vec::new();
        for (encoded_circle_id, payload) in rows {
            let circle_id: CircleId = encoded_circle_id
                .parse()
                .map_err(|error| DbError::Message(format!("parse current Circle id: {error}")))?;
            let state: CircleCurrentState = serde_json::from_slice(&payload).map_err(|error| {
                DbError::Message(format!("parse Circle current state: {error}"))
            })?;
            if !state.verify() || state.circle_id() != circle_id {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} has invalid current state"
                )));
            }
            // Only an active projection holds private history to acknowledge; an
            // inactive recipient has no access leaf and stages nothing.
            let Some(authoring) = state.authoring_state() else {
                tracing::debug!(
                    circle_id = %circle_id,
                    "skip Circle acknowledgement: recipient holds no active access"
                );
                continue;
            };
            let control = authoring.control.coord.clone();
            let epoch_id = authoring.control.value.epoch_id();
            let (encryption, key_fingerprint) =
                Self::circle_publication_context_on(conn, circle_id, &control)?;
            let seeded_from = Self::circle_bootstrap_coverage_ref_on(conn, circle_id)?;
            inputs.push(CircleAckPublicationInput {
                circle_id,
                control,
                epoch_id,
                key_fingerprint,
                encryption,
                seeded_from,
            });
        }
        Ok(inputs)
    }

    /// The latest activated Circle acknowledgement `device_id` published for
    /// `circle_id`, or `None` if that device has never had an acknowledgement
    /// activated. Snapshot stability reads this per access-holding device.
    pub(crate) async fn activated_circle_ack(
        &self,
        circle_id: CircleId,
        device_id: StoreDeviceId,
    ) -> Result<Option<CircleAckRef>, DbError> {
        self.sqlite()
            .call(move |conn| {
                conn.query_row(
                    "SELECT ack_ref FROM activated_circle_acks
                     WHERE circle_id = ?1 AND device_id = ?2",
                    rusqlite::params![circle_id.to_string(), device_id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(DbError::from)?
                .map(|raw| {
                    let reference: CircleAckRef = serde_json::from_str(&raw).map_err(|error| {
                        DbError::Message(format!("activated Circle acknowledgement ref: {error}"))
                    })?;
                    if reference.circle_id != circle_id {
                        return Err(DbError::Message(
                            "activated Circle acknowledgement names another Circle".to_string(),
                        ));
                    }
                    Ok(reference)
                })
                .transpose()
            })
            .await
    }

    /// The device ids that currently hold active Circle access to `circle_id`:
    /// every active Store device whose owner is a current member of the Circle's
    /// resolved roster. Snapshot stability requires each of these devices to have
    /// published a dominating acknowledgement, so a device that holds access but
    /// has never acknowledged keeps the snapshot unstable (fail closed). A
    /// `Closing`/`Inactive`/conflicted Circle authors no snapshot and returns an
    /// empty set.
    pub(crate) async fn active_circle_access_devices(
        &self,
        circle_id: CircleId,
    ) -> Result<BTreeSet<StoreDeviceId>, DbError> {
        let members = self.circle_current_roster_members(circle_id).await?;
        if members.is_empty() {
            return Ok(BTreeSet::new());
        }
        let frontier = CommitFrontier::from_refs(self.materialized_frontier().await?)
            .map_err(|error| DbError::Message(format!("shape current Store frontier: {error}")))?;
        let (_, device_state) = self
            .store_device_state_for_history_cut(&StoreHistoryCut(frontier.0))
            .await?;
        let owners: BTreeMap<StoreDeviceId, String> = self
            .activated_store_device_registration_records()
            .await?
            .into_iter()
            .map(|(_, registration)| (registration.device_id, registration.author_pubkey))
            .collect();
        let mut devices = BTreeSet::new();
        for (device_id, record) in device_state.devices {
            if !matches!(record.status, StoreDeviceStatus::Active) {
                continue;
            }
            let owner = owners.get(&device_id).ok_or_else(|| {
                DbError::Message(format!(
                    "active Store device {device_id} has no activated registration"
                ))
            })?;
            if members.contains(owner) {
                devices.insert(device_id);
            }
        }
        Ok(devices)
    }

    /// The pubkeys in `circle_id`'s current resolved roster, or an empty set when
    /// the Circle is not in an active local state (so it has no snapshot quorum).
    async fn circle_current_roster_members(
        &self,
        circle_id: CircleId,
    ) -> Result<BTreeSet<String>, DbError> {
        self.sqlite()
            .call(move |conn| {
                let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} has no current state"
                    )));
                };
                let Some((_current, _access, roster, _metadata)) = state.active() else {
                    return Ok(BTreeSet::new());
                };
                Ok(roster.members().into_keys().collect())
            })
            .await
    }

    /// The latest activated Circle acknowledgement each device that holds active
    /// access to `circle_id` has published — one per device. An inactive recipient
    /// holds no access and therefore publishes no acknowledgement, so it never
    /// appears here.
    #[cfg(test)]
    pub(crate) async fn activated_circle_acks(
        &self,
        circle_id: CircleId,
    ) -> Result<Vec<CircleAckRef>, DbError> {
        self.sqlite()
            .call(move |conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT ack_ref FROM activated_circle_acks \
                         WHERE circle_id = ?1 ORDER BY device_id",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([circle_id.to_string()], |row| row.get::<_, String>(0))
                    .map_err(DbError::from)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(DbError::from)?;
                drop(statement);
                rows.into_iter()
                    .map(|raw| {
                        let reference: CircleAckRef =
                            serde_json::from_str(&raw).map_err(|error| {
                                DbError::Message(format!(
                                    "activated Circle acknowledgement ref: {error}"
                                ))
                            })?;
                        if reference.circle_id != circle_id {
                            return Err(DbError::Message(
                                "activated Circle acknowledgement names another Circle".to_string(),
                            ));
                        }
                        Ok(reference)
                    })
                    .collect()
            })
            .await
    }

    pub(crate) async fn latest_published_circle_ack(
        &self,
        circle_id: CircleId,
    ) -> Result<Option<PublishedCircleAck>, DbError> {
        self.sqlite()
            .call(move |conn| Self::latest_published_circle_ack_on(conn, circle_id))
            .await
    }

    fn latest_published_circle_ack_on(
        conn: &Connection,
        circle_id: CircleId,
    ) -> Result<Option<PublishedCircleAck>, DbError> {
        let row: Option<(String, String, String, String)> = conn
            .query_row(
                "SELECT ack_ref, successor_slot, store_cut, control_coord
                 FROM published_circle_acks WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((reference, successor_slot, store_cut, control)) = row else {
            return Ok(None);
        };
        let reference: CircleAckRef = serde_json::from_str(&reference).map_err(|error| {
            DbError::Message(format!("published Circle acknowledgement ref: {error}"))
        })?;
        if reference.circle_id != circle_id || reference.sequence == 0 {
            return Err(DbError::Message(
                "published Circle acknowledgement names another Circle or sequence zero"
                    .to_string(),
            ));
        }
        Ok(Some(PublishedCircleAck {
            reference,
            successor_slot: serde_json::from_str(&successor_slot).map_err(|error| {
                DbError::Message(format!(
                    "published Circle acknowledgement successor slot: {error}"
                ))
            })?,
            store_cut: serde_json::from_str(&store_cut).map_err(|error| {
                DbError::Message(format!("published Circle acknowledgement cut: {error}"))
            })?,
            control: serde_json::from_str(&control).map_err(|error| {
                DbError::Message(format!("published Circle acknowledgement control: {error}"))
            })?,
        }))
    }

    pub(crate) async fn stage_circle_ack(
        &self,
        ack: CircleAck,
        prepared: PreparedExactObject,
    ) -> Result<CircleAckRef, DbError> {
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let bytes = ack.to_bytes();
                let (root, _, registration) = local_store_authority_on(&tx)?;
                let reference = CircleAckRef {
                    registration: ack.registration.clone(),
                    circle_id: ack.circle_id,
                    sequence: ack.sequence,
                    ack_hash: ack.ack_hash(),
                    object: prepared.reference().clone(),
                };
                let verified = CircleAck::parse_at(&bytes, &root, &reference, &registration)
                    .map_err(|error| {
                        DbError::Message(format!("stage Circle acknowledgement: {error}"))
                    })?;
                if verified != ack {
                    return Err(DbError::Message(
                        "staged Circle acknowledgement changed during exact verification"
                            .to_string(),
                    ));
                }
                let ack_ref = serde_json::to_string(&reference).map_err(|error| {
                    DbError::Message(format!("serialize exact Circle acknowledgement ref: {error}"))
                })?;
                let prepared = serde_json::to_string(&prepared).map_err(|error| {
                    DbError::Message(format!("serialize prepared Circle acknowledgement: {error}"))
                })?;
                tx.execute(
                    "INSERT INTO outbound_circle_acks (circle_id, ack_ref, ack_bytes, prepared_object)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![reference.circle_id.to_string(), ack_ref, bytes, prepared],
                )
                .map_err(DbError::from)?;
                tx.commit().map_err(DbError::from)?;
                Ok(reference)
            })
            .await
    }
}
