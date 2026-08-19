use crate::query_mapped_rows;
use crate::*;
use coven_protocol::circle::{
    CircleBootstrapCoverageRef, CircleControlCoord, CircleEpochId, CircleId,
};
use coven_protocol::objects::PreparedExactObject;
use coven_protocol::store_commit::{
    CircleAck, CircleAckRef, CommitFrontier, StoreDeviceId, StoreDeviceStatus, StoreHistoryCut,
};
use rusqlite::OptionalExtension;
use std::collections::{BTreeMap, BTreeSet};

use super::{StoreDatabase, StoreSession};

/// Everything one active Circle contributes to staging its device's next Circle
/// acknowledgement: the exact control and epoch the live projection derives from,
/// the access authority that seals the acknowledgement, and the retained bootstrap
/// coverage the projection was seeded from (`None` for a founder/source device).
pub struct CircleAckPublicationInput {
    circle_id: CircleId,
    control: CircleControlCoord,
    epoch_id: CircleEpochId,
    access: coven_protocol::circle_activation::CircleEpochAccess,
    seeded_from: Option<CircleBootstrapCoverageRef>,
}

impl CircleAckPublicationInput {
    pub fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    pub fn control(&self) -> &CircleControlCoord {
        &self.control
    }

    pub fn epoch_id(&self) -> CircleEpochId {
        self.epoch_id
    }

    pub fn seeded_from(&self) -> Option<&CircleBootstrapCoverageRef> {
        self.seeded_from.as_ref()
    }

    pub fn protocol_context(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        domain: coven_protocol::objects::CircleProtocolObjectDomain,
    ) -> coven_protocol::objects::ProtocolObjectContext {
        self.access.protocol_context(store_root_hash, domain)
    }

    pub fn key_fingerprint(&self) -> coven_keys::encryption::KeyFingerprint {
        self.access.key_fingerprint()
    }
}

/// The last Circle acknowledgement this device published for one Circle: its
/// exact reference, the successor slot its next acknowledgement occupies, and the
/// coverage it named (used to skip re-staging an unchanged acknowledgement).
pub struct PublishedCircleAck {
    pub reference: CircleAckRef,
    pub successor_slot: coven_protocol::objects::ObjectSlot,
    pub store_cut: CommitFrontier,
    pub control: CircleControlCoord,
}

impl StoreSession<'_> {
    fn circle_acknowledgement_publication_inputs(
        &self,
    ) -> Result<Vec<CircleAckPublicationInput>, DbError> {
        let conn = self.conn;
        let mut inputs = Vec::new();
        for state in super::circle_operations::circle_current_states_on(conn)? {
            let circle_id = state.circle_id();
            let Some(authoring) = state.authoring_state() else {
                tracing::debug!(
                    circle_id = %circle_id,
                    "skip Circle acknowledgement: recipient holds no active access"
                );
                continue;
            };
            let control = authoring.control.coord.clone();
            let epoch_id = authoring.control.value.epoch_id();
            let access = super::circle_publication_context_on(conn, circle_id, &control)?;
            let seeded_from =
                super::retained_merge_replay::circle_bootstrap_coverage_ref_on(conn, circle_id)?;
            inputs.push(CircleAckPublicationInput {
                circle_id,
                control,
                epoch_id,
                access,
                seeded_from,
            });
        }
        Ok(inputs)
    }

    fn activated_circle_ack(
        &self,
        circle_id: CircleId,
        device_id: StoreDeviceId,
    ) -> Result<Option<CircleAckRef>, DbError> {
        self.conn
            .query_row(
                "SELECT ack_ref FROM activated_circle_acks
                 WHERE circle_id = ?1 AND device_id = ?2",
                rusqlite::params![circle_id.to_string(), device_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|raw| parse_circle_ack_ref(&raw, circle_id, "activated"))
            .transpose()
    }

    fn circle_current_roster_members(
        &self,
        circle_id: CircleId,
    ) -> Result<BTreeSet<String>, DbError> {
        let Some(state) = super::circle_operations::circle_current_state_on(self.conn, circle_id)?
        else {
            return Err(DbError::Message(format!(
                "Circle {circle_id} has no current state"
            )));
        };
        let Some((_current, _access, roster, _metadata)) = state.active() else {
            return Ok(BTreeSet::new());
        };
        Ok(roster.members().into_keys().collect())
    }

    fn activated_circle_acks(&self, circle_id: CircleId) -> Result<Vec<CircleAckRef>, DbError> {
        query_mapped_rows(
            self.conn,
            "SELECT ack_ref FROM activated_circle_acks
             WHERE circle_id = ?1 ORDER BY device_id",
            [circle_id.to_string()],
            |row| row.get::<_, String>(0),
        )?
        .into_iter()
        .map(|raw| parse_circle_ack_ref(&raw, circle_id, "activated"))
        .collect()
    }

    fn latest_published_circle_ack(
        &self,
        circle_id: CircleId,
    ) -> Result<Option<PublishedCircleAck>, DbError> {
        let row: Option<(String, String, String, String)> = self
            .conn
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
        let reference = parse_circle_ack_ref(&reference, circle_id, "published")?;
        if reference.sequence == 0 {
            return Err(DbError::Message(
                "published Circle acknowledgement names sequence zero".to_string(),
            ));
        }
        Ok(Some(PublishedCircleAck {
            reference,
            successor_slot: serde_json::from_str(&successor_slot).map_err(|error| {
                DbError::context("published Circle acknowledgement successor slot", error)
            })?,
            store_cut: serde_json::from_str(&store_cut)
                .map_err(|error| DbError::context("published Circle acknowledgement cut", error))?,
            control: serde_json::from_str(&control).map_err(|error| {
                DbError::context("published Circle acknowledgement control", error)
            })?,
        }))
    }

    /// Whether any Circle acknowledgement is waiting to be published.
    ///
    /// A Store acknowledgement is what carries them to the cloud, so it stages
    /// itself when any are queued even if it has nothing of its own to say.
    fn outbound_circle_acks_pending(&self) -> Result<bool, DbError> {
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM outbound_circle_acks)",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(DbError::from)
    }

    fn stage_circle_ack(
        &mut self,
        ack: CircleAck,
        prepared: PreparedExactObject,
    ) -> Result<CircleAckRef, DbError> {
        let authority = self.local_store_authority()?;
        let registration = authority.value();
        let bytes = ack.to_bytes();
        let reference = CircleAckRef {
            registration: ack.registration.clone(),
            circle_id: ack.circle_id,
            control: ack.control.clone(),
            sequence: ack.sequence,
            ack_hash: ack.ack_hash(),
            object: prepared.reference().clone(),
        };
        let verified =
            CircleAck::parse_at(&bytes, &registration.store_root, &reference, registration)
                .map_err(|error| DbError::context("stage Circle acknowledgement", error))?;
        if verified != ack {
            return Err(DbError::Message(
                "staged Circle acknowledgement changed during exact verification".to_string(),
            ));
        }
        let ack_ref = serde_json::to_string(&reference).map_err(|error| {
            DbError::context("serialize exact Circle acknowledgement ref", error)
        })?;
        let prepared = serde_json::to_string(&prepared).map_err(|error| {
            DbError::context("serialize prepared Circle acknowledgement", error)
        })?;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO outbound_circle_acks (circle_id, ack_ref, ack_bytes, prepared_object)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![reference.circle_id.to_string(), ack_ref, bytes, prepared],
        )
        .map_err(DbError::from)?;
        tx.commit().map_err(DbError::from)?;
        Ok(reference)
    }
}

fn parse_circle_ack_ref(
    raw: &str,
    circle_id: CircleId,
    state: &str,
) -> Result<CircleAckRef, DbError> {
    let reference: CircleAckRef = serde_json::from_str(raw)
        .map_err(|error| DbError::context(format!("{state} Circle acknowledgement ref"), error))?;
    if reference.circle_id != circle_id {
        return Err(DbError::Message(format!(
            "{state} Circle acknowledgement names another Circle"
        )));
    }
    Ok(reference)
}

impl StoreDatabase {
    pub async fn circle_acknowledgement_publication_inputs(
        &self,
    ) -> Result<Vec<CircleAckPublicationInput>, DbError> {
        self.call_store(|session| session.circle_acknowledgement_publication_inputs())
            .await
    }

    /// The latest activated Circle acknowledgement `device_id` published for
    /// `circle_id`, or `None` if that device has never had an acknowledgement
    /// activated. Snapshot stability reads this per access-holding device.
    pub async fn activated_circle_ack(
        &self,
        circle_id: CircleId,
        device_id: StoreDeviceId,
    ) -> Result<Option<CircleAckRef>, DbError> {
        self.call_store(move |session| session.activated_circle_ack(circle_id, device_id))
            .await
    }

    /// The device ids that currently hold active Circle access to `circle_id`:
    /// every active Store device whose owner is a current member of the Circle's
    /// resolved roster. Snapshot stability requires each of these devices to have
    /// published a dominating acknowledgement, so a device that holds access but
    /// has never acknowledged keeps the snapshot unstable (fail closed). A
    /// `Closing`/`Inactive`/conflicted Circle authors no snapshot and returns an
    /// empty set.
    pub async fn active_circle_access_devices(
        &self,
        circle_id: CircleId,
    ) -> Result<BTreeSet<StoreDeviceId>, DbError> {
        let members = self.circle_current_roster_members(circle_id).await?;
        if members.is_empty() {
            return Ok(BTreeSet::new());
        }
        let frontier = CommitFrontier::from_refs(self.materialized_frontier().await?)
            .map_err(|error| DbError::context("shape current Store frontier", error))?;
        let (_, device_state) = self
            .store_device_state_for_history_cut(&StoreHistoryCut(frontier.0))
            .await?;
        let owners: BTreeMap<StoreDeviceId, String> = self
            .activated_store_device_registration_records()
            .await?
            .into_iter()
            .map(|registration| {
                (
                    registration.value().device_id,
                    registration.value().author_pubkey.clone(),
                )
            })
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
    pub async fn circle_current_roster_members(
        &self,
        circle_id: CircleId,
    ) -> Result<BTreeSet<String>, DbError> {
        self.call_store(move |session| session.circle_current_roster_members(circle_id))
            .await
    }

    /// The latest activated Circle acknowledgement every device that has ever
    /// acknowledged `circle_id` published — one per device, including devices whose
    /// owner has since been removed from the roster (rows are never deleted, so a
    /// removed recipient's last acknowledgement persists as the evidence bootstrap
    /// reclamation reads to prove that recipient lost authority).
    pub async fn activated_circle_acks(
        &self,
        circle_id: CircleId,
    ) -> Result<Vec<CircleAckRef>, DbError> {
        self.call_store(move |session| session.activated_circle_acks(circle_id))
            .await
    }

    pub async fn latest_published_circle_ack(
        &self,
        circle_id: CircleId,
    ) -> Result<Option<PublishedCircleAck>, DbError> {
        self.call_store(move |session| session.latest_published_circle_ack(circle_id))
            .await
    }

    pub async fn outbound_circle_acks_pending(&self) -> Result<bool, DbError> {
        self.call_store(|session| session.outbound_circle_acks_pending())
            .await
    }

    pub async fn stage_circle_ack(
        &self,
        ack: CircleAck,
        prepared: PreparedExactObject,
    ) -> Result<CircleAckRef, DbError> {
        self.call_store(move |session| session.stage_circle_ack(ack, prepared))
            .await
    }
}
