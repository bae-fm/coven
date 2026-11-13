//! The pairing on the device that shows its code: its journal row, the pass
//! that looks for the request and puts the handover or the marker in its
//! place, and the steps of a write that start, approve, decline and end it
//! (§13.2).
//!
//! A pairing is an invite to the showing device's own member whose approval
//! hands over the member's private pair. It claims no slot and records no
//! provider access, so nothing here starts another operation: the row alone
//! is the pairing, and the object at the request's one path is its storage.

use super::sequence_invites::{
    decode_request, kept_until, seconds, Expected, Opened, RequestChecks,
};
use super::{SequenceSync, Session};
use crate::sequence_sync::OperationState;
use crate::SyncError;
use coven_crypto::{InviteSecret, MemberIdentity, SecretText};
use coven_database::{
    operation_kind, DbError, Journal, NewOperation, OperationId, OperationRecord, SqlContext,
};
use coven_format::{codes::PairingCode, handover::Handover};
use coven_foundation::id_source::{DeviceId, IdSource, InviteId, MemberId};
use coven_storage::{CloudProvider, ObjectPath, RestoreStorage, S3Credentials, StorageFailure};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// How long a pairing lives after it is made, by the showing device's clock
/// (§13.2).
pub(crate) const LIFETIME: Duration = Duration::from_secs(30 * 60);

/// How a pairing ended, as the app is shown it (§13.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PairingEnd {
    /// This device applied the new device's add-device entry.
    Added { device: DeviceId, name: String },
    /// The person declined the request.
    Declined,
    /// The request failed a check.
    Refused,
    /// The pairing's lifetime ended first.
    Expired,
}

/// Where a pairing is, as its journal row encodes it.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum PairingState {
    /// The code is shown. The request, once read and checked, waits for the
    /// app: its exact plaintext.
    Waiting { request: Option<Vec<u8>> },
    /// This checked request is approved: `handover` is the object, sealed in
    /// the approving write, that takes the request's place.
    Approving { request: Vec<u8>, handover: Vec<u8> },
    /// The handover is in the request's place, waiting for history to show
    /// the new device registered.
    HandedOver { request: Vec<u8> },
    /// The pairing ended, and the app is shown how: the marker goes in the
    /// place of the request or the handover.
    Ending { how: PairingEnd },
    /// The app closed the screen: the marker goes in the place of the request
    /// or the handover, and then the row is finished.
    Forgetting,
    /// The marker is in place; the app reads how the pairing ended until it
    /// closes the screen.
    Ended { how: PairingEnd },
}

/// A pairing's journal row.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PairingWork {
    id: InviteId,
    /// The device that made the pairing, under which its request lives. A
    /// reset carries the pairing to an installation with another id, which
    /// ends the request where it is (§13.2, §19.3).
    device: DeviceId,
    /// The showing device's member, whose pair the handover carries.
    member: MemberId,
    secret: SecretText,
    expires_at: SystemTime,
    /// The code the app shows as a QR code, and the one it shares as text:
    /// the same on every provider but S3, where only the first holds the key.
    qr_code: SecretText,
    shared_code: SecretText,
    state: PairingState,
}

impl OperationState for PairingWork {}

impl PairingWork {
    pub(crate) fn read(record: &OperationRecord) -> Result<Self, SyncError> {
        Ok(serde_json::from_slice(&record.data)?)
    }

    fn secret(&self) -> Result<InviteSecret, SyncError> {
        Ok(InviteSecret::from_bytes(serde_json::from_str(self.secret.as_str())?))
    }

    fn path(&self, store: coven_foundation::id_source::StoreId) -> ObjectPath {
        ObjectPath::join_request(store, self.device, self.id)
    }

    /// Whether the pairing ends at its expiry: it waits for its request or
    /// the app, or has been approved.
    fn expires(&self) -> bool {
        matches!(
            self.state,
            PairingState::Waiting { .. }
                | PairingState::Approving { .. }
                | PairingState::HandedOver { .. }
        )
    }

    pub(crate) fn expired(&self, now: SystemTime) -> bool {
        self.expires() && now >= self.expires_at
    }

    /// End the pairing for `how`.
    fn end(&mut self, how: PairingEnd) {
        self.state = PairingState::Ending { how };
    }

    /// The app closed the screen, or the installation was reset: the marker
    /// goes in and the row is finished.
    pub(crate) fn forget(&mut self) {
        self.state = PairingState::Forgetting;
    }

    /// The pairing's expiry, while it can still end by it.
    pub(crate) fn expiry(&self) -> Option<SystemTime> {
        self.expires().then_some(self.expires_at)
    }

    /// End the pairing as its expiry does.
    pub(crate) fn expire(&mut self) {
        self.end(PairingEnd::Expired);
    }
}

/// A pairing a write made: what the screen that adds a device shows.
pub struct MadePairing {
    /// The pairing's id, which names its request's path.
    pub id: InviteId,
    /// The code to show as a QR code. On S3 it carries the key this device
    /// connects with.
    pub qr_code: String,
    /// The code to copy or share as text: the same, but with no key.
    pub shared_code: String,
    /// When the pairing ends, by this device's clock.
    pub expires_at: SystemTime,
}

/// Make a pairing, as a step of a write (§13.2). The write checks that this
/// device is registered and its member active in the slots this device has
/// applied, fixes a fresh secret and the codes, and journals the pairing,
/// with or without storage. On S3 `key`, the key this device connects with,
/// goes in the QR code and not in the shared one; an S3 pairing without it
/// fails with `InvalidConfiguration`. The pairing expires `LIFETIME` after
/// `now`.
pub(crate) fn make_pairing(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    held: &MemberIdentity,
    ids: &dyn IdSource,
    now: SystemTime,
    location: &coven_storage::StorageConfig,
    key: Option<S3Credentials>,
) -> Result<MadePairing, SyncError> {
    let expires_at = now.checked_add(LIFETIME).ok_or(
        StorageFailure::InvalidConfiguration
            .with_source("the clock cannot represent the pairing's expiry"),
    )?;
    let secret = InviteSecret::generate()?;
    let secret = SecretText::new(serde_json::to_string(secret.to_secret_bytes().as_bytes())?);
    let id = InviteId(ids.new_id());
    sql.journal_step(|journal| {
        let applied = journal.store_state()?;
        let member = crate::sequence_sync::registered(&applied, device, held)?;
        let store = applied.store.as_ref().ok_or(SyncError::NoStorage)?;
        let code = |storage: RestoreStorage| -> Result<SecretText, SyncError> {
            let text = PairingCode {
                store: store.id,
                expires_at: seconds(expires_at)?,
                pairing: id,
                member,
                secret: InviteSecret::from_bytes(serde_json::from_str(secret.as_str())?),
                showing: device,
                storage: storage.encode()?,
            }
            .to_text()?;
            Ok(SecretText::new(text.to_string()))
        };
        let shared_code = code(RestoreStorage::location_only(location.clone()))?;
        let qr_code = if location.provider() == CloudProvider::S3 {
            let key = key.ok_or_else(|| {
                StorageFailure::InvalidConfiguration
                    .with_source("this device holds no S3 key to give")
            })?;
            code(RestoreStorage { location: location.clone(), s3_key: Some(key) })?
        } else {
            SecretText::new(shared_code.as_str().to_owned())
        };
        let work = PairingWork {
            id,
            device,
            member,
            secret: SecretText::new(secret.as_str().to_owned()),
            expires_at,
            qr_code: SecretText::new(qr_code.as_str().to_owned()),
            shared_code: SecretText::new(shared_code.as_str().to_owned()),
            state: PairingState::Waiting { request: None },
        };
        journal.start_operation(&NewOperation {
            kind: operation_kind::PAIRING.into(),
            data: serde_json::to_vec(&work)?,
            started_by: "start_pairing".into(),
            pauses_writes: false,
        })?;
        Ok(MadePairing {
            id,
            qr_code: qr_code.as_str().to_owned(),
            shared_code: shared_code.as_str().to_owned(),
            expires_at,
        })
    })
}

/// Approve exactly this checked request, as a step of a write (§13.2), with
/// or without storage: `shown` is the request's plaintext the app was shown.
/// The step compares it with the request the pairing holds and seals the
/// member's current private pair, as `held` has it, to that request's
/// sealing key only when they are the same; a different request is refused,
/// never sealed to. The handover's bytes are fixed here, so a retry of the
/// pass that puts it in the request's place creates the same object. A
/// request the pairing no longer shows, and a pairing whose lifetime ended
/// by `now`, fail the step with `InvitationChanged`.
pub(crate) fn approve_request(
    sql: &SqlContext<'_, '_>,
    store: coven_foundation::id_source::StoreId,
    now: SystemTime,
    held: &MemberIdentity,
    pairing: InviteId,
    shown: &[u8],
) -> Result<OperationId, SyncError> {
    sql.journal_step(|journal| {
        let (record, mut work) = pairing_record(journal, pairing)?;
        if work.expired(now) {
            return Err(SyncError::InvitationChanged);
        }
        let PairingState::Waiting { request: Some(stored) } = &work.state else {
            return Err(SyncError::InvitationChanged);
        };
        if stored != shown {
            return Err(SyncError::InvitationChanged);
        }
        let stored = stored.clone();
        let applied = journal.store_state()?;
        let current = applied.active_member(&work.member).ok_or(SyncError::Removed)?.keys;
        let selected =
            held.activate(&current).ok_or(SyncError::MemberKeysReplaced { member: work.member })?;
        let sealing = decode_request(&stored)?.keys.sealing;
        let handover =
            Handover::seal(work.member, selected.keys(), &sealing, &work.path(store))?.encode();
        work.state = PairingState::Approving { request: stored, handover };
        journal.advance_operation(&work.update(&record)?)?;
        Ok(record.id)
    })
}

/// Decline exactly this checked request, as a step of a write: the pairing
/// ends as declined. A request the pairing no longer shows, an approved
/// pairing, and one whose lifetime ended by `now`, fail the step with
/// `InvitationChanged`.
pub(crate) fn decline_request(
    sql: &SqlContext<'_, '_>,
    now: SystemTime,
    pairing: InviteId,
    shown: &[u8],
) -> Result<OperationId, SyncError> {
    sql.journal_step(|journal| {
        let (record, mut work) = pairing_record(journal, pairing)?;
        if work.expired(now) {
            return Err(SyncError::InvitationChanged);
        }
        match &work.state {
            PairingState::Waiting { request: Some(stored) } if stored == shown => (),
            _ => return Err(SyncError::InvitationChanged),
        }
        work.end(PairingEnd::Declined);
        journal.advance_operation(&work.update(&record)?)?;
        Ok(record.id)
    })
}

/// The screen showing the pairing's code closed, as a step of a write: the
/// pairing ends at whatever stage it is, approved included, and is forgotten
/// once the marker is in the request's or handover's place. A pairing that
/// has ended is forgotten as it is; one already forgotten changes nothing.
pub(crate) fn end_pairing(
    sql: &SqlContext<'_, '_>,
    pairing: InviteId,
) -> Result<Option<OperationId>, SyncError> {
    sql.journal_step(|journal| {
        let (record, mut work) = match pairing_record(journal, pairing) {
            Ok(found) => found,
            Err(SyncError::InvitationChanged) => return Ok(None),
            Err(error) => return Err(error),
        };
        match work.state {
            PairingState::Forgetting => return Ok(None),
            // The marker is in place already: nothing is left to do.
            PairingState::Ended { .. } => journal.finish_operation(record.id)?,
            _ => {
                work.state = PairingState::Forgetting;
                journal.advance_operation(&work.update(&record)?)?;
            }
        }
        Ok(Some(record.id))
    })
}

/// The pairing's journal row and its state, as the write sees them; a
/// finished or unknown pairing fails with `InvitationChanged`.
fn pairing_record(
    journal: &Journal<'_>,
    pairing: InviteId,
) -> Result<(OperationRecord, PairingWork), SyncError> {
    for record in journal.operations(operation_kind::PAIRING)? {
        let work = PairingWork::read(&record)?;
        if work.id == pairing {
            return Ok((record, work));
        }
    }
    Err(SyncError::InvitationChanged)
}

/// Opening the store ends every pairing a closed handle or a crash left: no
/// screen shows one now. Each is forgotten once its marker is in place, and
/// one that had ended already is forgotten at once, all in one transaction
/// (§13.2).
pub async fn pairings_opened(database: &coven_database::Database) -> Result<(), DbError> {
    database
        .settle_operations(operation_kind::PAIRING, |records| {
            let mut updates = Vec::new();
            let mut finished = Vec::new();
            for record in records {
                let mut work: PairingWork =
                    serde_json::from_slice(&record.data).map_err(DbError::StateEncoding)?;
                match work.state {
                    PairingState::Forgetting => (),
                    PairingState::Ended { .. } => finished.push(record.id),
                    _ => {
                        work.state = PairingState::Forgetting;
                        updates.push(record.next_step(
                            serde_json::to_vec(&work).map_err(DbError::StateEncoding)?,
                        )?);
                    }
                }
            }
            Ok((updates, finished, ()))
        })
        .await
}

impl SequenceSync {
    /// Advance this device's pairings in a pass: look for each one's request,
    /// put the handover or the marker in its place, and register the new
    /// device as added. Every pass looks at every pairing; a pairing has no
    /// doubling wait (§13.2).
    pub(crate) async fn resume_pairings(&self, session: &Session) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != operation_kind::PAIRING {
                continue;
            }
            self.run_pairing(session, record.id).await?;
            self.state.check()?;
        }
        Ok(())
    }

    /// One pairing's attempt. Its failure is the pairing's pending record,
    /// saved only while the row is at the step the attempt read: the app or
    /// the expiry may have moved the pairing on, or forgotten it, since, and
    /// the failure then belongs to a step it has left. A failure that read no
    /// row fails the pass.
    async fn run_pairing(&self, session: &Session, id: OperationId) -> Result<(), SyncError> {
        let mut step = None;
        let Err(error) = self.advance_pairing(session, id, &mut step).await else {
            return Ok(());
        };
        let Some(step) = step else { return Err(error) };
        match self.database.operation(id).await? {
            Some(row) if row.last_step == step => self.record_pairing_error(id, step, &error).await,
            _ => Ok(()),
        }
    }

    /// Advance the pairing as far as it goes now. `step` follows the step of
    /// the row this attempt last read, which a failure belongs to.
    async fn advance_pairing(
        &self,
        session: &Session,
        id: OperationId,
        step: &mut Option<u32>,
    ) -> Result<(), SyncError> {
        loop {
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(coven_database::DbError::OperationChanged(id))?;
            *step = Some(record.last_step);
            let mut work = PairingWork::read(&record)?;
            let path = work.path(self.store());
            if work.expired(self.clock.now()) {
                work.expire();
                self.database.advance_operation(work.update(&record)?).await?;
                continue;
            }
            match &work.state {
                PairingState::Waiting { request: None } => {
                    let bytes = match self.storage.read(&path).await {
                        Ok(bytes) => bytes,
                        Err(error) if error.failure() == StorageFailure::NotFound => {
                            return self.found_nothing(id, record.last_step).await;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let secret = work.secret()?;
                    let checks = RequestChecks {
                        path: &path,
                        secret: &secret,
                        id: work.id,
                        member: work.member,
                        expected: Expected::OwnDevice,
                    };
                    match checks.open(&session.history.state, &bytes)? {
                        // A checked request waits for the app's decision:
                        // someone who saw the code may have sent it first.
                        Opened::Checked(request) => {
                            work.state = PairingState::Waiting { request: Some(request) };
                            self.database.advance_operation(work.update(&record)?).await?;
                            return Ok(());
                        }
                        // No attempt can pass it, and no other request can
                        // reach the one path while it is there: the pairing
                        // ends, as a decline ends it.
                        Opened::Refused(refusal) => {
                            tracing::warn!(?id, %refusal, "a pairing's request failed its checks");
                            work.end(PairingEnd::Refused);
                            self.database.advance_operation(work.update(&record)?).await?;
                        }
                    }
                }
                PairingState::Waiting { request: Some(_) } => {
                    return self.found_nothing(id, record.last_step).await;
                }
                PairingState::Approving { request, handover } => {
                    self.place_handover(&path, handover).await?;
                    let request = request.clone();
                    work.state = PairingState::HandedOver { request };
                    self.database.advance_operation(work.update(&record)?).await?;
                }
                PairingState::HandedOver { request } => {
                    // The handover stays until history shows the device the
                    // request named registered under this member, as an
                    // add-device entry by that member does.
                    let request = decode_request(request)?;
                    let registered = session
                        .history
                        .state
                        .devices
                        .get(&request.device)
                        .is_some_and(|device| device.member == work.member);
                    if !registered {
                        return self.found_nothing(id, record.last_step).await;
                    }
                    work.end(PairingEnd::Added {
                        device: request.device,
                        name: request.device_name,
                    });
                    self.database.advance_operation(work.update(&record)?).await?;
                }
                PairingState::Ending { .. } | PairingState::Forgetting => {
                    // The marker goes once a new device can no longer send a
                    // first request (`put_marker`).
                    let kept_until = kept_until(seconds(work.expires_at)?)?;
                    self.put_marker(&path, kept_until).await?;
                    let removal = coven_database::ObjectRemoval {
                        path: path.as_str().to_owned(),
                        after: kept_until,
                    };
                    match work.state {
                        PairingState::Ending { how } => {
                            work.state = PairingState::Ended { how };
                            self.database
                                .advance_operation_with_removal(work.update(&record)?, removal)
                                .await?;
                        }
                        _ => {
                            self.database
                                .finish_operation_with_removal(id, Vec::new(), removal, None)
                                .await?;
                        }
                    }
                    return Ok(());
                }
                PairingState::Ended { .. } => return Ok(()),
            }
        }
    }

    /// The attempt found nothing to change: whatever it failed at before has
    /// succeeded, so the pairing's pending record at this step ends.
    async fn found_nothing(&self, id: OperationId, step: u32) -> Result<(), SyncError> {
        self.database.clear_operation_pending_at(id, step).await?;
        Ok(())
    }

    /// Put the approved `handover` in the place of the request: delete the
    /// request, then create the handover, which is created once; a request
    /// that arrives in between is deleted again. The handover found already,
    /// from an earlier attempt, is done.
    async fn place_handover(&self, path: &ObjectPath, handover: &[u8]) -> Result<(), SyncError> {
        loop {
            match self.storage.read(path).await {
                Ok(found) if found == handover => return Ok(()),
                Ok(_) => self.storage.delete_if_present(path).await?,
                Err(error) if error.failure() == StorageFailure::NotFound => {
                    match self.storage.create(path, handover).await {
                        Ok(_) => return Ok(()),
                        Err(error) if error.failure() == StorageFailure::AlreadyExists => (),
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
#[path = "sequence_pairings_tests.rs"]
mod tests;
