//! A store's slot 1, decided when the store is created (§10.3).

use crate::{
    history::History,
    sequence_sync::sequence_copies::{admission_prefix, required_copies},
    sequence_sync::sequence_operations::{
        OperationProgress, OperationStep, Request, SequenceOperation,
    },
    slot_object, PendingRecord, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{ClaimAttempt, ClaimWork, Database};
use coven_format::{
    admin::StoreChange,
    path::ObjectPath,
    sequence::{SlotFrame, SlotHeader},
};
use coven_foundation::id_source::{DeviceId, StoreId};
use coven_merge::Slot;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

/// What slot 1 sets up: the store, its first admin and device, and the
/// first store key `ring` holds.
pub struct Creation<'a> {
    /// The new store's id.
    pub store: StoreId,
    /// The store's name.
    pub name: String,
    /// This installation's device id.
    pub device: DeviceId,
    /// The name of this installation, the first admin's first device.
    pub device_name: String,
    /// The first admin, who signs slot 1.
    pub member: &'a MemberIdentity,
    /// The custody ring holding the first store key and no other.
    pub ring: &'a StoreKeyring,
}

/// Seal slot 1, journal its exact bytes as this device's attempt at it with
/// the operation that sends it, and apply it, all in `database`. The store is
/// not published yet, so no other device can claim slot 1 first; setup
/// later sends these bytes (§10.3). The creator's copy of the store key
/// rides in slot 1's clear prefix, as an admission's copies do (§12.1).
pub async fn decide_creation(database: &Database, creation: Creation<'_>) -> Result<(), SyncError> {
    let Creation { store, name, device, device_name, member, ring } = creation;
    let key =
        ring.store_key_ids().next().ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
    let change = StoreChange::CreateStore {
        store,
        name,
        admin: member.keys().public_keys(),
        key,
        key_hash: ring.store_key(key)?.hash(),
        device_name,
    };
    let history = History::default();
    let copies = required_copies(&history, ring, &change, member.member());
    let operation = SequenceOperation {
        request: Request::Change(change.clone()),
        progress: OperationProgress::Running(OperationStep::Claiming),
    };
    let id = database.start_operation(operation.journaled("creation")?).await?;
    let path = ObjectPath::slot(store, Slot::FIRST.into());
    let frame = SlotFrame::Admin(change);
    let sealed = Arc::new(Mutex::new(None));
    let kept = sealed.clone();
    let (held, author, claimed) = (ring.clone(), member.clone(), path.clone());
    database
        .begin_claim(ClaimWork::Operation(id), move |claim| {
            let header = SlotHeader { device, claim, author: author.member(), after: 0 };
            let plaintext = frame.encode(&header, &claimed)?.len();
            let prefix = admission_prefix(&held, &claimed, key, copies, plaintext)??;
            let bytes =
                slot_object::seal_bounded(&claimed, &header, &frame, prefix, &held, &author)?;
            *kept.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(bytes.clone());
            Ok::<_, SyncError>(ClaimAttempt { path: claimed.clone(), bytes: Cursor::new(bytes) })
        })
        .await?;
    let bytes = sealed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .ok_or(coven_database::DbError::Invariant { what: "slot 1 was sealed" })?;
    let mut history = History::default();
    let received = slot_object::open_bounded(bytes, &path, ring, &history)?;
    let incoming = history.consume(received, member.member());
    let applied = database
        .apply_slot::<PendingRecord, _, _>(incoming, history.writers(device), || Ok(()))
        .await
        .map_err(|error| crate::write_object::database_failure(&path, error))?;
    match applied {
        coven_database::ApplyOutcome::Applied => Ok(()),
        _ => Err(coven_database::DbError::Invariant { what: "slot 1 applies whole" }.into()),
    }
}

#[cfg(test)]
#[path = "creation_tests.rs"]
pub(crate) mod tests;
