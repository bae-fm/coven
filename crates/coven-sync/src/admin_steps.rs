//! Administrative changes as steps of the app's write (§4, §11.1): the
//! write checks the change against the state this device has applied and
//! journals it, with or without storage. A pass then catches up, validates
//! it again against everything it read, and claims its slot; one the
//! caught-up state refuses is undone and rejected (§11.2).

use crate::sequence_sync::sequence_operations::SequenceOperation;
use crate::{effects, OperationRejection, SyncError, SyncState};
use coven_database::{Journal, NewOperation, OperationId, SqlContext, StoreState};
use coven_format::{admin::StoreChange, sequence::SlotHeader};
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::Slot;
use std::sync::Arc;

/// Journal an administrative change as a step of a write: renaming the
/// store, changing a role, removing a member or a device, and renaming,
/// deleting or changing the members of a circle. `None` when the applied
/// state already has what it asks for: nothing is journaled. A change the
/// applied state refuses fails the step with `Rejected`. Once the write
/// commits, a pass is asked for, which runs the operation as soon as
/// storage is connected.
pub fn administer(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    state: Arc<SyncState>,
    change: StoreChange,
) -> Result<Option<OperationId>, SyncError> {
    journal_change(sql, device, state, |_, _| Ok(change))
}

/// Journal the replacement of another member's public pair with `keys`, as
/// a step of a write, expecting the pair the applied state binds to them now
/// (§10.1, §12.3). A target that is this device's own member fails with
/// `Rejected(NotAllowed)`: replacing one's own pair is
/// `replace_member_keys`. A removed or unknown member fails with
/// `Rejected(TargetGone)`.
pub fn replace_member_keys_for(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    state: Arc<SyncState>,
    member: MemberId,
    keys: coven_crypto::MemberPublicKeys,
) -> Result<Option<OperationId>, SyncError> {
    journal_change(sql, device, state, |applied, author| {
        if member == author {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        let expected = effects::member(applied, &member)
            .ok_or(SyncError::Rejected(OperationRejection::TargetGone(
                crate::EntryTarget::Member(member),
            )))?
            .keys;
        Ok(StoreChange::ReplaceMemberKeys { member, expected, keys })
    })
}

/// Build the change from the applied state and this device's member, check
/// it there, and journal it in the write.
fn journal_change(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    state: Arc<SyncState>,
    change: impl FnOnce(&StoreState, MemberId) -> Result<StoreChange, SyncError>,
) -> Result<Option<OperationId>, SyncError> {
    let journaled = sql.journal_step(|journal| {
        let applied = journal.store_state()?;
        let author = applied
            .devices
            .get(&device)
            .filter(|registered| registered.active())
            .map(|registered| registered.member)
            .ok_or(SyncError::Rejected(OperationRejection::NotAllowed))?;
        let change = change(&applied, author)?;
        if !changes(journal, &applied, device, author, &change)? {
            return Ok(None);
        }
        let work = SequenceOperation::change(change);
        let kind = work.kind().listed();
        let operation = NewOperation {
            kind: "sequence".into(),
            data: serde_json::to_vec(&work)?,
            started_by: "operation".into(),
            pauses_writes: false,
        };
        Ok::<_, SyncError>(Some(journal.start_app_operation(&operation, &kind)?))
    })?;
    if journaled.is_some() {
        sql.after_commit(move || state.request_pass());
    }
    Ok(journaled)
}

/// Whether the change would still change the applied state, as its next
/// slot's entry by this device's member: what that state refuses fails
/// with `Rejected`. Only the slots this device has applied are read; the
/// pass validates again after catching up (§11.1).
fn changes(
    journal: &Journal<'_>,
    applied: &StoreState,
    device: DeviceId,
    author: MemberId,
    change: &StoreChange,
) -> Result<bool, SyncError> {
    let through = journal.applied_slot()?;
    let header = SlotHeader { device, claim: 1, author, after: through };
    let next = Slot(through.checked_add(1).ok_or(SyncError::SequenceExhausted)?);
    Ok(effects::checked_change(applied, &header, next, change, &Default::default(), &[])?.is_some())
}
