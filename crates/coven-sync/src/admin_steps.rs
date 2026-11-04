//! Administrative changes as steps of the app's write (§4, §11.1): the
//! write checks the change against the state this device has applied and
//! journals it, with or without storage. Making a circle is one too. A pass then catches up, validates
//! it again against everything it read, and claims its slot; one the
//! caught-up state refuses is undone and rejected (§11.2).

use crate::sequence_sync::sequence_operations::SequenceOperation;
use crate::{effects, OperationRejection, SequenceState, SyncError};
use coven_crypto::MemberIdentity;
use coven_database::{Journal, NewOperation, OperationId, SqlContext, StoreState};
use coven_format::{admin::StoreChange, sequence::SlotHeader};
use coven_foundation::id_source::{CircleId, DeviceId, IdSource, KeyId, MemberId};
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
    held: &MemberIdentity,
    state: Arc<SequenceState>,
    change: StoreChange,
) -> Result<Option<OperationId>, SyncError> {
    journal_change(sql, device, held, state, |_, _| Ok(change))
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
    held: &MemberIdentity,
    state: Arc<SequenceState>,
    member: MemberId,
    keys: coven_crypto::MemberPublicKeys,
) -> Result<Option<OperationId>, SyncError> {
    journal_change(sql, device, held, state, |applied, author| {
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

/// Journal making a circle, with this device's member as its first member,
/// as a step of a write, and return the new circle's id with the
/// operation's (§15.5). Any active member of the store may make one; the
/// pass keeps the circle's first key in custody, then claims its copy and
/// its introduction. The app can write rows into the circle once its slot
/// applies. A name that isn't a valid name fails with `Format`.
pub fn create_circle(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    held: &MemberIdentity,
    ids: &dyn IdSource,
    state: Arc<SequenceState>,
    name: String,
) -> Result<(CircleId, OperationId), SyncError> {
    coven_format::value::name(&name)?;
    let circle = CircleId(ids.new_id());
    let key = KeyId(ids.new_id());
    let operation = sql.journal_step(|journal| {
        registered(&journal.store_state()?, device, held)?;
        start(journal, SequenceOperation::circle(circle, name, key), "key")
    })?;
    sql.after_commit(move || state.request_pass());
    Ok((circle, operation))
}

/// This device's member, while the applied state has both active and binds
/// the member to the pair this installation holds, or its kept candidate:
/// only then does it author admin entries (§10, §12.3). A replaced binding
/// fails with `MemberKeysReplaced`, as the pass that applied it reported.
pub(crate) fn registered(
    applied: &StoreState,
    device: DeviceId,
    held: &MemberIdentity,
) -> Result<MemberId, SyncError> {
    let member = applied.devices.get(&device).filter(|registered| registered.active()).and_then(
        |registered| effects::member(applied, &registered.member).map(|m| (registered.member, m)),
    );
    let Some((id, member)) = member else {
        return Err(SyncError::Rejected(OperationRejection::NotAllowed));
    };
    if member.keys != held.keys().public_keys()
        && held.candidate().map(|keys| keys.public_keys()) != Some(member.keys)
    {
        return Err(SyncError::MemberKeysReplaced { member: id });
    }
    Ok(id)
}

/// Journal the operation as one the app's write asked for.
pub(crate) fn start(
    journal: &Journal<'_>,
    work: SequenceOperation,
    started_by: &str,
) -> Result<OperationId, SyncError> {
    let kind = work.kind().listed();
    Ok(journal.start_app_operation(&journaled(&work, started_by)?, &kind)?)
}

/// The journal row that starts `work`, before its first step.
pub(crate) fn journaled(
    work: &SequenceOperation,
    started_by: &str,
) -> Result<NewOperation, SyncError> {
    Ok(NewOperation {
        kind: "sequence".into(),
        data: serde_json::to_vec(work)?,
        started_by: started_by.into(),
        pauses_writes: false,
    })
}

/// Build the change from the applied state and this device's member, check
/// it there, and journal it in the write.
fn journal_change(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    held: &MemberIdentity,
    state: Arc<SequenceState>,
    change: impl FnOnce(&StoreState, MemberId) -> Result<StoreChange, SyncError>,
) -> Result<Option<OperationId>, SyncError> {
    let journaled = sql.journal_step(|journal| {
        let applied = journal.store_state()?;
        let author = registered(&applied, device, held)?;
        let change = change(&applied, author)?;
        if !changes(journal, &applied, device, author, &change)? {
            return Ok(None);
        }
        start(journal, SequenceOperation::change(change), "operation").map(Some)
    })?;
    if journaled.is_some() {
        sql.after_commit(move || state.request_pass());
    }
    Ok(journaled)
}

/// Whether the change would still change the applied state, as its next
/// slot's entry by this device's member: what that state refuses fails
/// with `Rejected`. Only the slots this device has applied are read; the
/// pass validates again after catching up (§11.1). A target those slots
/// don't name yet, such as a device registered since, isn't gone: the
/// change is journaled, and the pass decides once it has read on. One they
/// name as removed or deleted is refused at once.
pub(crate) fn changes(
    journal: &Journal<'_>,
    applied: &StoreState,
    device: DeviceId,
    author: MemberId,
    change: &StoreChange,
) -> Result<bool, SyncError> {
    let through = journal.applied_slot()?;
    let header = SlotHeader { device, claim: 1, author, after: through };
    let next = Slot(through.checked_add(1).ok_or(SyncError::SequenceExhausted)?);
    match effects::checked_change(applied, &header, next, change, &Default::default(), &[]) {
        Err(OperationRejection::TargetGone(target)) if unseen(applied, &target) => Ok(true),
        checked => Ok(checked?.is_some()),
    }
}

/// Whether the applied state has never named `target`: not even as removed
/// or deleted.
fn unseen(applied: &StoreState, target: &crate::EntryTarget) -> bool {
    match target {
        crate::EntryTarget::Member(member) => !applied.members.contains_key(member),
        crate::EntryTarget::Device(device) => !applied.devices.contains_key(device),
        crate::EntryTarget::Circle(circle) => {
            !applied.circles.contains_key(circle) && !applied.circle_deletions.contains_key(circle)
        }
    }
}
