//! Coven's objects that app SQL may read, made outside any store for an
//! app's test of its own schema (api.md E4), with sync's computation of the
//! sync state, which the database crate can't reach.

/// Make on `connection`, a connection an app's test opened itself outside
/// any store, every object of coven's that app SQL may read, and register
/// coven's SQL functions there, with `device` as this device's id, as
/// `coven_database::test_utils::make_readable_objects` says. The sync state
/// function is sync's own, as every store registers it.
pub fn make_readable_objects(
    connection: &coven_database::test_utils::Connection,
    device: coven_foundation::id_source::DeviceId,
) -> coven_database::SqliteResult<()> {
    coven_database::test_utils::make_readable_objects(
        connection,
        device,
        &coven_sync::sync_functions(),
    )
}

/// Journal `invite` on `connection`, where `make_readable_objects` made
/// coven's objects, as a store journals an invite at its stage, so that
/// `coven_invites` shows it. Returns the invite's operation.
pub fn record_invite(
    connection: &coven_database::test_utils::Connection,
    invite: coven_sync::RecordedInvite,
) -> Result<coven_database::OperationId, crate::CovenError> {
    let new = coven_sync::invite_operation(invite)?;
    Ok(coven_database::test_utils::journal_outside_store(connection, &new, None)?)
}

/// End the invite `invite` on `connection` as a store ends one that the
/// provider refused to share with `account` for `refusal`, a reason only a
/// person can fix: `coven_operations` lists it as refused, and it leaves
/// `coven_invites` (§13.2).
pub fn refuse_invite(
    connection: &coven_database::test_utils::Connection,
    invite: coven_database::OperationId,
    account: &str,
    refusal: coven_foundation::sharing::SharingRefusal,
) -> Result<(), crate::CovenError> {
    let outcome =
        coven_database::ListedOutcome::Invite(coven_database::InviteOutcome::SharingRefused {
            account: account.to_owned(),
            refusal,
        });
    let kind = coven_sync::OperationKind::Invite.listed();
    Ok(coven_database::test_utils::finish_outside_store(
        connection,
        invite,
        coven_database::test_utils::OutcomeListing::Unlisted { kind: &kind, outcome: &outcome },
    )?)
}

/// Journal `change` on `connection`, where `make_readable_objects` made
/// coven's objects, as the app's write journals it, so that
/// `coven_operations` lists it as running. Returns its operation.
pub fn record_change(
    connection: &coven_database::test_utils::Connection,
    change: coven_format::admin::StoreChange,
) -> Result<coven_database::OperationId, crate::CovenError> {
    let (new, kind) = coven_sync::change_operation(change)?;
    Ok(coven_database::test_utils::journal_outside_store(connection, &new, Some(&kind))?)
}

/// End the operation `id`, which the app's write journaled, on `connection`
/// as a store ends it with `outcome`: `coven_operations` lists the outcome
/// until the app acknowledges it (§11.4). An outcome not in the outcome type
/// of the operation's kind fails with `Invariant`, as coven never writes one.
pub fn finish_operation(
    connection: &coven_database::test_utils::Connection,
    id: coven_database::OperationId,
    outcome: impl Into<coven_database::ListedOutcome>,
) -> Result<(), crate::CovenError> {
    let outcome = outcome.into();
    let kind: Option<coven_sync::OperationKind> = connection
        .query_row("SELECT (SELECT kind FROM coven_operations WHERE operation=?1)", [id.0], |row| {
            row.get(0)
        })
        .map_err(coven_database::DbError::from)?;
    if kind.is_some_and(|kind| !kind.lists(&outcome)) {
        return Err(coven_database::DbError::Invariant {
            what: "an operation ends in the outcome type of its kind",
        }
        .into());
    }
    Ok(coven_database::test_utils::finish_outside_store(
        connection,
        id,
        coven_database::test_utils::OutcomeListing::Listed(&outcome),
    )?)
}

/// Save `record` on `connection`, where `make_readable_objects` made
/// coven's objects, as a store saves a pending record, so that
/// `coven_pending` shows it (§19.1).
pub fn record_pending(
    connection: &coven_database::test_utils::Connection,
    record: &coven_sync::PendingRecord,
) -> Result<(), crate::CovenError> {
    Ok(coven_database::test_utils::save_outside_store(connection, record)?)
}

/// Record on `connection`, where `make_readable_objects` made coven's
/// objects, that `location` is the store's storage and this device holds
/// its credentials, as opening a connected store records it. Until a test
/// does, `coven_sync_state` shows not synced, as for a store with no
/// storage (§19.1).
pub fn record_connected_storage(
    connection: &coven_database::test_utils::Connection,
    location: coven_storage::StorageConfig,
) -> Result<(), crate::CovenError> {
    Ok(coven_sync::record_connected_outside_store(connection, location)?)
}

/// Save `pass` on `connection`, where `make_readable_objects` made coven's
/// objects, as how the last pass ended, as a store's loop writes it when a
/// pass ends, so that `coven_sync_status.last_pass` holds it and
/// `coven_sync_state` computes its state from it beside the pending records
/// the test saves with `record_pending` (§19.1).
pub fn record_last_pass(
    connection: &coven_database::test_utils::Connection,
    pass: &coven_sync::LastPass,
) -> Result<(), crate::CovenError> {
    Ok(pass.record_outside_store(connection)?)
}

#[cfg(test)]
#[path = "readable_objects_tests.rs"]
mod tests;
