//! The app's write for a member's device as its registration applies (§13.2):
//! the person's profile, written in the transaction that first applies the
//! device's registration, from the facts of its member's latest admission.

use crate::{
    file_write::{FileWrite, WriteFiles},
    sqlite::DatabaseConnection,
    write_schema::WriteSchema,
    DbError, StoreState,
};
use coven_foundation::id_source::{CircleId, DeviceId, MemberId};
use rusqlite::{Params, Row};

/// The facts of a member's latest admission, which the joining write reads
/// (§13.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Joining {
    /// The member whose device registers.
    pub member: MemberId,
    /// The name the member's join request carried; none after a recovery.
    pub person: Option<String>,
    /// The circle their latest admission made.
    pub circle: CircleId,
}

/// App SQL inside a joining write: the statements of a local write and
/// nothing of coven's steps, so no operation, file or journal can be reached
/// from it. Returning an error rolls back the write and the registration it
/// runs with.
pub struct JoiningWrite<'write> {
    files: &'write FileWrite<'write>,
}

impl<'write> JoiningWrite<'write> {
    pub(crate) fn new(files: &'write FileWrite<'write>) -> Self {
        Self { files }
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.files.execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.files.execute_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.files.query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.files.query(sql, params, map)
    }
}

/// The app's joining write, as the database keeps it.
pub(crate) type JoiningHook =
    dyn Fn(&JoiningWrite<'_>, &Joining) -> Result<(), DbError> + Send + Sync;

/// A joining write owed to a transaction that registers this device: the
/// app's write and the facts it takes, with what a local write needs.
pub(crate) struct Registering<'a> {
    hook: &'a JoiningHook,
    joining: Joining,
    schema: &'a WriteSchema,
    device: DeviceId,
    now: std::time::SystemTime,
    files: &'a WriteFiles<'a>,
}

impl Registering<'_> {
    /// Make the owed write through `write`, which runs a local write's phases
    /// on its transaction with what that write needs and the app's SQL.
    pub(crate) fn make<R>(
        &self,
        write: impl FnOnce(
            &WriteSchema,
            DeviceId,
            std::time::SystemTime,
            &WriteFiles<'_>,
            &dyn Fn(&FileWrite<'_>) -> Result<(), DbError>,
        ) -> R,
    ) -> R {
        write(self.schema, self.device, self.now, self.files, &|files| {
            (self.hook)(&JoiningWrite::new(files), &self.joining)
        })
    }
}

/// The joining write the transaction that commits `state` owes, if any: the
/// app registered one, this device is a device of an active member that holds
/// the facts of an admission, and the state the database holds doesn't
/// register it yet. Read before the transaction begins, so the write runs in
/// the one transaction that first applies the registration, and in none after
/// it (§13.2).
pub(crate) fn owed<'a>(
    db: &DatabaseConnection,
    hook: Option<&'a JoiningHook>,
    state: &StoreState,
    schema: &'a WriteSchema,
    now: std::time::SystemTime,
    files: &'a WriteFiles<'a>,
) -> Result<Option<Registering<'a>>, DbError> {
    let (Some(hook), Some(device)) = (hook, db.local_device()) else {
        return Ok(None);
    };
    if crate::store_state_tables::device_member(db, device)?.is_some() {
        return Ok(None);
    }
    let Some(registered) = state.devices.get(&device) else {
        return Ok(None);
    };
    let Some(member) = state.active_member(&registered.member).filter(|_| registered.active())
    else {
        return Ok(None);
    };
    let Some(joined) = &member.joined else {
        return Ok(None);
    };
    Ok(Some(Registering {
        hook,
        joining: Joining {
            member: registered.member,
            person: joined.person.clone(),
            circle: joined.circle,
        },
        schema,
        device,
        now,
        files,
    }))
}

#[cfg(test)]
#[path = "joining_write_tests.rs"]
mod tests;
