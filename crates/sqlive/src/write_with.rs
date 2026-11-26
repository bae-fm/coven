//! A write in two phases on one transaction: the app's SQL, while the
//! pre-update hook keeps each row operation on the recorded tables one at a
//! time (coven.md §9.6), then the host's work under a session.

use super::event_capture::{EventCapture, Operation};
use super::stream_capture::{StreamCapture, StreamChanges};
use super::transaction_capture::{ChangeCapture, SupplementalChanges};
use super::write_transaction::WriteTransaction;
use super::HostSql;
use crate::authorization::Policy;
use crate::net_changes::Captured;

impl<P: Policy> HostSql<P> {
    /// Run `app` while the pre-update hook keeps each operation on the
    /// recorded tables, then `host` with those operations under a session,
    /// judge the live queries, commit and tell the subscriptions. `app` gets
    /// a call that returns the operations so far. `host` gets what `app`
    /// returned.
    pub fn write_with<A, T, E: crate::RollbackJoin<P::Failure, P::Refusal>>(
        &mut self,
        app: impl FnOnce(&HostSql<P>, &dyn Fn() -> Vec<Operation>) -> Result<A, E>,
        host: impl FnOnce(&HostSql<P>, &[Operation], A) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut transaction = WriteTransaction::begin(self, false)?;
        transaction.keep_schema_cookie();
        let outcome = phases(&mut transaction, app, host);
        transaction.commit(outcome)
    }

    /// A streamed transaction, as [`Self::stream_transaction`] runs one, in
    /// which `stream` says whether `write_with`'s phases follow in the same
    /// transaction. When they do, the app phase takes the hook the stream's
    /// capture held, which has seen every change of `stream`; both commit, or
    /// neither.
    pub fn stream_then_write<S, A, E: crate::RollbackJoin<P::Failure, P::Refusal>>(
        &mut self,
        stream: impl FnOnce(&HostSql<P>) -> Result<(S, bool), E>,
        app: impl FnOnce(&HostSql<P>, &dyn Fn() -> Vec<Operation>) -> Result<A, E>,
        host: impl FnOnce(&HostSql<P>, &[Operation], A) -> Result<(), E>,
    ) -> Result<S, E> {
        let mut transaction = WriteTransaction::begin(self, true)?;
        let outcome = StreamCapture::begin(&mut transaction).map_err(E::from).and_then(|capture| {
            let (result, writes) = transaction.within(|database| {
                let ran = stream(database)?;
                database.require_transaction()?;
                Ok::<_, E>(ran)
            })?;
            let mut changes = match capture.changes() {
                StreamChanges::Rows(rows) => rows,
                StreamChanges::Tables(tables) => transaction
                    .within(|database| crate::change_capture::table_changes(database, tables))?,
            };
            if writes {
                transaction.stop_observing_operations()?;
                transaction.keep_schema_cookie();
                let ((), rows) = phases(&mut transaction, app, host)?;
                changes.append(rows);
            }
            Ok((result, changes))
        });
        transaction.commit(outcome)
    }
}

/// Runs the phases of a write on `transaction`, whose connection holds the
/// transaction and whose hooks this sets. Returns the host's result and
/// what the phases changed, which the commit sends to the live queries as
/// each row's net change; `transaction` commits or rolls back with them.
fn phases<P: Policy, A, T, E: crate::RollbackJoin<P::Failure, P::Refusal>>(
    transaction: &mut WriteTransaction<'_, P>,
    app: impl FnOnce(&HostSql<P>, &dyn Fn() -> Vec<Operation>) -> Result<A, E>,
    host: impl FnOnce(&HostSql<P>, &[Operation], A) -> Result<T, E>,
) -> Result<(T, Captured), E> {
    let recording = EventCapture::begin(transaction);
    let begun = recording.map_err(E::from).and_then(|recording| {
        transaction.within(|database| {
            let begun = app(database, &|| recording.events())?;
            database.require_transaction()?;
            Ok((begun, recording))
        })
    });
    // The app phase ends: a session observes the rest of the transaction.
    let switched = match begun {
        Ok(begun) => transaction
            .stop_observing_operations()
            .and_then(|()| SupplementalChanges::observe(transaction))
            .map(|supplemental| (begun, supplemental))
            .map_err(E::from),
        Err(error) => Err(error),
    };
    switched.and_then(|((begun, recording), supplemental)| {
        transaction.within(|database| {
            let operations = recording.events();
            let mut changes = recording.local_changes();
            let mut capture = match &database.observation {
                Some(_) => Some(ChangeCapture::begin(database, supplemental)?),
                None => None,
            };
            let result = host(database, &operations, begun)?;
            database.require_transaction()?;
            if database.observation.is_some() {
                let kept =
                    database.handle.as_ref().map(crate::Handle::kept_columns).unwrap_or_default();
                let mut app = crate::change_capture::event_changes(database, &operations, &kept)?;
                app.append(changes);
                changes = app;
            } else {
                changes = Captured::default();
            }
            if let Some(capture) = &mut capture {
                changes.append(capture.take_changes()?);
            }
            Ok((result, changes))
        })
    })
}
