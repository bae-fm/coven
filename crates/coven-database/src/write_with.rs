//! A write in two phases on one transaction: the app's SQL, whose row
//! operations on the recorded tables are kept one at a time (§9.6), then the
//! host's work under a session.

use super::event_capture::{EventCapture, Operation};
use super::stream_capture::{StreamCapture, StreamChanges};
use super::transaction_capture::{ChangeCapture, SupplementalChanges};
use super::write_transaction::WriteTransaction;
use super::DatabaseConnection;
use crate::observation::RowChange;

impl DatabaseConnection {
    /// Run `app` while the pre-update hook keeps each operation on the
    /// recorded tables, then `host` with those operations under a session,
    /// judge the live queries, commit and tell the subscriptions. `app` gets
    /// a call that returns the operations so far. `host` gets what `app`
    /// returned.
    pub(crate) fn write_with<A, T, E: crate::WriteFailure>(
        &mut self,
        app: impl FnOnce(&DatabaseConnection, &dyn Fn() -> Vec<Operation>) -> Result<A, E>,
        host: impl FnOnce(&DatabaseConnection, &[Operation], A) -> Result<T, E>,
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
    /// neither. The write's result is none when `stream` said no.
    pub(crate) fn stream_then_write<S, A, T, E: crate::WriteFailure>(
        &mut self,
        stream: impl FnOnce(&DatabaseConnection) -> Result<(S, bool), E>,
        app: impl FnOnce(&DatabaseConnection, &dyn Fn() -> Vec<Operation>) -> Result<A, E>,
        host: impl FnOnce(&DatabaseConnection, &[Operation], A) -> Result<T, E>,
    ) -> Result<(S, Option<T>), E> {
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
            let written = if writes {
                transaction.stop_observing_operations()?;
                transaction.keep_schema_cookie();
                let (written, rows) = phases(&mut transaction, app, host)?;
                changes.extend(rows);
                Some(written)
            } else {
                None
            };
            Ok(((result, written), changes))
        });
        transaction.commit(outcome)
    }
}

/// The phases of a write on `transaction`, whose connection holds the
/// transaction and whose hooks this sets. Returns the result and the row
/// changes live queries are told of; `transaction` commits or rolls back with
/// them.
fn phases<A, T, E: crate::WriteFailure>(
    transaction: &mut WriteTransaction<'_>,
    app: impl FnOnce(&DatabaseConnection, &dyn Fn() -> Vec<Operation>) -> Result<A, E>,
    host: impl FnOnce(&DatabaseConnection, &[Operation], A) -> Result<T, E>,
) -> Result<(T, Vec<RowChange>), E> {
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
                let scoped = database
                    .handle
                    .as_ref()
                    .map(crate::observation::CommitObserver::scoped_columns)
                    .unwrap_or_default();
                let mut app = crate::change_capture::event_changes(database, &operations, &scoped)?;
                app.append(&mut changes);
                changes = app;
            } else {
                changes.clear();
            }
            if let Some(capture) = &mut capture {
                changes.extend(capture.take_changes()?);
            }
            Ok((result, changes))
        })
    })
}
