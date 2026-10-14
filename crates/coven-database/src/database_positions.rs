//! Atomic positions observations and confirmation of the local publication.

use super::{Database, DbError};
use crate::{pending, sqlite::DatabaseConnection, PendingValue};
use coven_foundation::id_source::DeviceId;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;

fn observations(db: &DatabaseConnection) -> Result<BTreeMap<DeviceId, Vec<u8>>, DbError> {
    Ok(db
        .query(
            "SELECT device,observation FROM _coven_peer_positions",
            [],
            |row| Ok((DeviceId(u64::from_be_bytes(row.get(0)?)), row.get(1)?)),
        )?
        .into_iter()
        .collect())
}

impl Database {
    /// Checked peer identities and values from the last completed observation.
    /// Sync owns their encoding and authentication; failure commits none of a scan.
    pub async fn positions_observation<P: DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<BTreeMap<DeviceId, P>, DbError> {
        self.call(|inner| {
            inner.access.readers.acquire_reader().with_reader(|db| {
                observations(db)?
                    .into_iter()
                    .map(|(device, bytes)| {
                        Ok((
                            device,
                            serde_json::from_slice(&bytes).map_err(DbError::PendingEncoding)?,
                        ))
                    })
                    .collect()
            })
        })
        .await
    }

    /// Commit a completed scan and its relevant peer reports together. Local
    /// conditions are replaced only for the named subjects. Equal observations
    /// and reports open no durable transaction.
    pub async fn complete_positions_observation<O, P>(
        &self,
        observed: BTreeMap<DeviceId, O>,
        reports: Vec<P>,
        local_subjects: Vec<P::Subject>,
        local: Vec<P>,
    ) -> Result<(), DbError>
    where
        O: Serialize + Send + 'static,
        P: PendingValue,
        P::Subject: Send + 'static,
    {
        let next = observed
            .into_iter()
            .map(|(device, value)| {
                Ok((
                    device,
                    serde_json::to_vec(&value).map_err(DbError::PendingEncoding)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, DbError>>()?;
        self.call(move |inner| inner.access.with_writer(|writer| {
            let previous = observations(writer)?;
            let reports = pending::Replacement::reports(writer, inner.access.device, reports)?;
            let local = local_changes(writer, inner.access.device, local_subjects, local)?;
            if previous == next && reports.is_empty() && local.is_empty() { return Ok(()); }
            writer.pending_transaction(|db| {
                for device in previous.keys().filter(|device| !next.contains_key(device)) {
                    db.internal_execute("DELETE FROM _coven_peer_positions WHERE device=?1", [device.0.to_be_bytes().as_slice()])?;
                }
                for (device, bytes) in next {
                    if previous.get(&device) != Some(&bytes) {
                        db.internal_execute("INSERT INTO _coven_peer_positions(device,observation) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET observation=excluded.observation", (device.0.to_be_bytes().as_slice(), bytes))?;
                    }
                }
                reports.apply(db)?;
                local.apply(db)
            })
        })).await
    }

    /// Replace local conditions for these exact subjects without rewriting an
    /// unchanged reason. Other observations, including received reports, remain.
    pub async fn replace_pending_subjects<P: PendingValue>(
        &self,
        subjects: Vec<P::Subject>,
        records: Vec<P>,
    ) -> Result<(), DbError>
    where
        P::Subject: Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                let changes = local_changes(writer, inner.access.device, subjects, records)?;
                if changes.is_empty() {
                    return Ok(());
                }
                writer.pending_transaction(|db| changes.apply(db))
            })
        })
        .await
    }

    /// Last confirmed canonical plaintext and any fixed unfinished replacement.
    /// The sync owner supplies the typed publication state, including its bytes.
    pub async fn positions_post<P: DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<Option<P>, DbError> {
        self.call(|inner| {
            inner.access.readers.acquire_reader().with_reader(|db| {
                let bytes: Option<Vec<u8>> = db.query_row(
                    "SELECT (SELECT state FROM _coven_positions_post WHERE singleton=1)",
                    [],
                    |r| r.get(0),
                )?;
                bytes
                    .map(|bytes| serde_json::from_slice(&bytes).map_err(DbError::PendingEncoding))
                    .transpose()
            })
        })
        .await
    }

    /// Retain an attempt before sending, or confirm it only after storage
    /// acknowledges those bytes, clearing its local failure in the same transaction.
    /// A repeated confirmation writes nothing.
    pub async fn save_positions_post<P, R>(
        &self,
        post: P,
        completed: Option<R::Subject>,
    ) -> Result<(), DbError>
    where
        P: Serialize + Send + 'static,
        R: PendingValue,
        R::Subject: Send + 'static,
    {
        let bytes = serde_json::to_vec(&post).map_err(DbError::PendingEncoding)?;
        self.call(move |inner| inner.access.with_writer(|writer| {
            let previous: Option<Vec<u8>> = writer.query_row("SELECT (SELECT state FROM _coven_positions_post WHERE singleton=1)", [], |r| r.get(0))?;
            let local = local_changes::<R>(writer, inner.access.device, completed.into_iter().collect(), Vec::new())?;
            if previous.as_ref() == Some(&bytes) && local.is_empty() { return Ok(()); }
            writer.pending_transaction(|db| {
                db.internal_execute("INSERT INTO _coven_positions_post(singleton,state) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET state=excluded.state", [bytes])?;
                local.apply(db)
            })
        })).await
    }
}

fn local_changes<P: PendingValue>(
    db: &DatabaseConnection,
    own: DeviceId,
    subjects: Vec<P::Subject>,
    records: Vec<P>,
) -> Result<pending::Replacement<P>, DbError> {
    if records.iter().any(|r| {
        r.reported_by() != own || r.operation().is_some() || !subjects.contains(r.subject())
    }) {
        return Err(DbError::DamagedDatabase);
    }
    let previous = pending::retained::<P>(db)?
        .into_iter()
        .filter(|r| r.reported_by() == own && subjects.contains(r.subject()))
        .collect();
    pending::Replacement::new(previous, records)
}
