//! Test transport support using the actual local-write queue.

use super::Database;
use crate::DbError;

impl Database {
    /// Count executed work on every connection without including counter reads.
    /// Install while the database is quiescent, before starting the measured pass.
    pub async fn test_track_work(
        &self,
        counter: crate::test_utils::WorkCounter,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.test_track_work(counter.clone());
                Ok::<_, DbError>(())
            })?;
            inner.access.readers.test_track_work(&counter);
            inner.inputs.test_track_work(&counter);
            Ok(())
        })
        .await
    }

    /// Observe transaction completion and file cleanup on the writer thread.
    /// Install after setup; the callback may terminate a subprocess to simulate
    /// a crash without Rust destructors or SQLite connection shutdown.
    pub async fn test_on_write_checkpoint(
        &self,
        callback: impl Fn(crate::test_utils::WriteCheckpoint) + Send + 'static,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.on_write_checkpoint(callback);
                Ok(())
            })
        })
        .await
    }

    /// Model a database last judged by another library release before reopening it.
    pub async fn test_pending_version(&self, version: &'static str) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute("UPDATE _coven_pending SET coven_version=?1", [version])?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Record a device's state as a counted administrative slot would, under
    /// a placeholder member when the device is not yet registered.
    pub async fn test_device_state(
        &self,
        device: coven_foundation::id_source::DeviceId,
        state: crate::DeviceState,
    ) -> Result<(), DbError> {
        let state = match state {
            crate::DeviceState::Active => "active",
            crate::DeviceState::Removed => "removed",
            crate::DeviceState::Replaced => "replaced",
        };
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    let member = [7u8; 16];
                    let keys = coven_crypto::MemberKeys::from_secret_bytes(
                        &[b"CVMK\x01".as_slice(), &[7; 64]].concat(),
                    )
                    .expect("fixed placeholder pair")
                    .public_keys();
                    db.internal_execute(
                        "INSERT INTO _coven_members(member,signing,sealing,access,role,removed)
                         VALUES(?1,?2,?3,x'00','member',0) ON CONFLICT DO NOTHING",
                        (
                            member.as_slice(),
                            keys.signing.as_bytes().as_slice(),
                            keys.sealing.as_bytes().as_slice(),
                        ),
                    )?;
                    db.internal_execute(
                        "INSERT INTO _coven_devices(device,member,name,state) VALUES(?1,?2,'test',?3)
                         ON CONFLICT(device) DO UPDATE SET state=excluded.state",
                        (device.0.to_be_bytes().as_slice(), member.as_slice(), state),
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Each queued write in number order, with whether its first attempt fixed
    /// its keys. Queue differentials compare this with the model's tried flags.
    pub async fn test_queued_attempts(&self) -> Result<Vec<(coven_merge::WriteId, bool)>, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.query(
                    "SELECT device,number,sealing_keys IS NOT NULL FROM _coven_uploads ORDER BY device,number",
                    [],
                    |r| {
                        Ok((
                            coven_merge::WriteId {
                                device: coven_foundation::id_source::DeviceId(
                                    crate::write_encoding::counter(r.get(0)?),
                                ),
                                number: crate::write_encoding::counter(r.get(1)?),
                            },
                            r.get(2)?,
                        ))
                    },
                )
            })
        })
        .await
    }

    /// Test transport: read actual queued records without reauthoring their writes.
    pub async fn test_queued_writes(
        &self,
    ) -> Result<Vec<coven_format::write::WriteRecord>, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer
                    .query(
                        "SELECT record FROM _coven_uploads ORDER BY device,number",
                        [],
                        |r| r.get::<_, Vec<u8>>(0),
                    )?
                    .iter()
                    .map(|bytes| {
                        coven_format::write_stream::decode_plaintext(bytes).map_err(Into::into)
                    })
                    .collect()
            })
        })
        .await
    }
}
