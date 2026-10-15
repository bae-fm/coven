//! Evidence that this installation still owns its counters, gathered by the
//! first phase before any send, and the durable decision to reset (§10).

use super::*;
use crate::identity::{self, Counters, Ends, Evidence, SessionEvidence};
use coven_database::IdentityReservations;
use coven_foundation::files::DeviceReset;

impl StoreLogSync {
    /// Judge this completed first phase. Own entries and key copies come from
    /// its next-number reads; one own write read follows the reserved number.
    /// Snapshot coverage and the own post are read once per session.
    pub(super) async fn identity_gate(
        &self,
        local: &LocalStoreLog,
        reserved: &IdentityReservations,
        listed_entries: u64,
        listed_copies: u64,
    ) -> identity::Gate {
        let scan = async {
            let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
            let next = reserved
                .writes
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or(StorageFailure::Protocol.with_source("write sequence exhausted"))?;
            // One GET: a hit is the object, whose bytes this check never needs,
            // and a miss is a 404 (SP "Counting a pass").
            let writes = match storage
                .stream_object(
                    &ObjectPath::device_log(local.store, local.device, next),
                    None,
                )
                .await
            {
                Ok(_) => next.get(),
                Err(error) if error.failure() == StorageFailure::NotFound => 0,
                Err(error) => return Err(error.into()),
            };
            let session = match self.send.session_evidence() {
                Some(session) => session,
                None => {
                    let session = self.session_identity_evidence(local).await?;
                    self.send.keep_session_evidence(session);
                    session
                }
            };
            Ok(Evidence {
                listed: Counters {
                    logs: Ends {
                        writes,
                        entries: listed_entries,
                    },
                    key_copies: listed_copies,
                    snapshots: session.snapshots,
                },
                covered: session.covered,
                posted: session.posted,
                replaced: identity::replaced(&local.log, local.device),
                removed: false,
            })
        };
        // Custody was compared with this database's id when the store opened.
        identity::check(local.device, reserved, Some(local.device), scan.await)
    }

    async fn session_identity_evidence(
        &self,
        local: &LocalStoreLog,
    ) -> Result<SessionEvidence, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let (covered, snapshots) = self
            .snapshot_identity_evidence(&local.log, local.device)
            .await?;
        let path = ObjectPath::positions(local.store, local.device);
        let mut posted = Ends::default();
        // Retained under the object's revision, so the positions scan that
        // follows in the pass reuses these bytes instead of reading them again.
        if let Some(object) = storage.status(&path).await? {
            let (_, bytes) = crate::object_input::ObjectInput::whole_replaceable(
                &self.database,
                storage,
                &object,
            )
            .await?;
            {
                let ring = self.store_keys.read()?;
                // Damage or an unmet condition counts as no post (§19.1).
                if let Ok(positions) =
                    crate::posted_positions::open(&bytes, &path, ring.as_ref(), &local.log)?
                {
                    for write in positions
                        .writes
                        .0
                        .iter()
                        .filter(|w| w.device == local.device)
                    {
                        posted.writes = posted.writes.max(write.number);
                    }
                    for entry in positions
                        .store_log
                        .0
                        .iter()
                        .filter(|e| e.device == local.device)
                    {
                        posted.entries = posted.entries.max(entry.number);
                    }
                }
            }
        }
        Ok(SessionEvidence {
            covered,
            posted,
            snapshots,
        })
    }

    /// Durably decide to replace this stopped copy, keeping the first fresh id,
    /// then refuse every later database call (§10). The app reopens through
    /// `reset_device`, which moves this copy aside and loads a new device.
    pub(crate) async fn record_device_reset(&self) -> SyncError {
        let Some(reason) = self.send.stopped() else {
            return coven_database::DbError::Invariant {
                what: "a device reset is decided only after sending stopped",
            }
            .into();
        };
        let recorded = (|| {
            let old = self
                .directory
                .settings()
                .map_err(SyncError::disk)?
                .device_id;
            let reset = self.directory.record_device_reset(&DeviceReset {
                old_device: old,
                new_device: self.ids.new_device_id(),
                reason,
            })?;
            self.database.stop_for_device_reset(reset)?;
            Ok::<_, SyncError>(())
        })();
        match recorded {
            Ok(()) => SyncError::DeviceReset(reason),
            Err(error) => error,
        }
    }
}

#[cfg(test)]
#[path = "store_log_identity_tests.rs"]
mod tests;
