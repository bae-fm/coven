//! Evidence that this installation still owns its counters, gathered by the
//! first phase before any send (§10).

use super::*;
use crate::identity::{self, Counters, Ends, Evidence, SessionEvidence};
use coven_database::IdentityReservations;

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
        identity::check(reserved, scan.await)
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
                coven_format::sealed_single::MAX_SINGLE_CHUNK_OBJECT as u64,
            )
            .await?;
            // Longer than any positions object: damage, so no post.
            if let Some(bytes) = bytes {
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
}

#[cfg(test)]
#[path = "store_log_identity_tests.rs"]
pub(crate) mod tests;
