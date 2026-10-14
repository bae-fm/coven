//! Whether write misses can be trusted, and the away-device path when not (§15).

use super::{point, StoreLogSync};
use crate::SyncError;

impl StoreLogSync {
    /// Before trusting write misses: observe storage time on return and a
    /// day before the recent-return window closes, then discover snapshots if
    /// misses still cannot be trusted (§15, SP steps 1 and 3). A failed
    /// observation leaves history pending; it never certifies a miss.
    pub(crate) async fn prepare_return(&mut self) -> Result<(), SyncError> {
        let saved = self.database.return_checkpoint().await?;
        let (sample, discovered) = {
            let observation = self.finality.lock().expect("finality lock poisoned");
            (observation.clock(), observation.discovered())
        };
        let refresh = match sample {
            None => true,
            Some((stored, started)) => {
                let checkpoint = saved.max(
                    discovered
                        .map(coven_format::value::StorageTime::from_system_time)
                        .transpose()?,
                );
                checkpoint.is_some()
                    && crate::recent_return::refresh_due(
                        checkpoint,
                        coven_format::value::StorageTime::from_system_time(stored)?,
                        self.clock.elapsed().saturating_sub(started),
                    )
            }
        };
        if refresh {
            self.observe_clock(&self.database.local_store_log().await?)
                .await?;
        }
        if !crate::recent_return::miss_is_recent(&self.finality, self.clock.as_ref(), saved) {
            self.discover_snapshots().await?;
        }
        Ok(())
    }

    /// The away-device path (§15): list each readable audience's snapshots
    /// after a fresh storage sample and reload only if a counting snapshot
    /// covers past this device's positions on some writer. Afterwards that
    /// sample certifies write misses for this session.
    pub(crate) async fn discover_snapshots(&mut self) -> Result<(), SyncError> {
        let _reads = self.reads.enter();
        let _shared = self.reads.share_snapshot_listings();
        let local = self.database.local_store_log().await?;
        let known = self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .clock();
        if known.is_none() {
            self.observe_clock(&local).await?;
        }
        let (sample, _) = self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .clock()
            .expect("observed storage time");
        let state = self.database.sync_state(Vec::new()).await?;
        let mut damages = Vec::new();
        let mut advances = false;
        for audience in self.snapshot_audiences(&local.log)? {
            let catalog = self
                .current_snapshot_candidates(&audience, &local.log, &mut damages)
                .await?;
            advances |= catalog
                .required_positions
                .0
                .iter()
                .any(|covered| covered.number > point(&state.positions, covered.device));
        }
        if advances {
            self.reload_from_snapshots().await?;
        }
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .snapshots_discovered(sample);
        Ok(())
    }

    /// Persist `S` for the next return; an unchanged time writes nothing.
    pub(crate) async fn save_return_checkpoint(
        &self,
        sample: std::time::SystemTime,
    ) -> Result<(), SyncError> {
        self.database
            .save_return_checkpoint(coven_format::value::StorageTime::from_system_time(sample)?)
            .await?;
        Ok(())
    }

    /// The storage time known before this pass's write discovery; it becomes
    /// the return checkpoint if the pass completes (SP step 9).
    pub(crate) fn return_sample(&self) -> Option<std::time::SystemTime> {
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .sample_time()
    }
}
