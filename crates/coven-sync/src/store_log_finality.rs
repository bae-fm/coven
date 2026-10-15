//! Provider clock observations and complete prefixes that certify finality.

use super::*;

impl StoreLogSync {
    /// Phase 1 (SP steps 1–4): the read-only membership and key-copy catch-up.
    /// A completed one observes storage time when a finality or drain
    /// threshold may have passed, then discovers again from that sample.
    /// Nothing here publishes an entry or shares a key. Returns whether the
    /// observation completed.
    pub(super) async fn scan_store_log(&self, require_complete: bool) -> Result<bool, SyncError> {
        let complete = self.scan_store_log_once(require_complete).await?;
        if !complete || !self.send.can_send() {
            return Ok(complete);
        }
        let local = self.database.local_store_log().await?;
        let drains = self.database.retirement_finalities().await?;
        let due = self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .probe_due(&local.log, &drains, self.clock.as_ref());
        if due && local.log.replay.state.devices.contains_key(&local.device) {
            self.observe_clock(&local).await?;
            return self.scan_store_log_once(require_complete).await;
        }
        Ok(complete)
    }

    /// A complete catch-up precedes this publication. The following
    /// scan captures its status time before making any discovery request.
    pub(crate) async fn observe_clock(&self, local: &LocalStoreLog) -> Result<(), SyncError> {
        let refreshed;
        let local = if self.send.can_send() {
            local
        } else {
            self.scan_store_log_once(true).await?;
            refreshed = self.database.local_store_log().await?;
            &refreshed
        };
        let member = self.operation_member()?;
        crate::write_seal::check_member(&local.log, &member, local.device)?;
        let newest = crate::key_introductions::newest(&local.log, &coven_merge::Audience::Store)
            .ok_or(SyncError::NoStorage)?;
        let ring = self
            .store_keys
            .read()?
            .ok_or(SyncError::KeyUnavailable(newest))?;
        // The clock object carries only this device's id and the store log it
        // read, which no key protects from members; any readable store key
        // serves (§9). Its recorded view locates the signer (D8, D9).
        let key_id =
            crate::write_seal::readable_key(&local.log, &ring, &coven_merge::Audience::Store)?;
        let key = ring.store_key(key_id)?;
        let path = ObjectPath::clock(local.store, local.device);
        let prefix = coven_format::sealed_single::SingleChunkPrefix::ClockObservation(key_id);
        let plain =
            coven_format::Object::ClockObservation(coven_format::objects::ClockObservation {
                device: local.device,
                store_log_read: local.log.positions(),
            })
            .encode()?;
        let chunk =
            key.derive()
                .seal_object_chunk(path.as_str(), &prefix.encode()?, 0, 0, &plain)?;
        let mut bytes = prefix.encode_chunk(&chunk)?;
        let mut hash = coven_crypto::ObjectHasher::new();
        hash.update(&bytes);
        bytes.extend_from_slice(
            member
                .keys()
                .sign_object(path.as_str(), &hash.finish())
                .as_bytes(),
        );
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        // Elapsed time counts from before the replacement, so the request,
        // the status call and any sleep between them all count (§15).
        let started = self.clock.elapsed();
        storage.replace(&path, &bytes).await?;
        let stored = storage.status(&path).await?.ok_or_else(|| {
            StorageFailure::NotFound.with_source("published clock is absent from exact status")
        })?;
        let mut observation = self.finality.lock().expect("finality lock poisoned");
        observation.observe(&stored, self.clock.elapsed());
        observation.clock_observed(stored.stored_at, started);
        Ok(())
    }
}

#[cfg(test)]
#[path = "store_log_finality_tests.rs"]
mod tests;
