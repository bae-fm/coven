//! Provider clock observations and complete prefixes that certify finality.

use super::*;

impl StoreLogSync {
    pub(super) async fn scan_store_log(&self, require_complete: bool) -> Result<(), SyncError> {
        let mut complete = self.scan_store_log_once(require_complete).await?;
        if complete {
            let mut local = self.database.local_store_log().await?;
            let member = self.operation_member()?;
            let mut ring = self.store_keys.read()?;
            let queued = local.upload.is_some();
            self.publish(&mut local, &member, &mut ring).await?;
            if queued && local.upload.is_none() {
                complete = self.scan_store_log_once(require_complete).await?;
            }
        }
        if complete && self.send.can_send() {
            let local = self.database.local_store_log().await?;
            let ring = self.store_keys.read()?;
            self.share_keys(&local.log, &ring).await?;
            let due = self
                .finality
                .lock()
                .expect("finality lock poisoned")
                .probe_due(&local.log, self.clock.as_ref());
            if due && local.log.replay.state.devices.contains_key(&local.device) {
                self.observe_clock(&local).await?;
                self.scan_store_log_once(require_complete).await?;
            }
        }
        Ok(())
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
        // The clock object carries only this device's id, which no key
        // protects from members; any readable store key serves (§9).
        let key_id =
            crate::write_seal::readable_key(&local.log, &ring, &coven_merge::Audience::Store)?;
        let key = ring.store_key(key_id)?;
        let path = ObjectPath::clock(local.store, local.device);
        let prefix = coven_format::sealed_single::SingleChunkPrefix::ClockObservation(key_id);
        let plain =
            coven_format::Object::ClockObservation(coven_format::objects::ClockObservation {
                device: local.device,
            })
            .encode()?;
        let chunk =
            key.derive()
                .seal_object_chunk(path.as_str(), &prefix.encode()?, 0, 0, &plain)?;
        let mut bytes = prefix.encode_chunk(&chunk)?;
        let mut hash = coven_crypto::ObjectHasher::new();
        hash.update(&bytes);
        bytes.extend_from_slice(member.sign_object(path.as_str(), &hash.finish()).as_bytes());
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        // Elapsed time counts from before the replacement, so the request,
        // the status call and any sleep between them all count (§15).
        let started = self.clock.elapsed();
        storage.replace(&path, &bytes).await?;
        let stored = storage.status(&path).await?.ok_or_else(|| {
            StorageFailure::NotFound.with_source("published clock is absent from exact status")
        })?;
        let mut observation = self.finality.lock().expect("finality lock poisoned");
        observation.observe(&stored);
        observation.clock_observed(stored.stored_at, started);
        Ok(())
    }
}

#[cfg(test)]
#[path = "store_log_finality_tests.rs"]
mod tests;
