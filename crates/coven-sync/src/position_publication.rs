//! Publish only changed canonical positions, preserving confirmation over failure.

use super::DeviceLogSync;
use crate::posted_positions::decode;
use crate::{PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_database::DbError;
use coven_format::{
    objects::{Fingerprint, PostedPositions},
    sealed_single::SingleChunkPrefix,
    Object,
};
use coven_merge::Audience;
use coven_storage::ObjectPath;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
struct Post {
    confirmed: Option<Vec<u8>>,
    attempt: Option<Attempt>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Attempt {
    plaintext: Vec<u8>,
    sealed: Vec<u8>,
}

impl DeviceLogSync {
    /// Observe peers once in the pass; later consumers reuse this exact state.
    pub(crate) async fn observe_positions(
        &self,
    ) -> Result<crate::posted_positions::Observation, SyncError> {
        let local = self.database.local_store_log().await?;
        let ring = self.store_keys.read()?;
        crate::posted_positions::observe(
            &self.database,
            &self.reads,
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            &local,
            ring.as_ref(),
        )
        .await
    }

    /// Confirm the current fully realized past, replacing storage only when the
    /// complete canonical plaintext changed or its post disappeared. A false
    /// result means the current past still waits; pending reports can still post.
    pub async fn post_positions(&mut self) -> Result<bool, SyncError> {
        let _reads = self.reads.enter();
        let observed = self.observe_positions().await?;
        let result = self.publish_positions(observed).await;
        if let Err(error) = &result {
            let own = self.database.sync_state(Vec::new()).await?.device;
            let reason = match &self.storage {
                Some(storage) => crate::posted_positions::failure(error, storage.as_ref()),
                None => PendingReason::NoStorage,
            };
            self.database
                .record_pending(PendingRecord {
                    subject: PendingSubject::Positions(own),
                    reason,
                    reported_by: own,
                })
                .await?;
        }
        result
    }

    async fn publish_positions(
        &self,
        mut observed: crate::posted_positions::Observation,
    ) -> Result<bool, SyncError> {
        self.compare_fingerprints().await?;
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let local = self.database.local_store_log().await?;
        crate::write_seal::check_member(&local.log, &member, local.device)?;
        let current = self.positions_for(&member, &local).await?;
        let ready = current.is_some();
        let mut post = self
            .database
            .positions_post::<Post>()
            .await?
            .unwrap_or_default();
        let mut positions = match current {
            Some(current) => current,
            None => {
                // Retained fingerprints describe the retained past exactly. With
                // no confirmation yet, the empty past asserts no realized work.
                let mut held = match &post.confirmed {
                    Some(bytes) => decode(bytes)?,
                    None => PostedPositions {
                        device: local.device,
                        writes: coven_format::value::WritePositions(Vec::new()),
                        store_log: coven_format::value::EntryPositions(Vec::new()),
                        schema_version: self.database.sync_state(Vec::new()).await?.schema_version,
                        fingerprints: Vec::new(),
                        pending: Vec::new(),
                    },
                };
                held.pending = crate::pending_logs::reports(&self.database).await?;
                held
            }
        };
        // A successful replacement resolves its own positions condition. The
        // published value and the confirmation transaction must agree about it.
        positions.pending.retain(|report| {
            report.subject != coven_format::pending::PendingSubject::Positions(local.device)
        });
        let plaintext = Object::PostedPositions(positions).encode()?;
        let remote = observed
            .positions
            .get(&local.device)
            .map(|value| Object::PostedPositions(value.clone()).encode())
            .transpose()?;
        if post.attempt.is_none() && remote.as_ref() == Some(&plaintext) {
            post.confirmed = Some(plaintext);
            self.database
                .save_positions_post::<_, PendingRecord>(
                    post,
                    Some(PendingSubject::Positions(local.device)),
                )
                .await?;
            return Ok(ready);
        }
        if post.attempt.is_none() {
            let newest = crate::key_introductions::newest(&local.log, &Audience::Store)
                .ok_or(SyncError::Removed)?;
            let ring = self
                .store_keys
                .read()?
                .ok_or(SyncError::KeyUnavailable(newest))?;
            let key = crate::write_seal::readable_key(&local.log, &ring, &Audience::Store)?;
            let path = ObjectPath::positions(local.store, local.device);
            let prefix = SingleChunkPrefix::PostedPositions(key);
            let sealed = ring.store_key(key)?.derive().seal_object_chunk(
                path.as_str(),
                &prefix.encode()?,
                0,
                0,
                &plaintext,
            )?;
            let mut bytes = prefix.encode_chunk(&sealed)?;
            let mut hash = coven_crypto::ObjectHasher::new();
            hash.update(&bytes);
            bytes.extend_from_slice(member.sign_object(path.as_str(), &hash.finish()).as_bytes());
            post.attempt = Some(Attempt {
                plaintext: plaintext.clone(),
                sealed: bytes,
            });
            self.database
                .save_positions_post::<_, PendingRecord>(post.clone(), None)
                .await?;
        }
        let attempt = post.attempt.as_ref().expect("retained publication attempt");
        if remote.as_ref() != Some(&attempt.plaintext) {
            self.storage
                .as_deref()
                .ok_or(SyncError::NoStorage)?
                .replace(
                    &ObjectPath::positions(local.store, local.device),
                    &attempt.sealed,
                )
                .await?;
        }
        let confirmed_current = ready && attempt.plaintext == plaintext;
        post.confirmed = Some(post.attempt.take().expect("confirmed attempt").plaintext);
        observed.positions.insert(
            local.device,
            decode(post.confirmed.as_ref().expect("confirmed plaintext"))?,
        );
        self.database
            .save_positions_post::<_, PendingRecord>(
                post,
                Some(PendingSubject::Positions(local.device)),
            )
            .await?;
        self.reads.keep_positions(observed);
        Ok(confirmed_current)
    }

    pub(super) async fn compare_fingerprints(&self) -> Result<(), SyncError> {
        let observed = self.observe_positions().await?;
        let Some(own) = self.current_positions().await? else {
            return Ok(());
        };
        let mut subjects = Vec::new();
        let mut records = Vec::new();
        for peer in observed
            .positions
            .values()
            .filter(|peer| peer.device != own.device)
        {
            if own.writes != peer.writes
                || own.store_log != peer.store_log
                || own.schema_version != peer.schema_version
            {
                continue;
            }
            for fingerprint in &own.fingerprints {
                if let Some(other) = peer.fingerprints.iter().find(|other| {
                    other.audience == fingerprint.audience && other.key == fingerprint.key
                }) {
                    let subject = PendingSubject::Agreement {
                        audience: fingerprint.audience.clone(),
                        peer: peer.device,
                    };
                    subjects.push(subject.clone());
                    if other.bytes != fingerprint.bytes {
                        records.push(PendingRecord {
                            subject,
                            reason: PendingReason::Disagrees,
                            reported_by: own.device,
                        });
                    }
                }
            }
        }
        self.database
            .replace_pending_subjects(subjects, records)
            .await?;
        Ok(())
    }
}

impl DeviceLogSync {
    pub(crate) async fn current_positions(&self) -> Result<Option<PostedPositions>, SyncError> {
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let local = self.database.local_store_log().await?;
        self.positions_for(&member, &local).await
    }

    async fn positions_for(
        &self,
        member: &coven_crypto::MemberKeys,
        local: &coven_database::LocalStoreLog,
    ) -> Result<Option<PostedPositions>, SyncError> {
        if let Some(operation) = self
            .database
            .first_operation(crate::operation_data::Data::RELOAD_KIND)
            .await?
        {
            crate::operation_data::Data::read(&operation)?;
            return Ok(None);
        }
        let store_key = crate::key_introductions::newest(&local.log, &Audience::Store)
            .ok_or(SyncError::Removed)?;
        let ring = self
            .store_keys
            .read()?
            .ok_or(SyncError::KeyUnavailable(store_key))?;
        // Fingerprints digest app data, so each uses its audience's usable key
        // from this pass's copy observation (§11, §19.1), never merely the
        // newest held one: a key an excluded member holds would let them test
        // guesses of data sealed with keys they lack (Appendix C12,
        // `PositionFingerprints`). An audience without a usable key omits its
        // fingerprint. The observation counts at any age: only a send needs it
        // fresh, so a pass reaching this step after five minutes still confirms
        // an unchanged post (`PositionPublication.pass_confirms_unchanged`).
        // With no completed observation no key is known usable, so the current
        // past waits and the confirmed post stands.
        let copies = match self.send.completed_observation() {
            Ok(copies) => copies
                .into_iter()
                .map(|copy| copy.prefix)
                .collect::<Vec<_>>(),
            Err(SyncError::Storage(error))
                if error.failure() == coven_storage::StorageFailure::CatchUpRequired =>
            {
                return Ok(None)
            }
            Err(error) => return Err(error),
        };
        let replay = &local.log.replay.state;
        let introductions = crate::key_introductions::introductions(&local.log);
        let me = member.member_id();
        let audiences = std::iter::once(Audience::Store).chain(
            replay
                .circles
                .keys()
                .filter(|circle| !replay.circle_deletions.contains_key(circle))
                .map(|circle| Audience::Circle(*circle)),
        );
        let selected: Vec<_> = audiences
            .filter_map(|audience| {
                let key = crate::key_introductions::select(
                    &introductions,
                    replay,
                    &me,
                    Some(copies.as_slice()),
                    crate::key_introductions::held_hash(Some(&ring)),
                    &audience,
                )
                .ok()?;
                Some((audience, key))
            })
            .collect();
        let hashers = selected
            .iter()
            .map(|(audience, key)| {
                Ok((
                    audience.clone(),
                    crate::write_seal::derive(&ring, audience, *key)?.fingerprint_hasher(),
                ))
            })
            .collect::<Result<_, SyncError>>()?;
        let state = self.database.sync_state(hashers).await?;
        crate::write_seal::check_member(&local.log, member, local.device)?;
        if state.uploads_pending || state.reloading {
            return Ok(None);
        }
        if local.log.positions() != state.store_log {
            return Err(DbError::StoreLogEntriesChanged.into());
        }
        let fingerprints = selected
            .into_iter()
            .zip(state.fingerprints)
            .map(|((audience, key), (actual, bytes))| {
                assert_eq!(audience, actual);
                Fingerprint {
                    audience,
                    key,
                    bytes,
                }
            })
            .collect();
        Ok(Some(PostedPositions {
            pending: crate::pending_logs::reports(&self.database).await?,
            device: state.device,
            writes: state.positions,
            store_log: state.store_log,
            schema_version: state.schema_version,
            fingerprints,
        }))
    }
}

#[cfg(test)]
#[path = "position_publication_tests.rs"]
mod tests;
