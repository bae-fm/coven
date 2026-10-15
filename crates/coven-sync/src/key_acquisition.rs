//! Opening this member's sealed key copies, with one verdict per copy (§11, §19.1).

use super::*;
use crate::key_introductions::{
    accepts_copy, introduction, introductions, needed, Introduction, Opened,
};
use crate::{PendingReason, PendingRecord, PendingSubject};
use coven_crypto::CryptoError;
use coven_format::sealed_single::SingleChunkObject;

enum Tentative {
    Confirmed,
    Refused(Refusal),
    Uncommitted,
    Waits(PendingReason),
}

fn refused(copy: &keys::ObservedCopy, failure: &Refusal) {
    tracing::warn!(
        path = copy.path.as_str(),
        error = %failure,
        "refused one key copy; a later copy can supply the key"
    );
}

/// What reading one received entry produced.
pub(super) enum EntryOpening {
    /// The entry, opened with an accepted key; its signature waits for its view.
    Opened(crate::store_log_object::OpenedEntry),
    /// No accepted key opens it yet; its key copy subject says why.
    WaitsForKey,
    /// The entry's own bytes failed a check.
    Refused(Refusal),
}

impl StoreLogSync {
    /// Acquire every acceptable key not yet held. Each key copy subject keeps
    /// its first unmet condition; holding the key or losing the need clears it.
    pub(super) async fn acquire_keys(
        &self,
        log: &StoreLog,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<(), SyncError> {
        let copies = keys::read_copies(
            &self.database,
            &self.reads,
            self.download_limit,
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            self.directory.id(),
        )
        .await?
        .copies;
        self.acquire_observed_keys(log, member, ring, &copies).await
    }

    /// Open copies already checked by the completed phase or confirmed locally.
    pub(super) async fn acquire_observed_keys(
        &self,
        log: &StoreLog,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
        copies: &[keys::ObservedCopy],
    ) -> Result<(), SyncError> {
        let introductions = introductions(log);
        let me = member.member();
        let mut conditions = BTreeMap::new();
        for (audience, key) in needed(log).into_keys() {
            let Some(reason) =
                self.acquire_copies(&audience, key, &introductions, member, ring, copies)?
            else {
                continue;
            };
            // Only a current audience member waits for a copy; a copy addressed
            // to this member that fails its checks is reported either way.
            if matches!(reason, PendingReason::Refused(_))
                || crate::key_introductions::belongs(&log.replay.state, &audience, &me)
            {
                let subject = PendingSubject::KeyCopy {
                    audience,
                    key,
                    member: me,
                    sealing: member.keys().public_keys().sealing,
                };
                conditions.insert(subject, reason);
            }
        }
        self.settle_key_conditions(&me, &introductions, conditions)
            .await
    }

    async fn settle_key_conditions(
        &self,
        me: &coven_foundation::id_source::MemberId,
        introductions: &[Introduction],
        conditions: BTreeMap<PendingSubject, PendingReason>,
    ) -> Result<(), SyncError> {
        let own = self.database.local_store_log().await?.device;
        let recorded: Vec<PendingRecord> = self
            .database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .filter(|record| {
                record.reported_by == own
                    && matches!(&record.subject, PendingSubject::KeyCopy { member, .. } if member == me)
            })
            .collect();
        for record in &recorded {
            let PendingSubject::KeyCopy { audience, key, .. } = &record.subject else {
                unreachable!("filtered key copy subjects")
            };
            // A key not introduced yet keeps its tentative wait (see `confirm_tentative`).
            let introduced = introductions
                .iter()
                .any(|i| &i.audience == audience && i.key == *key);
            if introduced && !conditions.contains_key(&record.subject) {
                self.database.clear_pending(record.subject.clone()).await?;
            }
        }
        for (subject, reason) in conditions {
            if !recorded
                .iter()
                .any(|record| record.subject == subject && record.reason == reason)
            {
                self.database
                    .record_pending(PendingRecord {
                        subject,
                        reason,
                        reported_by: own,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    /// Open a received entry. Its key is accepted already, or a copy addressed
    /// to this member supplies it. A key not introduced in the applied log is
    /// tentative: it is confirmed only by opening this entry and matching the
    /// hash its introduction commits to, in this entry or in one opened earlier
    /// in the same scan; until then nothing enters custody (§11).
    pub(super) async fn open_entry(
        &self,
        log: &StoreLog,
        scanned: &[&StoreLogEntry],
        envelope: &SingleChunkObject<'_>,
        path: &ObjectPath,
        key: KeyId,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<EntryOpening, SyncError> {
        let held = |ring: &Option<StoreKeyring>| {
            ring.as_ref()
                .and_then(|ring| ring.store_key(key).ok())
                .map(|held| match object::open(envelope, path, held) {
                    Ok(entry) => EntryOpening::Opened(entry),
                    Err(failure) => EntryOpening::Refused(failure),
                })
        };
        if let Some(opening) = held(ring) {
            return Ok(opening);
        }
        let introductions = introductions(log);
        let condition = if introductions
            .iter()
            .any(|i| i.audience == Audience::Store && i.key == key)
        {
            self.acquire(&Audience::Store, key, &introductions, member, ring)
                .await?
        } else {
            let scanned = scanned
                .iter()
                .find_map(|entry| match introduction(&entry.change) {
                    Some((Audience::Store, introduced, hash)) if introduced == key => Some(hash),
                    _ => None,
                });
            match self
                .confirm_tentative(envelope, path, key, scanned, member, ring)
                .await?
            {
                Tentative::Confirmed => None,
                Tentative::Refused(failure) => return Ok(EntryOpening::Refused(failure)),
                // Its introduction has not been opened yet in this scan.
                Tentative::Uncommitted => return Ok(EntryOpening::WaitsForKey),
                Tentative::Waits(reason) => Some(reason),
            }
        };
        self.record_key_condition(&Audience::Store, key, member, condition.clone())
            .await?;
        Ok(match condition {
            None => held(ring).expect("acquired store key"),
            Some(_) => EntryOpening::WaitsForKey,
        })
    }

    async fn confirm_tentative(
        &self,
        envelope: &SingleChunkObject<'_>,
        path: &ObjectPath,
        key: KeyId,
        scanned: Option<[u8; 32]>,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<Tentative, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let mut refusal = None;
        for copy in keys::read_copies(
            &self.database,
            &self.reads,
            self.download_limit,
            storage,
            self.directory.id(),
        )
        .await?
        .copies
        {
            if copy.prefix.audience != Audience::Store
                || copy.prefix.key != key
                || !copy.prefix.held_by(member)
            {
                continue;
            }
            let candidate = SealedKey::decode(&copy.bytes)
                .map_err(Refusal::from)
                .and_then(|sealed| {
                    sealed
                        .open_store_key(member, copy.path.as_str())
                        .map_err(unopened)
                })
                .and_then(|candidate| {
                    if candidate.id() == key {
                        Ok(candidate)
                    } else {
                        Err(Refusal::WrongIdentity { cause: None })
                    }
                });
            let candidate = match candidate {
                Ok(candidate) => candidate,
                Err(failure) => {
                    refused(&copy, &failure);
                    refusal.get_or_insert(failure);
                    continue;
                }
            };
            let entry = match object::open(envelope, path, &candidate) {
                Ok(entry) => entry,
                // A key that cannot decrypt the entry is not the one it names.
                Err(failure @ Refusal::Decryption { .. }) => {
                    refused(&copy, &failure);
                    refusal.get_or_insert(failure);
                    continue;
                }
                // The key opened the entry, whose own bytes then failed.
                Err(failure) => return Ok(Tentative::Refused(failure)),
            };
            let commitment = match introduction(&entry.entry.change) {
                Some((Audience::Store, introduced, hash)) if introduced == key => Some(hash),
                _ => scanned,
            };
            let Some(commitment) = commitment else {
                return Ok(Tentative::Uncommitted);
            };
            if let Err(error) = candidate.check_hash(&commitment) {
                let failure = Refusal::WrongIdentity {
                    cause: Some(Arc::new(error)),
                };
                refused(&copy, &failure);
                refusal.get_or_insert(failure);
                continue;
            }
            match ring {
                Some(ring) => ring.insert_store_key(candidate)?,
                None => *ring = Some(StoreKeyring::new(candidate)),
            }
            self.store_keys
                .persist(ring.as_ref().expect("confirmed keyring"))?;
            return Ok(Tentative::Confirmed);
        }
        Ok(Tentative::Waits(refusal.map_or(
            PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key,
            },
            PendingReason::Refused,
        )))
    }

    /// Make `condition` this member's record for one key copy, or end it.
    /// Returns the record now held.
    pub(super) async fn record_key_condition(
        &self,
        audience: &Audience,
        key: KeyId,
        member: &MemberIdentity,
        condition: Option<PendingReason>,
    ) -> Result<Option<PendingRecord>, SyncError> {
        let subject = PendingSubject::KeyCopy {
            audience: audience.clone(),
            key,
            member: member.member(),
            sealing: member.keys().public_keys().sealing,
        };
        let own = self.database.local_store_log().await?.device;
        let recorded = self
            .database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .find(|record| record.subject == subject && record.reported_by == own);
        let Some(reason) = condition else {
            if recorded.is_some() {
                self.database.clear_pending(subject).await?;
            }
            return Ok(None);
        };
        let record = PendingRecord {
            subject,
            reason,
            reported_by: own,
        };
        if recorded.as_ref() != Some(&record) {
            self.database.record_pending(record.clone()).await?;
        }
        Ok(Some(record))
    }

    /// Open this member's retained copies of one key in writer/number order. A copy that fails
    /// to open, names another key or misses the agreed commitment is refused by
    /// itself; a later valid copy still supplies the key (§11). Returns the
    /// key's first unmet condition, or `None` once it is held.
    async fn acquire(
        &self,
        audience: &Audience,
        key: KeyId,
        introductions: &[Introduction],
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<Option<PendingReason>, SyncError> {
        let copies = keys::read_copies(
            &self.database,
            &self.reads,
            self.download_limit,
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            self.directory.id(),
        )
        .await?
        .copies;
        self.acquire_copies(audience, key, introductions, member, ring, &copies)
    }

    fn acquire_copies(
        &self,
        audience: &Audience,
        key: KeyId,
        introductions: &[Introduction],
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
        copies: &[keys::ObservedCopy],
    ) -> Result<Option<PendingReason>, SyncError> {
        if keys::holds(ring.as_ref(), audience, key) {
            return Ok(None);
        }
        let unavailable = PendingReason::KeyUnavailable {
            audience: audience.clone(),
            key,
        };
        // A keyring always holds a store key; circle keys wait for one.
        if ring.is_none() && *audience != Audience::Store {
            return Ok(Some(unavailable));
        }
        let mut refusal = None;
        for copy in copies {
            if copy.prefix.audience != *audience
                || copy.prefix.key != key
                || !copy.prefix.held_by(member)
            {
                continue;
            }
            match open(copy, introductions, member, ring) {
                Ok(()) => {
                    self.store_keys
                        .persist(ring.as_ref().expect("opened keyring"))?;
                    return Ok(None);
                }
                Err(failure) => {
                    refused(copy, &failure);
                    refusal.get_or_insert(failure);
                }
            }
        }
        Ok(Some(refusal.map_or(unavailable, PendingReason::Refused)))
    }
}

/// Open one copy addressed to this member and keep its key if accepted.
fn unopened(error: CryptoError) -> Refusal {
    match error {
        CryptoError::KeyIdentity => Refusal::WrongIdentity {
            cause: Some(Arc::new(error)),
        },
        error => Refusal::Decryption {
            cause: Some(Arc::new(error)),
        },
    }
}

fn open(
    copy: &keys::ObservedCopy,
    introductions: &[Introduction],
    member: &MemberIdentity,
    ring: &mut Option<StoreKeyring>,
) -> Result<(), Refusal> {
    let envelope = SealedKey::decode(&copy.bytes).map_err(Refusal::from)?;
    let path = copy.path.as_str();
    let conflict = |error: coven_crypto::MaterialError| Refusal::WrongIdentity {
        cause: Some(Arc::new(error)),
    };
    match &copy.prefix.audience {
        Audience::Store => {
            let opened = envelope.open_store_key(member, path).map_err(unopened)?;
            check(
                introductions,
                &copy.prefix,
                member,
                Opened {
                    audience: Audience::Store,
                    key: opened.id(),
                    hash: opened.hash(),
                },
            )?;
            match ring {
                Some(ring) => ring.insert_store_key(opened).map_err(conflict),
                None => {
                    *ring = Some(StoreKeyring::new(opened));
                    Ok(())
                }
            }
        }
        Audience::Circle(_) => {
            let opened = envelope.open_circle_key(member, path).map_err(unopened)?;
            check(
                introductions,
                &copy.prefix,
                member,
                Opened {
                    audience: Audience::Circle(opened.circle()),
                    key: opened.id(),
                    hash: opened.hash(),
                },
            )?;
            ring.as_mut()
                .expect("checked keyring")
                .insert_circle_key(opened)
                .map_err(conflict)
        }
    }
}

fn check(
    introductions: &[Introduction],
    prefix: &coven_format::sealed_key::KeyCopyPrefix,
    member: &MemberIdentity,
    opened: Opened,
) -> Result<(), Refusal> {
    // The box opened with the held private credential its prefix names.
    if accepts_copy(
        introductions,
        prefix,
        &member.member(),
        &prefix.sealing,
        &opened,
    ) {
        Ok(())
    } else {
        Err(Refusal::WrongIdentity { cause: None })
    }
}

#[cfg(test)]
#[path = "key_acquisition_tests.rs"]
mod tests;
