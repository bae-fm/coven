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
    /// The entry, opened with an accepted key.
    Opened(StoreLogEntry),
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
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<(), SyncError> {
        let introductions = introductions(log);
        let me = member.member_id();
        let mut conditions = BTreeMap::new();
        for (audience, key) in needed(log).into_keys() {
            let Some(reason) = self
                .acquire(&audience, key, &introductions, member, ring, damages)
                .await?
            else {
                continue;
            };
            // Only a current audience member waits for a copy; a copy addressed
            // to this member that fails its checks is reported either way.
            if matches!(reason, PendingReason::Refused(_))
                || keys::recipients(log, &audience).any(|(id, _)| *id == me)
            {
                let subject = PendingSubject::KeyCopy {
                    audience,
                    key,
                    member: me.clone(),
                };
                conditions.insert(subject, reason);
            }
        }
        self.settle_key_conditions(&me, &introductions, conditions)
            .await
    }

    async fn settle_key_conditions(
        &self,
        me: &coven_crypto::MemberId,
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
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
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
            self.acquire(&Audience::Store, key, &introductions, member, ring, damages)
                .await?
        } else {
            let scanned = scanned
                .iter()
                .find_map(|entry| match introduction(&entry.change) {
                    Some((Audience::Store, introduced, hash)) if introduced == key => Some(hash),
                    _ => None,
                });
            match self
                .confirm_tentative(envelope, path, key, scanned, member, ring, damages)
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
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<Tentative, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let mut refusal = None;
        for copy in keys::read_copies(
            &self.database,
            &self.reads,
            self.download_limit,
            storage,
            self.directory.id(),
            damages,
        )
        .await?
        {
            if copy.prefix.audience != Audience::Store
                || copy.prefix.key != key
                || copy.prefix.member != member.member_id()
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
            let commitment = match introduction(&entry.change) {
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

    async fn record_key_condition(
        &self,
        audience: &Audience,
        key: KeyId,
        member: &MemberKeys,
        condition: Option<PendingReason>,
    ) -> Result<(), SyncError> {
        let subject = PendingSubject::KeyCopy {
            audience: audience.clone(),
            key,
            member: member.member_id(),
        };
        let own = self.database.local_store_log().await?.device;
        let recorded = self
            .database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .find(|record| record.subject == subject && record.reported_by == own);
        match condition {
            None if recorded.is_some() => self.database.clear_pending(subject).await?,
            Some(reason) if recorded.is_none_or(|record| record.reason != reason) => {
                self.database
                    .record_pending(PendingRecord {
                        subject,
                        reason,
                        reported_by: own,
                    })
                    .await?
            }
            _ => (),
        }
        Ok(())
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
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
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
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let mut refusal = None;
        for copy in keys::read_copies(
            &self.database,
            &self.reads,
            self.download_limit,
            storage,
            self.directory.id(),
            damages,
        )
        .await?
        {
            if copy.prefix.audience != *audience
                || copy.prefix.key != key
                || copy.prefix.member != member.member_id()
            {
                continue;
            }
            match open(&copy, introductions, member, ring) {
                Ok(()) => {
                    self.store_keys
                        .persist(ring.as_ref().expect("opened keyring"))?;
                    return Ok(None);
                }
                Err(failure) => {
                    refused(&copy, &failure);
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
    member: &MemberKeys,
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
    member: &MemberKeys,
    opened: Opened,
) -> Result<(), Refusal> {
    if accepts_copy(introductions, prefix, &member.member_id(), &opened) {
        Ok(())
    } else {
        Err(Refusal::WrongIdentity { cause: None })
    }
}

#[cfg(test)]
#[path = "key_acquisition_tests.rs"]
mod tests;
