//! Native CloudKit sharing and recipient acceptance.

use super::{
    native::{self as native, completion, error, CloudKitRequest},
    record::Record,
    CloudKitStorage,
};
use crate::*;
use block2::RcBlock;
use coven_crypto::SecretText;
use objc2::{rc::Retained, AnyThread};
use objc2_cloud_kit::{
    CKAcceptSharesOperation, CKContainer, CKFetchShareMetadataOperation,
    CKFetchShareParticipantsOperation, CKRecordSavePolicy, CKShare, CKShareMetadata,
    CKShareParticipant, CKShareParticipantAcceptanceStatus, CKShareParticipantPermission,
    CKShareParticipantRole, CKUserIdentityLookupInfo,
};
use objc2_foundation::{NSArray, NSCopying, NSError, NSString, NSURL};
use std::{
    ptr::NonNull,
    sync::{Arc, Mutex},
};

/// Where a share lives, from its metadata or from the invitation's location.
/// None of it is secret; the share URL never enters it.
struct ShareIdentity {
    container: String,
    zone: String,
    owner: String,
    record: String,
}

impl ShareIdentity {
    /// Every part of the share's metadata that differs from the invitation's
    /// location, with both values, or None when the share is the expected one.
    fn mismatch(&self, expected: &ShareIdentity) -> Option<String> {
        let differences: Vec<_> = [
            ("container", &self.container, &expected.container),
            ("zone name", &self.zone, &expected.zone),
            ("zone owner name", &self.owner, &expected.owner),
            ("share record name", &self.record, &expected.record),
        ]
        .into_iter()
        .filter(|(_, found, expected)| found != expected)
        .map(|(part, found, expected)| {
            format!("share metadata {part} {found:?}, invitation expects {expected:?}")
        })
        .collect();
        (!differences.is_empty()).then(|| differences.join("; "))
    }
}

struct Participant(Retained<CKShareParticipant>);
// SAFETY: Each participant is copied from its callback and moved to one caller.
// It is not Sync or Clone; all subsequent mutation is exclusive to that caller.
unsafe impl Send for Participant {}
struct Metadata(Retained<CKShareMetadata>);
// SAFETY: Share metadata is an immutable copied callback result, consumed by
// one acceptance operation. No alias can mutate it during that transfer.
unsafe impl Send for Metadata {}

/// One share participant as granting and revocation see it.
struct Seen {
    /// The participant's user record name, once CloudKit knows it.
    id: Option<String>,
    /// The email address it was looked up by, if any.
    email: Option<String>,
    owner: bool,
    participant_id: String,
}

/// One participant of a share as CloudKit returned it.
///
/// # Safety
/// `person` comes from a share record this caller holds, as every share
/// accessor here requires.
unsafe fn seen(person: &CKShareParticipant) -> Seen {
    let identity = person.userIdentity();
    Seen {
        id: identity
            .userRecordID()
            .map(|id| id.recordName().to_string()),
        email: identity
            .lookupInfo()
            .and_then(|lookup| lookup.emailAddress())
            .map(|email| email.to_string()),
        owner: person.role() == CKShareParticipantRole::Owner,
        participant_id: person.participantID().to_string(),
    }
}

/// Whether the saved share holds the participant just added for `account`:
/// `added` itself, or the participant added earlier for that account, which
/// CloudKit updates in its place. When the address belongs to the zone
/// owner's own iCloud account, CloudKit saves the share without it: only the
/// owner remains, and the owner cannot accept its own zone's share.
fn holds_invitee(participants: &[Seen], added: &str, account: &str) -> bool {
    participants.iter().any(|person| {
        !person.owner
            && (person.participant_id == added
                || person
                    .email
                    .as_deref()
                    .is_some_and(|email| email.eq_ignore_ascii_case(account)))
    })
}

/// What removing `account`, whose user record name is `target` when known,
/// does to a share: the participants to remove and the access that stays
/// (§4). A public share still reaches the removed member while it stays
/// public, which is reported; the participant is removed all the same, so
/// turning public access off later cuts them off.
fn revocation(
    share_name: &str,
    public: bool,
    participants: &[Seen],
    target: Option<&str>,
    account: &str,
) -> (Vec<usize>, Vec<RetainedAccess>) {
    let mut removed = Vec::new();
    let mut retained = Vec::new();
    if public {
        retained.push(RetainedAccess {
            provider_id: share_name.to_owned(),
            reason: RetainedAccessReason::OtherAccounts,
        });
    }
    for (index, person) in participants.iter().enumerate() {
        let matches = match (target, &person.id) {
            (Some(target), Some(id)) => target == id,
            _ => person
                .email
                .as_deref()
                .is_some_and(|email| email.eq_ignore_ascii_case(account)),
        };
        if matches && person.owner {
            retained.push(RetainedAccess {
                provider_id: person.participant_id.clone(),
                reason: RetainedAccessReason::StoreOwner,
            });
        } else if matches {
            removed.push(index);
        } else if person.id.is_none() && person.email.is_none() {
            retained.push(RetainedAccess {
                provider_id: person.participant_id.clone(),
                reason: RetainedAccessReason::UnidentifiedAccount,
            });
        }
    }
    (removed, retained)
}

fn participant(container: &CKContainer, email: &str) -> CloudKitRequest<Participant> {
    let (complete, result) = completion();
    let value = Arc::new(Mutex::new(None));
    let fetched = value.clone();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let lookup = CKUserIdentityLookupInfo::initWithEmailAddress(
            CKUserIdentityLookupInfo::alloc(),
            &NSString::from_str(email),
        );
        let operation = CKFetchShareParticipantsOperation::initWithUserIdentityLookupInfos(
            CKFetchShareParticipantsOperation::alloc(),
            &NSArray::from_retained_slice(&[lookup]),
        );
        let block = RcBlock::new(
            move |_lookup: NonNull<CKUserIdentityLookupInfo>,
                  participant: *mut CKShareParticipant,
                  failure: *mut NSError| {
                let response = match error(failure) {
                    Some(error) => Err(error),
                    None => participant
                        .as_ref()
                        .map(|value| Participant(value.copy()))
                        .ok_or_else(|| StorageFailure::Protocol.into()),
                };
                *fetched.lock().expect("CloudKit participant lock poisoned") = Some(response);
            },
        );
        operation.setPerShareParticipantCompletionBlock(Some(&block));
        let block = RcBlock::new(move |failure: *mut NSError| {
            let response = match error(failure) {
                Some(error) => Err(error),
                None => value
                    .lock()
                    .expect("CloudKit participant lock poisoned")
                    .take()
                    .unwrap_or_else(|| {
                        Err(StorageFailure::Protocol.with_source("CloudKit omitted participant"))
                    }),
            };
            complete(response);
        });
        operation.setFetchShareParticipantsCompletionBlock(Some(&block));
        container.addOperation(&operation);
        operation.into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

fn metadata(
    container: &CKContainer,
    url: &SecretText,
) -> Result<CloudKitRequest<Metadata>, StorageError> {
    let url = NSURL::URLWithString(&NSString::from_str(url.as_str())).ok_or_else(|| {
        StorageFailure::InvitationMismatch.with_source("NSURL cannot parse the share URL")
    })?;
    let (complete, result) = completion();
    let value = Arc::new(Mutex::new(None));
    let fetched = value.clone();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = CKFetchShareMetadataOperation::initWithShareURLs(
            CKFetchShareMetadataOperation::alloc(),
            &NSArray::from_retained_slice(&[url]),
        );
        let block = RcBlock::new(
            move |_url: NonNull<NSURL>, metadata: *mut CKShareMetadata, failure: *mut NSError| {
                let response = match error(failure) {
                    Some(error) => Err(error),
                    None => metadata
                        .as_ref()
                        .map(|value| Metadata(value.copy()))
                        .ok_or_else(|| StorageFailure::Protocol.into()),
                };
                *fetched.lock().expect("CloudKit metadata lock poisoned") = Some(response);
            },
        );
        operation.setPerShareMetadataBlock(Some(&block));
        let block = RcBlock::new(move |failure: *mut NSError| {
            let response = match error(failure) {
                Some(error) => Err(error),
                None => value
                    .lock()
                    .expect("CloudKit metadata lock poisoned")
                    .take()
                    .unwrap_or_else(|| {
                        Err(StorageFailure::Protocol.with_source("CloudKit omitted share metadata"))
                    }),
            };
            complete(response);
        });
        operation.setFetchShareMetadataCompletionBlock(Some(&block));
        container.addOperation(&operation);
        operation.into_super().into_super()
    });
    Ok(CloudKitRequest { operation, result })
}

fn accept(container: &CKContainer, metadata: Metadata) -> CloudKitRequest<()> {
    let (complete, result) = completion();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = CKAcceptSharesOperation::initWithShareMetadatas(
            CKAcceptSharesOperation::alloc(),
            &NSArray::from_retained_slice(&[metadata.0]),
        );
        let block = RcBlock::new(move |failure: *mut NSError| {
            complete(match error(failure) {
                Some(error) => Err(error),
                None => Ok(()),
            })
        });
        operation.setAcceptSharesCompletionBlock(Some(&block));
        container.addOperation(&operation);
        operation.into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

impl CloudKitStorage {
    pub(super) async fn account_identity(&self) -> Result<String, StorageError> {
        let current = native::current_account(&self.container).wait().await?;
        if current == self.owner {
            return Ok(current);
        }
        let mut share = self.fetch(&self.share_name(), &[]).wait().await?;
        unsafe {
            let person = share
                .share()?
                .currentUserParticipant()
                .ok_or(StorageFailure::Authentication)?;
            // The current user participant is the signed-in account. Live, its
            // user record reads `__defaultOwner__` (CKCurrentUserDefaultName),
            // never the record name the current-user fetch returns.
            let identity = person.userIdentity();
            // Every participant this adapter adds is looked up by email. Keep
            // that invitation identity on reconnect; an opaque current-user id
            // would not compare equal to this member's existing access entry.
            let email = identity
                .lookupInfo()
                .and_then(|lookup| lookup.emailAddress())
                .ok_or_else(|| {
                    StorageFailure::Protocol
                        .with_source("CloudKit omitted the participant's invitation email")
                })?
                .to_string();
            if email.trim().is_empty() {
                return Err(StorageFailure::Authentication.into());
            }
            Ok(email.trim().to_ascii_lowercase())
        }
    }

    fn share_name(&self) -> String {
        unsafe {
            CKShare::initWithRecordZoneID(CKShare::alloc(), &self.zone_id())
                .recordID()
                .recordName()
                .to_string()
        }
    }
    pub(super) async fn grant(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.require_owner().await?;
        let mut share = match self.fetch(&self.share_name(), &[]).wait().await {
            Ok(share) => share,
            Err(error) if error.failure() == StorageFailure::NotFound => {
                Record::from_share(unsafe {
                    CKShare::initWithRecordZoneID(CKShare::alloc(), &self.zone_id())
                })
            }
            Err(error) => return Err(error),
        };
        let person = participant(&self.container, account).wait().await?;
        let added = unsafe {
            let share = share.share()?;
            if share.publicPermission() != CKShareParticipantPermission::None {
                return Err(StorageFailure::PermissionDenied
                    .with_source("CloudKit public sharing must be managed by its owner"));
            }
            person
                .0
                .setPermission(CKShareParticipantPermission::ReadWrite);
            share.addParticipant(&person.0);
            person.0.participantID().to_string()
        };
        let mut saved = native::modify(
            &self.database,
            vec![share],
            &[],
            CKRecordSavePolicy::IfServerRecordUnchanged,
            None,
            vec![],
        )
        .wait()
        .await?;
        if saved.len() != 1 {
            return Err(StorageFailure::Protocol.into());
        }
        let participants: Vec<Seen> = unsafe {
            saved[0]
                .share()?
                .participants()
                .iter()
                .map(|person| seen(&person))
                .collect()
        };
        if !holds_invitee(&participants, &added, account) {
            return Err(StorageFailure::InviteeIsStoreOwner.into());
        }
        let url = unsafe { saved[0].share()?.URL() }.ok_or(StorageFailure::Protocol)?;
        let url = url
            .absoluteString()
            .ok_or(StorageFailure::Protocol)?
            .to_string();
        Ok(AccessGrant::Granted {
            invitation: StorageInvitation::new(
                self.config(),
                crate::invitation::InvitationAcceptance::CloudKitShare {
                    url: SecretText::new(url),
                },
            )?,
        })
    }
    pub(super) async fn revoke(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.require_owner().await?;
        if account == self.owner {
            return Ok(vec![RetainedAccess {
                provider_id: self.owner.clone(),
                reason: RetainedAccessReason::StoreOwner,
            }]);
        }
        let target = participant(&self.container, account).wait().await?;
        let target_id = unsafe {
            target
                .0
                .userIdentity()
                .userRecordID()
                .map(|id| id.recordName().to_string())
        };
        if target_id.as_deref() == Some(self.owner.as_str()) {
            return Ok(vec![RetainedAccess {
                provider_id: self.owner.clone(),
                reason: RetainedAccessReason::StoreOwner,
            }]);
        }
        let mut share = match self.fetch(&self.share_name(), &[]).wait().await {
            Ok(share) => share,
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(vec![]),
            Err(error) => return Err(error),
        };
        let (removed, retained) = unsafe {
            let native_share = share.share()?;
            let people: Vec<_> = native_share.participants().into_iter().collect();
            let seen: Vec<Seen> = people.iter().map(|person| seen(person)).collect();
            let public = native_share.publicPermission() != CKShareParticipantPermission::None;
            let (removed, retained) = revocation(
                &self.share_name(),
                public,
                &seen,
                target_id.as_deref(),
                account,
            );
            for index in &removed {
                native_share.removeParticipant(&people[*index]);
            }
            (!removed.is_empty(), retained)
        };
        if removed {
            native::modify(
                &self.database,
                vec![share],
                &[],
                CKRecordSavePolicy::IfServerRecordUnchanged,
                None,
                vec![],
            )
            .wait()
            .await?;
        }
        Ok(retained)
    }
    pub(super) async fn accept(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        let crate::invitation::InvitationAcceptance::CloudKitShare { url } = &invitation.acceptance
        else {
            return Err(StorageFailure::InvitationMismatch
                .with_source("the invitation carries no CloudKit share URL"));
        };
        let metadata = metadata(&self.container, url)?.wait().await?;
        let already_accepted = unsafe {
            let share = metadata.0.share();
            let id = share.recordID();
            let zone_id = id.zoneID();
            let found = ShareIdentity {
                container: metadata.0.containerIdentifier().to_string(),
                zone: zone_id.zoneName().to_string(),
                owner: zone_id.ownerName().to_string(),
                record: id.recordName().to_string(),
            };
            let expected = ShareIdentity {
                container: self.container_id.clone(),
                zone: self.zone.clone(),
                owner: self.owner.clone(),
                record: self.share_name(),
            };
            if let Some(mismatch) = found.mismatch(&expected) {
                return Err(StorageFailure::InvitationMismatch.with_source(mismatch));
            }
            if metadata.0.participantPermission() != CKShareParticipantPermission::ReadWrite {
                return Err(StorageFailure::PermissionDenied.into());
            }
            metadata.0.participantStatus() == CKShareParticipantAcceptanceStatus::Accepted
        };
        if !already_accepted {
            accept(&self.container, metadata).wait().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "cloudkit_access_tests.rs"]
mod tests;
