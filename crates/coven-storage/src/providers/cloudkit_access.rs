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

struct Participant(Retained<CKShareParticipant>);
// SAFETY: Each participant is copied from its callback and moved to one caller.
// It is not Sync or Clone; all subsequent mutation is exclusive to that caller.
unsafe impl Send for Participant {}
struct Metadata(Retained<CKShareMetadata>);
// SAFETY: Share metadata is an immutable copied callback result, consumed by
// one acceptance operation. No alias can mutate it during that transfer.
unsafe impl Send for Metadata {}

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
    let url = NSURL::URLWithString(&NSString::from_str(url.as_str()))
        .ok_or(StorageFailure::InvitationMismatch)?;
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
            let identity = person.userIdentity();
            if identity
                .userRecordID()
                .is_none_or(|id| id.recordName().to_string() != current)
            {
                return Err(StorageFailure::Protocol
                    .with_source("CloudKit participant disagrees with signed-in account"));
            }
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
        unsafe {
            let share = share.share()?;
            if share.publicPermission() != CKShareParticipantPermission::None {
                return Err(StorageFailure::PermissionDenied
                    .with_source("CloudKit public sharing must be managed by its owner"));
            }
            let is_owner = person
                .0
                .userIdentity()
                .userRecordID()
                .is_some_and(|id| id.recordName().to_string() == self.owner);
            if !is_owner {
                person
                    .0
                    .setPermission(CKShareParticipantPermission::ReadWrite);
                share.addParticipant(&person.0);
            }
        }
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
    pub(super) async fn revoke(
        &self,
        member: &MemberAccess,
    ) -> Result<Vec<RetainedAccess>, StorageError> {
        let MemberAccess::ProviderAccount(account) = member else {
            return Err(StorageFailure::InvalidConfiguration.into());
        };
        self.require_owner().await?;
        if account == &self.owner {
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
        let mut retained = Vec::new();
        let mut removed = false;
        unsafe {
            let native_share = share.share()?;
            if native_share.publicPermission() != CKShareParticipantPermission::None {
                return Ok(vec![RetainedAccess {
                    provider_id: self.share_name(),
                    reason: RetainedAccessReason::OtherAccounts,
                }]);
            }
            for person in native_share.participants() {
                let identity = person.userIdentity();
                let id = identity
                    .userRecordID()
                    .map(|id| id.recordName().to_string());
                let matches = match (&target_id, &id) {
                    (Some(target), Some(id)) => target == id,
                    _ => identity
                        .lookupInfo()
                        .and_then(|lookup| lookup.emailAddress())
                        .is_some_and(|email| email.to_string().eq_ignore_ascii_case(account)),
                };
                if matches && person.role() == CKShareParticipantRole::Owner {
                    retained.push(RetainedAccess {
                        provider_id: person.participantID().to_string(),
                        reason: RetainedAccessReason::StoreOwner,
                    });
                } else if matches {
                    native_share.removeParticipant(&person);
                    removed = true;
                } else if id.is_none()
                    && identity
                        .lookupInfo()
                        .and_then(|lookup| lookup.emailAddress())
                        .is_none()
                {
                    retained.push(RetainedAccess {
                        provider_id: person.participantID().to_string(),
                        reason: RetainedAccessReason::UnidentifiedAccount,
                    });
                }
            }
        }
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
            return Err(StorageFailure::InvitationMismatch.into());
        };
        let metadata = metadata(&self.container, url)?.wait().await?;
        let already_accepted = unsafe {
            let share = metadata.0.share();
            let id = share.recordID();
            let zone_id = id.zoneID();
            if metadata.0.containerIdentifier().to_string() != self.container_id
                || zone_id.zoneName().to_string() != self.zone
                || zone_id.ownerName().to_string() != self.owner
                || id.recordName().to_string() != self.share_name()
            {
                return Err(StorageFailure::InvitationMismatch.into());
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
