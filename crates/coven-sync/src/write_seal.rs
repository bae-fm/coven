//! The database-to-envelope stream; no write body is collected here.

use crate::key_introductions::{newest, Wait};
use crate::store_log_keys::ObservedCopy;
use crate::{PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_crypto::{DerivedKeys, MemberIdentity, ObjectHasher, StoreKeyring};
use coven_database::{DbError, StoreLog, WaitingUpload};
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write_stream::WriteHeaderFrame;
use coven_foundation::id_source::{KeyId, MemberId, StoreId};
use coven_merge::{Audience, WriteId};
use coven_storage::ObjectPath;

pub(crate) fn path(store: StoreId, write: WriteId) -> ObjectPath {
    ObjectPath::device_log(
        store,
        write.device,
        write.number.try_into().expect("positive write number"),
    )
}

pub(crate) fn derive(
    ring: &StoreKeyring,
    audience: &Audience,
    key: KeyId,
) -> Result<DerivedKeys, SyncError> {
    let derived = match audience {
        Audience::Store => ring.store_key(key).map(|key| key.derive()),
        Audience::Circle(circle) => ring.circle_key(*circle, key).map(|key| key.derive()),
    };
    derived.map_err(|error| match error {
        coven_crypto::MaterialError::UnknownStoreKey(_)
        | coven_crypto::MaterialError::UnknownCircleKey { .. } => SyncError::KeyUnavailable(key),
        error => error.into(),
    })
}

/// The key for a first attempt (§11): Appendix C12 selection over the
/// completed catch-up's copy observation. A member who has left a circle still sends its
/// pre-removal writes with the newest key it holds there (§14.6); that exposes
/// no new circle data to it.
pub(crate) fn first_attempt(
    log: &StoreLog,
    observation: &[ObservedCopy],
    ring: &StoreKeyring,
    member: &MemberId,
    audience: &Audience,
) -> Result<KeyId, SyncError> {
    let state = &log.replay.state;
    match audience {
        Audience::Store if state.store.is_none() => return Err(SyncError::NoStorage),
        Audience::Circle(circle) if !state.circles.contains_key(circle) => {
            return Err(SyncError::Rejected(coven_database::DropReason::TargetGone(
                coven_database::EntryTarget::Circle(*circle),
            )))
        }
        _ => {}
    }
    let introductions = crate::key_introductions::introductions(log);
    let copies = Some(
        observation
            .iter()
            .map(|copy| copy.prefix.clone())
            .collect::<Vec<_>>(),
    );
    let waiting = |key: Option<KeyId>| match key {
        Some(key) => SyncError::KeyUnavailable(key),
        None => DbError::Invariant {
            what: "a readable audience has introduced a key",
        }
        .into(),
    };
    match crate::key_introductions::select(
        &introductions,
        &crate::member_credentials::Credentials::new(log),
        member,
        copies.as_deref(),
        crate::key_introductions::held_hash(Some(ring)),
        audience,
    ) {
        Ok(key) => Ok(key),
        Err(Wait::Listing) => unreachable!("complete observation"),
        Err(Wait::Key) => Err(waiting(newest(log, audience))),
        Err(Wait::Membership) => match audience {
            Audience::Store => Err(SyncError::Removed),
            Audience::Circle(_) => readable_key(log, ring, audience),
        },
    }
}

/// The newest key of an audience this device holds and accepts, for reading.
/// Unlock needs a readable store key; this never decides a first attempt.
pub(crate) fn readable_key(
    log: &StoreLog,
    ring: &StoreKeyring,
    audience: &Audience,
) -> Result<KeyId, SyncError> {
    let needed = crate::key_introductions::needed(log);
    crate::key_introductions::introductions(log)
        .iter()
        .rev()
        .find(|i| {
            &i.audience == audience
                && crate::key_introductions::accepted(Some(ring), &needed, audience, i.key)
        })
        .map(|i| i.key)
        .ok_or_else(|| match newest(log, audience) {
            Some(key) => SyncError::KeyUnavailable(key),
            None => DbError::Invariant {
                what: "a readable audience has introduced a key",
            }
            .into(),
        })
}

/// Sending needs this installation's device and its member's current
/// credentials in the latest replay: an installation still holding replaced
/// keys stops sending, keeping its device id and queues (§10).
pub(crate) fn check_member(
    log: &StoreLog,
    member: &MemberIdentity,
    device: coven_foundation::id_source::DeviceId,
) -> Result<(), SyncError> {
    check_device(log, member, device)?;
    let current = crate::effects::member(&log.replay.state, &member.member())
        .expect("checked member")
        .keys;
    if current != member.keys().public_keys() {
        return Err(SyncError::MemberKeysReplaced {
            member: member.member(),
        });
    }
    Ok(())
}

/// This installation's device is still registered to its active member;
/// reading needs no more than this.
pub(crate) fn check_device(
    log: &StoreLog,
    member: &MemberIdentity,
    device: coven_foundation::id_source::DeviceId,
) -> Result<(), SyncError> {
    let state = &log.replay.state;
    let identity = member.member();
    match crate::effects::device(state, device) {
        _ if crate::effects::member(state, &identity).is_none() => Err(SyncError::Removed),
        Some(d) if d.member != identity => Err(SyncError::Removed),
        // A kept replacement of this id stops this copy; it never sends (§10).
        Some(d) if d.state == coven_database::DeviceState::Replaced => {
            Err(SyncError::DeviceIdInUse(crate::DeviceIdEvidence::Replaced))
        }
        Some(_) => Ok(()),
        None => Err(SyncError::Removed),
    }
}

pub(crate) fn check_upload_version(log: &StoreLog, schema: u32) -> Result<(), SyncError> {
    if let Some(version) = log
        .replay
        .state
        .schema
        .get(&Audience::Store)
        .filter(|v| v.number > schema)
    {
        return Err(SyncError::UpdateRequired(
            crate::RequiredUpdate::AppSchema {
                version: version.number,
            },
        ));
    }
    Ok(())
}

/// Fix a write's first-attempt keys. An audience without a usable key makes
/// the write wait: the error is a pending `KeyUnavailable` for that write.
pub(crate) fn select(
    log: &StoreLog,
    schema: u32,
    header: &WriteHeaderFrame,
    ring: &StoreKeyring,
    member: &MemberIdentity,
    observation: &[ObservedCopy],
) -> Result<WriteObjectPrefix, SyncError> {
    let write = header.header.position;
    check_member(log, member, write.device)?;
    check_upload_version(log, schema)?;
    let me = member.member();
    let first = |audience: &Audience| {
        first_attempt(log, observation, ring, &me, audience).map_err(|error| match error {
            SyncError::KeyUnavailable(key) => SyncError::Pending(Box::new(PendingRecord {
                subject: PendingSubject::Write(write),
                reason: PendingReason::KeyUnavailable {
                    audience: audience.clone(),
                    key,
                },
                reported_by: write.device,
            })),
            error => error,
        })
    };
    Ok(WriteObjectPrefix {
        format: coven_format::FormatVersion::CURRENT,
        store_key: first(&Audience::Store)?,
        part_keys: header
            .parts
            .iter()
            .map(|part| first(&part.audience))
            .collect::<Result<_, _>>()?,
    })
}

/// The write a first attempt left waiting for a usable key in one of its
/// audiences (§11), from the error `select` returned.
pub(crate) fn key_wait(error: &SyncError) -> Option<&PendingRecord> {
    match error {
        SyncError::Pending(record)
            if matches!(record.subject, PendingSubject::Write(_))
                && matches!(record.reason, PendingReason::KeyUnavailable { .. }) =>
        {
            Some(record)
        }
        _ => None,
    }
}

pub(crate) fn seal(
    store: StoreId,
    upload: WaitingUpload<'_>,
    ring: &StoreKeyring,
    member: &MemberIdentity,
    emit: &mut dyn FnMut(&[u8]) -> Result<(), SyncError>,
) -> Result<(), SyncError> {
    let WaitingUpload {
        header,
        header_frame,
        parts,
        keys,
    } = upload;
    let prefix = keys.ok_or(DbError::UploadNotAttempted {
        write: header.header.position,
    })?;
    let coven_format::FormatVersion::V1 = prefix.format;
    // Each part's keyed leaves commit to its original authorship (D11); the
    // header signature lets a snapshot carry them without the parts (D9).
    let roots = parts.roots::<SyncError>(|index, part| {
        Ok(derive(ring, &part.audience, prefix.part_keys[index])?.authorship_hasher())
    })?;
    let mut layout = WriteObjectLayout::new(
        prefix,
        &header_frame,
        header.parts.iter().map(|p| p.plaintext_length).collect(),
    )?;
    let aad = layout.prefix()?;
    let path = path(store, header.header.position);
    let mut hash = ObjectHasher::new();
    hash.update(&aad);
    emit(&aad)?;
    let coordinate = layout.next_chunk().expect("declared header chunk");
    let sealed = derive(ring, &Audience::Store, coordinate.key)?.reseal_object_chunk(
        path.as_str(),
        &aad,
        coordinate.section,
        coordinate.index,
        &header_frame,
    );
    let piece = layout.encode_chunk(&sealed)?;
    let signature = member
        .keys()
        .sign_write_header(path.as_str(), &aad, &piece, &roots);
    let tail = layout.tail(&coven_format::sealed_write::HeaderTail { roots, signature })?;
    hash.update(&piece);
    hash.update(&tail);
    emit(&piece)?;
    emit(&tail)?;
    let mut chunk = |layout: &mut WriteObjectLayout,
                     audience: &Audience,
                     plain: &[u8]|
     -> Result<(), SyncError> {
        let coordinate = layout.next_chunk().expect("declared plaintext chunk");
        let sealed = derive(ring, audience, coordinate.key)?.reseal_object_chunk(
            path.as_str(),
            &aad,
            coordinate.section,
            coordinate.index,
            plain,
        );
        let piece = layout.encode_chunk(&sealed)?;
        hash.update(&piece);
        emit(&piece)?;
        Ok(())
    };
    for (part, bytes) in parts {
        for plain in bytes {
            chunk(&mut layout, &part.audience, &plain?)?;
        }
    }
    let signature = member.keys().sign_object(path.as_str(), &hash.finish());
    emit(&layout.signature(&signature)?)?;
    layout.finish(&[])?;
    Ok(())
}
