//! The database-to-envelope stream; no write body is collected here.

use crate::{SyncError, SyncFailure};
use coven_crypto::{DerivedKeys, MemberId, MemberKeys, ObjectHasher, StoreKeyring};
use coven_database::{DbError, EntryOutcome, StoreLog, WaitingUpload};
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write_stream::WriteHeaderFrame;
use coven_foundation::id_source::KeyId;
use coven_merge::{Audience, WriteId};
use coven_storage::ObjectPath;

pub(crate) fn path(write: WriteId) -> ObjectPath {
    ObjectPath::device_log(
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

pub(crate) fn current(
    log: &StoreLog,
    ring: &StoreKeyring,
    audience: &Audience,
    member: &MemberId,
) -> Result<KeyId, SyncError> {
    let selected = match audience {
        Audience::Store => log.replay.state.store.as_ref().map(|store| store.key),
        Audience::Circle(circle) => log
            .replay
            .state
            .circles
            .get(circle)
            .map(|circle| circle.key),
    }
    .ok_or(SyncError::Rejected(coven_database::DropReason::TargetGone))?;
    if crate::store_log_keys::holds(Some(ring), audience, selected) {
        return Ok(selected);
    }
    // A former circle member still writes its earlier readable history (§14.3).
    // Select the latest held, kept introduction in replay order, never UUID order.
    if matches!(audience, Audience::Circle(circle) if !log.replay.state.circles[circle].members.contains(member))
    {
        for applied in log.entries.iter().rev() {
            if log.replay.entries[&applied.entry.position] != EntryOutcome::Kept {
                continue;
            }
            for (owner, key) in crate::store_log_keys::introduced(&applied.entry.change) {
                if &owner == audience && crate::store_log_keys::holds(Some(ring), audience, key) {
                    return Ok(key);
                }
            }
        }
    }
    Err(SyncError::KeyUnavailable(selected))
}

pub(crate) fn check_member(
    log: &StoreLog,
    member: &MemberKeys,
    device: coven_foundation::id_source::DeviceId,
) -> Result<(), SyncError> {
    let state = &log.replay.state;
    let identity = member.member_id();
    if crate::effects::member(state, &identity).is_none()
        || crate::effects::device(state, device).is_none_or(|d| d.member != identity)
    {
        return Err(SyncFailure::Removed.into());
    }
    Ok(())
}

pub(crate) fn check_upload_version(log: &StoreLog, schema: u32) -> Result<(), SyncError> {
    if log
        .replay
        .state
        .schema
        .get(&Audience::Store)
        .is_some_and(|v| v.number > schema)
    {
        return Err(SyncFailure::UpdateRequired.into());
    }
    Ok(())
}

pub(crate) fn select(
    log: &StoreLog,
    schema: u32,
    header: &WriteHeaderFrame,
    ring: &StoreKeyring,
    member: &MemberKeys,
) -> Result<WriteObjectPrefix, SyncError> {
    check_member(log, member, header.header.position.device)?;
    check_upload_version(log, schema)?;
    let store_key = current(log, ring, &Audience::Store, &member.member_id())?;
    Ok(WriteObjectPrefix {
        format: coven_format::FormatVersion::CURRENT,
        store_key,
        part_keys: header
            .parts
            .iter()
            .map(|part| current(log, ring, &part.audience, &member.member_id()))
            .collect::<Result<_, _>>()?,
    })
}

pub(crate) fn seal(
    upload: WaitingUpload<'_>,
    ring: &StoreKeyring,
    member: &MemberKeys,
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
    let mut layout = WriteObjectLayout::new(
        prefix,
        &header_frame,
        header.parts.iter().map(|p| p.plaintext_length).collect(),
    )?;
    let aad = layout.prefix()?;
    let path = path(header.header.position);
    let mut hash = ObjectHasher::new();
    hash.update(&aad);
    emit(&aad)?;
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
    chunk(&mut layout, &Audience::Store, &header_frame)?;
    for (part, bytes) in parts {
        for plain in bytes {
            chunk(&mut layout, &part.audience, &plain?)?;
        }
    }
    let signature = member.sign_object(path.as_str(), &hash.finish());
    emit(&layout.signature(&signature)?)?;
    layout.finish(&[])?;
    Ok(())
}
