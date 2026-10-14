//! The database-to-envelope stream; no write body is collected here.

use crate::SyncError;
use coven_crypto::{DerivedKeys, MemberId, MemberKeys, ObjectHasher, StoreKeyring};
use coven_database::{DbError, StoreLog, WaitingUpload};
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write_stream::WriteHeaderFrame;
use coven_foundation::id_source::{KeyId, StoreId};
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

/// The store key for a first attempt; see [`current`].
pub(crate) fn current_store(log: &StoreLog, ring: &StoreKeyring) -> Result<KeyId, SyncError> {
    current_key(log, ring, &Audience::Store, None)
}

/// The key for a first attempt: the newest acceptable introduction of the
/// audience, once held. Copies that expose a key are not consulted here.
pub(crate) fn current(
    log: &StoreLog,
    ring: &StoreKeyring,
    audience: &Audience,
    member: &MemberId,
) -> Result<KeyId, SyncError> {
    current_key(log, ring, audience, Some(member))
}

fn current_key(
    log: &StoreLog,
    ring: &StoreKeyring,
    audience: &Audience,
    member: Option<&MemberId>,
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
    let needed = crate::key_introductions::needed(log);
    let introductions = crate::key_introductions::introductions(log);
    // An existing audience has its kept creation's introduction. When every
    // introduction has conflicting commitments, the newest one names the wait.
    let selected = match crate::key_introductions::newest(log, audience) {
        Some(key) => key,
        None => {
            introductions
                .iter()
                .rev()
                .find(|introduction| &introduction.audience == audience)
                .ok_or(DbError::DamagedDatabase)?
                .key
        }
    };
    if crate::key_introductions::accepted(Some(ring), &needed, audience, selected) {
        return Ok(selected);
    }
    // A former circle member still writes its earlier readable history (§14.3).
    // Select the newest held, accepted introduction, never UUID order.
    if matches!((audience, member), (Audience::Circle(circle), Some(member)) if !state.circles[circle].members.contains(member))
    {
        for introduction in introductions.iter().rev() {
            if &introduction.audience == audience
                && crate::key_introductions::accepted(
                    Some(ring),
                    &needed,
                    audience,
                    introduction.key,
                )
            {
                return Ok(introduction.key);
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
        return Err(SyncError::Removed);
    }
    Ok(())
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

pub(crate) fn select(
    log: &StoreLog,
    schema: u32,
    header: &WriteHeaderFrame,
    ring: &StoreKeyring,
    member: &MemberKeys,
) -> Result<WriteObjectPrefix, SyncError> {
    check_member(log, member, header.header.position.device)?;
    check_upload_version(log, schema)?;
    let store_key = current_store(log, ring)?;
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
    store: StoreId,
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
    let path = path(store, header.header.position);
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
