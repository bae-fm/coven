//! Fixed original write envelopes and audience key derivation.

use crate::SyncError;
use coven_crypto::{CircleKey, DerivedKeys, MemberIdentity, ObjectHasher, StoreKey, StoreKeyring};
use coven_database::{UploadPlaintext, WaitingUpload};
use coven_format::sealed_write::WriteObjectLayout;
use coven_foundation::id_source::{KeyId, StoreId};
use coven_merge::Audience;

/// An audience's key this device holds.
pub(crate) enum HeldKey<'r> {
    Store(&'r StoreKey),
    Circle(&'r CircleKey),
}

impl HeldKey<'_> {
    /// The keys it derives for sealing and authorship.
    pub(crate) fn derive(&self) -> DerivedKeys {
        match self {
            Self::Store(key) => key.derive(),
            Self::Circle(key) => key.derive(),
        }
    }

    /// The hash its introduction commits to.
    pub(crate) fn hash(&self) -> [u8; 32] {
        match self {
            Self::Store(key) => key.hash(),
            Self::Circle(key) => key.hash(),
        }
    }
}

/// The audience's key `key`, when this device holds it.
pub(crate) fn held<'r>(
    ring: &'r StoreKeyring,
    audience: &Audience,
    key: KeyId,
) -> Option<HeldKey<'r>> {
    match audience {
        Audience::Store => ring.store_key(key).ok().map(HeldKey::Store),
        Audience::Circle(circle) => ring.circle_key(*circle, key).ok().map(HeldKey::Circle),
    }
}

/// The keys the audience's key `key` derives; `KeyUnavailable` when this
/// device doesn't hold it.
pub(crate) fn derive(
    ring: &StoreKeyring,
    audience: &Audience,
    key: KeyId,
) -> Result<DerivedKeys, SyncError> {
    held(ring, audience, key)
        .map(|held| held.derive())
        .ok_or_else(|| SyncError::KeyUnavailable { audience: audience.clone(), key })
}

pub(crate) fn seal(
    store: StoreId,
    upload: WaitingUpload<'_>,
    ring: &StoreKeyring,
    member: &MemberIdentity,
    emit: &mut dyn FnMut(&[u8]) -> Result<(), SyncError>,
) -> Result<(), SyncError> {
    let WaitingUpload { plaintext: UploadPlaintext { header, header_frame, parts }, keys: prefix } =
        upload;
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
    let path = coven_format::writes_slot::identity(store, header.header.position)?;
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
    let signature = member.keys().sign_write_header(path.as_str(), &aad, &piece, &roots);
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
