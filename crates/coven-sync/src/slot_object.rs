//! Authentication and bounded decoding of a complete sequence slot.

use crate::{
    chunk_reader::{ChunkReader, PartPlaintext, SlotPlaintext},
    history::History,
    write_object::{checked, damaged},
    Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, ObjectHasher, Signature, StoreKeyring};
use coven_database::{
    DownloadedPartStream, DownloadedWriteStream, HeaderEvidence, RetainedBytes, WriteAuthorship,
};
use coven_format::{
    floor::OriginalSlot,
    path::ObjectPath,
    sealed_slot::{BoundedSlot, SlotKind, SlotObjectPrefix},
    sealed_write::WriteObjectPrefix,
    sequence::{SlotFrame, SlotHeader},
};
use coven_merge::Audience;
use std::{
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

/// An authenticated slot: its signed envelope and its decoded body.
pub(crate) struct ReceivedSlot {
    pub(crate) envelope: SlotEnvelope,
    body: SlotBody,
}
/// Where a slot lives, its sealed header and routing, and its complete digest.
pub(crate) struct SlotEnvelope {
    pub(crate) path: ObjectPath,
    pub(crate) header: SlotHeader,
    pub(crate) prefix: SlotObjectPrefix,
    pub(crate) digest: [u8; 32],
}
impl ReceivedSlot {
    pub(crate) fn writes(&self) -> bool {
        matches!(self.body, SlotBody::Writes(_))
    }
    pub(crate) fn into_parts(self) -> (SlotEnvelope, SlotBody) {
        (self.envelope, self.body)
    }
}
pub(crate) enum SlotBody {
    Bounded {
        frame: SlotFrame,
        original: OriginalSlot,
    },
    Writes(Vec<DownloadedWriteStream<PartPlaintext>>),
}

pub(crate) fn read_exact(
    input: &mut impl Read,
    length: usize,
    path: &ObjectPath,
) -> Result<Vec<u8>, SyncError> {
    let mut bytes = vec![0; length];
    input
        .read_exact(&mut bytes)
        .map_err(|error| read_error(path, error))?;
    Ok(bytes)
}

pub(crate) fn read_error(path: &ObjectPath, error: std::io::Error) -> SyncError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        return damaged(
            path,
            Refusal::Parse {
                cause: Some(Arc::new(error)),
            },
        );
    }
    match error.get_ref() {
        Some(cause) if cause.is::<coven_crypto::CryptoError>() => damaged(
            path,
            Refusal::Decryption {
                cause: Some(Arc::new(
                    *error
                        .into_inner()
                        .expect("cause")
                        .downcast::<coven_crypto::CryptoError>()
                        .expect("crypto error"),
                )),
            },
        ),
        Some(cause) if cause.is::<coven_format::Error>() => {
            let format = *error
                .into_inner()
                .expect("cause")
                .downcast::<coven_format::Error>()
                .expect("format error");
            match checked::<()>(path, Err(format)) {
                Err(error) => error,
                Ok(()) => unreachable!("error supplied"),
            }
        }
        _ => SyncError::disk(error),
    }
}

/// A complete slot's clear prefix: its kind, sealing key and any copy
/// routing. It authenticates nothing until the slot's signature verifies.
pub(crate) fn clear_prefix(
    mut input: RetainedBytes,
    path: &ObjectPath,
) -> Result<SlotObjectPrefix, SyncError> {
    let size = input.len();
    let head = read_exact(
        &mut input,
        size.min(SlotObjectPrefix::MAX_LENGTH as u64) as usize,
        path,
    )?;
    let length = checked(path, SlotObjectPrefix::length(&head))?;
    checked(
        path,
        SlotObjectPrefix::decode(
            head.get(..length)
                .ok_or_else(|| damaged(path, Refusal::from(coven_format::Error::Truncated)))?,
        ),
    )
}

pub(crate) fn open(
    mut input: RetainedBytes,
    path: &ObjectPath,
    ring: &StoreKeyring,
    history: &History,
    readable: impl Fn(&Audience) -> bool,
) -> Result<ReceivedSlot, SyncError> {
    let size = input.len();
    let mut head = read_exact(
        &mut input,
        size.min(SlotObjectPrefix::MAX_LENGTH as u64) as usize,
        path,
    )?;
    let length = checked(path, SlotObjectPrefix::length(&head))?;
    let prefix = checked(
        path,
        SlotObjectPrefix::decode(
            head.get(..length)
                .ok_or_else(|| damaged(path, Refusal::from(coven_format::Error::Truncated)))?,
        ),
    )?;
    head.truncate(length);
    if !matches!(prefix.kind, SlotKind::Writes) {
        if size > coven_format::sequence::MAX_BOUNDED_SLOT as u64 {
            return Err(damaged(path, Refusal::Parse { cause: None }));
        }
        input.seek(SeekFrom::Start(0)).map_err(SyncError::disk)?;
        let bytes = read_exact(&mut input, size as usize, path)?;
        return open_bounded(bytes, path, ring, history);
    }
    let sealed_length = size
        .checked_sub(length as u64 + 64)
        .ok_or_else(|| damaged(path, Refusal::from(coven_format::Error::Truncated)))?;
    let plaintext_length = checked(
        path,
        coven_format::sealed_stream::plaintext_length(sealed_length),
    )?;
    if !history.accepted(ring, &coven_merge::Audience::Store, prefix.key) {
        return Err(SyncError::KeyUnavailable(prefix.key));
    }
    let key = crate::write_seal::derive(ring, &coven_merge::Audience::Store, prefix.key)?;
    let mut plain = ChunkReader::new(
        input.clone(),
        length as u64,
        plaintext_length,
        Arc::from(path.as_str()),
        Arc::from(head),
        Arc::new(key),
        0,
    );
    let bytes = read_exact(&mut plain, 47, path)?;
    let header = checked(path, SlotHeader::decode(&bytes, path))?;
    let count = u64::from_be_bytes(
        read_exact(&mut plain, 8, path)?
            .try_into()
            .expect("eight bytes"),
    );
    if count == 0 {
        return Err(damaged(path, Refusal::Parse { cause: None }));
    }
    let mut previous = None;
    let mut writes = Vec::new();
    for _ in 0..count {
        let length = u64::from_be_bytes(
            read_exact(&mut plain, 8, path)?
                .try_into()
                .expect("eight bytes"),
        );
        let position = plain.stream_position().map_err(SyncError::disk)?;
        if length == 0 || length > plain.len() - position {
            return Err(damaged(path, Refusal::Parse { cause: None }));
        }
        let write = inner(&mut plain, path, length, ring, history, &readable)?;
        checked(
            path,
            coven_format::writes_slot::check_write(&header, previous, &write.header.header),
        )?;
        if history
            .owner(
                write.header.header.position.device,
                write.header.header.read_through,
            )
            .map_err(|failure| damaged(path, failure))?
            .0
            != header.author
        {
            return Err(damaged(path, Refusal::NotAuthorized));
        }
        if writes
            .first()
            .is_some_and(|first: &DownloadedWriteStream<PartPlaintext>| {
                first.signer != write.signer
            })
        {
            return Err(damaged(path, Refusal::NotAuthorized));
        }
        previous = Some(write.header.header.position.number);
        writes.push(write);
    }
    if plain.stream_position().map_err(SyncError::disk)? != plain.len() {
        return Err(damaged(
            path,
            Refusal::from(coven_format::Error::TrailingBytes),
        ));
    }
    let digest = verify_complete(
        input,
        path,
        &writes.first().expect("positive batch count").signer,
        None,
    )?;
    Ok(ReceivedSlot {
        envelope: SlotEnvelope {
            path: path.clone(),
            header,
            prefix,
            digest,
        },
        body: SlotBody::Writes(writes),
    })
}

pub(crate) fn open_bounded(
    bytes: Vec<u8>,
    path: &ObjectPath,
    ring: &StoreKeyring,
    history: &History,
) -> Result<ReceivedSlot, SyncError> {
    let object = checked(path, BoundedSlot::decode(&bytes))?;
    let (header, frame) = bounded_plaintext(&bytes, path, ring)?;
    if !history.accepted(ring, &coven_merge::Audience::Store, object.prefix.key) {
        let introduced = match &frame {
            SlotFrame::Admin(change) => crate::history::introduction(change),
            _ => None,
        };
        match introduced {
            Some((coven_merge::Audience::Store, id, hash)) if id == object.prefix.key => {
                ring.store_key(id)?.check_hash(&hash).map_err(|error| {
                    damaged(
                        path,
                        Refusal::WrongIdentity {
                            cause: Some(Arc::new(error)),
                        },
                    )
                })?;
            }
            _ => return Err(SyncError::KeyUnavailable(object.prefix.key)),
        }
    }
    let signer = history
        .signer(&header, Some(&frame))
        .map_err(|failure| damaged(path, failure))?;
    let mut hash = ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    signer
        .verify_object(path.as_str(), &hash.finish(), &object.signature)
        .map_err(|error| {
            damaged(
                path,
                Refusal::Signature {
                    cause: Some(Arc::new(error)),
                },
            )
        })?;
    let prefix = object.prefix;
    let digest = *crate::identity::digest(&bytes).1.as_bytes();
    Ok(ReceivedSlot {
        envelope: SlotEnvelope {
            path: path.clone(),
            header,
            prefix,
            digest,
        },
        body: SlotBody::Bounded {
            frame,
            original: OriginalSlot {
                slot: path.slot_number().expect("checked slot path"),
                object: bytes,
            },
        },
    })
}

/// Bootstrap routing only. The caller must verify the original slot's
/// signature against reconstructed history before using it as authority.
pub(crate) fn bounded_plaintext(
    bytes: &[u8],
    path: &ObjectPath,
    ring: &StoreKeyring,
) -> Result<(SlotHeader, SlotFrame), SyncError> {
    let object = checked(path, BoundedSlot::decode(bytes))?;
    let prefix = object.prefix.encode()?;
    let key = crate::write_seal::derive(ring, &coven_merge::Audience::Store, object.prefix.key)?;
    let plain = key
        .open_object_chunk(path.as_str(), &prefix, 0, 0, object.chunk)
        .map_err(|error| {
            damaged(
                path,
                Refusal::Decryption {
                    cause: Some(Arc::new(error)),
                },
            )
        })?;
    let (header, frame) = checked(path, SlotFrame::decode(&plain, path))?;
    checked(path, object.prefix.kind.check_frame(&frame))?;
    Ok((header, frame))
}

/// The digests of a complete signed object: what its signature covers and
/// its complete bytes. Computing them needs the bytes once; verifying them
/// against a signer later needs no bytes.
#[derive(Clone)]
pub(crate) struct SignedDigest {
    signed: coven_crypto::ObjectDigest,
    complete: [u8; 32],
    signature: Signature,
}

impl SignedDigest {
    pub(crate) fn read(mut input: RetainedBytes, path: &ObjectPath) -> Result<Self, SyncError> {
        input.seek(SeekFrom::Start(0)).map_err(SyncError::disk)?;
        let mut left = input
            .len()
            .checked_sub(64)
            .ok_or_else(|| damaged(path, Refusal::from(coven_format::Error::Truncated)))?;
        let mut signed = ObjectHasher::new();
        let mut complete = ObjectHasher::new();
        while left > 0 {
            let bytes = read_exact(
                &mut input,
                left.min(coven_format::chunks::CHUNK_SIZE as u64) as usize,
                path,
            )?;
            signed.update(&bytes);
            complete.update(&bytes);
            left -= bytes.len() as u64;
        }
        let signature: [u8; 64] = read_exact(&mut input, 64, path)?
            .try_into()
            .expect("64 bytes");
        complete.update(&signature);
        Ok(Self {
            signed: signed.finish(),
            complete: *complete.finish().as_bytes(),
            signature: Signature::from_bytes(signature),
        })
    }

    /// Check the expected complete digest first, then the signature.
    pub(crate) fn verify(
        &self,
        path: &ObjectPath,
        signer: &coven_crypto::SigningKey,
        expected: Option<&[u8; 32]>,
    ) -> Result<[u8; 32], SyncError> {
        if expected.is_some_and(|expected| *expected != self.complete) {
            return Err(damaged(path, Refusal::WrongIdentity { cause: None }));
        }
        signer
            .verify_object(path.as_str(), &self.signed, &self.signature)
            .map_err(|error| {
                damaged(
                    path,
                    Refusal::Signature {
                        cause: Some(Arc::new(error)),
                    },
                )
            })?;
        Ok(self.complete)
    }
}

pub(crate) fn verify_complete(
    input: RetainedBytes,
    path: &ObjectPath,
    signer: &coven_crypto::SigningKey,
    expected: Option<&[u8; 32]>,
) -> Result<[u8; 32], SyncError> {
    SignedDigest::read(input, path)?.verify(path, signer, expected)
}

fn inner(
    input: &mut SlotPlaintext,
    path: &ObjectPath,
    length: u64,
    ring: &StoreKeyring,
    history: &History,
    readable: impl Fn(&Audience) -> bool,
) -> Result<DownloadedWriteStream<PartPlaintext>, SyncError> {
    let start = input.stream_position().map_err(SyncError::disk)?;
    let mut bytes = read_exact(input, 23, path)?;
    let prefix_length = checked(path, WriteObjectPrefix::length(&bytes))?;
    bytes.extend(read_exact(input, prefix_length - bytes.len(), path)?);
    let prefix = checked(path, WriteObjectPrefix::decode(&bytes))?;
    let aad: Arc<[u8]> = Arc::from(bytes.clone());
    let mut hash = ObjectHasher::new();
    hash.update(&bytes);
    let mut piece = read_exact(input, 4, path)?;
    let header_length = checked(path, WriteObjectPrefix::header_chunk_length(&piece))?;
    piece.extend(read_exact(input, header_length - 4, path)?);
    hash.update(&piece);
    let sealed = checked(path, WriteObjectPrefix::header_chunk(&piece))?;
    let key = crate::write_seal::derive(ring, &coven_merge::Audience::Store, prefix.store_key)?;
    let plain = key
        .open_identified_header::<{ coven_format::FRAME_PREFIX_LEN + 16 }>(&aad, sealed, |hint| {
            let write = coven_format::merge_fields::decode_write_id(
                &hint[coven_format::FRAME_PREFIX_LEN..],
            )
            .map_err(|_| coven_crypto::CryptoError::KeyIdentity)?;
            coven_format::writes_slot::identity(path.store(), write)
                .map_err(|_| coven_crypto::CryptoError::KeyIdentity)
        })
        .map_err(|error| {
            damaged(
                path,
                Refusal::Decryption {
                    cause: Some(Arc::new(error)),
                },
            )
        })?;
    let header = checked(
        path,
        coven_format::write_stream::WriteHeaderFrame::decode(&plain),
    )?;
    let identity: Arc<str> = Arc::from(coven_format::writes_slot::identity(
        path.store(),
        header.header.position,
    )?);
    let signer = history
        .write_signer(&header)
        .map_err(|failure| damaged(path, failure))?;
    let expected = checked(
        path,
        coven_format::sealed_write::sealed_length(
            plain.len(),
            &header
                .parts
                .iter()
                .map(|part| part.plaintext_length)
                .collect::<Vec<_>>(),
        ),
    )?;
    if expected != length {
        return Err(damaged(path, Refusal::Parse { cause: None }));
    }
    let tail_bytes = read_exact(
        input,
        WriteObjectPrefix::tail_length(prefix.part_keys.len()),
        path,
    )?;
    hash.update(&tail_bytes);
    let tail = checked(path, prefix.decode_tail(&tail_bytes))?;
    signer
        .verify_write_header(&identity, &aad, &piece, &tail.roots, &tail.signature)
        .map_err(|error| {
            damaged(
                path,
                Refusal::Signature {
                    cause: Some(Arc::new(error)),
                },
            )
        })?;
    let evidence = HeaderEvidence {
        prefix: aad.to_vec(),
        header: piece,
        roots: tail.roots,
        signature: *tail.signature.as_bytes(),
    };
    let mut layout = checked(
        path,
        prefix.clone().opened_header(
            &plain,
            header
                .parts
                .iter()
                .map(|part| part.plaintext_length)
                .collect(),
        ),
    )?;
    let mut parts = Vec::new();
    let mut hashers = Vec::new();
    while let Some(coordinate) = layout.next_chunk() {
        let part = &header.parts[coordinate.section as usize - 1];
        if coordinate.index == 0 {
            let readable = readable(&part.audience);
            let opens = readable && history.accepted(ring, &part.audience, coordinate.key);
            if readable && !opens {
                return Err(SyncError::KeyUnavailable(coordinate.key));
            }
            if opens {
                let key = Arc::new(crate::write_seal::derive(
                    ring,
                    &part.audience,
                    coordinate.key,
                )?);
                hashers.push(Some(key.authorship_hasher()));
                let position = input.stream_position().map_err(SyncError::disk)?;
                parts.push(DownloadedPartStream::Opened(ChunkReader::new(
                    input.clone(),
                    position,
                    part.plaintext_length,
                    identity.clone(),
                    aad.clone(),
                    key,
                    coordinate.section,
                )));
            } else {
                hashers.push(None);
                parts.push(DownloadedPartStream::Skipped);
            }
        }
        let mut bytes = read_exact(input, 4, path)?;
        let chunk_length = checked(path, layout.chunk_length(&bytes))?;
        bytes.extend(read_exact(input, chunk_length - 4, path)?);
        checked(path, layout.decode_chunk(&bytes))?;
        hash.update(&bytes);
    }
    let signature = checked(path, layout.read_signature(&read_exact(input, 64, path)?))?;
    checked(path, layout.finish(&[]))?;
    signer
        .verify_object(&identity, &hash.finish(), &signature)
        .map_err(|error| {
            damaged(
                path,
                Refusal::Signature {
                    cause: Some(Arc::new(error)),
                },
            )
        })?;
    if input.stream_position().map_err(SyncError::disk)? - start != length {
        return Err(damaged(path, Refusal::Parse { cause: None }));
    }
    Ok(DownloadedWriteStream {
        header,
        parts,
        signer,
        authorship: WriteAuthorship { evidence, hashers },
    })
}

pub(crate) fn seal_bounded(
    path: &ObjectPath,
    header: &SlotHeader,
    frame: &SlotFrame,
    prefix: SlotObjectPrefix,
    ring: &StoreKeyring,
    member: &MemberIdentity,
) -> Result<Vec<u8>, SyncError> {
    let aad = prefix.encode()?;
    let plain = frame.encode(header, path)?;
    let key = crate::write_seal::derive(ring, &coven_merge::Audience::Store, prefix.key)?;
    let sealed = key.reseal_object_chunk(path.as_str(), &aad, 0, 0, &plain);
    let mut bytes = prefix.encode_chunk(&sealed)?;
    let mut hash = ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(
        member
            .keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    Ok(bytes)
}
