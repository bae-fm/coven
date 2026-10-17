//! Fixed-size encryption of plaintext streams before provider publication.

use coven_crypto::{DerivedKeys, MemberIdentity, ObjectHasher, Signature};
use coven_format::{chunks::CHUNK_SIZE, sealed_stream::StreamObjectLayout};
use coven_storage::ObjectPath;
use std::io::{self, Read};

pub(crate) struct SealedStream<R> {
    input: R,
    path: ObjectPath,
    prefix: Vec<u8>,
    key: DerivedKeys,
    member: MemberIdentity,
    layout: StreamObjectLayout,
    hash: Option<ObjectHasher>,
    piece: io::Cursor<Vec<u8>>,
}
impl<R> SealedStream<R> {
    pub(crate) fn new(
        input: R,
        path: ObjectPath,
        prefix: Vec<u8>,
        prefix_signature: Option<Signature>,
        key: DerivedKeys,
        member: MemberIdentity,
    ) -> Self {
        let mut bytes = prefix.clone();
        if let Some(signature) = prefix_signature {
            bytes.extend_from_slice(signature.as_bytes());
        }
        let mut hash = ObjectHasher::new();
        hash.update(&bytes);
        Self {
            input,
            path,
            prefix,
            key,
            member,
            layout: StreamObjectLayout::new(),
            hash: Some(hash),
            piece: io::Cursor::new(bytes),
        }
    }
}
impl<R: Read> Read for SealedStream<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let length = self.piece.read(output)?;
            if length > 0 {
                return Ok(length);
            }
            if self.hash.is_none() {
                return Ok(0);
            }
            let mut plaintext = zeroize::Zeroizing::new(Vec::new());
            self.input
                .by_ref()
                .take(CHUNK_SIZE as u64)
                .read_to_end(&mut plaintext)?;
            let bytes = if plaintext.is_empty() {
                let hash = self.hash.take().expect("stream has a hash").finish();
                self.member
                    .keys()
                    .sign_object(self.path.as_str(), &hash)
                    .as_bytes()
                    .to_vec()
            } else {
                let sealed = if self.path.slot_number().is_some() {
                    self.key.reseal_object_chunk(
                        self.path.as_str(),
                        &self.prefix,
                        0,
                        self.layout.index(),
                        &plaintext,
                    )
                } else {
                    self.key
                        .seal_object_chunk(
                            self.path.as_str(),
                            &self.prefix,
                            0,
                            self.layout.index(),
                            &plaintext,
                        )
                        .map_err(io::Error::other)?
                };
                let piece = self
                    .layout
                    .encode_chunk(&sealed)
                    .map_err(io::Error::other)?;
                self.hash
                    .as_mut()
                    .expect("stream has a hash")
                    .update(&piece);
                piece
            };
            self.piece = io::Cursor::new(bytes);
        }
    }
}
