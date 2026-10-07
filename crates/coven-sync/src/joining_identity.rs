//! Fixed joining keys and request bytes, sealed locally using the scanned invite.

use crate::SyncError;
use coven_crypto::{MemberKeys, ObjectHasher};
use coven_format::{
    codes::InviteCode,
    objects::JoinRequest,
    sealed_single::{SingleChunkObject, SingleChunkPrefix},
    store_log::MemberPublicKeys,
    Object,
};
use coven_foundation::files::AtomicFile;
use coven_storage::{ObjectPath, Storage, StorageFailure};
use zeroize::Zeroizing;

/// An unpublished member identity and its immutable request. The only durable
/// local copy is encrypted with the invite secret, bound to store and invite.
/// No member keys or provider credentials enter final custody while waiting.
pub struct JoiningIdentity {
    keys: MemberKeys,
    request: Vec<u8>,
    invite: coven_foundation::id_source::InviteId,
    attempted: bool,
}

impl JoiningIdentity {
    /// Restore a fixed request or prepare it before any provider mutation.
    /// A changed invite or device name cannot silently reuse another request.
    pub fn prepare(
        file: &AtomicFile,
        code: &InviteCode,
        device_name: &str,
    ) -> Result<Self, SyncError> {
        let path = ObjectPath::join_request(code.invite);
        let prefix = SingleChunkPrefix::JoinRequest;
        let saved = file.read_optional()?;
        if let Some(saved) = saved {
            let bytes = Zeroizing::new(code.secret.join_request_key().open_object_chunk(
                &local_path(code),
                code.store.0.as_bytes(),
                0,
                0,
                &saved,
            )?);
            let attempted = match bytes.first() {
                Some(0) => false,
                Some(1) => true,
                _ => return Err(coven_database::DbError::DamagedDatabase.into()),
            };
            let key_length = bytes
                .get(1..5)
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            let key_length =
                u32::from_be_bytes(key_length.try_into().expect("four bytes")) as usize;
            let end = 5usize
                .checked_add(key_length)
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            let keys = MemberKeys::from_secret_bytes(
                bytes
                    .get(5..end)
                    .ok_or(coven_database::DbError::DamagedDatabase)?,
            )?;
            let request = bytes
                .get(end..)
                .ok_or(coven_database::DbError::DamagedDatabase)?
                .to_vec();
            let object = SingleChunkObject::decode(&request)?;
            let SingleChunkObject::JoinRequest { chunk, signature } = &object else {
                return Err(coven_database::DbError::DamagedDatabase.into());
            };
            let mut hash = ObjectHasher::new();
            hash.update(&object.signed_bytes()?);
            keys.member_id()
                .verify_object(path.as_str(), &hash.finish(), signature)?;
            let plain = code.secret.join_request_key().open_object_chunk(
                path.as_str(),
                &prefix.encode()?,
                0,
                0,
                chunk,
            )?;
            let Object::JoinRequest(opened) = Object::decode(&plain)? else {
                return Err(coven_database::DbError::DamagedDatabase.into());
            };
            if opened.invite != code.invite
                || opened.keys.signing != keys.member_id()
                || opened.keys.sealing != keys.sealing_public_key()
                || opened.device_name != device_name
            {
                return Err(SyncError::InvitationChanged);
            }
            return Ok(Self {
                keys,
                request,
                invite: code.invite,
                attempted,
            });
        }
        let keys = MemberKeys::generate()?;
        let plain = Object::JoinRequest(JoinRequest {
            invite: code.invite,
            keys: MemberPublicKeys {
                signing: keys.member_id(),
                sealing: keys.sealing_public_key(),
            },
            device_name: device_name.into(),
        })
        .encode()?;
        let chunk = code.secret.join_request_key().seal_object_chunk(
            path.as_str(),
            &prefix.encode()?,
            0,
            0,
            &plain,
        )?;
        let mut hash = ObjectHasher::new();
        hash.update(&prefix.encode_chunk(&chunk)?);
        let request = SingleChunkObject::JoinRequest {
            chunk: &chunk,
            signature: keys.sign_object(path.as_str(), &hash.finish()),
        }
        .encode()?;
        let identity = Self {
            keys,
            request,
            invite: code.invite,
            attempted: false,
        };
        identity.save(file, code)?;
        Ok(identity)
    }

    /// Secret material for composing the unpublished member custody.
    pub fn member_keys(&self) -> MemberKeys {
        self.keys.clone()
    }

    /// Whether publication may already have happened before an interruption.
    pub fn attempted(&self) -> bool {
        self.attempted
    }

    /// Record possible publication before issuing it. On restart an absent
    /// request is terminal: re-creating it could resurrect a declined invite.
    pub fn record_attempt(
        &mut self,
        file: &AtomicFile,
        code: &InviteCode,
    ) -> Result<(), SyncError> {
        self.attempted = true;
        self.save(file, code)
    }

    fn save(&self, file: &AtomicFile, code: &InviteCode) -> Result<(), SyncError> {
        let keys = self.keys.to_secret_bytes();
        let mut plain = Zeroizing::new(Vec::with_capacity(
            5 + keys.as_bytes().len() + self.request.len(),
        ));
        plain.push(u8::from(self.attempted));
        plain.extend_from_slice(&(keys.as_bytes().len() as u32).to_be_bytes());
        plain.extend_from_slice(keys.as_bytes());
        plain.extend_from_slice(&self.request);
        let sealed = code.secret.join_request_key().seal_object_chunk(
            &local_path(code),
            code.store.0.as_bytes(),
            0,
            0,
            &plain,
        )?;
        file.replace(&sealed)?;
        Ok(())
    }

    pub(crate) async fn publish(&self, storage: &dyn Storage) -> Result<(), SyncError> {
        let path = ObjectPath::join_request(self.invite);
        match storage.create(&path, &self.request).await {
            Ok(()) => Ok(()),
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                if storage.read(&path).await? != self.request {
                    return Err(SyncError::InvitationChanged);
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn pending(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        match storage.read(&ObjectPath::join_request(self.invite)).await {
            Ok(bytes) if bytes == self.request => Ok(true),
            Ok(_) => Err(SyncError::InvitationChanged),
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

fn local_path(code: &InviteCode) -> String {
    format!("coven/local-join/{}/{}", code.store, code.invite)
}
