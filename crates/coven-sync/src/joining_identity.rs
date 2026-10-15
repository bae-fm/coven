//! Fixed joining keys and request bytes, sealed locally using the scanned invite.

use crate::SyncError;
use coven_crypto::{MemberIdentity, MemberKeys, ObjectHasher};
use coven_format::{
    codes::InviteCode,
    objects::JoinRequest,
    sealed_single::{SingleChunkObject, SingleChunkPrefix},
    Object,
};
use coven_foundation::files::{AtomicFile, StoreDir, StoreFile};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use zeroize::Zeroizing;

/// An unpublished member identity and its immutable request. The member id
/// is the one the invite fixes; the keys are this device's fresh pair. The
/// only durable local copy is encrypted with the invite secret, bound to the
/// complete code. No member keys or provider credentials enter final custody
/// while waiting.
pub struct JoiningIdentity {
    identity: MemberIdentity,
    request: Vec<u8>,
    path: ObjectPath,
    attempted: bool,
    initial_key: coven_foundation::id_source::KeyId,
    inviting_writer: coven_foundation::id_source::DeviceId,
    key_copy_number: u64,
}

impl JoiningIdentity {
    /// Restore a fixed request or prepare it before any provider mutation.
    /// A changed invite or device name cannot silently reuse another request.
    pub fn prepare(
        directory: &StoreDir,
        code: &InviteCode,
        device_name: &str,
    ) -> Result<Self, SyncError> {
        if code.store != directory.id() {
            return Err(SyncError::WrongStore {
                expected: directory.id(),
                actual: code.store,
            });
        }
        let file = directory.owned_file(StoreFile::Bootstrap);
        let binding = code.to_bytes()?;
        let path = ObjectPath::join_request(code.store, code.invite);
        let prefix = SingleChunkPrefix::JoinRequest;
        let saved = file.read_optional()?;
        if let Some(saved) = saved {
            let bytes = Zeroizing::new(code.secret.join_request_key().open_object_chunk(
                &local_path(code),
                &binding,
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
            let identity = MemberIdentity::new(
                code.member,
                MemberKeys::from_secret_bytes(
                    bytes
                        .get(5..end)
                        .ok_or(coven_database::DbError::DamagedDatabase)?,
                )?,
            );
            let keys = identity.keys().public_keys();
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
            keys.signing
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
                || opened.member != code.member
                || opened.keys != keys
                || opened.device_name != device_name
            {
                return Err(SyncError::InvitationChanged);
            }
            return Ok(Self {
                identity,
                request,
                path,
                attempted,
                initial_key: code.initial_key,
                inviting_writer: code.inviting_writer,
                key_copy_number: code.key_copy_number,
            });
        }
        // The request repeats the id the invite fixed and names this
        // device's fresh pair, which signs it (D8).
        let identity = MemberIdentity::generate(code.member)?;
        let plain = Object::JoinRequest(JoinRequest {
            invite: code.invite,
            member: code.member,
            keys: identity.keys().public_keys(),
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
            signature: identity.keys().sign_object(path.as_str(), &hash.finish()),
        }
        .encode()?;
        let identity = Self {
            identity,
            request,
            path,
            attempted: false,
            initial_key: code.initial_key,
            inviting_writer: code.inviting_writer,
            key_copy_number: code.key_copy_number,
        };
        identity.save(&file, code)?;
        Ok(identity)
    }

    /// The invite's member id with this device's pair, for composing the
    /// unpublished member custody.
    pub fn identity(&self) -> MemberIdentity {
        self.identity.clone()
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
        let keys = self.identity.keys().to_secret_bytes();
        let mut plain = Zeroizing::new(Vec::with_capacity(
            5 + keys.as_bytes().len() + self.request.len(),
        ));
        plain.push(u8::from(self.attempted));
        plain.extend_from_slice(&(keys.as_bytes().len() as u32).to_be_bytes());
        plain.extend_from_slice(keys.as_bytes());
        plain.extend_from_slice(&self.request);
        let sealed = code.secret.join_request_key().seal_object_chunk(
            &local_path(code),
            &code.to_bytes()?,
            0,
            0,
            &plain,
        )?;
        file.replace(&sealed)?;
        Ok(())
    }

    pub(crate) async fn publish(
        &self,
        storage: &dyn Storage,
        send: &crate::SendGate,
    ) -> Result<(), SyncError> {
        let path = &self.path;
        let _bootstrap = send.bootstrap([path.clone()].into(), false);
        match storage.create(path, &self.request).await {
            Ok(()) => Ok(()),
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                if storage.read(path).await? != self.request {
                    return Err(SyncError::InvitationChanged);
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn pending(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        Ok(storage.status(&self.path).await?.is_some())
    }

    pub(crate) async fn initial_key(
        &self,
        database: &coven_database::Database,
        limit: std::num::NonZeroUsize,
        storage: &dyn Storage,
    ) -> Result<Option<coven_crypto::StoreKey>, SyncError> {
        let store = self.path.store();
        let kind = coven_database::LogKind::KeyCopy;
        let mut input = crate::discovery::retained(database, store, kind).await?;
        crate::discovery::scan_tail(
            database,
            &crate::pass_reads::PassReads::default(),
            storage,
            store,
            kind,
            self.inviting_writer,
            self.key_copy_number,
            limit,
            &mut input,
        )
        .await?;
        for ((writer, number), read) in input {
            if writer != self.inviting_writer || number.get() <= self.key_copy_number {
                continue;
            }
            let sealed = match coven_format::sealed_key::SealedKey::decode(&read.bytes) {
                Ok(sealed) => sealed,
                Err(error) => {
                    if let Some(update) = crate::error::required_format(&error) {
                        return Err(SyncError::UpdateRequired(update));
                    }
                    tracing::warn!(path = read.metadata.path.as_str(), %error, "invalid invitation key copy");
                    continue;
                }
            };
            let prefix = sealed.prefix();
            if prefix.audience == coven_merge::Audience::Store
                && prefix.key == self.initial_key
                && prefix.held_by(&self.identity)
            {
                match sealed.open_store_key(&self.identity, read.metadata.path.as_str()) {
                    Ok(key) => return Ok(Some(key)),
                    Err(error) => {
                        tracing::warn!(path = read.metadata.path.as_str(), %error, "unusable invitation key copy")
                    }
                }
            }
        }
        Ok(None)
    }
}

fn local_path(code: &InviteCode) -> String {
    format!("coven/local-join/{}/{}", code.store, code.invite)
}
