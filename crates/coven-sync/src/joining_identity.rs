//! Fixed joining keys and request bytes, sealed locally using the scanned invite.

use crate::SyncError;
use coven_crypto::{
    ConfirmationNumber, MemberIdentity, MemberKeys, MemberPublicKeys, ObjectHasher,
};
use coven_format::{
    codes::{InviteCode, InviteDeliveryKind},
    objects::JoinRequest,
    sealed_single::{SingleChunkObject, SingleChunkPrefix},
    Object,
};
use coven_foundation::files::StoreDirAccess;
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
    /// The name the device gave itself in its request.
    device_name: String,
    inviting_writer: coven_foundation::id_source::DeviceId,
    read_through: u64,
    /// Set for a sent invite, whose admin compares it before approving.
    confirmation: Option<ConfirmationNumber>,
}

impl JoiningIdentity {
    /// Restore a fixed request or prepare it before any provider mutation.
    /// A changed invite or device name cannot silently reuse another request.
    pub fn prepare(
        directory: &StoreDir,
        code: &InviteCode,
        device_name: &str,
    ) -> Result<Self, SyncError> {
        match Self::saved(directory, code)? {
            Some(saved) if saved.device_name != device_name => Err(SyncError::InvitationChanged),
            Some(saved) => Ok(saved),
            None => Self::create(directory, code, device_name),
        }
    }

    /// Continue a join from its saved request, with the device name that
    /// request carries (§13.2). A join's code is saved only after its
    /// request, so a missing request is damaged local state.
    pub fn resume(directory: &StoreDir, code: &InviteCode) -> Result<Self, SyncError> {
        Self::saved(directory, code)?.ok_or_else(|| coven_database::DbError::DamagedDatabase.into())
    }

    /// The request this directory keeps for `code`, opened with the code's
    /// secret and checked against it.
    fn saved(directory: &StoreDir, code: &InviteCode) -> Result<Option<Self>, SyncError> {
        if code.store != directory.id() {
            return Err(SyncError::WrongStore { expected: directory.id(), actual: code.store });
        }
        let file = directory.owned_file(StoreFile::Bootstrap);
        let binding = code.to_bytes()?;
        let path = ObjectPath::join_request(code.store, code.invite);
        let prefix = SingleChunkPrefix::JoinRequest;
        let Some(saved) = file.read_optional()? else {
            return Ok(None);
        };
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
        let key_length = bytes.get(1..5).ok_or(coven_database::DbError::DamagedDatabase)?;
        let key_length = u32::from_be_bytes(key_length.try_into().expect("four bytes")) as usize;
        let end = 5usize.checked_add(key_length).ok_or(coven_database::DbError::DamagedDatabase)?;
        let identity = MemberIdentity::new(
            code.member,
            MemberKeys::from_secret_bytes(
                bytes.get(5..end).ok_or(coven_database::DbError::DamagedDatabase)?,
            )?,
        );
        let keys = identity.keys().public_keys();
        let request = bytes.get(end..).ok_or(coven_database::DbError::DamagedDatabase)?.to_vec();
        let object = SingleChunkObject::decode(&request)?;
        let SingleChunkObject::JoinRequest { chunk, signature } = &object;
        let mut hash = ObjectHasher::new();
        hash.update(&object.signed_bytes()?);
        keys.signing.verify_object(path.as_str(), &hash.finish(), signature)?;
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
        if opened.invite != code.invite || opened.member != code.member || opened.keys != keys {
            return Err(SyncError::InvitationChanged);
        }
        Ok(Some(Self {
            confirmation: confirmation(code, &keys),
            identity,
            request,
            path,
            attempted,
            device_name: opened.device_name,
            inviting_writer: code.inviting_writer,
            read_through: code.read_through,
        }))
    }

    fn create(
        directory: &StoreDir,
        code: &InviteCode,
        device_name: &str,
    ) -> Result<Self, SyncError> {
        let path = ObjectPath::join_request(code.store, code.invite);
        let prefix = SingleChunkPrefix::JoinRequest;
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
            confirmation: confirmation(code, &identity.keys().public_keys()),
            identity,
            request,
            path,
            attempted: false,
            device_name: device_name.into(),
            inviting_writer: code.inviting_writer,
            read_through: code.read_through,
        };
        identity.save(&directory.owned_file(StoreFile::Bootstrap), code)?;
        Ok(identity)
    }

    /// The invite's member id with this device's pair, for composing the
    /// unpublished member custody.
    pub fn identity(&self) -> MemberIdentity {
        self.identity.clone()
    }

    /// The inviting device: only its kept binding admits this request (§13.2).
    pub fn approver(&self) -> coven_foundation::id_source::DeviceId {
        self.inviting_writer
    }

    /// The inviting device's applied slot when it made the invite: copies for
    /// this request can only follow it (§13.2).
    pub fn read_through(&self) -> u64 {
        self.read_through
    }

    /// The name the device gave itself in its request, which it registers
    /// with once admitted.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// The number the admin of a sent invite compares before approving; the
    /// admin's device computes it from the same keys (format.md D11).
    pub fn confirmation(&self) -> Option<ConfirmationNumber> {
        self.confirmation
    }

    /// The request's path, named by its invite.
    pub fn path(&self) -> &ObjectPath {
        &self.path
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

    /// The provider refused the request, so it was never published: a
    /// retry sends it again (§13.2).
    pub fn record_refusal(
        &mut self,
        file: &AtomicFile,
        code: &InviteCode,
    ) -> Result<(), SyncError> {
        self.attempted = false;
        self.save(file, code)
    }

    fn save(&self, file: &AtomicFile, code: &InviteCode) -> Result<(), SyncError> {
        let keys = self.identity.keys().to_secret_bytes();
        let mut plain =
            Zeroizing::new(Vec::with_capacity(5 + keys.as_bytes().len() + self.request.len()));
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

    /// Publish the recorded immutable request. An occupied path must hold
    /// exactly these bytes (§19.3).
    pub async fn publish(&self, storage: &dyn Storage) -> Result<(), SyncError> {
        match storage.create(&self.path, &self.request).await {
            Ok(_) => Ok(()),
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                if storage.read(&self.path).await? != self.request {
                    return Err(SyncError::InvitationChanged);
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Whether the request object still exists.
    pub(crate) async fn pending(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        Ok(storage.status(&self.path).await?.is_some())
    }
}

fn confirmation(code: &InviteCode, keys: &MemberPublicKeys) -> Option<ConfirmationNumber> {
    match code.delivery {
        InviteDeliveryKind::InPerson => None,
        InviteDeliveryKind::Sent => Some(ConfirmationNumber::of(code.invite, keys)),
    }
}

fn local_path(code: &InviteCode) -> String {
    format!("coven/local-join/{}/{}", code.store, code.invite)
}
