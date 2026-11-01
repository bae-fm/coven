//! Fixed joining keys and request bytes, sealed locally using the scanned invite.

use crate::SyncError;
use coven_crypto::{
    ConfirmationNumber, EncryptionKey, MemberIdentity, MemberKeys, MemberPublicKeys, ObjectHasher,
};
use coven_format::{
    codes::{InviteCode, InviteDeliveryKind},
    objects::JoinRequest,
    sealed_single::{SingleChunkObject, SingleChunkPrefix},
    Object,
};
use coven_format::{device_gone::DeviceGone, invite_end::InviteEnd};
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
    state: RequestState,
    /// What seals the saved request for the code it was made for.
    sealing: Sealing,
    /// The name the device gave itself in its request.
    device_name: String,
    inviting_writer: coven_foundation::id_source::DeviceId,
    read_through: u64,
    /// When the invite ends, in seconds since the Unix epoch: no first
    /// request is sent after it.
    expires_at: u64,
    /// Set for a sent invite, whose admin compares it before approving.
    confirmation: Option<ConfirmationNumber>,
}

/// How far the join has come, as its saved request records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestState {
    /// Not sent: a retry sends it.
    Unsent,
    /// It may have been sent: on restart an absent request is final.
    Sent,
    /// Its copies opened this member's keys: approval has begun (§13.2).
    Opened,
}

impl RequestState {
    fn byte(self) -> u8 {
        match self {
            Self::Unsent => 0,
            Self::Sent => 1,
            Self::Opened => 2,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Unsent),
            1 => Some(Self::Sent),
            2 => Some(Self::Opened),
            _ => None,
        }
    }
}

/// The invite secret's key and the complete code that seal the saved
/// request, so it opens only with the code it was made for.
struct Sealing {
    key: EncryptionKey,
    binding: Zeroizing<Vec<u8>>,
    path: String,
}

impl Sealing {
    fn new(code: &InviteCode) -> Result<Self, SyncError> {
        Ok(Self {
            key: code.secret.join_request_key(),
            binding: code.to_bytes()?,
            path: local_path(code),
        })
    }
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
        let sealing = Sealing::new(code)?;
        let path = ObjectPath::join_request(code.store, code.inviting_writer, code.invite);
        let prefix = SingleChunkPrefix::JoinRequest;
        let Some(sealed) = file.read_optional()? else {
            return Ok(None);
        };
        let bytes = Zeroizing::new(sealing.key.open_object_chunk(
            &sealing.path,
            &sealing.binding,
            0,
            0,
            &sealed,
        )?);
        let state = bytes
            .first()
            .copied()
            .and_then(RequestState::from_byte)
            .ok_or(coven_database::DbError::DamagedDatabase)?;
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
            state,
            sealing,
            device_name: opened.device_name,
            inviting_writer: code.inviting_writer,
            read_through: code.read_through,
            expires_at: code.expires_at,
        }))
    }

    fn create(
        directory: &StoreDir,
        code: &InviteCode,
        device_name: &str,
    ) -> Result<Self, SyncError> {
        let path = ObjectPath::join_request(code.store, code.inviting_writer, code.invite);
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
            state: RequestState::Unsent,
            sealing: Sealing::new(code)?,
            device_name: device_name.into(),
            inviting_writer: code.inviting_writer,
            read_through: code.read_through,
            expires_at: code.expires_at,
        };
        identity.save(&directory.owned_file(StoreFile::Bootstrap))?;
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
        self.state != RequestState::Unsent
    }

    /// Whether this member's keys have opened, in this call or an earlier
    /// one: approval has begun, and the join never reports waiting for it
    /// again (§13.2, E10).
    pub fn opened(&self) -> bool {
        self.state == RequestState::Opened
    }

    /// Record possible publication before issuing it. On restart an absent
    /// request is terminal: re-creating it could resurrect a declined invite.
    pub fn record_attempt(&mut self, file: &AtomicFile) -> Result<(), SyncError> {
        self.record(RequestState::Sent, file)
    }

    /// The provider refused the request, so it was never published: a
    /// retry sends it again (§13.2).
    pub fn record_refusal(&mut self, file: &AtomicFile) -> Result<(), SyncError> {
        self.record(RequestState::Unsent, file)
    }

    /// This member's copies opened its keys. Recorded before the join
    /// reports loading, so a resumed join knows approval has begun before
    /// its first check (§13.2, E10).
    pub(crate) fn record_opened(&mut self, file: &AtomicFile) -> Result<(), SyncError> {
        self.record(RequestState::Opened, file)
    }

    fn record(&mut self, state: RequestState, file: &AtomicFile) -> Result<(), SyncError> {
        self.state = state;
        self.save(file)
    }

    fn save(&self, file: &AtomicFile) -> Result<(), SyncError> {
        let keys = self.identity.keys().to_secret_bytes();
        let mut plain =
            Zeroizing::new(Vec::with_capacity(5 + keys.as_bytes().len() + self.request.len()));
        plain.push(self.state.byte());
        plain.extend_from_slice(&(keys.as_bytes().len() as u32).to_be_bytes());
        plain.extend_from_slice(keys.as_bytes());
        plain.extend_from_slice(&self.request);
        let sealed = self.sealing.key.seal_object_chunk(
            &self.sealing.path,
            &self.sealing.binding,
            0,
            0,
            &plain,
        )?;
        file.replace(&sealed)?;
        Ok(())
    }

    /// Publish the recorded immutable request. An occupied path must hold
    /// exactly these bytes (§19.3). `false`, sending nothing, once the
    /// inviting device is gone for good: an admin's device has ended its
    /// requests, and none would ever be answered (§13.2).
    pub async fn publish(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        if self.inviter_gone(storage).await? {
            return Ok(false);
        }
        match storage.create(&self.path, &self.request).await {
            Ok(_) => Ok(true),
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                let found = storage.read(&self.path).await?;
                if found == self.request {
                    return Ok(true);
                }
                // The invite ended before the request was sent, and its
                // marker holds the path (§13.2).
                if InviteEnd::decode(&found).is_ok() {
                    return Ok(false);
                }
                Err(SyncError::InvitationChanged)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Whether this request is still in storage: an ended invite's marker
    /// in its place, or nothing, means it is gone. A request beside its
    /// inviting device's gone marker reached storage after an admin's
    /// device listed that device's requests, and no device will list them
    /// again, so this device deletes it, and it is gone too (§13.2).
    pub(crate) async fn pending(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        let present = match storage.read(&self.path).await {
            Ok(found) => found == self.request,
            Err(error) if error.failure() == StorageFailure::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if present && self.inviter_gone(storage).await? {
            match storage.delete(&self.path).await {
                Ok(()) => (),
                Err(error) if error.failure() == StorageFailure::NotFound => (),
                Err(error) => return Err(error.into()),
            }
            return Ok(false);
        }
        Ok(present)
    }

    /// Whether the inviting device's gone marker is in storage: it is gone
    /// for good, and an admin's device has ended its requests (§13.2).
    async fn inviter_gone(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        let marker = ObjectPath::device_gone(self.path.store(), self.inviting_writer);
        match storage.read(&marker).await {
            Ok(found) => {
                DeviceGone::decode(&found)?;
                Ok(true)
            }
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether the invite still lets a first request be sent at `now`.
    pub(crate) fn open_at(&self, now: std::time::SystemTime) -> bool {
        now.duration_since(std::time::UNIX_EPOCH)
            .map_or(true, |since| since.as_secs() < self.expires_at)
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
