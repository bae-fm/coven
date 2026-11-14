//! Fixed joining keys and request bytes, sealed locally using the scanned invite.

use crate::request_at::{Placed, RequestAt};
use crate::{Joiner, SyncError};
use coven_crypto::{
    CircleKey, ConfirmationNumber, EncryptionKey, InviteSecret, MemberIdentity, MemberKeys,
    ObjectHasher,
};
use coven_format::invite_end::InviteEnd;
use coven_format::{
    admin::NewCircle,
    codes::InviteCode,
    objects::{JoinRequest, JoiningCircle},
    sealed_key::{CopyBox, KeyCopyPrefix},
    sealed_single::{SingleChunkObject, SingleChunkPrefix},
    sealed_slot::SlotObjectPrefix,
    Object,
};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{AtomicFile, StoreDir, StoreFile};
use coven_foundation::id_source::{CircleId, DeviceId, IdSource, KeyId};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use zeroize::Zeroizing;

/// The request as it is stored at `path`: sealed with the invite secret's
/// request key and signed by the pair it names (§13.2).
pub fn sealed_join_request(
    secret: &InviteSecret,
    path: &ObjectPath,
    request: &JoinRequest,
    signer: &MemberKeys,
) -> Result<Vec<u8>, SyncError> {
    let prefix = SingleChunkPrefix::JoinRequest;
    let plain = Object::JoinRequest(request.clone()).encode()?;
    let chunk = secret.join_request_key().seal_object_chunk(
        path.as_str(),
        &prefix.encode()?,
        0,
        0,
        &plain,
    )?;
    let mut hash = ObjectHasher::new();
    hash.update(&prefix.encode_chunk(&chunk)?);
    Ok(SingleChunkObject::JoinRequest {
        chunk: &chunk,
        signature: signer.sign_object(path.as_str(), &hash.finish()),
    }
    .encode()?)
}

/// The request stored at `path` as `sealed_join_request` sealed it, opened
/// with the invite secret's request key and checked as signed by the pair
/// it names, before the caller compares what it says; with its plaintext.
/// `None` when the chunk holds another kind of object (§13.2).
pub(crate) fn opened_join_request(
    secret: &InviteSecret,
    path: &ObjectPath,
    bytes: &[u8],
) -> Result<Option<(JoinRequest, Vec<u8>)>, SyncError> {
    let envelope = SingleChunkObject::decode(bytes)?;
    let SingleChunkObject::JoinRequest { chunk, signature } = &envelope;
    let plain = secret.join_request_key().open_object_chunk(
        path.as_str(),
        &envelope.prefix().encode()?,
        0,
        0,
        chunk,
    )?;
    let Object::JoinRequest(request) = Object::decode(&plain)? else {
        return Ok(None);
    };
    let mut hash = ObjectHasher::new();
    hash.update(&envelope.signed_bytes()?);
    request.keys.signing.verify_object(path.as_str(), &hash.finish(), signature)?;
    Ok(Some((request, plain)))
}

/// What the app says of the circle its joining person's request makes: the
/// admission will make it, with the person as its only member (§13.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CircleTerms {
    /// The circle's name, which the person's profile will find it by.
    pub name: String,
    /// The version of the app's newest breaking migration, zero if none:
    /// the circle's base (§15.5).
    pub breaking_version: u32,
}

/// An unpublished member identity and its immutable request. The member id
/// is the one the invite fixes; the keys are this device's fresh pair. The
/// only durable local copy is encrypted with the invite secret, bound to the
/// complete code. No member keys or provider credentials enter final custody
/// while waiting. One join holds it, from preparing or resuming the request
/// until the join ends.
pub struct JoiningIdentity {
    /// The store's bootstrap file, which keeps the sealed request and how far
    /// the join has come.
    file: AtomicFile,
    identity: MemberIdentity,
    request: Vec<u8>,
    path: ObjectPath,
    state: JoinStage,
    /// What seals the saved request for the code it was made for.
    sealing: Sealing,
    /// The name the device gave itself in its request.
    device_name: String,
    /// Who the request is for, and the new person's name it carries.
    joiner: Joiner,
    /// The circle the request makes, whose key's only copy is its own box.
    circle: JoiningCircle,
    inviting_writer: DeviceId,
    read_through: u64,
    /// When the invite ends, in seconds since the Unix epoch: no first
    /// request is sent after it.
    expires_at: u64,
    /// The number the admin compares before approving.
    confirmation: ConfirmationNumber,
}

/// How far the join has come, as its saved request records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinStage {
    /// Not sent, and the invite's share not accepted: a cancel has nothing
    /// to take back.
    Unsent,
    /// Not sent, and the provider's acceptance of the invite's share may be
    /// in the joining account: a retry sends it, and a cancel takes the
    /// acceptance back first (§13.2).
    Accepted,
    /// It may have been sent: on restart an absent request is final.
    Sent,
    /// Its copies opened this member's keys, in the admission that carries
    /// them: the approving device's admission is in history (§13.2).
    Opened,
    /// The join decided to end without a store, and is deleting what it
    /// kept: whatever call finds it finishes the deletion, without
    /// connecting (§13.4).
    Ended,
}

impl JoinStage {
    fn byte(self) -> u8 {
        match self {
            Self::Unsent => 0,
            Self::Accepted => 1,
            Self::Sent => 2,
            Self::Opened => 3,
            Self::Ended => 4,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Unsent),
            1 => Some(Self::Accepted),
            2 => Some(Self::Sent),
            3 => Some(Self::Opened),
            4 => Some(Self::Ended),
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
    /// A new request names `device`, the joining installation's id, for the
    /// approval to register, `name`, the new person's name: given when
    /// the invite admits a new person, absent when it admits a member's new
    /// keys, as the code says, and the circle `circle` describes, with ids
    /// from `ids`. A changed invite, device name, name or circle name cannot
    /// silently reuse another request.
    pub fn prepare(
        directory: &StoreDir,
        code: &InviteCode,
        device: DeviceId,
        device_name: &str,
        name: Option<String>,
        circle: &CircleTerms,
        ids: &dyn IdSource,
    ) -> Result<Self, SyncError> {
        let joiner = match name {
            Some(name) => Joiner::NewPerson { name },
            None => Joiner::MemberKeys,
        };
        if joiner.admits() != code.admits {
            return Err(crate::CodeError::WrongJoiner {
                expected: code.admits,
                actual: joiner.admits(),
            }
            .into());
        }
        match Self::saved(directory, code)? {
            Some(saved)
                if saved.device_name != device_name
                    || saved.joiner != joiner
                    || saved.circle.circle.name != circle.name =>
            {
                Err(SyncError::InvitationChanged)
            }
            Some(saved) => Ok(saved),
            None => Self::create(directory, code, device, device_name, joiner, circle, ids),
        }
    }

    /// `prepare` with a circle named "Circle" at base zero and random ids, for
    /// a test whose join doesn't care about its circle.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn prepare_for_test(
        directory: &StoreDir,
        code: &InviteCode,
        device: DeviceId,
        device_name: &str,
        name: Option<String>,
    ) -> Result<Self, SyncError> {
        let circle = CircleTerms { name: "Circle".into(), breaking_version: 0 };
        Self::prepare(
            directory,
            code,
            device,
            device_name,
            name,
            &circle,
            &coven_foundation::id_source::UuidIds,
        )
    }

    /// Continue a join from its saved request, with the device name and
    /// person that request carries (§13.2). A join's code is saved only after its
    /// request, so a missing request is damaged local state.
    pub fn resume(directory: &StoreDir, code: &InviteCode) -> Result<Self, SyncError> {
        Self::saved(directory, code)?.ok_or_else(|| coven_database::DbError::DamagedDatabase.into())
    }

    /// The request this directory keeps for `code`, opened with the code's
    /// secret and checked against it.
    #[expect(
        clippy::map_err_ignore,
        reason = "a request for the member's own device is no invite's, so the invitation changed"
    )]
    fn saved(directory: &StoreDir, code: &InviteCode) -> Result<Option<Self>, SyncError> {
        crate::CodeError::require_store(directory.id(), code.store)?;
        let file = directory.owned_file(StoreFile::Bootstrap);
        let sealing = Sealing::new(code)?;
        let path = code.request_path();
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
            .and_then(JoinStage::from_byte)
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
        let (opened, _) = opened_join_request(&code.secret, &path, &request)?
            .ok_or(coven_database::DbError::DamagedDatabase)?;
        if opened.invite != code.invite || opened.member != code.member || opened.keys != keys {
            return Err(SyncError::InvitationChanged);
        }
        // A request for the member's own device is no invite's, so the
        // invitation changed; an invite's always names its circle.
        let circle = opened.joiner.circle().cloned().ok_or(SyncError::InvitationChanged)?;
        Ok(Some(Self {
            file,
            confirmation: ConfirmationNumber::of(code.invite, &keys),
            identity,
            request,
            path,
            state,
            sealing,
            device_name: opened.device_name,
            joiner: opened.joiner.try_into().map_err(|_| SyncError::InvitationChanged)?,
            circle,
            inviting_writer: code.inviting_writer,
            read_through: code.read_through,
            expires_at: code.expires_at,
        }))
    }

    fn create(
        directory: &StoreDir,
        code: &InviteCode,
        device: DeviceId,
        device_name: &str,
        joiner: Joiner,
        terms: &CircleTerms,
        ids: &dyn IdSource,
    ) -> Result<Self, SyncError> {
        let path = code.request_path();
        // The request repeats the id the invite fixed and names this
        // device's fresh pair, which signs it, and this installation (D8).
        let identity = MemberIdentity::generate(code.member)?;
        let circle = joining_circle(
            code.store,
            code.member,
            &identity.keys().public_keys().sealing,
            terms,
            ids,
        )?;
        let request = sealed_join_request(
            &code.secret,
            &path,
            &JoinRequest {
                invite: code.invite,
                member: code.member,
                keys: identity.keys().public_keys(),
                device,
                device_name: device_name.into(),
                joiner: joiner.clone().requesting(circle.clone()),
            },
            identity.keys(),
        )?;
        let identity = Self {
            file: directory.owned_file(StoreFile::Bootstrap),
            confirmation: ConfirmationNumber::of(code.invite, &identity.keys().public_keys()),
            identity,
            request,
            path,
            state: JoinStage::Unsent,
            sealing: Sealing::new(code)?,
            device_name: device_name.into(),
            joiner,
            circle,
            inviting_writer: code.inviting_writer,
            read_through: code.read_through,
            expires_at: code.expires_at,
        };
        identity.save()?;
        Ok(identity)
    }

    /// The invite's member id with this device's pair, for composing the
    /// unpublished member custody.
    pub fn identity(&self) -> MemberIdentity {
        self.identity.clone()
    }

    /// The inviting device: only its admission admits this request (§13.2).
    pub fn approver(&self) -> DeviceId {
        self.inviting_writer
    }

    /// The inviting device's applied slot when it made the invite: copies for
    /// this request can only follow it (§13.2).
    pub fn read_through(&self) -> u64 {
        self.read_through
    }

    /// The name the device gave itself in its request, which its admission
    /// registers it with.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// The name of the circle the request makes.
    pub fn circle_name(&self) -> &str {
        &self.circle.circle.name
    }

    /// The circle the request makes, with the key's copy it carries.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn circle(&self) -> &JoiningCircle {
        &self.circle
    }

    /// Who the request is for. A new person's name is kept here until the
    /// join publishes the store (§13.2).
    pub fn joiner(&self) -> &Joiner {
        &self.joiner
    }

    /// The number the admin compares before approving; the admin's device
    /// computes it from the same keys (format.md D11).
    pub fn confirmation(&self) -> ConfirmationNumber {
        self.confirmation
    }

    /// The request's path, named by its invite.
    pub fn path(&self) -> &ObjectPath {
        &self.path
    }

    /// Whether publication may already have happened before an interruption.
    pub fn attempted(&self) -> bool {
        matches!(self.state, JoinStage::Sent | JoinStage::Opened)
    }

    /// Whether the invite's share may have been accepted for the joining
    /// account while the request is unsent: a cancel takes it back before
    /// it removes the join (§13.2).
    pub fn accepted(&self) -> bool {
        self.state == JoinStage::Accepted
    }

    /// Whether the join already decided to end, and only its deletion is
    /// left (§13.4).
    pub fn ended(&self) -> bool {
        self.state == JoinStage::Ended
    }

    /// Whether this member's keys have opened, in this call or an earlier
    /// one: the admission carrying them is in history, and the join never
    /// reports waiting for approval again (§13.2, E10).
    pub fn opened(&self) -> bool {
        self.state == JoinStage::Opened
    }

    /// Record, before accepting the invite's share, that the acceptance may
    /// be in the joining account, so a cancel before sending, in this call
    /// or after a restart, takes it back (§13.2). A join already accepted,
    /// or one that may have sent, is past this.
    pub fn record_acceptance(&mut self) -> Result<(), SyncError> {
        match self.state {
            JoinStage::Unsent => self.record(JoinStage::Accepted),
            JoinStage::Accepted | JoinStage::Sent | JoinStage::Opened | JoinStage::Ended => Ok(()),
        }
    }

    /// Record possible publication before issuing it. On restart a request
    /// that is absent with nothing in its place is sent again; an ended
    /// invite's marker in its place ends the join (`resend`).
    pub fn record_attempt(&mut self) -> Result<(), SyncError> {
        self.record(JoinStage::Sent)
    }

    /// The provider refused the request, so it was never published: a
    /// retry sends it again, and a cancel takes back the acceptance that
    /// came before it (§13.2).
    pub fn record_refusal(&mut self) -> Result<(), SyncError> {
        self.record(JoinStage::Accepted)
    }

    /// This member's copies opened its keys, in its admission. Recorded
    /// before the join reports loading, so a resumed join knows it was
    /// approved before its first check (§13.2, E10).
    pub(crate) fn record_opened(&mut self) -> Result<(), SyncError> {
        self.record(JoinStage::Opened)
    }

    fn record(&mut self, state: JoinStage) -> Result<(), SyncError> {
        self.state = state;
        self.save()
    }

    fn save(&self) -> Result<(), SyncError> {
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
        self.file.replace(&sealed)?;
        Ok(())
    }

    /// The request and where it goes, for the storage work it shares with a
    /// pairing's.
    fn at(&self) -> RequestAt<'_> {
        RequestAt { path: &self.path, request: &self.request, showing: self.inviting_writer }
    }

    /// Publish the recorded immutable request. An occupied path must hold
    /// exactly these bytes (§19.3). `false`, sending nothing, once the
    /// inviting device is gone for good: an admin's device has ended its
    /// requests, and none would ever be answered (§13.2).
    pub async fn publish(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        let at = self.at();
        if at.showing_gone(storage).await? {
            return Ok(false);
        }
        match at.place(storage).await? {
            Placed::Ours => Ok(true),
            // The invite ended before the request was sent, and its marker
            // holds the path (§13.2).
            Placed::Other(found) if InviteEnd::decode(&found).is_ok() => Ok(false),
            Placed::Other(_) => Err(SyncError::InvitationChanged),
        }
    }

    /// Send the request again when nothing is in its place: the call that
    /// recorded the send stopped before the create reached storage. Any end
    /// of the invite leaves its marker at the path, which the create finds
    /// taken, so a declined or approved invite is never revived; a request
    /// that is there already is left alone. What the join concludes is for
    /// the check that follows, not for this send (§13.2).
    pub(crate) async fn resend(&self, storage: &dyn Storage) -> Result<(), SyncError> {
        match storage.read(&self.path).await {
            Ok(_) => Ok(()),
            Err(error) if error.failure() == StorageFailure::NotFound => {
                self.publish(storage).await.map(drop)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Whether this request is still in storage: an ended invite's marker
    /// in its place, or nothing, means it is gone. A request beside its
    /// inviting device's gone marker is deleted, and is gone too
    /// (`RequestAt::pending`).
    pub(crate) async fn pending(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        self.at().pending(storage).await
    }

    /// Whether the invite still lets a first request be sent at `now`.
    pub(crate) fn open_at(&self, now: std::time::SystemTime) -> bool {
        now.duration_since(std::time::UNIX_EPOCH)
            .map_or(true, |since| since.as_secs() < self.expires_at)
    }
}

/// The circle `member`'s join request makes in `store`: its id, key and the
/// key's only copy, sealed to the member's own new `sealing` key before the
/// key is dropped (§13.2, D11). The saved request holds the box and nothing
/// else holds the key.
pub(crate) fn joining_circle(
    store: coven_foundation::id_source::StoreId,
    member: coven_foundation::id_source::MemberId,
    sealing: &coven_crypto::SealingKey,
    terms: &CircleTerms,
    ids: &dyn IdSource,
) -> Result<JoiningCircle, SyncError> {
    let circle = CircleId(ids.new_id());
    let key_id = KeyId(ids.new_id());
    let key = CircleKey::generate(circle, key_id)?;
    let routing = KeyCopyPrefix {
        audience: coven_merge::Audience::Circle(circle),
        key: key_id,
        member,
        sealing: *sealing,
    };
    let binding = SlotObjectPrefix::own_binding(store, &routing)?;
    let sealed = coven_crypto::seal_own_circle_key(&key, &routing.sealing, &binding)?;
    Ok(JoiningCircle {
        circle: NewCircle {
            circle,
            name: terms.name.clone(),
            breaking_version: terms.breaking_version,
            key: key_id,
            key_hash: key.hash(),
        },
        copy: CopyBox { routing, sealed },
    })
}

/// The circle a join request of `member` with the sealing key `sealing` would
/// make in `store`, named "Circle" at base zero, for a test that seals a
/// request itself.
#[cfg(any(test, feature = "test-utils"))]
pub fn test_joining_circle(
    store: coven_foundation::id_source::StoreId,
    member: coven_foundation::id_source::MemberId,
    sealing: &coven_crypto::SealingKey,
    ids: &dyn IdSource,
) -> Result<JoiningCircle, SyncError> {
    joining_circle(
        store,
        member,
        sealing,
        &CircleTerms { name: "Circle".into(), breaking_version: 0 },
        ids,
    )
}

/// The admission of `member` with `keys` that a join request would make, for
/// a test that admits a member without a join: the circle it makes, named
/// `name`, and the member's own copy of its key.
#[cfg(any(test, feature = "test-utils"))]
#[expect(clippy::too_many_arguments, reason = "an admission's every field")]
pub fn admitting(
    store: coven_foundation::id_source::StoreId,
    member: coven_foundation::id_source::MemberId,
    keys: coven_crypto::MemberPublicKeys,
    role: coven_format::admin::MemberRole,
    access: coven_format::MemberAccess,
    device: DeviceId,
    device_name: &str,
    ids: &dyn IdSource,
) -> Result<(coven_format::admin::StoreChange, CopyBox), SyncError> {
    let terms = CircleTerms { name: format!("circle of {member}"), breaking_version: 0 };
    let JoiningCircle { circle, copy } = joining_circle(store, member, &keys.sealing, &terms, ids)?;
    Ok((
        coven_format::admin::StoreChange::AdmitMember {
            member,
            keys,
            role,
            access,
            device,
            device_name: device_name.into(),
            circle,
        },
        copy,
    ))
}

/// Record that the join with `code` ends without a store, before anything it
/// kept is deleted: a call that stops part way through the deletion finds the
/// record and finishes it without connecting. Nothing saved is nothing to
/// record.
pub fn record_ending(directory: &StoreDir, code: &InviteCode) -> Result<(), SyncError> {
    match JoiningIdentity::saved(directory, code)? {
        Some(mut saved) => saved.record(JoinStage::Ended),
        None => Ok(()),
    }
}

/// Whether the request kept for `code` was recorded as ended
/// (`record_ending`).
pub fn ended_for(directory: &StoreDir, code: &InviteCode) -> Result<bool, SyncError> {
    Ok(JoiningIdentity::saved(directory, code)?.is_some_and(|saved| saved.ended()))
}

fn local_path(code: &InviteCode) -> String {
    format!("coven/local-join/{}/{}", code.store, code.invite)
}
