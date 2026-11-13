use crate::{CloudProvider, ObjectPath, StorageConfig, StorageError, StorageFailure};
use coven_crypto::SecretBytes;
use coven_crypto::SecretText;
use coven_foundation::id_source::DeviceId;
use serde::{Deserialize, Serialize};
use std::ops::Range;

pub(crate) const CLOUDKIT_PART_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const DROPBOX_PART_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const ONEDRIVE_PART_SIZE: usize = 24 * 320 * 1024;
/// The most parts one S3 multipart upload holds.
pub(crate) const S3_MAX_PARTS: usize = 10_000;
/// The largest body one S3 request carries: a whole object's PUT, one part,
/// or the object one `CopyObject` copies.
pub(crate) const S3_REQUEST_LIMIT: u64 = 5 * 1024 * 1024 * 1024;
/// S3 part sizes are whole multiples of this.
const S3_PART_UNIT: u64 = 8 * 1024 * 1024;

/// A provider upload recorded by the caller's operation (§17.9, §11).
/// The destination retains its original absent/revision condition across retries.
///
/// Record it before sending parts and after each call, including failed calls:
/// a publication conflict invalidates the native session even if abort fails.
/// Record it, too, after admitting a part that marks its plan
/// ([`crate::PartPermit::marked`]), before sending that part. After a crash,
/// call `resume_upload` to discover bytes accepted before a reply was lost. The
/// bytes of this value include an upload capability: keep them secret. Dropping
/// a session never aborts it; explicit abort abandons its pending bytes.
/// On copy profiles the recorded token names the temporary object as well as
/// identifying publication, and the session is recorded before the native
/// multipart upload exists: the first part initiates it. Abort deletes it even after native multipart completion; failed
/// copy or cleanup keeps the recording incomplete so it can be retried.
/// OneDrive creation returns its planned temporary UUID before any provider request.
/// After recording it, resume and record the returned native capability before
/// sending parts. The UUID stays through receipt, copying, verification
/// and deletion; native transfer completion alone is not publication.
/// Providers may retain closed session data until their expiry, without publishing it.
/// When recovery requires complete-byte comparison with a published object,
/// resumption resets the confirmed offset to zero. Feed the encrypted
/// bytes through `upload_part` again: they are compared to the published object,
/// without uploading them. Only a complete match confirms publication.
#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "RecordedUploadSession")]
pub struct UploadSession {
    /// The device whose connection began it: its temporaries and provider
    /// session sit in that device's reserved folder (D10).
    pub(crate) device: DeviceId,
    pub(crate) location: StorageConfig,
    pub(crate) path: ObjectPath,
    pub(crate) total: u64,
    pub(crate) confirmed: u64,
    pub(crate) part_size: usize,
    pub(crate) state: SessionState,
}

// The wire boundary must construct an unchecked recording before validation.
// Keeping this private makes both Serde and `decode` return validated sessions.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedUploadSession {
    device: DeviceId,
    location: StorageConfig,
    path: ObjectPath,
    total: u64,
    confirmed: u64,
    part_size: usize,
    state: SessionState,
}

impl TryFrom<RecordedUploadSession> for UploadSession {
    type Error = StorageError;
    fn try_from(recorded: RecordedUploadSession) -> Result<Self, Self::Error> {
        let session = Self {
            device: recorded.device,
            location: recorded.location,
            path: recorded.path,
            total: recorded.total,
            confirmed: recorded.confirmed,
            part_size: recorded.part_size,
            state: recorded.state,
        };
        session.validate()?;
        Ok(session)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum SessionState {
    S3 {
        id: SecretText,
        token: SecretText,
        parts: Vec<S3Part>,
    },
    S3Restart {
        id: SecretText,
        token: SecretText,
    },
    /// A session whose upload name is recorded before the native multipart
    /// upload is initiated, so no upload exists that its recording does not
    /// name (Appendix E.10).
    S3Planned {
        token: SecretText,
        started: PlanStart,
    },
    Dropbox {
        id: SecretText,
    },
    OneDrive {
        temporary: uuid::Uuid,
        upload: OneDriveUpload,
    },
    CloudKit {
        id: SecretText,
    },
    VerifyPublished,
    Complete,
    /// The deterministic provider's session named before its first part
    /// starts it, as an S3 multipart upload is (Appendix E.10).
    #[cfg(any(test, feature = "test-utils"))]
    MemoryPlanned {
        started: PlanStart,
    },
    #[cfg(any(test, feature = "test-utils"))]
    Memory {
        id: u64,
        complete: bool,
    },
    /// The deterministic provider's session whose parts are independent, as
    /// an S3 multipart upload's are: the indexes, from zero, of the parts it
    /// confirmed, in order.
    #[cfg(any(test, feature = "test-utils"))]
    MemoryParts {
        id: u64,
        parts: Vec<u64>,
        complete: bool,
    },
}

/// Whether a planned session's first part may have started its upload
/// (§17.9, Appendix E.10). The recording keeps one fact, whether the plan is
/// marked; what this process knows besides lives in memory only.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "bool", into = "bool")]
pub(crate) enum PlanStart {
    /// Unmarked: no request that may start the upload was sent, so nothing
    /// is at the plan's name.
    Unstarted,
    /// Marked by the connection's admission of the first part, which has not
    /// been sent: this process knows nothing is at the name yet.
    Starting,
    /// Marked, and the first part may have started an upload at the name: it
    /// is going out here, or went out, or the recording was restored.
    MayHaveStarted,
}

impl From<bool> for PlanStart {
    fn from(marked: bool) -> Self {
        if marked {
            Self::MayHaveStarted
        } else {
            Self::Unstarted
        }
    }
}

impl From<PlanStart> for bool {
    fn from(start: PlanStart) -> Self {
        start != PlanStart::Unstarted
    }
}

/// Whether a planned session's start went out: set where the provider hands
/// the start's request to the network, past every check made as it goes,
/// the publication gate's and a provider cooldown's (§17.9, Appendix E.10).
/// The start's attempts share it, so one that went out before a retry was
/// refused still counts.
#[derive(Clone, Default)]
pub(crate) struct StartSent(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl StartSent {
    /// The start's request goes out now.
    pub(crate) fn going(&self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn went(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct S3Part {
    pub(crate) number: i32,
    pub(crate) size: u64,
    pub(crate) etag: String,
}

/// Native transfer and publication are separate; every phase retains the
/// temporary's identity until its deletion has succeeded.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum OneDriveUpload {
    /// The temporary identity exists before any native request (§20.6).
    Planned {
        /// Only this process can know initiation has never been attempted.
        #[serde(skip)]
        unstarted: bool,
    },
    Receiving {
        url: SecretText,
    },
    Uploaded {
        item: String,
    },
    Copying {
        monitor: SecretText,
    },
    // The copy definitively failed; cleanup may already have removed its source.
    Refused,
    VerifyPublished,
    Published,
}

impl S3Part {
    /// Whether this is one of the parts of an object of `total` bytes in
    /// parts of `part_size`: its number names one, its size is that part's,
    /// and the provider tagged it.
    pub(crate) fn fits(&self, total: u64, part_size: usize) -> bool {
        u64::try_from(self.number)
            .ok()
            .and_then(|number| number.checked_sub(1))
            .and_then(|index| part_span(total, part_size, index))
            .is_some_and(|span| span.end - span.start == self.size)
            && !self.etag.is_empty()
    }
}

/// The bytes of part `index`, counted from zero, of an object of `total`
/// bytes in parts of `part_size`; none past its last part.
pub(crate) fn part_span(total: u64, part_size: usize, index: u64) -> Option<Range<u64>> {
    let start = index.checked_mul(part_size as u64)?;
    (start < total).then(|| start..start.saturating_add(part_size as u64).min(total))
}

/// One part a provider whose parts are independent confirmed, for the
/// caller to add to its session ([`UploadSession::confirm`]).
pub struct ConfirmedPart(pub(crate) Confirmation);

impl std::fmt::Debug for ConfirmedPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let index = match &self.0 {
            Confirmation::S3(part) => i64::from(part.number) - 1,
            #[cfg(any(test, feature = "test-utils"))]
            Confirmation::Memory { index } => *index as i64,
        };
        f.debug_struct("ConfirmedPart").field("index", &index).finish()
    }
}

pub(crate) enum Confirmation {
    S3(S3Part),
    /// Part `index`, from zero, of a deterministic provider's session.
    #[cfg(any(test, feature = "test-utils"))]
    Memory {
        index: u64,
    },
}

impl UploadSession {
    /// Mark a planned session's plan as its first part is admitted, before
    /// the part is sent: whether that changed the recording, which the caller
    /// then saves before sending (§17.9, Appendix E.10).
    pub(crate) fn mark_start(&mut self) -> bool {
        let started = match &mut self.state {
            SessionState::S3Planned { started, .. } => started,
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::MemoryPlanned { started } => started,
            _ => return false,
        };
        if *started != PlanStart::Unstarted {
            return false;
        }
        *started = PlanStart::Starting;
        true
    }
    /// Settle a planned session's plan after its start failed. One whose
    /// request went out may have started an upload, and stays marked; one
    /// refused before it went out began nothing, so its plan is unmarked
    /// again, for the caller to save before the failure is reported
    /// (Appendix E.10).
    pub(crate) fn start_failed(&mut self, start: &StartSent) {
        let started = match &mut self.state {
            SessionState::S3Planned { started, .. } => started,
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::MemoryPlanned { started } => started,
            _ => return,
        };
        if !start.went() {
            *started = PlanStart::Unstarted;
        }
    }
    /// Whether native initiation must follow recording this identity, before
    /// any part is sent. Resume it and save the returned session first (§20.6).
    pub fn needs_initiation(&self) -> bool {
        matches!(self.state, SessionState::OneDrive { upload: OneDriveUpload::Planned { .. }, .. })
    }
    /// The device whose connection began this upload.
    pub fn device(&self) -> DeviceId {
        self.device
    }
    /// The destination encrypted-object path.
    pub fn path(&self) -> &ObjectPath {
        &self.path
    }
    /// Bytes confirmed by the provider; only these may advance the operation.
    pub fn confirmed_bytes(&self) -> u64 {
        self.confirmed
    }
    /// Total encrypted bytes promised when the session began.
    pub fn total_bytes(&self) -> u64 {
        self.total
    }
    /// Maximum part size. After resuming a partial part, send only the bytes
    /// remaining to the next part boundary (or the end of the object).
    pub fn part_size(&self) -> usize {
        self.part_size
    }
    /// Whether publication and provider upload cleanup have succeeded.
    pub fn is_complete(&self) -> bool {
        match self.state {
            SessionState::Complete => true,
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::Memory { complete, .. } | SessionState::MemoryParts { complete, .. } => {
                complete
            }
            _ => false,
        }
    }
    /// Whether this session's parts are independent: the provider keeps
    /// each by its index, whatever order they arrive in and however often
    /// one is sent, so they may go together (§17.9, Appendix E.10). Only an
    /// S3 multipart upload's are, once its first part has started it.
    pub fn parts_independent(&self) -> bool {
        match self.state {
            SessionState::S3 { .. } => true,
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::MemoryParts { complete, .. } => !complete,
            _ => false,
        }
    }
    /// The bytes of each part still to send, in order: where parts are
    /// independent, every part the provider hasn't confirmed; elsewhere only
    /// the next, which goes before any after it. After resuming a partial
    /// part, the next runs only to its part's end.
    pub fn parts_to_send(&self) -> Vec<Range<u64>> {
        if self.is_complete() || self.confirmed >= self.total {
            return Vec::new();
        }
        if let Some(confirmed) = self.confirmed_parts() {
            return (0..self.total.div_ceil(self.part_size as u64))
                .filter(|index| confirmed.binary_search(index).is_err())
                .filter_map(|index| part_span(self.total, self.part_size, index))
                .collect();
        }
        let next = (self.confirmed / self.part_size as u64 + 1) * self.part_size as u64;
        std::iter::once(self.confirmed..next.min(self.total)).collect()
    }
    /// The indexes, from zero, of an independent session's confirmed parts,
    /// in order.
    fn confirmed_parts(&self) -> Option<Vec<u64>> {
        match &self.state {
            SessionState::S3 { parts, .. } => Some(
                parts
                    .iter()
                    .filter_map(|part| u64::try_from(part.number).ok()?.checked_sub(1))
                    .collect(),
            ),
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::MemoryParts { parts, complete: false, .. } => Some(parts.clone()),
            _ => None,
        }
    }
    /// Add a part the provider confirmed to an independent session. A part
    /// that isn't one of the session's, or one it holds already, is refused
    /// and changes nothing.
    pub fn confirm(&mut self, part: ConfirmedPart) -> Result<(), StorageError> {
        let (total, part_size) = (self.total, self.part_size);
        let size = match (&mut self.state, part.0) {
            (SessionState::S3 { parts, .. }, Confirmation::S3(part)) => {
                if !part.fits(total, part_size) {
                    return Err(StorageFailure::Protocol
                        .with_source("S3 confirmed a part the session doesn't have"));
                }
                let at = parts
                    .binary_search_by_key(&part.number, |held| held.number)
                    .err()
                    .ok_or(StorageFailure::InvalidPart)?;
                let size = part.size;
                parts.insert(at, part);
                size
            }
            #[cfg(any(test, feature = "test-utils"))]
            (
                SessionState::MemoryParts { parts, complete: false, .. },
                Confirmation::Memory { index },
            ) => {
                let span = part_span(total, part_size, index).ok_or(StorageFailure::Protocol)?;
                let at = parts.binary_search(&index).err().ok_or(StorageFailure::InvalidPart)?;
                parts.insert(at, index);
                span.end - span.start
            }
            _ => return Err(StorageFailure::SessionMismatch.into()),
        };
        self.confirmed += size;
        Ok(())
    }
    /// Encode the complete recorded state without printing upload credentials.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }
    /// Read a recording only if its provider, destination, state and progress agree.
    /// Validation covers the location, state variant, nonempty identifiers, part
    /// counts and sizes, and confirmed byte offset.
    /// The adapter also checks that it names the adapter's location before any request.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let value: RecordedUploadSession = crate::secret_json::decode(bytes)?;
        value.try_into()
    }
    fn validate(&self) -> Result<(), StorageError> {
        self.location.validate()?;
        // The floor pointer is one small record replaced in one request; no
        // session targets it.
        if self.path.is_replaceable() {
            return Err(StorageFailure::InvalidPath.into());
        }
        if self.part_size == 0
            || self.total == 0
            || self.confirmed > self.total
            || (self.is_complete() && self.confirmed != self.total)
        {
            return Err(StorageFailure::InvalidPart.into());
        }
        let provider = self.location.provider();
        match &self.state {
            SessionState::S3 { id, token, parts } if provider == CloudProvider::S3 => {
                nonempty(id)?;
                nonempty(token)?;
                if parts.len() > S3_MAX_PARTS {
                    return Err(StorageFailure::InvalidPart.into());
                }
                // Each confirmed part once, in number order, whichever
                // arrived first.
                let mut confirmed = 0u64;
                let mut previous = 0;
                for part in parts {
                    if part.number <= previous || !part.fits(self.total, self.part_size) {
                        return Err(StorageFailure::InvalidPart.into());
                    }
                    previous = part.number;
                    confirmed += part.size;
                }
                if confirmed != self.confirmed {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::S3Restart { id, token } if provider == CloudProvider::S3 => {
                nonempty(id)?;
                nonempty(token)?;
                if self.confirmed != 0 {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::S3Planned { token, .. } if provider == CloudProvider::S3 => {
                nonempty(token)?;
                if self.confirmed != 0 {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::Dropbox { id } if provider == CloudProvider::Dropbox => nonempty(id)?,
            SessionState::OneDrive { upload, .. } if provider == CloudProvider::OneDrive => {
                match upload {
                    OneDriveUpload::Planned { .. } => {
                        if self.confirmed != 0 {
                            return Err(StorageFailure::InvalidPart.into());
                        }
                    }
                    OneDriveUpload::Receiving { url } => {
                        transfer_url(url)?;
                        if self.confirmed == self.total {
                            return Err(StorageFailure::InvalidPart.into());
                        }
                    }
                    OneDriveUpload::Uploaded { item } => {
                        if item.is_empty() || self.confirmed != self.total {
                            return Err(StorageFailure::InvalidPart.into());
                        }
                    }
                    OneDriveUpload::Copying { monitor } => {
                        transfer_url(monitor)?;
                        if self.confirmed != self.total {
                            return Err(StorageFailure::InvalidPart.into());
                        }
                    }
                    OneDriveUpload::Published | OneDriveUpload::Refused
                        if self.confirmed != self.total =>
                    {
                        return Err(StorageFailure::InvalidPart.into());
                    }
                    OneDriveUpload::VerifyPublished if self.confirmed == self.total => {
                        return Err(StorageFailure::InvalidPart.into());
                    }
                    OneDriveUpload::VerifyPublished
                    | OneDriveUpload::Published
                    | OneDriveUpload::Refused => {}
                }
            }
            SessionState::CloudKit { id } if provider == CloudProvider::CloudKit => {
                check_cloudkit_id(id.as_str())?
            }
            SessionState::VerifyPublished if provider == CloudProvider::Dropbox => {
                if self.confirmed == self.total {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::Complete => {}
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::MemoryPlanned { .. } => {
                return if self.confirmed == 0 {
                    Ok(())
                } else {
                    Err(StorageFailure::InvalidPart.into())
                };
            }
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::Memory { id, .. } => {
                return if *id != 0 { Ok(()) } else { Err(StorageFailure::InvalidPart.into()) };
            }
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::MemoryParts { id, parts, complete } => {
                let mut confirmed = 0u64;
                for (at, index) in parts.iter().enumerate() {
                    let span = part_span(self.total, self.part_size, *index)
                        .ok_or(StorageFailure::InvalidPart)?;
                    if at > 0 && parts[at - 1] >= *index {
                        return Err(StorageFailure::InvalidPart.into());
                    }
                    confirmed += span.end - span.start;
                }
                let held = if *complete { self.total } else { confirmed };
                return if *id != 0 && held == self.confirmed {
                    Ok(())
                } else {
                    Err(StorageFailure::InvalidPart.into())
                };
            }
            _ => return Err(StorageFailure::SessionMismatch.into()),
        }
        let valid_size = match provider {
            CloudProvider::S3 => self.part_size == s3_part_size(self.total)?,
            CloudProvider::Dropbox => self.part_size == DROPBOX_PART_SIZE,
            CloudProvider::OneDrive => self.part_size == ONEDRIVE_PART_SIZE,
            CloudProvider::CloudKit => self.part_size == CLOUDKIT_PART_SIZE,
        };
        if !valid_size {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(())
    }
    pub(crate) fn check(&self, location: &StorageConfig) -> Result<(), StorageError> {
        self.validate()?;
        if !self.location.same_location(location) {
            return Err(StorageFailure::SessionMismatch.into());
        }
        Ok(())
    }
    /// Where a part of `len` bytes starting `offset` bytes in ends, if it is
    /// one this session may send now: where parts are independent, a whole
    /// part the provider hasn't confirmed; elsewhere the next, from the
    /// confirmed bytes to at most its part's end.
    pub(crate) fn part_end(&self, offset: u64, len: usize) -> Result<u64, StorageError> {
        let end = offset.checked_add(len as u64).ok_or(StorageFailure::InvalidPart)?;
        if self.is_complete() || len == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        if let Some(confirmed) = self.confirmed_parts() {
            let index = offset / self.part_size as u64;
            let whole = offset.is_multiple_of(self.part_size as u64)
                && part_span(self.total, self.part_size, index).is_some_and(|span| span.end == end);
            if !whole || confirmed.binary_search(&index).is_ok() {
                return Err(StorageFailure::InvalidPart.into());
            }
            return Ok(end);
        }
        if offset != self.confirmed
            || len > self.part_size
            || end > self.total
            || (end != self.total && end % self.part_size as u64 != 0)
        {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(end)
    }
    /// The bytes `len` bytes sent as the session's next part cover, if they
    /// fit it: the first part still to send.
    pub(crate) fn next_part(&self, len: usize) -> Result<Range<u64>, StorageError> {
        let offset = self
            .parts_to_send()
            .first()
            .map(|part| part.start)
            .ok_or(StorageFailure::InvalidPart)?;
        Ok(offset..self.part_end(offset, len)?)
    }
}

pub(crate) fn s3_part_size(total: u64) -> Result<usize, StorageError> {
    let parts = S3_MAX_PARTS as u64;
    if total == 0 || total > parts * S3_REQUEST_LIMIT {
        return Err(StorageFailure::InvalidPart.into());
    }
    usize::try_from(total.div_ceil(parts).div_ceil(S3_PART_UNIT) * S3_PART_UNIT)
        .map_err(|error| StorageFailure::InvalidPart.with_source(error))
}
pub(crate) fn check_cloudkit_id(value: &str) -> Result<(), StorageError> {
    coven_foundation::id_source::canonical_uuid(value)
        .map_err(|error| StorageFailure::SessionMismatch.with_source(error))?;
    Ok(())
}
fn nonempty(id: &SecretText) -> Result<(), StorageError> {
    if id.as_str().is_empty() {
        return Err(StorageFailure::SessionMismatch.into());
    }
    Ok(())
}
fn transfer_url(value: &SecretText) -> Result<(), StorageError> {
    let url = url::Url::parse(value.as_str())
        .map_err(|error| StorageFailure::SessionMismatch.with_source(error))?;
    if !crate::web_url::is_web_url(&url) || url.fragment().is_some() {
        return Err(StorageFailure::SessionMismatch.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
