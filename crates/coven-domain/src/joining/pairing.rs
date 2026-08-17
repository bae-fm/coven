use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use coven_foundation::code_envelope;
use coven_foundation::config::CloudProvider;
use coven_keys::keys::{self, UserKeypair};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;

const PAIRING_CODE_PREFIX: &str = "coven:device-pairing:";
const PAIRING_REQUEST_DOMAIN: &[u8] = b"coven.device-pairing-request.v1\0";
const PAIRING_VERSION: u32 = 1;

/// The one code an existing device displays. Possession of this code grants
/// access only to this pairing session; Store credentials remain sealed to the
/// joining identity the existing device approves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevicePairingOffer {
    version: u32,
    pairing_public_key: String,
    endpoints: Vec<SocketAddr>,
    store_name: String,
    cloud_provider: CloudProvider,
    expires_at_unix_seconds: i64,
}

impl DevicePairingOffer {
    pub fn new(
        pairing_key: &UserKeypair,
        endpoints: Vec<SocketAddr>,
        store_name: String,
        cloud_provider: CloudProvider,
        expires_at_unix_seconds: i64,
    ) -> Result<Self, DevicePairingError> {
        if endpoints.is_empty() {
            return Err(DevicePairingError::NoEndpoint);
        }
        if store_name.trim().is_empty() {
            return Err(DevicePairingError::EmptyStoreName);
        }
        Ok(Self {
            version: PAIRING_VERSION,
            pairing_public_key: keys::public_key_hex(pairing_key),
            endpoints,
            store_name,
            cloud_provider,
            expires_at_unix_seconds,
        })
    }

    pub fn encode(&self) -> String {
        code_envelope::encode_code(PAIRING_CODE_PREFIX, self)
    }

    pub fn decode(code: &str) -> Result<Self, DevicePairingError> {
        let offer: Self = code_envelope::decode_code(PAIRING_CODE_PREFIX, code)?;
        offer.validate()?;
        Ok(offer)
    }

    /// Whether `code` carries the device-pairing envelope. This identifies
    /// which decoder owns a scanned code without accepting or validating its
    /// payload.
    pub fn is_pairing_code(code: &str) -> bool {
        code.trim().starts_with(PAIRING_CODE_PREFIX)
    }

    pub fn session_id(&self) -> &str {
        &self.pairing_public_key
    }

    pub fn endpoints(&self) -> &[SocketAddr] {
        &self.endpoints
    }

    pub(crate) fn pairing_public_key(&self) -> &str {
        &self.pairing_public_key
    }

    pub fn store_name(&self) -> &str {
        &self.store_name
    }

    pub fn cloud_provider(&self) -> &CloudProvider {
        &self.cloud_provider
    }

    pub fn expires_at_unix_seconds(&self) -> i64 {
        self.expires_at_unix_seconds
    }

    fn validate(&self) -> Result<(), DevicePairingError> {
        if self.version != PAIRING_VERSION {
            return Err(DevicePairingError::UnsupportedVersion(self.version));
        }
        decode_32("pairing public key", &self.pairing_public_key)?;
        if self.endpoints.is_empty() {
            return Err(DevicePairingError::NoEndpoint);
        }
        if self.store_name.trim().is_empty() {
            return Err(DevicePairingError::EmptyStoreName);
        }
        Ok(())
    }

    fn digest(&self) -> [u8; 32] {
        Sha256::digest(
            serde_json::to_vec(self).expect("device pairing offer serialization cannot fail"),
        )
        .into()
    }

    fn recipient(&self) -> Result<[u8; 32], DevicePairingError> {
        keys::ed25519_hex_to_x25519_public_key(&self.pairing_public_key)
            .map_err(DevicePairingError::Key)
    }
}

/// The identity submitted after scanning an owner's offer. The whole signed
/// request is sealed to that offer's ephemeral key before it crosses the LAN.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevicePairingRequest {
    version: u32,
    offer_hash: String,
    public_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_account_email: Option<String>,
    signature: String,
}

impl DevicePairingRequest {
    pub fn signed(
        offer: &DevicePairingOffer,
        identity: &UserKeypair,
        provider_account_email: Option<String>,
    ) -> Self {
        let mut request = Self {
            version: PAIRING_VERSION,
            offer_hash: hex::encode(offer.digest()),
            public_key: keys::public_key_hex(identity),
            provider_account_email,
            signature: String::new(),
        };
        request.signature = hex::encode(identity.sign(&request.signing_bytes()));
        request
    }

    pub fn public_key(&self) -> &str {
        &self.public_key
    }

    pub fn provider_account_email(&self) -> Option<&str> {
        self.provider_account_email.as_deref()
    }

    pub fn seal(&self, offer: &DevicePairingOffer) -> Result<Vec<u8>, DevicePairingError> {
        self.verify(offer)?;
        let plaintext =
            serde_json::to_vec(self).expect("device pairing request serialization cannot fail");
        Ok(keys::seal_box_encrypt(&plaintext, &offer.recipient()?))
    }

    pub fn open(
        ciphertext: &[u8],
        offer: &DevicePairingOffer,
        pairing_key: &UserKeypair,
    ) -> Result<Self, DevicePairingError> {
        if keys::public_key_hex(pairing_key) != offer.pairing_public_key {
            return Err(DevicePairingError::PairingKeyMismatch);
        }
        let plaintext = keys::seal_box_decrypt(ciphertext, &pairing_key.to_x25519_secret_key())?;
        let request: Self = serde_json::from_slice(&plaintext)?;
        request.verify(offer)?;
        Ok(request)
    }

    fn verify(&self, offer: &DevicePairingOffer) -> Result<(), DevicePairingError> {
        if self.version != PAIRING_VERSION {
            return Err(DevicePairingError::UnsupportedVersion(self.version));
        }
        if self.offer_hash != hex::encode(offer.digest()) {
            return Err(DevicePairingError::OfferMismatch);
        }
        decode_32("joining public key", &self.public_key)?;
        decode_64("pairing request signature", &self.signature)?;
        if !keys::verify_signature_hex(&self.public_key, &self.signature, &self.signing_bytes()) {
            return Err(DevicePairingError::InvalidSignature);
        }
        Ok(())
    }

    fn signing_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct SignedFields<'a> {
            version: u32,
            offer_hash: &'a str,
            public_key: &'a str,
            provider_account_email: &'a Option<String>,
        }
        let mut bytes = PAIRING_REQUEST_DOMAIN.to_vec();
        bytes.extend(
            serde_json::to_vec(&SignedFields {
                version: self.version,
                offer_hash: &self.offer_hash,
                public_key: &self.public_key,
                provider_account_email: &self.provider_account_email,
            })
            .expect("device pairing request serialization cannot fail"),
        );
        bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedDevicePairingRequest {
    pub session_id: String,
    pub ciphertext: String,
}

/// The joining device's retained side of a pairing attempt. It owns no secret
/// key bytes; the pending identity stays in the configured key custody and is
/// addressed by the signed request's public key until the join commits.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedDevicePairing {
    offer: DevicePairingOffer,
    request: DevicePairingRequest,
    sealed_request: SealedDevicePairingRequest,
    state: PreparedDevicePairingState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
enum PreparedDevicePairingState {
    AwaitingInvitation,
    ProviderAccessPending { invitation: Vec<u8> },
    LibraryInstallationPending { invitation: Vec<u8> },
}

/// The durable user-visible phase of one joining-device enrollment.
/// Invitation bytes remain private because they contain the sealed Store
/// admission; callers only need the operation they can resume.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DevicePairingPhase {
    AwaitingInvitation,
    ProviderAccessPending,
    LibraryInstallationPending,
}

impl PreparedDevicePairing {
    pub fn phase(&self) -> DevicePairingPhase {
        match &self.state {
            PreparedDevicePairingState::AwaitingInvitation => {
                DevicePairingPhase::AwaitingInvitation
            }
            PreparedDevicePairingState::ProviderAccessPending { .. } => {
                DevicePairingPhase::ProviderAccessPending
            }
            PreparedDevicePairingState::LibraryInstallationPending { .. } => {
                DevicePairingPhase::LibraryInstallationPending
            }
        }
    }

    pub fn pending(
        layout: &coven_foundation::store_dir::StoreLayout,
    ) -> Result<Vec<Self>, DevicePairingError> {
        let mut pending = Vec::new();
        for (path, bytes) in layout.pending_device_pairing_journals()? {
            let pairing: Self = serde_json::from_slice(&bytes)?;
            let expected = layout.pending_device_pairing_path(pairing.offer.session_id())?;
            if expected != path {
                return Err(DevicePairingError::PreparedPairingPathMismatch {
                    expected,
                    actual: path,
                });
            }
            pairing.request.verify(&pairing.offer)?;
            pending.push(pairing);
        }
        pending.sort_by(|left, right| left.offer.session_id().cmp(right.offer.session_id()));
        Ok(pending)
    }

    pub fn open_or_create(
        pairing_code: &str,
        provider_account_email: Option<String>,
        layout: &coven_foundation::store_dir::StoreLayout,
    ) -> Result<Self, DevicePairingError> {
        let offer = DevicePairingOffer::decode(pairing_code)?;
        let path = layout.pending_device_pairing_path(offer.session_id())?;
        let file = coven_foundation::atomic_file::AtomicFile::new(path);
        if let Some(bytes) = file.read_optional()? {
            let pairing: Self = serde_json::from_slice(&bytes)?;
            if pairing.offer != offer
                || pairing.request.provider_account_email != provider_account_email
            {
                return Err(DevicePairingError::PreparedPairingMismatch);
            }
            pairing.request.verify(&pairing.offer)?;
            return Ok(pairing);
        }
        let identity = keys::mint_pending_identity()?;
        let request =
            DevicePairingRequest::signed(&offer, &identity, provider_account_email.clone());
        let sealed_request = SealedDevicePairingRequest::new(&offer, &request)?;
        let pairing = Self {
            offer,
            request,
            sealed_request,
            state: PreparedDevicePairingState::AwaitingInvitation,
        };
        file.replace(
            &serde_json::to_vec(&pairing)
                .expect("prepared device pairing serialization cannot fail"),
        )?;
        Ok(pairing)
    }

    pub fn offer(&self) -> &DevicePairingOffer {
        &self.offer
    }

    pub fn request(&self) -> &DevicePairingRequest {
        &self.request
    }

    pub fn sealed_request(&self) -> &SealedDevicePairingRequest {
        &self.sealed_request
    }

    pub(crate) fn record_invitation_received(
        &self,
        layout: &coven_foundation::store_dir::StoreLayout,
        invitation: &[u8],
    ) -> Result<Self, DevicePairingError> {
        match &self.state {
            PreparedDevicePairingState::ProviderAccessPending {
                invitation: durable,
            }
            | PreparedDevicePairingState::LibraryInstallationPending {
                invitation: durable,
            } if durable == invitation => return Ok(self.clone()),
            PreparedDevicePairingState::ProviderAccessPending { .. }
            | PreparedDevicePairingState::LibraryInstallationPending { .. } => {
                return Err(DevicePairingError::InvitationConflict)
            }
            PreparedDevicePairingState::AwaitingInvitation => {}
        }
        self.replace_state(
            layout,
            PreparedDevicePairingState::ProviderAccessPending {
                invitation: invitation.to_vec(),
            },
        )
    }

    pub(crate) fn record_library_installation_pending(
        &self,
        layout: &coven_foundation::store_dir::StoreLayout,
    ) -> Result<Self, DevicePairingError> {
        let invitation = match &self.state {
            PreparedDevicePairingState::ProviderAccessPending { invitation } => invitation.clone(),
            PreparedDevicePairingState::LibraryInstallationPending { .. } => {
                return Ok(self.clone())
            }
            PreparedDevicePairingState::AwaitingInvitation => {
                return Err(DevicePairingError::InvitationMissing)
            }
        };
        self.replace_state(
            layout,
            PreparedDevicePairingState::LibraryInstallationPending { invitation },
        )
    }

    fn replace_state(
        &self,
        layout: &coven_foundation::store_dir::StoreLayout,
        state: PreparedDevicePairingState,
    ) -> Result<Self, DevicePairingError> {
        let pending = Self {
            offer: self.offer.clone(),
            request: self.request.clone(),
            sealed_request: self.sealed_request.clone(),
            state,
        };
        let path = layout.pending_device_pairing_path(self.offer.session_id())?;
        coven_foundation::atomic_file::AtomicFile::new(path).replace(
            &serde_json::to_vec(&pending)
                .expect("prepared device pairing serialization cannot fail"),
        )?;
        Ok(pending)
    }

    pub(crate) fn pending_invitation(&self) -> Option<&[u8]> {
        match &self.state {
            PreparedDevicePairingState::AwaitingInvitation => None,
            PreparedDevicePairingState::ProviderAccessPending { invitation }
            | PreparedDevicePairingState::LibraryInstallationPending { invitation } => {
                Some(invitation)
            }
        }
    }

    pub fn finish(
        &self,
        layout: &coven_foundation::store_dir::StoreLayout,
    ) -> Result<(), DevicePairingError> {
        let path = layout.pending_device_pairing_path(self.offer.session_id())?;
        coven_foundation::atomic_file::AtomicFile::new(path.clone()).remove()?;
        coven_foundation::atomic_file::sync_parent_dir_blocking(&path)?;
        Ok(())
    }

    pub fn abandon(
        self,
        layout: &coven_foundation::store_dir::StoreLayout,
    ) -> Result<(), DevicePairingError> {
        keys::discard_pending_identity(self.request.public_key())?;
        self.finish(layout)
    }
}

impl SealedDevicePairingRequest {
    pub fn new(
        offer: &DevicePairingOffer,
        request: &DevicePairingRequest,
    ) -> Result<Self, DevicePairingError> {
        Ok(Self {
            session_id: offer.session_id().to_string(),
            ciphertext: URL_SAFE_NO_PAD.encode(request.seal(offer)?),
        })
    }

    pub fn open(
        &self,
        offer: &DevicePairingOffer,
        pairing_key: &UserKeypair,
    ) -> Result<DevicePairingRequest, DevicePairingError> {
        if self.session_id != offer.session_id() {
            return Err(DevicePairingError::OfferMismatch);
        }
        let ciphertext = URL_SAFE_NO_PAD.decode(&self.ciphertext)?;
        DevicePairingRequest::open(&ciphertext, offer, pairing_key)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DevicePairingError {
    #[error("pairing code: {0}")]
    Code(#[from] code_envelope::EnvelopeError),
    #[error("unsupported pairing version {0}")]
    UnsupportedVersion(u32),
    #[error("pairing offer has no reachable endpoint")]
    NoEndpoint,
    #[error("pairing offer has an empty Store name")]
    EmptyStoreName,
    #[error("{field}: {source}")]
    Hex {
        field: &'static str,
        source: hex::FromHexError,
    },
    #[error("{field} must contain {expected} bytes")]
    HexLength {
        field: &'static str,
        expected: usize,
    },
    #[error("pairing key: {0}")]
    Key(#[from] keys::KeyError),
    #[error("pairing request JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("pairing journal: {0}")]
    Journal(#[from] coven_foundation::atomic_file::FileError),
    #[error("pairing journal path: {0}")]
    JournalPath(#[from] coven_foundation::store_dir::PathTokenError),
    #[error("pairing request ciphertext: {0}")]
    Ciphertext(#[from] base64::DecodeError),
    #[error("pairing request names another offer")]
    OfferMismatch,
    #[error("pairing request was opened with another session key")]
    PairingKeyMismatch,
    #[error("pairing request signature is invalid")]
    InvalidSignature,
    #[error("the durable pairing attempt has different immutable inputs")]
    PreparedPairingMismatch,
    #[error("the durable pairing attempt already holds another device invitation")]
    InvitationConflict,
    #[error("the durable pairing attempt has not received a device invitation")]
    InvitationMissing,
    #[error(
        "the durable pairing path is {}, expected {}",
        .actual.display(),
        .expected.display()
    )]
    PreparedPairingPathMismatch {
        expected: std::path::PathBuf,
        actual: std::path::PathBuf,
    },
}

fn decode_32(field: &'static str, value: &str) -> Result<[u8; 32], DevicePairingError> {
    decode_fixed(field, value)
}

fn decode_64(field: &'static str, value: &str) -> Result<[u8; 64], DevicePairingError> {
    decode_fixed(field, value)
}

fn decode_fixed<const N: usize>(
    field: &'static str,
    value: &str,
) -> Result<[u8; N], DevicePairingError> {
    let bytes = hex::decode(value).map_err(|source| DevicePairingError::Hex { field, source })?;
    bytes
        .try_into()
        .map_err(|_| DevicePairingError::HexLength { field, expected: N })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(pairing_key: &UserKeypair) -> DevicePairingOffer {
        DevicePairingOffer::new(
            pairing_key,
            vec!["127.0.0.1:24821".parse().expect("loopback endpoint")],
            "Pairing Test Store".to_string(),
            CloudProvider::GoogleDrive,
            1_900_000_000,
        )
        .expect("pairing offer")
    }

    #[test]
    fn one_scanned_offer_binds_the_signed_joining_identity_and_provider_account() {
        let pairing_key = UserKeypair::generate();
        let offer = offer(&pairing_key);
        let decoded = DevicePairingOffer::decode(&offer.encode()).expect("decode pairing offer");
        let joining_identity = UserKeypair::generate();
        let request = DevicePairingRequest::signed(
            &decoded,
            &joining_identity,
            Some("member@example.com".to_string()),
        );
        let sealed =
            SealedDevicePairingRequest::new(&decoded, &request).expect("seal pairing request");
        let opened = sealed
            .open(&decoded, &pairing_key)
            .expect("open pairing request");

        assert_eq!(opened.public_key(), keys::public_key_hex(&joining_identity));
        assert_eq!(opened.provider_account_email(), Some("member@example.com"));
        assert_eq!(decoded.store_name(), "Pairing Test Store");
        assert_eq!(decoded.cloud_provider(), &CloudProvider::GoogleDrive);
    }

    #[test]
    fn pairing_offer_recognizes_its_envelope_before_decoding() {
        assert!(DevicePairingOffer::is_pairing_code(
            "  coven:device-pairing:not-yet-decoded  "
        ));
        assert!(!DevicePairingOffer::is_pairing_code(
            "coven:restore-payload"
        ));
        assert!(!DevicePairingOffer::is_pairing_code("not-a-coven-code"));
    }

    #[test]
    fn a_request_cannot_cross_pairing_sessions() {
        let first_key = UserKeypair::generate();
        let first = offer(&first_key);
        let second_key = UserKeypair::generate();
        let second = DevicePairingOffer::new(
            &second_key,
            vec!["127.0.0.1:24821".parse().expect("loopback endpoint")],
            "Other Store".to_string(),
            CloudProvider::Dropbox,
            1_900_000_000,
        )
        .expect("second offer");
        let request = DevicePairingRequest::signed(&first, &UserKeypair::generate(), None);
        let mut sealed =
            SealedDevicePairingRequest::new(&first, &request).expect("seal first request");
        sealed.session_id = second.session_id().to_string();

        assert!(matches!(
            sealed.open(&second, &second_key),
            Err(DevicePairingError::Key(_)) | Err(DevicePairingError::OfferMismatch)
        ));
    }

    #[test]
    fn prepared_pairing_reopens_the_same_pending_identity_and_abandons_it_exactly() {
        coven_keys::keys::test_keyring::install();
        let pairing_key = UserKeypair::generate();
        let offer = offer(&pairing_key);
        let app = tempfile::tempdir().expect("pairing app directory");
        let layout = coven_foundation::store_dir::StoreLayout::new(app.path());
        let first = PreparedDevicePairing::open_or_create(
            &offer.encode(),
            Some("member@example.com".to_string()),
            &layout,
        )
        .expect("prepare pairing");
        let first_pubkey = first.request().public_key().to_string();
        let reopened = PreparedDevicePairing::open_or_create(
            &offer.encode(),
            Some("member@example.com".to_string()),
            &layout,
        )
        .expect("reopen pairing");

        assert_eq!(reopened.request().public_key(), first_pubkey);
        assert!(matches!(
            PreparedDevicePairing::open_or_create(
                &offer.encode(),
                Some("other@example.com".to_string()),
                &layout,
            ),
            Err(DevicePairingError::PreparedPairingMismatch)
        ));
        reopened.abandon(&layout).expect("abandon pairing");
        assert!(coven_keys::keys::peek_pending_identity(&first_pubkey).is_err());
        assert!(!layout
            .pending_device_pairing_path(offer.session_id())
            .expect("pairing journal path")
            .exists());
    }

    #[test]
    fn received_invitation_is_durable_until_library_installation_finishes() {
        coven_keys::keys::test_keyring::install();
        let pairing_key = UserKeypair::generate();
        let offer = offer(&pairing_key);
        let app = tempfile::tempdir().expect("pairing app directory");
        let layout = coven_foundation::store_dir::StoreLayout::new(app.path());
        let prepared = PreparedDevicePairing::open_or_create(
            &offer.encode(),
            Some("member@example.com".to_string()),
            &layout,
        )
        .expect("prepare pairing");
        let invitation = b"validated sealed invitation";

        let awaiting_provider = prepared
            .record_invitation_received(&layout, invitation)
            .expect("record received invitation");
        assert_eq!(
            awaiting_provider.phase(),
            DevicePairingPhase::ProviderAccessPending
        );
        awaiting_provider
            .record_library_installation_pending(&layout)
            .expect("record pending library installation");
        let reopened = PreparedDevicePairing::open_or_create(
            &offer.encode(),
            Some("member@example.com".to_string()),
            &layout,
        )
        .expect("reopen pairing after process restart");
        std::fs::write(
            layout.pending_device_pairings_dir().join(".tmp.crashed"),
            b"incomplete atomic stage",
        )
        .expect("seed interrupted atomic stage");
        let pending =
            PreparedDevicePairing::pending(&layout).expect("enumerate pending device enrollments");

        assert_eq!(
            reopened.phase(),
            DevicePairingPhase::LibraryInstallationPending
        );
        assert_eq!(reopened.pending_invitation(), Some(invitation.as_slice()));
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].pending_invitation(), Some(invitation.as_slice()));
    }
}
