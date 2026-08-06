use super::probe::*;
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderAdminGrantId(pub ObjectHash);

impl ProviderAdminGrantId {
    pub fn from_random_bytes(bytes: [u8; 32]) -> Self {
        Self(ObjectHash::from_digest(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderAccessGrantId(pub ObjectHash);

impl ProviderAccessGrantId {
    pub fn from_random_bytes(bytes: [u8; 32]) -> Self {
        Self(ObjectHash::from_digest(bytes))
    }
}

/// Stable provider authority that can be withdrawn without rediscovering a
/// member by mutable account metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAccessLocator {
    S3SharedCredentialGeneration {
        generation: u64,
        access_key_id_hash: ObjectHash,
    },
    GoogleDrivePermission {
        drive_id: String,
        permission_id: String,
    },
    DropboxSharedFolderMember {
        namespace_id: String,
        account_id: String,
    },
    OneDrivePermission {
        drive_id: String,
        item_id: String,
        permission_id: String,
    },
    CloudKitPrivateZoneOwner {
        owner_name: String,
        zone_name: String,
        owner_record_name: String,
    },
    CloudKitParticipant {
        share_record_name: String,
        owner_name: String,
        zone_name: String,
        participant_record_name: String,
    },
}

impl ProviderAccessLocator {
    pub fn for_current_administrator(
        binding: &crate::objects::ResolvedProviderBinding,
    ) -> Result<Self, StorageError> {
        binding.validate()?;
        match (&binding.store, &binding.device.principal) {
            (
                StoreProviderBinding::S3 { .. },
                crate::objects::ProviderPrincipalId::CustomS3Credential { access_key_id_hash },
            ) => Ok(Self::S3SharedCredentialGeneration {
                generation: 1,
                access_key_id_hash: *access_key_id_hash,
            }),
            (
                StoreProviderBinding::GoogleDrive {
                    corpus: crate::objects::GoogleDriveCorpus::SharedDrive { drive_id, .. },
                },
                crate::objects::ProviderPrincipalId::GoogleDrive { permission_id },
            ) => Ok(Self::GoogleDrivePermission {
                drive_id: drive_id.clone(),
                permission_id: permission_id.clone(),
            }),
            (
                StoreProviderBinding::Dropbox { namespace_id },
                crate::objects::ProviderPrincipalId::Dropbox { account_id },
            ) => Ok(Self::DropboxSharedFolderMember {
                namespace_id: namespace_id.clone(),
                account_id: account_id.clone(),
            }),
            (
                StoreProviderBinding::CloudKit {
                    owner_name,
                    zone_name,
                    ..
                },
                crate::objects::ProviderPrincipalId::CloudKitPrivateZoneOwner { record_name },
            ) => Ok(Self::CloudKitPrivateZoneOwner {
                owner_name: owner_name.clone(),
                zone_name: zone_name.clone(),
                owner_record_name: record_name.clone(),
            }),
            _ => Err(StorageError::Configuration(
                "provider adapter did not expose the administrator's exact access locator"
                    .to_string(),
            )),
        }
    }

    pub fn validate_for(
        &self,
        store: &StoreProviderBinding,
        provider: &ProviderDeviceBinding,
    ) -> Result<(), StorageError> {
        provider.validate_for(store)?;
        let valid = match (store, &provider.principal, self) {
            (
                StoreProviderBinding::S3 { .. },
                crate::objects::ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: provider_hash,
                },
                Self::S3SharedCredentialGeneration {
                    generation,
                    access_key_id_hash,
                },
            ) => *generation > 0 && provider_hash == access_key_id_hash,
            (
                StoreProviderBinding::S3 { .. },
                crate::objects::ProviderPrincipalId::Aws { .. },
                Self::S3SharedCredentialGeneration { generation, .. },
            ) => *generation > 0,
            (
                StoreProviderBinding::GoogleDrive {
                    corpus: crate::objects::GoogleDriveCorpus::SharedDrive { drive_id, .. },
                },
                crate::objects::ProviderPrincipalId::GoogleDrive { permission_id },
                Self::GoogleDrivePermission {
                    drive_id: locator_drive,
                    permission_id: locator_permission,
                },
            ) => drive_id == locator_drive && permission_id == locator_permission,
            (
                StoreProviderBinding::Dropbox { namespace_id },
                crate::objects::ProviderPrincipalId::Dropbox { account_id },
                Self::DropboxSharedFolderMember {
                    namespace_id: locator_namespace,
                    account_id: locator_account,
                },
            ) => namespace_id == locator_namespace && account_id == locator_account,
            (
                StoreProviderBinding::OneDrive {
                    drive_id,
                    folder_id,
                },
                crate::objects::ProviderPrincipalId::OneDrive { .. },
                Self::OneDrivePermission {
                    drive_id: locator_drive,
                    item_id,
                    permission_id,
                },
            ) => drive_id == locator_drive && folder_id == item_id && !permission_id.is_empty(),
            (
                StoreProviderBinding::CloudKit {
                    owner_name,
                    zone_name,
                    ..
                },
                crate::objects::ProviderPrincipalId::CloudKitPrivateZoneOwner { record_name },
                Self::CloudKitPrivateZoneOwner {
                    owner_name: locator_owner,
                    zone_name: locator_zone,
                    owner_record_name,
                },
            ) => {
                owner_name == locator_owner
                    && zone_name == locator_zone
                    && record_name == owner_record_name
            }
            (
                StoreProviderBinding::CloudKit {
                    owner_name,
                    zone_name,
                    ..
                },
                crate::objects::ProviderPrincipalId::CloudKitSharedZoneParticipant { record_name },
                Self::CloudKitParticipant {
                    share_record_name,
                    owner_name: locator_owner,
                    zone_name: locator_zone,
                    participant_record_name,
                },
            ) => {
                !share_record_name.is_empty()
                    && owner_name == locator_owner
                    && zone_name == locator_zone
                    && record_name == participant_record_name
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(StorageError::Configuration(
                "provider access locator differs from its Store and provider binding".to_string(),
            ))
        }
    }
}

/// The wire body of one member's provider access grant. Every field here is
/// signed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMemberProviderAccessGrantBody {
    pub grant_id: ProviderAccessGrantId,
    pub member_pubkey: String,
    pub provider: ProviderDeviceBinding,
    pub locator: ProviderAccessLocator,
    pub administrator_grant: ProviderAdminGrantId,
    pub administrator: StoreDeviceRegistrationRef,
}

impl crate::store_commit::SignedBody for StoreMemberProviderAccessGrantBody {
    const DOMAIN: &'static [u8] = MEMBER_ACCESS_GRANT_DOMAIN;
}

pub type StoreMemberProviderAccessGrant =
    crate::store_commit::Signed<StoreMemberProviderAccessGrantBody>;

impl StoreMemberProviderAccessGrant {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        grant_id: ProviderAccessGrantId,
        member_pubkey: String,
        provider: ProviderDeviceBinding,
        locator: ProviderAccessLocator,
        administrator_grant: ProviderAdminGrantId,
        administrator: StoreDeviceRegistrationRef,
        store: &StoreProviderBinding,
        administrator_registration: &StoreDeviceRegistration,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
    ) -> Result<Self, ProviderProbeError> {
        administrator
            .verify_registration(administrator_registration)
            .map_err(|error| ProviderProbeError::InvalidReceipt(error.to_string()))?;
        if administrator_signer.public_key_hex() != administrator_registration.device_signing_pubkey
        {
            return invalid("provider access grant signer is not the administrator device");
        }
        locator.validate_for(store, &provider)?;
        Ok(crate::store_commit::Signed::sign_by_device(
            StoreMemberProviderAccessGrantBody {
                grant_id,
                member_pubkey,
                provider,
                locator,
                administrator_grant,
                administrator,
            },
            administrator_signer,
        ))
    }

    pub fn grant_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn verify(
        &self,
        store: &StoreProviderBinding,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), ProviderProbeError> {
        self.administrator
            .verify_registration(administrator)
            .map_err(|error| ProviderProbeError::InvalidReceipt(error.to_string()))?;
        self.provider
            .validate_for(store)
            .map_err(ProviderProbeError::Storage)?;
        self.locator
            .validate_for(store, &self.provider)
            .map_err(ProviderProbeError::Storage)?;
        if self
            .verify_by(&administrator.device_signing_pubkey)
            .is_err()
        {
            return invalid("provider access grant signature is invalid");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMemberProviderAccessGrantRef {
    pub grant_id: ProviderAccessGrantId,
    pub grant_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl StoreMemberProviderAccessGrantRef {
    pub fn from_grant(grant: &StoreMemberProviderAccessGrant, object: ExactObjectRef) -> Self {
        Self {
            grant_id: grant.grant_id.clone(),
            grant_hash: grant.grant_hash(),
            object,
        }
    }

    pub fn verify(&self, grant: &StoreMemberProviderAccessGrant) -> Result<(), ProviderProbeError> {
        if self.grant_id != grant.grant_id || self.grant_hash != grant.grant_hash() {
            return invalid("provider access grant reference differs from its signed grant");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatedStoreMemberProviderAccessGrant {
    pub grant: StoreMemberProviderAccessGrant,
    pub grant_ref: StoreMemberProviderAccessGrantRef,
    pub activation: StoreBatchCommitRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAccessWithdrawal {
    Direct {
        locator: ProviderAccessLocator,
        verified_absent: bool,
    },
    S3CredentialRotation {
        retired_generation: u64,
        active_generation: u64,
        retired_credential_verified_rejected: bool,
    },
}

impl ProviderAccessWithdrawal {
    pub(super) fn validate(&self) -> Result<(), ProviderProbeError> {
        let valid = match self {
            Self::Direct {
                verified_absent, ..
            } => *verified_absent,
            Self::S3CredentialRotation {
                retired_generation,
                active_generation,
                retired_credential_verified_rejected,
            } => {
                *retired_generation > 0
                    && retired_generation.checked_add(1) == Some(*active_generation)
                    && *retired_credential_verified_rejected
            }
        };
        if valid {
            Ok(())
        } else {
            invalid("provider access withdrawal does not prove the stored authority is unusable")
        }
    }

    pub fn verify_for_locator(
        &self,
        locator: &ProviderAccessLocator,
    ) -> Result<(), ProviderProbeError> {
        self.validate()?;
        let matches = match (self, locator) {
            (
                Self::Direct {
                    locator: withdrawn, ..
                },
                expected,
            ) => withdrawn == expected,
            (
                Self::S3CredentialRotation {
                    retired_generation, ..
                },
                ProviderAccessLocator::S3SharedCredentialGeneration { generation, .. },
            ) => retired_generation == generation,
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            invalid("provider access withdrawal differs from the stored authority locator")
        }
    }
}
