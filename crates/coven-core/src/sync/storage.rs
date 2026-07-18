//! Exact storage access for signed protocol objects and stored blob bodies.
//!
//! Every remote object is addressed by an [`ExactObjectRef`]. The logical key
//! supplies domain separation and the physical locator selects the one provider
//! object whose stored size and hash the signed reference authenticates. Prefix
//! enumeration and provider names never select protocol authority.
use async_trait::async_trait;
use std::path::Path;

use crate::storage::cloud::{CloudHeadVersion, ObjectSlot};
use crate::sync::store_commit::ObjectHash;

/// Signed object kind bound into protection AAD and checked against the
/// semantic path before storage I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtectedObjectDomain {
    StoreProtocolRoot,
    StoreCommit,
    StoreHead,
    StoreAck,
    StoreDeviceRegistration,
    StoreDeviceSelfRetirement,
    DeviceJoinAttempt,
    DeviceJoinOutcome,
    DeviceJoinAbandonment,
    DeviceJoinCleanupReceipt,
    ProviderAccessGrant,
    ProviderAccessWithdrawal,
    OwnerRecoveryNode,
    StoreSnapshotMeta,
    StoreSnapshotImage,
    StoreMembershipEntry,
    StoreMembershipHead,
    StoreMembershipResolution,
    StorePackage,
    CircleControl,
    CircleRoster,
    CircleRosterResolution,
    CircleMetadata,
    CirclePackage,
    CircleAccessLeaf,
    CircleAccessEnvelope,
}

#[derive(Clone, Copy)]
struct ProtocolObjectMetadata {
    aad_label: &'static [u8],
    path: ProtocolPathRule,
    extension: &'static str,
}

#[derive(Clone, Copy)]
enum ProtocolPathRule {
    Exact(&'static [ExactPathShape]),
    StoreDeviceRegistration,
    StoreMembershipHead,
    StoreCandidate {
        kind: &'static str,
        component_count: usize,
    },
    CircleCandidate {
        kind: &'static str,
        component_count: usize,
    },
}

#[derive(Clone, Copy)]
struct ExactPathShape {
    component_count: usize,
    fixed_components: &'static [(usize, &'static str)],
}

impl ProtocolPathRule {
    fn accepts(self, semantic_prefix: &str) -> bool {
        match self {
            Self::Exact(shapes) => shapes
                .iter()
                .any(|shape| accepts_path_shape(semantic_prefix, *shape)),
            Self::StoreDeviceRegistration => {
                (accepts_path_shape(
                    semantic_prefix,
                    ExactPathShape {
                        component_count: 3,
                        fixed_components: &[(0, "store-v1"), (1, "devices")],
                    },
                ) && semantic_prefix.split('/').nth(2) != Some("founder"))
                    || accepts_path_shape(
                        semantic_prefix,
                        ExactPathShape {
                            component_count: 5,
                            fixed_components: &[
                                (0, "store-v1"),
                                (1, "devices"),
                                (2, "founder"),
                                (4, "registration"),
                            ],
                        },
                    )
            }
            Self::StoreMembershipHead => {
                (accepts_path_shape(
                    semantic_prefix,
                    ExactPathShape {
                        component_count: 7,
                        fixed_components: &[(0, "store-v1"), (1, "membership"), (2, "heads")],
                    },
                ) && semantic_prefix.split('/').nth(3) != Some("founder"))
                    || accepts_path_shape(
                        semantic_prefix,
                        ExactPathShape {
                            component_count: 6,
                            fixed_components: &[
                                (0, "store-v1"),
                                (1, "membership"),
                                (2, "heads"),
                                (3, "founder"),
                                (5, "1"),
                            ],
                        },
                    )
            }
            Self::StoreCandidate {
                kind,
                component_count,
            } => accepts_candidate_path(
                semantic_prefix,
                component_count,
                &[(0, "store-v1"), (1, "candidates"), (3, kind)],
            ),
            Self::CircleCandidate {
                kind,
                component_count,
            } => accepts_candidate_path(
                semantic_prefix,
                component_count,
                &[(0, "circles"), (2, "candidates"), (4, kind)],
            ),
        }
    }
}

fn accepts_path_shape(semantic_prefix: &str, shape: ExactPathShape) -> bool {
    let components = semantic_prefix.split('/').collect::<Vec<_>>();
    components.len() == shape.component_count
        && components.iter().all(|component| !component.is_empty())
        && shape
            .fixed_components
            .iter()
            .all(|(index, expected)| components[*index] == *expected)
}

fn accepts_candidate_path(
    semantic_prefix: &str,
    component_count: usize,
    fixed_components: &[(usize, &str)],
) -> bool {
    let components = semantic_prefix.split('/').collect::<Vec<_>>();
    components.len() == component_count
        && components.iter().all(|component| !component.is_empty())
        && fixed_components.iter().all(|(index, expected)| {
            components[*index] == *expected
                && components
                    .iter()
                    .filter(|component| **component == *expected)
                    .count()
                    == 1
        })
}

impl ProtectedObjectDomain {
    fn metadata(self) -> ProtocolObjectMetadata {
        match self {
            Self::StoreProtocolRoot => ProtocolObjectMetadata {
                aad_label: b"store-protocol-root",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 2,
                    fixed_components: &[(0, "store-v1"), (1, "store-protocol-root")],
                }]),
                extension: ".json",
            },
            Self::StoreCommit => ProtocolObjectMetadata {
                aad_label: b"store-commit",
                path: ProtocolPathRule::StoreCandidate {
                    kind: "commits",
                    component_count: 7,
                },
                extension: ".json",
            },
            Self::StoreHead => ProtocolObjectMetadata {
                aad_label: b"store-head",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "heads")],
                }]),
                extension: ".json",
            },
            Self::StoreAck => ProtocolObjectMetadata {
                aad_label: b"store-ack",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "acks")],
                }]),
                extension: ".json",
            },
            Self::StoreDeviceRegistration => ProtocolObjectMetadata {
                aad_label: b"store-device-registration",
                path: ProtocolPathRule::StoreDeviceRegistration,
                extension: ".json",
            },
            Self::StoreDeviceSelfRetirement => ProtocolObjectMetadata {
                aad_label: b"store-device-self-retirement",
                path: ProtocolPathRule::StoreCandidate {
                    kind: "device-self-retirements",
                    component_count: 6,
                },
                extension: ".json",
            },
            Self::DeviceJoinAttempt => ProtocolObjectMetadata {
                aad_label: b"device-join-attempt",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 3,
                    fixed_components: &[(0, "store-v1"), (1, "device-join-attempts")],
                }]),
                extension: ".json",
            },
            Self::DeviceJoinOutcome => ProtocolObjectMetadata {
                aad_label: b"device-join-outcome",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 3,
                    fixed_components: &[(0, "store-v1"), (1, "device-join-outcomes")],
                }]),
                extension: ".json",
            },
            Self::DeviceJoinAbandonment => ProtocolObjectMetadata {
                aad_label: b"device-join-abandonment",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 3,
                    fixed_components: &[(0, "store-v1"), (1, "device-join-attempts")],
                }]),
                extension: ".json",
            },
            Self::DeviceJoinCleanupReceipt => ProtocolObjectMetadata {
                aad_label: b"device-join-cleanup-receipt",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 3,
                    fixed_components: &[(0, "store-v1"), (1, "device-join-cleanup-receipts")],
                }]),
                extension: ".json",
            },
            Self::ProviderAccessGrant => ProtocolObjectMetadata {
                aad_label: b"provider-access-grant",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "provider-access"), (2, "grants")],
                }]),
                extension: ".json",
            },
            Self::ProviderAccessWithdrawal => ProtocolObjectMetadata {
                aad_label: b"provider-access-withdrawal",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[
                        (0, "store-v1"),
                        (1, "provider-access"),
                        (2, "withdrawals"),
                    ],
                }]),
                extension: ".json",
            },
            Self::OwnerRecoveryNode => ProtocolObjectMetadata {
                aad_label: b"owner-recovery-node",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 5,
                    fixed_components: &[(0, "store-v1"), (1, "recovery")],
                }]),
                extension: ".json",
            },
            Self::StoreSnapshotMeta => ProtocolObjectMetadata {
                aad_label: b"store-snapshot-meta",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "snapshots")],
                }]),
                extension: ".json",
            },
            Self::StoreSnapshotImage => ProtocolObjectMetadata {
                aad_label: b"store-snapshot-image",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "snapshot-images")],
                }]),
                extension: ".db",
            },
            Self::StoreMembershipEntry => ProtocolObjectMetadata {
                aad_label: b"store-membership-entry",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 8,
                    fixed_components: &[(0, "store-v1"), (1, "membership"), (2, "entries")],
                }]),
                extension: ".json",
            },
            Self::StoreMembershipHead => ProtocolObjectMetadata {
                aad_label: b"store-membership-head",
                path: ProtocolPathRule::StoreMembershipHead,
                extension: ".json",
            },
            Self::StoreMembershipResolution => ProtocolObjectMetadata {
                aad_label: b"store-membership-resolution",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 6,
                    fixed_components: &[(0, "store-v1"), (1, "membership"), (2, "resolutions")],
                }]),
                extension: ".json",
            },
            Self::StorePackage => ProtocolObjectMetadata {
                aad_label: b"store-package",
                path: ProtocolPathRule::StoreCandidate {
                    kind: "packages",
                    component_count: 7,
                },
                extension: ".pkg",
            },
            Self::CircleControl => ProtocolObjectMetadata {
                aad_label: b"circle-control",
                path: ProtocolPathRule::Exact(&[
                    ExactPathShape {
                        component_count: 10,
                        fixed_components: &[(0, "circle-control"), (2, "merge"), (3, "entries")],
                    },
                    ExactPathShape {
                        component_count: 9,
                        fixed_components: &[(0, "circle-control"), (2, "merge"), (3, "heads")],
                    },
                    ExactPathShape {
                        component_count: 6,
                        fixed_components: &[(0, "circle-control"), (2, "serial")],
                    },
                ]),
                extension: ".json",
            },
            Self::CircleRoster => ProtocolObjectMetadata {
                aad_label: b"circle-roster",
                path: ProtocolPathRule::Exact(&[
                    ExactPathShape {
                        component_count: 10,
                        fixed_components: &[(0, "circles"), (2, "roster"), (3, "entries")],
                    },
                    ExactPathShape {
                        component_count: 9,
                        fixed_components: &[(0, "circles"), (2, "roster"), (3, "heads")],
                    },
                ]),
                extension: ".json",
            },
            Self::CircleRosterResolution => ProtocolObjectMetadata {
                aad_label: b"circle-roster-resolution",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 7,
                    fixed_components: &[(0, "circles"), (2, "roster"), (3, "resolutions")],
                }]),
                extension: ".json",
            },
            Self::CircleMetadata => ProtocolObjectMetadata {
                aad_label: b"circle-metadata",
                path: ProtocolPathRule::Exact(&[
                    ExactPathShape {
                        component_count: 10,
                        fixed_components: &[(0, "circles"), (2, "metadata"), (3, "entries")],
                    },
                    ExactPathShape {
                        component_count: 9,
                        fixed_components: &[(0, "circles"), (2, "metadata"), (3, "heads")],
                    },
                ]),
                extension: ".json",
            },
            Self::CirclePackage => ProtocolObjectMetadata {
                aad_label: b"circle-package",
                path: ProtocolPathRule::CircleCandidate {
                    kind: "packages",
                    component_count: 8,
                },
                extension: ".pkg",
            },
            Self::CircleAccessLeaf => ProtocolObjectMetadata {
                aad_label: b"circle-access-leaf",
                path: ProtocolPathRule::CircleCandidate {
                    kind: "access-leaves",
                    component_count: 9,
                },
                extension: "",
            },
            Self::CircleAccessEnvelope => ProtocolObjectMetadata {
                aad_label: b"circle-access-envelope",
                path: ProtocolPathRule::CircleCandidate {
                    kind: "access-envelopes",
                    component_count: 8,
                },
                extension: ".json",
            },
        }
    }

    pub(crate) fn aad_label(self) -> &'static [u8] {
        self.metadata().aad_label
    }

    pub(crate) fn extension(self) -> &'static str {
        self.metadata().extension
    }
}

/// A domain protected by the Store key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreProtocolObjectDomain(ProtectedObjectDomain);

/// A domain protected by a Circle epoch key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CircleProtocolObjectDomain(ProtectedObjectDomain);

/// Typed protocol-object domain names. Each name's value carries the only
/// protection class its object kind permits.
pub struct ProtocolObjectDomain;

#[allow(non_upper_case_globals)]
impl ProtocolObjectDomain {
    pub const StoreProtocolRoot: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreProtocolRoot);
    pub const StoreCommit: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreCommit);
    pub const StoreHead: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreHead);
    pub const StoreAck: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreAck);
    pub const StoreDeviceRegistration: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreDeviceRegistration);
    pub const StoreDeviceSelfRetirement: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreDeviceSelfRetirement);
    pub const DeviceJoinAttempt: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinAttempt);
    pub const DeviceJoinOutcome: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinOutcome);
    pub const DeviceJoinAbandonment: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinAbandonment);
    pub const DeviceJoinCleanupReceipt: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinCleanupReceipt);
    pub const ProviderAccessGrant: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::ProviderAccessGrant);
    pub const ProviderAccessWithdrawal: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::ProviderAccessWithdrawal);
    pub const OwnerRecoveryNode: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::OwnerRecoveryNode);
    pub const StoreSnapshotMeta: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreSnapshotMeta);
    pub const StoreSnapshotImage: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreSnapshotImage);
    pub const StoreMembershipEntry: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreMembershipEntry);
    pub const StoreMembershipHead: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreMembershipHead);
    pub const StoreMembershipResolution: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StoreMembershipResolution);
    pub const StorePackage: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::StorePackage);
    pub const CircleControl: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::CircleControl);
    pub const CircleAccessEnvelope: StoreProtocolObjectDomain =
        StoreProtocolObjectDomain(ProtectedObjectDomain::CircleAccessEnvelope);
    pub const CircleRoster: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleRoster);
    pub const CircleRosterResolution: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleRosterResolution);
    pub const CircleMetadata: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleMetadata);
    pub const CirclePackage: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CirclePackage);
}

/// Authenticated storage context for one immutable semantic object.
///
/// Store protection cannot be paired with a Circle-encrypted domain:
///
/// ```compile_fail
/// use coven_core::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
/// use coven_core::ObjectHash;
///
/// let root = ObjectHash::digest(b"store");
/// let _ = ProtocolObjectContext::store(root, ProtocolObjectDomain::CircleMetadata);
/// ```
///
/// Circle protection cannot be paired with a Store domain:
///
/// ```compile_fail
/// use coven_core::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
/// use coven_core::{EncryptionService, ObjectHash};
///
/// let root = ObjectHash::digest(b"store");
/// let encryption = EncryptionService::from_key([7; 32]);
/// let _ = ProtocolObjectContext::circle(
///     root,
///     ProtocolObjectDomain::StoreCommit,
///     encryption,
/// );
/// ```
pub struct ProtocolObjectContext {
    store_root_hash: ObjectHash,
    domain: ProtectedObjectDomain,
    protection: ProtocolObjectProtection,
}

#[derive(Clone)]
pub(crate) enum ProtocolObjectProtection {
    Store,
    Circle(crate::encryption::EncryptionService),
    RecipientSealed,
}

impl ProtocolObjectContext {
    pub fn store(store_root_hash: ObjectHash, domain: StoreProtocolObjectDomain) -> Self {
        Self {
            store_root_hash,
            domain: domain.0,
            protection: ProtocolObjectProtection::Store,
        }
    }

    pub fn circle(
        store_root_hash: ObjectHash,
        domain: CircleProtocolObjectDomain,
        encryption: crate::encryption::EncryptionService,
    ) -> Self {
        Self {
            store_root_hash,
            domain: domain.0,
            protection: ProtocolObjectProtection::Circle(encryption),
        }
    }

    pub fn recipient_sealed(store_root_hash: ObjectHash) -> Self {
        Self {
            store_root_hash,
            domain: ProtectedObjectDomain::CircleAccessLeaf,
            protection: ProtocolObjectProtection::RecipientSealed,
        }
    }

    pub fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub(crate) fn domain(&self) -> ProtectedObjectDomain {
        self.domain
    }

    pub(crate) fn protection(&self) -> &ProtocolObjectProtection {
        &self.protection
    }

    pub fn validate_path(&self, semantic_prefix: &str) -> Result<(), StorageError> {
        let metadata = self.domain.metadata();
        if semantic_prefix.contains("/copies/") || !metadata.path.accepts(semantic_prefix) {
            return Err(StorageError::Parse(format!(
                "object domain {:?} does not accept semantic path {semantic_prefix:?}",
                self.domain
            )));
        }
        Ok(())
    }

    pub fn validate_extension(&self, extension: &str) -> Result<(), StorageError> {
        if extension != self.domain.extension() {
            return Err(StorageError::Parse(format!(
                "object domain {:?} does not accept extension {extension:?}",
                self.domain
            )));
        }
        Ok(())
    }

    pub fn validate_reference(
        &self,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<(), StorageError> {
        self.validate_slot(object.slot(), semantic_prefix)
    }

    pub fn validate_slot(
        &self,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(), StorageError> {
        self.validate_path(semantic_prefix)?;
        let expected = format!("{semantic_prefix}{}", self.domain.extension());
        if slot.logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "protocol object {:?} does not match semantic path {semantic_prefix:?}",
                slot.logical_key()
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionToken(CloudHeadVersion);

impl VersionToken {
    pub(crate) fn from_cloud(version: CloudHeadVersion) -> Self {
        Self(version)
    }

    pub(crate) fn cloud(&self) -> &CloudHeadVersion {
        &self.0
    }
}

impl serde::Serialize for VersionToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.as_provider())
    }
}

impl<'de> serde::Deserialize<'de> for VersionToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        CloudHeadVersion::from_provider(value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedObject {
    pub bytes: Vec<u8>,
    pub version: VersionToken,
}

/// Protection selected by the audience authority that prepares a blob spool.
#[derive(Clone)]
pub enum BlobSpoolProtection {
    Opaque(crate::encryption::EncryptionService),
    Browsable,
}

#[derive(Clone, Copy)]
pub struct BlobWriteAuthority<'a> {
    pub reference: &'a crate::sync::store_commit::StoreDeviceRegistrationRef,
    pub registration: &'a crate::sync::store_commit::StoreDeviceRegistration,
}

impl<'a> BlobWriteAuthority<'a> {
    pub fn new(
        reference: &'a crate::sync::store_commit::StoreDeviceRegistrationRef,
        registration: &'a crate::sync::store_commit::StoreDeviceRegistration,
    ) -> Result<Self, StorageError> {
        reference
            .verify_registration(registration)
            .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
        Ok(Self {
            reference,
            registration,
        })
    }
}

/// Provider namespace/corpus facts signed once by the Store root.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreProviderBinding {
    S3 {
        endpoint: S3EndpointBinding,
        region: String,
        bucket: String,
        key_prefix: Option<String>,
    },
    GoogleDrive {
        corpus: GoogleDriveCorpus,
    },
    Dropbox {
        namespace_id: String,
    },
    OneDrive {
        drive_id: String,
        folder_id: String,
    },
    CloudKit {
        container_id: String,
        environment: CloudKitEnvironment,
        owner_name: String,
        zone_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3EndpointBinding {
    Aws { partition: String },
    Custom { origin: String },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "corpus", rename_all = "snake_case", deny_unknown_fields)]
pub enum GoogleDriveCorpus {
    MyDrive { folder_id: String },
    SharedDrive { drive_id: String, folder_id: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudKitEnvironment {
    Development,
    Production,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderPrincipalId {
    Aws {
        account_id: String,
        principal: AwsPrincipal,
    },
    CustomS3Credential {
        access_key_id_hash: ObjectHash,
    },
    GoogleDrive {
        permission_id: String,
    },
    Dropbox {
        account_id: String,
    },
    OneDrive {
        user_id: String,
    },
    CloudKitPrivateZoneOwner {
        record_name: String,
    },
    CloudKitSharedZoneParticipant {
        record_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AwsPrincipal {
    Root,
    User { arn: String, user_id: String },
    Role { role_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDeviceBinding {
    pub principal: ProviderPrincipalId,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedProviderBinding {
    pub store: StoreProviderBinding,
    pub device: ProviderDeviceBinding,
}

impl StoreProviderBinding {
    pub fn validate(&self) -> Result<(), StorageError> {
        fn present(label: &str, value: &str) -> Result<(), StorageError> {
            if value.is_empty() {
                Err(StorageError::Configuration(format!("{label} is empty")))
            } else {
                Ok(())
            }
        }

        match self {
            Self::S3 {
                endpoint,
                region,
                bucket,
                key_prefix,
            } => {
                present("S3 region", region)?;
                present("S3 bucket", bucket)?;
                if key_prefix.as_deref().is_some_and(str::is_empty) {
                    return Err(StorageError::Configuration(
                        "S3 key prefix is empty instead of absent".to_string(),
                    ));
                }
                match endpoint {
                    S3EndpointBinding::Aws { partition } => present("AWS partition", partition),
                    S3EndpointBinding::Custom { origin } => {
                        let canonical = super::provider::canonical_custom_s3_origin(origin)?;
                        if canonical != *origin {
                            return Err(StorageError::Configuration(
                                "custom S3 origin is not canonical".to_string(),
                            ));
                        }
                        Ok(())
                    }
                }
            }
            Self::GoogleDrive { corpus } => match corpus {
                GoogleDriveCorpus::MyDrive { folder_id } => {
                    present("Google Drive folder id", folder_id)
                }
                GoogleDriveCorpus::SharedDrive {
                    drive_id,
                    folder_id,
                } => {
                    present("Google Drive id", drive_id)?;
                    present("Google Drive folder id", folder_id)
                }
            },
            Self::Dropbox { namespace_id } => present("Dropbox namespace id", namespace_id),
            Self::OneDrive {
                drive_id,
                folder_id,
            } => {
                present("OneDrive drive id", drive_id)?;
                present("OneDrive folder id", folder_id)
            }
            Self::CloudKit {
                container_id,
                owner_name,
                zone_name,
                ..
            } => {
                present("CloudKit container id", container_id)?;
                present("CloudKit owner name", owner_name)?;
                present("CloudKit zone name", zone_name)
            }
        }
    }
}

impl ProviderDeviceBinding {
    pub fn validate_for(&self, store: &StoreProviderBinding) -> Result<(), StorageError> {
        fn present(label: &str, value: &str) -> Result<(), StorageError> {
            if value.is_empty() {
                Err(StorageError::Configuration(format!("{label} is empty")))
            } else {
                Ok(())
            }
        }

        let compatible = matches!(
            (store, &self.principal),
            (
                StoreProviderBinding::S3 {
                    endpoint: S3EndpointBinding::Aws { .. },
                    ..
                },
                ProviderPrincipalId::Aws { .. }
            ) | (
                StoreProviderBinding::S3 {
                    endpoint: S3EndpointBinding::Custom { .. },
                    ..
                },
                ProviderPrincipalId::CustomS3Credential { .. }
            ) | (
                StoreProviderBinding::GoogleDrive { .. },
                ProviderPrincipalId::GoogleDrive { .. }
            ) | (
                StoreProviderBinding::Dropbox { .. },
                ProviderPrincipalId::Dropbox { .. }
            ) | (
                StoreProviderBinding::OneDrive { .. },
                ProviderPrincipalId::OneDrive { .. }
            ) | (
                StoreProviderBinding::CloudKit { .. },
                ProviderPrincipalId::CloudKitPrivateZoneOwner { .. }
                    | ProviderPrincipalId::CloudKitSharedZoneParticipant { .. }
            )
        );
        if !compatible {
            return Err(StorageError::Configuration(
                "provider principal is incompatible with the Store provider binding".to_string(),
            ));
        }
        match &self.principal {
            ProviderPrincipalId::Aws {
                account_id,
                principal,
            } => {
                if account_id.len() != 12 || !account_id.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(StorageError::Configuration(
                        "AWS account id must contain exactly 12 decimal digits".to_string(),
                    ));
                }
                match principal {
                    AwsPrincipal::Root => Ok(()),
                    AwsPrincipal::User { arn, user_id } => {
                        present("AWS user id", user_id)?;
                        let fields: Vec<_> = arn.splitn(6, ':').collect();
                        let StoreProviderBinding::S3 {
                            endpoint: S3EndpointBinding::Aws { partition },
                            ..
                        } = store
                        else {
                            return Err(StorageError::Configuration(
                                "AWS user principal is bound to non-AWS S3".to_string(),
                            ));
                        };
                        if fields.len() != 6
                            || fields[0] != "arn"
                            || fields[1] != partition
                            || fields[2] != "iam"
                            || !fields[3].is_empty()
                            || fields[4] != account_id
                            || !fields[5].starts_with("user/")
                            || fields[5].len() == "user/".len()
                        {
                            return Err(StorageError::Configuration(
                                "AWS IAM user ARN is malformed or differs from its Store binding"
                                    .to_string(),
                            ));
                        }
                        Ok(())
                    }
                    AwsPrincipal::Role { role_id } => {
                        present("AWS role id", role_id)?;
                        if role_id.contains(':') {
                            return Err(StorageError::Configuration(
                                "AWS role id must be the stable prefix before the session separator"
                                    .to_string(),
                            ));
                        }
                        Ok(())
                    }
                }
            }
            ProviderPrincipalId::CustomS3Credential { .. } => Ok(()),
            ProviderPrincipalId::GoogleDrive { permission_id } => {
                present("Google Drive permission id", permission_id)
            }
            ProviderPrincipalId::Dropbox { account_id } => {
                present("Dropbox account id", account_id)
            }
            ProviderPrincipalId::OneDrive { user_id } => present("OneDrive user id", user_id),
            ProviderPrincipalId::CloudKitPrivateZoneOwner { record_name } => {
                present("CloudKit private-zone owner record name", record_name)
            }
            ProviderPrincipalId::CloudKitSharedZoneParticipant { record_name } => {
                present("CloudKit shared-zone participant record name", record_name)
            }
        }
    }
}

impl ResolvedProviderBinding {
    pub fn validate(&self) -> Result<(), StorageError> {
        self.store.validate()?;
        self.device.validate_for(&self.store)
    }
}

/// Exact stored representation of one immutable object.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ExactObjectRef {
    slot: ObjectSlot,
    stored_size: u64,
    stored_hash: ObjectHash,
}

impl ExactObjectRef {
    pub fn new(slot: ObjectSlot, stored_size: u64, stored_hash: ObjectHash) -> Self {
        Self {
            slot,
            stored_size,
            stored_hash,
        }
    }

    pub fn slot(&self) -> &ObjectSlot {
        &self.slot
    }

    pub fn stored_size(&self) -> u64 {
        self.stored_size
    }

    pub fn stored_hash(&self) -> ObjectHash {
        self.stored_hash
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<(), StorageError> {
        if bytes.len() as u64 != self.stored_size || ObjectHash::digest(bytes) != self.stored_hash {
            return Err(StorageError::InvalidContent(format!(
                "exact object {} does not match stored size/hash",
                self.slot.logical_key()
            )));
        }
        Ok(())
    }
}

/// Immutable stored bytes and the exact reference derived from them.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedExactObject {
    reference: ExactObjectRef,
    stored_bytes: Vec<u8>,
}

impl<'de> serde::Deserialize<'de> for PreparedExactObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            reference: ExactObjectRef,
            stored_bytes: Vec<u8>,
        }

        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.reference, fields.stored_bytes).map_err(serde::de::Error::custom)
    }
}

impl PreparedExactObject {
    pub fn new(reference: ExactObjectRef, stored_bytes: Vec<u8>) -> Result<Self, StorageError> {
        reference.verify(&stored_bytes)?;
        Ok(Self {
            reference,
            stored_bytes,
        })
    }

    pub fn reference(&self) -> &ExactObjectRef {
        &self.reference
    }

    pub fn stored_bytes(&self) -> &[u8] {
        &self.stored_bytes
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoordinationError {
    #[error("serial coordination is unavailable: {0}")]
    Unavailable(String),
    #[error("coordination head not found: {0}")]
    NotFound(String),
    #[error("coordination storage failed: {0}")]
    Storage(String),
    #[error("coordination object could not be opened: {0}")]
    Open(String),
}

#[derive(Debug, thiserror::Error)]
pub enum CreateHeadError {
    #[error("coordination head already exists")]
    AlreadyExists,
    #[error(transparent)]
    Coordination(#[from] CoordinationError),
}

#[derive(Debug, thiserror::Error)]
pub enum ReplaceHeadError {
    #[error("coordination head version no longer matches")]
    VersionMismatch,
    #[error(transparent)]
    Coordination(#[from] CoordinationError),
}

/// Mandatory compare-and-swap operations exposed only by eligible adapters.
#[async_trait]
pub trait CoordinationStorage: Send + Sync {
    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, CoordinationError>;

    async fn read_head(&self, key: &str) -> Result<VersionedObject, CoordinationError>;

    async fn create_head(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<VersionedObject, CreateHeadError>;

    async fn replace_head(
        &self,
        key: &str,
        expected: &VersionToken,
        bytes: &[u8],
    ) -> Result<VersionedObject, ReplaceHeadError>;

    async fn delete_head(&self, key: &str) -> Result<(), CoordinationError>;
}

/// Error type for storage operations.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    #[error("storage operation failed: {0}")]
    Storage(String),
    #[error("{operation}; storage cleanup failed: {cleanup}")]
    CleanupFailed {
        #[source]
        operation: Box<StorageError>,
        cleanup: Box<StorageError>,
    },
    #[error("{operation}; exact response-loss readback failed: {readback}")]
    UnresolvedOutcome {
        #[source]
        operation: Box<StorageError>,
        readback: Box<StorageError>,
    },
    #[error("storage configuration is invalid: {0}")]
    Configuration(String),
    #[error("storage object parse failed: {0}")]
    Parse(String),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("storage object already exists: {0}")]
    AlreadyExists(String),
    #[error("reserved storage slot contains different bytes: {0}")]
    SlotCollision(String),
    #[error("decryption failed: {0}")]
    Decryption(String),
    #[error("remote blob content is invalid: {0}")]
    InvalidContent(String),
    #[error("local blob filesystem failed: {0}")]
    LocalFilesystem(String),
    /// This device has not adopted a store-key rotation the cloud already
    /// committed; see [`crate::sync::cloud_storage::RotationPending`].
    #[error("{0}")]
    RotationPending(#[from] crate::sync::cloud_storage::RotationPending),
}

impl From<crate::storage::cloud::CloudHomeError> for StorageError {
    fn from(e: crate::storage::cloud::CloudHomeError) -> Self {
        match e {
            crate::storage::cloud::CloudHomeError::NotFound(key) => StorageError::NotFound(key),
            crate::storage::cloud::CloudHomeError::AlreadyExists(key) => {
                StorageError::AlreadyExists(key)
            }
            crate::storage::cloud::CloudHomeError::Configuration(msg) => {
                StorageError::Configuration(msg)
            }
            crate::storage::cloud::CloudHomeError::Transport(msg) => StorageError::Storage(msg),
            crate::storage::cloud::CloudHomeError::CleanupFailed { operation, cleanup } => {
                StorageError::CleanupFailed {
                    operation: Box::new(StorageError::from(*operation)),
                    cleanup: Box::new(StorageError::from(*cleanup)),
                }
            }
            crate::storage::cloud::CloudHomeError::UnresolvedOutcome {
                operation,
                readback,
            } => StorageError::UnresolvedOutcome {
                operation: Box::new(StorageError::from(*operation)),
                readback: Box::new(StorageError::from(*readback)),
            },
            crate::storage::cloud::CloudHomeError::Io(io_err) => {
                StorageError::Storage(format!("I/O error: {io_err}"))
            }
        }
    }
}

impl StorageError {
    pub fn is_transport(&self) -> bool {
        match self {
            Self::Storage(_) => true,
            Self::CleanupFailed { operation, .. } | Self::UnresolvedOutcome { operation, .. } => {
                operation.is_transport()
            }
            _ => false,
        }
    }

    pub(crate) fn definitely_uncommitted(&self) -> bool {
        !self.is_transport()
    }

    pub fn cleanup_causes(&self) -> Option<(&StorageError, &StorageError)> {
        match self {
            Self::CleanupFailed { operation, cleanup } => Some((operation, cleanup)),
            _ => None,
        }
    }
}

impl From<crate::store_dir::PathTokenError> for StorageError {
    /// A blob id/namespace/cloud_path that can't form a safe object key is bad
    /// data, surfaced so the caller refuses the blob rather than reaching storage
    /// with a key that could escape its prefix.
    fn from(e: crate::store_dir::PathTokenError) -> Self {
        StorageError::Parse(format!("unsafe blob path: {e}"))
    }
}

#[async_trait]
pub trait SyncStorage: Send + Sync {
    /// Return the cloud home's fixed Store blob opening protection. Circle blobs
    /// use their exact activated Circle key instead.
    fn store_blob_protection(&self) -> Result<BlobSpoolProtection, StorageError>;

    /// Resolve the provider corpus and authenticated principal used by this
    /// adapter. Registrations bind the principal before allocating descendants.
    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, StorageError>;

    /// Reserve the exact provider slot for a protocol object.
    async fn allocate_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<ObjectSlot, StorageError>;

    /// Seal canonical protocol bytes once and bind their exact stored size/hash.
    fn prepare_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        slot: ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<PreparedExactObject, StorageError>;

    /// Create the prepared bytes at their reserved slot, settling lost responses
    /// by exact readback and refusing different bytes at an occupied slot.
    async fn create_protocol_object(
        &self,
        prepared: &PreparedExactObject,
    ) -> Result<(), StorageError>;

    /// Read and open one exact Store protocol object using the signed
    /// semantic prefix as encryption AAD.
    async fn read_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError>;

    /// Read one predecessor-reserved successor slot and return both its opened
    /// bytes and the completed exact reference derived from the stored bytes.
    async fn read_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, ExactObjectRef), StorageError>;

    /// Read one predecessor-reserved successor slot while retaining its exact
    /// stored representation for a durable retry journal.
    async fn read_prepared_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, PreparedExactObject), StorageError>;

    /// Delete one exact Store protocol object and verify absence.
    async fn delete_protocol_object(&self, object: &ExactObjectRef) -> Result<(), StorageError>;

    /// Reserve the exact provider slot for a stored blob body.
    async fn allocate_blob_slot(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
    ) -> Result<ObjectSlot, StorageError>;

    /// Verify one plaintext source against its locator and write the exact stored
    /// representation to an atomically committed, directory-synced spool file.
    async fn seal_blob_to_spool(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        protection: BlobSpoolProtection,
        plaintext_file: &Path,
        spool_file: &Path,
    ) -> Result<(), StorageError>;

    /// Derive an exact reference from an immutable stored blob file.
    async fn prepare_blob_object(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        slot: ObjectSlot,
        stored_file: &Path,
    ) -> Result<crate::blob::locator::StoredBlobRef, StorageError>;

    /// Create the exact stored blob body from its immutable local file.
    async fn create_blob_object_from_file(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        authority: &BlobWriteAuthority<'_>,
        stored_file: &Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), StorageError>;

    /// Read one exact stored blob body and verify its signed size/hash reference.
    async fn verify_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError>;

    /// Read and verify one exact stored blob body into an unpublished sibling.
    /// The caller commits it with overwrite semantics for coven-owned paths or
    /// no-replace semantics for user-owned destinations.
    async fn stage_exact_blob_download(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        dest: &Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, StorageError>;

    /// Download and exact-verify the stored object, open it under the
    /// audience-owned protection, and return an unpublished plaintext file only
    /// after its locator size and hash have also been verified.
    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
        dest: &Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, StorageError>;

    /// Delete one exact stored blob body.
    async fn delete_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError>;

    /// Upload a wrapped store key that `owner_pubkey` sealed for `recipient_pubkey`.
    /// Writes to `keys/{owner_pubkey_hex}/{recipient_pubkey_hex}{suffix}`. An owner
    /// wraps only into its own prefix, so a recipient can hold a wrap from each
    /// owner and no two owners contend for one slot. The bytes are already a sealed
    /// box, so the home cipher stores them verbatim regardless of suffix.
    async fn put_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Download the wrapped store key `owner_pubkey` sealed for `recipient_pubkey`.
    /// Reads from `keys/{owner_pubkey_hex}/{recipient_pubkey_hex}{suffix}`.
    /// `create_invitation` reads the inviting owner's existing slot for the invitee
    /// before overwriting it, so a failed invite can restore the exact prior object
    /// rather than stripping a re-invited member's wrapped key. Returns `NotFound`
    /// when that owner has no wrapped key for the recipient yet.
    async fn get_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<Vec<u8>, StorageError>;

    /// Delete the wrapped store key `owner_pubkey` sealed for `recipient_pubkey`.
    /// Removes `keys/{owner_pubkey_hex}/{recipient_pubkey_hex}{suffix}`. An owner
    /// can delete only wraps in its own prefix; a revoked member's wraps under
    /// other owners' prefixes are pre-rotation (they wrap a key the member already
    /// held) and are reclaimed when those owners next rotate.
    async fn delete_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DomainPathCase {
        domain: ProtectedObjectDomain,
        valid: &'static [&'static str],
        cross_domain: &'static str,
    }

    fn validates(domain: ProtectedObjectDomain, path: &str) -> bool {
        ProtocolObjectContext {
            store_root_hash: ObjectHash::digest(b"protocol path grammar"),
            domain,
            protection: ProtocolObjectProtection::Store,
        }
        .validate_path(path)
        .is_ok()
    }

    #[test]
    fn every_protocol_domain_requires_its_exact_path_grammar() {
        let cases = [
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreProtocolRoot,
                valid: &["store-v1/store-protocol-root"],
                cross_domain: "store-v1/heads/device/1",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreCommit,
                valid: &["store-v1/candidates/family/commits/device/1/hash"],
                cross_domain: "store-v1/candidates/family/packages/device/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreHead,
                valid: &["store-v1/heads/device/1"],
                cross_domain: "store-v1/acks/device/1",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreAck,
                valid: &["store-v1/acks/device/1"],
                cross_domain: "store-v1/heads/device/1",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreDeviceRegistration,
                valid: &[
                    "store-v1/devices/device",
                    "store-v1/devices/founder/creation/registration",
                ],
                cross_domain: "store-v1/device-join-attempts/attempt",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreDeviceSelfRetirement,
                valid: &["store-v1/candidates/family/device-self-retirements/device/hash"],
                cross_domain: "store-v1/candidates/family/packages/device/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::DeviceJoinAttempt,
                valid: &["store-v1/device-join-attempts/attempt"],
                cross_domain: "store-v1/device-join-outcomes/attempt",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::DeviceJoinOutcome,
                valid: &["store-v1/device-join-outcomes/attempt"],
                cross_domain: "store-v1/device-join-attempts/attempt",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::DeviceJoinAbandonment,
                valid: &["store-v1/device-join-attempts/attempt"],
                cross_domain: "store-v1/device-join-outcomes/attempt",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::DeviceJoinCleanupReceipt,
                valid: &["store-v1/device-join-cleanup-receipts/attempt"],
                cross_domain: "store-v1/device-join-attempts/attempt",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::ProviderAccessGrant,
                valid: &["store-v1/provider-access/grants/grant"],
                cross_domain: "store-v1/provider-access/withdrawals/grant",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::ProviderAccessWithdrawal,
                valid: &["store-v1/provider-access/withdrawals/grant"],
                cross_domain: "store-v1/provider-access/grants/grant",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::OwnerRecoveryNode,
                valid: &["store-v1/recovery/owner/grant/1"],
                cross_domain: "store-v1/snapshots/owner/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreSnapshotMeta,
                valid: &["store-v1/snapshots/owner/hash"],
                cross_domain: "store-v1/snapshot-images/owner/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreSnapshotImage,
                valid: &["store-v1/snapshot-images/owner/hash"],
                cross_domain: "store-v1/snapshots/owner/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreMembershipEntry,
                valid: &["store-v1/membership/entries/owner/grant/stream/1/hash"],
                cross_domain: "store-v1/membership/heads/owner/grant/stream/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreMembershipHead,
                valid: &[
                    "store-v1/membership/heads/owner/grant/stream/1",
                    "store-v1/membership/heads/founder/creation/1",
                ],
                cross_domain: "store-v1/membership/entries/owner/grant/stream/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StoreMembershipResolution,
                valid: &["store-v1/membership/resolutions/conflict/resolver/hash"],
                cross_domain: "store-v1/membership/entries/owner/grant/stream/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::StorePackage,
                valid: &["store-v1/candidates/family/packages/device/1/hash"],
                cross_domain: "store-v1/candidates/family/commits/device/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::CircleControl,
                valid: &[
                    "circle-control/circle/merge/entries/owner/device/grant/stream/1/hash",
                    "circle-control/circle/merge/heads/owner/device/grant/stream/1",
                    "circle-control/circle/serial/owner/1/hash",
                ],
                cross_domain: "circles/circle/roster/entries/owner/device/grant/stream/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::CircleRoster,
                valid: &[
                    "circles/circle/roster/entries/owner/device/grant/stream/1/hash",
                    "circles/circle/roster/heads/owner/device/grant/stream/1",
                ],
                cross_domain: "circles/circle/roster/resolutions/conflict/resolver/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::CircleRosterResolution,
                valid: &["circles/circle/roster/resolutions/conflict/resolver/hash"],
                cross_domain: "circles/circle/roster/entries/owner/device/grant/stream/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::CircleMetadata,
                valid: &[
                    "circles/circle/metadata/entries/owner/device/grant/stream/1/hash",
                    "circles/circle/metadata/heads/owner/device/grant/stream/1",
                ],
                cross_domain: "circles/circle/roster/entries/owner/device/grant/stream/1/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::CirclePackage,
                valid: &["circles/circle/candidates/family/packages/device/1/hash"],
                cross_domain:
                    "circles/circle/candidates/family/access-envelopes/owner/recipient/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::CircleAccessLeaf,
                valid: &[
                    "circles/circle/candidates/family/access-leaves/owner/epoch/recipient/leaf",
                ],
                cross_domain:
                    "circles/circle/candidates/family/access-envelopes/owner/recipient/hash",
            },
            DomainPathCase {
                domain: ProtectedObjectDomain::CircleAccessEnvelope,
                valid: &["circles/circle/candidates/family/access-envelopes/owner/recipient/hash"],
                cross_domain:
                    "circles/circle/candidates/family/access-leaves/owner/epoch/recipient/leaf",
            },
        ];

        for case in cases {
            for valid in case.valid {
                assert!(
                    validates(case.domain, valid),
                    "{:?} rejected {valid}",
                    case.domain
                );
                assert!(
                    !validates(case.domain, &format!("{valid}/extra")),
                    "{:?} accepted an extra component after {valid}",
                    case.domain,
                );
                let (missing, _) = valid
                    .rsplit_once('/')
                    .expect("protocol paths have more than one component");
                assert!(
                    !validates(case.domain, missing),
                    "{:?} accepted a missing component in {valid}",
                    case.domain,
                );
            }
            assert!(
                !validates(case.domain, case.cross_domain),
                "{:?} accepted cross-domain path {}",
                case.domain,
                case.cross_domain,
            );
        }
        assert!(!validates(
            ProtectedObjectDomain::StoreDeviceRegistration,
            "store-v1/devices/founder",
        ));
        assert!(!validates(
            ProtectedObjectDomain::StoreMembershipHead,
            "store-v1/membership/heads/founder/creation/1/extra",
        ));
    }

    #[test]
    fn candidate_protocol_domains_reject_reordered_and_nested_components() {
        let cases = [
            (
                ProtectedObjectDomain::StoreCommit,
                "store-v1/candidates/family/commits/device/1/hash",
                1,
                3,
            ),
            (
                ProtectedObjectDomain::StoreDeviceSelfRetirement,
                "store-v1/candidates/family/device-self-retirements/device/hash",
                1,
                3,
            ),
            (
                ProtectedObjectDomain::StorePackage,
                "store-v1/candidates/family/packages/device/1/hash",
                1,
                3,
            ),
            (
                ProtectedObjectDomain::CirclePackage,
                "circles/circle/candidates/family/packages/device/1/hash",
                2,
                4,
            ),
            (
                ProtectedObjectDomain::CircleAccessLeaf,
                "circles/circle/candidates/family/access-leaves/owner/epoch/recipient/leaf",
                2,
                4,
            ),
            (
                ProtectedObjectDomain::CircleAccessEnvelope,
                "circles/circle/candidates/family/access-envelopes/owner/recipient/hash",
                2,
                4,
            ),
        ];

        for (domain, valid, candidates_index, kind_index) in cases {
            let components = valid.split('/').collect::<Vec<_>>();
            let mut reordered = components.clone();
            reordered.swap(candidates_index, candidates_index + 1);
            let mut nested = components.clone();
            nested[kind_index] = "nested";
            nested[kind_index + 1] = components[kind_index];
            let mut repeated_kind = components.clone();
            repeated_kind[kind_index + 1] = components[kind_index];
            let mut empty_family = components.clone();
            empty_family[candidates_index + 1] = "";
            for malformed in [reordered, nested, repeated_kind, empty_family] {
                let malformed = malformed.join("/");
                assert!(
                    !validates(domain, &malformed),
                    "{domain:?} accepted malformed path {malformed}",
                );
            }
        }
    }
}
