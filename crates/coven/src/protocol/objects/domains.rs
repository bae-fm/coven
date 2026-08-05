/// Signed object kind bound into protection AAD and checked against the
/// semantic path before storage I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtectedObjectDomain {
    StoreProtocolRoot,
    StoreCommit,
    StoreHead,
    StoreAck,
    StoreDeviceRegistration,
    DeviceJoinAttempt,
    DeviceJoinOutcome,
    DeviceJoinAbandonment,
    DeviceJoinCleanupReceipt,
    DeviceJoinTransport,
    StoreDeviceExclusionProposal,
    StoreDeviceExclusionOutcome,
    StoreReclaimEvidence,
    StoreReclaimAuthorization,
    StoreReclaimReceipt,
    ProviderAccessGrant,
    OwnerRecoveryNode,
    StoreSnapshotMeta,
    StoreSnapshotImage,
    StoreMembershipEntry,
    StoreMembershipHead,
    StoreMembershipResolution,
    StoreWrappedKey,
    StorePackage,
    CircleControl,
    CircleRoster,
    CircleRosterResolution,
    CircleMetadata,
    CirclePackage,
    CircleBootstrapImage,
    CircleEpochCloseIntent,
    CircleEpochCloseOutcome,
    CircleEpochCloseResponse,
    CircleAccessLeaf,
    CircleAccessEnvelope,
    CircleAcknowledgement,
    CircleSnapshotMeta,
    CircleSnapshotImage,
}

#[derive(Clone, Copy)]
pub(super) struct ProtocolObjectMetadata {
    pub(super) aad_label: &'static [u8],
    pub(super) path: ProtocolPathRule,
    pub(super) extension: &'static str,
}

#[derive(Clone, Copy)]
pub(super) enum ProtocolPathRule {
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
pub(super) struct ExactPathShape {
    component_count: usize,
    fixed_components: &'static [(usize, &'static str)],
}

impl ProtocolPathRule {
    pub(super) fn accepts(self, semantic_prefix: &str) -> bool {
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
    pub(super) fn metadata(self) -> ProtocolObjectMetadata {
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
            Self::DeviceJoinTransport => ProtocolObjectMetadata {
                aad_label: b"device-join-transport",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "device-join-transport")],
                }]),
                extension: ".json",
            },
            Self::StoreDeviceExclusionProposal => ProtocolObjectMetadata {
                aad_label: b"store-device-exclusion-proposal",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 5,
                    fixed_components: &[(0, "store-v1"), (1, "device-exclusion-proposals")],
                }]),
                extension: ".json",
            },
            Self::StoreDeviceExclusionOutcome => ProtocolObjectMetadata {
                aad_label: b"store-device-exclusion-outcome",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "device-exclusion-outcomes")],
                }]),
                extension: ".json",
            },
            Self::StoreReclaimEvidence => ProtocolObjectMetadata {
                aad_label: b"store-reclaim-evidence",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "reclaim"), (2, "evidence")],
                }]),
                extension: ".json",
            },
            Self::StoreReclaimAuthorization => ProtocolObjectMetadata {
                aad_label: b"store-reclaim-authorization",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "reclaim"), (2, "authorizations")],
                }]),
                extension: ".json",
            },
            Self::StoreReclaimReceipt => ProtocolObjectMetadata {
                aad_label: b"store-reclaim-receipt",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 4,
                    fixed_components: &[(0, "store-v1"), (1, "reclaim"), (2, "receipts")],
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
            Self::StoreWrappedKey => ProtocolObjectMetadata {
                aad_label: b"store-wrapped-key",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 5,
                    fixed_components: &[(0, "keys")],
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
            Self::CircleBootstrapImage => ProtocolObjectMetadata {
                aad_label: b"circle-bootstrap-image",
                path: ProtocolPathRule::CircleCandidate {
                    kind: "bootstraps",
                    component_count: 9,
                },
                extension: ".db",
            },
            Self::CircleEpochCloseIntent => ProtocolObjectMetadata {
                aad_label: b"circle-epoch-close-intent",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 6,
                    fixed_components: &[(0, "circles"), (2, "epoch-close"), (4, "intent")],
                }]),
                extension: ".json",
            },
            Self::CircleEpochCloseOutcome => ProtocolObjectMetadata {
                aad_label: b"circle-epoch-close-outcome",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 5,
                    fixed_components: &[(0, "circles"), (2, "epoch-close"), (4, "outcome")],
                }]),
                extension: ".json",
            },
            Self::CircleEpochCloseResponse => ProtocolObjectMetadata {
                aad_label: b"circle-epoch-close-response",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 6,
                    fixed_components: &[(0, "circles"), (2, "epoch-close"), (4, "responses")],
                }]),
                extension: ".json",
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
            Self::CircleAcknowledgement => ProtocolObjectMetadata {
                aad_label: b"circle-acknowledgement",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 5,
                    fixed_components: &[(0, "circles"), (2, "acks")],
                }]),
                extension: ".json",
            },
            Self::CircleSnapshotMeta => ProtocolObjectMetadata {
                aad_label: b"circle-snapshot-meta",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 5,
                    fixed_components: &[(0, "circles"), (2, "snapshots")],
                }]),
                extension: ".json",
            },
            Self::CircleSnapshotImage => ProtocolObjectMetadata {
                aad_label: b"circle-snapshot-image",
                path: ProtocolPathRule::Exact(&[ExactPathShape {
                    component_count: 5,
                    fixed_components: &[(0, "circles"), (2, "snapshot-images")],
                }]),
                extension: ".db",
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
pub(crate) struct StoreEncryptedProtocolObjectDomain(pub(super) ProtectedObjectDomain);

/// A signed Store control-plane domain whose bytes must remain readable before
/// the reader has adopted the Store data key named by those bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SignedStoreProtocolObjectDomain(pub(super) ProtectedObjectDomain);

/// A domain protected by a Circle epoch key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CircleProtocolObjectDomain(pub(super) ProtectedObjectDomain);

/// A domain whose canonical bytes already carry recipient-specific encryption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecipientSealedProtocolObjectDomain(pub(super) ProtectedObjectDomain);

/// Typed protocol-object domain names. Each name's value carries the only
/// protection class its object kind permits.
pub(crate) struct ProtocolObjectDomain;

#[allow(non_upper_case_globals)]
impl ProtocolObjectDomain {
    pub(crate) const StoreProtocolRoot: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreProtocolRoot);
    pub(crate) const StoreCommit: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreCommit);
    pub(crate) const StoreHead: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreHead);
    pub(crate) const StoreAck: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreAck);
    pub(crate) const StoreDeviceRegistration: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreDeviceRegistration);
    pub(crate) const DeviceJoinAttempt: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinAttempt);
    pub(crate) const DeviceJoinOutcome: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinOutcome);
    pub(crate) const DeviceJoinAbandonment: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinAbandonment);
    pub(crate) const DeviceJoinCleanupReceipt: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinCleanupReceipt);
    /// Device-join artifacts in transit. The bytes carry their own per-attempt
    /// seal, so the storage layer stores them as it received them.
    pub(crate) const DeviceJoinTransport: RecipientSealedProtocolObjectDomain =
        RecipientSealedProtocolObjectDomain(ProtectedObjectDomain::DeviceJoinTransport);
    pub(crate) const StoreDeviceExclusionProposal: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreDeviceExclusionProposal);
    pub(crate) const StoreDeviceExclusionOutcome: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreDeviceExclusionOutcome);
    pub(crate) const StoreReclaimEvidence: StoreEncryptedProtocolObjectDomain =
        StoreEncryptedProtocolObjectDomain(ProtectedObjectDomain::StoreReclaimEvidence);
    pub(crate) const StoreReclaimAuthorization: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreReclaimAuthorization);
    pub(crate) const StoreReclaimReceipt: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreReclaimReceipt);
    pub(crate) const ProviderAccessGrant: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::ProviderAccessGrant);
    pub(crate) const OwnerRecoveryNode: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::OwnerRecoveryNode);
    pub(crate) const StoreSnapshotMeta: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreSnapshotMeta);
    pub(crate) const StoreSnapshotImage: StoreEncryptedProtocolObjectDomain =
        StoreEncryptedProtocolObjectDomain(ProtectedObjectDomain::StoreSnapshotImage);
    pub(crate) const StoreMembershipEntry: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreMembershipEntry);
    pub(crate) const StoreMembershipHead: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreMembershipHead);
    pub(crate) const StoreMembershipResolution: SignedStoreProtocolObjectDomain =
        SignedStoreProtocolObjectDomain(ProtectedObjectDomain::StoreMembershipResolution);
    pub(crate) const StoreWrappedKey: RecipientSealedProtocolObjectDomain =
        RecipientSealedProtocolObjectDomain(ProtectedObjectDomain::StoreWrappedKey);
    pub(crate) const CircleAccessLeaf: RecipientSealedProtocolObjectDomain =
        RecipientSealedProtocolObjectDomain(ProtectedObjectDomain::CircleAccessLeaf);
    pub(crate) const StorePackage: StoreEncryptedProtocolObjectDomain =
        StoreEncryptedProtocolObjectDomain(ProtectedObjectDomain::StorePackage);
    pub(crate) const CircleControl: StoreEncryptedProtocolObjectDomain =
        StoreEncryptedProtocolObjectDomain(ProtectedObjectDomain::CircleControl);
    pub(crate) const CircleAccessEnvelope: StoreEncryptedProtocolObjectDomain =
        StoreEncryptedProtocolObjectDomain(ProtectedObjectDomain::CircleAccessEnvelope);
    pub(crate) const CircleRoster: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleRoster);
    pub(crate) const CircleRosterResolution: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleRosterResolution);
    pub(crate) const CircleMetadata: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleMetadata);
    pub(crate) const CirclePackage: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CirclePackage);
    pub(crate) const CircleBootstrapImage: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleBootstrapImage);
    pub(crate) const CircleEpochCloseIntent: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleEpochCloseIntent);
    pub(crate) const CircleEpochCloseOutcome: StoreEncryptedProtocolObjectDomain =
        StoreEncryptedProtocolObjectDomain(ProtectedObjectDomain::CircleEpochCloseOutcome);
    pub(crate) const CircleEpochCloseResponse: StoreEncryptedProtocolObjectDomain =
        StoreEncryptedProtocolObjectDomain(ProtectedObjectDomain::CircleEpochCloseResponse);
    pub(crate) const CircleAcknowledgement: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleAcknowledgement);
    pub(crate) const CircleSnapshotMeta: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleSnapshotMeta);
    pub(crate) const CircleSnapshotImage: CircleProtocolObjectDomain =
        CircleProtocolObjectDomain(ProtectedObjectDomain::CircleSnapshotImage);
}
