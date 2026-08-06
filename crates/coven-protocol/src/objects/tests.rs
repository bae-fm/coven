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
        protection: ProtocolObjectProtection::StoreEncrypted,
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
            domain: ProtectedObjectDomain::StoreDeviceExclusionProposal,
            valid: &["store-v1/device-exclusion-proposals/device/proposal/hash"],
            cross_domain: "store-v1/device-exclusion-outcomes/device/proposal",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::StoreDeviceExclusionOutcome,
            valid: &["store-v1/device-exclusion-outcomes/device/proposal"],
            cross_domain: "store-v1/device-exclusion-proposals/device/proposal/hash",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::StoreReclaimEvidence,
            valid: &["store-v1/reclaim/evidence/hash"],
            cross_domain: "store-v1/reclaim/authorizations/hash",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::StoreReclaimAuthorization,
            valid: &["store-v1/reclaim/authorizations/hash"],
            cross_domain: "store-v1/reclaim/evidence/hash",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::StoreReclaimReceipt,
            valid: &["store-v1/reclaim/receipts/hash"],
            cross_domain: "store-v1/reclaim/authorizations/hash",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::ProviderAccessGrant,
            valid: &["store-v1/provider-access/grants/grant"],
            cross_domain: "store-v1/provider-access/withdrawals/grant",
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
            domain: ProtectedObjectDomain::StoreWrappedKey,
            valid: &["keys/owner/recipient/1/hash"],
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
            cross_domain: "circles/circle/candidates/family/access-envelopes/owner/recipient/hash",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::CircleBootstrapImage,
            valid: &["circles/circle/candidates/family/bootstraps/owner/epoch/recipient/hash"],
            cross_domain: "circles/circle/candidates/family/packages/device/1/hash",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::CircleEpochCloseIntent,
            valid: &["circles/circle/epoch-close/close/intent/hash"],
            cross_domain: "circles/circle/epoch-close/close/outcome",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::CircleEpochCloseOutcome,
            valid: &["circles/circle/epoch-close/close/outcome"],
            cross_domain: "circles/circle/epoch-close/close/responses/device",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::CircleEpochCloseResponse,
            valid: &["circles/circle/epoch-close/close/responses/device"],
            cross_domain: "circles/circle/epoch-close/close/outcome",
        },
        DomainPathCase {
            domain: ProtectedObjectDomain::CircleAccessLeaf,
            valid: &["circles/circle/candidates/family/access-leaves/owner/epoch/recipient/leaf"],
            cross_domain: "circles/circle/candidates/family/access-envelopes/owner/recipient/hash",
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
            ProtectedObjectDomain::CircleBootstrapImage,
            "circles/circle/candidates/family/bootstraps/owner/epoch/recipient/hash",
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
