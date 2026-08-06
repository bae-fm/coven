//! Signed, hash-addressed Store commit protocol objects.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::membership::{
    verify_membership_entry, AuthorHead, AuthorStreamId, MembershipChange, MembershipCoord,
    MembershipEntry, MembershipEntryRef, MembershipGrantCreationAuthority, MembershipGrantId,
    MembershipHeadRef, StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use crate::circle::{
    AccessLeafId, CircleBootstrapCoverageRef, CircleBootstrapRef, CircleControlCoord,
    CircleEpochId, CircleId, CircleMetadataCoord, CircleMetadataHeadRef,
    CircleRosterConflictResolutionRef, CircleRosterCoord, CircleRosterHeadRef,
};
use crate::circle_control::StoreMembershipStateRef;
use crate::objects::ObjectSlot;
use crate::objects::{ExactObjectRef, ProviderDeviceBinding};
use crate::write::WriteId;
use coven_keys::encryption::KeyFingerprint;
use coven_keys::keys::{self, UserKeypair};

mod ack_snapshot;
mod batch_commit;
mod circle_ack;
mod circle_snapshot;
mod device_join;
pub mod device_join_exchange;
pub mod device_join_journal;
mod device_state;
mod heads;
mod identifiers;
mod operation_refs;
mod packages;
mod protocol_root;
mod registration;
mod retained_history;
mod signed;
mod validation;

pub use ack_snapshot::*;
pub use batch_commit::*;
pub use circle_ack::*;
pub use circle_snapshot::*;
pub use device_join::*;
pub use device_state::*;
pub use heads::*;
pub use identifiers::*;
pub use packages::*;
pub use protocol_root::*;
pub use registration::*;
pub use retained_history::*;
pub use signed::{Signed, SignedBody};
pub use validation::*;

mod ordered_map_entries {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Ord + Serialize,
        V: Serialize,
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: Ord + Deserialize<'de>,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let entries = Vec::<(K, V)>::deserialize(deserializer)?;
        let entry_count = entries.len();
        let map = entries.into_iter().collect::<BTreeMap<_, _>>();
        if map.len() != entry_count {
            return Err(serde::de::Error::custom(
                "ordered map entries contain a duplicate key",
            ));
        }
        Ok(map)
    }
}

pub const STORE_PROTOCOL_VERSION: u32 = 1;

pub(crate) const STORE_PROTOCOL_ROOT_SEMANTIC_PATH: &str = "store-v1/store-protocol-root";
#[cfg(any(test, feature = "test-utils"))]
pub const STORE_PROTOCOL_ROOT_LOGICAL_KEY: &str = "store-v1/store-protocol-root.json";
pub(crate) const STORE_CANDIDATE_PREFIX: &str = "store-v1/candidates/";
pub(crate) const STORE_HEAD_PREFIX: &str = "store-v1/heads/";
pub(crate) const STORE_ACK_PREFIX: &str = "store-v1/acks/";
pub(crate) const STORE_DEVICE_REGISTRATION_PREFIX: &str = "store-v1/devices/";
pub(crate) const STORE_DEVICE_JOIN_ATTEMPT_PREFIX: &str = "store-v1/device-join-attempts/";
pub(crate) const STORE_DEVICE_JOIN_OUTCOME_PREFIX: &str = "store-v1/device-join-outcomes/";
pub(crate) const STORE_DEVICE_JOIN_CLEANUP_RECEIPT_PREFIX: &str =
    "store-v1/device-join-cleanup-receipts/";
pub(crate) const STORE_DEVICE_EXCLUSION_PROPOSAL_PREFIX: &str =
    "store-v1/device-exclusion-proposals/";
pub(crate) const STORE_DEVICE_EXCLUSION_OUTCOME_PREFIX: &str =
    "store-v1/device-exclusion-outcomes/";
pub(crate) const STORE_PROVIDER_ACCESS_GRANT_PREFIX: &str = "store-v1/provider-access/grants/";
pub(crate) const STORE_OWNER_RECOVERY_PREFIX: &str = "store-v1/recovery/";
pub(crate) const STORE_SNAPSHOT_META_PREFIX: &str = "store-v1/snapshots/";
pub(crate) const STORE_SNAPSHOT_IMAGE_PREFIX: &str = "store-v1/snapshot-images/";
pub(crate) const STORE_MEMBERSHIP_ENTRY_PREFIX: &str = "store-v1/membership/entries/";
pub(crate) const STORE_MEMBERSHIP_HEAD_PREFIX: &str = "store-v1/membership/heads/";

const STORE_PROTOCOL_ROOT_DOMAIN: &[u8] = b"coven.store-protocol-root.v1\0";
const COMMIT_DOMAIN: &[u8] = b"coven.store-batch-commit.v1\0";
const HEAD_DOMAIN: &[u8] = b"coven.store-device-head.v1\0";
const MERGE_HISTORY_SUMMARY_DOMAIN: &[u8] = b"coven.retained-merge-history-summary.v1\0";
const REGISTRATION_DOMAIN: &[u8] = b"coven.store-device-registration.v1\0";
const DEVICE_JOIN_ATTEMPT_DOMAIN: &[u8] = b"coven.device-join-attempt.v1\0";
const DEVICE_READINESS_DOMAIN: &[u8] = b"coven.device-readiness.v1\0";
const DEVICE_JOIN_OUTCOME_DOMAIN: &[u8] = b"coven.device-join-outcome.v1\0";
const DEVICE_EXCLUSION_PROPOSAL_DOMAIN: &[u8] = b"coven.store-device-exclusion-proposal.v1\0";
const DEVICE_EXCLUSION_DOMAIN: &[u8] = b"coven.store-device-exclusion.v1\0";
const DEVICE_EXCLUSION_CANCELLATION_DOMAIN: &[u8] =
    b"coven.store-device-exclusion-cancellation.v1\0";
const OWNER_RECOVERY_NODE_DOMAIN: &[u8] = b"coven.owner-recovery-node.v1\0";
const ACK_DOMAIN: &[u8] = b"coven.store-ack.v1\0";
const CIRCLE_ACK_DOMAIN: &[u8] = b"coven.circle-ack.v1\0";
const CIRCLE_SNAPSHOT_DOMAIN: &[u8] = b"coven.circle-snapshot-meta.v1\0";
const SNAPSHOT_DOMAIN: &[u8] = b"coven.snapshot-meta.v1\0";
const CANDIDATE_FAMILY_DOMAIN: &[u8] = b"coven.candidate-family.v1\0";
const STREAM_ACTIVATION_ID_DOMAIN: &[u8] = b"coven.stream-activation-id.v1\0";
const AUTHOR_STREAM_ID_DOMAIN: &[u8] = b"coven.author-stream-id.v1\0";

#[cfg(test)]
#[path = "store_commit/tests.rs"]
mod tests;
