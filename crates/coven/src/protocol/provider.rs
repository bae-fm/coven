use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::protocol::membership::{
    MembershipCoord, MembershipEntry, MembershipGrantId, OwnerStreamBarrier,
};
use crate::protocol::objects::ObjectSlot;
use crate::protocol::objects::{
    ExactObjectRef, ProviderDeviceBinding, StorageError, StoreProviderBinding,
};
use crate::protocol::store_commit::{
    DeviceJoinAttemptId, DeviceJoinAttemptRef, ObjectHash, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreRootRef,
};

const EXACT_TRANSCRIPT_DOMAIN: &[u8] = b"coven.provider-exact-slot-probe.v1\0";
const CROSS_TRANSCRIPT_DOMAIN: &[u8] = b"coven.provider-cross-principal-probe.v1\0";
const CROSS_CHALLENGE_DOMAIN: &[u8] = b"coven.provider-cross-principal-challenge.v1\0";
const CROSS_RESPONSE_DOMAIN: &[u8] = b"coven.provider-cross-principal-response.v1\0";
const PAYLOAD_DOMAIN: &[u8] = b"coven.provider-probe-payload.v1\0";
const MEMBER_ACCESS_GRANT_DOMAIN: &[u8] = b"coven.provider-member-access-grant.v1\0";
pub(crate) const PROBE_PAYLOAD_LEN: usize = 256;
pub(crate) const PROBE_RANGE_START: u64 = 31;
pub(crate) const PROBE_RANGE_END: u64 = 173;

mod access;
mod admin;
mod cross_principal;
mod probe;

pub use access::*;
pub use admin::*;
pub use cross_principal::*;
pub use probe::*;

pub(crate) fn canonical_custom_s3_origin(input: &str) -> Result<String, StorageError> {
    if input.ends_with('/') {
        return Err(StorageError::Configuration(
            "custom S3 endpoint must not have a trailing slash".to_string(),
        ));
    }
    let parsed = url::Url::parse(input).map_err(|error| {
        StorageError::Configuration(format!("invalid custom S3 endpoint: {error}"))
    })?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err(StorageError::Configuration(
            "custom S3 endpoint must be an HTTP origin without user info, path, query, or fragment"
                .to_string(),
        ));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| StorageError::Configuration("custom S3 endpoint has no host".to_string()))?;
    let port = parsed.port();
    let default_port = matches!(
        (parsed.scheme(), port),
        ("http", Some(80)) | ("https", Some(443))
    );
    Ok(if let Some(port) = port.filter(|_| !default_port) {
        format!("{}://{}:{port}", parsed.scheme(), host.to_ascii_lowercase())
    } else {
        format!("{}://{}", parsed.scheme(), host.to_ascii_lowercase())
    })
}

pub(crate) async fn advance_cross_completion(
    journal: &dyn ProviderProbeJournal,
    durable: &mut ProviderProbeJournalRecord,
    record: &mut CrossPrincipalCompletionJournal,
    progress: CrossPrincipalCompletionProgress,
) -> Result<(), ProviderProbeError> {
    record.progress = progress;
    let next = ProviderProbeJournalRecord::CrossPrincipal(record.clone());
    journal.advance(durable, next.clone()).await?;
    *durable = next;
    Ok(())
}

pub(crate) async fn advance_exact(
    journal: &dyn ProviderProbeJournal,
    durable: &mut ProviderProbeJournalRecord,
    record: &mut ExactProbeJournal,
    progress: ExactProbeProgress,
) -> Result<(), ProviderProbeError> {
    record.progress = progress;
    let next = ProviderProbeJournalRecord::Exact(record.clone());
    journal.advance(durable, next.clone()).await?;
    *durable = next;
    Ok(())
}

mod ordered_owner_barriers {
    use super::*;

    pub(super) fn serialize<S>(
        map: &BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<MembershipGrantId, OwnerStreamBarrier>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(MembershipGrantId, OwnerStreamBarrier)>::deserialize(deserializer)?;
        let count = entries.len();
        let map = entries.into_iter().collect::<BTreeMap<_, _>>();
        if map.len() != count {
            return Err(serde::de::Error::custom(
                "provider administrator owner barriers contain a duplicate grant",
            ));
        }
        Ok(map)
    }
}

#[cfg(test)]
pub(crate) mod test_fixtures;
#[cfg(test)]
mod tests;
