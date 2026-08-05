use super::chunking::*;
use super::*;

const EXACT_MANIFEST_MAGIC: &[u8] = b"coven-cloudkit-exact-manifest-v1\0";

pub(super) fn exact_part_key(logical_key: &str, index: usize) -> String {
    format!("{logical_key}.exact-part{index}")
}

pub(super) fn encode_exact_manifest(part_count: usize, total_len: usize) -> Vec<u8> {
    let mut bytes = EXACT_MANIFEST_MAGIC.to_vec();
    bytes.extend_from_slice(part_count.to_string().as_bytes());
    bytes.push(b'\n');
    bytes.extend_from_slice(total_len.to_string().as_bytes());
    bytes.push(b'\n');
    bytes
}

pub(super) fn decode_exact_manifest(bytes: &[u8]) -> Result<(usize, usize), CloudHomeError> {
    let text = std::str::from_utf8(bytes.strip_prefix(EXACT_MANIFEST_MAGIC).ok_or_else(|| {
        CloudHomeError::Transport("CloudKit exact object has an invalid manifest".to_string())
    })?)
    .map_err(|error| CloudHomeError::Transport(format!("CloudKit exact manifest: {error}")))?;
    let mut lines = text.lines();
    let part_count = lines
        .next()
        .ok_or_else(|| {
            CloudHomeError::Transport("CloudKit exact manifest omitted part count".to_string())
        })?
        .parse::<usize>()
        .map_err(|error| {
            CloudHomeError::Transport(format!("CloudKit exact manifest part count: {error}"))
        })?;
    let total_len = lines
        .next()
        .ok_or_else(|| {
            CloudHomeError::Transport("CloudKit exact manifest omitted length".to_string())
        })?
        .parse::<usize>()
        .map_err(|error| {
            CloudHomeError::Transport(format!("CloudKit exact manifest length: {error}"))
        })?;
    if lines.next().is_some() || part_count != total_len.div_ceil(CHUNK_SIZE) {
        return Err(CloudHomeError::Transport(
            "CloudKit exact manifest shape does not match its length".to_string(),
        ));
    }
    Ok((part_count, total_len))
}

pub(super) fn read_exact_cloudkit_object(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    logical_key: &str,
) -> Result<(Vec<u8>, Vec<CloudKitRecordVersion>), CloudHomeError> {
    let manifest = ops.read_versioned_record(scope, logical_key)?;
    let (part_count, total_len) = decode_exact_manifest(&manifest.bytes)?;
    let mut bytes = Vec::with_capacity(total_len);
    let mut records = Vec::with_capacity(part_count + 1);
    records.push(CloudKitRecordVersion {
        key: logical_key.to_string(),
        version: manifest.version,
    });
    for index in 0..part_count {
        let key = exact_part_key(logical_key, index);
        let part = read_exact_part(ops, scope, logical_key, part_count, total_len, index, &key)?;
        bytes.extend_from_slice(&part.bytes);
        records.push(CloudKitRecordVersion {
            key,
            version: part.version,
        });
    }
    Ok((bytes, records))
}

/// The plaintext length part `index` of an exact object carries: a full chunk
/// for every part but the last, which holds the remainder.
pub(super) fn exact_part_len(part_count: usize, total_len: usize, index: usize) -> usize {
    if index + 1 == part_count {
        total_len - index * CHUNK_SIZE
    } else {
        CHUNK_SIZE
    }
}

/// Read one part record and refuse a length its manifest does not assign it. A
/// part that is short is not the part the manifest describes, so splicing it
/// would silently serve the wrong bytes at every later offset.
pub(super) fn read_exact_part(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    logical_key: &str,
    part_count: usize,
    total_len: usize,
    index: usize,
    key: &str,
) -> Result<CloudVersionedObject, CloudHomeError> {
    let part = ops.read_versioned_record(scope, key)?;
    let expected_len = exact_part_len(part_count, total_len, index);
    if part.bytes.len() != expected_len {
        return Err(CloudHomeError::Transport(format!(
            "CloudKit exact object {logical_key:?} part {index} has {} bytes, expected {expected_len}",
            part.bytes.len()
        )));
    }
    Ok(part)
}

/// Read one byte range of an exact CloudKit object, fetching only the part
/// records that cover it.
///
/// The whole-object sibling is [`read_exact_cloudkit_object`]. Both read the
/// same manifest, but this one never touches a part the range does not reach —
/// which is what makes a ranged read of a blob cost the range. Reading the whole
/// object and slicing would answer correctly and cost the object, so the
/// caller's O(range) guarantee lives or dies here.
pub(super) fn read_exact_cloudkit_range(
    ops: &dyn CloudKitOps,
    scope: &CloudKitScope,
    logical_key: &str,
    start: usize,
    end: usize,
) -> Result<Vec<u8>, CloudHomeError> {
    let manifest = ops.read_versioned_record(scope, logical_key)?;
    let (part_count, total_len) = decode_exact_manifest(&manifest.bytes)?;
    if end > total_len {
        return Err(CloudHomeError::Transport(format!(
            "range {start}..{end} exceeds CloudKit exact object {logical_key:?} size {total_len}"
        )));
    }
    let first = start / CHUNK_SIZE;
    let last = (end - 1) / CHUNK_SIZE;
    if last >= part_count {
        return Err(CloudHomeError::Transport(format!(
            "range {start}..{end} needs part {last} of CloudKit exact object {logical_key:?}, which has {part_count}"
        )));
    }
    let mut bytes = Vec::with_capacity(end - start);
    for index in first..=last {
        let key = exact_part_key(logical_key, index);
        let part = read_exact_part(ops, scope, logical_key, part_count, total_len, index, &key)?;
        let part_start = index * CHUNK_SIZE;
        let from = start.saturating_sub(part_start);
        let to = (end - part_start).min(part.bytes.len());
        bytes.extend_from_slice(&part.bytes[from..to]);
    }
    Ok(bytes)
}

#[async_trait]
impl ExactSlotStorage for CloudKitCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<crate::protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        use crate::protocol::objects::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
            StoreProviderBinding,
        };

        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let identity = blocking(move || ops.provider_identity(&scope)).await?;
        if identity.container_id.is_empty()
            || identity.owner_name.is_empty()
            || identity.zone_name.is_empty()
            || identity.current_user_record_name.is_empty()
        {
            return Err(CloudHomeError::Configuration(
                "CloudKit provider identity contains an empty stable identifier".to_string(),
            ));
        }
        if let CloudKitScope::Shared {
            owner_name,
            zone_name,
        } = &self.scope
        {
            if owner_name != &identity.owner_name || zone_name != &identity.zone_name {
                return Err(CloudHomeError::Configuration(format!(
                    "CloudKit provider identity resolved zone {}/{}, expected {owner_name}/{zone_name}",
                    identity.owner_name, identity.zone_name
                )));
            }
        }
        let principal = match &self.scope {
            CloudKitScope::Private => ProviderPrincipalId::CloudKitPrivateZoneOwner {
                record_name: identity.current_user_record_name,
            },
            CloudKitScope::Shared { .. } => ProviderPrincipalId::CloudKitSharedZoneParticipant {
                record_name: identity.current_user_record_name,
            },
        };
        Ok(ResolvedProviderBinding {
            store: StoreProviderBinding::CloudKit {
                container_id: identity.container_id,
                environment: identity.environment,
                owner_name: identity.owner_name,
                zone_name: identity.zone_name,
            },
            device: ProviderDeviceBinding { principal },
        })
    }

    async fn cross_principal_evidence(
        &self,
    ) -> Result<crate::protocol::provider::CrossPrincipalProviderEvidence, CloudHomeError> {
        use crate::protocol::provider::{CloudKitAcceptedShare, CrossPrincipalProviderEvidence};
        use crate::protocol::store_commit::ObjectHash;

        let CloudKitScope::Shared {
            owner_name,
            zone_name,
        } = &self.scope
        else {
            return Err(CloudHomeError::Configuration(
                "CloudKit cross-principal evidence requires an accepted shared zone".to_string(),
            ));
        };
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let accepted = blocking(move || ops.accepted_read_write_share(&scope)).await?;
        let binding = self.provider_binding().await?;
        let crate::protocol::objects::ProviderPrincipalId::CloudKitSharedZoneParticipant {
            record_name,
        } = binding.device.principal
        else {
            return Err(CloudHomeError::Configuration(
                "CloudKit adapter returned a non-CloudKit principal".to_string(),
            ));
        };
        if accepted.share_record_name.is_empty()
            || accepted.owner_name != *owner_name
            || accepted.zone_name != *zone_name
            || accepted.participant_record_name != record_name
            || accepted.permission != CloudKitSharePermission::ReadWrite
            || accepted.acceptance != CloudKitShareAcceptance::Accepted
            || accepted.canonical_record.is_empty()
        {
            return Err(CloudHomeError::Configuration(
                "CloudKit accepted share does not prove read-write participation in the selected zone"
                    .to_string(),
            ));
        }
        let share_slot = ObjectSlot::logical(format!(
            "__coven_cloudkit_share__/{}",
            hex::encode(ObjectHash::digest(accepted.share_record_name.as_bytes()).as_bytes())
        ))?;
        Ok(CrossPrincipalProviderEvidence::CloudKit(
            CloudKitAcceptedShare {
                share: crate::protocol::objects::ExactObjectRef::new(
                    share_slot,
                    accepted.canonical_record.len() as u64,
                    ObjectHash::digest(&accepted.canonical_record),
                ),
                share_record_name: accepted.share_record_name,
                owner_name: accepted.owner_name,
                zone_name: accepted.zone_name,
                participant_record_name: accepted.participant_record_name,
            },
        ))
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        mut body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let total_len = usize::try_from(body.len()).map_err(|_| {
            CloudHomeError::Transport(format!(
                "CloudKit object {:?} is too large for this platform",
                slot.logical_key()
            ))
        })?;
        let part_count = total_len.div_ceil(CHUNK_SIZE);
        let staging = self.begin_atomic_create().await?;
        let mut requested_keys = Vec::with_capacity(part_count + 1);
        let mut written_len = 0usize;
        for index in 0..part_count {
            let part = match body.next_part(CHUNK_SIZE).await {
                Ok(Some(part)) => part,
                Ok(None) => {
                    return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                        "CloudKit object {:?} ended after {written_len} of {total_len} bytes",
                        slot.logical_key()
                    ))))
                }
                Err(error) => return Err(staging.cleanup_failure(error)),
            };
            written_len += part.len();
            let key = exact_part_key(slot.logical_key(), index);
            if let Err(error) = staging
                .clone()
                .stage_record(CloudKitRecordCreate {
                    key: key.clone(),
                    data: part.to_vec(),
                })
                .await
            {
                return Err(staging.cleanup_failure(error));
            }
            requested_keys.push(key);
        }
        match body.next_part(CHUNK_SIZE).await {
            Ok(None) if written_len == total_len => {}
            Ok(None) => {
                return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                    "CloudKit object {:?} yielded {written_len} bytes, expected {total_len}",
                    slot.logical_key()
                ))))
            }
            Ok(Some(extra)) => {
                return Err(staging.cleanup_failure(CloudHomeError::Transport(format!(
                    "CloudKit object {:?} yielded at least {} bytes, expected {total_len}",
                    slot.logical_key(),
                    written_len + extra.len()
                ))))
            }
            Err(error) => return Err(staging.cleanup_failure(error)),
        }
        if let Err(error) = staging
            .clone()
            .stage_record(CloudKitRecordCreate {
                key: slot.logical_key().to_string(),
                data: encode_exact_manifest(part_count, total_len),
            })
            .await
        {
            return Err(staging.cleanup_failure(error));
        }
        requested_keys.push(slot.logical_key().to_string());
        let created = match staging.clone().commit().await {
            Ok(created) => created,
            Err(CloudHomeError::AlreadyExists(_)) => {
                return Err(staging.cleanup_failure(CloudHomeError::AlreadyExists(
                    slot.logical_key().to_string(),
                )));
            }
            Err(operation) => {
                match self
                    .settle_atomic_create_response_loss(requested_keys.clone())
                    .await
                {
                    Ok(AtomicCreateReadback::Created(created)) => {
                        staging.disarm();
                        created
                    }
                    Ok(AtomicCreateReadback::Absent) => {
                        return Err(staging.cleanup_failure(operation))
                    }
                    Err(readback) => {
                        staging.disarm();
                        return Err(CloudHomeError::UnresolvedOutcome {
                            operation: Box::new(operation),
                            readback: Box::new(readback),
                        });
                    }
                }
            }
        };
        if created.len() != requested_keys.len()
            || created
                .iter()
                .zip(&requested_keys)
                .any(|(record, requested)| &record.key != requested)
        {
            self.authoritative_created_records(requested_keys).await?;
        }
        progress(total_len as u64);
        Ok(())
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let logical_key = slot.logical_key().to_string();
        blocking(move || {
            read_exact_cloudkit_object(&*ops, &scope, &logical_key).map(|value| value.0)
        })
        .await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let start = usize::try_from(start)
            .map_err(|_| CloudHomeError::Configuration("range start is too large".to_string()))?;
        let end = usize::try_from(end)
            .map_err(|_| CloudHomeError::Configuration("range end is too large".to_string()))?;
        if end < start {
            return Err(CloudHomeError::Configuration(format!(
                "invalid range {start}..{end}"
            )));
        }
        if end == start {
            return Ok(Vec::new());
        }
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let logical_key = slot.logical_key().to_string();
        blocking(move || read_exact_cloudkit_range(&*ops, &scope, &logical_key, start, end)).await
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), crate::storage::cloud::CloudFileReadError> {
        let bytes = self.read_at(slot).await?;
        let stream: crate::storage::cloud::CloudObjectStream = Box::pin(
            futures_util::stream::once(async move { Ok(Bytes::from(bytes)) }),
        );
        crate::storage::cloud::write_cloud_object_stream(destination, stream)
            .await
            .map(drop)
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("CloudKit")?;
        let ops = self.ops.clone();
        let scope = self.scope.clone();
        let logical_key = slot.logical_key().to_string();
        blocking(move || {
            let records = match read_exact_cloudkit_object(&*ops, &scope, &logical_key) {
                Ok((_, records)) => records,
                Err(CloudHomeError::NotFound(_)) => return Ok(()),
                Err(error) => return Err(error),
            };
            ops.delete_record_versions(&scope, &records)
        })
        .await
    }
}
