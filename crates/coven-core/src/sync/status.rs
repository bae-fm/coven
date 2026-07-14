/// Sync status derived from device heads.
///
/// After each pull, the caller has the full list of `DeviceHead`s. This
/// module provides a type to summarize that into a human-readable status
/// for the UI: when we last synced, and what other devices are doing.
use super::store_commit::StoreDeviceHead;

/// Activity summary for a single remote device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceActivity {
    pub device_id: String,
    /// Hex-encoded Ed25519 public key the device's head verified against — the
    /// member the device belongs to. Empty only for a head that carried no author.
    pub author: String,
    pub last_seq: u64,
    /// RFC 3339 timestamp of the device's last sync. None if the head
    /// carried no timestamp.
    pub last_sync: Option<String>,
}

/// Sync status derived from the heads fetched during a pull.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStatus {
    /// When this device last synced (RFC 3339). None if never synced.
    pub last_sync_time: Option<String>,
    /// Activity of other devices.
    pub other_devices: Vec<DeviceActivity>,
}

/// Build a `SyncStatus` from a list of device heads.
///
/// `our_device_id` identifies the local device so its head can be
/// separated from the "other devices" list.
/// `local_sync_time` is the RFC 3339 timestamp of when *we* last
/// completed a sync cycle (tracked locally, not from the heads).
pub fn build_sync_status(
    heads: &[StoreDeviceHead],
    our_device_id: &str,
    local_sync_time: Option<&str>,
) -> SyncStatus {
    let mut other_devices: Vec<DeviceActivity> = Vec::new();

    for head in heads {
        if head.device_id == our_device_id {
            continue;
        }

        let activity = DeviceActivity {
            device_id: head.device_id.clone(),
            author: head.author_pubkey.clone(),
            last_seq: head.slot_sequence(),
            last_sync: Some(head.published_at.clone()),
        };
        match other_devices
            .iter_mut()
            .find(|current| current.device_id == activity.device_id)
        {
            Some(current) if current.last_seq < activity.last_seq => *current = activity,
            Some(_) => {}
            None => other_devices.push(activity),
        }
    }

    SyncStatus {
        last_sync_time: local_sync_time.map(|s| s.to_string()),
        other_devices,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::UserKeypair;
    use crate::sync::store_commit::{CommitPosition, ObjectHash};

    fn head(device_id: &str, seq: u64, published_at: &str) -> StoreDeviceHead {
        StoreDeviceHead::signed(
            ObjectHash::digest(b"status-store-protocol-root"),
            device_id.to_string(),
            Some(CommitPosition {
                seq,
                commit_hash: ObjectHash::digest(format!("{device_id}/{seq}").as_bytes()),
            }),
            published_at.to_string(),
            &UserKeypair::generate(),
        )
        .expect("sign Store head")
    }

    #[test]
    fn build_status_with_no_heads() {
        let status = build_sync_status(&[], "dev-1", None);
        assert_eq!(status.last_sync_time, None);
        assert!(status.other_devices.is_empty());
    }

    #[test]
    fn build_status_excludes_own_device() {
        let heads = vec![
            head("dev-1", 5, "2026-02-10T12:00:00Z"),
            head("dev-2", 3, "2026-02-10T11:55:00Z"),
        ];

        let status = build_sync_status(&heads, "dev-1", Some("2026-02-10T12:00:00Z"));
        assert_eq!(
            status.last_sync_time,
            Some("2026-02-10T12:00:00Z".to_string())
        );
        assert_eq!(status.other_devices.len(), 1);
        assert_eq!(status.other_devices[0].device_id, "dev-2");
        assert!(!status.other_devices[0].author.is_empty());
        assert_eq!(status.other_devices[0].last_seq, 3);
    }

    #[test]
    fn build_status_uses_the_signed_publication_timestamp() {
        let heads = vec![head("dev-2", 10, "2026-02-10T11:55:00Z")];

        let status = build_sync_status(&heads, "dev-1", None);
        assert_eq!(status.last_sync_time, None);
        assert_eq!(status.other_devices.len(), 1);
        assert_eq!(
            status.other_devices[0].last_sync.as_deref(),
            Some("2026-02-10T11:55:00Z")
        );
    }
}
