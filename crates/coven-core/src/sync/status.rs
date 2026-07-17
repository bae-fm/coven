/// Sync status derived from device heads.
///
/// After each pull, the caller has the full list of `DeviceHead`s. This
/// module provides a type to summarize that into a human-readable status
/// for the UI: when we last synced, and what other devices are doing.
use super::store_pull::VerifiedStoreDeviceHead;

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
    heads: &[VerifiedStoreDeviceHead],
    our_device_id: &str,
    local_sync_time: Option<&str>,
) -> SyncStatus {
    let mut other_devices: Vec<DeviceActivity> = Vec::new();

    for head in heads {
        if head.author.device_id.to_string() == our_device_id {
            continue;
        }

        let activity = DeviceActivity {
            device_id: head.author.device_id.to_string(),
            author: head.author.author_pubkey.clone(),
            last_seq: head.head.slot_sequence(),
            last_sync: None,
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
