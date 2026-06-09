/// Sync status derived from device heads.
///
/// After each pull, the caller has the full list of `DeviceHead`s. This
/// module provides a type to summarize that into a human-readable status
/// for the UI: when we last synced, and what other devices are doing.
use super::storage::DeviceHead;

/// Activity summary for a single remote device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceActivity {
    pub device_id: String,
    pub last_seq: u64,
    /// RFC 3339 timestamp of the device's last sync. None if the head
    /// was written before timestamps were added.
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
    heads: &[DeviceHead],
    our_device_id: &str,
    local_sync_time: Option<&str>,
) -> SyncStatus {
    let mut other_devices = Vec::new();

    for head in heads {
        if head.device_id == our_device_id {
            continue;
        }

        other_devices.push(DeviceActivity {
            device_id: head.device_id.clone(),
            last_seq: head.seq,
            last_sync: head.last_sync.clone(),
        });
    }

    SyncStatus {
        last_sync_time: local_sync_time.map(|s| s.to_string()),
        other_devices,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_status_with_no_heads() {
        let status = build_sync_status(&[], "dev-1", None);
        assert_eq!(status.last_sync_time, None);
        assert!(status.other_devices.is_empty());
    }

    #[test]
    fn build_status_excludes_own_device() {
        let heads = vec![
            DeviceHead {
                device_id: "dev-1".into(),
                seq: 5,
                snapshot_seq: None,
                last_sync: Some("2026-02-10T12:00:00Z".into()),
                cursors: std::collections::HashMap::new(),
            },
            DeviceHead {
                device_id: "dev-2".into(),
                seq: 3,
                snapshot_seq: None,
                last_sync: Some("2026-02-10T11:55:00Z".into()),
                cursors: std::collections::HashMap::new(),
            },
        ];

        let status = build_sync_status(&heads, "dev-1", Some("2026-02-10T12:00:00Z"));
        assert_eq!(
            status.last_sync_time,
            Some("2026-02-10T12:00:00Z".to_string())
        );
        assert_eq!(status.other_devices.len(), 1);
        assert_eq!(status.other_devices[0].device_id, "dev-2");
        assert_eq!(status.other_devices[0].last_seq, 3);
    }

    #[test]
    fn build_status_with_no_timestamps() {
        let heads = vec![DeviceHead {
            device_id: "dev-2".into(),
            seq: 10,
            snapshot_seq: None,
            last_sync: None,
            cursors: std::collections::HashMap::new(),
        }];

        let status = build_sync_status(&heads, "dev-1", None);
        assert_eq!(status.last_sync_time, None);
        assert_eq!(status.other_devices.len(), 1);
        assert_eq!(status.other_devices[0].last_sync, None);
    }
}
