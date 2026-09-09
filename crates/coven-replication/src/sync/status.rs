//! Per-device activity derived from the accepted commits a pull fetched: what every
//! other device in the store has published, for a host to render "which devices
//! synced, and how far".

/// Activity summary for a single remote device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceActivity {
    pub device_id: String,
    /// Hex-encoded Ed25519 public key the commit verified against.
    pub author: String,
    /// The device's highest accepted commit sequence.
    pub last_seq: u64,
}

/// The activity of every device other than this one, read off the commits a pull
/// fetched. `our_device_id` identifies the local device so its own commits are left
/// out; each remaining device is reported once, at its highest accepted sequence.
pub(crate) fn other_device_activity(
    commits: &[coven_protocol::store_commit::VerifiedStoreBatchCommit],
    our_device_id: &str,
) -> Vec<DeviceActivity> {
    let mut other_devices: Vec<DeviceActivity> = Vec::new();

    for commit in commits {
        if commit.author().device_id.to_string() == our_device_id {
            continue;
        }

        let activity = DeviceActivity {
            device_id: commit.author().device_id.to_string(),
            author: commit.author().author_pubkey.clone(),
            last_seq: commit.reference().coord.sequence(),
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

    other_devices
}
