//! Per-device activity derived from the device heads a pull fetched: what every
//! other device in the store has published, for a host to render "which devices
//! synced, and how far".

use super::store::VerifiedStoreDeviceHead;

/// Activity summary for a single remote device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceActivity {
    pub device_id: String,
    /// Hex-encoded Ed25519 public key the device's head verified against — the
    /// member the device belongs to. Empty only for a head that carried no author.
    pub author: String,
    /// The device's highest published head sequence.
    pub last_seq: u64,
}

/// The activity of every device other than this one, read off the heads a pull
/// fetched. `our_device_id` identifies the local device so its own head is left
/// out; each remaining device is reported once, at its highest head sequence.
pub(crate) fn other_device_activity(
    heads: &[VerifiedStoreDeviceHead],
    our_device_id: &str,
) -> Vec<DeviceActivity> {
    let mut other_devices: Vec<DeviceActivity> = Vec::new();

    for head in heads {
        if head.author.device_id.to_string() == our_device_id {
            continue;
        }

        let activity = DeviceActivity {
            device_id: head.author.device_id.to_string(),
            author: head.author.author_pubkey.clone(),
            last_seq: head.head.slot_sequence(),
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
