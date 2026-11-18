//! This device's own secrets, which belong to no store (E11).

use super::Coven;
use coven_crypto::custody::KeyError;

impl Coven {
    /// This device's secret `name`, or `None` if it was never set (E11). A
    /// device secret belongs to the device, not to a store: every store on
    /// this device reads the same one, and deleting a store leaves it. Under
    /// `Coven::in_memory` it lives in the test's in-memory keychain.
    pub fn device_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        self.keychain.device_secret(name)
    }

    /// Keep `value` as this device's secret `name`, such as a pairing with a
    /// speaker, in a device-only entry under the app's keychain service and
    /// the same access policy as coven's keys (E11). Names are arbitrary
    /// strings. Coven records the name in this device's secret list before
    /// writing the value, so `delete_device_secrets` finds every saved
    /// secret; a failure between the two may leave an extra name, and
    /// retrying is safe.
    pub fn set_device_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        self.keychain.set_device_secret(name, value)
    }

    /// Delete this device's secret `name` before its listed name; an absent
    /// secret succeeds, and retrying after a failure is safe (E11).
    pub fn delete_device_secret(&self, name: &str) -> Result<(), KeyError> {
        self.keychain.delete_device_secret(name)
    }

    /// Delete every device secret, then their list, without the app naming
    /// them or coven decoding a value, as an app's reset of this device does
    /// (E11). Store entries and the person's entries stay. Absent entries
    /// succeed, so retrying after a failure finishes.
    pub fn delete_device_secrets(&self) -> Result<(), KeyError> {
        self.keychain.delete_device_secrets()
    }
}

#[cfg(test)]
#[path = "coven_device_secrets_tests.rs"]
mod tests;
