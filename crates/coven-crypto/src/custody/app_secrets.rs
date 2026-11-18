//! The app's own secrets, kept beside coven's entries in this device's
//! keychain: each value in a device-only entry of its own, and their names in
//! one list entry, so a deletion finds every value without the app naming
//! them (§12.4, D.6).

use super::keychain::{account, EntryScope};
use super::{KeyError, Keychain};
use crate::{wire, MaterialError, SecretBytes};
use coven_foundation::id_source::StoreId;
use std::collections::BTreeSet;

pub(super) const HOST_SECRET_NAMES_ENTRY: &str = "host-secret-names";
pub(super) const DEVICE_SECRET_NAMES_ENTRY: &str = "device-secret-names";

/// Whose secrets one list holds.
#[derive(Clone, Copy, Debug)]
pub(super) enum SecretsOwner {
    /// One store's host secrets, which go when the store is deleted from
    /// this device: accounts `host-secret-<hex>:<store>`.
    Store(StoreId),
    /// This device's own secrets, which belong to no store and outlive
    /// every store: accounts `device-secret-<hex>`, with no store suffix.
    Device,
}

impl SecretsOwner {
    fn names_account(self) -> String {
        match self {
            Self::Store(store) => account(HOST_SECRET_NAMES_ENTRY, store),
            Self::Device => DEVICE_SECRET_NAMES_ENTRY.to_owned(),
        }
    }

    fn value_account(self, name: &str) -> String {
        match self {
            Self::Store(store) => account(&host_secret_entry(name), store),
            Self::Device => device_secret_entry(name),
        }
    }
}

/// The secret `name` of `owner`, or `None` if it was never set (E11).
#[expect(
    clippy::map_err_ignore,
    reason = "an app secret fails only by not being UTF-8, which Encoding names"
)]
pub(super) fn read(
    keychain: &Keychain,
    owner: SecretsOwner,
    name: &str,
) -> Result<Option<String>, KeyError> {
    keychain
        .read(EntryScope::DeviceOnly, &owner.value_account(name))?
        .map(|bytes| {
            std::str::from_utf8(bytes.as_bytes())
                .map(str::to_owned)
                .map_err(|_| MaterialError::Encoding.into())
        })
        .transpose()
}

/// Keep `value` as the secret `name` of `owner`, or delete it for `None`.
/// Saving records the name before writing the value; deleting removes the
/// value before the name. So the list covers every value that exists, and a
/// failure between the two steps leaves only an extra name. The keychain's
/// lock serializes every list's updates in this process.
pub(super) fn change(
    keychain: &Keychain,
    owner: SecretsOwner,
    name: &str,
    value: Option<&str>,
) -> Result<(), KeyError> {
    keychain.changing_secret_names(|| {
        let bytes = keychain.read(EntryScope::DeviceOnly, &owner.names_account())?;
        let mut names = match &bytes {
            Some(bytes) => decode_names(bytes.as_bytes())?,
            None => BTreeSet::new(),
        };
        match value {
            Some(value) => {
                names.insert(name);
                write_names(keychain, owner, &names)?;
                keychain.write(EntryScope::DeviceOnly, &owner.value_account(name), value.as_bytes())
            }
            None => {
                keychain.delete(EntryScope::DeviceOnly, &owner.value_account(name))?;
                names.remove(name);
                write_names(keychain, owner, &names)
            }
        }
    })
}

/// Delete every listed secret of `owner`, then the list, without decoding a
/// value. Absent entries succeed, so repeating after a failure finishes.
pub(super) fn delete_all(keychain: &Keychain, owner: SecretsOwner) -> Result<(), KeyError> {
    keychain.changing_secret_names(|| {
        let names_account = owner.names_account();
        if let Some(bytes) = keychain.read(EntryScope::DeviceOnly, &names_account)? {
            for name in decode_names(bytes.as_bytes())? {
                keychain.delete(EntryScope::DeviceOnly, &owner.value_account(name))?;
            }
        }
        keychain.delete(EntryScope::DeviceOnly, &names_account)
    })
}

fn write_names(
    keychain: &Keychain,
    owner: SecretsOwner,
    names: &BTreeSet<&str>,
) -> Result<(), KeyError> {
    if names.is_empty() {
        keychain.delete(EntryScope::DeviceOnly, &owner.names_account())
    } else {
        keychain.write(
            EntryScope::DeviceOnly,
            &owner.names_account(),
            encode_names(names)?.as_bytes(),
        )
    }
}

// Hex keeps empty, NUL-containing and Unicode names distinct and separates every
// app name from coven's own entries, even on case-insensitive native keychains.
pub(super) fn host_secret_entry(name: &str) -> String {
    format!("host-secret-{}", hex::encode(name.as_bytes()))
}

// No store entry's account lacks its `:<store>` suffix, and hex holds no
// colon, so a device secret's account never names a store's entry.
pub(super) fn device_secret_entry(name: &str) -> String {
    format!("device-secret-{}", hex::encode(name.as_bytes()))
}

// Local custody framing: a versioned prefix and length-prefixed UTF-8 names.
// Values live in independent native entries and have no coven framing overhead.
pub(super) const SECRET_NAMES_PREFIX: &[u8] = b"CVHN\x01";

#[expect(
    clippy::map_err_ignore,
    reason = "a name list fails only by an invalid length or name: Encoding"
)]
pub(super) fn decode_names(mut bytes: &[u8]) -> Result<BTreeSet<&str>, MaterialError> {
    wire::prefix(&mut bytes, SECRET_NAMES_PREFIX)?;
    let mut names = BTreeSet::new();
    while !bytes.is_empty() {
        let len =
            usize::try_from(wire::number(&mut bytes)?).map_err(|_| MaterialError::Encoding)?;
        let name = std::str::from_utf8(wire::take(&mut bytes, len)?)
            .map_err(|_| MaterialError::Encoding)?;
        if !names.insert(name) {
            return Err(MaterialError::Encoding);
        }
    }
    Ok(names)
}

fn encode_names(names: &BTreeSet<&str>) -> Result<SecretBytes, MaterialError> {
    let len = names
        .iter()
        .try_fold(SECRET_NAMES_PREFIX.len(), |len, name| {
            len.checked_add(8)?.checked_add(name.len())
        })
        .ok_or(MaterialError::Encoding)?;
    let mut bytes = SecretBytes::new(Vec::with_capacity(len));
    bytes.0.extend_from_slice(SECRET_NAMES_PREFIX);
    for name in names {
        bytes.0.extend_from_slice(&(name.len() as u64).to_le_bytes());
        bytes.0.extend_from_slice(name.as_bytes());
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "app_secrets_tests.rs"]
mod tests;
