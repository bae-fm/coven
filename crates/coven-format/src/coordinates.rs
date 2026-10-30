//! Where a file is, as its record carries it (format.md D12): in storage at a
//! fixed location, or only on one device.

use crate::{error::Rule, file_reference::FileReference, Error};
use coven_crypto::SecretText;
use coven_foundation::id_source::DeviceId;

/// A file's one place: never both and never neither.
#[derive(Debug)]
pub enum Coordinates {
    /// Stored at `<store>/files/<device>/<id>`, readable with the key.
    Cloud(FileReference),
    /// Only on this device, which no other device can read it from.
    DeviceOnly(DeviceId),
}

impl Coordinates {
    /// Decode `file <device> <id> <key>` or `device <device>`, each in its
    /// one canonical spelling.
    #[expect(
        clippy::map_err_ignore,
        reason = "malformed coordinates are invalid as a whole"
    )]
    pub fn decode(text: &str) -> Result<Self, Error> {
        if let Some(device) = text.strip_prefix("device ") {
            let number: u64 = device.parse().map_err(|_| invalid())?;
            if number.to_string() != device {
                return Err(invalid());
            }
            return Ok(Self::DeviceOnly(DeviceId(number)));
        }
        FileReference::decode(text)
            .map(Self::Cloud)
            .map_err(|_| invalid())
    }

    /// Encode into secret text, so diagnostics never reveal a file key.
    pub fn encode(&self) -> SecretText {
        match self {
            Self::Cloud(reference) => reference.encode(),
            Self::DeviceOnly(device) => SecretText::new(format!("device {}", device.0)),
        }
    }
}

fn invalid() -> Error {
    Error::Invalid {
        field: "file coordinates",
        rule: Rule::KeyEncoding,
    }
}

#[cfg(test)]
#[path = "coordinates_tests.rs"]
mod tests;
