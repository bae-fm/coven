//! Supported object formats, independent of the format selected for new objects.

use crate::Error;

/// The format of an object or a persisted upload attempt (§17.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatVersion {
    /// Appendix D's first format.
    V1,
}

impl FormatVersion {
    /// The format selected for a new object. Retries use their recorded value.
    pub const CURRENT: Self = Self::V1;

    /// The version number written after the object's kind.
    pub const fn number(self) -> u16 {
        match self {
            Self::V1 => 1,
        }
    }

    /// Read the format from a clear `kind | version` prefix, before parsing
    /// version-specific fields. The object's decoder checks the kind separately.
    pub fn decode(prefix: &[u8]) -> Result<Self, Error> {
        let bytes = prefix.get(..3).ok_or(Error::Truncated)?;
        match u16::from_be_bytes([bytes[1], bytes[2]]) {
            1 => Ok(Self::V1),
            version => Err(Error::UnsupportedVersion(version)),
        }
    }
}

#[cfg(test)]
#[path = "version_tests.rs"]
mod tests;
