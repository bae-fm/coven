//! An S3 location's base prefix: a folder path inside the bucket (§20.3).
//!
//! A person types it with or without slashes at its ends; they mean nothing,
//! so `/music/bae/` and `music/bae` are the same prefix. [`S3Prefix::parse`]
//! drops them once, where the location is built, and every request, policy
//! and listing after that uses the one canonical spelling.

use crate::{ObjectPath, ObjectPrefix};
use serde::{Deserialize, Deserializer, Serialize};

/// The longest folder name a base prefix may hold, in bytes.
const SEGMENT_LIMIT: usize = 255;

/// A base prefix in its canonical spelling: folder names joined by single
/// slashes, with no slash at either end. Empty is the bucket's root.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(into = "String")]
pub struct S3Prefix(String);

/// Why a typed base prefix was refused, naming the folder at fault where
/// there is one. It serializes, like `StorageFailure`, so an app can record
/// a refusal it showed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum S3PrefixError {
    /// Two slashes in a row, as in `music//bae`, leave a folder with no name.
    #[error("the S3 prefix has two slashes in a row")]
    EmptySegment,
    /// A folder name holds a character other than an ASCII letter or digit,
    /// `-` or `_`.
    #[error("the S3 prefix's folder `{segment}` contains `{character}`; folder names use only letters, digits, - and _")]
    InvalidCharacter {
        /// The folder name, as typed.
        segment: String,
        /// The first character it may not hold.
        character: char,
    },
    /// A folder name is longer than 255 bytes.
    #[error("the S3 prefix's folder `{segment}` is longer than 255 characters")]
    SegmentTooLong {
        /// The folder name, as typed.
        segment: String,
    },
}

impl S3Prefix {
    /// Parse a prefix as a person typed it. Slashes at either end are
    /// dropped, so a prefix of only slashes is the bucket's root, as an empty
    /// one is. What is left must be folder names of ASCII letters, digits,
    /// `-` and `_`, at most 255 bytes each, joined by single slashes; the
    /// first folder that is not is named in the error.
    pub fn parse(typed: &str) -> Result<Self, S3PrefixError> {
        let trimmed = typed.trim_matches('/');
        if !trimmed.is_empty() {
            for segment in trimmed.split('/') {
                check_segment(segment)?;
            }
        }
        Ok(Self(trimmed.to_owned()))
    }

    /// The canonical spelling: empty, or folder names joined by single slashes.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this is the bucket's root.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// What every key under this prefix starts with: nothing at the bucket's
    /// root, otherwise the prefix and a slash, so a sibling such as `dev-old`
    /// beside `dev` never matches.
    pub fn key_prefix(&self) -> String {
        if self.0.is_empty() {
            String::new()
        } else {
            format!("{}/", self.0)
        }
    }

    /// The S3 key of an object in the layout: its path under this prefix.
    pub fn key(&self, path: &ObjectPath) -> String {
        self.key_prefix() + path.as_str()
    }

    /// The S3 key prefix that selects these objects under this prefix.
    pub fn key_prefix_of(&self, objects: &ObjectPrefix) -> String {
        self.key_prefix() + objects.as_str()
    }
}

fn check_segment(segment: &str) -> Result<(), S3PrefixError> {
    if segment.is_empty() {
        return Err(S3PrefixError::EmptySegment);
    }
    if let Some(character) =
        segment.chars().find(|c| !(c.is_ascii_alphanumeric() || *c == '-' || *c == '_'))
    {
        return Err(S3PrefixError::InvalidCharacter { segment: segment.to_owned(), character });
    }
    if segment.len() > SEGMENT_LIMIT {
        return Err(S3PrefixError::SegmentTooLong { segment: segment.to_owned() });
    }
    Ok(())
}

impl std::fmt::Display for S3Prefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<S3Prefix> for String {
    fn from(prefix: S3Prefix) -> Self {
        prefix.0
    }
}

impl S3Prefix {
    /// Read a prefix coven recorded. It was written canonical, so only that
    /// spelling is accepted: a recorded `dev/` is damage, not a typed alias
    /// of `dev`.
    pub(crate) fn recorded(recorded: &str) -> Result<Self, crate::StorageError> {
        let prefix = Self::parse(recorded)
            .map_err(|error| crate::StorageFailure::Encoding.with_source(error))?;
        if prefix.0 != recorded {
            return Err(crate::StorageFailure::Encoding
                .with_source("a recorded S3 prefix has a slash at an end"));
        }
        Ok(prefix)
    }
}

impl<'de> Deserialize<'de> for S3Prefix {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::recorded(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[path = "s3_prefix_tests.rs"]
mod tests;
