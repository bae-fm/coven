//! Object-check verdicts shared by immediate errors and recorded reports (§19.1).

use coven_crypto::CryptoError;
use coven_database::DbError;
use coven_format::pending::RefusalCode;
use coven_merge::MergeError;
use std::{error::Error, fmt, sync::Arc};

/// One vocabulary for refused objects. Native causes survive immediate calls;
/// persisted and peer reports retain only the variant. Equality ignores causes.
#[derive(Clone, Debug)]
pub enum Refusal {
    /// Opening the object or its authenticated path failed.
    Decryption {
        /// The native check failure, absent on a recorded report.
        cause: Option<Arc<CryptoError>>,
    },
    /// The author's signature did not verify.
    Signature {
        /// The native signature failure, absent on a recorded report.
        cause: Option<Arc<CryptoError>>,
    },
    /// The bytes violate their format.
    Parse {
        /// The native decoder failure, absent on a recorded report.
        cause: Option<Arc<dyn Error + Send + Sync>>,
    },
    /// The authenticated write failed merge or application-schema checks.
    InvalidWrite {
        /// The native validation failure, absent on a recorded report.
        cause: Option<Arc<DbError>>,
    },
    /// The author's recorded view does not authorize this object.
    NotAuthorized,
    /// The timestamp or recorded past violates causality.
    InvalidCausality {
        /// The native merge failure, if this check produced one.
        cause: Option<Arc<MergeError>>,
    },
    /// The object's identity disagrees with its path, author or key material.
    WrongIdentity {
        /// The native format, merge or key-material failure, if one was produced.
        cause: Option<Arc<dyn Error + Send + Sync>>,
    },
    /// Authenticated file plaintext disagrees with the row's content hash.
    ContentHash,
}

impl PartialEq for Refusal {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}
impl Eq for Refusal {}

impl Error for Refusal {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decryption { cause } | Self::Signature { cause } => {
                cause.as_deref().map(|e| e as &dyn Error)
            }
            Self::Parse { cause } | Self::WrongIdentity { cause } => {
                cause.as_deref().map(|e| e as &dyn Error)
            }
            Self::InvalidWrite { cause } => cause.as_deref().map(|e| e as &dyn Error),
            Self::InvalidCausality { cause } => cause.as_deref().map(|e| e as &dyn Error),
            Self::NotAuthorized | Self::ContentHash => None,
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Decryption { .. } => "decryption failed",
            Self::Signature { .. } => "signature failed",
            Self::Parse { .. } => "parse failed",
            Self::InvalidWrite { .. } => "invalid write",
            Self::NotAuthorized => "object is not authorized",
            Self::InvalidCausality { .. } => "invalid causality",
            Self::WrongIdentity { .. } => "wrong object identity",
            Self::ContentHash => "file content hash differs",
        })?;
        if let Some(cause) = self.source() {
            write!(f, ": {cause}")?;
        }
        Ok(())
    }
}

impl From<coven_format::Error> for Refusal {
    fn from(error: coven_format::Error) -> Self {
        match error {
            coven_format::Error::Merge(
                error @ (MergeError::CausalTimestamp(_)
                | MergeError::CausalClosure(_)
                | MergeError::DuplicateTimestamp(_, _)),
            ) => Self::InvalidCausality {
                cause: Some(Arc::new(error)),
            },
            coven_format::Error::Merge(error @ MergeError::TimestampDevice(_)) => {
                Self::WrongIdentity {
                    cause: Some(Arc::new(error)),
                }
            }
            error @ coven_format::Error::Invalid {
                rule: coven_format::error::Rule::TimestampDevice,
                ..
            } => Self::WrongIdentity {
                cause: Some(Arc::new(error)),
            },
            error => Self::Parse {
                cause: Some(Arc::new(error)),
            },
        }
    }
}

// The format crate cannot depend on DbError's owning crate. Its wire
// value represents exactly the same variants without process-local causes.
impl From<&Refusal> for RefusalCode {
    fn from(failure: &Refusal) -> Self {
        match failure {
            Refusal::Decryption { .. } => Self::Decryption,
            Refusal::Signature { .. } => Self::Signature,
            Refusal::Parse { .. } => Self::Parse,
            Refusal::InvalidWrite { .. } => Self::InvalidWrite,
            Refusal::NotAuthorized => Self::NotAuthorized,
            Refusal::InvalidCausality { .. } => Self::InvalidCausality,
            Refusal::WrongIdentity { .. } => Self::WrongIdentity,
            Refusal::ContentHash => Self::ContentHash,
        }
    }
}

impl From<RefusalCode> for Refusal {
    fn from(failure: RefusalCode) -> Self {
        match failure {
            RefusalCode::Decryption => Self::Decryption { cause: None },
            RefusalCode::Signature => Self::Signature { cause: None },
            RefusalCode::Parse => Self::Parse { cause: None },
            RefusalCode::InvalidWrite => Self::InvalidWrite { cause: None },
            RefusalCode::NotAuthorized => Self::NotAuthorized,
            RefusalCode::InvalidCausality => Self::InvalidCausality { cause: None },
            RefusalCode::WrongIdentity => Self::WrongIdentity { cause: None },
            RefusalCode::ContentHash => Self::ContentHash,
        }
    }
}

#[cfg(test)]
#[path = "refusal_tests.rs"]
mod tests;
