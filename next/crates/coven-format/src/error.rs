//! Failures to encode or decode a plaintext object.

/// A structural requirement a plaintext object did not meet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// A text code has the wrong prefix, or a frame has an unexpected kind.
    Kind,
    /// Base32 length or unused trailing bits are not canonical.
    CodePadding,
    /// A required name, collection or number was empty or zero.
    Required,
    /// A set was not in strictly increasing order, or repeated an identity.
    Order,
    /// A removed row is absent from merged state.
    Generation,
    /// A timestamp named a different device than its write or entry.
    TimestampDevice,
    /// A had-read set included an invalid position for its own device.
    OwnPosition,
    /// A part, row or removal rule named an audience it cannot contain or describe.
    Audience,
    /// Old and new column values did not match the operation.
    ColumnOperation,
    /// A foreign key names different numbers of source and target columns.
    ForeignKeyColumns,
    /// A real number was NaN or encoded negative zero.
    Real,
    /// A row key contained null.
    NullKey,
    /// A key has a noncanonical numeric or escaped byte representation.
    KeyEncoding,
    /// A lost write's disposition disagrees with its recorded cause.
    LostWriteCause,
    /// A snapshot record was outside the positions the snapshot covers.
    Coverage,
    /// Snapshot sections, counts or their end marker did not match the header.
    SnapshotSequence,
    /// A stream's declared byte length or row count disagrees with its frames.
    StreamLength,
    /// A chunk's index or byte length did not match its file header.
    Chunk,
}

/// A typed failure, preserving the field and the failed structural requirement.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Merge rejected a row change, written reference or decoded row state.
    Merge(coven_merge::MergeError),
    /// Crypto rejected encoded secret material.
    Material(coven_crypto::MaterialError),
    /// Crypto rejected an invalid or weak Ed25519 member identity.
    InvalidMemberId,
    /// More bytes were required to finish the object.
    Truncated,
    /// Bytes remained after the end of an object.
    TrailingBytes,
    /// The object's format version is not supported.
    UnsupportedVersion(u16),
    /// A kind or variant tag is unknown.
    UnknownTag {
        /// The field containing the tag.
        field: &'static str,
        /// The unrecognized tag.
        tag: u8,
    },
    /// A length exceeds the format's bound.
    Limit {
        /// The field or resource whose bound was exceeded.
        field: &'static str,
        /// The declared or requested length.
        actual: usize,
        /// The largest accepted length.
        maximum: usize,
    },
    /// A string was not UTF-8.
    Utf8,
    /// An object failed a structural check.
    Invalid {
        /// The field that failed the check.
        field: &'static str,
        /// The violated requirement.
        rule: Rule,
    },
    /// A typed code contained a character outside its uppercase base32 alphabet.
    CodeCharacter {
        /// The byte offset of the character.
        offset: usize,
    },
    /// A typed code's checksum did not match its contents.
    Checksum,
    /// Memory could not be reserved within the format's bounds.
    Allocation,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Merge(e) => write!(f, "invalid merge input: {e}"),
            Self::Material(e) => write!(f, "invalid secret material: {e}"),
            Self::InvalidMemberId => f.write_str("invalid or weak member public key"),
            Self::Truncated => f.write_str("object is truncated"),
            Self::TrailingBytes => f.write_str("bytes follow the object's end"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported format version {v}"),
            Self::UnknownTag { field, tag } => write!(f, "unknown {field} tag {tag}"),
            Self::Limit {
                field,
                actual,
                maximum,
            } => write!(f, "{field} length {actual} exceeds {maximum}"),
            Self::Utf8 => f.write_str("text is not UTF-8"),
            Self::Invalid { field, rule } => write!(f, "{field} violates {rule:?}"),
            Self::CodeCharacter { offset } => write!(f, "invalid code character at byte {offset}"),
            Self::Checksum => f.write_str("code checksum does not match"),
            Self::Allocation => f.write_str("could not reserve memory for the object"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Merge(e) => Some(e),
            Self::Material(e) => Some(e),
            _ => None,
        }
    }
}

pub(crate) fn require(ok: bool, field: &'static str, rule: Rule) -> Result<(), Error> {
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid { field, rule })
    }
}

pub(crate) fn bound(n: usize, maximum: usize, field: &'static str) -> Result<(), Error> {
    if n <= maximum {
        Ok(())
    } else {
        Err(Error::Limit {
            field,
            actual: n,
            maximum,
        })
    }
}
