use crate::{LostKey, RowId, WriteId};

/// Invalid input to the pure merge; no partially changed state is returned.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MergeError {
    /// Milliseconds do not fit in the timestamp's 48 bits.
    #[error("milliseconds exceed 48 bits: {0}")]
    MillisecondsOutOfRange(u64),
    /// The final millisecond and counter have both been used.
    #[error("timestamp space exhausted")]
    TimestampExhausted,
    /// Advancing a generation would exceed its representation.
    #[error("generation space exhausted")]
    GenerationExhausted,
    /// An insert was not made at an even generation, or an edit/delete at an odd one.
    #[error("operation has the wrong generation parity: {0}")]
    GenerationParity(u64),
    /// No write the author had read moved this row to the specified generation.
    #[error("generation {generation} of {row:?} was not read by the author")]
    GenerationNotSeen {
        /// The row, including its audience.
        row: RowId,
        /// The generation the change was made at.
        generation: u64,
    },
    /// Causality has not brought the receiving row to this generation.
    #[error("change to {row:?} is ahead of the receiving generation")]
    GenerationAhead {
        /// The receiving row.
        row: RowId,
    },
    /// A required write is missing from the applied set.
    #[error("write {0:?} has not been applied")]
    MissingWrite(WriteId),
    /// A write occurs twice in the input or has already been applied.
    #[error("write {0:?} occurs twice")]
    DuplicateWrite(WriteId),
    /// Two different writes have equal timestamps.
    #[error("writes {0:?} and {1:?} have equal timestamps")]
    DuplicateTimestamp(WriteId, WriteId),
    /// A timestamp is not larger than that of a write its author had read.
    #[error("write {0:?} is not stamped after its past")]
    CausalTimestamp(WriteId),
    /// The timestamp's device is different from the write's device.
    #[error("timestamp device differs from write {0:?}")]
    TimestampDevice(WriteId),
    /// A write's row changes do not include the row passed to `apply`.
    #[error("write has no change for {0:?}")]
    MissingChange(RowId),
    /// A parent generation must name an existing incarnation, an odd generation.
    #[error("parent generation is not odd: {0}")]
    ParentGeneration(u64),
    /// A reference reaches an audience some readers of the child cannot read.
    #[error("reference from {0:?} reaches an unreadable audience")]
    ReferenceAudience(RowId),
    /// The caller omitted the current generation of a substituted default parent.
    #[error("default parent generation was not supplied for {0:?}")]
    MissingDefaultGeneration(RowId),
    /// A removal view omitted a parent from the region it declared closed.
    #[error("removal region does not contain parent {0:?}")]
    RegionNotClosed(RowId),
    /// The removal view listed a row more than once.
    #[error("removal view lists {0:?} twice")]
    DuplicateRow(RowId),
    /// Stored generations must be contiguous from one through the current one.
    #[error("stored generation {0} is missing or out of order")]
    GenerationGap(u64),
    /// Each generation must be stamped after the preceding generation.
    #[error("stored generation {0} is not stamped after its predecessor")]
    GenerationTimestamp(u64),
    /// A deleted row cannot retain winning cells.
    #[error("deleted row has winning cells")]
    DeletedRowHasCells,
    /// A cell's setter cannot precede the start of its incarnation.
    #[error("cell {0} precedes its incarnation")]
    CellBeforeIncarnation(String),
    /// A lost record has an invalid incarnation or canonical replacer.
    #[error("invalid lost value {0:?}")]
    InvalidLostValue(LostKey),
    /// A required cell of a unique constraint has never been set.
    #[error("unique constraint column {0} has no winning value")]
    MissingClaimColumn(String),
    /// A unique constraint must have at least one column.
    #[error("unique constraint has no columns")]
    EmptyClaim,
}
