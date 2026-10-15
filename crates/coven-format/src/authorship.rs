//! Authorship leaves of a write part, its keyed tree and multiproofs (D11).
//!
//! Each part commits to its original creations, cells, deletions and SQL
//! operations as separate leaves, so a snapshot can prove one surviving cell
//! or a row's creation without revealing values that were later overwritten.
//! Hashing is keyed with the part's audience key; the tree splits like
//! RFC 6962, the left side holding the largest power of two below the count.

use crate::error::{bound, require, Error, Rule};
use crate::value::{name, row, Value};
use crate::wire::{get_blob, put_blob, wire_struct, Decoder, Encoder, Wire, MAX_ITEMS};
use crate::write::{PartRecord, RowChange, RowExecution, Step, WritePart};
use crate::write_sources::ChangeId;
use coven_merge::{ColumnValue, Operation, RowId};

/// Keyed hashing of one part's leaves. Branches are unkeyed (D11).
pub trait PartHasher {
    /// One leaf's encoded bytes.
    fn leaf(&self, leaf: &[u8]) -> [u8; 32];
}

impl PartHasher for coven_crypto::AuthorshipHasher {
    fn leaf(&self, leaf: &[u8]) -> [u8; 32] {
        coven_crypto::AuthorshipHasher::leaf(self, leaf)
    }
}

use coven_crypto::{authorship_empty, authorship_node};

/// Whether a cell was set by an insert or an update.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CellKind {
    /// The incarnation's first values.
    Insert,
    /// A later change in the same incarnation.
    Update,
}

/// One authenticated original fact of a part (D11).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Leaf {
    /// An insert's creation marker, with the original creation it copies.
    /// It carries no values, so it stays openable after every one changes.
    Creation {
        /// The inserted row.
        row: RowId,
        /// The generation before the insert.
        generation: u64,
        /// The copied row's original creation, for an audience copy.
        creator: Option<ChangeId>,
    },
    /// One column an insert or update set.
    Cell {
        /// The changed row.
        row: RowId,
        /// The generation before the change.
        generation: u64,
        /// Insert or update.
        kind: CellKind,
        /// The column.
        column: String,
        /// Its value and recorded parents.
        value: ColumnValue<Value>,
    },
    /// A delete's marker.
    Deletion {
        /// The deleted row.
        row: RowId,
        /// The generation before the delete.
        generation: u64,
    },
    /// One SQL operation on the row, in execution order.
    Step {
        /// The row the operation changed.
        row: RowId,
        /// The operation.
        step: Step,
    },
}
impl Wire for Leaf {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Creation {
                row,
                generation,
                creator,
            } => {
                0u8.put(out)?;
                row.put(out)?;
                generation.put(out)?;
                creator.put(out)
            }
            Self::Cell {
                row,
                generation,
                kind,
                column,
                value,
            } => {
                1u8.put(out)?;
                row.put(out)?;
                generation.put(out)?;
                (*kind as u8).put(out)?;
                column.put(out)?;
                value.put(out)
            }
            Self::Deletion { row, generation } => {
                2u8.put(out)?;
                row.put(out)?;
                generation.put(out)
            }
            Self::Step { row, step } => {
                3u8.put(out)?;
                row.put(out)?;
                step.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Creation {
                row: Wire::get(input)?,
                generation: Wire::get(input)?,
                creator: Wire::get(input)?,
            }),
            1 => Ok(Self::Cell {
                row: Wire::get(input)?,
                generation: Wire::get(input)?,
                kind: match u8::get(input)? {
                    0 => CellKind::Insert,
                    1 => CellKind::Update,
                    tag => {
                        return Err(Error::UnknownTag {
                            field: "cell kind",
                            tag,
                        })
                    }
                },
                column: crate::wire::get_name(input)?,
                value: Wire::get(input)?,
            }),
            2 => Ok(Self::Deletion {
                row: Wire::get(input)?,
                generation: Wire::get(input)?,
            }),
            3 => Ok(Self::Step {
                row: Wire::get(input)?,
                step: Wire::get(input)?,
            }),
            tag => Err(Error::UnknownTag {
                field: "authorship leaf",
                tag,
            }),
        }
    }
}
impl Leaf {
    /// The leaf's canonical bytes, the input of its keyed hash.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut out = Encoder::new();
        self.put(&mut out)?;
        Ok(out.bytes)
    }

    /// Decode exactly one leaf.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut input = Decoder::new(bytes)?;
        let leaf = Self::get(&mut input)?;
        input.finish()?;
        leaf.validate()?;
        Ok(leaf)
    }

    /// The row the leaf describes.
    pub fn row(&self) -> &RowId {
        match self {
            Self::Creation { row, .. }
            | Self::Cell { row, .. }
            | Self::Deletion { row, .. }
            | Self::Step { row, .. } => row,
        }
    }

    fn validate(&self) -> Result<(), Error> {
        row(self.row())?;
        match self {
            Self::Cell { column, value, .. } => {
                name(column)?;
                crate::merge_wire::column(value)
            }
            Self::Step { row, step } => crate::write::Execution {
                steps: vec![step.clone()],
                sources: Default::default(),
            }
            .validate(row),
            Self::Creation { .. } | Self::Deletion { .. } => Ok(()),
        }
    }
}

/// A net change's leaves, then one per operation (D11).
pub fn change_leaves(change: &RowChange) -> Vec<Leaf> {
    let row = &change.row;
    let generation = change.change.generation;
    let mut leaves = Vec::new();
    let cells = |kind, columns: &std::collections::BTreeMap<String, ColumnValue<Value>>| {
        columns
            .iter()
            .map(move |(column, value)| Leaf::Cell {
                row: row.clone(),
                generation,
                kind,
                column: column.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>()
    };
    match &change.change.operation {
        Operation::Insert(columns) => {
            leaves.push(Leaf::Creation {
                row: row.clone(),
                generation,
                creator: change.creator.clone(),
            });
            leaves.extend(cells(CellKind::Insert, columns));
        }
        Operation::Update(columns) => leaves.extend(cells(CellKind::Update, columns)),
        Operation::Delete => leaves.push(Leaf::Deletion {
            row: row.clone(),
            generation,
        }),
    }
    leaves.extend(step_leaves(row, &change.execution.steps));
    leaves
}

/// An execution-only row's leaves: one per operation.
pub fn execution_leaves(execution: &RowExecution) -> Vec<Leaf> {
    step_leaves(&execution.row, &execution.execution.steps).collect()
}

fn step_leaves<'a>(row: &'a RowId, steps: &'a [Step]) -> impl Iterator<Item = Leaf> + 'a {
    steps.iter().map(|step| Leaf::Step {
        row: row.clone(),
        step: step.clone(),
    })
}

/// One record's leaves.
pub fn record_leaves(record: PartRecord<'_>) -> Vec<Leaf> {
    match record {
        PartRecord::Change(change) => change_leaves(change),
        PartRecord::Execution(execution) => execution_leaves(execution),
    }
}

/// Every leaf of a part, in its stream's record order. Dismissals have none.
pub fn part_leaves(part: &WritePart) -> Vec<Leaf> {
    part.records().flat_map(record_leaves).collect()
}

/// The size of a tree's left subtree: the largest power of two below `count`.
fn split(count: u64) -> u64 {
    debug_assert!(count > 1);
    1 << (63 - (count - 1).leading_zeros())
}

/// Computes a part's root from its leaves in order, keeping one hash per
/// complete subtree, so a streamed part never holds its leaves.
pub struct RootBuilder<'h, H: PartHasher> {
    hasher: &'h H,
    /// Perfect subtrees, largest first, with their sizes.
    stack: Vec<([u8; 32], u64)>,
}
impl<'h, H: PartHasher> RootBuilder<'h, H> {
    /// Start an empty part.
    pub fn new(hasher: &'h H) -> Self {
        Self {
            hasher,
            stack: Vec::new(),
        }
    }

    /// Add the next leaf, returning its keyed hash.
    pub fn push(&mut self, leaf: &Leaf) -> Result<[u8; 32], Error> {
        let hash = self.hasher.leaf(&leaf.encode()?);
        self.stack = push_hash(std::mem::take(&mut self.stack), hash);
        Ok(hash)
    }

    /// The root over every leaf pushed.
    pub fn finish(self) -> [u8; 32] {
        finish_hashes(self.stack)
    }
}

fn push_hash(mut stack: Vec<([u8; 32], u64)>, mut hash: [u8; 32]) -> Vec<([u8; 32], u64)> {
    let mut size = 1;
    while stack.last().is_some_and(|(_, top)| *top == size) {
        let (left, _) = stack.pop().expect("checked");
        hash = authorship_node(&left, &hash);
        size *= 2;
    }
    stack.push((hash, size));
    stack
}

fn finish_hashes(stack: Vec<([u8; 32], u64)>) -> [u8; 32] {
    let mut stack = stack.into_iter().rev();
    let Some((mut root, _)) = stack.next() else {
        return authorship_empty();
    };
    for (left, _) in stack {
        root = authorship_node(&left, &root);
    }
    root
}

/// The root over a part's leaf hashes, without its key.
pub fn root_of_hashes(hashes: &[[u8; 32]]) -> [u8; 32] {
    finish_hashes(
        hashes
            .iter()
            .fold(Vec::new(), |stack, hash| push_hash(stack, *hash)),
    )
}

/// The root of a part over its leaves.
pub fn root(hasher: &impl PartHasher, leaves: &[Leaf]) -> Result<[u8; 32], Error> {
    let mut builder = RootBuilder::new(hasher);
    for leaf in leaves {
        builder.push(leaf)?;
    }
    Ok(builder.finish())
}

/// Opened leaves of one part and one hash per maximal unopened subtree, in
/// left-to-right order (D7). The tree's shape follows from `leaves`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Multiproof {
    /// How many leaves the part has.
    pub leaves: u64,
    /// The opened leaves with their positions, in increasing position order.
    pub revealed: Vec<(u64, Vec<u8>)>,
    /// The roots of the subtrees with no opened leaf.
    pub hidden: Vec<[u8; 32]>,
}
impl Wire for Multiproof {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.leaves.put(out)?;
        bound(self.revealed.len(), MAX_ITEMS, "collection")?;
        out.charge_items(self.revealed.len())?;
        (self.revealed.len() as u32).put(out)?;
        for (index, leaf) in &self.revealed {
            index.put(out)?;
            put_blob(leaf, out)?;
        }
        self.hidden.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self {
            leaves: Wire::get(input)?,
            revealed: crate::wire::get_sequence(input, |input| {
                Ok((u64::get(input)?, get_blob(input)?))
            })?,
            hidden: Wire::get(input)?,
        })
    }
}
impl Multiproof {
    /// Open the leaves `keep` selects, hiding each subtree that has none.
    pub fn prove(
        hasher: &impl PartHasher,
        leaves: &[Leaf],
        keep: impl Fn(&Leaf) -> bool,
    ) -> Result<Self, Error> {
        let encoded = leaves
            .iter()
            .map(Leaf::encode)
            .collect::<Result<Vec<_>, _>>()?;
        let hashes: Vec<_> = encoded.iter().map(|leaf| hasher.leaf(leaf)).collect();
        let opened = leaves
            .iter()
            .zip(encoded)
            .map(|(leaf, bytes)| keep(leaf).then_some(bytes))
            .collect::<Vec<_>>();
        Ok(Self::from_hashes(&hashes, opened))
    }

    /// Open the leaves given with their bytes, hiding each subtree that has
    /// none by its root over the part's retained leaf hashes, without a key.
    pub fn from_hashes(hashes: &[[u8; 32]], opened: Vec<Option<Vec<u8>>>) -> Self {
        debug_assert_eq!(hashes.len(), opened.len());
        let mut proof = Self {
            leaves: hashes.len() as u64,
            revealed: Vec::new(),
            hidden: Vec::new(),
        };
        if !hashes.is_empty() {
            prune(hashes, &opened, 0, &mut proof);
        }
        proof
    }

    /// Every opened leaf, decoded.
    pub fn opened(&self) -> Result<Vec<Leaf>, Error> {
        self.revealed
            .iter()
            .map(|(_, leaf)| Leaf::decode(leaf))
            .collect()
    }

    /// The root these openings and hashes reconstruct. Positions must be in
    /// range and increasing, and every hash must be used exactly once.
    pub fn root(&self, hasher: &impl PartHasher) -> Result<[u8; 32], Error> {
        require(
            self.revealed.windows(2).all(|pair| pair[0].0 < pair[1].0)
                && self.revealed.last().is_none_or(|(i, _)| *i < self.leaves),
            "multiproof positions",
            Rule::Order,
        )?;
        for (_, leaf) in &self.revealed {
            Leaf::decode(leaf)?;
        }
        if self.leaves == 0 {
            require(
                self.revealed.is_empty() && self.hidden.is_empty(),
                "multiproof hashes",
                Rule::StreamLength,
            )?;
            return Ok(authorship_empty());
        }
        let mut revealed = self.revealed.iter().peekable();
        let mut hidden = self.hidden.iter();
        let root = rebuild(hasher, 0, self.leaves, &mut revealed, &mut hidden)?;
        require(
            revealed.next().is_none() && hidden.next().is_none(),
            "multiproof hashes",
            Rule::StreamLength,
        )?;
        Ok(root)
    }
}

fn subtree(hashes: &[[u8; 32]]) -> [u8; 32] {
    if hashes.len() == 1 {
        return hashes[0];
    }
    let (left, right) = hashes.split_at(split(hashes.len() as u64) as usize);
    authorship_node(&subtree(left), &subtree(right))
}

fn prune(hashes: &[[u8; 32]], opened: &[Option<Vec<u8>>], start: u64, proof: &mut Multiproof) {
    if opened.iter().all(Option::is_none) {
        proof.hidden.push(subtree(hashes));
    } else if hashes.len() == 1 {
        proof
            .revealed
            .push((start, opened[0].clone().expect("an opened leaf")));
    } else {
        let left = split(hashes.len() as u64) as usize;
        prune(&hashes[..left], &opened[..left], start, proof);
        prune(&hashes[left..], &opened[left..], start + left as u64, proof);
    }
}

fn rebuild<'a>(
    hasher: &impl PartHasher,
    start: u64,
    count: u64,
    revealed: &mut std::iter::Peekable<impl Iterator<Item = &'a (u64, Vec<u8>)>>,
    hidden: &mut impl Iterator<Item = &'a [u8; 32]>,
) -> Result<[u8; 32], Error> {
    let opens = revealed
        .peek()
        .is_some_and(|(index, _)| *index < start + count);
    if !opens {
        return hidden.next().copied().ok_or(Error::Invalid {
            field: "multiproof hashes",
            rule: Rule::StreamLength,
        });
    }
    if count == 1 {
        let (_, leaf) = revealed.next().expect("checked");
        return Ok(hasher.leaf(leaf));
    }
    let left = split(count);
    let left_hash = rebuild(hasher, start, left, revealed, hidden)?;
    let right_hash = rebuild(hasher, start + left, count - left, revealed, hidden)?;
    Ok(authorship_node(&left_hash, &right_hash))
}

/// A write's evidence for its part in one audience (D7 section 7): the
/// cleartext prefix, the sealed header chunk, every part root, the header
/// signature and a multiproof of that part's needed leaves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteEvidence {
    /// The write.
    pub write: coven_merge::WriteId,
    /// The exact cleartext `kind | version | prefix` bytes.
    pub prefix: Vec<u8>,
    /// The sealed header chunk exactly as stored, with its length field.
    pub header: Vec<u8>,
    /// Every part's root, in header order.
    pub roots: Vec<[u8; 32]>,
    /// The header signature over these bytes (D9).
    pub signature: [u8; 64],
    /// The openings in the snapshot audience's part.
    pub proof: Multiproof,
}
impl Wire for WriteEvidence {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.write.put(out)?;
        put_blob(&self.prefix, out)?;
        put_blob(&self.header, out)?;
        self.roots.put(out)?;
        self.signature.put(out)?;
        self.proof.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self {
            write: Wire::get(input)?,
            prefix: get_blob(input)?,
            header: get_blob(input)?,
            roots: Wire::get(input)?,
            signature: Wire::get(input)?,
            proof: Wire::get(input)?,
        })
    }
}
impl WriteEvidence {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::value::positive(self.write.number)?;
        let prefix = crate::sealed_write::WriteObjectPrefix::decode(&self.prefix)?;
        require(
            prefix.part_keys.len() == self.roots.len(),
            "write evidence roots",
            Rule::StreamLength,
        )?;
        crate::sealed_write::WriteObjectPrefix::header_chunk(&self.header)?;
        for (_, leaf) in &self.proof.revealed {
            Leaf::decode(leaf)?;
        }
        Ok(())
    }
}

/// A row's selected original creation (D7 section 6): the generation-zero
/// insert with the smallest timestamp, and the creation it names as its
/// owner, which differs only for an audience copy (§8.8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Creation {
    /// The row.
    pub row: RowId,
    /// The selected generation-zero insert's write.
    pub write: coven_merge::WriteId,
    /// The original creation: this insert itself, or the creation it copied.
    pub creator: ChangeId,
}
wire_struct!(Creation, row, write, creator);
impl Creation {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        crate::value::positive(self.write.number)?;
        crate::value::positive(self.creator.write.number)?;
        // The creator keeps its original write's table name across renames.
        require(
            self.creator.row.key == self.row.key,
            "creation row",
            Rule::Required,
        )?;
        require(
            (self.creator.write == self.write) == (self.creator.row.audience == self.row.audience),
            "creation copy",
            Rule::Required,
        )
    }
}

#[cfg(test)]
#[path = "authorship_tests.rs"]
mod tests;
