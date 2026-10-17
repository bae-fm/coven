//! Another member's authorship of the creations and cells a snapshot carries
//! (§9.6, §16.1, D7, D9, D11).
//!
//! A snapshot's own signature covers what its author's member wrote. For every
//! owned row created, or cell set, by another member's write, the snapshot
//! carries that write's signed header and an opening of the needed leaves in
//! the snapshot audience's part, so it cannot invent their authorship. A
//! migration write's changes have no leaves; its authenticated header shows
//! it is one. Openings need only leaf hashes: a device that received a part
//! whole keeps them and can open any leaf again without the audience key.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_crypto::AuthorshipHasher;
use coven_format::authorship::{CellKind, Leaf, Multiproof, WriteEvidence};
use coven_format::merge_fields;
use coven_format::snapshot_rows::OriginalRow;
use coven_format::write::WriteDisposition;
use coven_format::write_stream::WriteHeaderFrame;
use coven_foundation::id_source::{KeyId, MemberId};
use coven_merge::{Audience, ColumnValue, RowId, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

/// What a loader needs from sync to authenticate another member's write.
pub trait Authorship: Send + Sync {
    /// Open the evidence's sealed header with the store key its prefix names,
    /// and verify its header signature with the signing key of its device's
    /// member, current in the header's recorded prefix (D9).
    fn header(&self, evidence: &WriteEvidence) -> Result<WriteHeaderFrame, DbError>;

    /// The keyed leaf hasher of an audience's part sealed with this key (D11).
    fn hasher(&self, audience: &Audience, key: KeyId) -> Result<AuthorshipHasher, DbError>;
}

/// A write's signed header and every part root, retained from receipt or a
/// loaded snapshot (D9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderEvidence {
    /// The exact cleartext `kind | version | prefix` bytes.
    pub prefix: Vec<u8>,
    /// The sealed header chunk exactly as stored, with its length field.
    pub header: Vec<u8>,
    /// Every part's root, in header order.
    pub roots: Vec<[u8; 32]>,
    /// The header signature.
    pub signature: [u8; 64],
}

pub(crate) fn retain_header(
    db: &DatabaseConnection,
    write: WriteId,
    evidence: &HeaderEvidence,
) -> Result<(), DbError> {
    let roots: Vec<u8> = evidence.roots.iter().flatten().copied().collect();
    db.internal_execute(
        "INSERT INTO _coven_write_evidence(write_id,prefix,header,roots,signature) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(write_id) DO NOTHING",
        params![
            encoded(merge_fields::encode_write_id(&write))?,
            evidence.prefix,
            evidence.header,
            roots,
            evidence.signature.as_slice()
        ],
    )?;
    Ok(())
}

fn header(db: &DatabaseConnection, write: WriteId) -> Result<Option<HeaderEvidence>, DbError> {
    Ok(db
        .query(
            "SELECT prefix,header,roots,signature FROM _coven_write_evidence WHERE write_id=?1",
            [encoded(merge_fields::encode_write_id(&write))?],
            |r| {
                let roots: Vec<u8> = r.get(2)?;
                let signature: Vec<u8> = r.get(3)?;
                Ok(HeaderEvidence {
                    prefix: r.get(0)?,
                    header: r.get(1)?,
                    roots: roots
                        .chunks_exact(32)
                        .map(|root| root.try_into().expect("32 bytes"))
                        .collect(),
                    signature: signature.try_into().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            3,
                            "signature".into(),
                            rusqlite::types::Type::Blob,
                        )
                    })?,
                })
            },
        )?
        .pop())
}

/// Keep a received part's leaf hashes, so its leaves can be opened later.
pub(crate) fn retain_leaves(
    db: &DatabaseConnection,
    write: WriteId,
    audience: &Audience,
    hashes: &[[u8; 32]],
) -> Result<(), DbError> {
    let hashes: Vec<u8> = hashes.iter().flatten().copied().collect();
    db.internal_execute(
        "INSERT INTO _coven_part_leaves(write_id,audience,hashes) VALUES(?1,?2,?3) ON CONFLICT(write_id,audience) DO NOTHING",
        params![
            encoded(merge_fields::encode_write_id(&write))?,
            audience_text(audience),
            hashes
        ],
    )?;
    Ok(())
}

/// What the snapshot audience's rows need opened in one write.
#[derive(Default)]
struct Needs {
    creations: BTreeSet<RowId>,
    /// Row, current column, value and the row's present generation.
    cells: Vec<(RowId, String, ColumnValue<coven_format::value::Value>, u64)>,
}

/// Every owned row this device holds came from a whole received part or a
/// snapshot's opening of it, so its evidence is retained.
fn missing(write: WriteId) -> DbError {
    tracing::error!(
        ?write,
        "another member's write lacks retained authorship evidence"
    );
    DbError::Invariant {
        what: "another member's write that set an owned row keeps its evidence",
    }
}

/// A retained write's plaintext header.
fn written(
    db: &DatabaseConnection,
    write: WriteId,
) -> Result<Option<coven_format::write::WriteHeader>, DbError> {
    if let Some(frame) = crate::write_headers::read(db, write)? {
        return Ok(Some(frame.header));
    }
    let header: Option<Vec<u8>> = db
        .query(
            "SELECT header FROM _coven_authored_parts WHERE write_id=?1 LIMIT 1",
            [encoded(merge_fields::encode_write_id(&write))?],
            |r| r.get(0),
        )?
        .pop();
    header
        .map(|bytes| Ok(merge_fields::decode_write_header(&bytes)?))
        .transpose()
}

/// Who signed a snapshot: its device, and that device's member when known.
/// The signature itself vouches for whatever they wrote (§16.2).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Author {
    pub(crate) device: coven_foundation::id_source::DeviceId,
    pub(crate) member: Option<MemberId>,
}

impl Author {
    /// Whether another member's write made this change: the snapshot needs
    /// that write's evidence for it.
    fn other(&self, db: &DatabaseConnection, write: WriteId) -> Result<bool, DbError> {
        Ok(write.device != self.device
            && (self.member.is_none()
                || crate::ownership::member(db, write.device)? != self.member))
    }
}

/// Whether a leaf names this row, read through the write's schema version.
fn same_row(
    db: &DatabaseConnection,
    version: u32,
    current: u32,
    leaf: &RowId,
    row: &RowId,
) -> Result<bool, DbError> {
    Ok(leaf.key == row.key
        && leaf.audience == row.audience
        && crate::source_names::table(db, version, &leaf.table, current)?.as_deref()
            == Some(row.table.as_str()))
}

/// Whether a leaf proves this need. The leaves name rows and columns as the
/// write did, before any later rename.
fn proves(
    db: &DatabaseConnection,
    version: u32,
    current: u32,
    leaf: &Leaf,
    needs: &Needs,
    creators: &BTreeMap<RowId, Option<coven_format::write_sources::ChangeId>>,
) -> Result<bool, DbError> {
    match leaf {
        Leaf::Creation {
            row,
            generation: 0,
            creator,
        } => {
            for needed in &needs.creations {
                if same_row(db, version, current, row, needed)?
                    && creators.get(needed) == Some(creator)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Leaf::Cell {
            row,
            generation,
            kind,
            column,
            value,
        } => {
            let incarnation = match kind {
                CellKind::Insert => generation + 1,
                CellKind::Update => *generation,
            };
            for (needed, name, needed_value, present) in &needs.cells {
                if *present == incarnation
                    && value == needed_value
                    && same_row(db, version, current, row, needed)?
                    && crate::source_names::column(
                        db,
                        version,
                        &row.table,
                        column,
                        current,
                        &needed.table,
                    )?
                    .as_deref()
                        == Some(name.as_str())
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

/// The evidence a snapshot of `audience` written by `author` carries: every
/// other member's write that created an owned row, or set a cell of a present
/// owned row, with that part's needed leaves opened. The snapshot device's
/// own writes are its signer's.
pub(crate) fn evidence(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    audience: &Audience,
    author: Author,
) -> Result<Vec<WriteEvidence>, DbError> {
    let mut needs = BTreeMap::<WriteId, Needs>::new();
    let mut creators = BTreeMap::new();
    let other = |write: WriteId| author.other(db, write);
    crate::snapshot_view::creations::<DbError>(db, audience, |creation| {
        let row = creation.row.clone();
        if crate::ownership::owner_table(schema, &row.table) && other(creation.write)? {
            let claim =
                (creation.creator.write != creation.write).then(|| creation.creator.clone());
            creators.insert(row.clone(), claim);
            needs
                .entry(creation.write)
                .or_default()
                .creations
                .insert(row);
        }
        Ok(())
    })?;
    crate::snapshot_view::rows(db, schema, audience, |merged, _| {
        let state = merged.state;
        let row = state.row();
        if crate::ownership::owner_table(schema, &row.table) && state.present() {
            for (column, cell) in state.cells() {
                if other(cell.write)? {
                    needs.entry(cell.write).or_default().cells.push((
                        row.clone(),
                        column.clone(),
                        cell.value.clone(),
                        state.generation(),
                    ));
                }
            }
        }
        Ok::<_, DbError>(())
    })?;
    let current = db.schema_version()?;
    let mut evidence = Vec::new();
    for (write, needs) in needs {
        let original = header(db, write)?.ok_or(missing(write))?;
        let plain = written(db, write)?.ok_or(missing(write))?;
        let proof = if plain.disposition == WriteDisposition::Migration {
            Multiproof {
                leaves: 0,
                revealed: Vec::new(),
                hidden: Vec::new(),
            }
        } else {
            open(
                db,
                write,
                audience,
                plain.schema_version,
                current,
                &needs,
                &creators,
            )?
        };
        evidence.push(WriteEvidence {
            write,
            prefix: original.prefix,
            header: original.header,
            roots: original.roots,
            signature: original.signature,
            proof,
        });
    }
    Ok(evidence)
}

/// Open the needed leaves of one part: from its records and retained leaf
/// hashes when held whole, or pruning a loaded snapshot's authenticated openings.
fn open(
    db: &DatabaseConnection,
    write: WriteId,
    audience: &Audience,
    version: u32,
    current: u32,
    needs: &Needs,
    creators: &BTreeMap<RowId, Option<coven_format::write_sources::ChangeId>>,
) -> Result<Multiproof, DbError> {
    let id = encoded(merge_fields::encode_write_id(&write))?;
    let hashes: Option<Vec<u8>> = db.query_row(
        "SELECT (SELECT hashes FROM _coven_part_leaves WHERE write_id=?1 AND audience=?2)",
        params![id, audience_text(audience)],
        |r| r.get(0),
    )?;
    if let Some(hashes) = hashes {
        let hashes: Vec<[u8; 32]> = hashes
            .chunks_exact(32)
            .map(|hash| hash.try_into().expect("32 bytes"))
            .collect();
        let mut leaves = Vec::new();
        db.for_each::<_, DbError>(
            "SELECT record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 ORDER BY table_name,key",
            params![id, audience_text(audience)],
            |r| {
                let record = OriginalRow::decode(&r.get::<_, Vec<u8>>(0)?)?;
                leaves.extend(coven_format::authorship::record_leaves(record.record()));
                Ok(())
            },
        )?;
        if leaves.len() != hashes.len() {
            return Err(DbError::DamagedDatabase);
        }
        let mut opened = Vec::with_capacity(leaves.len());
        for leaf in &leaves {
            opened.push(if proves(db, version, current, leaf, needs, creators)? {
                Some(leaf.encode()?)
            } else {
                None
            });
        }
        return Ok(Multiproof::from_hashes(&hashes, opened));
    }
    let (proof, hashes) = db
        .query(
            "SELECT proof,hashes FROM _coven_part_proofs WHERE write_id=?1 AND audience=?2",
            params![id, audience_text(audience)],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )?
        .pop()
        .ok_or_else(|| missing(write))?;
    let proof = decoded(merge_fields::decode_multiproof(&proof))?;
    let hashes: Vec<[u8; 32]> = hashes
        .chunks_exact(32)
        .map(|hash| hash.try_into().expect("32 bytes"))
        .collect();
    let keep = proof
        .opened()?
        .iter()
        .map(|leaf| proves(db, version, current, leaf, needs, creators))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(proof.retain_opened(&hashes, &keep))
}

/// Authenticate one write's evidence in a loaded snapshot of `audience`: its
/// header signature, then its opening against that part's signed root. Keep
/// both for snapshots this device writes later.
pub(crate) struct AuthenticatedEvidence {
    pub(crate) opened: WriteHeaderFrame,
    pub(crate) leaves: Vec<Leaf>,
    pub(crate) hashes: Vec<[u8; 32]>,
    pub(crate) part: bool,
}

pub(crate) fn authenticate(
    authorship: &dyn Authorship,
    audience: &Audience,
    evidence: &WriteEvidence,
) -> Result<AuthenticatedEvidence, DbError> {
    let opened = authorship.header(evidence)?;
    if opened.header.position != evidence.write {
        return Err(crate::snapshot_error::invalid(
            "write evidence names another write",
        ));
    }
    let prefix = coven_format::sealed_write::WriteObjectPrefix::decode(&evidence.prefix)?;
    let part = opened
        .parts
        .iter()
        .position(|part| part.audience == *audience);
    let (leaves, hashes) = match part {
        Some(index) => {
            let hasher = authorship.hasher(audience, prefix.part_keys[index])?;
            let (root, hashes) = evidence.proof.root_with_leaf_hashes(&hasher)?;
            if root != evidence.roots[index] {
                return Err(crate::snapshot_error::invalid(
                    "write evidence opening differs from its signed root",
                ));
            }
            (evidence.proof.opened()?, hashes)
        }
        None if evidence.proof.leaves == 0 => (Vec::new(), Vec::new()),
        None => {
            return Err(crate::snapshot_error::invalid(
                "write evidence opens a part the write does not have",
            ))
        }
    };
    Ok(AuthenticatedEvidence {
        opened,
        leaves,
        hashes,
        part: part.is_some(),
    })
}

/// Retain already authenticated evidence for snapshots this device writes later.
pub(crate) fn load(
    db: &DatabaseConnection,
    audience: &Audience,
    evidence: &WriteEvidence,
    verified: &AuthenticatedEvidence,
) -> Result<(), DbError> {
    crate::write_headers::retain(db, &verified.opened)?;
    retain_header(
        db,
        evidence.write,
        &HeaderEvidence {
            prefix: evidence.prefix.clone(),
            header: evidence.header.clone(),
            roots: evidence.roots.clone(),
            signature: evidence.signature,
        },
    )?;
    if verified.part {
        let hashes: Vec<u8> = verified.hashes.iter().flatten().copied().collect();
        db.internal_execute(
            "INSERT INTO _coven_part_proofs(write_id,audience,proof,hashes) VALUES(?1,?2,?3,?4) ON CONFLICT(write_id,audience) DO UPDATE SET proof=excluded.proof,hashes=excluded.hashes",
            params![
                encoded(merge_fields::encode_write_id(&evidence.write))?,
                audience_text(audience),
                encoded(merge_fields::encode_multiproof(&evidence.proof))?,
                hashes
            ],
        )?;
    }
    Ok(())
}

/// Refuse a loaded snapshot of `audience` by `author` that names another
/// member as creating an owned row, or setting one of its present cells,
/// without that write's authenticated opening (§9.6, §16.2).
pub(crate) fn check(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    audience: &Audience,
    author: Author,
    authenticated: &BTreeMap<WriteId, AuthenticatedEvidence>,
) -> Result<(), DbError> {
    let current = db.schema_version()?;
    let refused = || {
        crate::snapshot_error::invalid(
            "snapshot names another member's authorship without its write's evidence",
        )
    };
    let other = |write: WriteId| author.other(db, write);
    let mut needs = BTreeMap::<WriteId, Needs>::new();
    let mut creators = BTreeMap::new();
    crate::ownership::visit::<DbError>(db, audience, |row, creation| {
        if crate::ownership::owner_table(schema, &row.table) && other(creation.write)? {
            let claim =
                (creation.creator.write != creation.write).then(|| creation.creator.clone());
            creators.insert(row.clone(), claim);
            needs
                .entry(creation.write)
                .or_default()
                .creations
                .insert(row);
        }
        Ok(())
    })?;
    // The loaded rows are still the snapshot's staged values.
    let loaded = BTreeSet::from([audience.clone()]);
    let store = crate::merge_store::MergeStore::from_snapshot(db, &schema.schema, &loaded);
    for declaration in schema.declarations.iter().filter(|d| d.owner_only) {
        let rows = db.query(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1 AND table_name=?2",
            params![audience_text(audience), declaration.name],
            crate::row_queries::read_identity,
        )?;
        for row in rows {
            let state = store.row_without_losses(&row)?.state;
            if !state.present() {
                continue;
            }
            for (column, cell) in state.cells() {
                if other(cell.write)? {
                    needs.entry(cell.write).or_default().cells.push((
                        row.clone(),
                        column.clone(),
                        cell.value.clone(),
                        state.generation(),
                    ));
                }
            }
        }
    }
    for (write, needs) in needs {
        let Some(verified) = authenticated.get(&write) else {
            return Err(refused());
        };
        if verified.opened.header.disposition == WriteDisposition::Migration {
            continue;
        }
        let version = verified.opened.header.schema_version;
        let leaves = &verified.leaves;
        for row in &needs.creations {
            let single = Needs {
                creations: BTreeSet::from([row.clone()]),
                cells: Vec::new(),
            };
            let mut found = false;
            for leaf in leaves {
                found |= proves(db, version, current, leaf, &single, &creators)?;
            }
            if !found {
                return Err(refused());
            }
        }
        for cell in &needs.cells {
            let single = Needs {
                creations: BTreeSet::new(),
                cells: vec![cell.clone()],
            };
            let mut found = false;
            for leaf in leaves {
                found |= proves(db, version, current, leaf, &single, &creators)?;
            }
            if !found {
                return Err(refused());
            }
        }
    }
    Ok(())
}

/// Hash and check a received part's leaves against its signed root, keeping
/// the hashes for later openings.
pub(crate) struct PartRoot {
    hasher: AuthorshipHasher,
    root: [u8; 32],
    hashes: Vec<[u8; 32]>,
}

impl PartRoot {
    pub(crate) fn new(hasher: AuthorshipHasher, root: [u8; 32]) -> Self {
        Self {
            hasher,
            root,
            hashes: Vec::new(),
        }
    }

    /// Add one record's leaves, in stream order.
    pub(crate) fn record(
        &mut self,
        record: coven_format::write::PartRecord<'_>,
    ) -> Result<(), DbError> {
        for leaf in coven_format::authorship::record_leaves(record) {
            self.hashes.push(self.hasher.leaf(&leaf.encode()?));
        }
        Ok(())
    }

    /// Refuse a part whose leaves do not reproduce its signed root.
    pub(crate) fn finish(self) -> Result<Vec<[u8; 32]>, DbError> {
        if coven_format::authorship::root_of_hashes(&self.hashes) != self.root {
            return Err(crate::snapshot_error::invalid(
                "write part differs from its signed authorship root",
            ));
        }
        Ok(self.hashes)
    }
}

#[cfg(test)]
#[path = "authorship_tests.rs"]
mod tests;
