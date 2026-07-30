//! Canonical signed contract for the schema shape that decides sync routing.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::store_commit::ObjectHash;
use crate::sync::session::{foreign_key_edges, GateRole, RowIdentity, SyncedTable};

const SYNC_ROUTING_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SyncRoutingContractError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    ForeignKey(#[from] crate::sync::session::ForeignKeySchemaError),
    #[error("synced table {child_table:?} has a foreign key to undeclared table {parent_table:?}")]
    UndeclaredForeignKeyTarget {
        child_table: String,
        parent_table: String,
    },
    #[error(
        "foreign key from {child_table:?} to {parent_table:?} targets non-primary columns {columns:?} without a matching non-partial UNIQUE key and collation"
    )]
    MissingUniqueParentKey {
        child_table: String,
        parent_table: String,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncRoutingContract {
    bytes: Vec<u8>,
    hash: ObjectHash,
    has_scoped_graph: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalContract {
    version: u32,
    tables: Vec<CanonicalTable>,
    foreign_keys: Vec<CanonicalForeignKey>,
    parent_unique_keys: Vec<CanonicalUniqueKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalTable {
    name: String,
    row_identity: CanonicalRowIdentity,
    role: CanonicalRole,
    audience_parent_column: Option<String>,
    asset: bool,
    blob: Option<CanonicalBlob>,
    required_columns: Vec<CanonicalColumn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalRowIdentity {
    IndependentUuid,
    SharedKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum CanonicalRole {
    Plain,
    RemoteRoot,
    GatedRoot { column: String },
    ScopedRoot { column: String },
    GatedByDescendants,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalBlob {
    id_column: String,
    size_column: String,
    hash_column: String,
    namespace: String,
    cloud_path_column: Option<String>,
    scope: CanonicalBlobScope,
    provenance: CanonicalProvenance,
    fill: CanonicalCacheFill,
    replacement: CanonicalBlobReplacement,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "name")]
enum CanonicalBlobScope {
    Master,
    Derived(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalProvenance {
    UserProvided,
    HostProvided,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalCacheFill {
    CacheEager,
    CacheLazy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalBlobReplacement {
    Replaceable,
    WriteOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalColumn {
    ordinal: i64,
    name: String,
    declared_type: String,
    not_null: bool,
    primary_key_rank: i64,
    collation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalForeignKey {
    child_table: String,
    parent_table: String,
    columns: Vec<CanonicalForeignKeyColumn>,
    on_update: String,
    on_delete: String,
    match_clause: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalForeignKeyColumn {
    child: CanonicalColumn,
    parent: CanonicalColumn,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalUniqueKey {
    parent_table: String,
    columns: Vec<CanonicalUniqueKeyColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalUniqueKeyColumn {
    column: CanonicalColumn,
    index_collation: String,
    descending: bool,
}

impl SyncRoutingContract {
    pub(crate) fn from_connection(
        conn: &rusqlite::Connection,
        declarations: &[SyncedTable],
    ) -> Result<Self, SyncRoutingContractError> {
        let mut declarations = declarations.to_vec();
        declarations.sort_by(|left, right| left.name().cmp(right.name()));
        let synced_names = declarations
            .iter()
            .map(|table| table.name().to_string())
            .collect::<BTreeSet<_>>();
        let mut tables = Vec::with_capacity(declarations.len());
        let mut foreign_keys = Vec::new();
        let mut parent_unique_keys = BTreeSet::new();
        for declaration in declarations {
            tables.push(canonical_table(conn, &declaration)?);
            let (table_foreign_keys, table_unique_keys) =
                canonical_foreign_keys(conn, declaration.name(), &synced_names)?;
            foreign_keys.extend(table_foreign_keys);
            parent_unique_keys.extend(table_unique_keys);
        }
        foreign_keys.sort();
        Ok(Self::from_canonical(CanonicalContract {
            version: SYNC_ROUTING_CONTRACT_VERSION,
            tables,
            foreign_keys,
            parent_unique_keys: parent_unique_keys.into_iter().collect(),
        }))
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let canonical: CanonicalContract = serde_json::from_slice(bytes)
            .map_err(|error| format!("parse sync-routing contract: {error}"))?;
        if canonical.version != SYNC_ROUTING_CONTRACT_VERSION {
            return Err(format!(
                "unsupported sync-routing contract version {}",
                canonical.version
            ));
        }
        let parsed = Self::from_canonical(canonical);
        if parsed.bytes != bytes {
            return Err("sync-routing contract bytes are not canonical".to_string());
        }
        Ok(parsed)
    }

    fn from_canonical(canonical: CanonicalContract) -> Self {
        let has_scoped_graph = canonical
            .tables
            .iter()
            .any(|table| matches!(table.role, CanonicalRole::ScopedRoot { .. }));
        let bytes = serde_json::to_vec(&canonical)
            .expect("SyncRoutingContract canonical serialization cannot fail");
        Self {
            hash: ObjectHash::digest(&bytes),
            bytes,
            has_scoped_graph,
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn hash(&self) -> ObjectHash {
        self.hash
    }

    pub(crate) fn has_scoped_graph(&self) -> bool {
        self.has_scoped_graph
    }
}

fn canonical_table(
    conn: &rusqlite::Connection,
    declaration: &SyncedTable,
) -> Result<CanonicalTable, SyncRoutingContractError> {
    let columns = live_columns(conn, declaration.name())?;
    let role = match declaration.gate_role() {
        GateRole::Plain => CanonicalRole::Plain,
        GateRole::RemoteRoot => CanonicalRole::RemoteRoot,
        GateRole::GatedRoot { gate_column } => CanonicalRole::GatedRoot {
            column: gate_column.clone(),
        },
        GateRole::ScopedRoot { audience_column } => CanonicalRole::ScopedRoot {
            column: audience_column.clone(),
        },
        GateRole::GatedByDescendants => CanonicalRole::GatedByDescendants,
    };
    let blob = declaration.blob().map(|blob| CanonicalBlob {
        id_column: blob.id_column.clone(),
        size_column: blob.size_column.clone(),
        hash_column: blob.hash_column.clone(),
        namespace: blob.namespace.clone(),
        cloud_path_column: blob.cloud_path_column.clone(),
        scope: match &blob.scope {
            crate::blob::BlobScope::Master => CanonicalBlobScope::Master,
            crate::blob::BlobScope::Derived(name) => CanonicalBlobScope::Derived(name.clone()),
        },
        provenance: match blob.provenance {
            crate::blob::Provenance::UserProvided => CanonicalProvenance::UserProvided,
            crate::blob::Provenance::HostProvided => CanonicalProvenance::HostProvided,
        },
        fill: match blob.fill {
            crate::blob::CacheFill::CacheEager => CanonicalCacheFill::CacheEager,
            crate::blob::CacheFill::CacheLazy => CanonicalCacheFill::CacheLazy,
        },
        replacement: match blob.replacement {
            crate::blob::BlobReplacement::Replaceable => CanonicalBlobReplacement::Replaceable,
            crate::blob::BlobReplacement::WriteOnce => CanonicalBlobReplacement::WriteOnce,
        },
    });
    let mut required_names = BTreeSet::from(["id".to_string(), "_updated_at".to_string()]);
    if let Some(column) = declaration.gate_column() {
        required_names.insert(column.to_string());
    }
    if let Some(column) = declaration.audience_column() {
        required_names.insert(column.to_string());
    }
    if let Some(column) = declaration.audience_parent_column() {
        required_names.insert(column.to_string());
    }
    if let Some(blob) = declaration.blob() {
        required_names.insert(blob.id_column.clone());
        required_names.insert(blob.size_column.clone());
        required_names.insert(blob.hash_column.clone());
        if let Some(column) = &blob.cloud_path_column {
            required_names.insert(column.clone());
        }
    }
    let required_columns = required_names
        .into_iter()
        .map(|name| {
            columns
                .get(&name)
                .cloned()
                .ok_or_else(|| rusqlite::Error::InvalidColumnName(name).into())
        })
        .collect::<Result<Vec<_>, SyncRoutingContractError>>()?;
    Ok(CanonicalTable {
        name: declaration.name().to_string(),
        row_identity: match declaration.row_identity() {
            RowIdentity::IndependentUuid => CanonicalRowIdentity::IndependentUuid,
            RowIdentity::SharedKey => CanonicalRowIdentity::SharedKey,
        },
        role,
        audience_parent_column: declaration.audience_parent_column().map(str::to_string),
        asset: declaration.is_asset(),
        blob,
        required_columns,
    })
}

fn canonical_foreign_keys(
    conn: &rusqlite::Connection,
    child_table: &str,
    synced_names: &BTreeSet<String>,
) -> Result<(Vec<CanonicalForeignKey>, Vec<CanonicalUniqueKey>), SyncRoutingContractError> {
    let child_columns = live_columns(conn, child_table)?;
    let mut foreign_keys = Vec::new();
    let mut parent_unique_keys = BTreeSet::new();
    for edge in foreign_key_edges(conn, child_table)? {
        if !synced_names.contains(&edge.parent_table) {
            return Err(SyncRoutingContractError::UndeclaredForeignKeyTarget {
                child_table: child_table.to_string(),
                parent_table: edge.parent_table,
            });
        }
        let parent_columns = live_columns(conn, &edge.parent_table)?;
        let columns = edge
            .columns
            .iter()
            .map(|column| {
                let child = child_columns
                    .get(&column.child)
                    .cloned()
                    .ok_or_else(|| rusqlite::Error::InvalidColumnName(column.child.clone()))?;
                let parent = parent_columns
                    .get(&column.parent)
                    .cloned()
                    .ok_or_else(|| rusqlite::Error::InvalidColumnName(column.parent.clone()))?;
                Ok(CanonicalForeignKeyColumn { child, parent })
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let target_columns = edge
            .columns
            .iter()
            .map(|column| column.parent.clone())
            .collect::<Vec<_>>();
        let mut primary_key = parent_columns
            .values()
            .filter(|column| column.primary_key_rank > 0)
            .collect::<Vec<_>>();
        primary_key.sort_by_key(|column| column.primary_key_rank);
        let primary_key = primary_key
            .into_iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        if target_columns != primary_key {
            let unique_keys = canonical_unique_parent_keys(
                conn,
                &edge.parent_table,
                &target_columns,
                &parent_columns,
            )?;
            if unique_keys.is_empty() {
                return Err(SyncRoutingContractError::MissingUniqueParentKey {
                    child_table: child_table.to_string(),
                    parent_table: edge.parent_table,
                    columns: target_columns,
                });
            }
            parent_unique_keys.extend(unique_keys);
        }
        foreign_keys.push(CanonicalForeignKey {
            child_table: child_table.to_string(),
            parent_table: edge.parent_table,
            columns,
            on_update: edge.on_update,
            on_delete: edge.on_delete,
            match_clause: edge.match_clause,
        });
    }
    foreign_keys.sort();
    Ok((foreign_keys, parent_unique_keys.into_iter().collect()))
}

fn live_columns(
    conn: &rusqlite::Connection,
    table: &str,
) -> Result<BTreeMap<String, CanonicalColumn>, SyncRoutingContractError> {
    let sql = format!(
        "PRAGMA table_info({})",
        crate::sync::session::quote_ident(table)
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)? != 0,
            row.get::<_, i64>(5)?,
        ))
    })?;
    let mut columns = BTreeMap::new();
    for row in rows {
        let (ordinal, name, declared_type, not_null, primary_key_rank) = row?;
        let (_, collation, _, _, _) = conn.column_metadata(None::<&str>, table, name.as_str())?;
        let collation = collation
            .ok_or_else(|| rusqlite::Error::InvalidColumnName(name.clone()))?
            .to_str()
            .map_err(|error| rusqlite::Error::Utf8Error(0, error))?
            .to_ascii_uppercase();
        columns.insert(
            name.clone(),
            CanonicalColumn {
                ordinal,
                name,
                declared_type: declared_type.to_ascii_uppercase(),
                not_null,
                primary_key_rank,
                collation,
            },
        );
    }
    Ok(columns)
}

fn canonical_unique_parent_keys(
    conn: &rusqlite::Connection,
    parent_table: &str,
    target_columns: &[String],
    parent_columns: &BTreeMap<String, CanonicalColumn>,
) -> Result<Vec<CanonicalUniqueKey>, SyncRoutingContractError> {
    let sql = format!(
        "PRAGMA index_list({})",
        crate::sync::session::quote_ident(parent_table)
    );
    let mut statement = conn.prepare(&sql)?;
    let indexes = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
                row.get::<_, i64>(4)? != 0,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut keys = BTreeSet::new();
    for (index_name, unique, partial) in indexes {
        if !unique || partial {
            continue;
        }
        let sql = format!(
            "PRAGMA index_xinfo({})",
            crate::sync::session::quote_ident(&index_name)
        );
        let mut statement = conn.prepare(&sql)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)? != 0,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, i64>(5)? != 0,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut index_columns = rows
            .into_iter()
            .filter(|(_, _, _, _, key)| *key)
            .collect::<Vec<_>>();
        index_columns.sort_by_key(|(sequence, _, _, _, _)| *sequence);
        let Some(names) = index_columns
            .iter()
            .map(|(_, name, _, _, _)| name.clone())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        if names != target_columns {
            continue;
        }
        let mut columns = Vec::with_capacity(index_columns.len());
        let mut usable = true;
        for ((_, _, descending, index_collation, _), name) in index_columns.into_iter().zip(names) {
            let column = parent_columns
                .get(&name)
                .cloned()
                .ok_or_else(|| rusqlite::Error::InvalidColumnName(name.clone()))?;
            let index_collation = index_collation
                .ok_or_else(|| rusqlite::Error::InvalidColumnName(name.clone()))?
                .to_ascii_uppercase();
            if index_collation != column.collation {
                usable = false;
            }
            columns.push(CanonicalUniqueKeyColumn {
                column,
                index_collation,
                descending,
            });
        }
        if usable {
            keys.insert(CanonicalUniqueKey {
                parent_table: parent_table.to_string(),
                columns,
            });
        }
    }
    Ok(keys.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE parents (
                id TEXT PRIMARY KEY,
                ordinary TEXT DEFAULT 'first',
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE INDEX parents_ordinary ON parents(ordinary);
             CREATE TABLE children (
                id TEXT PRIMARY KEY,
                parent_id TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE,
                ordinary INTEGER,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )
        .expect("schema");
        conn
    }

    fn declarations() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("children", RowIdentity::IndependentUuid)
                .inherits_audience_through("parent_id"),
            SyncedTable::new("parents", RowIdentity::IndependentUuid).scoped_by("audience"),
        ]
    }

    #[test]
    fn canonical_hash_binds_routing_declarations_and_synced_foreign_keys() {
        let conn = schema();
        let declarations = declarations();
        let contract =
            SyncRoutingContract::from_connection(&conn, &declarations).expect("routing contract");
        let reversed = SyncRoutingContract::from_connection(
            &conn,
            &declarations.iter().cloned().rev().collect::<Vec<_>>(),
        )
        .expect("reordered contract");
        assert_eq!(contract.bytes(), reversed.bytes());
        assert_eq!(contract.hash(), reversed.hash());
        assert!(contract.has_scoped_graph());
        assert_eq!(
            SyncRoutingContract::from_bytes(contract.bytes()).expect("parse exact contract"),
            contract,
        );

        let changed = vec![
            SyncedTable::new("children", RowIdentity::SharedKey)
                .inherits_audience_through("parent_id"),
            declarations[1].clone(),
        ];
        assert_ne!(
            contract.hash(),
            SyncRoutingContract::from_connection(&conn, &changed)
                .expect("changed contract")
                .hash()
        );
    }

    #[test]
    fn ordinary_columns_indexes_defaults_and_local_tables_do_not_change_the_hash() {
        let conn = schema();
        let declarations = declarations();
        let before = SyncRoutingContract::from_connection(&conn, &declarations)
            .expect("routing contract before ordinary migration");
        conn.execute_batch(
            "ALTER TABLE parents ADD COLUMN later TEXT DEFAULT 'second';
             CREATE INDEX children_ordinary ON children(ordinary);
             CREATE TABLE local_notes (id TEXT PRIMARY KEY) STRICT;",
        )
        .expect("ordinary migration");
        let after = SyncRoutingContract::from_connection(&conn, &declarations)
            .expect("routing contract after ordinary migration");
        assert_eq!(before.bytes(), after.bytes());
        assert_eq!(before.hash(), after.hash());
    }

    #[test]
    fn noncanonical_or_unknown_contract_bytes_are_rejected() {
        let contract = SyncRoutingContract::from_connection(&schema(), &declarations())
            .expect("routing contract");
        let mut value: serde_json::Value =
            serde_json::from_slice(contract.bytes()).expect("parse contract json");
        value
            .as_object_mut()
            .expect("contract object")
            .insert("unknown".to_string(), serde_json::Value::Bool(true));
        assert!(SyncRoutingContract::from_bytes(&serde_json::to_vec(&value).unwrap()).is_err());

        let pretty = serde_json::to_vec_pretty(
            &serde_json::from_slice::<serde_json::Value>(contract.bytes()).unwrap(),
        )
        .unwrap();
        assert!(SyncRoutingContract::from_bytes(&pretty).is_err());
    }

    #[test]
    fn required_column_ordinal_changes_the_contract() {
        fn contract(parent_columns: &str) -> SyncRoutingContract {
            let conn = rusqlite::Connection::open_in_memory().expect("open");
            conn.execute_batch(&format!(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents ({parent_columns}) STRICT;
                 CREATE TABLE children (
                    id TEXT PRIMARY KEY,
                    parent_id TEXT NOT NULL REFERENCES parents(id),
                    _updated_at TEXT NOT NULL
                 ) STRICT;"
            ))
            .expect("schema");
            SyncRoutingContract::from_connection(&conn, &declarations()).expect("contract")
        }

        let before = contract(
            "id TEXT PRIMARY KEY,
             ordinary TEXT,
             audience TEXT,
             _updated_at TEXT NOT NULL",
        );
        let after = contract(
            "ordinary TEXT,
             id TEXT PRIMARY KEY,
             audience TEXT,
             _updated_at TEXT NOT NULL",
        );
        assert_ne!(before.hash(), after.hash());
    }

    #[test]
    fn routing_column_collation_changes_the_contract() {
        fn contract(audience: &str) -> SyncRoutingContract {
            let conn = rusqlite::Connection::open_in_memory().expect("open");
            conn.execute_batch(&format!(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents (
                    id TEXT PRIMARY KEY,
                    {audience},
                    _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE children (
                    id TEXT PRIMARY KEY,
                    parent_id TEXT NOT NULL REFERENCES parents(id),
                    _updated_at TEXT NOT NULL
                 ) STRICT;"
            ))
            .expect("schema");
            SyncRoutingContract::from_connection(&conn, &declarations()).expect("contract")
        }

        let binary = contract("audience TEXT COLLATE BINARY");
        let no_case = contract("audience TEXT COLLATE NOCASE");
        assert_ne!(binary.hash(), no_case.hash());
    }

    #[test]
    fn synced_foreign_key_to_local_table_is_rejected() {
        let conn = rusqlite::Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "CREATE TABLE local_parents (id TEXT PRIMARY KEY) STRICT;
             CREATE TABLE children (
                id TEXT PRIMARY KEY,
                local_parent_id TEXT NOT NULL REFERENCES local_parents(id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )
        .expect("schema");

        let error = SyncRoutingContract::from_connection(
            &conn,
            &[SyncedTable::new("children", RowIdentity::IndependentUuid)],
        )
        .expect_err("synced-to-local foreign key must be rejected");
        assert!(matches!(
            error,
            SyncRoutingContractError::UndeclaredForeignKeyTarget {
                child_table,
                parent_table,
            } if child_table == "children" && parent_table == "local_parents"
        ));
    }

    #[test]
    fn non_primary_foreign_key_requires_matching_unique_parent_key() {
        fn connection(index: &str) -> rusqlite::Connection {
            let conn = rusqlite::Connection::open_in_memory().expect("open");
            conn.execute_batch(&format!(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents (
                    id TEXT PRIMARY KEY,
                    code TEXT COLLATE NOCASE NOT NULL,
                    _updated_at TEXT NOT NULL
                 ) STRICT;
                 {index}
                 CREATE TABLE children (
                    id TEXT PRIMARY KEY,
                    parent_code TEXT NOT NULL,
                    _updated_at TEXT NOT NULL,
                    FOREIGN KEY (parent_code) REFERENCES parents(code)
                 ) STRICT;"
            ))
            .expect("schema");
            conn
        }
        let declarations = vec![
            SyncedTable::new("parents", RowIdentity::IndependentUuid),
            SyncedTable::new("children", RowIdentity::IndependentUuid),
        ];

        SyncRoutingContract::from_connection(
            &connection("CREATE UNIQUE INDEX parents_code ON parents(code COLLATE NOCASE);"),
            &declarations,
        )
        .expect("matching structural unique key");
        for index in [
            "",
            "CREATE UNIQUE INDEX parents_code ON parents(code COLLATE BINARY);",
        ] {
            let error = SyncRoutingContract::from_connection(&connection(index), &declarations)
                .expect_err("missing or changed unique key must be rejected");
            assert!(matches!(
                error,
                SyncRoutingContractError::MissingUniqueParentKey { .. }
            ));
        }
    }

    #[test]
    fn foreign_key_declaration_order_does_not_change_the_contract() {
        fn contract(foreign_keys: &str) -> SyncRoutingContract {
            let conn = rusqlite::Connection::open_in_memory().expect("open");
            conn.execute_batch(&format!(
                "CREATE TABLE left_parents (
                    id TEXT PRIMARY KEY,
                    _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE right_parents (
                    id TEXT PRIMARY KEY,
                    _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE children (
                    id TEXT PRIMARY KEY,
                    left_id TEXT NOT NULL,
                    right_id TEXT NOT NULL,
                    _updated_at TEXT NOT NULL,
                    {foreign_keys}
                 ) STRICT;"
            ))
            .expect("schema");
            SyncRoutingContract::from_connection(
                &conn,
                &[
                    SyncedTable::new("left_parents", RowIdentity::IndependentUuid),
                    SyncedTable::new("right_parents", RowIdentity::IndependentUuid),
                    SyncedTable::new("children", RowIdentity::IndependentUuid),
                ],
            )
            .expect("contract")
        }

        let left_then_right = contract(
            "FOREIGN KEY (left_id) REFERENCES left_parents(id) ON DELETE CASCADE,
             FOREIGN KEY (right_id) REFERENCES right_parents(id) ON UPDATE CASCADE",
        );
        let right_then_left = contract(
            "FOREIGN KEY (right_id) REFERENCES right_parents(id) ON UPDATE CASCADE,
             FOREIGN KEY (left_id) REFERENCES left_parents(id) ON DELETE CASCADE",
        );
        assert_eq!(left_then_right.bytes(), right_then_left.bytes());
    }
}
