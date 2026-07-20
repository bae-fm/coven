//! Private accepted-history baselines and deterministic retained replay.

use std::collections::BTreeSet;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use super::store_commit::{CommitFrontier, ObjectHash, StoreDeviceRegistrationRef, StoreRootRef};
use crate::database::{
    DbError, COVEN_INITIALIZED_STATE_KEY, COVEN_SCHEMA_MANIFEST_STATE_KEY,
    SERIAL_KEY_GENERATION_STATE_KEY, SERIAL_MEMBERSHIP_STATE_KEY, SERIAL_PROVIDER_ADMIN_STATE_KEY,
    SERIAL_WRAPPED_KEYS_STATE_KEY, STORE_DEVICE_GENESIS_STATE_KEY, SYNC_ROUTING_CONTRACT_STATE_KEY,
    SYNC_ROUTING_HASH_STATE_KEY, WRITE_POLICY_STATE_KEY,
};
use crate::sync::membership_ops::{
    MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX, OWNER_PUBKEY_STATE_KEY,
};
use crate::WritePolicy;

pub(crate) const GENERATION_ZERO: u64 = 0;

const GENESIS_PRESERVED_TABLES: &[&str] = &[
    "protocol_state",
    "store_protocol_root_authority",
    "store_device_registration_activations",
];
const SQLITE_DATABASE_HEADER: &[u8; 16] = b"SQLite format 3\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedReplayGenesisAuthority {
    pub(crate) store_root: StoreRootRef,
    pub(crate) founder_registration: StoreDeviceRegistrationRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedReplayBaseline {
    pub(crate) generation: u64,
    pub(crate) write_policy: WritePolicy,
    pub(crate) exact_cut: CommitFrontier,
    pub(crate) schema_version: u32,
    pub(crate) routing_hash: ObjectHash,
    pub(crate) image_hash: ObjectHash,
    pub(crate) image_bytes: Vec<u8>,
    pub(crate) authority: RetainedReplayGenesisAuthority,
}

impl RetainedReplayBaseline {
    pub(crate) fn generation_zero(
        source: &Connection,
        write_policy: WritePolicy,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: RetainedReplayGenesisAuthority,
    ) -> Result<Self, DbError> {
        let image_bytes = project_generation_zero_image(source, write_policy)?;
        let baseline = Self {
            generation: GENERATION_ZERO,
            write_policy,
            exact_cut: match write_policy {
                WritePolicy::MergeConcurrent => CommitFrontier::MergeConcurrent(Default::default()),
                WritePolicy::Serial => CommitFrontier::Serial(None),
            },
            schema_version,
            routing_hash,
            image_hash: ObjectHash::digest(&image_bytes),
            image_bytes,
            authority,
        };
        baseline.validate_image()?;
        Ok(baseline)
    }

    pub(crate) fn canonical_authority_bytes(&self) -> Result<Vec<u8>, DbError> {
        serde_json::to_vec(&self.authority).map_err(|error| {
            DbError::Message(format!(
                "serialize retained replay genesis authority: {error}"
            ))
        })
    }

    pub(crate) fn validate_image(&self) -> Result<(), DbError> {
        if self.generation != GENERATION_ZERO
            || self.exact_cut.policy() != self.write_policy
            || !matches!(
                (&self.write_policy, &self.exact_cut),
                (
                    WritePolicy::MergeConcurrent,
                    CommitFrontier::MergeConcurrent(frontier)
                ) if frontier.is_empty()
            ) && !matches!(
                (&self.write_policy, &self.exact_cut),
                (WritePolicy::Serial, CommitFrontier::Serial(None))
            )
            || self.image_hash != ObjectHash::digest(&self.image_bytes)
        {
            return Err(DbError::Message(
                "generation-zero retained replay baseline metadata is inconsistent".to_string(),
            ));
        }
        let image = open_image(&self.image_bytes)?;
        validate_generation_zero_image(
            &image,
            self.write_policy,
            self.schema_version,
            self.routing_hash,
        )
    }
}

pub(crate) fn open_image(image: &[u8]) -> Result<Connection, DbError> {
    if image.len() < 20 || &image[..SQLITE_DATABASE_HEADER.len()] != SQLITE_DATABASE_HEADER {
        return Err(DbError::Message(
            "retained replay image is not a SQLite database".to_string(),
        ));
    }
    let mut image = image.to_vec();
    // sqlite3_deserialize cannot use an image whose header requests WAL. File
    // databases carry that mode in bytes 18 and 19; the private in-memory copy
    // has no WAL file, so it must use rollback journaling.
    image[18] = 1;
    image[19] = 1;
    let mut connection = Connection::open_in_memory().map_err(DbError::from)?;
    connection
        .deserialize_read_exact(rusqlite::MAIN_DB, image.as_slice(), image.len(), false)
        .map_err(DbError::from)?;
    Ok(connection)
}

fn serialized_database(connection: &Connection) -> Result<Vec<u8>, DbError> {
    connection
        .serialize(rusqlite::MAIN_DB)
        .map(|bytes| bytes.to_vec())
        .map_err(DbError::from)
}

fn project_generation_zero_image(
    source: &Connection,
    write_policy: WritePolicy,
) -> Result<Vec<u8>, DbError> {
    let source_bytes = serialized_database(source)?;
    let image = open_image(&source_bytes)?;
    image
        .pragma_update(None, "foreign_keys", "OFF")
        .map_err(DbError::from)?;
    let transaction = image.unchecked_transaction().map_err(DbError::from)?;
    for table in crate::db::user_table_names(&transaction).map_err(DbError::from)? {
        if GENESIS_PRESERVED_TABLES.contains(&table.as_str()) {
            continue;
        }
        transaction
            .execute_batch(&format!(
                "DELETE FROM {}",
                super::session::quote_ident(&table)
            ))
            .map_err(DbError::from)?;
    }
    let protocol_keys = protocol_state_keys(&transaction)?;
    for key in protocol_keys {
        if !generation_zero_protocol_key(write_policy, &key) {
            transaction
                .execute("DELETE FROM protocol_state WHERE key = ?1", [&key])
                .map_err(DbError::from)?;
        }
    }
    transaction.commit().map_err(DbError::from)?;
    image.execute_batch("VACUUM").map_err(DbError::from)?;
    image
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(DbError::from)?;
    let violations: bool = image
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if violations {
        return Err(DbError::Message(
            "generation-zero retained replay image violates foreign keys".to_string(),
        ));
    }
    serialized_database(&image)
}

fn validate_generation_zero_image(
    image: &Connection,
    write_policy: WritePolicy,
    schema_version: u32,
    routing_hash: ObjectHash,
) -> Result<(), DbError> {
    let stored_schema_version: u32 = image
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(DbError::from)?;
    if stored_schema_version != schema_version {
        return Err(DbError::Message(format!(
            "retained replay image schema version is {stored_schema_version}, expected {schema_version}"
        )));
    }
    let stored_policy: String = image
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [WRITE_POLICY_STATE_KEY],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let stored_policy: WritePolicy = serde_json::from_str(&stored_policy).map_err(|error| {
        DbError::Message(format!(
            "retained replay image write policy is invalid: {error}"
        ))
    })?;
    let stored_routing_hash: String = image
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [SYNC_ROUTING_HASH_STATE_KEY],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if stored_policy != write_policy || stored_routing_hash != routing_hash.to_string() {
        return Err(DbError::Message(
            "retained replay image policy or routing hash differs from its baseline".to_string(),
        ));
    }
    let protocol_keys = protocol_state_keys(image)?;
    let membership_head_cursor_count = protocol_keys
        .iter()
        .filter(|key| key.starts_with(MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX))
        .count();
    if protocol_keys
        .iter()
        .any(|key| !generation_zero_protocol_key(write_policy, key))
        || !required_generation_zero_protocol_keys(write_policy)
            .iter()
            .all(|key| protocol_keys.contains(*key))
        || match write_policy {
            WritePolicy::MergeConcurrent => membership_head_cursor_count != 1,
            WritePolicy::Serial => membership_head_cursor_count != 0,
        }
    {
        return Err(DbError::Message(
            "retained replay image protocol state is not the generation-zero set".to_string(),
        ));
    }
    for table in crate::db::user_table_names(image).map_err(DbError::from)? {
        let count: i64 = image
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {}",
                    super::session::quote_ident(&table)
                ),
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let expected = match table.as_str() {
            "protocol_state" => None,
            "store_protocol_root_authority" | "store_device_registration_activations" => Some(1),
            _ => Some(0),
        };
        if expected.is_some_and(|expected| count != expected) {
            return Err(DbError::Message(format!(
                "generation-zero retained replay image table {table:?} has {count} rows"
            )));
        }
    }
    let violations: bool = image
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if violations {
        return Err(DbError::Message(
            "generation-zero retained replay image violates foreign keys".to_string(),
        ));
    }
    Ok(())
}

fn protocol_state_keys(connection: &Connection) -> Result<BTreeSet<String>, DbError> {
    let mut statement = connection
        .prepare("SELECT key FROM protocol_state ORDER BY key")
        .map_err(DbError::from)?;
    let keys = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(DbError::from)?
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .map_err(DbError::from)?;
    Ok(keys)
}

fn required_generation_zero_protocol_keys(write_policy: WritePolicy) -> &'static [&'static str] {
    const MERGE: &[&str] = &[
        COVEN_INITIALIZED_STATE_KEY,
        COVEN_SCHEMA_MANIFEST_STATE_KEY,
        OWNER_PUBKEY_STATE_KEY,
        STORE_DEVICE_GENESIS_STATE_KEY,
        SYNC_ROUTING_CONTRACT_STATE_KEY,
        SYNC_ROUTING_HASH_STATE_KEY,
        WRITE_POLICY_STATE_KEY,
    ];
    const SERIAL: &[&str] = &[
        COVEN_INITIALIZED_STATE_KEY,
        COVEN_SCHEMA_MANIFEST_STATE_KEY,
        SERIAL_KEY_GENERATION_STATE_KEY,
        SERIAL_MEMBERSHIP_STATE_KEY,
        SERIAL_PROVIDER_ADMIN_STATE_KEY,
        SERIAL_WRAPPED_KEYS_STATE_KEY,
        STORE_DEVICE_GENESIS_STATE_KEY,
        SYNC_ROUTING_CONTRACT_STATE_KEY,
        SYNC_ROUTING_HASH_STATE_KEY,
        WRITE_POLICY_STATE_KEY,
    ];
    match write_policy {
        WritePolicy::MergeConcurrent => MERGE,
        WritePolicy::Serial => SERIAL,
    }
}

fn generation_zero_protocol_key(write_policy: WritePolicy, key: &str) -> bool {
    required_generation_zero_protocol_keys(write_policy).contains(&key)
        || write_policy == WritePolicy::MergeConcurrent
            && key.starts_with(MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;

    fn populate_fixture(connection: &Connection, policy: WritePolicy) {
        connection
            .execute_batch(
                "CREATE TABLE host_rows (
                     id TEXT PRIMARY KEY,
                     secret TEXT NOT NULL
                 ) STRICT;",
            )
            .expect("create host table");
        crate::db::apply_coven_schema(connection).expect("create Coven tables");
        crate::db::apply_coven_routing_schema(connection).expect("create routing tables");
        connection
            .execute_batch(
                "INSERT INTO host_rows VALUES ('host', 'projection-secret-marker');
                 INSERT INTO remote_objects
                 VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '{}');
                 INSERT INTO store_protocol_root_authority
                 VALUES (1, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                         x'01', '{}');
                 INSERT INTO store_device_registration_activations
                 VALUES ('founder',
                         'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                         'author', 'device', x'01', '{}', '{}');",
            )
            .expect("insert projection rows");
        let mut keys = required_generation_zero_protocol_keys(policy)
            .iter()
            .map(|key| ((*key).to_string(), "{}".to_string()))
            .collect::<Vec<_>>();
        keys.iter_mut()
            .find(|(key, _)| key == WRITE_POLICY_STATE_KEY)
            .expect("write policy key")
            .1 = serde_json::to_string(&policy).expect("serialize policy");
        keys.iter_mut()
            .find(|(key, _)| key == SYNC_ROUTING_HASH_STATE_KEY)
            .expect("routing hash key")
            .1 = ObjectHash::digest(b"routing").to_string();
        if policy == WritePolicy::MergeConcurrent {
            keys.push((
                format!("{MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX}founder/stream"),
                "{}".to_string(),
            ));
        }
        keys.push(("local_device_id".to_string(), "excluded-device".to_string()));
        for (key, value) in keys {
            connection
                .execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (key, value),
                )
                .expect("insert protocol state");
        }
    }

    fn fixture(policy: WritePolicy) -> Connection {
        let connection = Connection::open_in_memory().expect("open projection fixture");
        populate_fixture(&connection, policy);
        connection
    }

    #[test]
    fn generation_zero_projection_accepts_a_wal_database() {
        let directory = tempfile::tempdir().expect("create projection directory");
        let connection = Connection::open(directory.path().join("store.sqlite3"))
            .expect("open file projection fixture");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .expect("enable WAL");
        assert_eq!(journal_mode, "wal");
        populate_fixture(&connection, WritePolicy::MergeConcurrent);

        let bytes = project_generation_zero_image(&connection, WritePolicy::MergeConcurrent)
            .expect("project WAL database");
        let image = open_image(&bytes).expect("open projected WAL image");
        assert_eq!(
            image
                .query_row("SELECT COUNT(*) FROM host_rows", [], |row| row
                    .get::<_, i64>(0))
                .expect("count projected host rows"),
            0
        );
    }

    #[test]
    fn generation_zero_projection_reads_uncommitted_founder_state_and_removes_local_bytes() {
        let mut source = fixture(WritePolicy::MergeConcurrent);
        source
            .execute("DELETE FROM store_device_registration_activations", [])
            .expect("remove committed founder fixture");
        let transaction = source.transaction().expect("begin founder transaction");
        transaction
            .execute(
                "INSERT INTO store_device_registration_activations
                 VALUES ('founder',
                         'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                         'author', 'device', x'01', '{}', '{}')",
                [],
            )
            .expect("insert uncommitted founder");

        let bytes = project_generation_zero_image(&transaction, WritePolicy::MergeConcurrent)
            .expect("project uncommitted founder state");
        assert!(!bytes
            .windows(b"projection-secret-marker".len())
            .any(|window| window == b"projection-secret-marker"));
        let image = open_image(&bytes).expect("open projected image");
        assert_eq!(
            image
                .query_row(
                    "SELECT device_id FROM store_device_registration_activations",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("read uncommitted founder from image"),
            "founder"
        );
        assert_eq!(
            image
                .query_row("SELECT COUNT(*) FROM host_rows", [], |row| row
                    .get::<_, i64>(0))
                .expect("count projected host rows"),
            0
        );
        assert_eq!(
            image
                .query_row("SELECT COUNT(*) FROM remote_objects", [], |row| row
                    .get::<_, i64>(0))
                .expect("count projected remote objects"),
            0
        );
        assert!(image
            .query_row(
                "SELECT value FROM protocol_state WHERE key = 'local_device_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("query excluded local state")
            .is_none());
        transaction.rollback().expect("rollback founder fixture");
    }
}
