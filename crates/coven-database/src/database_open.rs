use super::*;
use coven_foundation::stage_timing::StageTimings;
use coven_foundation::store_dir::StoreDir;

pub(crate) enum CovenMetadataOpen<'a> {
    Detect,
    /// A borrowed install authority. Borrowed, not owned, so one verified install
    /// can install onto more than one connection — the restore selection queries a
    /// throwaway copy through the same authority it later installs for real.
    VerifiedSnapshot(&'a VerifiedSnapshotBootstrapInstall),
}

fn has_coven_initialization_marker(conn: &Connection) -> Result<bool, DbError> {
    let protocol_state_exists: bool = conn
        .query_row(
            "SELECT EXISTS(\
             SELECT 1 FROM main.sqlite_schema \
             WHERE type = 'table' AND name = 'protocol_state'\
         )",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if !protocol_state_exists {
        return Ok(false);
    }
    let marker = get_protocol_state_on(conn, COVEN_INITIALIZED_STATE_KEY)?;
    match marker.as_deref() {
        None => Ok(false),
        Some(COVEN_INITIALIZED_STATE_VALUE) => Ok(true),
        Some(value) => Err(DbError::Message(format!(
            "Store database has invalid Coven initialization marker {value:?}"
        ))),
    }
}

fn initialize_coven_metadata_on(
    conn: &Connection,
    sync_routing_contract: &SyncRoutingContract,
    install_routing_schema: bool,
) -> Result<(), DbError> {
    apply_coven_schema(conn).map_err(DbError::from)?;
    if install_routing_schema {
        apply_coven_routing_schema(conn).map_err(DbError::from)?;
    }
    let schema_manifest = validate_live_coven_schema(conn, install_routing_schema)?;
    let contract_json = String::from_utf8(sync_routing_contract.bytes().to_vec())
        .map_err(|error| DbError::context("encode sync-routing contract metadata", error))?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (SYNC_ROUTING_CONTRACT_STATE_KEY, contract_json),
    )
    .map_err(DbError::from)?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (COVEN_SCHEMA_MANIFEST_STATE_KEY, schema_manifest),
    )
    .map_err(DbError::from)?;
    initialize_coven_schema_version(conn)?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (
            SYNC_ROUTING_HASH_STATE_KEY,
            sync_routing_contract.hash().to_string(),
        ),
    )
    .map_err(DbError::from)?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (COVEN_INITIALIZED_STATE_KEY, COVEN_INITIALIZED_STATE_VALUE),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

fn validate_live_coven_schema(conn: &Connection, include_routing: bool) -> Result<String, DbError> {
    let expected = expected_coven_schema_manifest(include_routing)?;
    let actual = live_coven_schema_manifest(conn).map_err(DbError::from)?;
    if actual != *expected {
        return Err(DbError::Message(format!(
            "Coven schema does not match the current table, index, constraint, primary-key, STRICT, and WITHOUT ROWID declarations: expected {expected:?}, found {actual:?}"
        )));
    }
    serde_json::to_string(&expected)
        .map_err(|error| DbError::context("serialize Coven schema manifest", error))
}

pub(crate) fn validate_initialized_coven_schema(
    conn: &Connection,
    include_routing: bool,
) -> Result<(), DbError> {
    let expected = expected_coven_schema_manifest(include_routing)?;
    let stored_json =
        get_protocol_state_on(conn, COVEN_SCHEMA_MANIFEST_STATE_KEY)?.ok_or_else(|| {
            DbError::Message(
                "Store database is missing required Coven schema manifest metadata".to_string(),
            )
        })?;
    let stored: CovenSchemaManifest = serde_json::from_str(&stored_json)
        .map_err(|error| DbError::context("Store Coven schema manifest is invalid", error))?;
    if stored != *expected {
        return Err(DbError::Message(format!(
            "Store Coven schema manifest does not match the current schema: stored {stored:?}, current {expected:?}"
        )));
    }
    validate_live_coven_schema(conn, include_routing)?;
    Ok(())
}

pub(crate) fn load_coven_metadata(conn: &Connection) -> Result<SyncRoutingContract, DbError> {
    if !has_coven_initialization_marker(conn)? {
        return Err(DbError::Message(
            "Store database is missing required Coven initialization metadata".to_string(),
        ));
    }
    let contract_bytes =
        get_protocol_state_on(conn, SYNC_ROUTING_CONTRACT_STATE_KEY)?.ok_or_else(|| {
            DbError::Message(
                "Store database is missing required sync_routing_contract metadata".to_string(),
            )
        })?;
    let contract = SyncRoutingContract::from_bytes(contract_bytes.as_bytes())?;
    let stored_hash =
        get_protocol_state_on(conn, SYNC_ROUTING_HASH_STATE_KEY)?.ok_or_else(|| {
            DbError::Message(
                "Store database is missing required sync_routing_hash metadata".to_string(),
            )
        })?;
    let stored_hash: ObjectHash = stored_hash
        .parse()
        .map_err(|error| DbError::context("Store sync_routing_hash metadata is invalid", error))?;
    if stored_hash != contract.hash() {
        return Err(DbError::Message(format!(
            "Store sync-routing contract hashes to {}, but metadata records {stored_hash}",
            contract.hash(),
        )));
    }
    Ok(contract)
}

fn validate_sync_routing_contract(
    pinned: &SyncRoutingContract,
    resolved: &SyncRoutingContract,
) -> Result<(), DbError> {
    if pinned.bytes() != resolved.bytes() || pinned.hash() != resolved.hash() {
        return Err(DbError::Message(format!(
            "Store sync-routing hash is {}, but open resolved {}",
            pinned.hash(),
            resolved.hash(),
        )));
    }
    Ok(())
}

fn validate_durable_coven_state(conn: &Connection) -> Result<(), DbError> {
    let foreign_key_violation: Option<(String, Option<i64>, String, i64)> = conn
        .query_row(
            "SELECT \"table\", rowid, parent, fkid
             FROM pragma_foreign_key_check
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    if let Some((table, rowid, parent, foreign_key)) = foreign_key_violation {
        return Err(DbError::Message(format!(
            "Store database foreign key {foreign_key} from {table} row {rowid:?} to {parent} is invalid"
        )));
    }
    Ok(())
}

impl DatabaseCore {
    pub(crate) fn open(
        path: &Path,
        store_dir: StoreDir,
        connection_durability: crate::connection_io::ConnectionDurability,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: Arc<Hlc>,
        coven_migration_policy: CovenMigrationPolicy,
        migrations: &[Migration],
        metadata_open: CovenMetadataOpen<'_>,
    ) -> Result<Self, OpenError> {
        // A device join spends most of its wall time inside this function, and
        // from the caller it is one opaque step. Every phase below scales with
        // something different — the migration ladder with the number of
        // migrations, the snapshot install and the two full-image passes at the
        // end with the size of the image — so they are named separately.
        let mut timings = StageTimings::start("Store database open");
        let mut conn = timings.mark("open the connection", || {
            let conn = Connection::open(path).map_err(DbError::from)?;
            // WAL so the read-only connection `Coven::open` pairs with this writer
            // keeps serving reads while this one commits, rather than queueing
            // behind a rollback journal's exclusive commit lock. See
            // `configure_connection_durability` for the whole choice.
            crate::connection_io::configure_connection_durability(&conn, connection_durability)?;
            conn.pragma_update(None, "foreign_keys", "ON")
                .map_err(DbError::from)?;
            Ok::<_, OpenError>(conn)
        })?;
        let initialized = match &metadata_open {
            CovenMetadataOpen::VerifiedSnapshot(_) => false,
            CovenMetadataOpen::Detect => {
                let initialized = has_coven_initialization_marker(&conn)?;
                if !initialized
                    && !live_coven_schema_manifest(&conn)
                        .map_err(DbError::from)?
                        .is_empty()
                {
                    return Err(DbError::Message(
                        "Store database contains Coven schema objects without the required initialization marker"
                            .to_string(),
                    )
                    .into());
                }
                initialized
            }
        };
        let pinned_routing_contract = initialized
            .then(|| load_coven_metadata(&conn))
            .transpose()?;
        let (schema_version, sync_routing_contract, gates, blob_decls) = {
            let tx = conn.transaction().map_err(DbError::from)?;
            let outcome = (|| -> Result<_, OpenError> {
                if let Some(pinned) = &pinned_routing_contract {
                    timings.mark("migrate Coven schema", || {
                        run_coven_migrations_in_transaction(
                            &tx,
                            pinned.has_scoped_graph(),
                            coven_migration_policy,
                        )
                    })?;
                }
                let schema_version = timings.mark("migrate host schema", || {
                    run_migrations_in_transaction(&tx, migrations)
                })?;

                // The host ladder and routing validation share this transaction.
                // A pending migration that changes confidentiality topology cannot
                // leave either its DDL or `user_version` advance committed.
                // Contract validation reads every row identity in every synced
                // table, so it scales with the image rather than the schema and
                // is named apart from the introspection that follows it.
                timings.mark("validate the host tables", || {
                    validate_host_synced_tables(&tx, &synced_tables)
                })?;
                let (resolved, gates, blob_decls) =
                    timings.mark("resolve the host tables", || {
                        let resolved = SyncRoutingContract::from_connection(&tx, &synced_tables)
                            .map_err(DbError::from)?;
                        match (&pinned_routing_contract, &metadata_open) {
                            (Some(pinned), _) => {
                                validate_sync_routing_contract(pinned, &resolved)?;
                                validate_initialized_coven_schema(
                                    &tx,
                                    resolved.has_scoped_graph(),
                                )?;
                            }
                            (None, CovenMetadataOpen::VerifiedSnapshot(_)) => {
                                run_uninitialized_snapshot_coven_migrations_in_transaction(
                                    &tx,
                                    resolved.has_scoped_graph(),
                                    coven_migration_policy,
                                )?;
                                initialize_coven_metadata_on(
                                    &tx,
                                    &resolved,
                                    resolved.has_scoped_graph(),
                                )?;
                            }
                            (None, CovenMetadataOpen::Detect) => {
                                initialize_coven_metadata_on(
                                    &tx,
                                    &resolved,
                                    resolved.has_scoped_graph(),
                                )?;
                            }
                        }
                        pin_host_device_id_on(&tx, hlc.device_id(), initialized)?;
                        let gates =
                            Gates::from_tables(&tx, &synced_tables).map_err(DbError::from)?;
                        let blob_decls =
                            BlobDecls::from_tables(&tx, &synced_tables).map_err(DbError::from)?;
                        Ok::<_, OpenError>((resolved, gates, blob_decls))
                    })?;
                if let CovenMetadataOpen::VerifiedSnapshot(install) = &metadata_open {
                    if resolved.has_scoped_graph() {
                        let routing_key = install.routing_key.as_ref().ok_or_else(|| {
                            DbError::Message(
                                "scoped snapshot bootstrap requires Store routing encryption"
                                    .to_string(),
                            )
                        })?;
                        timings.mark("validate the snapshot routing", || {
                            gate::validate_snapshot_routing_state(
                                &tx,
                                &gates,
                                routing_key,
                                &coven_protocol::circle::Audience::Store,
                            )
                            .map_err(DbError::from)
                        })?;
                    }
                    timings.mark("install the snapshot", || {
                        crate::store::install_verified_snapshot_bootstrap_on(
                            &tx,
                            &store_dir,
                            install,
                            schema_version,
                            resolved.hash(),
                            &synced_tables,
                        )
                    })?;
                }
                Ok((schema_version, resolved, gates, blob_decls))
            })();
            match outcome {
                Ok(initialized) => {
                    timings.mark("commit the open", || tx.commit().map_err(DbError::from))?;
                    initialized
                }
                Err(error) => return Err(error),
            }
        };
        let sync_routing_hash = sync_routing_contract.hash();
        // Both of these walk the whole database rather than anything this open
        // changed: the foreign-key check visits every row of every child table,
        // and the clock seed reads a max per synced table — over an expression
        // no index serves, so it is a scan too. With the row-identity pass
        // above, a freshly installed snapshot image is read end to end three
        // times before the database is usable.
        timings.mark("check foreign keys", || validate_durable_coven_state(&conn))?;
        // Seed the register clock so a restart cannot mint a stamp behind a value
        // already on disk. Floor = max(persisted high-water, max synced-row
        // `_updated_at`).
        timings.mark("seed the clock", || {
            let persisted = get_protocol_state_on(&conn, HIGHWATER_STATE_KEY)?;
            seed_from(&hlc, persisted, "HLC high-water mark in protocol_state")?;
            let seed_wall_ms = hlc.wall_now_ms();
            let seed_bound_ms = seed_wall_ms.saturating_add(MAX_FUTURE_SKEW_MS);
            let on_disk = scan_max_updated_at(&conn, &synced_tables, seed_bound_ms)?;
            seed_from(&hlc, on_disk, "`_updated_at` in synced tables")
        })?;

        let synced_tables = Arc::new(synced_tables);
        let gates = Arc::new(gates);
        let blob_decls = Arc::new(blob_decls);
        timings.mark("install the guards", || {
            blob_decls
                .install_cleanup_guards(&conn)
                .map_err(DbError::from)?;
            gate::attach_empty_clone(&conn, &gates)
                .map_err(|error| DbError::context("install host transaction gate", error))
        })?;
        timings.report();
        Ok(DatabaseCore::new(
            store_dir,
            conn,
            hlc,
            synced_tables,
            schema_version,
            sync_routing_hash,
            gates,
            blob_decls,
            blob_tombstone_grace,
            transfer_limits,
            true,
        ))
    }

    /// Open the connection at `path` read-only: a `SQLITE_OPEN_READONLY`
    /// connection resolving the same gate/blob models a writer open resolves, but
    /// running no migration ladder and no schema/bookkeeping writes. It refuses a
    /// db a newer binary migrated past this one (the writer's `SchemaTooNew`
    /// policy), since its models must understand the on-disk schema. Backs
    /// [`Database::open_read_only`]; see it for why a reader takes no store lock.
    pub(crate) fn open_read_only(
        path: &Path,
        store_dir: StoreDir,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<Self, OpenError> {
        use rusqlite::OpenFlags;
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
        let conn = Connection::open_with_flags(path, flags).map_err(DbError::from)?;
        // `foreign_keys` is a per-connection runtime setting, not a write to the db
        // file, so it is allowed on a read-only connection; keeping it on matches the
        // writer's relational view. A read never inserts, so it enforces nothing new.
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;
        // Open against the on-disk schema exactly as the writer left it: run no
        // migration ladder (that writes), but refuse a schema newer than this binary
        // knows — the same policy `run_migrations` applies — because the gate and blob
        // models below are resolved against a schema this binary must understand.
        let schema_version = crate::ensure_schema_supported(&conn, migrations)?;

        // Reads only (PRAGMA table_info): assert the host tables the writer created
        // still present the synced-table contract, so a wrong schema fails loud at
        // open rather than mid-read.
        validate_host_synced_tables(&conn, &synced_tables)?;
        let pinned_routing_contract = load_coven_metadata(&conn)?;
        validate_coven_schema_for_reader(&conn, pinned_routing_contract.has_scoped_graph())?;
        validate_host_device_id_on(&conn, hlc.device_id())?;
        let sync_routing_contract =
            SyncRoutingContract::from_connection(&conn, &synced_tables).map_err(DbError::from)?;
        validate_sync_routing_contract(&pinned_routing_contract, &sync_routing_contract)?;
        let sync_routing_hash = sync_routing_contract.hash();
        validate_durable_coven_state(&conn)?;

        let synced_tables = Arc::new(synced_tables);

        // No register-clock seeding: a reader never mints an `_updated_at`, so it has
        // no stamp to keep ahead of on-disk values.
        let gates = Arc::new(Gates::from_tables(&conn, &synced_tables).map_err(DbError::from)?);
        let blob_decls =
            Arc::new(BlobDecls::from_tables(&conn, &synced_tables).map_err(DbError::from)?);
        Ok(DatabaseCore::new(
            store_dir,
            conn,
            hlc,
            synced_tables,
            schema_version,
            sync_routing_hash,
            gates,
            blob_decls,
            blob_tombstone_grace,
            transfer_limits,
            false,
        ))
    }
}
