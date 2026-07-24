use super::*;

pub(super) enum CovenMetadataOpen<'a> {
    Detect,
    /// A borrowed install authority. Borrowed, not owned, so one verified install
    /// can install onto more than one connection — the restore selection queries a
    /// throwaway copy through the same authority it later installs for real.
    VerifiedSnapshot(&'a VerifiedSnapshotBootstrapInstall),
}

fn protocol_state_exists(conn: &Connection) -> Result<bool, DbError> {
    conn.query_row(
        "SELECT EXISTS(\
             SELECT 1 FROM main.sqlite_schema \
             WHERE type = 'table' AND name = 'protocol_state'\
         )",
        [],
        |row| row.get(0),
    )
    .map_err(DbError::from)
}

fn has_coven_initialization_marker(conn: &Connection) -> Result<bool, DbError> {
    if !protocol_state_exists(conn)? {
        return Ok(false);
    }
    let marker = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_INITIALIZED_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?;
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
        crate::db::apply_coven_routing_schema(conn).map_err(DbError::from)?;
    }
    let schema_manifest = validate_live_coven_schema(conn, install_routing_schema)?;
    let contract_json =
        String::from_utf8(sync_routing_contract.bytes().to_vec()).map_err(|error| {
            DbError::Message(format!("encode sync-routing contract metadata: {error}"))
        })?;
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
    let expected = expected_coven_schema_manifest(include_routing).map_err(DbError::from)?;
    let actual = live_coven_schema_manifest(conn).map_err(DbError::from)?;
    if actual != expected {
        return Err(DbError::Message(format!(
            "Coven schema does not match the current table, index, constraint, primary-key, STRICT, and WITHOUT ROWID declarations: expected {expected:?}, found {actual:?}"
        )));
    }
    serde_json::to_string(&expected)
        .map_err(|error| DbError::Message(format!("serialize Coven schema manifest: {error}")))
}

pub(super) fn validate_initialized_coven_schema(
    conn: &Connection,
    include_routing: bool,
) -> Result<(), DbError> {
    let expected = expected_coven_schema_manifest(include_routing).map_err(DbError::from)?;
    let stored_json = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_SCHEMA_MANIFEST_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message(
                "Store database is missing required Coven schema manifest metadata".to_string(),
            )
        })?;
    let stored: CovenSchemaManifest = serde_json::from_str(&stored_json).map_err(|error| {
        DbError::Message(format!("Store Coven schema manifest is invalid: {error}"))
    })?;
    if stored != expected {
        return Err(DbError::Message(format!(
            "Store Coven schema manifest does not match the current schema: stored {stored:?}, current {expected:?}"
        )));
    }
    validate_live_coven_schema(conn, include_routing)?;
    Ok(())
}

pub(super) fn load_coven_metadata(conn: &Connection) -> Result<SyncRoutingContract, DbError> {
    if !has_coven_initialization_marker(conn)? {
        return Err(DbError::Message(
            "Store database is missing required Coven initialization metadata".to_string(),
        ));
    }
    let contract_bytes = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [SYNC_ROUTING_CONTRACT_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message(
                "Store database is missing required sync_routing_contract metadata".to_string(),
            )
        })?;
    let contract = SyncRoutingContract::from_bytes(contract_bytes.as_bytes()).map_err(|error| {
        DbError::Message(format!("Store sync-routing contract is invalid: {error}"))
    })?;
    let stored_hash = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [SYNC_ROUTING_HASH_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message(
                "Store database is missing required sync_routing_hash metadata".to_string(),
            )
        })?;
    let stored_hash: ObjectHash = stored_hash.parse().map_err(|error| {
        DbError::Message(format!(
            "Store sync_routing_hash metadata is invalid: {error}"
        ))
    })?;
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

impl DatabaseCore {
    pub(super) fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
        metadata_open: CovenMetadataOpen<'_>,
    ) -> Result<(Self, DatabaseState, UpdatedAtStamper), OpenError> {
        let mut conn = open_connection(path)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;

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
        if let Some(pinned) = &pinned_routing_contract {
            validate_initialized_coven_schema(&conn, pinned.has_scoped_graph())?;
        }
        let (schema_version, sync_routing_contract, gates, blob_decls) = {
            let tx = conn.transaction().map_err(DbError::from)?;
            let outcome = (|| -> Result<_, OpenError> {
                let schema_version = run_migrations_in_transaction(&tx, migrations)?;

                // The host ladder and routing validation share this transaction.
                // A pending migration that changes confidentiality topology cannot
                // leave either its DDL or `user_version` advance committed.
                validate_host_synced_tables(&tx, &synced_tables)?;
                let resolved = SyncRoutingContract::from_connection(&tx, &synced_tables)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                if let Some(pinned) = &pinned_routing_contract {
                    validate_sync_routing_contract(pinned, &resolved)?;
                    validate_initialized_coven_schema(&tx, resolved.has_scoped_graph())?;
                } else {
                    initialize_coven_metadata_on(&tx, &resolved, resolved.has_scoped_graph())?;
                }
                pin_host_device_id_on(&tx, hlc.device_id(), initialized)?;
                let gates = Gates::from_tables(&tx, &synced_tables)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let blob_decls = BlobDecls::from_tables(&tx, &synced_tables)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                if let CovenMetadataOpen::VerifiedSnapshot(install) = &metadata_open {
                    if resolved.has_scoped_graph() {
                        let routing_key = install.routing_key.as_ref().ok_or_else(|| {
                            DbError::Message(
                                "scoped snapshot bootstrap requires Store routing encryption"
                                    .to_string(),
                            )
                        })?;
                        gate::validate_snapshot_routing_state(
                            &tx,
                            &gates,
                            routing_key,
                            &crate::sync::circle::Audience::Store,
                        )
                        .map_err(|error| DbError::Message(error.to_string()))?;
                    }
                    install.install_on(&tx, schema_version, resolved.hash(), &synced_tables)?;
                }
                Ok((schema_version, resolved, gates, blob_decls))
            })();
            match outcome {
                Ok(initialized) => {
                    tx.commit().map_err(DbError::from)?;
                    initialized
                }
                Err(error) => return Err(error),
            }
        };
        let sync_routing_hash = sync_routing_contract.hash();
        // Seed the register clock so a restart cannot mint a stamp behind a value
        // already on disk. Floor = max(persisted high-water, max synced-row
        // `_updated_at`).
        let persisted = conn
            .query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [HIGHWATER_STATE_KEY],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?;
        seed_from(&hlc, persisted, "HLC high-water mark in protocol_state")?;
        let seed_wall_ms = hlc.wall_now_ms();
        let seed_bound_ms = seed_wall_ms.saturating_add(MAX_FUTURE_SKEW_MS);
        let on_disk = scan_max_updated_at(&conn, &synced_tables, seed_bound_ms)?;
        seed_from(&hlc, on_disk, "`_updated_at` in synced tables")?;

        let stamper = UpdatedAtStamper::new(hlc.clone());
        let synced_tables = Arc::new(synced_tables);
        let gates = Arc::new(gates);
        let blob_decls = Arc::new(blob_decls);
        blob_decls
            .install_cleanup_guards(&conn)
            .map_err(|e| DbError::Message(e.to_string()))?;
        gate::attach_empty_clone(&conn, &gates)
            .map_err(|error| DbError::Message(format!("install host transaction gate: {error}")))?;
        let core = DatabaseCore {
            conn,
            hlc,
            synced_tables,
            schema_version,
            sync_routing_hash,
            gates,
            blob_decls,
            blob_tombstone_grace,
            transfer_limits,
        };
        let state = core.state();

        Ok((core, state, stamper))
    }

    /// Open the connection at `path` read-only: a `SQLITE_OPEN_READONLY`
    /// connection resolving the same gate/blob models a writer open resolves, but
    /// running no migration ladder and no schema/bookkeeping writes. It refuses a
    /// db a newer binary migrated past this one (the writer's `SchemaTooNew`
    /// policy), since its models must understand the on-disk schema. Backs
    /// [`Database::open_read_only`]; see it for why a reader takes no store lock.
    pub(super) fn open_read_only(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Self, DatabaseState), OpenError> {
        let conn = open_connection_read_only(path)?;
        // `foreign_keys` is a per-connection runtime setting, not a write to the db
        // file, so it is allowed on a read-only connection; keeping it on matches the
        // writer's relational view. A read never inserts, so it enforces nothing new.
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;
        // Open against the on-disk schema exactly as the writer left it: run no
        // migration ladder (that writes), but refuse a schema newer than this binary
        // knows — the same policy `run_migrations` applies — because the gate and blob
        // models below are resolved against a schema this binary must understand.
        let schema_version = crate::migration::ensure_schema_supported(&conn, migrations)?;

        // Reads only (PRAGMA table_info): assert the host tables the writer created
        // still present the synced-table contract, so a wrong schema fails loud at
        // open rather than mid-read.
        validate_host_synced_tables(&conn, &synced_tables)?;
        let pinned_routing_contract = load_coven_metadata(&conn)?;
        validate_initialized_coven_schema(&conn, pinned_routing_contract.has_scoped_graph())?;
        validate_host_device_id_on(&conn, hlc.device_id())?;
        let sync_routing_contract = SyncRoutingContract::from_connection(&conn, &synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))?;
        validate_sync_routing_contract(&pinned_routing_contract, &sync_routing_contract)?;
        let sync_routing_hash = sync_routing_contract.hash();

        let synced_tables = Arc::new(synced_tables);

        // No register-clock seeding: a reader never mints an `_updated_at`, so it has
        // no stamp to keep ahead of on-disk values.
        let gates = Arc::new(
            Gates::from_tables(&conn, &synced_tables)
                .map_err(|e| DbError::Message(e.to_string()))?,
        );
        let blob_decls = Arc::new(
            BlobDecls::from_tables(&conn, &synced_tables)
                .map_err(|e| DbError::Message(e.to_string()))?,
        );
        let core = DatabaseCore {
            conn,
            hlc,
            synced_tables,
            schema_version,
            sync_routing_hash,
            gates,
            blob_decls,
            blob_tombstone_grace,
            transfer_limits,
        };
        let state = core.state();
        Ok((core, state))
    }

    fn state(&self) -> DatabaseState {
        DatabaseState {
            hlc: self.hlc.clone(),
            synced_tables: self.synced_tables.clone(),
            schema_version: self.schema_version,
            sync_routing_hash: self.sync_routing_hash,
            gates: self.gates.clone(),
            blob_decls: self.blob_decls.clone(),
            blob_tombstone_grace: self.blob_tombstone_grace,
            transfer_limits: self.transfer_limits,
            store_runtime: crate::sync::store::StoreDatabaseRuntime::default(),
            local_blob_cleanup: Arc::new(tokio::sync::Mutex::new(())),
            ids: Arc::new(crate::id_provider::UuidProvider),
            write_statuses: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(any(test, feature = "test-utils"))]
            test_pause_points: Arc::new(TestPausePoints::default()),
            #[cfg(any(test, feature = "test-utils"))]
            merge_materialization_failure: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub(super) fn connection(&self) -> &Connection {
        &self.conn
    }
}

/// A unit of work for the connection thread: a caller's closure to run against
/// the owned core, or the sentinel the last [`Database`] clone sends as it drops
/// to stop the thread. A `Run` closure carries its own reply channel, so it
/// returns `()` — [`Database::on_connection_thread`] builds it to capture the
/// caller's result and send it back.
pub(super) enum DbJob {
    Run(Box<dyn FnOnce(&mut DatabaseCore) + Send>),
    Stop,
}

/// The connection thread's send channel and join handle, shared by every
/// [`Database`] clone through an `Arc`. The last clone to drop shuts the thread
/// down and joins it, so the connection closes on the thread that owned it before
/// control returns to the dropper.
pub(super) struct ConnectionThread {
    pub(super) jobs: tokio::sync::mpsc::UnboundedSender<DbJob>,
    pub(super) join: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ConnectionThread {
    fn drop(&mut self) {
        // Reached only when the last `Database` clone drops — no other clone can
        // still be sending. Queue `Stop` behind whatever jobs are already in
        // flight so the thread drains them, exits, and drops the core (closing the
        // connection). A send error means the thread already stopped.
        let _ = self.jobs.send(DbJob::Stop);
        let handle = match self.join.take() {
            Some(handle) => handle,
            None => return,
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            // A `Database` handle roams freely across async tasks, so this last
            // drop can land inside a runtime task. Joining here would block that
            // worker until the queue drains — the very stall this thread exists to
            // remove. Detach instead: the thread drains its queue, drops the core,
            // and exits on its own. Every queued job's effect is durable, so a
            // thread left unjoined loses nothing; only the deterministic close
            // moves off this task.
            drop(handle);
        } else {
            // Sync context — tests, process teardown — where there is no worker to
            // stall. Join for a deterministic shutdown: the connection is fully
            // closed before we return. Jobs run under `catch_unwind`, so the thread
            // never unwinds from a caller's closure; a join error is a real fault
            // (a panic in the core's own drop) and is surfaced, not swallowed.
            if handle.join().is_err() {
                error!("database connection thread panicked");
            }
        }
    }
}

/// Own the connection on this thread and run each queued job in send order until
/// `Stop`. The channel's FIFO is the serialization the `tokio::Mutex` used to
/// provide, and the connection never leaves this thread.
pub(super) fn run_connection_thread(
    mut core: DatabaseCore,
    mut jobs: tokio::sync::mpsc::UnboundedReceiver<DbJob>,
) {
    while let Some(job) = jobs.blocking_recv() {
        match job {
            DbJob::Run(f) => f(&mut core),
            DbJob::Stop => break,
        }
    }
    // Loop exited: drop `core` here — closing the connection on the thread that
    // has owned it throughout.
}
