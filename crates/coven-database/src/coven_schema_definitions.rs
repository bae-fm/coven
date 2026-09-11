pub(crate) const CLOUD_OUTBOX_COLUMNS: &str = "
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL CHECK (operation IN ('upload', 'delete')),
    table_name TEXT,
    row_id TEXT,
    column_name TEXT,
    row_stamp TEXT,
    root_table TEXT,
    root_id TEXT,
    root_label TEXT,
    row_ref TEXT CHECK (row_ref IS NULL OR json_valid(row_ref)),
    upload_state TEXT CHECK (upload_state IS NULL OR json_valid(upload_state)),
    stored_ref TEXT CHECK (stored_ref IS NULL OR json_valid(stored_ref)),
    source_path TEXT,
    retain_pinned INTEGER CHECK (retain_pinned IS NULL OR retain_pinned IN (0, 1)),
    created_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    last_attempt_at TEXT,
    CHECK (
        (operation = 'upload' AND table_name IS NOT NULL AND row_id IS NOT NULL
         AND column_name IS NOT NULL AND row_stamp IS NOT NULL
         AND root_table IS NOT NULL AND root_id IS NOT NULL AND root_label IS NOT NULL
         AND row_ref IS NOT NULL
         AND stored_ref IS NULL AND source_path IS NOT NULL AND retain_pinned IS NOT NULL
         AND upload_state IS NOT NULL)
        OR
        (operation = 'delete' AND table_name IS NULL AND row_id IS NULL
         AND column_name IS NULL AND row_stamp IS NULL
         AND root_table IS NULL AND root_id IS NULL AND root_label IS NULL
         AND row_ref IS NULL
         AND stored_ref IS NOT NULL AND source_path IS NULL AND retain_pinned IS NULL
         AND upload_state IS NULL)
    ),
    UNIQUE (operation, table_name, row_id, column_name, row_stamp),
    UNIQUE (stored_ref)
";

pub(crate) const BLOB_MAKE_REMOTE_INTENTS_COLUMNS: &str = "
    root_table TEXT NOT NULL,
    root_id TEXT NOT NULL,
    root_label TEXT NOT NULL,
    retain_pinned INTEGER NOT NULL CHECK (retain_pinned IN (0, 1)),
    state TEXT NOT NULL CHECK (state IN ('uploading', 'cancelling', 'publishing')),
    write_id TEXT UNIQUE,
    CHECK ((state = 'publishing') = (write_id IS NOT NULL)),
    PRIMARY KEY (root_table, root_id)
";

pub(crate) const CLOUD_OUTBOX_V0_COLUMNS: &str = "
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL CHECK (operation IN ('upload', 'delete')),
    table_name TEXT,
    row_id TEXT,
    column_name TEXT,
    row_stamp TEXT,
    root_table TEXT,
    root_id TEXT,
    row_ref TEXT CHECK (row_ref IS NULL OR json_valid(row_ref)),
    upload_state TEXT CHECK (upload_state IS NULL OR json_valid(upload_state)),
    stored_ref TEXT CHECK (stored_ref IS NULL OR json_valid(stored_ref)),
    source_path TEXT,
    retain_pinned INTEGER CHECK (retain_pinned IS NULL OR retain_pinned IN (0, 1)),
    created_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    last_attempt_at TEXT,
    CHECK (
        (operation = 'upload' AND table_name IS NOT NULL AND row_id IS NOT NULL
         AND column_name IS NOT NULL AND row_stamp IS NOT NULL
         AND root_table IS NOT NULL AND root_id IS NOT NULL AND row_ref IS NOT NULL
         AND stored_ref IS NULL AND source_path IS NOT NULL AND retain_pinned IS NOT NULL
         AND upload_state IS NOT NULL)
        OR
        (operation = 'delete' AND table_name IS NULL AND row_id IS NULL
         AND column_name IS NULL AND row_stamp IS NULL
         AND root_table IS NULL AND root_id IS NULL AND row_ref IS NULL
         AND stored_ref IS NOT NULL AND source_path IS NULL AND retain_pinned IS NULL
         AND upload_state IS NULL)
    ),
    UNIQUE (operation, table_name, row_id, column_name, row_stamp),
    UNIQUE (stored_ref)
";

pub(crate) const BLOB_MAKE_REMOTE_INTENTS_V0_COLUMNS: &str = "
    root_table TEXT NOT NULL,
    root_id TEXT NOT NULL,
    retain_pinned INTEGER NOT NULL CHECK (retain_pinned IN (0, 1)),
    state TEXT NOT NULL CHECK (state IN ('uploading', 'cancelling', 'publishing')),
    write_id TEXT UNIQUE,
    CHECK ((state = 'publishing') = (write_id IS NOT NULL)),
    PRIMARY KEY (root_table, root_id)
";

pub(crate) const OBJECT_OWNERSHIP_TRIGGERS: &str = "
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_inert_on_insert
BEFORE INSERT ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is protocol-inert');
END;
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_inert_on_update
BEFORE UPDATE OF object_id ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is protocol-inert');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_remote_on_insert
BEFORE INSERT ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity has active ownership');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_remote_on_update
BEFORE UPDATE OF object_id ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity has active ownership');
END;
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_reclaimed_on_insert
BEFORE INSERT ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_reclaimed_on_update
BEFORE UPDATE OF object_id ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_reclaimed_on_insert
BEFORE INSERT ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_reclaimed_on_update
BEFORE UPDATE OF object_id ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS reclaimed_store_package_identity_must_be_closed_on_insert
BEFORE INSERT ON reclaimed_store_packages
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
    UNION ALL
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'reclaimed Store package identity has another ownership state');
END;
CREATE TRIGGER IF NOT EXISTS reclaimed_store_package_identity_must_be_closed_on_update
BEFORE UPDATE OF object_id ON reclaimed_store_packages
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
    UNION ALL
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'reclaimed Store package identity has another ownership state');
END;
";

/// `retained_replay_baselines` as version 1 of the Coven schema wrote it. The
/// current shape dropped `generation` (always zero) and `exact_cut` (the
/// coverage the baseline's authority already carries).
pub(crate) const RETAINED_REPLAY_BASELINES_V1_COLUMNS: &str = "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    exact_cut TEXT NOT NULL CHECK (json_valid(exact_cut)),
    schema_version INTEGER NOT NULL CHECK (schema_version >= 0),
    routing_hash TEXT NOT NULL CHECK (length(routing_hash) = 64),
    image_payload_hash TEXT NOT NULL CHECK (length(image_payload_hash) = 64),
    authority_hash TEXT NOT NULL CHECK (length(authority_hash) = 64)
";

/// `outbound_store_snapshot` as version 1 wrote it. The current shape dropped
/// `image_ref` and `rollup_ref`: the signed snapshot metadata names its
/// payloads itself.
pub(crate) const OUTBOUND_STORE_SNAPSHOT_V1_COLUMNS: &str = "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    snapshot_ref TEXT NOT NULL CHECK (json_valid(snapshot_ref)),
    meta_prepared TEXT NOT NULL CHECK (json_valid(meta_prepared)),
    image_ref TEXT NOT NULL CHECK (json_valid(image_ref)),
    rollup_ref TEXT NOT NULL CHECK (json_valid(rollup_ref)),
    meta_bytes BLOB NOT NULL,
    blobs TEXT NOT NULL CHECK (json_valid(blobs))
";

/// `outbound_circle_snapshot` as version 1 wrote it. The current shape dropped
/// `image_ref` for the same reason as the Store snapshot's.
pub(crate) const OUTBOUND_CIRCLE_SNAPSHOT_V1_COLUMNS: &str = "
    circle_id TEXT PRIMARY KEY,
    snapshot_ref TEXT NOT NULL CHECK (json_valid(snapshot_ref)),
    meta_prepared TEXT NOT NULL CHECK (json_valid(meta_prepared)),
    image_ref TEXT NOT NULL CHECK (json_valid(image_ref)),
    meta_bytes BLOB NOT NULL
";
