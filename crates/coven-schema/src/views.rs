//! Coven's state as the app's SQL reads it (Appendix C.1, api.md E4): views
//! over coven's own tables, so they are true in every transaction that
//! changes those tables. App SQL reads them and nothing writes them.
//!
//! Each view reads each of its tables through a column, in a join or a
//! subquery: SQLite names the view to the authorizer for each column the
//! view reads, which is how coven's authorizer tells a read through a view
//! from a read of coven's tables themselves. A table whose columns a statement leaves
//! unused, as in `SELECT count(*) FROM coven_uploads`, SQLite reports with no
//! column and no view; that read is allowed only for a table the same
//! statement reached through a view, so a view never only counts a table's
//! rows.

/// What app SQL may read of coven's: these views, the files table, the
/// tables coven keeps for the app's triggers to follow
/// (`pins.rs`, `pin_requests.rs`, `moves.rs`, `upload_queue.rs`, `pending.rs`,
/// `sync_status.rs`, `retry_effect.rs`), and the sizes of the store's own
/// files (`local_bytes_schema.rs`).
pub const READABLE: [&str; 28] = [
    crate::FILES,
    "coven_file_reads",
    "coven_user_files",
    "coven_uploads",
    "coven_upload_phases",
    "coven_upload_pause",
    "coven_network",
    "coven_sending",
    "coven_downloading",
    "coven_device_only_downloading",
    "coven_device_only_placed",
    "coven_cache_bytes",
    "coven_library_bytes",
    "coven_archive_bytes",
    crate::UPLOAD_QUEUE,
    crate::PINS,
    crate::PIN_REQUESTS,
    crate::MOVES,
    crate::SYNC_STATUS,
    crate::RETRY_EFFECT,
    "coven_sync_state",
    crate::PENDING,
    "coven_invites",
    "coven_operations",
    "coven_members",
    "coven_devices",
    "coven_circles",
    "coven_circle_members",
];

pub(crate) const SCHEMA: &str = concat!(
    // Where a read on this device takes each file's bytes from (§17.7), by
    // the file's record: a stored file from the cache or storage, a file on
    // this device only from its bytes here, its original or coven's copy, and
    // a file on another device only from nowhere, since a read of it fails
    // with `DeviceOnly`.
    "CREATE VIEW coven_file_reads(file,source) AS
        SELECT f.id,CASE WHEN coven_file_device(f.coordinates) IS NULL THEN 'stored'
            WHEN coven_file_device(f.coordinates)=_coven_device() THEN 'here' END
        FROM coven_files AS f;",
    // The user's originals this device records, by file: the path, as the
    // operating system's bytes, which `coven_path_text` shows, and the size
    // and exact modification time recorded when each was staged, as whole
    // seconds since the Unix epoch rounded down and the nanoseconds past
    // that second (§17.3). An original kept only for a reset that may
    // bring its record back isn't listed: its file has no record here, or
    // one on another device only (§17.11).
    "CREATE VIEW coven_user_files(file,path,size,modified_seconds,modified_nanos) AS
        SELECT u.file,u.path,u.size,_coven_unix_seconds(u.modified_at),
            _coven_unix_nanos(u.modified_at)
        FROM _coven_user_files AS u
        WHERE NOT EXISTS(SELECT 1 FROM _coven_undoable_files AS k WHERE k.file=u.file)
            OR EXISTS(SELECT 1 FROM main.coven_files AS f WHERE f.id=u.file
                AND coalesce(coven_file_device(f.coordinates)=_coven_device(),1));",
    // The sync state a status screen shows, in one row (§19.1): its kind,
    // when sync last finished, the pending record that names it, and the
    // whole state as sync encodes it. `_coven_sync_state` computes it from
    // the facts the subquery reads (`sync_facts.rs`), in sync's own Rust
    // code, so that an app's screen reads the state in its own live query
    // and the Rust code stays the one definition the Lean model checks
    // (`sync_functions.rs`). OFFSET 0 keeps SQLite from merging the
    // subquery into the outer SELECT, which would call the function once
    // for each column. The subquery reads the files being sent through
    // `coven_sending`, so a live query of this view runs again as a send
    // starts or ends. The subquery also reads `coven_sync_status` and
    // `coven_retry_effect`, so on a read-only handle the policy refuses each
    // read of this view with `StatementForbidden` (`WRITER_STATE`), and the
    // read returns no state.
    "CREATE VIEW coven_sync_state(state,last_synced,pending,detail) AS
        SELECT x.state->>'$.state',x.last_synced,x.state->>'$.pending',
            x.state->'$.detail'
        FROM (SELECT _coven_sync_state(",
    crate::facts!(),
    ") AS state,s.last_synced AS last_synced
            FROM coven_sync_status AS s JOIN coven_retry_effect AS r
            LIMIT 1 OFFSET 0) AS x;",
    // The files waiting in this device's upload queue, as `uploads()` lists
    // them, with what each waits for and the bytes the provider confirmed.
    // Files sharing one upload share its place. The pause holds only
    // trailing uploads (§17.4), and so does the setting to move files only
    // on unmetered networks while the network is metered (§17.9). `sending`
    // and the network are this process's, which no table holds
    // (`transfers_in_flight.rs`, `network_facts.rs`). `move` is the upload's
    // move, for the app to cancel, but never a bound upload's, which can't
    // be cancelled.
    "CREATE VIEW coven_uploads(file,position,paused,metered,retry_at,pending,
            bytes_confirmed,bytes_total,sending,move) AS
        SELECT l.file,u.id,z.paused IS NOT NULL AND u.holds IS NULL,
            w.unmetered_only IS NOT NULL AND _coven_network_metered() AND u.holds IS NULL,
            _coven_unix_ms(r.not_before),e.value,
            u.bytes_confirmed,u.bytes_total,_coven_sending(u.object),
            CASE WHEN u.holds IS NULL THEN u.move END
        FROM _coven_upload_files AS l
        JOIN _coven_file_uploads AS u ON u.id=l.upload
        LEFT JOIN _coven_upload_pause AS z
        LEFT JOIN _coven_unmetered_only AS w
        LEFT JOIN _coven_retries AS r ON r.upload=u.id
        LEFT JOIN _coven_pending AS e ON e.device=u.device AND e.object=u.object;",
    // What each file in this device's upload queue is doing, as `uploads()`
    // gives each one's phase (§17.9): `_coven_upload_phase` computes it from
    // the facts it reads (`sync_facts.rs`), in sync's own Rust code, as
    // `_coven_sync_state` computes the state. The inner subquery reads once
    // what holds every transfer and what holds trailing uploads; each upload
    // adds whether this process sends it, read through `coven_sending` so a
    // live query of this view runs again as a send starts or ends, whether
    // storage holds its bytes, whether it is bound, its retry wait, its own
    // record and its move's, and its original. Files sharing one upload
    // share its phase. `at` is when the wait ends, for a retry as
    // `coven_uploads.retry_at` holds it and for a cooldown as
    // `coven_retry_effect.cooling_until` does. OFFSET 0 keeps SQLite from
    // merging either subquery into the SELECT around it, which would call
    // the function once for each column. The view reads `coven_sync_status`
    // and `coven_retry_effect`, so on a read-only handle the policy refuses
    // each read of it with `StatementForbidden` (`WRITER_STATE`).
    "CREATE VIEW coven_upload_phases(file,phase,wait,at,pending) AS
        SELECT y.file,y.phase->>'$.phase',y.phase->>'$.wait',
            CASE y.phase->>'$.wait' WHEN 'retrying' THEN y.retry_at
                WHEN 'cooling_down' THEN y.cooling_until END,
            y.phase->>'$.pending'
        FROM (SELECT l.file AS file,_coven_unix_ms(t.not_before) AS retry_at,
                x.cooling_until AS cooling_until,
                _coven_upload_phase(x.syncing,x.cooling_until,x.last_pass,x.paused,
                    x.unmetered_only,x.metered,x.keys_changing,
                    u.id IN (SELECT c.position FROM coven_sending AS c),u.stored,
                    u.holds IS NOT NULL,t.not_before,o.id,o.value,m.id,m.value,
                    u.original) AS phase
            FROM (SELECT r.syncing AS syncing,r.cooling_until AS cooling_until,
                    s.last_pass AS last_pass,z.paused IS NOT NULL AS paused,
                    n.unmetered_only AS unmetered_only,n.metered AS metered,
                    w.paused IS NOT NULL AS keys_changing
                FROM coven_sync_status AS s
                JOIN coven_retry_effect AS r
                JOIN coven_network AS n
                LEFT JOIN _coven_upload_pause AS z
                LEFT JOIN _coven_write_pause AS w ON w.paused=1
                LIMIT 1 OFFSET 0) AS x
            CROSS JOIN _coven_upload_files AS l
            JOIN _coven_file_uploads AS u ON u.id=l.upload
            LEFT JOIN _coven_retries AS t ON t.upload=u.id
            LEFT JOIN _coven_pending AS o
                ON o.work='upload' AND o.device=u.device AND o.object=u.object
            LEFT JOIN _coven_pending AS m ON m.move=u.move
            LIMIT -1 OFFSET 0) AS y;",
    // Whether the app paused uploads, as one row whether or not anything is
    // queued. The pause table's one row, if any, joins a constant one.
    "CREATE VIEW coven_upload_pause(paused) AS
        SELECT z.paused IS NOT NULL FROM (SELECT 1) AS one
        LEFT JOIN _coven_upload_pause AS z;",
    // The device's network as this process last heard of it, which no table
    // holds (`network_facts.rs`), and the setting that holds transfers on a
    // metered one, as one row (§17.9). The setting's one row, if any, joins
    // a constant one.
    "CREATE VIEW coven_network(usable,metered,unmetered_only) AS
        SELECT _coven_network_usable(),_coven_network_metered(),w.unmetered_only IS NOT NULL
        FROM (SELECT 1) AS one
        LEFT JOIN _coven_unmetered_only AS w;",
    // The queued files this process is sending now, by each upload in
    // flight, each found by its uploader and object: counting a window's or
    // an album's takes a lookup per file. CROSS JOIN keeps SQLite from
    // planning the few files innermost, behind a scan of the queue.
    "CREATE VIEW coven_sending(file,position) AS
        SELECT l.file,u.id
        FROM json_each(_coven_sending_objects()) AS j
        CROSS JOIN _coven_file_uploads AS u
            ON u.device=unhex(substr(j.value,1,16)) AND u.object=unhex(substr(j.value,17))
        CROSS JOIN _coven_upload_files AS l ON l.upload=u.id;",
    // The stored files this process is downloading now to keep whole, by
    // each download in flight, each found by its stored bytes.
    "CREATE VIEW coven_downloading(file) AS
        SELECT f.file
        FROM json_each(_coven_downloading_objects()) AS j
        CROSS JOIN _coven_stored_files AS f
            ON f.device=unhex(substr(j.value,1,16)) AND f.object=unhex(substr(j.value,17));",
    // Each namespace's cache on this device, in bytes (§17.8): its budget,
    // its chunks and headers (`cached`), which the budget counts, its
    // pinned whole copies (`pinned`), which it doesn't, and the partial
    // downloads that pins left in it (`pinning`), which the cache neither
    // counts nor evicts. Each namespace the app has declared in this store
    // has a row, and a namespace keeps its row when a later version of the
    // app no longer declares it.
    "CREATE VIEW coven_cache_bytes(namespace,budget,cached,pinned,pinning) AS
        SELECT b.namespace,b.budget,b.cached,b.pinned,coalesce(sum(p.size),0)
        FROM _coven_cache_budgets AS b
        LEFT JOIN _coven_cache_partials AS p ON p.namespace=b.namespace
        GROUP BY b.namespace;",
    // This device's invites that haven't ended, in the order they were made,
    // from the operations journal: each from the write that makes it, while
    // its share is recorded and its access granted, its request is awaited
    // or approved, until a write of its state ends it, approval, decline,
    // cancel or expiry alike.
    // `invite` is the journal entry, the invite's state as coven-sync encodes
    // it (`InviteWork`), which its `Invite` reads; `operation` names its
    // pending record in `coven_pending`. `stage` is `granting` until the
    // code is shown, then `waiting`, then `approving` once the admin
    // approved the request. `request_member` and `joiner_name` come from the
    // request a waiting or approving invite holds, which is encoded bytes:
    // the member it names, and the name a new person typed, NULL for a
    // recovery; both NULL while no request has arrived. `invite_id` is the
    // invite's id as canonical UUID text, the id its request path and
    // `Invite::id` carry.
    "CREATE VIEW coven_invites(invite,operation,stage,request_member,joiner_name,invite_id) AS
        SELECT o.data,o.id,
            CASE WHEN json_type(CAST(o.data AS TEXT),'$.state.Waiting') IS NOT NULL THEN 'waiting'
                WHEN json_type(CAST(o.data AS TEXT),'$.state.Approving') IS NOT NULL THEN 'approving'
                ELSE 'granting' END,
            _coven_request_member(_coven_json_bytes(
                coalesce(json_extract(CAST(o.data AS TEXT),'$.state.Waiting.request'),
                    json_extract(CAST(o.data AS TEXT),'$.state.Approving.request')))),
            _coven_request_joiner(_coven_json_bytes(
                coalesce(json_extract(CAST(o.data AS TEXT),'$.state.Waiting.request'),
                    json_extract(CAST(o.data AS TEXT),'$.state.Approving.request')))),
            json_extract(CAST(o.data AS TEXT),'$.id')
        FROM _coven_operations AS o
        WHERE o.kind='",
    crate::invite_kind!(),
    "'
            AND (json_extract(CAST(o.data AS TEXT),'$.state') IN ('Requested','Grant')
                OR json_type(CAST(o.data AS TEXT),'$.state.Sharing') IS NOT NULL
                OR json_type(CAST(o.data AS TEXT),'$.state.Waiting') IS NOT NULL
                OR json_type(CAST(o.data AS TEXT),'$.state.Approving') IS NOT NULL);",
    // The operations the app's writes journaled, in the order they were
    // journaled, each until the app acknowledges its outcome: `kind` as
    // coven-sync names it, and `outcome` NULL while it runs, then how it
    // ended, encoded in the outcome type of its kind (`ListedOutcome`).
    // `outcome_kind` names the outcome's variant in snake case, as that type
    // names it, and is NULL while the operation runs; `account` is the
    // address a sharing refusal names, NULL for every other outcome.
    "CREATE VIEW coven_operations(operation,kind,outcome,outcome_kind,account) AS
        SELECT a.operation,a.kind,a.outcome,
            _coven_variant_kind(CAST(a.outcome AS TEXT)),
            json_extract(CAST(a.outcome AS TEXT),'$.SharingRefused.account')
        FROM _coven_app_operations AS a;",
    // Every member this device's applied slots name, former ones included,
    // with their public keys and how they reach storage: the account or the
    // S3 key id the stored access names, whichever it is.
    "CREATE VIEW coven_members(member,role,removed,is_self,signing,sealing,account,
            access_key_id) AS
        SELECT m.member,m.role,m.removed,d.device IS NOT NULL,m.signing,m.sealing,
            json_extract(CAST(m.access AS TEXT),'$.ProviderAccount'),
            json_extract(CAST(m.access AS TEXT),'$.S3AccessKey.access_key_id')
        FROM _coven_members AS m
        LEFT JOIN _coven_devices AS d ON d.member=m.member AND d.device=_coven_device();",
    "CREATE VIEW coven_devices(device,member,name,state,is_self) AS
        SELECT d.device,d.member,d.name,d.state,d.device=_coven_device()
        FROM _coven_devices AS d;",
    // The circles this device's member is in, as `Circles::list()` lists
    // them: a deleted circle isn't listed.
    "CREATE VIEW coven_circles(circle,name) AS
        SELECT c.circle,c.name
        FROM _coven_devices AS me
        JOIN _coven_circle_members AS m ON m.member=me.member
        JOIN _coven_circles AS c ON c.circle=m.circle
        LEFT JOIN _coven_circle_deletions AS x ON x.circle=c.circle
        WHERE me.device=_coven_device() AND x.circle IS NULL;",
    // Each circle's members who remain in the store, as
    // `Circles::members()` lists them: none for a deleted circle.
    "CREATE VIEW coven_circle_members(circle,member,is_self) AS
        SELECT m.circle,m.member,me.device IS NOT NULL
        FROM _coven_circle_members AS m
        JOIN _coven_members AS s ON s.member=m.member
        LEFT JOIN _coven_circle_deletions AS x ON x.circle=m.circle
        LEFT JOIN _coven_devices AS me ON me.member=m.member AND me.device=_coven_device()
        WHERE s.removed=0 AND x.circle IS NULL;",
);
