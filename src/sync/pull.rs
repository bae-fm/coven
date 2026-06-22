/// Pull changesets from the sync storage and apply them to the local database.
///
/// Protocol:
/// 1. List heads from the sync storage (one S3 LIST call).
/// 2. Compare each device's seq to our local `sync_cursors` table.
/// 3. For each device that's ahead of our cursor, fetch new changesets.
/// 4. Unpack envelope, check schema_version, apply with LWW.
/// 5. Update sync_cursors for that device.
///
/// After all changesets are applied, any that had FK constraint violations
/// are retried once -- the parent rows should now exist from other devices'
/// changesets applied in the same batch.
use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info, warn};

use super::apply::apply_changeset_lww_with_schema;
use super::conflict::TableSchema;
use super::envelope::{self, verify_changeset_signature};
use super::hlc::Timestamp;
use super::membership::{MemberRole, MembershipChain};
use super::push::SCHEMA_VERSION;
use super::session::SyncedTable;
use super::storage::{DeviceHead, SyncStorage};
use crate::blob::BlobPlan;
use crate::changeset::RowChange;
use crate::database::Database;
use crate::library_dir::LibraryDir;

/// Cursor value meaning "we have applied no changesets from this device".
/// Per the sync protocol, device sequence numbers start at 1 (the first
/// changeset a device produces is `local_seq + 1` where `local_seq` is
/// initially 0), so a cursor of 0 selects every changeset that device has
/// ever produced. A missing entry in the `sync_cursors` table is equivalent
/// to this initial value — the device is simply one we've never pulled from.
const INITIAL_CURSOR: u64 = 0;

/// Look up our applied seq for a remote device, returning the protocol's
/// initial cursor (0) and logging when we encounter the device for the first
/// time. The log line is the visible trace that distinguishes "never seen"
/// from "seen and at seq 0" (which is impossible — device seqs start at 1).
fn cursor_for_device(cursors: &HashMap<String, u64>, device_id: &str) -> u64 {
    match cursors.get(device_id) {
        Some(seq) => *seq,
        None => {
            debug!(%device_id, "no cursor for device, starting from initial");
            INITIAL_CURSOR
        }
    }
}

/// Whether `pubkey` is a current member of `chain` in any role (Owner, Member, or
/// the read-only Follower). The gate for honoring a device's *head*: a Follower
/// reads, so its head is legitimate even though it may not author changesets.
fn is_current_member(chain: &MembershipChain, pubkey: &str) -> bool {
    chain.current_members().iter().any(|(pk, _)| pk == pubkey)
}

/// Whether `pubkey` is a current *Owner* of `chain`. The gate for honoring a
/// `min_schema_version` floor: only an owner may raise the fleet's required
/// schema, so a Member- or Follower-signed floor is ignored.
fn is_current_owner(chain: &MembershipChain, pubkey: &str) -> bool {
    chain
        .current_members()
        .iter()
        .any(|(pk, role)| pk == pubkey && *role == MemberRole::Owner)
}

/// Summary of a pull operation.
#[derive(Debug)]
pub struct PullResult {
    /// Total changesets successfully applied.
    pub changesets_applied: u64,
    /// Number of distinct remote devices we pulled from.
    pub devices_pulled: u64,
    /// Asset downloads failed — cursor not advanced, will retry next cycle.
    pub asset_downloads_failed: bool,
    /// Changesets skipped due to schema version being newer than ours.
    pub skipped_schema: u64,
    /// All device heads fetched during this pull (including our own).
    /// Used by the sync status UI to show other devices' activity.
    pub remote_heads: Vec<DeviceHead>,
    /// Row changes from all applied changesets, for the host to map to domain
    /// events. Empty if nothing was applied.
    pub row_changes: Vec<RowChange>,
    /// The greatest `_updated_at` among all applied rows, parsed as an HLC
    /// [`Timestamp`]. The caller advances the HLC past this so a subsequent
    /// local write sorts causally after everything just pulled. `None` if
    /// nothing applied (or no applied row carried a parseable `_updated_at`).
    pub max_applied_updated_at: Option<Timestamp>,
}

/// A changeset that had FK violations on first apply and needs retry.
struct DeferredChangeset {
    device_id: String,
    seq: u64,
    changeset: Vec<u8>,
}

/// Pull and apply all new changesets from the sync storage.
///
/// `db` is the owned connection handle; all apply and schema reads run through
/// it on the connection thread. The caller MUST have suspended the capture
/// session before calling — the protocol requires ending capture before pulling
/// so the applied rows are not re-recorded into the next outgoing changeset.
///
/// `tables` is the synced set [`Database::open`] owns — the host's declared
/// tables plus coven's injected `item_keys` (for the `_updated_at` index map and
/// apply conflict resolution); call sites pass `db.synced_tables()`. `cursors`
/// maps device_id -> last_seq we've applied from that device.
///
/// Returns the updated cursors map and a summary of what was applied.
#[allow(clippy::too_many_arguments)]
pub async fn pull_changes(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    our_device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &LibraryDir,
    blob_plan: &dyn BlobPlan,
) -> Result<(HashMap<String, u64>, PullResult), PullError> {
    let _ = library_dir;
    // The library's established owner, pinned at create/join/restore (issue #102).
    // `Some` for an opaque (encrypted) library — its membership chain is anchored
    // to this pubkey; `None` for a browsable library, which has no chain and is
    // open by design.
    let owner_pubkey = db
        .get_sync_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|e| PullError::Apply(format!("read pinned owner: {e}")))?;

    // Load the current membership chain to validate changeset authorship, anchored
    // to the pinned owner. For an owner-pinned (opaque) library a chain whose
    // founder isn't that owner — or a chain that has been wiped entirely — is a
    // takeover attempt (#95/#104), so we refuse the cycle rather than trust it. A
    // browsable library has no owner pinned and legitimately has no chain.
    let membership_chain: Option<MembershipChain> = match storage.list_membership_entries().await {
        Ok(entries) if !entries.is_empty() => {
            match super::membership_ops::download_chain(storage, &entries).await {
                Ok(chain) => {
                    if let Some(owner) = &owner_pubkey {
                        if !chain.is_founded_by(owner) {
                            return Err(PullError::MembershipTampered(format!(
                                "chain founder {:?} is not the pinned owner {owner}",
                                chain.founder_pubkey()
                            )));
                        }
                    }
                    Some(chain)
                }
                Err(e) => {
                    // The listing is non-empty but the chain won't load/validate —
                    // malformed or corrupt. For an owner-pinned library this is a
                    // refound/tamper: fail closed (refuse the cycle) rather than fall
                    // open to "no chain, accept everything". Browsable (no owner) has
                    // no chain to validate, so it stays open.
                    if let Some(owner) = &owner_pubkey {
                        return Err(PullError::MembershipTampered(format!(
                            "membership chain failed to load/validate for owner-pinned \
                             library (owner {owner}): {e}"
                        )));
                    }
                    warn!("failed to load membership chain for validation: {e}");
                    None
                }
            }
        }
        Ok(_) => {
            if let Some(owner) = &owner_pubkey {
                return Err(PullError::MembershipTampered(format!(
                    "membership chain is empty but owner {owner} is pinned (wiped membership/*)"
                )));
            }
            None
        }
        Err(e) => {
            // Can't even list membership. For an owner-pinned library we cannot
            // verify authorship, so fail closed (abort, retry next cycle) rather
            // than apply changesets unvalidated. Browsable stays open.
            if owner_pubkey.is_some() {
                return Err(PullError::Storage(e));
            }
            warn!("failed to list membership entries for validation: {e}");
            None
        }
    };

    // Check min_schema_version before processing any changesets. The floor is an
    // untrusted control object, so it is honored only when its verified author is
    // a current *owner*: a non-owner-signed (or, with a chain present, unsigned)
    // floor is a freeze/downgrade attempt and is ignored. `get_min_schema_version`
    // already verified the signature and surfaced the author; this is the
    // authorization half. With no chain (a browsable library) we keep the prior
    // behavior and honor any floor present.
    if let Some(min) = storage
        .get_min_schema_version()
        .await
        .map_err(PullError::Storage)?
    {
        let honor = match membership_chain.as_ref() {
            Some(chain) => is_current_owner(chain, &min.author_pubkey),
            None => true,
        };
        if honor {
            if SCHEMA_VERSION < min.version {
                return Err(PullError::SchemaVersionTooOld {
                    local_version: SCHEMA_VERSION,
                    min_version: min.version,
                });
            }
        } else {
            warn!(
                author = ?min.author_pubkey,
                version = min.version,
                "ignoring min_schema_version not signed by a current owner"
            );
        }
    }

    // List heads, then drop any the membership chain doesn't authorize. A head is
    // a signed control object (`list_heads` verified its signature is valid for the
    // slot and surfaced the author); when a chain is present, a head whose author
    // is not a current member is a forgery that would otherwise pollute sync status
    // and drive a per-seq fetch loop, so it is skipped here. Our own head goes
    // through the same gate — its author is this device, a current member, so it is
    // kept (and dropped if this device was removed, which is correct). With no chain
    // (browsable) every head is kept, as before.
    let heads = storage.list_heads().await.map_err(PullError::Storage)?;
    let heads: Vec<DeviceHead> = heads
        .into_iter()
        .filter(|head| match membership_chain.as_ref() {
            Some(chain) => {
                let authorized = is_current_member(chain, &head.author_pubkey);
                if !authorized {
                    warn!(
                        device_id = %head.device_id,
                        author = %head.author_pubkey,
                        "skipping head not authored by a current member"
                    );
                }
                authorized
            }
            None => true,
        })
        .collect();

    // Per-table `_updated_at` column index map. Built once from the live schema
    // (so column additions stay safe) and shared by both the apply — every
    // changeset in this pull reuses it instead of re-querying `PRAGMA table_info`
    // — and the HLC advance over applied rows. `Arc` so it moves into each apply's
    // `'static` conflict closure without re-deriving it.
    let schema: Arc<TableSchema> = {
        let tables = tables.to_vec();
        Arc::new(
            db.call(move |conn| {
                let table_refs: Vec<&str> = tables.iter().map(|t| t.name()).collect();
                TableSchema::from_db(conn, &table_refs)
            })
            .await
            .map_err(|e| PullError::Apply(e.0))?,
        )
    };

    let mut updated_cursors = cursors.clone();
    let mut result = PullResult {
        changesets_applied: 0,
        devices_pulled: 0,
        asset_downloads_failed: false,
        skipped_schema: 0,
        remote_heads: heads.clone(),
        row_changes: Vec::new(),
        max_applied_updated_at: None,
    };
    let mut deferred: Vec<DeferredChangeset> = Vec::new();

    for head in &heads {
        // Skip our own device.
        if head.device_id == our_device_id {
            continue;
        }

        let local_seq = cursor_for_device(cursors, &head.device_id);
        if head.seq <= local_seq {
            continue;
        }

        info!(
            device_id = %head.device_id,
            local_seq,
            remote_seq = head.seq,
            "pulling changesets"
        );

        let mut pulled_any = false;

        for seq in (local_seq + 1)..=head.seq {
            // The storage client returns already-decrypted bytes per its trait
            // contract. Implementations handle download + decryption internally.
            let envelope_bytes = match storage.get_changeset(&head.device_id, seq).await {
                Ok(data) => data,
                Err(e) => {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        error = %e,
                        "failed to fetch changeset, stopping pull for this device"
                    );
                    break;
                }
            };

            let (env, changeset_bytes) =
                envelope::unpack(&envelope_bytes).map_err(PullError::InvalidEnvelope)?;

            // Validate that changeset_size in the envelope matches the actual
            // bytes. A mismatch indicates corruption or a buggy encoder.
            if env.changeset_size != changeset_bytes.len() {
                warn!(
                    device_id = %head.device_id,
                    seq,
                    expected = env.changeset_size,
                    actual = changeset_bytes.len(),
                    "changeset_size mismatch in envelope"
                );
            }

            // Schema version check: skip changesets from a newer schema.
            if env.schema_version > SCHEMA_VERSION {
                warn!(
                    device_id = %head.device_id,
                    seq,
                    remote_version = env.schema_version,
                    local_version = SCHEMA_VERSION,
                    "skipping changeset with newer schema version"
                );
                result.skipped_schema += 1;
                // Do NOT advance the cursor. This changeset is genuine and
                // becomes applicable once the local app updates past this schema;
                // advancing past it would strand its rows forever, because an
                // already-running device never re-bootstraps from a snapshot in a
                // normal cycle. Stop pulling this device here — every later seq it
                // produced is at least this schema version too, so nothing after
                // it could apply anyway, and leaving the cursor put makes the next
                // cycle re-fetch from this seq once we've upgraded.
                break;
            }

            // Signature check: reject changesets with invalid signatures.
            if !verify_changeset_signature(&env, &changeset_bytes) {
                warn!(
                    device_id = %head.device_id,
                    seq,
                    "changeset has invalid signature, skipping"
                );
                updated_cursors.insert(head.device_id.clone(), seq);
                continue;
            }

            // Membership validation: in a chain-enabled library every changeset
            // must be signed (its signature is verified above) by a current
            // write-capable member. Coven always signs at creation, so an
            // unsigned or non-member changeset here is forged -- reject it.
            //
            // The check is non-temporal: it asks whether the author is an Owner
            // or Member *now*, not at some envelope-embedded timestamp. That
            // timestamp is author-signed and so spoofable; revocation is
            // enforced by the key rotation that `remove_member` performs (a
            // removed member cannot produce changesets the chain admits, because
            // they lack the rotated library key and their auth key file).
            if let Some(chain) = membership_chain.as_ref() {
                // A Follower is a read-only member, so a changeset it authored
                // is rejected here even though it is registered — the logical
                // half of read-only enforcement (the proxy gates Follower
                // writes too).
                let authorized = env
                    .author_pubkey
                    .as_ref()
                    .is_some_and(|pk| chain.can_write_now(pk));
                if !authorized {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        author = ?env.author_pubkey,
                        "changeset not authored by a write-capable member, skipping"
                    );
                    updated_cursors.insert(head.device_id.clone(), seq);
                    continue;
                }
            }

            if changeset_bytes.is_empty() {
                updated_cursors.insert(head.device_id.clone(), seq);
                continue;
            }

            // Walk the changeset BEFORE applying it: the walk both drives blob
            // downloads and is surfaced to the host for domain-event mapping, and a
            // row must never be applied before its blobs are durable on disk
            // (#111). A walk failure means we cannot know which blobs the changeset
            // needs, so we cannot safely apply it -- skip it without applying or
            // advancing the cursor (it surfaces, and retries next cycle). This is
            // the bug the old "walk -> Vec::new()" substitution hid: it applied the
            // rows and silently dropped their blobs.
            let changes = match crate::changeset::walk(&changeset_bytes) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        "failed to walk changeset, skipping without applying: {e}"
                    );
                    result.asset_downloads_failed = true;
                    break;
                }
            };

            // The item content keys this changeset itself mints, recovered from its
            // `item_keys` rows BEFORE applying, so an item-scoped blob's key is
            // available without the changeset being applied first. Keys minted by
            // earlier (already-applied) changesets are not here -- `download_blobs`
            // falls back to the DB for those.
            let in_changeset_keys = match item_keys_in_changeset(&changeset_bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        "failed to read item_keys from changeset, skipping without applying: {e}"
                    );
                    result.asset_downloads_failed = true;
                    break;
                }
            };

            // Download + fsync every referenced blob BEFORE applying any row. If
            // any fails, skip the whole changeset -- nothing applied, cursor not
            // advanced -- and stop this device's pull so a later seq's success
            // can't carry the cursor past the failed seq (its blobs would then
            // never be re-fetched). The next cycle resumes at this seq.
            let blobs_ok =
                download_changeset_blobs(db, &changes, blob_plan, storage, &in_changeset_keys)
                    .await;
            if !blobs_ok {
                warn!(
                    "Blob download failed for {}/{}, not applying; cursor not advanced",
                    head.device_id, seq
                );
                result.asset_downloads_failed = true;
                break;
            }

            // Every referenced blob is now durable on disk: apply the changeset.
            let apply_result = {
                let schema = schema.clone();
                let bytes = changeset_bytes.clone();
                db.call(move |conn| apply_changeset_lww_with_schema(conn, &bytes, schema))
                    .await
                    .map_err(|e| PullError::Apply(e.0))?
            };

            if apply_result.had_fk_violations {
                deferred.push(DeferredChangeset {
                    device_id: head.device_id.clone(),
                    seq,
                    changeset: changeset_bytes.clone(),
                });
            }

            advance_max_updated_at(&mut result.max_applied_updated_at, &changes, &schema);

            result.changesets_applied += 1;
            result.row_changes.extend(changes);

            pulled_any = true;
            updated_cursors.insert(head.device_id.clone(), seq);
        }

        if pulled_any {
            result.devices_pulled += 1;
        }
    }

    // Retry changesets that had FK constraint violations. After applying all
    // changesets from all devices, the parent rows should now exist.
    if !deferred.is_empty() {
        info!(
            count = deferred.len(),
            "retrying changesets with FK violations"
        );

        for d in &deferred {
            let retry_result = {
                let schema = schema.clone();
                let bytes = d.changeset.clone();
                db.call(move |conn| apply_changeset_lww_with_schema(conn, &bytes, schema))
                    .await
                    .map_err(|e| PullError::Apply(e.0))?
            };

            if retry_result.had_fk_violations {
                warn!(
                    device_id = %d.device_id,
                    seq = d.seq,
                    "changeset still has FK violations after retry, skipping"
                );
            }
        }
    }

    Ok((updated_cursors, result))
}

/// Advance `max` past the greatest `_updated_at` among `changes`, parsing each
/// as an HLC [`Timestamp`]. A row whose `_updated_at` fails to parse is logged
/// and skipped — it must not panic the pull or silently default the clock.
fn advance_max_updated_at(
    max: &mut Option<Timestamp>,
    changes: &[RowChange],
    schema: &TableSchema,
) {
    for change in changes {
        let Some(cols) = schema.get(&change.table) else {
            // A table not in this device's synced set (a newer peer's schema): the
            // apply omitted its rows, so there is no applied `_updated_at` here to
            // advance the clock past.
            debug!(
                table = %change.table,
                "applied changeset references a table absent from the synced set, not advancing HLC"
            );
            continue;
        };
        let idx = cols.updated_at;
        let Some(raw) = change.col(idx) else {
            // A DELETE carries no new-state columns, and an absent value at the
            // schema's `_updated_at` index means this row change has no stamp to
            // advance past — expected for deletes, but a genuinely wrong index
            // or a schema mismatch surfaces here as the same absence, so log it.
            debug!(
                table = %change.table,
                updated_at_idx = idx,
                "applied row change has no _updated_at value (DELETE or absent new-state column), not advancing HLC past it"
            );
            continue;
        };
        match Timestamp::parse(raw) {
            Some(ts) => {
                if max.as_ref().is_none_or(|cur| ts > *cur) {
                    *max = Some(ts);
                }
            }
            None => warn!(
                table = %change.table,
                value = raw,
                "applied row has an unparseable _updated_at, not advancing HLC past it"
            ),
        }
    }
}

/// Build the `item_id -> 32-byte content key` map an incoming changeset mints in
/// its own `item_keys` rows, recovered BEFORE the changeset is applied so an
/// item-scoped blob can be downloaded ahead of the apply (issue #111). A key
/// column that isn't exactly 32 bytes is bad data: it is logged and dropped, so
/// the blob falls back to the DB key — and fails the download if none exists,
/// which skips the changeset rather than encrypting under a wrong key.
fn item_keys_in_changeset(changeset_bytes: &[u8]) -> Result<HashMap<String, [u8; 32]>, String> {
    let mut map = HashMap::new();
    for (item_id, key) in
        crate::changeset::insert_text_blob_pairs(changeset_bytes, "item_keys", 0, 1)?
    {
        match <[u8; 32]>::try_from(key.as_slice()) {
            Ok(k) => {
                map.insert(item_id, k);
            }
            Err(_) => warn!(
                item_id = %item_id,
                len = key.len(),
                "item_keys row has a non-32-byte key, ignoring"
            ),
        }
    }
    Ok(map)
}

/// Download blobs a changeset references. Returns true if all succeeded.
/// The host's [`BlobPlan`] decides which row-changes carry blobs, their cloud
/// namespace/scope, and the local destination path; the per-blob download runs
/// through [`download_blobs`].
///
/// `in_changeset_keys` are the item keys this changeset itself mints, so an
/// `Item(id)`-scoped blob resolves its key WITHOUT the changeset having been
/// applied first (issue #111) — the download precedes the apply.
async fn download_changeset_blobs(
    db: &Database,
    changes: &[RowChange],
    blob_plan: &dyn BlobPlan,
    storage: &dyn SyncStorage,
    in_changeset_keys: &HashMap<String, [u8; 32]>,
) -> bool {
    download_blobs(
        db,
        blob_plan.blobs_to_pull(changes),
        storage,
        in_changeset_keys,
    )
    .await
}

/// Resolve a blob's public scope to its internal key scope WITHOUT requiring the
/// minting changeset to be applied first: an `Item(id)` scope takes its key from
/// `in_changeset_keys` (keys this changeset mints) when present, else from the DB
/// (keys minted by earlier, already-applied changesets, or carried by a
/// snapshot). `Master`/`Derived` need no key lookup. This is what lets the pull
/// download an item-scoped blob before applying the changeset (issue #111).
async fn resolve_pull_scope(
    scope: crate::blob::BlobScope,
    in_changeset_keys: &HashMap<String, [u8; 32]>,
    db: &Database,
) -> Result<crate::blob::ResolvedScope, crate::database::DbError> {
    use crate::blob::{BlobScope, ResolvedScope};
    match scope {
        BlobScope::Item(item_id) => match in_changeset_keys.get(&item_id) {
            Some(key) => Ok(ResolvedScope::Key(*key)),
            None => db.resolve_blob_scope(BlobScope::Item(item_id)).await,
        },
        other => db.resolve_blob_scope(other).await,
    }
}

/// Download each blob in `blobs` to its `local_path`, decrypting via storage
/// (which returns plaintext) and writing the bytes. Returns true if every blob
/// succeeded. Skips blobs whose local file already exists.
///
/// Each blob's public scope is resolved to the internal key scope here — not in
/// [`BlobPlan`], which has no DB — via [`resolve_pull_scope`], which consults
/// `in_changeset_keys` (item keys the current changeset mints) before the DB so
/// resolution does not depend on the changeset being applied. A blob whose item
/// key is missing is a failed download (logged, `all_ok = false`) so a caller
/// that gates on the result can retry once the key row has landed.
///
/// Shared by the incremental pull (per applied changeset) and the snapshot
/// bootstrap backfill (per row in the freshly bootstrapped DB, where
/// `in_changeset_keys` is empty and keys come from the DB the snapshot carried),
/// so the download/decrypt/write path lives in one place.
pub(crate) async fn download_blobs(
    db: &Database,
    blobs: Vec<crate::blob::BlobRef>,
    storage: &dyn SyncStorage,
    in_changeset_keys: &HashMap<String, [u8; 32]>,
) -> bool {
    let mut all_ok = true;
    for blob in blobs {
        match crate::local_blob::exists(&blob.local_path).await {
            // Already on disk — don't re-download.
            Ok(true) => continue,
            Ok(false) => {}
            // Couldn't tell (a real storage failure): fall through and re-download,
            // which overwrites idempotently. Logged so the failure isn't silent.
            Err(e) => {
                warn!(id = %blob.id, error = %e, "cannot check for local blob; attempting download");
            }
        }

        let resolved = match resolve_pull_scope(blob.scope.clone(), in_changeset_keys, db).await {
            Ok(r) => r,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot resolve blob scope, skipping download");
                all_ok = false;
                continue;
            }
        };

        match storage
            .get_blob(
                &blob.namespace,
                &blob.id,
                resolved,
                blob.cloud_path.as_deref(),
            )
            .await
        {
            Ok(bytes) => {
                // `local_blob::write` creates the parent directories itself, so the
                // pull path stays the same on native (filesystem) and wasm (OPFS).
                if let Err(e) = crate::local_blob::write(&blob.local_path, &bytes).await {
                    warn!(id = %blob.id, error = %e, "failed to write blob");
                    all_ok = false;
                }
            }
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "failed to download blob");
                all_ok = false;
            }
        }
    }
    all_ok
}

#[derive(Debug)]
pub enum PullError {
    Storage(super::storage::StorageError),
    InvalidEnvelope(super::envelope::UnpackError),
    Apply(String),
    /// The sync storage requires a schema version newer than ours.
    /// The client must upgrade before syncing.
    SchemaVersionTooOld {
        local_version: u32,
        min_version: u32,
    },
    /// The membership chain is not anchored to the library's pinned owner — it was
    /// wiped and/or refounded under a different key (an owner-takeover attempt,
    /// issue #95). The cycle is refused rather than trusting the tampered chain.
    MembershipTampered(String),
}

impl std::fmt::Display for PullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PullError::Storage(e) => write!(f, "storage error: {e}"),
            PullError::InvalidEnvelope(e) => write!(f, "invalid changeset envelope: {e}"),
            PullError::Apply(e) => write!(f, "changeset apply failed: {e}"),
            PullError::SchemaVersionTooOld {
                local_version,
                min_version,
            } => write!(
                f,
                "Update the app to keep syncing — this library was upgraded by a newer device (schema v{min_version}; you have v{local_version})."
            ),
            PullError::MembershipTampered(e) => write!(f, "membership chain tampered: {e}"),
        }
    }
}

impl std::error::Error for PullError {}
