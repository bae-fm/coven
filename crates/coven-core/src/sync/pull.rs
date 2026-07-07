/// Pull changesets from the sync storage and apply them to the local database.
///
/// Protocol:
/// 1. List heads from the sync storage (one S3 LIST call).
/// 2. Compare each device's seq to our local `sync_cursors` table.
/// 3. For each device that's ahead of our cursor, fetch new changesets.
/// 4. Unpack envelope, check schema_version, apply with LWW.
/// 5. Update sync_cursors for that device.
///
/// After all changesets are applied, any that had FK violations are retried once
/// -- the parent rows should now exist from other devices'
/// changesets applied in the same batch.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use super::apply::apply_changeset_lww_with_schema;
use super::conflict::TableSchema;
use super::envelope::{self, verify_changeset_signature};
use super::hlc::Timestamp;
use super::membership::{MemberRole, MembershipChain, MembershipCoord, MembershipEntry};
use super::session::SyncedTable;
use super::storage::{DeviceHead, SyncStorage};
use crate::blob::decl::BlobDecls;
use crate::blob::{CacheFill, Provenance};
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

/// Whether `pubkey` is a current member in any role (Owner, Member, or the
/// read-only Follower). The gate for honoring a device's *head*: a Follower
/// reads, so its head is legitimate even though it may not author changesets.
fn is_current_member(members: &[(String, MemberRole)], pubkey: &str) -> bool {
    members.iter().any(|(pk, _)| pk == pubkey)
}

fn is_current_owner(members: &[(String, MemberRole)], pubkey: &str) -> bool {
    members
        .iter()
        .any(|(pk, role)| pk == pubkey && *role == MemberRole::Owner)
}

fn can_write_now(members: &[(String, MemberRole)], pubkey: &str) -> bool {
    members
        .iter()
        .any(|(pk, role)| pk == pubkey && role.can_write())
}

/// A changeset skipped because its author is not a write-capable member, judged
/// against the exact membership entry the changeset is signed under (so it is
/// forged or revoked, not merely a propagation lag). Surfaced so the host can
/// warn the user, the way `skipped_schema` surfaces a newer-version skip.
#[derive(Debug, Clone)]
pub struct RejectedUnauthorized {
    pub device_id: String,
    pub seq: u64,
    /// Hex-encoded author pubkey, or `None` for an unsigned changeset in a chain
    /// library (which nothing can authorize).
    pub author: Option<String>,
}

/// A changeset rejected because its signature did not verify — forged or corrupt,
/// not a propagation lag. The cursor is held at this seq so the bad object stalls
/// only its device stream instead of suppressing data permanently. The author
/// claim is unverifiable (the signature is invalid), so only the object's
/// location identifies it. Surfaced so the host can warn the user, the way
/// `RejectedUnauthorized` does.
#[derive(Debug, Clone)]
pub struct InvalidSignature {
    pub device_id: String,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldChangesetReason {
    /// The object decrypted but its bytes are not a valid envelope (no null
    /// separator, or the JSON metadata did not parse). Present-but-invalid cloud
    /// data, held like a size mismatch so one bad object stalls only its own
    /// stream instead of failing the whole pull.
    MalformedEnvelope {
        error: String,
    },
    SizeMismatch {
        expected: usize,
        actual: usize,
    },
    ApplyFailed {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldChangeset {
    pub device_id: String,
    pub seq: u64,
    pub reason: HeldChangesetReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintConflict {
    pub device_id: String,
    pub seq: u64,
    pub table: String,
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
    /// Changesets skipped because their author is not a write-capable member,
    /// judged against the exact membership entry the changeset is signed under
    /// (forged or revoked). The cursor advanced past each so the client is not
    /// stuck; surfaced so the host can warn. Per-cycle, like `skipped_schema`.
    pub rejected_unauthorized: Vec<RejectedUnauthorized>,
    /// Changesets whose signature did not verify (forged or corrupt). The cursor
    /// is held and this device stream stops at the bad seq; surfaced so the host
    /// can warn. Per-cycle, like `skipped_schema`.
    pub invalid_signatures: Vec<InvalidSignature>,
    /// Changesets whose envelope or apply step failed after the object was present
    /// and readable. The cursor is held and this device stream stops at the bad
    /// seq; other device streams continue.
    pub held_changesets: Vec<HeldChangeset>,
    /// Changesets that hit a non-retryable SQLite constraint conflict while
    /// applying. The conflicting row is omitted and the cursor advances; surfaced
    /// so the host can warn about the unresolved uniqueness/constraint clash.
    pub constraint_conflicts: Vec<ConstraintConflict>,
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
    old_changes: Vec<RowChange>,
    changes: Vec<RowChange>,
}

struct CompletedChangeset<'a> {
    device_id: &'a str,
    seq: u64,
    old_changes: &'a [RowChange],
    changes: &'a [RowChange],
}

/// Pull and apply all new changesets from the sync storage.
///
/// `db` is the owned connection handle; all apply and schema reads run through
/// it. The apply of incoming rows goes through
/// [`Database::apply_changeset`], which disables capture around just that apply so
/// the applied rows are not re-recorded into the next outgoing changeset — the
/// caller leaves capture ENABLED, so a host write landing during this pull's
/// network phases is still captured. Every other `db.call` here (schema read,
/// blob-scope resolution, cursor bookkeeping) runs on the normal enabled path.
///
/// `tables` is the synced set coven owns — the host's declared tables plus
/// coven's injected `item_keys` (for the `_updated_at` index map and apply
/// conflict resolution); call sites pass `db.synced_tables()`. `cursors` maps
/// device_id -> last_seq we've applied from that device.
///
/// Returns the updated cursors map and a summary of what was applied.
pub async fn pull_changes(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    our_device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &LibraryDir,
) -> Result<(HashMap<String, u64>, PullResult), PullError> {
    // The opened database handle already resolved blob declarations from the final
    // synced set + live schema. Pull reuses that model for download-before-apply of
    // CacheEager blobs and apply-side cache drops for deleted blob-bearing rows.
    let blob_decls = db.blob_decls();
    // The receiver's current wall-clock millis, read once from the register clock
    // and passed down to bound an incoming `_updated_at`'s physical component. A
    // stamp grossly beyond this (plus a generous offline allowance) is a broken
    // clock or buggy client: it must not win last-writer-wins (the apply's conflict
    // handler refuses it) nor ratchet the local clock (`advance_max_updated_at`
    // skips it). Read once here, not sampled per row, so the bound is stable across
    // the whole pull.
    let receiver_wall_ms = db.receive_wall_ms();
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
    let mut membership_chain: Option<MembershipChain> =
        match storage.list_membership_entries().await {
            Ok(entries) if !entries.is_empty() => {
                // Load + validate the chain and anchor it to the pinned owner (the
                // same load+anchor the snapshot authorize runs). For an owner-pinned
                // library a chain that won't validate, or one founded by a different
                // key, is a refound/tamper: fail closed (refuse the cycle) rather than
                // fall open to "no chain, accept everything". Browsable (no owner) has
                // nothing to anchor, so a load failure there just leaves it open.
                match super::membership_ops::load_anchored_chain(
                    storage,
                    &entries,
                    owner_pubkey.as_deref(),
                )
                .await
                {
                    Ok(chain) => Some(chain),
                    Err(e) => {
                        if owner_pubkey.is_some() {
                            return Err(PullError::MembershipTampered(e.to_string()));
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
    let mut membership_members = membership_chain
        .as_ref()
        .map(MembershipChain::current_members);

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
        let honor = match membership_members.as_ref() {
            Some(members) => is_current_owner(members, &min.author_pubkey),
            None => true,
        };
        if honor {
            if db.schema_version() < min.version {
                return Err(PullError::SchemaVersionTooOld {
                    local_version: db.schema_version(),
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
        .filter(|head| match membership_members.as_ref() {
            Some(members) => {
                let authorized = is_current_member(members, &head.author_pubkey);
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
        rejected_unauthorized: Vec::new(),
        invalid_signatures: Vec::new(),
        held_changesets: Vec::new(),
        constraint_conflicts: Vec::new(),
        remote_heads: heads.clone(),
        row_changes: Vec::new(),
        max_applied_updated_at: None,
    };
    let mut deferred: Vec<DeferredChangeset> = Vec::new();
    let mut applied_devices: HashSet<String> = HashSet::new();

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

            // A present but unparseable envelope is bad cloud data, not a reason to
            // fail the whole pull. Hold this device's cursor and surface it, the same
            // as a size mismatch or apply failure; other device streams continue.
            let (env, changeset_bytes) = match envelope::unpack(&envelope_bytes) {
                Ok(unpacked) => unpacked,
                Err(e) => {
                    error!(
                        device_id = %head.device_id,
                        seq,
                        error = %e,
                        "changeset envelope is malformed; holding cursor for this device"
                    );
                    result.held_changesets.push(HeldChangeset {
                        device_id: head.device_id.clone(),
                        seq,
                        reason: HeldChangesetReason::MalformedEnvelope {
                            error: e.to_string(),
                        },
                    });
                    break;
                }
            };

            // Schema version check: skip changesets from a newer schema.
            if env.schema_version > db.schema_version() {
                warn!(
                    device_id = %head.device_id,
                    seq,
                    remote_version = env.schema_version,
                    local_version = db.schema_version(),
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

            // Signature check: reject changesets with invalid signatures. A bad
            // signature is forged or corrupt; hold the cursor and stop this device
            // stream at the bad seq so it surfaces as a bounded stall instead of
            // silently suppressing the seq's data.
            if !verify_changeset_signature(&env, &changeset_bytes) {
                error!(
                    device_id = %head.device_id,
                    seq,
                    "changeset has invalid signature; holding cursor for this device"
                );
                result.invalid_signatures.push(InvalidSignature {
                    device_id: head.device_id.clone(),
                    seq,
                });
                break;
            }

            // The envelope's declared changeset_size must match the trailing bytes.
            // A mismatch is present-but-invalid cloud data. Hold this device's
            // cursor and surface it; do not advance past bytes whose integrity check
            // failed.
            if env.changeset_size != changeset_bytes.len() {
                error!(
                    device_id = %head.device_id,
                    seq,
                    expected = env.changeset_size,
                    actual = changeset_bytes.len(),
                    "changeset_size mismatch in envelope; holding cursor for this device"
                );
                result.held_changesets.push(HeldChangeset {
                    device_id: head.device_id.clone(),
                    seq,
                    reason: HeldChangesetReason::SizeMismatch {
                        expected: env.changeset_size,
                        actual: changeset_bytes.len(),
                    },
                });
                break;
            }

            // Membership validation: in a chain-enabled library every changeset
            // must be signed (its signature is verified above) by a current
            // write-capable member. Coven always signs at creation, so an unsigned
            // or non-member changeset is forged. But an authorized member's
            // changeset can also legitimately arrive *before* the entry that
            // authorizes them is visible locally — membership entries and
            // changesets are separate, unordered object streams. Judging against
            // the cycle-start chain alone would skip such a changeset permanently.
            //
            // So when the cycle-start chain does not already authorize the author,
            // resolve the gap against the exact membership entry the changeset is
            // signed under (a keyed GET is strongly consistent) before deciding.
            // The check stays non-temporal: revocation is enforced by the key
            // rotation `remove_member` performs and by merging any Remove we hold,
            // not by an author-supplied timestamp. A Follower is registered but not
            // write-capable, so a changeset it authored is rejected here too.
            if membership_chain.is_some() {
                let author = env.author_pubkey.as_deref();
                let authorized = membership_members
                    .as_ref()
                    .is_some_and(|members| author.is_some_and(|pk| can_write_now(members, pk)));
                if !authorized {
                    match resolve_membership_authorization(
                        storage,
                        membership_chain.as_ref().unwrap(),
                        owner_pubkey.as_deref(),
                        author,
                        env.membership_grant.as_ref(),
                    )
                    .await
                    {
                        MembershipJudgment::Authorized(refreshed) => {
                            // We were behind: a refreshed chain (and, if needed, the
                            // named grant entry) authorizes the author. Adopt it for
                            // the rest of this cycle so other newly-authorized peers
                            // don't each re-trigger a reload.
                            membership_members = Some(refreshed.current_members());
                            membership_chain = Some(refreshed);
                        }
                        MembershipJudgment::Unauthorized => {
                            // We hold the exact entry the author claims authorizes
                            // them (or it provably does not exist) and it does not
                            // grant write — forged or revoked. Skip and advance so
                            // the client is not stuck, and surface it.
                            error!(
                                device_id = %head.device_id,
                                seq,
                                author = ?env.author_pubkey,
                                "changeset author is not a write-capable member \
                                 against the membership entry it is signed under; \
                                 skipping (forged or revoked)"
                            );
                            result.rejected_unauthorized.push(RejectedUnauthorized {
                                device_id: head.device_id.clone(),
                                seq,
                                author: env.author_pubkey.clone(),
                            });
                            updated_cursors.insert(head.device_id.clone(), seq);
                            continue;
                        }
                        MembershipJudgment::Indeterminate => {
                            // Membership isn't consistently readable yet, so we
                            // can't tell behind-vs-forged. Leave the cursor and stop
                            // this device's pull so the next cycle retries rather
                            // than skipping a possibly-legitimate changeset.
                            warn!(
                                device_id = %head.device_id,
                                seq,
                                "cannot yet authorize changeset author (membership \
                                 not readable); leaving cursor for retry"
                            );
                            break;
                        }
                    }
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
            let old_changes = match crate::changeset::walk_old(&changeset_bytes) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        "failed to walk old changeset values, skipping without applying: {e}"
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

            // Download + fsync every CacheEager blob BEFORE applying any row. If
            // any fails, skip the whole changeset -- nothing applied, cursor not
            // advanced -- and stop this device's pull so a later seq's success
            // can't carry the cursor past the failed seq (its blobs would then
            // never be re-fetched). The next cycle resumes at this seq. CacheLazy
            // blobs are not downloaded here — they are fetched on first read.
            let cache_eager = match cache_eager_blobs(&blob_decls, &old_changes, &changes) {
                Ok(blobs) => blobs,
                Err(e) => {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        "failed to scan blob declarations from changeset, skipping without applying: {e}"
                    );
                    result.asset_downloads_failed = true;
                    break;
                }
            };
            let blobs_ok =
                download_blobs(db, cache_eager, storage, library_dir, &in_changeset_keys).await;
            if !blobs_ok {
                warn!(
                    "Blob download failed for {}/{}, not applying; cursor not advanced",
                    head.device_id, seq
                );
                result.asset_downloads_failed = true;
                break;
            }

            // Every referenced blob is now durable on disk: apply the changeset.
            // Through `apply_changeset` so capture is disabled around just this
            // apply (the applied rows must not echo as this device's own writes),
            // while host writes outside it stay captured.
            let apply_attempt = {
                let schema = schema.clone();
                let bytes = changeset_bytes.clone();
                db.apply_changeset(move |conn| {
                    apply_changeset_lww_with_schema(conn, &bytes, schema, receiver_wall_ms)
                })
                .await
            };
            let apply_result = match apply_attempt {
                Ok(result) => result,
                Err(e) => {
                    error!(
                        device_id = %head.device_id,
                        seq,
                        error = %e.0,
                        "changeset apply failed; holding cursor for this device"
                    );
                    result.held_changesets.push(HeldChangeset {
                        device_id: head.device_id.clone(),
                        seq,
                        reason: HeldChangesetReason::ApplyFailed { error: e.0 },
                    });
                    break;
                }
            };

            if apply_result.had_fk_violations {
                deferred.push(DeferredChangeset {
                    device_id: head.device_id.clone(),
                    seq,
                    changeset: changeset_bytes.clone(),
                    old_changes,
                    changes,
                });
                break;
            }
            record_constraint_conflicts(
                &mut result,
                &head.device_id,
                seq,
                apply_result.constraint_conflict_tables,
            );

            if !finish_applied_changeset(
                &mut result,
                &mut updated_cursors,
                &mut applied_devices,
                CompletedChangeset {
                    device_id: &head.device_id,
                    seq,
                    old_changes: &old_changes,
                    changes: &changes,
                },
                &blob_decls,
                db,
                library_dir,
                &schema,
                receiver_wall_ms,
            )
            .await
            {
                break;
            }
        }
    }

    // Retry changesets that had FK violations. After applying all changesets
    // from all devices, the parent rows should now exist.
    if !deferred.is_empty() {
        info!(
            count = deferred.len(),
            "retrying changesets with FK violations"
        );

        for d in &deferred {
            let retry_attempt = {
                let schema = schema.clone();
                let bytes = d.changeset.clone();
                db.apply_changeset(move |conn| {
                    apply_changeset_lww_with_schema(conn, &bytes, schema, receiver_wall_ms)
                })
                .await
            };
            let retry_result = match retry_attempt {
                Ok(result) => result,
                Err(e) => {
                    error!(
                        device_id = %d.device_id,
                        seq = d.seq,
                        error = %e.0,
                        "deferred changeset apply failed; holding cursor for this device"
                    );
                    result.held_changesets.push(HeldChangeset {
                        device_id: d.device_id.clone(),
                        seq: d.seq,
                        reason: HeldChangesetReason::ApplyFailed { error: e.0 },
                    });
                    continue;
                }
            };

            if retry_result.had_fk_violations {
                warn!(
                    device_id = %d.device_id,
                    seq = d.seq,
                    "changeset still has FK violations after retry; cursor not advanced"
                );
                continue;
            }
            record_constraint_conflicts(
                &mut result,
                &d.device_id,
                d.seq,
                retry_result.constraint_conflict_tables,
            );
            if !finish_applied_changeset(
                &mut result,
                &mut updated_cursors,
                &mut applied_devices,
                CompletedChangeset {
                    device_id: &d.device_id,
                    seq: d.seq,
                    old_changes: &d.old_changes,
                    changes: &d.changes,
                },
                &blob_decls,
                db,
                library_dir,
                &schema,
                receiver_wall_ms,
            )
            .await
            {
                continue;
            }
        }
    }

    result.devices_pulled = applied_devices.len() as u64;

    Ok((updated_cursors, result))
}

async fn finish_applied_changeset(
    result: &mut PullResult,
    updated_cursors: &mut HashMap<String, u64>,
    applied_devices: &mut HashSet<String>,
    applied: CompletedChangeset<'_>,
    blob_decls: &BlobDecls,
    db: &Database,
    library_dir: &LibraryDir,
    schema: &TableSchema,
    receiver_wall_ms: u64,
) -> bool {
    // A changeset that DELETEs a blob-bearing row (a gate retract or a genuine
    // delete) leaves this peer holding the blob's local copy. Drop it wherever it
    // is: cache, pinned cache, and local store. A peer never writes a cloud
    // tombstone here; that belongs to the deleting / make-Local owner.
    if let Err(e) = drop_deleted_blob_local(
        applied.old_changes,
        applied.changes,
        blob_decls,
        db,
        library_dir,
    )
    .await
    {
        warn!(
            "Dropping local copies for deleted blob rows failed for {}/{}: {e}; \
             cursor not advanced, will retry next cycle",
            applied.device_id, applied.seq
        );
        result.asset_downloads_failed = true;
        return false;
    }

    advance_max_updated_at(
        &mut result.max_applied_updated_at,
        applied.changes,
        schema,
        receiver_wall_ms,
    );
    result.changesets_applied += 1;
    result.row_changes.extend(applied.changes.to_vec());
    applied_devices.insert(applied.device_id.to_string());
    updated_cursors.insert(applied.device_id.to_string(), applied.seq);
    true
}

fn record_constraint_conflicts(
    result: &mut PullResult,
    device_id: &str,
    seq: u64,
    tables: Vec<String>,
) {
    for table in tables {
        error!(
            device_id = %device_id,
            seq,
            table = %table,
            "changeset hit a non-retryable SQLite constraint conflict; row omitted"
        );
        result.constraint_conflicts.push(ConstraintConflict {
            device_id: device_id.to_string(),
            seq,
            table,
        });
    }
}

/// Outcome of deciding whether a changeset's author may write, when the
/// cycle-start membership chain did not already authorize them.
enum MembershipJudgment {
    /// Authorized against a refreshed chain (and, if needed, the exact grant entry
    /// the changeset names). Carries the chain to adopt for the rest of the cycle.
    Authorized(MembershipChain),
    /// Genuinely not authorized: forged, revoked, the wrong role, or a grant
    /// coordinate that does not exist. Skip and advance the cursor.
    Unauthorized,
    /// Can't be determined yet — membership storage is transiently unavailable or
    /// not consistently readable. Don't advance; retry next cycle.
    Indeterminate,
}

/// Decide whether `author` may write when the cycle-start chain didn't already
/// say so. The cycle-start chain is built from a LIST that can lag the entry that
/// authorizes a freshly-added member, so this reloads the chain, and if still
/// unauthorized, fetches the exact entry the changeset is signed under by
/// coordinate — a keyed GET is strongly consistent, so a `NotFound` is
/// authoritative — then re-judges against the merged chain.
///
/// `owner_pubkey` is the library's pinned owner (issue #102) when it is an opaque
/// (owner-anchored) library. Any chain this adopts or merges must still be founded
/// by that owner; a refreshed/merged chain that is not is a takeover attempt and
/// does not authorize anyone, so it is treated as unauthorized — never adopted.
async fn resolve_membership_authorization(
    storage: &dyn SyncStorage,
    current: &MembershipChain,
    owner_pubkey: Option<&str>,
    author: Option<&str>,
    grant: Option<&MembershipCoord>,
) -> MembershipJudgment {
    let Some(author) = author else {
        // Unsigned changeset in a chain library: nothing can authorize it.
        return MembershipJudgment::Unauthorized;
    };

    // Best-effort fresh reload to catch an authorizing entry the cycle-start LIST
    // lagged. This is ONLY an optimization to refresh the broader chain; nothing
    // here decides retry-vs-skip, so nothing here returns Indeterminate. On ANY
    // reload problem — a storage read failure, an unparseable/undecryptable entry,
    // or a whole-set validation failure (a member can plant a bogus, listable
    // entry) — fall back to the known-good cycle-start chain. The keyed grant GET
    // below is the sole authority: it fetches the one specific entry (bypassing a
    // poisoned whole set) and it alone separates a real I/O error (retry) from
    // forged content (skip). A genuine storage outage therefore still surfaces as
    // Indeterminate via that GET, while no bogus entry can stall the pull.
    let fresh = match reload_entries(storage).await {
        Some(entries) if entries.is_empty() => {
            // Append-only chain came back empty mid-cycle — an anomalous transient.
            warn!("membership re-list came back empty mid-cycle; keeping the cycle-start chain");
            current.clone()
        }
        Some(entries) => {
            let only_entries = entries.into_iter().map(|(_, e)| e).collect();
            build_anchored_chain(only_entries, owner_pubkey).unwrap_or_else(|| {
                warn!(
                    "reloaded membership chain failed validation or is not anchored to \
                     the pinned owner; falling back to the cycle-start chain"
                );
                current.clone()
            })
        }
        None => current.clone(),
    };
    if fresh.can_write_now(author) {
        return MembershipJudgment::Authorized(fresh);
    }

    // Still unauthorized against a fresh chain. Resolve via the coordinate the
    // changeset is signed under.
    let Some(coord) = grant else {
        // A chain-library changeset must carry its authorizing coordinate; its
        // absence means forged or pre-binding — genuinely unauthorized.
        return MembershipJudgment::Unauthorized;
    };
    let entry = match storage
        .get_membership_entry(&coord.author_pubkey, coord.seq)
        .await
    {
        Ok(bytes) => match serde_json::from_slice::<MembershipEntry>(&bytes) {
            Ok(entry) => entry,
            // The named object exists but isn't a parseable membership entry — a
            // bogus or corrupt claim. Log the cause and reject; not a reason to stall.
            Err(e) => {
                warn!(
                    grant_author = %coord.author_pubkey,
                    grant_seq = coord.seq,
                    error = %e,
                    "membership grant entry the changeset names did not parse; \
                     treating the changeset as unauthorized"
                );
                return MembershipJudgment::Unauthorized;
            }
        },
        // A keyed GET is strongly consistent: NotFound means the claimed grant
        // does not exist, so the changeset is forged.
        Err(super::storage::StorageError::NotFound(_)) => return MembershipJudgment::Unauthorized,
        // A transient storage error: we can't tell yet, so retry next cycle.
        Err(e) => {
            warn!(
                grant_author = %coord.author_pubkey,
                grant_seq = coord.seq,
                error = %e,
                "transient storage error fetching the named membership grant; \
                 leaving the cursor to retry next cycle"
            );
            return MembershipJudgment::Indeterminate;
        }
    };

    // Merge the named grant into the fresh chain and re-judge against the whole,
    // so a Remove we already hold still revokes write. The merged chain must STILL
    // be anchored to the pinned owner — a grant that, once merged, would re-found
    // the chain under a different key does not authorize anyone.
    let mut entries = fresh.entries().to_vec();
    entries.push(entry);
    match build_anchored_chain(entries, owner_pubkey) {
        Some(merged) if merged.can_write_now(author) => MembershipJudgment::Authorized(merged),
        Some(_) => MembershipJudgment::Unauthorized,
        // The fresh chain plus the fetched grant did not form a valid owner-anchored
        // chain: the named grant is invalid (bad signature, signed by someone who is
        // not a current owner, or it un-anchors the chain) and so does not establish
        // this author's write access. Treat it as unauthorized — skip and advance —
        // NEVER indeterminate. Indeterminate is reserved strictly for storage *read*
        // failures (handled above); routing a validation failure there would let a
        // member plant a parseable-but-invalid entry, name it as the grant, and stall
        // every other device's pull forever — the exact failure this fix prevents. The
        // rare cost is a genuinely-lagging intermediate entry (an owner who was
        // themselves only just added); those rows still reach this device through the
        // snapshot channel, so nothing is lost.
        None => {
            warn!(
                grant_author = %coord.author_pubkey,
                grant_seq = coord.seq,
                "membership chain did not validate (or lost its owner anchor) after \
                 merging the named grant; treating the changeset as unauthorized"
            );
            MembershipJudgment::Unauthorized
        }
    }
}

/// Build a validated chain from `entries` that is also anchored to `owner_pubkey`
/// when one is pinned (issue #102). Returns `None` if the entries don't form a
/// valid chain OR the resulting chain's founder isn't the pinned owner — either
/// way the chain must not be trusted to authorize a write. A library with no
/// pinned owner (browsable) skips the anchor check.
fn build_anchored_chain(
    entries: Vec<MembershipEntry>,
    owner_pubkey: Option<&str>,
) -> Option<MembershipChain> {
    let chain = match MembershipChain::from_entries(entries) {
        Ok(chain) => chain,
        Err(e) => {
            warn!("membership entries did not form a valid chain: {e}");
            return None;
        }
    };
    match owner_pubkey {
        Some(owner) if !chain.is_founded_by(owner) => None,
        _ => Some(chain),
    }
}

/// Re-list and re-download the membership entries for a best-effort chain refresh.
/// Returns `None` on ANY problem — a list/get I/O error OR an unparseable /
/// undecryptable entry — logging the cause; the caller then falls back to the
/// known-good cycle-start chain. Deliberately never signals "retry": a planted,
/// listable entry that can't be parsed is attacker-controllable content, and
/// folding it into a retry would let one such object stall every device's pull
/// forever. Whether a genuine I/O outage should retry is decided downstream by the
/// keyed grant GET, which is per-entry and can separate I/O from content.
async fn reload_entries(
    storage: &dyn SyncStorage,
) -> Option<Vec<(MembershipCoord, MembershipEntry)>> {
    let keys = match storage.list_membership_entries().await {
        Ok(keys) => keys,
        Err(e) => {
            warn!("membership re-list failed while resolving authorization: {e}");
            return None;
        }
    };
    match super::membership_ops::download_entries(storage, &keys).await {
        Ok(entries) => Some(entries),
        Err(e) => {
            warn!("membership re-download failed while resolving authorization: {e}");
            None
        }
    }
}

/// Advance `max` past the greatest `_updated_at` among `changes`, parsing each
/// as an HLC [`Timestamp`]. A row whose `_updated_at` fails to parse is logged
/// and skipped — it must not panic the pull or silently default the clock.
///
/// `max` becomes the value the caller advances the local HLC past, and that
/// advance is deliberately uncapped (it trusts a value already written to disk).
/// So the bound lives here, at the point a stamp is *collected*: a grossly-future
/// stamp — beyond `receiver_wall_ms` + [`super::hlc::MAX_FUTURE_SKEW_MS`] — is
/// logged and skipped, so it can never ratchet the clock. A conflicting row with
/// such a stamp was already refused by the apply, but a *non-conflicting* INSERT
/// (no local row to conflict with) reaches here as an applied row, so this is the
/// gate that stops it from dragging the clock forward.
fn advance_max_updated_at(
    max: &mut Option<Timestamp>,
    changes: &[RowChange],
    schema: &TableSchema,
    receiver_wall_ms: u64,
) {
    for change in changes {
        let Some(idx) = schema.updated_at(&change.table) else {
            // A table not in this device's synced set (a newer peer's schema): the
            // apply omitted its rows, so there is no applied `_updated_at` here to
            // advance the clock past.
            debug!(
                table = %change.table,
                "applied changeset references a table absent from the synced set, not advancing HLC"
            );
            continue;
        };
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
            Some(ts) if !ts.is_within_future_bound(receiver_wall_ms) => warn!(
                table = %change.table,
                value = raw,
                receiver_wall_ms,
                "applied row's _updated_at is grossly beyond the offline-skew \
                 allowance, not advancing HLC past it"
            ),
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

pub(crate) struct BlobDownload {
    blob: crate::blob::BlobRef,
    size: BlobDownloadSize,
}

enum BlobDownloadSize {
    Declared(u64),
    ExistingRow(crate::blob::BlobRef),
    InstalledRow,
    Missing,
}

impl BlobDownload {
    fn from_change(
        blob: crate::blob::BlobRef,
        source_size: Option<u64>,
        size_lookup_blob: Option<crate::blob::BlobRef>,
    ) -> Self {
        let size = match (source_size, size_lookup_blob) {
            (Some(size), _) => BlobDownloadSize::Declared(size),
            (None, Some(blob)) => BlobDownloadSize::ExistingRow(blob),
            (None, None) => BlobDownloadSize::Missing,
        };
        Self { blob, size }
    }

    pub(crate) fn from_installed_db(blob: crate::blob::BlobRef) -> Self {
        Self {
            blob,
            size: BlobDownloadSize::InstalledRow,
        }
    }

    async fn resolve_source_size(
        db: &Database,
        blob: &crate::blob::BlobRef,
        size: BlobDownloadSize,
    ) -> Result<u64, String> {
        match size {
            BlobDownloadSize::Declared(size) => Ok(size),
            BlobDownloadSize::ExistingRow(lookup) => {
                crate::blob::cache::expected_blob_size(db, &lookup)
                    .await
                    .map_err(|e| e.to_string())
            }
            BlobDownloadSize::InstalledRow => crate::blob::cache::expected_blob_size(db, blob)
                .await
                .map_err(|e| e.to_string()),
            BlobDownloadSize::Missing => Err(format!(
                "incoming blob {}/{} has no declared size",
                blob.namespace, blob.id
            )),
        }
    }
}

/// The `CacheEager` blobs the `changes` reference, derived per row from the
/// declarations. The cache fill a pulling device fetches into its cache before
/// applying the rows — fill-based, regardless of provenance. The incoming row's
/// declared plaintext size rides with each blob because incremental pull downloads
/// before applying that row to the DB. When an UPDATE changes the blob id but not
/// its size, SQLite omits the unchanged size column, so the old blob ref is kept
/// as the pre-apply DB lookup key for that unchanged size.
pub(crate) fn cache_eager_blobs(
    blob_decls: &BlobDecls,
    old_changes: &[RowChange],
    changes: &[RowChange],
) -> Result<Vec<BlobDownload>, crate::blob::decl::BlobDeclError> {
    if old_changes.len() != changes.len() {
        return Err(crate::blob::decl::BlobDeclError::ChangesetWalkMismatch {
            old_count: old_changes.len(),
            new_count: changes.len(),
        });
    }
    old_changes
        .iter()
        .zip(changes)
        .filter_map(
            |(old, change)| match blob_decls.ref_and_size_from_change(change) {
                Ok(Some((blob, size))) if blob.fill == CacheFill::CacheEager => {
                    match blob_decls.ref_from_change(old) {
                        Ok(old_blob) => Some(Ok(BlobDownload::from_change(blob, size, old_blob))),
                        Err(e) => Some(Err(e)),
                    }
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            },
        )
        .collect()
}

/// The **host-provided** blobs the `changes` reference, derived per row from the
/// declarations. The inline push uploads these before publishing the changeset:
/// coven owns a host-provided blob's bytes (in its local store or cache), so it can
/// upload it inline as its row reaches a changeset — provenance-based, regardless of
/// cache fill. A user-provided blob is the user's own file, uploaded only via
/// `make_remote` (which reads the user's path), so it is never on this inline path.
pub(crate) fn host_provided_blobs(
    blob_decls: &BlobDecls,
    changes: &[RowChange],
) -> Result<Vec<crate::blob::BlobRef>, crate::blob::decl::BlobDeclError> {
    changes
        .iter()
        .filter(|change| {
            matches!(
                change.op,
                crate::changeset::ChangeOp::Insert | crate::changeset::ChangeOp::Update
            )
        })
        .filter_map(|change| match blob_decls.ref_from_change(change) {
            Ok(Some(blob)) if blob.provenance == Provenance::HostProvided => Some(Ok(blob)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .collect()
}

/// Drop the local copy of every blob a deleted row in `changes` references —
/// wherever it is: the cache (a Remote blob's `pinned/` + `cache/` copies) and the
/// local store (a host-provided Local blob). Runs after a changeset applies: a
/// DELETE of a blob-bearing row (a gate retract or a genuine delete) must not leave
/// this peer's budget-exempt `pinned/` copy or its local-store copy behind. A failed
/// drop is propagated, not swallowed: the caller leaves the cursor unadvanced and
/// retries the whole changeset next cycle, so the cleanup is never silently lost.
/// Retry is safe — re-applying the changeset is idempotent and both drops ignore an
/// already-absent file. A peer normally holds a host-provided blob in its cache (it
/// was pulled there), but the local-store drop covers the case where the bytes are
/// there too; a deleted row needs neither.
async fn drop_deleted_blob_local(
    old_changes: &[RowChange],
    new_changes: &[RowChange],
    blob_decls: &BlobDecls,
    db: &Database,
    library_dir: &LibraryDir,
) -> Result<(), crate::blob::cache::BlobCacheError> {
    if old_changes.len() != new_changes.len() {
        return Err(crate::blob::cache::BlobCacheError::ChangesetWalkMismatch {
            old_count: old_changes.len(),
            new_count: new_changes.len(),
        });
    }
    for (old, new) in old_changes.iter().zip(new_changes) {
        let old_blob_to_drop = match old.op {
            crate::changeset::ChangeOp::Delete => blob_decls
                .ref_from_change(old)
                .map_err(|e| crate::blob::cache::BlobCacheError::Io(e.to_string()))?,
            crate::changeset::ChangeOp::Update => {
                let Some(old_blob) = blob_decls
                    .ref_from_change(old)
                    .map_err(|e| crate::blob::cache::BlobCacheError::Io(e.to_string()))?
                else {
                    continue;
                };
                let should_drop = match blob_decls
                    .ref_from_change(new)
                    .map_err(|e| crate::blob::cache::BlobCacheError::Io(e.to_string()))?
                {
                    Some(new_blob) => {
                        old_blob.namespace != new_blob.namespace || old_blob.id != new_blob.id
                    }
                    None => true,
                };
                if should_drop {
                    Some(old_blob)
                } else {
                    None
                }
            }
            crate::changeset::ChangeOp::Insert => None,
        };
        if let Some(blob) = old_blob_to_drop {
            let decls = db.blob_decls();
            let namespace = blob.namespace.clone();
            let id = blob.id.clone();
            let live_row = db
                .call(move |conn| {
                    decls
                        .row_for_blob_in_namespace(conn, &namespace, &id)
                        .map_err(|e| crate::database::DbError(e.to_string()))
                })
                .await
                .map_err(|e| crate::blob::cache::BlobCacheError::Io(e.to_string()))?;
            if live_row.is_some() {
                continue;
            }
            crate::blob::cache::drop_all_local_copies(library_dir, &blob.namespace, &blob.id)
                .await?;
        }
    }
    Ok(())
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

/// Download each blob in `blobs` into the evictable cache
/// `storage/cache/<namespace>/<id>` under `library_dir`, decrypting via storage
/// (which returns plaintext) and writing the bytes atomically. Returns true if every
/// blob succeeded. Skips blobs already present in either cache folder (`pinned/` or
/// `cache/`).
///
/// Only `CacheEager` blobs reach here (callers filter). On a peer the release is
/// Remote, so a `CacheEager` blob's bytes are a cache copy — evictable +
/// re-fetchable, not pinned: it lands in `storage/cache/<namespace>/<id>`, where it
/// evicts against its own namespace's budget (a cover never wiped by audio pressure).
/// (A cover that later falls out of that budget shows a placeholder until the next
/// read re-fetches it; covers are not pinned.) The destination is coven-built from
/// the validated namespace + blob id.
///
/// Each blob's public scope is resolved to the internal key scope here — the blob
/// declaration carries no key — via [`resolve_pull_scope`], which consults
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
    blobs: Vec<BlobDownload>,
    storage: &dyn SyncStorage,
    library_dir: &LibraryDir,
    in_changeset_keys: &HashMap<String, [u8; 32]>,
) -> bool {
    let mut all_ok = true;
    for download in blobs {
        let BlobDownload { blob, size } = download;
        // The blob's `id`/`namespace`/`cloud_path` come from a row in an incoming
        // changeset authored by any write-capable member. An id or namespace that is
        // not a single safe path token, or a cloud_path that escapes its prefix,
        // would write attacker-chosen bytes outside the library or under a forged
        // cloud key — refuse the blob as bad data before resolving or writing it, the
        // same posture as any other failed blob in this loop. The id check makes
        // local traversal unrepresentable: the destination path below is built from
        // the validated id by coven, so there is no host-supplied local path left to
        // independently re-check.
        if let Err(e) = crate::library_dir::validate_path_token(&blob.namespace) {
            error!(id = %blob.id, namespace = %blob.namespace, "blob namespace is not a safe path token ({e}); refusing");
            all_ok = false;
            continue;
        }
        if let Err(e) = crate::library_dir::validate_path_token(&blob.id) {
            error!(id = %blob.id, namespace = %blob.namespace, "blob id is not a safe path token ({e}); refusing");
            all_ok = false;
            continue;
        }
        if let Some(cloud_path) = blob.cloud_path.as_deref() {
            if let Err(e) = crate::library_dir::validate_cloud_path(cloud_path) {
                error!(id = %blob.id, cloud_path = %cloud_path, "blob cloud_path escapes its prefix ({e}); refusing");
                all_ok = false;
                continue;
            }
        }

        // The coven-built destination: the evictable cache
        // `storage/cache/<namespace>/<id>`. Building it validates the namespace and id
        // again (and that the id can form the `{ab}/{cd}` shard); a failure is the
        // same bad-data refusal as the token check above. A pinned copy (in
        // `pinned/`) is checked for presence below but never written here — a pull
        // populates the evictable cache, never the kept folder.
        let dest = match library_dir.cache_blob_path(&blob.namespace, &blob.id) {
            Ok(p) => p,
            Err(e) => {
                error!(id = %blob.id, "cannot build cache blob path ({e}); refusing");
                all_ok = false;
                continue;
            }
        };
        let pinned = match library_dir.pinned_blob_path(&blob.namespace, &blob.id) {
            Ok(p) => p,
            Err(e) => {
                error!(id = %blob.id, "cannot build pinned blob path ({e}); refusing");
                all_ok = false;
                continue;
            }
        };

        // Already on disk in either cache folder — don't re-download. A failed
        // existence check is a local-disk fault, not a missing blob — and the
        // download's own write would hit the same fault. Hold the cursor and retry
        // next cycle rather than treat the error as absence.
        match cached_in_either_folder(&dest, &pinned).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                error!(id = %blob.id, error = %e, "cannot check for local blob; holding");
                all_ok = false;
                continue;
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
        let source_size = match BlobDownload::resolve_source_size(db, &blob, size).await {
            Ok(size) => size,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot read blob size, skipping download");
                all_ok = false;
                continue;
            }
        };

        match storage
            .read_blob_to_file(
                &blob.namespace,
                &blob.id,
                resolved,
                blob.cloud_path.as_deref(),
                source_size,
                &dest,
            )
            .await
        {
            Ok(()) => {}
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "failed to download blob");
                all_ok = false;
            }
        }
    }
    all_ok
}

/// Whether a pulled blob is already on disk in either cache folder (`cache/` or
/// `pinned/`), so a pull doesn't re-download a blob a read already cached or the
/// user pinned. An existence-check failure (a broken filesystem) is surfaced, never
/// collapsed into "absent": re-downloading over a present file would mask the fault.
async fn cached_in_either_folder(
    cache: &std::path::Path,
    pinned: &std::path::Path,
) -> Result<bool, String> {
    for path in [cache, pinned] {
        if crate::local_blob::exists(path).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Debug)]
pub enum PullError {
    Storage(super::storage::StorageError),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn change_walk_pairing_rejects_mismatched_lengths() {
        let old_changes = vec![RowChange {
            table: "files".to_string(),
            op: crate::changeset::ChangeOp::Update,
            columns: vec![Some("file-1".to_string())],
        }];
        let new_changes = Vec::new();
        let tmp = tempfile::tempdir().expect("temp dir");
        let library_dir = LibraryDir::new(tmp.path());
        let (db, _stamper) = Database::open(
            std::path::Path::new(":memory:"),
            Vec::new(),
            "test-device".to_string(),
            &[],
        )
        .expect("open database");
        let conn = rusqlite::Connection::open_in_memory().expect("db");
        let decls = BlobDecls::from_tables(&conn, &[]).expect("decls");

        let err = drop_deleted_blob_local(&old_changes, &new_changes, &decls, &db, &library_dir)
            .await
            .expect_err("mismatched changeset walks fail");

        assert!(matches!(
            err,
            crate::blob::cache::BlobCacheError::ChangesetWalkMismatch {
                old_count: 1,
                new_count: 0
            }
        ));
    }
}
