/// Pull changesets from the sync storage and apply them to the local database.
///
/// Protocol:
/// 1. List heads from the sync storage (one S3 LIST call).
/// 2. Compare each device's seq to our local `sync_cursors` table.
/// 3. For each device that's ahead of our cursor, fetch new changesets.
/// 4. Unpack envelope, check schema_version, apply — resolving conflicts by
///    row arbitration on `_updated_at` plus a column-level premerge.
/// 5. Update sync_cursors for that device.
///
/// After all changesets are applied, any that had FK violations are retried once
/// -- the parent rows should now exist from other devices'
/// changesets applied in the same batch.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use super::apply::{resolve_and_apply_changeset_with_schema_on, BlobLocationAssignment};
use super::conflict::TableSchema;
use super::envelope::{self, verify_changeset_signature};
use super::hlc::Timestamp;
use super::membership::{MemberRole, MembershipChain, MembershipCoord};
use super::session::SyncedTable;
use super::storage::{DeviceHead, StorageError, SyncStorage};
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::{self, LocalBlobCleanupIntent};
use crate::blob::{CacheFill, Provenance};
use crate::changeset::RowChange;
use crate::database::Database;
use crate::store_dir::StoreDir;

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

/// Durably skip one authenticated changeset, then reflect that committed
/// position in this pull's returned cursor vector.
async fn advance_skipped_changeset(
    db: &Database,
    updated_cursors: &mut HashMap<String, u64>,
    device_id: &str,
    seq: u64,
) -> Result<(), crate::database::DbError> {
    db.advance_sync_cursor(device_id, seq - 1, seq).await?;
    updated_cursors.insert(device_id.to_string(), seq);
    Ok(())
}

fn is_current_owner(members: &[(String, MemberRole)], pubkey: &str) -> bool {
    members
        .iter()
        .any(|(pk, role)| pk == pubkey && *role == MemberRole::Owner)
}

/// A changeset skipped because its author is not a write-capable member, judged
/// against the exact membership entry the changeset is signed under (so it is
/// forged or revoked, not merely a propagation lag). Surfaced so the host can
/// warn the user, the way `skipped_schema` surfaces a newer-version skip.
#[derive(Debug, Clone)]
pub struct RejectedUnauthorized {
    pub device_id: String,
    pub seq: u64,
    /// Hex-encoded author pubkey claimed by the signed envelope.
    pub author: String,
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
    /// The envelope's signature covers its self-declared position
    /// (`device_id`, `seq`), but that position does not match the storage location
    /// the object was fetched from. The bytes are authentic for a *different*
    /// position, so an object relocated here would replay one changeset in
    /// another's place (and, by advancing the cursor, suppress the real occupant).
    /// Held so the host sees the relocation as tamper rather than a generic stall.
    PositionMismatch {
        declared_device_id: String,
        declared_seq: u64,
    },
    SizeMismatch {
        expected: usize,
        actual: usize,
    },
    ApplyFailed {
        error: String,
    },
    /// The object at this seq is absent from storage: its changeset was reclaimed
    /// (deleted as superseded) past this device's cursor. The cursor holds at the
    /// gap rather than advancing over it, and this device stream stops here;
    /// surfaced so the host reports reclaimed history instead of a generic stall.
    /// Reclamation pins its floor at every current reader's cursors and fails
    /// closed on a head it cannot read, so a reader that still needs this seq never
    /// has it deleted — this state should therefore be unreachable, and surfacing
    /// it loudly is what proves that.
    MissingChangeset,
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
    /// A blob needed before apply failed to download. The affected changeset and
    /// its cursor remain pending.
    pub asset_downloads_failed: bool,
    /// A post-commit local blob cleanup could not remove its files. The row and
    /// cursor are already durable; the cleanup intent remains durable too.
    pub local_blob_cleanup_pending: bool,
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
    /// applying. The whole changeset rolls back and its cursor stays put;
    /// surfaced so the host can warn about the unresolved clash.
    pub constraint_conflicts: Vec<ConstraintConflict>,
    /// All device heads fetched during this pull (including our own).
    /// Used by the sync status UI to show other devices' activity.
    pub remote_heads: Vec<DeviceHead>,
    /// Row changes from applied changesets, for the host to map to domain events.
    /// A refresh *hint*, not an exhaustive log: several accepted changesets can
    /// touch the same row, so a host maps these to which rows to refresh and then
    /// re-reads each by primary key rather than treating the list as final row
    /// state. Empty if nothing was applied.
    pub row_changes: Vec<RowChange>,
}

/// A changeset that had FK violations on first apply and needs retry.
struct DeferredChangeset {
    device_id: String,
    seq: u64,
    changeset: Vec<u8>,
    changes: Vec<RowChange>,
    /// Exact row/location bindings re-applied alongside the rows on retry.
    blob_uploads: Vec<BlobLocationAssignment>,
    cleanup_candidates: Vec<LocalBlobCleanupCandidate>,
}

struct CompletedChangeset<'a> {
    device_id: &'a str,
    seq: u64,
    changes: &'a [RowChange],
}

enum RemoteApplyOutcome {
    Applied,
    DeferredForeignKey,
    ConstraintConflict(Vec<String>),
}

/// The membership state one sync cycle judges every authorization against, loaded
/// and anchored once at the top of the cycle and threaded to the pull, the
/// write-grant binding, the snapshot-authorization decision, and the tombstone GC.
/// Loading it once (instead of each of those sites re-listing and re-downloading
/// the chain) both saves the round-trips and — more importantly — makes every
/// authorization decision in the cycle judge the *same* chain state, so two reads
/// can't disagree mid-cycle. Because [`load_anchored_chain`] writes the reader's
/// per-author head watermark, loading once also writes that watermark once.
///
/// [`load_anchored_chain`]: super::membership_ops::load_anchored_chain
pub struct CycleMembership {
    /// The owner-anchored, committed chain. `None` is representable for
    /// pre-initialization bootstrap callers; initialized cycles always carry one.
    pub chain: Option<MembershipChain>,
    /// The store's pinned owner (issue #102). `None` only before initialization.
    /// An owner-pinned store that fails to produce a valid chain aborts the load,
    /// so `Some(owner)` here always travels with `Some(chain)`.
    pub pinned_owner: Option<String>,
    /// The raw membership listing this cycle read (one `list_membership_entries`).
    /// The key refresh reads the visible activation coordinates from it, which is
    /// the LIST view (an entry is visible as soon as it is listed), distinct from
    /// the committed chain (an entry is committed only once a head certifies it).
    pub listed_entries: Vec<(String, u64)>,
}

/// Load and anchor the cycle's membership chain once. Every successful listing
/// is validated; a loader error aborts regardless of owner pin because an
/// unpinned database may already have accepted author floors. Only the loader's
/// explicit `Ok(None)` is an unpinned chain-less store. A LIST transport error
/// cannot establish that the store has no membership state, so it always aborts.
pub async fn load_cycle_membership(
    storage: &dyn SyncStorage,
    db: &Database,
) -> Result<CycleMembership, PullError> {
    // The store's established owner, pinned at create/join/restore (issue #102).
    // An initialized plaintext or encrypted store has `Some`; `None` is reserved
    // for bootstrap callers that run before owner establishment.
    let pinned_owner = db
        .get_sync_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|e| PullError::Apply(format!("read pinned owner: {e}")))?;

    let listed_entries = storage
        .list_membership_entries()
        .await
        .map_err(PullError::Storage)?;

    // Load + validate the chain and anchor it to the pinned owner. Every
    // successful LIST result, including an empty one, reaches the same optional
    // loader: persisted author floors can recover heads and entries omitted by
    // the listing. For an owner-pinned store a chain that won't validate, or one
    // founded by a different key, is tamper. This also fails loud without an
    // owner pin: an unpinned database may already hold accepted author floors,
    // whose missing or regressed state must not turn authorization off. The
    // database makes this load monotonic per author.
    let chain = match super::membership_ops::load_anchored_chain_if_known(
        storage,
        &listed_entries,
        pinned_owner.as_deref(),
        Some(db),
    )
    .await
    {
        Ok(chain) => chain,
        Err(error) => return Err(PullError::MembershipTampered(error.to_string())),
    };

    Ok(CycleMembership {
        chain,
        pinned_owner,
        listed_entries,
    })
}

/// Pull and apply all new changesets from the sync storage.
///
/// `db` is the owned connection handle; all apply and schema reads run through
/// it. The apply of incoming rows is a plain `db.call` — only a host write wrapped
/// in a journaled transaction is ever captured, so applied rows are never recorded
/// into the next outgoing changeset, while a host write landing during this pull's
/// network phases journals normally.
///
/// `tables` is the host's declared synced set; call sites pass
/// `db.synced_tables()`. The durable `sync_cursors` table is the sole pull
/// position. `membership_chain` and `owner_pubkey` are the cycle's once-loaded
/// membership (see [`CycleMembership`]).
///
/// Returns the updated cursors map and a summary of what was applied.
pub async fn pull_changes(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    our_device_id: &str,
    store_dir: &StoreDir,
    mut membership_chain: Option<MembershipChain>,
    owner_pubkey: Option<String>,
) -> Result<(HashMap<String, u64>, PullResult), PullError> {
    let cursors = db
        .get_all_sync_cursors()
        .await
        .map_err(|e| PullError::Apply(e.0))?;

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
    // The membership chain to validate changeset authorship against, loaded and
    // anchored once for the whole cycle by `load_cycle_membership` and handed in
    // here (with the store's pinned owner). `resolve_membership_authorization`
    // may still refresh it mid-pull to catch an authorizing entry the cycle-start
    // listing lagged, so it stays mutable.
    let membership_members = membership_chain
        .as_ref()
        .map(MembershipChain::current_members);

    // Check min_schema_version before processing any changesets. The floor is an
    // untrusted control object, so it is honored only when its verified author is
    // a current *owner*: a non-owner-signed (or, with a chain present, unsigned)
    // floor is a freeze/downgrade attempt and is ignored. `get_min_schema_version`
    // already verified the signature and surfaced the author; this is the
    // authorization half. With no chain (only before initialization) any
    // verified floor is honored because no owner has been established yet.
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

    // List every verified head. Membership authorization happens on each signed
    // envelope because a newly-added member's head can become visible before this
    // cycle's membership listing exposes its committed grant. Filtering the head
    // against the cycle-start chain would discard that stream before the grant's
    // signed author head can be resolved.
    // The pull uses the verified heads and ignores the unreadable-slot count: one
    // member's bad head must not wedge every device's sync. (Changeset reclamation
    // reads that same count to fail closed — a different, destructive decision.)
    let heads = storage
        .list_heads()
        .await
        .map_err(PullError::Storage)?
        .heads;

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
        local_blob_cleanup_pending: false,
        skipped_schema: 0,
        rejected_unauthorized: Vec::new(),
        invalid_signatures: Vec::new(),
        held_changesets: Vec::new(),
        constraint_conflicts: Vec::new(),
        remote_heads: heads.clone(),
        row_changes: Vec::new(),
    };
    result.local_blob_cleanup_pending = local_cleanup::drain(db, store_dir)
        .await
        .map_err(|e| PullError::Apply(e.0))?;
    let mut deferred: Vec<DeferredChangeset> = Vec::new();
    let mut applied_devices: HashSet<String> = HashSet::new();

    for head in &heads {
        // Skip our own device.
        if head.device_id == our_device_id {
            continue;
        }

        let local_seq = cursor_for_device(&cursors, &head.device_id);
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
                Err(StorageError::NotFound(_)) => {
                    // The changeset is gone — history was reclaimed past this
                    // device's cursor. Hold the cursor at the gap (never advance
                    // over a changeset this device has not applied) and name it, so
                    // the host reports reclaimed history rather than a generic
                    // stall. Other device streams continue.
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        "changeset missing — history reclaimed past this cursor; holding"
                    );
                    result.held_changesets.push(HeldChangeset {
                        device_id: head.device_id.clone(),
                        seq,
                        reason: HeldChangesetReason::MissingChangeset,
                    });
                    break;
                }
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

            // The signature covers the envelope's self-declared position
            // (device_id, seq). Bind that to the position this object was actually
            // fetched from BEFORE anything else reads the envelope: an object whose
            // authentic bytes belong to a different position was relocated here, and
            // applying it would replay that changeset in this slot while the cursor
            // advance suppresses the real occupant. Anyone holding the store key can
            // re-seal a peer's changeset under another key's authenticated data, so
            // this is the only check that ties the signed content to its location.
            // It precedes the schema gate so a relocated object cannot be laundered
            // into the benign skipped-schema count; hold the cursor and stop this
            // device's stream, the same discipline as a malformed or size-mismatched
            // object.
            if env.device_id != head.device_id || env.seq != seq {
                error!(
                    device_id = %head.device_id,
                    seq,
                    declared_device_id = %env.device_id,
                    declared_seq = env.seq,
                    "changeset envelope declares a different position than it was \
                     fetched from; holding cursor for this device"
                );
                result.held_changesets.push(HeldChangeset {
                    device_id: head.device_id.clone(),
                    seq,
                    reason: HeldChangesetReason::PositionMismatch {
                        declared_device_id: env.device_id.clone(),
                        declared_seq: env.seq,
                    },
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

            // Signature check: reject changesets with invalid signatures. A bad
            // signature is forged or corrupt; hold the cursor and stop this device
            // stream at the bad seq so it surfaces as a bounded stall instead of
            // silently suppressing the seq's data. This runs before the schema gate
            // so that only an authentic envelope's schema_version steers control
            // flow: a forged object with a large schema_version must surface as
            // tamper, not be laundered into the benign skipped-schema count as
            // routine version skew.
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

            // The verified device head commits this stream to one signer. The
            // envelope must carry the same signer: another member's authentic
            // changeset cannot be relocated into this stream, and an unsigned
            // envelope cannot occupy a signed member stream. This mismatch is
            // permanent attacker-controlled content, so reject and advance rather
            // than letting it hold the stream forever.
            if env.author_pubkey != head.author_pubkey {
                error!(
                    device_id = %head.device_id,
                    seq,
                    head_author = %head.author_pubkey,
                    envelope_author = %env.author_pubkey,
                    "changeset signer does not match the verified device-head signer; rejecting"
                );
                result.rejected_unauthorized.push(RejectedUnauthorized {
                    device_id: head.device_id.clone(),
                    seq,
                    author: env.author_pubkey.clone(),
                });
                advance_skipped_changeset(db, &mut updated_cursors, &head.device_id, seq)
                    .await
                    .map_err(|e| PullError::Apply(e.0))?;
                continue;
            }

            // Schema version check: skip changesets from a newer schema. The
            // envelope is authenticated above, so this schema_version is the one the
            // author actually wrote against.
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

            // Every initialized-store changeset binds itself to its exact effective
            // write grant. Current membership alone is insufficient: the coordinate
            // is what ties the signed envelope to the committed membership prefix.
            // A newly-added author is resolved by reloading the signed author heads
            // with the named grant's author included as a discovery candidate.
            if let Some(current) = membership_chain.as_ref() {
                match resolve_membership_authorization(
                    storage,
                    db,
                    current,
                    owner_pubkey.as_deref(),
                    &env.author_pubkey,
                    env.membership_grant.as_ref(),
                )
                .await
                {
                    MembershipJudgment::Authorized(refreshed) => {
                        // Adopt any newly committed membership state for later
                        // changesets in this cycle.
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
                            author = %env.author_pubkey,
                            "changeset author is not a write-capable member \
                             against the membership entry it is signed under; \
                             skipping (forged or revoked)"
                        );
                        result.rejected_unauthorized.push(RejectedUnauthorized {
                            device_id: head.device_id.clone(),
                            seq,
                            author: env.author_pubkey.clone(),
                        });
                        advance_skipped_changeset(db, &mut updated_cursors, &head.device_id, seq)
                            .await
                            .map_err(|e| PullError::Apply(e.0))?;
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

            if changeset_bytes.is_empty() {
                advance_skipped_changeset(db, &mut updated_cursors, &head.device_id, seq)
                    .await
                    .map_err(|e| PullError::Apply(e.0))?;
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

            let blob_uploads = match blob_location_assignments(
                db,
                &blob_decls,
                &changes,
                &schema,
                &env.blob_locations,
            )
            .await
            {
                Ok(uploads) => uploads,
                Err(e) => {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        "failed to validate introduced blobs, skipping without applying: {e}"
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
            // The author of this changeset uploaded the blobs it introduces, so it
            // is the prefix they live under.
            let blobs_ok =
                download_blobs(db, cache_eager, storage, store_dir, &env.blob_locations).await;
            if !blobs_ok {
                warn!(
                    "Blob download failed for {}/{}, not applying; cursor not advanced",
                    head.device_id, seq
                );
                result.asset_downloads_failed = true;
                break;
            }

            // Every referenced blob is now durable on disk: apply the changeset.
            // A plain `call` — applied rows are never journaled (only a
            // `run_pending_journaled_transaction_on` host write is), so they can't
            // echo as this device's own outgoing changes.
            let cleanup_candidates =
                match local_blob_cleanup_intents(&blob_decls, &old_changes, &changes) {
                    Ok(intents) => intents,
                    Err(e) => {
                        error!(
                            device_id = %head.device_id,
                            seq,
                            error = %e,
                            "failed to derive local blob cleanup intents; holding cursor"
                        );
                        result.held_changesets.push(HeldChangeset {
                            device_id: head.device_id.clone(),
                            seq,
                            reason: HeldChangesetReason::ApplyFailed {
                                error: e.to_string(),
                            },
                        });
                        break;
                    }
                };
            let apply_outcome = match commit_remote_changeset(
                db,
                &head.device_id,
                seq,
                &changeset_bytes,
                &changes,
                schema.clone(),
                receiver_wall_ms,
                &blob_uploads,
                &cleanup_candidates,
            )
            .await
            {
                Ok(outcome) => outcome,
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

            #[cfg(any(test, feature = "test-utils"))]
            db.reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id: head.device_id.clone(),
                seq,
            })
            .await;

            match apply_outcome {
                RemoteApplyOutcome::Applied => {}
                RemoteApplyOutcome::DeferredForeignKey => {
                    deferred.push(DeferredChangeset {
                        device_id: head.device_id.clone(),
                        seq,
                        changeset: changeset_bytes.clone(),
                        changes,
                        blob_uploads,
                        cleanup_candidates,
                    });
                    break;
                }
                RemoteApplyOutcome::ConstraintConflict(tables) => {
                    record_constraint_conflicts(&mut result, &head.device_id, seq, tables);
                    break;
                }
            }

            finish_applied_changeset(
                &mut result,
                &mut updated_cursors,
                &mut applied_devices,
                CompletedChangeset {
                    device_id: &head.device_id,
                    seq,
                    changes: &changes,
                },
                db,
                store_dir,
            )
            .await
            .map_err(|e| PullError::Apply(e.0))?;
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
            let retry_outcome = match commit_remote_changeset(
                db,
                &d.device_id,
                d.seq,
                &d.changeset,
                &d.changes,
                schema.clone(),
                receiver_wall_ms,
                &d.blob_uploads,
                &d.cleanup_candidates,
            )
            .await
            {
                Ok(outcome) => outcome,
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

            #[cfg(any(test, feature = "test-utils"))]
            db.reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id: d.device_id.clone(),
                seq: d.seq,
            })
            .await;

            match retry_outcome {
                RemoteApplyOutcome::Applied => {}
                RemoteApplyOutcome::DeferredForeignKey => {
                    warn!(
                        device_id = %d.device_id,
                        seq = d.seq,
                        "changeset still has FK violations after retry; cursor not advanced"
                    );
                    continue;
                }
                RemoteApplyOutcome::ConstraintConflict(tables) => {
                    record_constraint_conflicts(&mut result, &d.device_id, d.seq, tables);
                    continue;
                }
            }
            finish_applied_changeset(
                &mut result,
                &mut updated_cursors,
                &mut applied_devices,
                CompletedChangeset {
                    device_id: &d.device_id,
                    seq: d.seq,
                    changes: &d.changes,
                },
                db,
                store_dir,
            )
            .await
            .map_err(|e| PullError::Apply(e.0))?;
        }
    }

    result.devices_pulled = applied_devices.len() as u64;

    Ok((updated_cursors, result))
}

#[allow(clippy::too_many_arguments)]
async fn commit_remote_changeset(
    db: &Database,
    device_id: &str,
    seq: u64,
    changeset: &[u8],
    changes: &[RowChange],
    schema: Arc<TableSchema>,
    receiver_wall_ms: u64,
    blob_uploads: &[BlobLocationAssignment],
    cleanup_candidates: &[LocalBlobCleanupCandidate],
) -> Result<RemoteApplyOutcome, crate::database::DbError> {
    let device_id = device_id.to_string();
    let changeset = changeset.to_vec();
    let blob_uploads = blob_uploads.to_vec();
    let cleanup_candidates = cleanup_candidates.to_vec();
    let mut changeset_max = None;
    advance_max_updated_at(&mut changeset_max, changes, &schema, receiver_wall_ms);
    let hlc = db.hlc();
    let blob_decls = db.blob_decls();
    db.call(move |conn| {
        let tx = conn
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        let cleanup_intents =
            resolve_local_blob_cleanup_intents(&tx, &blob_decls, cleanup_candidates)
                .map_err(|error| crate::database::DbError(error.to_string()))?;
        let apply = resolve_and_apply_changeset_with_schema_on(
            &tx,
            &changeset,
            schema,
            receiver_wall_ms,
            &blob_uploads,
        )?;
        if !apply.constraint_conflict_tables.is_empty() {
            tx.rollback().map_err(crate::database::DbError::from)?;
            return Ok(RemoteApplyOutcome::ConstraintConflict(
                apply.constraint_conflict_tables,
            ));
        }
        if apply.had_fk_violations {
            tx.rollback().map_err(crate::database::DbError::from)?;
            return Ok(RemoteApplyOutcome::DeferredForeignKey);
        }
        for intent in cleanup_intents {
            local_cleanup::record_if_unreferenced_on(&tx, &blob_decls, &intent)?;
        }
        Database::advance_sync_cursor_on(&tx, &device_id, seq - 1, seq)?;
        tx.commit().map_err(crate::database::DbError::from)?;
        if let Some(max_applied) = &changeset_max {
            hlc.advance_past(max_applied);
        }
        Ok(RemoteApplyOutcome::Applied)
    })
    .await
}

async fn finish_applied_changeset(
    result: &mut PullResult,
    updated_cursors: &mut HashMap<String, u64>,
    applied_devices: &mut HashSet<String>,
    applied: CompletedChangeset<'_>,
    db: &Database,
    store_dir: &StoreDir,
) -> Result<(), crate::database::DbError> {
    result.changesets_applied += 1;
    result.row_changes.extend(applied.changes.to_vec());
    applied_devices.insert(applied.device_id.to_string());
    updated_cursors.insert(applied.device_id.to_string(), applied.seq);

    // Cleanup obligations were committed beside the row and cursor. Draining them
    // does not control whether the changeset is materialized; a failed filesystem
    // operation leaves its durable intent for the next drain. Keep this after the
    // row-and-cursor transaction advanced the shared clock before returning, so
    // awaited filesystem work cannot expose a committed row under an older clock.
    result.local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
    Ok(())
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
            "changeset hit a non-retryable SQLite constraint conflict; changeset rolled back"
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

/// Decide whether the envelope names the exact committed entry that currently
/// grants `author` write access. A grant already present in `current` is decided
/// without storage reads. Otherwise the chain is reloaded through signed author
/// heads, including the grant coordinate's author as a discovery candidate, so a
/// lagging LIST cannot hide a committed grant and a bare stored entry can never
/// authorize a write.
///
/// `owner_pubkey` is the store's pinned owner after initialization, for either
/// storage representation. Any chain this adopts or merges must still be founded
/// by that owner; a refreshed/merged chain that is not is a takeover attempt and
/// does not authorize anyone, so it is treated as unauthorized — never adopted.
async fn resolve_membership_authorization(
    storage: &dyn SyncStorage,
    db: &Database,
    current: &MembershipChain,
    owner_pubkey: Option<&str>,
    author: &str,
    grant: Option<&MembershipCoord>,
) -> MembershipJudgment {
    let Some(coord) = grant else {
        return MembershipJudgment::Unauthorized;
    };
    if current.can_write_now(author) && current.write_grant_coord(author).as_ref() == Some(coord) {
        return MembershipJudgment::Authorized(current.clone());
    }

    let entries = match storage.list_membership_entries().await {
        Ok(entries) => entries,
        Err(error) => {
            warn!(
                error = %error,
                "membership listing unavailable while resolving a changeset grant"
            );
            return MembershipJudgment::Indeterminate;
        }
    };

    let fresh = match super::membership_ops::load_anchored_chain_with_candidates(
        storage,
        &entries,
        std::slice::from_ref(coord),
        owner_pubkey,
        Some(db),
    )
    .await
    {
        Ok(Some(chain)) => chain,
        Ok(None) => return MembershipJudgment::Unauthorized,
        Err(super::membership_ops::AnchoredChainError::StorageUnavailable {
            operation,
            source,
        }) => {
            warn!(
                %operation,
                error = %source,
                "membership storage unavailable while resolving a changeset grant"
            );
            return MembershipJudgment::Indeterminate;
        }
        Err(error) => {
            warn!(
                grant_author = %coord.author_pubkey,
                grant_seq = coord.seq,
                error = %error,
                "named membership grant is not in a valid committed chain; rejecting changeset"
            );
            return MembershipJudgment::Unauthorized;
        }
    };

    if fresh.can_write_now(author) && fresh.write_grant_coord(author).as_ref() == Some(coord) {
        MembershipJudgment::Authorized(fresh)
    } else {
        MembershipJudgment::Unauthorized
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
            // Incoming apply rejects the entire changeset before mutation when any
            // operation names an undeclared table. Reaching this after a successful
            // apply means its walked rows and the apply schema disagree.
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

pub(crate) struct BlobDownload {
    blob: crate::blob::BlobRef,
    size: BlobDownloadSize,
    hash: BlobDownloadHash,
    cloud_path: BlobDownloadCloudPath,
}

/// The row a change is about, named the way every change names it: its table and primary
/// key. A changeset UPDATE reports only the columns whose values CHANGED, so a column it
/// omits has to be read back from the row itself — and this is the only handle on that row
/// that a change always carries, and that a device always resolves.
///
/// Naming it by the change's *old blob id* instead — "the row that carries blob X" — asks a
/// question a device whose row has already moved on cannot answer: a concurrent repointing
/// of the same row leaves no row carrying X at all, and the lookup fails on a row that is
/// sitting right there under its primary key.
///
/// What the row can answer is the value it currently holds, which is the author's omitted
/// value exactly when this device agrees with the author's pre-image on that column. It
/// does not when a *concurrent* change moved that same column: then the author's value is
/// in no local state and in no part of the changeset, and the download fails its hash
/// check. Recovering an omitted column from local state cannot cover that; only reading the
/// blob's length from its own cloud object would, and the object is reachable precisely
/// because its key names the blob.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PreApplyRow {
    table: String,
    pk: String,
}

/// A cleanup obligation derived from the signed row change. SQLite omits unchanged
/// columns from an UPDATE, so the old hash may need to be read from `pre_apply` while
/// the apply transaction still holds the row's prior state.
#[derive(Clone, Debug)]
struct LocalBlobCleanupCandidate {
    namespace: String,
    blob_id: String,
    content_hash: Option<String>,
    pre_apply: Option<PreApplyRow>,
}

/// Where the download reads a blob's declared plaintext size and content hash
/// from. Both ride with the changeset row when the row change carries them
/// (`Declared`); an update that changed the blob id but not size/hash omits those
/// columns — from its old values as well as its new ones — so they are read back from the
/// [`PreApplyRow`] (`ExistingRow`); a snapshot backfill reads both from the freshly
/// bootstrapped DB row (`InstalledRow`).
enum BlobDownloadSize {
    Declared(u64),
    ExistingRow(PreApplyRow),
    InstalledRow,
}

enum BlobDownloadHash {
    Declared(String),
    ExistingRow(PreApplyRow),
    InstalledRow,
}

/// Where the download reads a blob's readable cloud path from — the key a browsable
/// home stores it at. The same three sources the size and hash have, with one
/// difference: a blob legitimately has no cloud path at all (`Absent`) on an opaque
/// home, which keys by id, so its absence is a value rather than an error. An update
/// that repointed a row at a new blob left its cloud path alone, so the column is
/// missing from the change and the pre-apply row holds the (unchanged) value.
enum BlobDownloadCloudPath {
    Declared(String),
    ExistingRow(PreApplyRow),
    Absent,
}

impl BlobDownload {
    fn from_change(
        blob: crate::blob::BlobRef,
        source_size: Option<u64>,
        source_hash: Option<String>,
        lookup_blob: Option<crate::blob::BlobRef>,
        pre_apply: Option<PreApplyRow>,
    ) -> Result<Self, crate::blob::decl::BlobDeclError> {
        let size = match (source_size, pre_apply.clone()) {
            (Some(size), _) => BlobDownloadSize::Declared(size),
            (None, Some(row)) => BlobDownloadSize::ExistingRow(row),
            (None, None) => {
                return Err(crate::blob::decl::BlobDeclError::Gate(format!(
                    "incoming blob {}/{} has no declared size or pre-apply row",
                    blob.namespace, blob.id
                )));
            }
        };
        let hash = match (source_hash, pre_apply.clone()) {
            (Some(hash), _) => BlobDownloadHash::Declared(hash),
            (None, Some(row)) => BlobDownloadHash::ExistingRow(row),
            (None, None) => {
                return Err(crate::blob::decl::BlobDeclError::Gate(format!(
                    "incoming blob {}/{} has no declared content hash or pre-apply row",
                    blob.namespace, blob.id
                )));
            }
        };
        let cloud_path = match (blob.cloud_path.clone(), lookup_blob, pre_apply) {
            (Some(path), _, _) => BlobDownloadCloudPath::Declared(path),
            (None, Some(_), Some(row)) => BlobDownloadCloudPath::ExistingRow(row),
            (None, _, _) => BlobDownloadCloudPath::Absent,
        };
        Ok(Self {
            blob,
            size,
            hash,
            cloud_path,
        })
    }

    pub(crate) fn from_installed_db(blob: crate::blob::BlobRef) -> Self {
        // A row read whole out of the bootstrapped DB carries its cloud path already.
        let cloud_path = match blob.cloud_path.clone() {
            Some(path) => BlobDownloadCloudPath::Declared(path),
            None => BlobDownloadCloudPath::Absent,
        };
        Self {
            blob,
            size: BlobDownloadSize::InstalledRow,
            hash: BlobDownloadHash::InstalledRow,
            cloud_path,
        }
    }

    fn resolve_source_size(
        blob: &crate::blob::BlobRef,
        size: BlobDownloadSize,
        record: Option<&super::envelope::BlobLocationRecord>,
    ) -> Result<u64, String> {
        if let Some(record) = record {
            let signed_size = record.plaintext_size;
            if let BlobDownloadSize::Declared(declared_size) = &size {
                if *declared_size != signed_size {
                    return Err(format!(
                        "incoming blob {}/{} declares size {declared_size}, but its signed location record declares {signed_size}",
                        blob.namespace, blob.id
                    ));
                }
            }
            return Ok(signed_size);
        }
        match size {
            BlobDownloadSize::Declared(size) => Ok(size),
            BlobDownloadSize::ExistingRow(_) => Err(format!(
                "incoming blob {}/{} size was not resolved from its pre-apply row",
                blob.namespace, blob.id
            )),
            BlobDownloadSize::InstalledRow => Err(format!(
                "installed blob {}/{} size was not resolved from its live row",
                blob.namespace, blob.id
            )),
        }
    }

    fn resolve_source_hash(
        blob: &crate::blob::BlobRef,
        hash: BlobDownloadHash,
        record: Option<&super::envelope::BlobLocationRecord>,
    ) -> Result<String, String> {
        if let Some(record) = record {
            let signed_hash = record.content_hash.as_str();
            if let BlobDownloadHash::Declared(declared_hash) = &hash {
                if declared_hash != signed_hash {
                    return Err(format!(
                        "incoming blob {}/{} declares content hash {declared_hash}, but its signed location record declares {signed_hash}",
                        blob.namespace, blob.id
                    ));
                }
            }
            return Ok(signed_hash.to_string());
        }
        match hash {
            BlobDownloadHash::Declared(hash) => Ok(hash),
            BlobDownloadHash::ExistingRow(_) => Err(format!(
                "incoming blob {}/{} content hash was not resolved from its pre-apply row",
                blob.namespace, blob.id
            )),
            BlobDownloadHash::InstalledRow => Err(format!(
                "installed blob {}/{} content hash was not resolved from its live row",
                blob.namespace, blob.id
            )),
        }
    }

    fn resolve_cloud_path(cloud_path: BlobDownloadCloudPath) -> Result<Option<String>, String> {
        match cloud_path {
            BlobDownloadCloudPath::Declared(path) => Ok(Some(path)),
            BlobDownloadCloudPath::ExistingRow(_) => {
                Err("partial blob cloud path was not resolved from its pre-apply row".to_string())
            }
            BlobDownloadCloudPath::Absent => Ok(None),
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
        .filter(|(_, change)| !matches!(change.op, crate::changeset::ChangeOp::Delete))
        .filter_map(
            |(old, change)| match blob_decls.ref_size_hash_from_change(change) {
                Ok(Some((blob, size, hash))) if blob.fill == CacheFill::CacheEager => {
                    // The row this change is about: every change carries its primary key,
                    // in its old values as well as its new ones.
                    let pre_apply = (change.op == crate::changeset::ChangeOp::Update)
                        .then(|| {
                            change.pk().map(|pk| PreApplyRow {
                                table: change.table.clone(),
                                pk: pk.to_string(),
                            })
                        })
                        .flatten();
                    match blob_decls.ref_from_change(old) {
                        Ok(old_blob) => Some(BlobDownload::from_change(
                            blob, size, hash, old_blob, pre_apply,
                        )),
                        Err(e) => Some(Err(e)),
                    }
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            },
        )
        .collect()
}

/// Bind each referenced blob location to the signed row version that carries it.
/// The apply records the location only when that exact row version wins.
async fn blob_location_assignments(
    db: &Database,
    blob_decls: &BlobDecls,
    changes: &[RowChange],
    schema: &TableSchema,
    records: &[super::envelope::BlobLocationRecord],
) -> Result<Vec<BlobLocationAssignment>, crate::blob::decl::BlobDeclError> {
    let mut referenced = Vec::new();
    for new in changes {
        let Some((new_blob, declared_size, declared_hash)) =
            blob_decls.ref_size_hash_from_change(new)?
        else {
            continue;
        };
        if matches!(new.op, crate::changeset::ChangeOp::Delete) {
            continue;
        }
        let pk = new.pk().ok_or_else(|| {
            crate::blob::decl::BlobDeclError::Gate(format!(
                "blob-bearing {} row has no primary key",
                new.table
            ))
        })?;
        let version_index = schema.updated_at(&new.table).ok_or_else(|| {
            crate::blob::decl::BlobDeclError::Gate(format!(
                "blob-bearing table {} has no _updated_at column",
                new.table
            ))
        })?;
        let row_version = new.col(version_index).ok_or_else(|| {
            crate::blob::decl::BlobDeclError::Gate(format!(
                "blob-bearing {}/{} has no _updated_at value",
                new.table, pk
            ))
        })?;
        referenced.push((
            new.table.clone(),
            pk.to_string(),
            row_version.to_string(),
            new_blob.namespace,
            new_blob.id,
            new.op,
            declared_size,
            declared_hash,
        ));
    }
    let mut by_blob = HashMap::new();
    for record in records {
        crate::store_dir::validate_path_token(&record.namespace).map_err(|error| {
            crate::blob::decl::BlobDeclError::Gate(format!(
                "invalid blob location namespace: {error}"
            ))
        })?;
        crate::store_dir::validate_path_token(&record.blob_id).map_err(|error| {
            crate::blob::decl::BlobDeclError::Gate(format!("invalid blob location id: {error}"))
        })?;
        record.location.validate().map_err(|error| {
            crate::blob::decl::BlobDeclError::Gate(format!("invalid blob location: {error}"))
        })?;
        if by_blob
            .insert(
                (record.namespace.clone(), record.blob_id.clone()),
                record.clone(),
            )
            .is_some()
        {
            return Err(crate::blob::decl::BlobDeclError::Gate(format!(
                "duplicate blob location for {}/{}",
                record.namespace, record.blob_id
            )));
        }
    }
    let referenced_keys = referenced
        .iter()
        .map(|(_, _, _, namespace, id, _, _, _)| (namespace.clone(), id.clone()))
        .collect::<HashSet<_>>();
    let mut resolved = Vec::with_capacity(referenced.len());
    for (table, pk, row_version, namespace, id, op, declared_size, declared_hash) in referenced {
        let record = by_blob
            .get(&(namespace.clone(), id.clone()))
            .ok_or_else(|| {
                crate::blob::decl::BlobDeclError::Gate(format!(
                    "changeset omits cloud location for {namespace}/{id}"
                ))
            })?;
        let (effective_size, effective_hash) = match (declared_size, declared_hash) {
            (Some(size), Some(hash)) => (size, hash),
            (partial_size, partial_hash) => {
                if op != crate::changeset::ChangeOp::Update {
                    return Err(crate::blob::decl::BlobDeclError::Gate(format!(
                        "incoming blob {namespace}/{id} omits required size or content hash"
                    )));
                }
                let decls = db.blob_decls();
                let lookup_table = table.clone();
                let lookup_pk = pk.clone();
                let content = db
                    .call(move |conn| {
                        decls
                            .live_content_for_row(conn, &lookup_table, &lookup_pk)
                            .map_err(|error| crate::database::DbError(error.to_string()))?
                            .ok_or_else(|| {
                                crate::database::DbError(format!(
                                    "pre-apply blob row {lookup_table}/{lookup_pk} does not exist"
                                ))
                            })
                    })
                    .await
                    .map_err(|error| crate::blob::decl::BlobDeclError::Gate(error.to_string()))?;
                let size = match partial_size {
                    Some(size) => size,
                    None => content.plaintext_size,
                };
                let hash = match partial_hash {
                    Some(hash) => hash,
                    None => content.content_hash,
                };
                (size, hash)
            }
        };
        if effective_size != record.plaintext_size {
            return Err(crate::blob::decl::BlobDeclError::Gate(format!(
                "incoming blob {namespace}/{id} declares size {effective_size}, but its signed location record declares {}",
                record.plaintext_size
            )));
        }
        if effective_hash != record.content_hash {
            return Err(crate::blob::decl::BlobDeclError::Gate(format!(
                "incoming blob {namespace}/{id} declares content hash {effective_hash}, but its signed location record declares {}",
                record.content_hash
            )));
        }
        resolved.push(BlobLocationAssignment {
            table,
            pk,
            row_version,
            namespace,
            blob_id: id,
            location: record.location.clone(),
        });
    }
    if let Some((namespace, id)) = by_blob
        .keys()
        .find(|key| !referenced_keys.contains(*key))
        .cloned()
    {
        return Err(crate::blob::decl::BlobDeclError::Gate(format!(
            "changeset carries cloud location for unreferenced blob {namespace}/{id}"
        )));
    }
    Ok(resolved)
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

/// Derive every local-blob cleanup obligation from a changeset before its rows
/// apply. The caller stores these intents in the same transaction as the rows and
/// cursor, so filesystem cleanup may happen afterward without leaving an
/// unrecorded obligation. A DELETE removes its old blob; an UPDATE does so only
/// when it repoints or clears the blob reference.
fn local_blob_cleanup_intents(
    blob_decls: &BlobDecls,
    old_changes: &[RowChange],
    new_changes: &[RowChange],
) -> Result<Vec<LocalBlobCleanupCandidate>, crate::blob::decl::BlobDeclError> {
    if old_changes.len() != new_changes.len() {
        return Err(crate::blob::decl::BlobDeclError::ChangesetWalkMismatch {
            old_count: old_changes.len(),
            new_count: new_changes.len(),
        });
    }
    let mut intents = Vec::new();
    for (old, new) in old_changes.iter().zip(new_changes) {
        let old_blob_to_drop = match old.op {
            crate::changeset::ChangeOp::Delete => blob_decls
                .ref_size_hash_from_change(old)?
                .map(|(blob, _, hash)| (blob, hash)),
            crate::changeset::ChangeOp::Update => {
                let Some((old_blob, _, old_hash)) = blob_decls.ref_size_hash_from_change(old)?
                else {
                    continue;
                };
                let should_drop = match blob_decls.ref_size_hash_from_change(new)? {
                    Some((new_blob, _, new_hash)) => {
                        old_blob.namespace != new_blob.namespace
                            || old_blob.id != new_blob.id
                            || new_hash.as_ref().or(old_hash.as_ref()) != old_hash.as_ref()
                    }
                    None => true,
                };
                if should_drop {
                    Some((old_blob, old_hash))
                } else {
                    None
                }
            }
            crate::changeset::ChangeOp::Insert => None,
        };
        if let Some((blob, content_hash)) = old_blob_to_drop {
            intents.push(LocalBlobCleanupCandidate {
                namespace: blob.namespace,
                blob_id: blob.id,
                content_hash,
                pre_apply: old.pk().map(|pk| PreApplyRow {
                    table: old.table.clone(),
                    pk: pk.to_string(),
                }),
            });
        }
    }
    Ok(intents)
}

/// Resolve every candidate to the exact immutable local-store version before the
/// row apply mutates the pre-image. This runs inside the same transaction as apply,
/// intent insertion, and cursor advancement.
fn resolve_local_blob_cleanup_intents(
    conn: &rusqlite::Connection,
    blob_decls: &BlobDecls,
    candidates: Vec<LocalBlobCleanupCandidate>,
) -> Result<Vec<LocalBlobCleanupIntent>, crate::blob::decl::BlobDeclError> {
    candidates
        .into_iter()
        .map(|candidate| {
            let content_hash = match candidate.content_hash {
                Some(hash) => hash,
                None => {
                    let pre_apply = candidate.pre_apply.ok_or_else(|| {
                        crate::blob::decl::BlobDeclError::Gate(format!(
                            "removed blob {}/{} has no signed content hash or carrying row",
                            candidate.namespace, candidate.blob_id
                        ))
                    })?;
                    blob_decls
                        .hash_for_row(conn, &pre_apply.table, &pre_apply.pk)?
                        .ok_or_else(|| {
                            crate::blob::decl::BlobDeclError::Gate(format!(
                                "removed blob {}/{} has no signed content hash",
                                candidate.namespace, candidate.blob_id
                            ))
                        })?
                }
            };
            Ok(LocalBlobCleanupIntent::new(
                candidate.namespace,
                candidate.blob_id,
                content_hash,
            ))
        })
        .collect()
}

/// Download each blob in `blobs` into the evictable cache
/// `storage/cache/<namespace>/<id>/<content_hash>` under `store_dir`, decrypting via storage
/// (which returns plaintext) and writing the bytes atomically. Returns true if every
/// blob succeeded. Skips blobs already present in either cache folder (`pinned/` or
/// `cache/`).
///
/// Only `CacheEager` blobs reach here (callers filter). On a peer the release is
/// Remote, so a `CacheEager` blob's bytes are a cache copy — evictable +
/// re-fetchable, not pinned: it lands in `storage/cache/<namespace>/<id>/<content_hash>`, where it
/// evicts against its own namespace's budget (a cover never wiped by audio pressure).
/// (A cover that later falls out of that budget shows a placeholder until the next
/// read re-fetches it; covers are not pinned.) The destination is coven-built from
/// the validated namespace + blob id.
///
/// Shared by the incremental pull (per applied changeset) and the snapshot
/// bootstrap backfill (per row in the freshly bootstrapped DB), so the
/// download/decrypt/write path lives in one place.
/// Download a set of blobs into the cache. Changesets provide each exact
/// location; snapshot backfill resolves the location from the preserved local index.
pub(crate) async fn download_blobs(
    db: &Database,
    blobs: Vec<BlobDownload>,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    known_locations: &[super::envelope::BlobLocationRecord],
) -> bool {
    let mut all_ok = true;
    for download in blobs {
        let BlobDownload {
            mut blob,
            mut size,
            mut hash,
            mut cloud_path,
        } = download;
        let size_from_installed_row = matches!(size, BlobDownloadSize::InstalledRow);
        let hash_from_installed_row = matches!(hash, BlobDownloadHash::InstalledRow);
        if size_from_installed_row != hash_from_installed_row {
            error!(id = %blob.id, namespace = %blob.namespace, "installed blob metadata source is incomplete; refusing");
            all_ok = false;
            continue;
        }
        if size_from_installed_row {
            let content = match crate::blob::cache::live_blob_content(db, &blob).await {
                Ok(content) => content,
                Err(error) => {
                    warn!(id = %blob.id, namespace = %blob.namespace, %error, "cannot read installed blob content, skipping download");
                    all_ok = false;
                    continue;
                }
            };
            blob = content.blob;
            size = BlobDownloadSize::Declared(content.plaintext_size);
            hash = BlobDownloadHash::Declared(content.content_hash);
            cloud_path = match blob.cloud_path.clone() {
                Some(path) => BlobDownloadCloudPath::Declared(path),
                None => BlobDownloadCloudPath::Absent,
            };
        }
        let mut pre_apply = None;
        for candidate in [
            match &size {
                BlobDownloadSize::ExistingRow(row) => Some(row),
                _ => None,
            },
            match &hash {
                BlobDownloadHash::ExistingRow(row) => Some(row),
                _ => None,
            },
            match &cloud_path {
                BlobDownloadCloudPath::ExistingRow(row) => Some(row),
                _ => None,
            },
        ]
        .into_iter()
        .flatten()
        {
            if pre_apply.is_some_and(|row| row != candidate) {
                error!(id = %blob.id, namespace = %blob.namespace, "partial blob metadata names different pre-apply rows; refusing");
                all_ok = false;
                pre_apply = None;
                break;
            }
            pre_apply = Some(candidate);
        }
        if matches!(size, BlobDownloadSize::ExistingRow(_))
            || matches!(hash, BlobDownloadHash::ExistingRow(_))
            || matches!(cloud_path, BlobDownloadCloudPath::ExistingRow(_))
        {
            let Some(pre_apply) = pre_apply.cloned() else {
                continue;
            };
            let decls = db.blob_decls();
            let table = pre_apply.table.clone();
            let pk = pre_apply.pk.clone();
            let content = match db
                .call(move |conn| {
                    decls
                        .live_content_for_row(conn, &table, &pk)
                        .map_err(|error| crate::database::DbError(error.to_string()))?
                        .ok_or_else(|| {
                            crate::database::DbError(format!(
                                "pre-apply blob row {table}/{pk} does not exist"
                            ))
                        })
                })
                .await
            {
                Ok(content) => content,
                Err(error) => {
                    warn!(id = %blob.id, namespace = %blob.namespace, %error, "cannot read exact pre-apply blob content, skipping download");
                    all_ok = false;
                    continue;
                }
            };
            if matches!(size, BlobDownloadSize::ExistingRow(_)) {
                size = BlobDownloadSize::Declared(content.plaintext_size);
            }
            if matches!(hash, BlobDownloadHash::ExistingRow(_)) {
                hash = BlobDownloadHash::Declared(content.content_hash);
            }
            if matches!(cloud_path, BlobDownloadCloudPath::ExistingRow(_)) {
                cloud_path = match content.blob.cloud_path {
                    Some(path) => BlobDownloadCloudPath::Declared(path),
                    None => BlobDownloadCloudPath::Absent,
                };
            }
        }
        // The blob's `id`/`namespace`/`cloud_path` come from a row in an incoming
        // changeset authored by any write-capable member. An id or namespace that is
        // not a single safe path token, or a cloud_path that escapes its prefix,
        // would write attacker-chosen bytes outside the store or under a forged
        // cloud key — refuse the blob as bad data before resolving or writing it, the
        // same posture as any other failed blob in this loop. The id check makes
        // local traversal unrepresentable: the destination path below is built from
        // the validated id by coven, so there is no host-supplied local path left to
        // independently re-check. The cloud path is checked below, once resolved,
        // because that resolved value is the one the read keys with.
        if let Err(e) = crate::store_dir::validate_path_token(&blob.namespace) {
            error!(id = %blob.id, namespace = %blob.namespace, "blob namespace is not a safe path token ({e}); refusing");
            all_ok = false;
            continue;
        }
        if let Err(e) = crate::store_dir::validate_path_token(&blob.id) {
            error!(id = %blob.id, namespace = %blob.namespace, "blob id is not a safe path token ({e}); refusing");
            all_ok = false;
            continue;
        }

        let location_record = known_locations
            .iter()
            .find(|record| record.namespace == blob.namespace && record.blob_id == blob.id);

        let source_size = match BlobDownload::resolve_source_size(&blob, size, location_record) {
            Ok(size) => size,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot read blob size, skipping download");
                all_ok = false;
                continue;
            }
        };

        // The author-signed content hash the downloaded plaintext must match. A
        // blob whose row does not carry one is refused rather than downloaded
        // unverified — the hash is the authority that pins the bytes to the row's
        // author, so a missing one is bad data, not a case to skip past.
        let expected_hash = match BlobDownload::resolve_source_hash(&blob, hash, location_record) {
            Ok(hash) => hash,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot read blob content hash, skipping download");
                all_ok = false;
                continue;
            }
        };

        let dest = match store_dir.cache_blob_path(&blob.namespace, &blob.id, &expected_hash) {
            Ok(path) => path,
            Err(error) => {
                error!(id = %blob.id, %error, "cannot build cache blob path; refusing");
                all_ok = false;
                continue;
            }
        };
        let pinned = match store_dir.pinned_blob_path(&blob.namespace, &blob.id, &expected_hash) {
            Ok(path) => path,
            Err(error) => {
                error!(id = %blob.id, %error, "cannot build pinned blob path; refusing");
                all_ok = false;
                continue;
            }
        };

        match cached_in_either_folder(&dest, &pinned, source_size, &expected_hash).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                error!(id = %blob.id, error = %e, "cannot validate local blob; holding");
                all_ok = false;
                continue;
            }
        }

        // The readable key a browsable home stores the blob at. A row repointed at a
        // new blob leaves its cloud path alone, so the change omits the column and the
        // path comes from the pre-apply row instead — which is the same value, that
        // being what "the change did not touch it" means.
        let cloud_path = match BlobDownload::resolve_cloud_path(cloud_path) {
            Ok(path) => path,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot read blob cloud path, skipping download");
                all_ok = false;
                continue;
            }
        };
        if let Some(path) = cloud_path.as_deref() {
            if let Err(e) = crate::store_dir::validate_cloud_path(path) {
                error!(id = %blob.id, cloud_path = %path, "blob cloud_path escapes its prefix ({e}); refusing");
                all_ok = false;
                continue;
            }
        }

        // The exact location from signed metadata or the snapshot-preserved local index.
        let location = match location_record {
            Some(record) => record.location.clone(),
            None if !known_locations.is_empty() => {
                warn!(id = %blob.id, namespace = %blob.namespace, "changeset omits blob location; skipping download");
                all_ok = false;
                continue;
            }
            None => match crate::blob::cache::resolve_blob_location(db, storage, &blob).await {
                Ok(location) => location,
                Err(e) => {
                    warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot resolve blob location, skipping download");
                    all_ok = false;
                    continue;
                }
            },
        };

        match storage
            .read_blob_to_file(
                &blob.namespace,
                &location,
                &blob.id,
                blob.scope.clone(),
                cloud_path.as_deref(),
                source_size,
                &expected_hash,
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
    expected_size: u64,
    expected_hash: &str,
) -> Result<bool, String> {
    for path in [cache, pinned] {
        if crate::local_blob::exists(path).await? {
            let size = crate::local_blob::file_len(path).await?;
            let hash = crate::blob::content_hash_file(path).await?;
            if size == expected_size && hash == expected_hash {
                return Ok(true);
            }
            crate::local_blob::remove_file(path).await?;
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
    /// The membership chain is not anchored to the store's pinned owner — it was
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
                "Update the app to keep syncing — this store was upgraded by a newer device (schema v{min_version}; you have v{local_version})."
            ),
            PullError::MembershipTampered(e) => write!(f, "membership chain tampered: {e}"),
        }
    }
}

impl std::error::Error for PullError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::session::{BlobDecl, SyncedTable};
    use crate::sync::test_helpers::{
        capture_bytes, exec, open_test_db_with_blob, temp_store_dir, MockSyncStorage,
    };

    #[tokio::test]
    async fn cycle_membership_list_failure_aborts_an_unpinned_store() {
        let storage = MockSyncStorage::new();
        storage.fail_membership_listing();
        let (db, _stamper) = Database::open(
            std::path::Path::new(":memory:"),
            Vec::new(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            "test-device".to_string(),
            &[],
        )
        .expect("open database");

        let error = match load_cycle_membership(&storage, &db).await {
            Ok(_) => panic!("a membership LIST failure must abort every store"),
            Err(error) => error,
        };

        assert!(matches!(error, PullError::Storage(_)));
    }

    #[test]
    fn cleanup_intent_derivation_rejects_mismatched_lengths() {
        let old_changes = vec![RowChange {
            table: "files".to_string(),
            op: crate::changeset::ChangeOp::Update,
            columns: vec![Some("file-1".to_string())],
        }];
        let new_changes = Vec::new();
        let conn = rusqlite::Connection::open_in_memory().expect("db");
        let decls = BlobDecls::from_tables(&conn, &[]).expect("decls");

        let err = local_blob_cleanup_intents(&decls, &old_changes, &new_changes)
            .expect_err("mismatched changeset walks fail");

        assert!(matches!(
            err,
            crate::blob::decl::BlobDeclError::ChangesetWalkMismatch {
                old_count: 1,
                new_count: 0
            }
        ));
    }

    #[tokio::test]
    async fn cleanup_guard_rejects_remote_row_re_reference_without_advancing_cursor() {
        let blob_decl = || {
            BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
                .with_id_column("blob_id")
        };
        let source = open_test_db_with_blob(blob_decl());
        exec(
            &source,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Parent', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        )
        .await;
        let changeset = capture_bytes(
            &source,
            &["INSERT INTO note_photos \
               (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
               VALUES ('remote-row', 'n1', 'cover', 9, 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17', 'guarded-blob', \
                       '0000000002000-0000-dev1', '2026-01-01')"],
        )
        .await;
        let changes = crate::changeset::walk(&changeset).expect("walk remote changeset");

        let target = open_test_db_with_blob(blob_decl());
        exec(
            &target,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Parent', NULL, \
                     '0000000001000-0000-dev2', '2026-01-01')",
        )
        .await;
        exec(
            &target,
            "INSERT INTO note_photos \
             (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
             VALUES ('hash-row', 'n1', 'cover', 9, \
                     'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', \
                     'guarded-blob', '0000000001500-0000-dev2', '2026-01-01')",
        )
        .await;
        target
            .call(|conn| {
                conn.execute(
                    "INSERT INTO local_cleanup_intents (namespace, blob_id, content_hash) \
                     VALUES ('photos', 'guarded-blob', 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17')",
                    [],
                )
                .map(|_| ())
                .map_err(crate::database::DbError::from)
            })
            .await
            .unwrap();
        let hash_only_update = target
            .call(|conn| {
                conn.execute(
                    "UPDATE note_photos SET \
                     hash = 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17' \
                     WHERE id = 'hash-row'",
                    [],
                )
                .map(|_| ())
                .map_err(crate::database::DbError::from)
            })
            .await;
        assert!(
            hash_only_update.is_err(),
            "changing only the content hash cannot make the version under cleanup live"
        );
        let direct_write = target
            .call(|conn| {
                conn.execute(
                    "INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
                     VALUES ('direct-row', 'n1', 'cover', 9, 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17', 'guarded-blob', \
                             '0000000002000-0000-dev2', '2026-01-01')",
                    [],
                )
                .map(|_| ())
                .map_err(crate::database::DbError::from)
            })
            .await;
        assert!(
            direct_write.is_err(),
            "the TEMP guard rejects equivalent direct SQL on this connection"
        );
        let schema = {
            let tables = target.synced_tables().to_vec();
            Arc::new(
                target
                    .call(move |conn| {
                        let names: Vec<&str> = tables.iter().map(SyncedTable::name).collect();
                        TableSchema::from_db(conn, &names)
                    })
                    .await
                    .unwrap(),
            )
        };

        let rejected = commit_remote_changeset(
            &target,
            "dev1",
            1,
            &changeset,
            &changes,
            schema.clone(),
            target.receive_wall_ms(),
            &[],
            &[],
        )
        .await
        .unwrap();
        assert!(
            matches!(
                rejected,
                RemoteApplyOutcome::ConstraintConflict(ref tables)
                    if tables == &["note_photos".to_string()]
            ),
            "the TEMP guard rejects the remote row as a constraint conflict"
        );
        assert!(!target
            .call(|conn| {
                conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM note_photos WHERE id = 'remote-row')",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(crate::database::DbError::from)
            })
            .await
            .unwrap());
        assert_eq!(
            target.get_all_sync_cursors().await.unwrap().get("dev1"),
            None
        );

        let (_tmp, store_dir) = temp_store_dir();
        assert!(!local_cleanup::drain(&target, &store_dir).await.unwrap());
        let applied = commit_remote_changeset(
            &target,
            "dev1",
            1,
            &changeset,
            &changes,
            schema,
            target.receive_wall_ms(),
            &[],
            &[],
        )
        .await
        .unwrap();
        assert!(matches!(applied, RemoteApplyOutcome::Applied));
        assert_eq!(
            target.get_all_sync_cursors().await.unwrap().get("dev1"),
            Some(&1)
        );
    }
}
