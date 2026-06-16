//! Browser-only storage setup for the wasm build.
//!
//! The wasm [`crate::database::Database`] opens its connection against SQLite's
//! default VFS. [`install_browser_storage`] makes that default the opfs-sahpool
//! VFS, which backs each SQLite file with a per-file OPFS
//! `FileSystemSyncAccessHandle`, so the database survives a page reload. The host
//! calls it once on the DB Worker before opening any `Database`.
//!
//! The sync access handles the VFS uses exist only on a dedicated Worker (not the
//! main thread), so all DB code — install and every `Database` method — runs on
//! that Worker.

use sqlite_wasm_vfs::sahpool::{install as install_opfs_sahpool, OpfsSAHPoolCfgBuilder};

use crate::database::DbError;

/// How many OPFS files the VFS pool may hold. Each open library is one database
/// file plus, transiently, its rollback journal, so the pool must seat several
/// times the number of libraries a page opens at once. The crate default of 6 is
/// too tight — a handful of libraries exhausts it and the next open fails with
/// `SQLITE_CANTOPEN` — so reserve headroom for a real multi-library page.
const OPFS_POOL_CAPACITY: u32 = 32;

/// Install the opfs-sahpool VFS and make it SQLite's default, so connections
/// opened by name become durable in the browser's Origin Private File System.
///
/// Idempotent: the underlying install registers the VFS only if one of the same
/// name is not already registered, and a static lock serializes concurrent
/// installs — so a second call (e.g. a Worker that reinitializes) returns the
/// existing registration without error. Run once before opening any
/// [`crate::database::Database`].
///
/// Errors if called off a dedicated Worker (the OPFS sync access handles are
/// unavailable on the main thread) or if OPFS itself is unreachable.
pub async fn install_browser_storage() -> Result<(), DbError> {
    let cfg = OpfsSAHPoolCfgBuilder::new()
        .initial_capacity(OPFS_POOL_CAPACITY)
        .build();
    install_opfs_sahpool::<sqlite_wasm_rs::WasmOsCallback>(&cfg, true)
        .await
        .map_err(|e| DbError(format!("failed to install opfs-sahpool VFS: {e}")))?;
    tracing::debug!("opfs-sahpool VFS installed and set as the default SQLite VFS");
    Ok(())
}
