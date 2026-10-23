//! The application surface cannot bypass store owners or their journals.
//! Each example names one operation, so a failure cannot hide another leak.
//!
//! `StoreDir::database_path` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::database_path;
//! ```
//!
//! `StoreDir::input_database_path` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::input_database_path;
//! ```
//!
//! `StoreDir::unfinished_device_reset` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::unfinished_device_reset;
//! ```
//!
//! `StoreDir::settings` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::settings;
//! ```
//!
//! `StoreDir::lock_exclusive` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_exclusive;
//! ```
//!
//! `StoreDir::lock_for_open` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_for_open;
//! ```
//!
//! `StoreDir::lock_read_only` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_read_only;
//! ```
//!
//! `StoreDir::lock_cache_write` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_cache_write;
//! ```
//!
//! `StoreDir::exclude_cache_writes` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::exclude_cache_writes;
//! ```
//!
//! `StoreDir::lock_for_deletion` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_for_deletion;
//! ```
//!
//! `StoreDir::verify_lock` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::verify_lock;
//! ```
//!
//! `StoreDir::owned_file` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::owned_file;
//! ```
//!
//! `StoreDir::file` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::file;
//! ```
//!
//! `StoreLayout::begin_bootstrap` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreLayout::begin_bootstrap;
//! ```
//!
//! `OAuthClients::refresh` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::OAuthClients::refresh;
//! ```
//!
//! `FileRef::reference` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::FileRef::reference;
//! ```
//!
//! `FileRef::encode` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::FileRef::encode;
//! ```
//!
//! `FileRef::decode` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::FileRef::decode;
//! ```
//!
//! `UploadQueue::from_state` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::UploadQueue::from_state;
//! ```
//!
//! `MigrationChange` belongs to the internal protocol.
//! ```compile_fail
//! use coven::MigrationChange;
//! ```
//!
//! `MigrationOutcome` belongs to the internal protocol.
//! ```compile_fail
//! use coven::MigrationOutcome;
//! ```

//!
//! SQL callback types are available without another application dependency.
//! ```
//! use coven::{SqliteError as Error, SqliteResult as Result, Params, Row, ToSql, types::Value};
//! fn map(row: &Row<'_>) -> Result<Value> { row.get(0) }
//! fn query<P: Params>(sql: coven::SqlReadContext<'_>, params: P) -> Result<Value> {
//!     sql.query_row("SELECT ?1", params, map)
//! }
//! fn bind(value: &dyn ToSql) { let _ = value.to_sql(); }
//! let missing: Result<()> = Err(Error::QueryReturnedNoRows);
//! assert!(matches!(missing, Err(Error::QueryReturnedNoRows)));
//! ```
//!
//! Raw connections are outside coven's SQL surface.
//! ```compile_fail
//! use coven::Connection;
//! ```
//!
//! Helpers absent from coven's signatures are not added to its surface.
//! ```compile_fail
//! use coven::OptionalExtension;
//! ```

#[test]
fn app_file_replacement_preserves_open_readers_and_uses_the_format_chunk_size() {
    use crate::{write_atomic, CHUNK_SIZE};
    use std::io::Read;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    write_atomic(&path, b"old").unwrap();
    let mut reader = std::fs::File::open(&path).unwrap();
    let bytes = vec![7; CHUNK_SIZE + 3];
    write_atomic(&path, &bytes).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(
        bytes
            .chunks(CHUNK_SIZE)
            .map(<[u8]>::len)
            .collect::<Vec<_>>(),
        [65536, 3]
    );
    let mut old = Vec::new();
    reader.read_to_end(&mut old).unwrap();
    assert_eq!(old, b"old");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);

    let occupied = directory.path().join("occupied");
    std::fs::create_dir(&occupied).unwrap();
    let error: crate::FileError =
        write_atomic(&occupied, b"cannot replace a directory").unwrap_err();
    assert!(!error.installed_new_bytes());
    assert!(occupied.is_dir());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
}

#[cfg(feature = "test-utils")]
#[test]
fn app_tests_can_find_open_files_and_assert_they_are_released() {
    use crate::{assert_no_open_files_under, open_files_under, write_atomic};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    write_atomic(&path, b"settings").unwrap();
    let path = path.canonicalize().unwrap();
    assert_no_open_files_under(directory.path());
    let file = std::fs::File::open(&path).unwrap();
    assert_eq!(open_files_under(directory.path()), vec![path]);
    assert!(std::panic::catch_unwind(|| assert_no_open_files_under(directory.path())).is_err());
    drop(file);
    assert_no_open_files_under(directory.path());
}

/// What an app awaits from a call that bootstraps a device or runs one of
/// coven's operations stays small: the work under it reaches tens of KiB, and
/// each such call holds that on the heap. An app's own future embeds these
/// calls' futures, and a runtime polls them on 2 MiB worker threads. The bound
/// is 8 KiB, over twice the largest of them (`open`, 3 KiB) and under half of
/// any unboxed operation (16–25 KiB).
#[test]
fn bootstrap_and_operation_futures_stay_small() {
    use crate::{
        BootstrapStatus, CovenBuilder, CovenHandle, InviteAccess, InviteDelivery, InviteId,
        InviteTarget, JoinRequest, MemberId, MemberPublicKeys, OperationId, S3Credentials,
        SavedRestoreCode, StoreId,
    };
    use std::future::Future;
    use tokio::sync::watch::Receiver;

    /// The size of the future `_call` returns, without calling it.
    fn size<A, F: Future>(_call: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    type Status = fn(BootstrapStatus);
    let sizes = [
        (
            "restore_from_code",
            size(
                |(b, c, s, d, o, x): (
                    CovenBuilder,
                    &'static str,
                    Option<S3Credentials>,
                    &'static str,
                    Status,
                    &'static Receiver<bool>,
                )| crate::restore_from_code(b, c, s, d, o, x),
            ),
        ),
        (
            "restore_from_keychain",
            size(
                |(b, c, s, d, o, x): (
                    CovenBuilder,
                    &'static SavedRestoreCode,
                    Option<S3Credentials>,
                    &'static str,
                    Status,
                    &'static Receiver<bool>,
                )| crate::restore_from_keychain(b, c, s, d, o, x),
            ),
        ),
        (
            "join_with_invite",
            size(
                |(b, c, d, o, x): (
                    CovenBuilder,
                    &'static str,
                    &'static str,
                    Status,
                    &'static Receiver<bool>,
                )| crate::join_with_invite(b, c, d, o, x),
            ),
        ),
        (
            "create_store",
            size(|(b, n): (CovenBuilder, &'static str)| b.create_store(n)),
        ),
        ("open", size(|(b, s): (CovenBuilder, StoreId)| b.open(s))),
        (
            "open_read_only",
            size(|(b, s): (CovenBuilder, StoreId)| b.open_read_only(s)),
        ),
        (
            "reset_device",
            size(|(b, s, d): (CovenBuilder, StoreId, &'static str)| b.reset_device(s, d)),
        ),
        (
            "restore_device",
            size(
                |(b, s, c, k, d, o, x): (
                    CovenBuilder,
                    StoreId,
                    &'static str,
                    Option<S3Credentials>,
                    &'static str,
                    Status,
                    &'static Receiver<bool>,
                )| b.restore_device(s, c, k, d, o, x),
            ),
        ),
        (
            "create_invite",
            size(
                |(h, t, a, d): (
                    &'static CovenHandle,
                    InviteTarget,
                    InviteAccess,
                    InviteDelivery,
                )| h.create_invite(t, a, d),
            ),
        ),
        (
            "approve_join_request",
            size(|(h, r): (&'static CovenHandle, &'static JoinRequest)| h.approve_join_request(r)),
        ),
        (
            "decline_join_request",
            size(|(h, r): (&'static CovenHandle, &'static JoinRequest)| h.decline_join_request(r)),
        ),
        (
            "cancel_invite",
            size(|(h, i): (&'static CovenHandle, &'static InviteId)| h.cancel_invite(i)),
        ),
        (
            "retry_pending_operation",
            size(|(h, o): (&'static CovenHandle, OperationId)| h.retry_pending_operation(o)),
        ),
        (
            "reload_from_snapshot",
            size(|h: &'static CovenHandle| h.reload_from_snapshot()),
        ),
        (
            "reset_store",
            size(|h: &'static CovenHandle| h.reset_store()),
        ),
        (
            "replace_member_keys",
            size(|h: &'static CovenHandle| h.replace_member_keys()),
        ),
        (
            "replace_member_keys_for",
            size(
                |(h, m, k): (&'static CovenHandle, &'static MemberId, MemberPublicKeys)| {
                    h.replace_member_keys_for(m, k)
                },
            ),
        ),
    ];
    let large: Vec<_> = sizes
        .into_iter()
        .filter(|(_, size)| *size > 8 * 1024)
        .collect();
    assert!(large.is_empty(), "futures over 8 KiB: {large:?}");
}
