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
//! `StoreDir::record_device_reset` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::record_device_reset;
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
//! `MemberAccess` belongs to the internal protocol.
//! ```compile_fail
//! use coven::MemberAccess;
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
