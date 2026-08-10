use coven_foundation::store_dir::StoreDir;
use coven_protocol::store_commit::ObjectHash;
use rusqlite::Connection;

use super::payload_spool::{
    read_payload_blocking, read_verified_payload_blocking, write_payload_blocking,
    PayloadSpoolError,
};

/// One Store's row connection and matching payload directory.
///
/// A record whose bytes live in the spool is half a row and half a file, so
/// record operations carry both halves as one scoped value. Operations that
/// touch rows alone continue to take the connection in their private SQL leaf.
#[derive(Clone, Copy)]
pub(crate) struct StoreRecords<'store> {
    pub(crate) conn: &'store Connection,
    pub(crate) store_dir: &'store StoreDir,
}

impl<'store> StoreRecords<'store> {
    pub(crate) fn new(conn: &'store Connection, store_dir: &'store StoreDir) -> Self {
        Self { conn, store_dir }
    }

    pub(crate) fn payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        read_payload_blocking(self.store_dir, hash)
    }

    pub(crate) fn verified_payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        read_verified_payload_blocking(self.store_dir, hash)
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        write_payload_blocking(self.store_dir, bytes)
    }
}

/// One Store transaction and its matching payload directory.
///
/// Payloads land before the row naming them commits, while ownership claims
/// land in this transaction. Keeping both borrows together prevents a record
/// mutation from using another Store's payload directory.
#[derive(Clone, Copy)]
pub(crate) struct StoreRecordTransaction<'store, 'connection> {
    pub(crate) transaction: &'store rusqlite::Transaction<'connection>,
    pub(crate) store_dir: &'store StoreDir,
}

impl<'store, 'connection> StoreRecordTransaction<'store, 'connection> {
    pub(crate) fn new(
        transaction: &'store rusqlite::Transaction<'connection>,
        store_dir: &'store StoreDir,
    ) -> Self {
        Self {
            transaction,
            store_dir,
        }
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        write_payload_blocking(self.store_dir, bytes)
    }
}
