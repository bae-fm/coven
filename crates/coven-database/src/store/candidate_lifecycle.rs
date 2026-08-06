use super::{
    candidate_records::*,
    publication_state::{MergeAbandonmentOutcome, PreparedStoreWriteState},
    StoreDatabase,
};
use crate::*;
use coven_protocol::objects::{ExactObjectRef, PreparedExactObject};
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use coven_protocol::store_commit::{
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistrationRef,
};
use coven_protocol::write::{WriteId, WriteResolution, WriteStatus};
use rusqlite::OptionalExtension;

mod abandonment;
mod cleanup;
mod terminal;

impl StoreDatabase {}
