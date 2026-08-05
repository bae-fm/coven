use super::{
    candidate_records::*,
    publication_state::{MergeAbandonmentOutcome, PreparedStoreWriteState},
    StoreDatabase,
};
use crate::database::*;
use crate::protocol::objects::{ExactObjectRef, PreparedExactObject};
use crate::protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use crate::protocol::store_commit::{
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistrationRef,
};
use crate::write::{WriteId, WriteResolution, WriteStatus};
use rusqlite::OptionalExtension;

mod abandonment;
mod cleanup;
mod terminal;

impl StoreDatabase {}
