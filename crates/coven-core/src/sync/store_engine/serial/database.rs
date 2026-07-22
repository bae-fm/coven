mod acknowledgements;
mod branch_resolution;
mod candidate_cleanup;

use std::collections::BTreeSet;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};

use crate::blob::decl::BlobDecls;
use crate::database::{
    begin_remote_candidate_nonactivation_on, candidate_graph_exact_objects,
    finish_outbound_store_ack_on, load_activated_registration_on, load_outbound_store_ack_on,
    load_protocol_inert_object_on, load_remote_object_on, parse_prepared_serial_candidate,
    parse_prepared_serial_write_state, persist_exact_remote_object_on,
    required_store_root_authority_on, store_serial_predecessor_on, update_remote_object_on,
    CandidateCleanupObject, Database, DbError, OutboundStoreAckActivation,
    PreparedWriteMaterialization, StoreWriteBase, StoreWriteRouting, LOCAL_DEVICE_ID_STATE_KEY,
    SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY,
};
use crate::sync::gate::Gates;
use crate::sync::membership::SerialMembershipState;
use crate::sync::remote_object::{
    remote_object_id, CandidateNonactivationProof, VerifiedCandidateNonactivation,
};
use crate::sync::session::SyncedTable;
use crate::sync::storage::VersionedObject;
use crate::sync::store_commit::{
    ObjectHash, StoreAckRef, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreSerialHead, StoreSerialHeadState, StoreSerialPredecessor, SERIAL_STREAM_ID,
};
use crate::sync::store_outbound::{
    PreparedSerialStoreOperationCommit, PreparedStoreOperationCommit, StoreOutboundError,
};
use crate::write::{
    PendingBranchId, PublishedPosition, WriteId, WriteReceipt, WriteResolution, WriteStatus,
};
use crate::WritePolicy;

#[derive(Clone, Copy)]
pub(super) struct SerialDatabase<'a> {
    database: &'a Database,
}

impl<'a> SerialDatabase<'a> {
    pub(super) fn new(database: &'a Database) -> Self {
        Self { database }
    }

    pub(super) async fn required_device_id(self) -> Result<String, StoreOutboundError> {
        self.database
            .get_protocol_state(LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| StoreOutboundError::Database(error.to_string()))?
            .ok_or(StoreOutboundError::MissingState {
                key: LOCAL_DEVICE_ID_STATE_KEY,
            })
    }

    pub(super) async fn should_stop_before_pull(
        self,
        authoritative_head: Option<StoreBatchCommitRef>,
    ) -> Result<bool, DbError> {
        let Some(branch) = self.database.unresolved_serial_branch().await? else {
            return Ok(false);
        };
        let stale = branch.base != authoritative_head;
        if !branch.conflicted && stale {
            let authoritative_predecessor = self.exact_predecessor(authoritative_head).await?;
            self.database
                .mark_serial_branch_conflict(
                    branch.branch_id,
                    branch.base,
                    authoritative_predecessor,
                )
                .await?;
        }
        Ok(branch.conflicted || stale)
    }

    pub(super) async fn required_membership(self) -> Result<SerialMembershipState, DbError> {
        self.database
            .serial_authorization_state()
            .await?
            .map(|state| state.membership)
            .ok_or_else(|| DbError::Message("materialized Serial authorization is absent".into()))
    }
}
