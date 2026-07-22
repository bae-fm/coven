mod candidate_cleanup;

use std::collections::BTreeSet;

use crate::database::{
    begin_remote_candidate_nonactivation_on, candidate_graph_exact_objects,
    load_activated_registration_on, load_remote_object_on, parse_prepared_serial_candidate,
    required_store_root_authority_on, update_remote_object_on, CandidateCleanupObject, Database,
    DbError, StoreWriteBase, LOCAL_DEVICE_ID_STATE_KEY, SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY,
};
use crate::sync::membership::SerialMembershipState;
use crate::sync::remote_object::remote_object_id;
use crate::sync::store_commit::{
    ObjectHash, StoreBatchCommitRef, StoreCommitCoord, StoreSerialHeadState,
};
use crate::sync::store_outbound::StoreOutboundError;
use crate::write::{PendingBranchId, WriteId, WriteResolution, WriteStatus};

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
            let authoritative_predecessor = self
                .database
                .exact_serial_predecessor(authoritative_head)
                .await?;
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
