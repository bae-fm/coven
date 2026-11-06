//! Recording the S3 key a member made in the provider's console as their own
//! (§14.3). The person makes, replaces and deletes keys in the console; coven
//! records only which key id each member uses.

use super::sequence_operations::{Request, SequenceOperation};
use super::SequenceSync;
use crate::SyncError;
use coven_database::OperationId;
use coven_format::{admin::StoreChange, MemberAccess};
use coven_foundation::id_source::MemberId;
use coven_storage::StorageFailure;
use std::sync::Arc;

impl SequenceSync {
    /// Record a key made in the provider's console as this member's own (§14.3).
    pub async fn record_access_key(
        self: &Arc<Self>,
        access_key_id: String,
    ) -> Result<(), SyncError> {
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?.member();
        self.administer(StoreChange::SetAccess {
            member,
            access: MemberAccess::S3AccessKey { access_key_id },
        })
        .await
    }

    /// The operation the journal runs recording `access` as `member`'s, if
    /// any (§14.3).
    pub(crate) async fn access_operation(
        &self,
        member: MemberId,
        access: &MemberAccess,
    ) -> Result<Option<OperationId>, SyncError> {
        for record in self.database.operations().await? {
            if record.kind != coven_database::operation_kind::SEQUENCE {
                continue;
            }
            let work: SequenceOperation = serde_json::from_slice(&record.data)?;
            if let Request::Change(StoreChange::SetAccess { member: recorded, access: journaled }) =
                &work.request
            {
                if *recorded == member && journaled == access {
                    return Ok(Some(record.id));
                }
            }
        }
        Ok(None)
    }

    /// Whether the journal runs an operation recording `access` as this
    /// member's (§14.3).
    pub(crate) async fn journals_access(&self, access: &MemberAccess) -> Result<bool, SyncError> {
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?.member();
        Ok(self.access_operation(member, access).await?.is_some())
    }
}
