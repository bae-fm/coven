//! Recording the S3 key a member made in the provider's console as their own
//! (§14.3). The person makes, replaces and deletes keys in the console; coven
//! records only which key id each member uses.

use super::SequenceSync;
use crate::SyncError;
use coven_format::{admin::StoreChange, MemberAccess};
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
}
