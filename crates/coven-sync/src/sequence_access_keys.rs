//! Recording the S3 key a member made in the provider's console as their own
//! (§14.3). The person makes, replaces and deletes keys in the console; coven
//! records only which key id each member uses.

use super::sequence_operations::Request;
use super::SequenceSync;
use crate::{OperationOutcome, SyncError};
use coven_crypto::MemberIdentity;
use coven_database::ClaimWork;
use coven_format::{admin::StoreChange, MemberAccess};
use coven_storage::StorageFailure;
use std::future::Future;
use std::sync::Arc;

impl SequenceSync {
    /// Record a key made in the provider's console as this member's own and
    /// take it (§14.3). The entry setting the access record is journaled
    /// first; `take` then makes this connection and custody hold the key; the
    /// entry is claimed with it. Passes and other writes wait for the
    /// session throughout, so none runs the entry before its key is taken.
    /// A record that already names the key journals nothing, and `take` still
    /// runs. A journaled entry for the same key is the one that runs.
    ///
    /// If `take` fails, the entry ends in this call without claiming, and the
    /// call fails with that error. The entry's claim failing leaves it
    /// journaled for passes to retry.
    pub(crate) async fn use_access_key(
        self: &Arc<Self>,
        access_key_id: String,
        take: impl Future<Output = Result<(), SyncError>> + Send,
    ) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        // Another operation's uncertain claim would hold this entry's back:
        // run that operation now, without waiting out its retry wait, so it
        // lands or ends before this call takes its key (§11.4).
        if let Some(claim) = self.database.current_claim().await? {
            if let ClaimWork::Operation(other) = claim.work {
                self.retry_operation(other).await?;
                self.run_administration(&mut session, &mut ring, &member, other).await?;
            }
        }
        self.resume_administration(&mut session, &mut ring, &member).await?;
        let request = Request::Change(StoreChange::SetAccess {
            member: member.member(),
            access: MemberAccess::S3AccessKey { access_key_id },
        });
        let entry = match self.journaled(|journaled| journaled.carries(&request)).await? {
            Some(id) => Some(id),
            None => match self.validate_request(&session, &member, &request)? {
                Ok(true) => Some(self.start_request_row(request, "operation").await?),
                Ok(false) => None,
                Err(reason) => {
                    return crate::operation_outcome::into_slot(OperationOutcome::Rejected(reason))
                        .map(drop)
                }
            },
        };
        if let Err(error) = take.await {
            // The key was not taken: the entry ends without claiming.
            if let Some(id) = entry {
                if let Err(cleanup) =
                    self.run_administration(&mut session, &mut ring, &member, id).await
                {
                    return Err(SyncError::cleanup(error, cleanup));
                }
            }
            return Err(error);
        }
        match entry {
            Some(id) => self
                .run_administration(&mut session, &mut ring, &member, id)
                .await
                .and_then(crate::operation_outcome::into_slot)
                .map(drop),
            None => Ok(()),
        }
    }

    /// Whether `request` records an S3 key as this member's own that this
    /// connection doesn't sign with: an install that never happened, so the
    /// entry ends without claiming (§14.3).
    pub(crate) async fn awaits_access_key(
        &self,
        member: &MemberIdentity,
        request: &Request,
    ) -> bool {
        let Request::Change(StoreChange::SetAccess {
            member: recorded,
            access: MemberAccess::S3AccessKey { access_key_id },
        }) = request
        else {
            return false;
        };
        *recorded == member.member()
            && self.storage.config().provider() == coven_storage::CloudProvider::S3
            && self.storage.s3_access_key_id().await.as_deref() != Some(access_key_id.as_str())
    }
}

#[cfg(test)]
#[path = "sequence_access_keys_tests.rs"]
mod tests;
