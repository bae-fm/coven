//! Recording the S3 key a member made in the provider's console as their own
//! (§14.3). The person makes, replaces and deletes keys in the console; coven
//! records only which key id each member uses.

use super::sequence_operations::Request;
use super::SequenceSync;
use crate::{OperationOutcome, SyncError};
use coven_crypto::MemberIdentity;
use coven_database::ClaimWork;
use coven_format::{admin::StoreChange, MemberAccess};
use coven_foundation::task::CarriesBug as _;
use coven_storage::StorageFailure;
use std::future::Future;
use std::sync::Arc;

impl SequenceSync {
    /// Record a key made in the provider's console as this member's own and
    /// take it (§14.3). This owner's connection signs with the key, which
    /// passed its provider check. The entry setting the access record is
    /// journaled first; `take` then makes custody, and the store's
    /// connection, hold the key; then the entry and an earlier claim that
    /// holds it back run with the key. Passes and other writes wait for the
    /// session throughout, so none runs the entry before its key is taken.
    /// A record that already names the key journals nothing, and `take`
    /// still runs. A journaled entry for the same key is the one that runs.
    /// Answers the key id the record named, when the call has an entry.
    ///
    /// Before `take` succeeds the call fails having changed nothing: an
    /// entry it journaled ends without claiming. Once the key is taken, a
    /// storage failure leaves the entry journaled with the failure in its
    /// pending record, for passes to retry with the key, and the call
    /// succeeds. Every other failure still fails it: the caught-up history
    /// refusing the entry or a removal applying (§14.1), and a failure of
    /// this device's database, a bug or a shutdown.
    pub(crate) async fn use_access_key(
        self: &Arc<Self>,
        access_key_id: String,
        take: impl Future<Output = Result<(), SyncError>> + Send,
    ) -> Result<Option<String>, SyncError> {
        let mut session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        let request = Request::Change(StoreChange::SetAccess {
            member: member.member(),
            access: MemberAccess::S3AccessKey { access_key_id: access_key_id.clone() },
        });
        // Validating an entry needs this member's record, and so does
        // naming the key it replaces.
        let recorded = session
            .history
            .state
            .active_member(&member.member())
            .ok_or(SyncError::Removed(coven_database::Removal::Member))?
            .access
            .clone();
        let earlier = self.journaled(|journaled| journaled.carries(&request)).await?;
        let entry = match earlier {
            Some(id) => Some(id),
            None => match self.validate_request(&session, &member, &request)? {
                Ok(true) => Some(self.start_request_row(request, "operation").await?),
                Ok(false) => None,
                Err(reason) => return Err(SyncError::Rejected(reason)),
            },
        };
        let replaced = match (entry, recorded) {
            (Some(_), Some(MemberAccess::S3AccessKey { access_key_id: held }))
                if held != access_key_id =>
            {
                Some(held)
            }
            _ => None,
        };
        if let Err(error) = take.await {
            // The key was not taken: the entry this call journaled ends
            // without claiming. One an earlier call journaled stays, and a
            // pass ends it so while custody holds another key.
            if let Some(id) = entry.filter(|_| earlier.is_none()) {
                let ended =
                    self.database.finish_operation_with_outcome(id, OperationOutcome::Satisfied);
                if let Err(cleanup) = ended.await {
                    return Err(SyncError::cleanup(error, cleanup.into()));
                }
            }
            return Err(error);
        }
        let Some(id) = entry else {
            return Ok(None);
        };
        // The key is taken. An earlier claim holds the entry's back: run its
        // operation now, without waiting out its retry wait, with the key.
        // Another key's entry ends without claiming (§11.4).
        let claimed = async {
            if let Some(claim) = self.database.current_claim().await? {
                match claim.work {
                    ClaimWork::Operation(other) if other != id => {
                        self.retry_operation(other).await?;
                        self.run_administration(&mut session, &mut ring, &member, other).await?;
                    }
                    _ => {}
                }
            }
            self.run_administration(&mut session, &mut ring, &member, id)
                .await
                .and_then(crate::operation_outcome::into_slot)
        };
        match claimed.await {
            Ok(_) => Ok(replaced),
            // Storage failed or left uncertain a claim, this entry's or the
            // earlier one it waits behind: the operation's pending record
            // keeps the failure, and passes claim the entry with the key.
            Err(SyncError::Storage(error)) if error.bug().is_none() => {
                tracing::info!("the new access key's entry waits for a pass: {error}");
                Ok(replaced)
            }
            // A refusal of the caught-up history, which comes only once this
            // member or device is removed, a removal, and a failure of this
            // device itself, its database, a bug or a shutdown, reach the
            // caller with the key taken.
            Err(error) => Err(error),
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
