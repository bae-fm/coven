use super::*;

/// Each attempted native call in the in-memory provider, before fault injection.
/// This provider has one page per listing and no implicit metadata lookups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryRequest {
    /// Read a whole object or an explicit byte range (including missing paths).
    Read(ObjectPath, Option<ByteRange>),
    /// Enumerate one prefix in one native call.
    List(ObjectPrefix),
    /// Publish an immutable object.
    Create(ObjectPath),
    /// Replace an object.
    Replace(ObjectPath),
    /// Query exact metadata.
    Status(ObjectPath),
    /// Delete an object.
    Delete(ObjectPath),
    /// Account, permission or multipart control/part call.
    Other(&'static str),
}

impl MemoryStorage {
    /// Attempted calls on this connection; `for_device` starts a separate trace.
    pub fn request_trace(&self) -> Vec<MemoryRequest> {
        self.provider
            .request_trace
            .lock()
            .expect("request trace poisoned")
            .clone()
    }
}

impl MemoryStorage {
    /// Hold one listing's reply after taking its snapshot. The test is notified
    /// when the result is fixed, and can mutate remote state before releasing it.
    pub async fn hold_next_listing(
        &self,
        prefix: ObjectPrefix,
        listed: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_listing.is_none(), "a listing is already held");
        state.held_listing = Some(HeldRequest {
            prefix,
            listed,
            resume,
        });
    }
    /// Hold a single-request creation before publication, notifying its observer.
    pub async fn hold_next_creation(
        &self,
        prefix: ObjectPrefix,
        started: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_creation.is_none(), "a creation is already held");
        state.held_creation = Some(HeldRequest {
            prefix,
            listed: started,
            resume,
        });
    }
}

/// A provider permission request held before changing its durable grant.
pub(super) struct HeldAccess {
    account: String,
    grant: bool,
    started: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

impl MemoryStorage {
    /// Hold one account request before it reaches the provider's grant state.
    pub async fn hold_next_access(
        &self,
        account: &str,
        grant: bool,
        started: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(
            state.held_access.is_none(),
            "an access request is already held"
        );
        state.held_access = Some(HeldAccess {
            account: account.into(),
            grant,
            started,
            resume,
        });
    }
}

impl MemoryProvider {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "a send fails only when the receiver is gone, which this names"
    )]
    pub(super) async fn hold_access(&self, account: &str, grant: bool) {
        let held = {
            let mut state = self.state.lock().await;
            if state
                .held_access
                .as_ref()
                .is_some_and(|h| h.account == account && h.grant == grant)
            {
                state.held_access.take()
            } else {
                None
            }
        };
        if let Some(held) = held {
            let _ = held.started.send(());
            held.resume
                .await
                .expect("held access request was not released");
        }
    }
}
