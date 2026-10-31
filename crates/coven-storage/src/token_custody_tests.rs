use super::*;
use crate::{StorageFailure, TokenCustody};
use coven_crypto::SecretText;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Custody that hands out `refreshed-N` once per rejection, or fails.
pub(crate) struct CountingCustody {
    pub(crate) refreshes: AtomicUsize,
    fail: bool,
}
impl CountingCustody {
    pub(crate) fn new(fail: bool) -> Arc<Self> {
        Arc::new(Self { refreshes: AtomicUsize::new(0), fail })
    }
    pub(crate) fn count(&self) -> usize {
        self.refreshes.load(Ordering::SeqCst)
    }
}
#[async_trait::async_trait]
impl TokenCustody for CountingCustody {
    async fn replace(&self, rejected: &OAuthTokens) -> Result<OAuthTokens, StorageError> {
        let n = self.refreshes.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(StorageFailure::File.with_source("custody refused the replacement"));
        }
        Ok(OAuthTokens {
            access_token: SecretText::new(format!("refreshed-{n}")),
            ..rejected.clone()
        })
    }
}

fn tokens(token: &str) -> OAuthTokens {
    OAuthTokens {
        client_id: "client".into(),
        access: crate::OAuthAccess::OneDrive,
        access_token: SecretText::new(token.into()),
        refresh_token: Some(SecretText::new("refresh".into())),
    }
}

/// Custody whose replacement waits for `release`, counting calls.
struct Gated {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    fail: bool,
}
impl Gated {
    fn new(fail: bool) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            fail,
        })
    }
}
#[async_trait::async_trait]
impl TokenCustody for Gated {
    async fn replace(&self, rejected: &OAuthTokens) -> Result<OAuthTokens, StorageError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        if self.fail {
            return Err(StorageFailure::File.with_source("custody write failed"));
        }
        Ok(OAuthTokens {
            access_token: SecretText::new(format!("refreshed-{n}")),
            ..rejected.clone()
        })
    }
}

#[tokio::test]
async fn concurrent_rejections_of_one_token_share_one_replacement() {
    let custody = Gated::new(false);
    let held = Arc::new(HeldTokens::new(tokens("token"), custody.clone()));
    let used = held.current().await;
    let first = tokio::spawn({
        let (held, used) = (held.clone(), used.clone());
        async move { held.rejected(&used).await }
    });
    custody.entered.notified().await;
    let second = tokio::spawn({
        let held = held.clone();
        async move { held.rejected(&used).await }
    });
    tokio::task::yield_now().await;
    custody.release.add_permits(1);
    for result in [first.await.unwrap(), second.await.unwrap()] {
        assert_eq!(result.unwrap().access_token.as_str(), "refreshed-0");
    }
    assert_eq!(custody.calls.load(Ordering::SeqCst), 1);
    assert_eq!(held.current().await.access_token.as_str(), "refreshed-0");
}

#[tokio::test]
async fn a_rejection_of_an_already_replaced_token_does_not_refresh_again() {
    let custody = Gated::new(false);
    let held = HeldTokens::new(tokens("token"), custody.clone());
    held.install(tokens("installed")).await;
    let replacement = held.rejected(&tokens("token")).await.unwrap();
    assert_eq!(replacement.access_token.as_str(), "installed");
    assert_eq!(custody.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tokens_installed_during_a_replacement_are_not_overwritten_by_it() {
    let custody = Gated::new(false);
    let held = Arc::new(HeldTokens::new(tokens("token"), custody.clone()));
    let rejected = tokio::spawn({
        let held = held.clone();
        async move { held.rejected(&tokens("token")).await }
    });
    custody.entered.notified().await;
    let installed = tokio::spawn({
        let held = held.clone();
        async move { held.install(tokens("upgraded")).await }
    });
    tokio::task::yield_now().await;
    custody.release.add_permits(1);
    rejected.await.unwrap().unwrap();
    installed.await.unwrap();
    assert_eq!(held.current().await.access_token.as_str(), "upgraded");
}

#[tokio::test]
async fn a_failed_replacement_installs_nothing_and_reaches_the_caller() {
    let custody = Gated::new(true);
    custody.release.add_permits(1);
    let held = HeldTokens::new(tokens("token"), custody.clone());
    let error = held.rejected(&tokens("token")).await.err().unwrap();
    assert_eq!(error.failure(), StorageFailure::File);
    assert_eq!(held.current().await.access_token.as_str(), "token");
}
