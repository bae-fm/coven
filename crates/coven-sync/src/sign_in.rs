//! Custody of an OAuth sign-in: coven's session custody until setup or a
//! restore publishes the store, or a join saves its invite code; this
//! installation's device custody after it (§13.2, §20.2, E5, E10). A token is
//! used until the provider rejects it; its one refresh is committed here
//! before any connection uses it.

use crate::{commit_credentials, CredentialKeychains, CredentialRollback, SavedName, SyncError};
use coven_crypto::custody::{PersonEntries, StoreKeychain};
use coven_format::codes::RestoreCode;
use coven_storage::providers::OAuthTokenRefresh;
use coven_storage::{
    providers::OAuthClients, ConnectionCredentials, OAuthTokens, StorageCredentials, StorageError,
    StorageFailure, StorageSettings, TokenCustody,
};
use std::sync::{Arc, Mutex};

/// This installation's committed storage credentials. Every write of them
/// goes through [`Self::write`], so a refresh commits its replacement only
/// while custody still holds the tokens the provider rejected.
pub struct DeviceCredentials {
    keychains: CredentialKeychains,
    oauth: Option<OAuthClients>,
    writes: Mutex<()>,
    refreshing: tokio::sync::Mutex<()>,
}

impl DeviceCredentials {
    /// Compose once per installation, beside its device-only entries and the
    /// person's entries: for an open store, or for a join from the moment it keeps
    /// its sign-in, which the joined store's handle goes on using (§13.2).
    pub fn new(
        keychain: Arc<StoreKeychain>,
        person: Arc<PersonEntries>,
        oauth: Option<OAuthClients>,
    ) -> Self {
        Self {
            keychains: CredentialKeychains::new(keychain, person),
            oauth,
            writes: Mutex::new(()),
            refreshing: tokio::sync::Mutex::new(()),
        }
    }

    /// Change committed credentials, serialized with every refresh's commit.
    pub fn write<T>(
        &self,
        change: impl FnOnce(&CredentialKeychains) -> Result<T, SyncError>,
    ) -> Result<T, SyncError> {
        let _write = self.writes.lock().expect("credential writes poisoned");
        change(&self.keychains)
    }

    /// The saved location with these credentials; None when either is absent.
    pub fn connection(
        &self,
        settings: &StorageSettings,
    ) -> Result<Option<ConnectionCredentials>, SyncError> {
        self.keychains.connection(settings)
    }

    /// The OAuth tokens committed here, which a new connection starts with.
    /// Refuses when no OAuth sign-in is committed.
    pub fn tokens(&self) -> Result<OAuthTokens, SyncError> {
        self.write(held_tokens)
    }

    /// Save the restore code with the store's name beside the OAuth sign-in
    /// committed here, leaving that sign-in as it is: a join publishes this
    /// way, since it has kept its sign-in here while it waited, refreshes
    /// included (§13.2). Refuses when no OAuth sign-in is committed.
    pub fn commit_code(
        &self,
        code: &RestoreCode,
        name: &SavedName,
    ) -> Result<CredentialRollback, SyncError> {
        self.write(|keychains| {
            held_tokens(keychains)?;
            crate::credential_custody::commit_account_code(keychains, code, name)
        })
    }
}

pub(crate) fn held_tokens(keychains: &CredentialKeychains) -> Result<OAuthTokens, SyncError> {
    let bytes = keychains
        .storage_credentials()?
        .ok_or(StorageFailure::Authentication.with_source("no sign-in is in device custody"))?;
    match StorageCredentials::decode(bytes.as_bytes())? {
        StorageCredentials::OAuth(tokens) => Ok(tokens),
        _ => Err(StorageFailure::InvalidConfiguration
            .with_source("device custody holds no OAuth sign-in")
            .into()),
    }
}

#[async_trait::async_trait]
impl TokenCustody for DeviceCredentials {
    async fn replace(&self, rejected: &OAuthTokens) -> Result<OAuthTokens, StorageError> {
        let _one = self.refreshing.lock().await;
        let held = held_tokens(&self.keychains).map_err(storage_error)?;
        if held.access_token.as_str() != rejected.access_token.as_str() {
            return Ok(held);
        }
        let refreshed = refresh(self.oauth.as_ref(), &held).await?;
        self.commit_refresh(rejected, refreshed)
    }
}

impl DeviceCredentials {
    /// Commit `refreshed` in place of `rejected`, unless custody holds other
    /// tokens by now, which then win: a sign-in committed meanwhile, or
    /// another refresh of the same rejection.
    fn commit_refresh(
        &self,
        rejected: &OAuthTokens,
        refreshed: OAuthTokens,
    ) -> Result<OAuthTokens, StorageError> {
        self.write(|keychain| {
            let held = held_tokens(keychain)?;
            if held.access_token.as_str() != rejected.access_token.as_str() {
                return Ok(held);
            }
            commit_credentials(keychain, &StorageCredentials::OAuth(refreshed.clone()), None)?;
            Ok(refreshed)
        })
        .map_err(storage_error)
    }
}

/// A sign-in held for this session only, until setup or bootstrap publishes
/// it to device custody: with the store, or for a join once it saves its
/// invite code. It then refreshes there (§13.2, §20.2).
pub struct SessionSignIn {
    /// The tokens this session holds, or published; with the device custody
    /// holding them since publication.
    state: Mutex<(OAuthTokens, Option<Arc<DeviceCredentials>>)>,
    oauth: Option<OAuthClients>,
    refreshing: tokio::sync::Mutex<()>,
}

impl SessionSignIn {
    /// Hold tokens from a completed sign-in.
    pub fn new(tokens: OAuthTokens, oauth: Option<OAuthClients>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((tokens, None)),
            oauth,
            refreshing: tokio::sync::Mutex::new(()),
        })
    }

    /// The provider this sign-in is for.
    pub fn provider(&self) -> coven_storage::CloudProvider {
        self.state.lock().expect("sign-in poisoned").0.access.provider()
    }

    /// The tokens a new connection starts with.
    pub fn tokens(&self) -> Result<OAuthTokens, SyncError> {
        match &*self.state.lock().expect("sign-in poisoned") {
            (tokens, None) => Ok(tokens.clone()),
            (_, Some(device)) => device.tokens(),
        }
    }

    /// Commit the held tokens to device custody with `commit`, after which
    /// rejections refresh there. Returns `commit`'s result; nothing moves if it fails.
    pub fn publish<T>(
        &self,
        device: &Arc<DeviceCredentials>,
        commit: impl FnOnce(&CredentialKeychains, &OAuthTokens) -> Result<T, SyncError>,
    ) -> Result<T, SyncError> {
        let mut state = self.state.lock().expect("sign-in poisoned");
        let (tokens, published) = &mut *state;
        if published.is_some() {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("this sign-in is already in device custody")
                .into());
        }
        let result = device.write(|keychain| commit(keychain, tokens))?;
        *published = Some(device.clone());
        Ok(result)
    }

    /// Hold the published tokens in this session again, after the commit that
    /// published them was rolled back.
    pub fn unpublish(&self) {
        self.state.lock().expect("sign-in poisoned").1 = None;
    }
}

#[async_trait::async_trait]
impl TokenCustody for SessionSignIn {
    async fn replace(&self, rejected: &OAuthTokens) -> Result<OAuthTokens, StorageError> {
        let _one = self.refreshing.lock().await;
        let held = match &*self.state.lock().expect("sign-in poisoned") {
            (tokens, None) => Ok(tokens.clone()),
            (_, Some(device)) => Err(device.clone()),
        };
        let held = match held {
            Ok(held) => held,
            Err(device) => return device.replace(rejected).await,
        };
        if held.access_token.as_str() != rejected.access_token.as_str() {
            return Ok(held);
        }
        let refreshed = refresh(self.oauth.as_ref(), &held).await?;
        let device = match &mut *self.state.lock().expect("sign-in poisoned") {
            // Only this refresh, serialized above, changes held tokens.
            (tokens, None) => {
                *tokens = refreshed.clone();
                return Ok(refreshed);
            }
            (_, Some(device)) => device.clone(),
        };
        // Publication moved the rejected tokens to device custody meanwhile:
        // this one refresh is committed there instead (§20.2).
        device.commit_refresh(rejected, refreshed)
    }
}

async fn refresh(
    oauth: Option<&OAuthClients>,
    held: &OAuthTokens,
) -> Result<OAuthTokens, StorageError> {
    oauth
        .ok_or(StorageFailure::InvalidConfiguration.with_source("OAuth clients are absent"))?
        .refresh(held)
        .await
}

fn storage_error(error: SyncError) -> StorageError {
    match error {
        SyncError::Storage(error) => error,
        // Custody could not keep the replacement: nothing was installed.
        error => StorageFailure::File.with_source(error),
    }
}

#[cfg(test)]
#[path = "sign_in_tests.rs"]
mod tests;
