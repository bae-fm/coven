//! Where a bootstrap connection's provider sign-in is held and refreshes: the
//! builder's session custody until a restore publishes it, or the device
//! custody a join keeps it in beside its saved code (§13.2, §20.2, E10).

use super::*;
use coven_storage::{OAuthTokens, ProviderAuth};
use coven_sync::{DeviceCredentials, SessionSignIn};

/// The session sign-in for a bootstrap connection to `provider`.
pub(super) fn account_sign_in(
    builder: &CovenBuilder,
    provider: CloudProvider,
) -> Result<Arc<SessionSignIn>, BootstrapError> {
    builder
        .oauth_flow()
        .ok_or(OAuthError::Unavailable(provider))?;
    let held = builder
        .authentication
        .as_ref()
        .ok_or(OAuthError::Reauthorize(provider))?;
    crate::authentication::check_provider(held, provider)?;
    Ok(held.clone())
}

/// Where a bootstrap connection's sign-in refreshes when the provider rejects
/// its token.
pub(super) enum SignInCustody {
    /// An S3 key or the Apple account: nothing refreshes.
    None,
    /// A restore's sign-in, held for this session until publication commits
    /// it with the store.
    Session(Arc<SessionSignIn>),
    /// A join's sign-in, kept in the store's device credentials beside its
    /// saved invite code. The joined store's handle goes on using this
    /// custody.
    Device {
        credentials: Arc<DeviceCredentials>,
        /// The call started from the sign-in an earlier call kept, not from
        /// a new one the builder holds.
        kept: bool,
    },
}

impl SignInCustody {
    /// A kept sign-in the provider refused, even after its one refresh, needs
    /// a new sign-in: the call ends with `OAuthError::Reauthorize`, as when
    /// none is kept, and the join stays unfinished (§13.2). A new sign-in the
    /// provider refuses keeps the provider's own failure.
    pub(super) fn refused<T>(
        &self,
        provider: CloudProvider,
        result: Result<T, BootstrapError>,
    ) -> Result<T, BootstrapError> {
        match (result, self) {
            (
                Err(BootstrapError::Sync(SyncError::Storage(error))),
                Self::Device { kept: true, .. },
            ) if error.failure() == StorageFailure::Authentication => {
                Err(OAuthError::Reauthorize(provider).into())
            }
            (result, _) => result,
        }
    }
}

/// The sign-in a join on Dropbox or OneDrive connects through, kept in
/// `store`'s device credentials from now until the join ends. A sign-in the
/// builder holds replaces the kept one once it is shown to grant `location`'s
/// access; one that doesn't is refused and leaves the kept one. Without one,
/// the join resumes from the kept sign-in; with neither, the call needs a
/// sign-in (§13.2).
pub(super) fn join_sign_in(
    builder: &CovenBuilder,
    store: StoreId,
    location: &StorageConfig,
) -> Result<(SignInCustody, OAuthTokens), BootstrapError> {
    let provider = location.provider();
    let keychain = builder.keychain()?;
    let credentials = Arc::new(DeviceCredentials::new(
        Arc::new(StoreKeychain::new(keychain.clone(), store)),
        Arc::new(SyncedKeychain::new(keychain, store)),
        builder.oauth.clone(),
    ));
    let kept = builder.authentication.is_none();
    let tokens = if kept {
        if credentials
            .write(|keychains| keychains.storage_credentials())?
            .is_none()
        {
            return Err(match builder.oauth_flow() {
                Some(_) => OAuthError::Reauthorize(provider),
                None => OAuthError::Unavailable(provider),
            }
            .into());
        }
        credentials.tokens()?
    } else {
        let held = account_sign_in(builder, provider)?;
        let tokens = held.tokens()?;
        ConnectionCredentials {
            location: location.clone(),
            credentials: StorageCredentials::OAuth(tokens.clone()),
        }
        .validate()
        .map_err(SyncError::from)?;
        // Its refreshes commit there from now on, before any connection uses
        // them (§20.2).
        held.publish(&credentials, |keychains, tokens| {
            coven_sync::commit_credentials(
                keychains,
                &StorageCredentials::OAuth(tokens.clone()),
                None,
            )
            .map(drop)
        })?;
        tokens
    };
    Ok((SignInCustody::Device { credentials, kept }, tokens))
}

/// How bootstrap reaches `data`: an S3 key, the Apple account, or the sign-in
/// in `custody`, with the tokens it holds now.
pub(super) fn provider_auth(
    data: &ConnectionCredentials,
    custody: &SignInCustody,
) -> Result<ProviderAuth, BootstrapError> {
    Ok(match (&data.credentials, custody) {
        (StorageCredentials::OAuth(_), SignInCustody::Session(sign_in)) => ProviderAuth::OAuth {
            tokens: sign_in.tokens()?,
            custody: sign_in.clone(),
        },
        (StorageCredentials::OAuth(_), SignInCustody::Device { credentials, .. }) => {
            ProviderAuth::OAuth {
                tokens: credentials.tokens()?,
                custody: credentials.clone(),
            }
        }
        (StorageCredentials::S3(keys), SignInCustody::None) => ProviderAuth::S3(keys.clone()),
        (StorageCredentials::CloudKit, SignInCustody::None) => ProviderAuth::CloudKit,
        _ => {
            return Err(SyncError::from(
                StorageFailure::InvalidConfiguration
                    .with_source("only an OAuth location connects through a sign-in"),
            )
            .into())
        }
    })
}
