//! Session custody for a sign-in awaiting storage setup or bootstrap
//! publication (§20.2, E10): a completed sign-in is held for this session only.

use crate::{CloudProvider, OAuthError};
use coven_sync::SessionSignIn;

/// A held sign-in serves only its own provider; another needs a new sign-in.
pub(crate) fn check_provider(
    held: &SessionSignIn,
    provider: CloudProvider,
) -> Result<(), OAuthError> {
    if held.provider() != provider {
        return Err(OAuthError::Reauthorize(provider));
    }
    Ok(())
}

#[cfg(test)]
#[path = "authentication_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::SignIn;
