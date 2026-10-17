//! Registered and granted provider access, separate from provider locations.

use crate::{CloudProvider, StorageError, StorageFailure};
use serde::{Deserialize, Serialize};

/// Dropbox fixes this access level on the app registration (§20.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropboxAccess {
    /// Paths relative to the app's folder; sharing is unavailable.
    AppFolder,
    /// Full-account access, permitting a shared folder namespace.
    FullDropbox,
}

/// The access requested by one sign-in and retained with its credentials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OAuthAccess {
    /// The required choice for Dropbox creation; no default when both keys exist.
    Dropbox(DropboxAccess),
    /// OneDrive's configured file and account scopes.
    OneDrive,
}

impl OAuthAccess {
    /// The provider receiving this authorization request.
    pub fn provider(self) -> CloudProvider {
        match self {
            Self::Dropbox(_) => CloudProvider::Dropbox,
            Self::OneDrive => CloudProvider::OneDrive,
        }
    }

    pub(crate) fn scope(self) -> &'static str {
        match self {
            Self::Dropbox(DropboxAccess::AppFolder) => "files.content.read files.content.write files.metadata.read account_info.read",
            Self::Dropbox(DropboxAccess::FullDropbox) => "files.content.read files.content.write files.metadata.read sharing.read sharing.write account_info.read",
            Self::OneDrive => "Files.ReadWrite.All offline_access User.Read",
        }
    }

    /// Check a token reply against this access. `refreshable` says whether
    /// the resulting tokens hold a refresh token, issued by this reply or kept
    /// from the one it refreshed.
    pub(crate) fn granted(
        self,
        scope: Option<&str>,
        refreshable: bool,
    ) -> Result<Self, StorageError> {
        let refused = || Err(StorageFailure::AccessNotGranted { required: self }.into());
        // Microsoft's reply lists only the Graph permissions the access token
        // carries, never `offline_access`; that grant is the refresh token,
        // "only provided if offline_access scope was requested"
        // (learn.microsoft.com/entra/identity-platform/v2-oauth2-auth-code-flow).
        if self == Self::OneDrive && !refreshable {
            return refused();
        }
        // OAuth 2.0 defines omission as the requested scope, not an empty grant.
        let Some(scope) = scope else { return Ok(self) };
        let scopes: Vec<_> = scope
            .split_ascii_whitespace()
            .map(|granted| match self {
                // An unqualified permission names Microsoft Graph (scopes-oidc).
                Self::OneDrive => granted
                    .strip_prefix("https://graph.microsoft.com/")
                    .unwrap_or(granted),
                Self::Dropbox(_) => granted,
            })
            .collect();
        if self
            .scope()
            .split_ascii_whitespace()
            .filter(|required| !(self == Self::OneDrive && *required == "offline_access"))
            .all(|required| scopes.contains(&required))
        {
            Ok(self)
        } else {
            refused()
        }
    }
}

/// Dropbox's permanent creation choice, retained in settings and restore codes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DropboxLocation {
    /// Not-shared: the app folder itself is the path root.
    AppFolder,
    /// Shared-capable: a namespace independent of the member's mount path.
    SharedFolder {
        /// The provider's shared folder namespace id.
        namespace_id: String,
    },
}

impl DropboxLocation {
    /// The app registration required to reach this location.
    pub fn access(&self) -> DropboxAccess {
        match self {
            Self::AppFolder => DropboxAccess::AppFolder,
            Self::SharedFolder { .. } => DropboxAccess::FullDropbox,
        }
    }
}

#[cfg(test)]
#[path = "provider_access_tests.rs"]
mod tests;
