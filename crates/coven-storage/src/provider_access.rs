//! Registered and granted provider access, separate from provider locations.

use crate::{CloudProvider, StorageError, StorageFailure};
use serde::{Deserialize, Serialize};

/// Dropbox fixes this access level on the app registration (§4).
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

    pub(crate) fn granted(self, scope: Option<&str>) -> Result<Self, StorageError> {
        // Only Dropbox's grant is checked against its registration's scopes.
        if self == Self::OneDrive {
            return Ok(self);
        }
        // OAuth 2.0 defines omission as the requested scope, not an empty grant.
        let Some(scope) = scope else { return Ok(self) };
        let scopes: Vec<_> = scope.split_ascii_whitespace().collect();
        if self
            .scope()
            .split_ascii_whitespace()
            .all(|required| scopes.contains(&required))
        {
            Ok(self)
        } else {
            Err(StorageFailure::AccessNotGranted { required: self }.into())
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
