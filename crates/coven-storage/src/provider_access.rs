//! Registered and granted provider access, separate from provider locations.

use crate::{CloudProvider, StorageError, StorageFailure};
use serde::{Deserialize, Serialize};

/// Google's registered maximum, or a sign-in's granted access (§4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GoogleDriveAccess {
    /// Files created or opened by this person through the app's Cloud project.
    File,
    /// All Drive files; required to read other members' files in a shared store.
    Full,
}

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
    /// New stores request File; inviting or joining another person requires Full.
    GoogleDrive(GoogleDriveAccess),
    /// The required choice for Dropbox creation; no default when both keys exist.
    Dropbox(DropboxAccess),
    /// OneDrive's configured file and account scopes.
    OneDrive,
}

impl OAuthAccess {
    /// The provider receiving this authorization request.
    pub fn provider(self) -> CloudProvider {
        match self {
            Self::GoogleDrive(_) => CloudProvider::GoogleDrive,
            Self::Dropbox(_) => CloudProvider::Dropbox,
            Self::OneDrive => CloudProvider::OneDrive,
        }
    }

    pub(crate) fn scope(self) -> &'static str {
        match self {
            Self::GoogleDrive(GoogleDriveAccess::File) => "https://www.googleapis.com/auth/drive.file",
            Self::GoogleDrive(GoogleDriveAccess::Full) => "https://www.googleapis.com/auth/drive",
            Self::Dropbox(DropboxAccess::AppFolder) => "files.content.read files.content.write files.metadata.read account_info.read",
            Self::Dropbox(DropboxAccess::FullDropbox) => "files.content.read files.content.write files.metadata.read sharing.read sharing.write account_info.read",
            Self::OneDrive => "Files.ReadWrite.All offline_access User.Read",
        }
    }

    pub(crate) fn granted(self, scope: Option<&str>) -> Result<Self, StorageError> {
        // OneDrive's token handling is unchanged; this access distinction applies
        // to Google's incremental grants and Dropbox's app registrations.
        if self == Self::OneDrive {
            return Ok(self);
        }
        // OAuth 2.0 defines omission as the requested scope, not an empty grant.
        let Some(scope) = scope else { return Ok(self) };
        let scopes: Vec<_> = scope.split_ascii_whitespace().collect();
        if matches!(self, Self::GoogleDrive(_))
            && scopes.contains(&Self::GoogleDrive(GoogleDriveAccess::Full).scope())
        {
            return Ok(Self::GoogleDrive(GoogleDriveAccess::Full));
        }
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

    /// Whether this grant can serve the requested access without another sign-in.
    pub fn permits(self, required: Self) -> bool {
        self == required
            || matches!(
                (self, required),
                (
                    Self::GoogleDrive(GoogleDriveAccess::Full),
                    Self::GoogleDrive(GoogleDriveAccess::File)
                )
            )
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
