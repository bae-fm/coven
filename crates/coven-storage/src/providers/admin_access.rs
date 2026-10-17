//! Where the storage account's admin access comes from (§4). The person
//! chooses a signed-in CLI and its profile or account, a browser sign-in, or
//! a pasted admin key; guided console steps need none. Coven reads no CLI
//! configuration and runs no CLI before that choice. What a source yields is
//! held in memory for one operation, never logged or stored, and a retry
//! obtains it again.
#![cfg_attr(any(target_os = "ios", target_os = "android"), allow(unused))]

use crate::{AdminCredential, StorageError, StorageFailure};
use coven_crypto::SecretText;
use coven_foundation::clock::ClockRef;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use url::Url;

/// Shows a device sign-in page and its code, as AWS IAM Identity Center asks
/// (§4). The desktop implementation opens the system browser.
#[async_trait::async_trait]
pub trait DeviceSignInPresenter: Send + Sync {
    /// Show the page where the person confirms this code.
    async fn show(&self, verification_url: &str, user_code: &str) -> Result<(), StorageError>;
}

/// Opens the device sign-in page in the system browser. Desktop only.
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub struct DesktopDeviceSignIn;

#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[async_trait::async_trait]
impl DeviceSignInPresenter for DesktopDeviceSignIn {
    async fn show(&self, verification_url: &str, _user_code: &str) -> Result<(), StorageError> {
        open::that_detached(verification_url)
            .map_err(|error| StorageFailure::ProviderUnavailable.with_source(error))
    }
}

/// The source of admin access the person chose (§4).
pub enum AdminAccessSource {
    #[cfg(not(any(target_os = "ios", target_os = "android")))]
    /// `aws configure export-credentials --format process`, with this profile
    /// or the CLI's own selection. Desktop only.
    AwsCli {
        /// The CLI profile.
        profile: Option<String>,
    },
    #[cfg(not(any(target_os = "ios", target_os = "android")))]
    /// `gcloud auth print-access-token`, for this account or the active one.
    /// Desktop only.
    GcloudCli {
        /// The signed-in account.
        account: Option<String>,
    },
    #[cfg(not(any(target_os = "ios", target_os = "android")))]
    /// Google sign-in in the browser with the app's Google client, for the
    /// `cloud-platform` scope. Desktop only.
    GoogleSignIn,
    #[cfg(not(any(target_os = "ios", target_os = "android")))]
    /// AWS IAM Identity Center device authorization, then the role's
    /// short-lived credentials. Desktop only.
    AwsIdentityCenter {
        /// The access portal's start URL.
        start_url: String,
        /// The Identity Center region.
        region: String,
        /// The AWS account holding the storage.
        account_id: String,
        /// The permission set's role in that account.
        role_name: String,
        /// Shows the sign-in page and code.
        presenter: Arc<dyn DeviceSignInPresenter>,
    },
    /// A credential the person pasted: an admin access key or an API token.
    Pasted(AdminCredential),
}

impl std::fmt::Debug for AdminAccessSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(not(any(target_os = "ios", target_os = "android")))]
            Self::AwsCli { profile } => f.debug_struct("AwsCli").field("profile", profile).finish(),
            #[cfg(not(any(target_os = "ios", target_os = "android")))]
            Self::GcloudCli { account } => f
                .debug_struct("GcloudCli")
                .field("account", account)
                .finish(),
            #[cfg(not(any(target_os = "ios", target_os = "android")))]
            Self::GoogleSignIn => f.write_str("GoogleSignIn"),
            #[cfg(not(any(target_os = "ios", target_os = "android")))]
            Self::AwsIdentityCenter {
                start_url, region, ..
            } => f
                .debug_struct("AwsIdentityCenter")
                .field("start_url", start_url)
                .field("region", region)
                .finish_non_exhaustive(),
            Self::Pasted(credential) => f.debug_tuple("Pasted").field(credential).finish(),
        }
    }
}

/// Run the chosen source and return its credential.
pub async fn obtain_admin_access(
    source: AdminAccessSource,
    oauth: Option<&super::OAuthFlow>,
    clock: ClockRef,
) -> Result<AdminCredential, StorageError> {
    match source {
        AdminAccessSource::Pasted(credential) => Ok(credential),
        #[cfg(not(any(target_os = "ios", target_os = "android")))]
        AdminAccessSource::AwsCli { profile } => {
            process_credentials(&run(aws_cli(profile.as_deref())).await?)
        }
        #[cfg(not(any(target_os = "ios", target_os = "android")))]
        AdminAccessSource::GcloudCli { account } => {
            bearer(&run(gcloud_cli(account.as_deref())).await?)
        }
        #[cfg(not(any(target_os = "ios", target_os = "android")))]
        AdminAccessSource::GoogleSignIn => {
            let flow = oauth.ok_or_else(|| {
                StorageFailure::ProviderUnavailable.with_source("no Google sign-in is configured")
            })?;
            flow.google_cloud_admin()
                .await
                .map(AdminCredential::BearerToken)
                .map_err(|error| match error {
                    super::OAuthError::Storage(error) => error,
                    error @ (super::OAuthError::Unavailable(_)
                    | super::OAuthError::InvalidRedirect) => {
                        StorageFailure::InvalidConfiguration.with_source(error)
                    }
                    error => StorageFailure::Authentication.with_source(error),
                })
        }
        #[cfg(not(any(target_os = "ios", target_os = "android")))]
        AdminAccessSource::AwsIdentityCenter {
            start_url,
            region,
            account_id,
            role_name,
            presenter,
        } => {
            identity_center(
                &IdentityCenterEndpoints::for_region(&region)?,
                &start_url,
                &account_id,
                &role_name,
                presenter.as_ref(),
                clock,
            )
            .await
        }
    }
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
fn aws_cli(profile: Option<&str>) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("aws");
    command.args(["configure", "export-credentials", "--format", "process"]);
    if let Some(profile) = profile {
        command.args(["--profile", profile]);
    }
    command
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
fn gcloud_cli(account: Option<&str>) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("gcloud");
    command.args(["auth", "print-access-token"]);
    if let Some(account) = account {
        command.arg(account);
    }
    command
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
/// The CLI's standard output, erased on drop. A failure keeps the exit
/// status, never the CLI's text, which can echo configuration.
async fn run(
    mut command: tokio::process::Command,
) -> Result<zeroize::Zeroizing<Vec<u8>>, StorageError> {
    let output = command
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|error| StorageFailure::ProviderUnavailable.with_source(error))?;
    let stdout = zeroize::Zeroizing::new(output.stdout);
    if !output.status.success() {
        return Err(StorageFailure::Authentication
            .with_source(format!("the CLI exited with {}", output.status)));
    }
    Ok(stdout)
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ProcessCredentials {
    version: u32,
    access_key_id: String,
    secret_access_key: SecretText,
    session_token: Option<SecretText>,
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
fn process_credentials(output: &[u8]) -> Result<AdminCredential, StorageError> {
    let parsed: ProcessCredentials = serde_json::from_slice(output)
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    if parsed.version != 1 || parsed.access_key_id.is_empty() {
        return Err(StorageFailure::Protocol.with_source("unexpected credential process output"));
    }
    Ok(AdminCredential::AccessKey {
        access_key_id: parsed.access_key_id,
        secret_access_key: parsed.secret_access_key,
        session_token: parsed.session_token,
        iam_endpoint: None,
    })
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
fn bearer(output: &[u8]) -> Result<AdminCredential, StorageError> {
    let token =
        std::str::from_utf8(output).map_err(|error| StorageFailure::Protocol.with_source(error))?;
    let token = token.trim();
    if token.is_empty() {
        return Err(StorageFailure::Authentication.with_source("the CLI printed no token"));
    }
    Ok(AdminCredential::BearerToken(SecretText::new(
        token.to_owned(),
    )))
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
/// Identity Center's OIDC and portal endpoints.
pub(crate) struct IdentityCenterEndpoints {
    pub(crate) oidc: Url,
    pub(crate) portal: Url,
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
impl IdentityCenterEndpoints {
    fn for_region(region: &str) -> Result<Self, StorageError> {
        if region.is_empty()
            || !region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(StorageFailure::InvalidConfiguration.with_source("invalid AWS region"));
        }
        let url = |text: String| {
            Url::parse(&text)
                .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))
        };
        Ok(Self {
            oidc: url(format!("https://oidc.{region}.amazonaws.com/"))?,
            portal: url(format!("https://portal.sso.{region}.amazonaws.com/"))?,
        })
    }
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
/// Register a public client, ask the person to confirm its device code, poll
/// for the token at the interval Identity Center gives, then exchange it for
/// the role's short-lived credentials.
pub(crate) async fn identity_center(
    endpoints: &IdentityCenterEndpoints,
    start_url: &str,
    account_id: &str,
    role_name: &str,
    presenter: &dyn DeviceSignInPresenter,
    clock: ClockRef,
) -> Result<AdminCredential, StorageError> {
    let http = super::http::client()
        .map_err(|error| super::http::transport(crate::CloudProvider::S3, error))?;
    let post = |path: &str, body: serde_json::Value| {
        let url = endpoints.oidc.join(path);
        let http = http.clone();
        async move {
            let url =
                url.map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
            let response = http
                .post(url)
                .json(&body)
                .send()
                .await
                .map_err(|error| super::http::transport(crate::CloudProvider::S3, error))?;
            let status = response.status().as_u16();
            let value: serde_json::Value = response
                .json()
                .await
                .map_err(|error| StorageFailure::Protocol.with_source(error))?;
            Ok::<_, StorageError>((status, value))
        }
    };
    let text = |value: &serde_json::Value, field: &str| {
        super::http::string(value, field).map(str::to_owned)
    };
    let (status, client) = post(
        "client/register",
        serde_json::json!({"clientName": "coven", "clientType": "public"}),
    )
    .await?;
    if status != 200 {
        return Err(StorageFailure::Refused.with_source("Identity Center refused registration"));
    }
    let client_id = text(&client, "clientId")?;
    let client_secret = SecretText::new(text(&client, "clientSecret")?);
    let (status, device) = post(
        "device_authorization",
        serde_json::json!({
            "clientId": client_id,
            "clientSecret": client_secret.as_str(),
            "startUrl": start_url,
        }),
    )
    .await?;
    if status != 200 {
        return Err(StorageFailure::Refused.with_source("Identity Center refused the device"));
    }
    let device_code = SecretText::new(text(&device, "deviceCode")?);
    let verification = text(&device, "verificationUriComplete")?;
    let user_code = text(&device, "userCode")?;
    let mut interval = Duration::from_secs(device["interval"].as_u64().unwrap_or(5));
    let mut remaining = Duration::from_secs(device["expiresIn"].as_u64().unwrap_or(600));
    presenter.show(&verification, &user_code).await?;
    let access_token = loop {
        if remaining.is_zero() {
            return Err(StorageFailure::Authentication.with_source("device sign-in expired"));
        }
        clock.sleep(interval).await;
        remaining = remaining.saturating_sub(interval);
        let (status, token) = post(
            "token",
            serde_json::json!({
                "clientId": client_id,
                "clientSecret": client_secret.as_str(),
                "grantType": "urn:ietf:params:oauth:grant-type:device_code",
                "deviceCode": device_code.as_str(),
            }),
        )
        .await?;
        if status == 200 {
            break SecretText::new(text(&token, "accessToken")?);
        }
        match token["error"].as_str() {
            Some("authorization_pending") => {}
            Some("slow_down") => interval += Duration::from_secs(5),
            Some("access_denied") => {
                return Err(StorageFailure::PermissionDenied.with_source("device sign-in denied"))
            }
            _ => return Err(StorageFailure::Authentication.with_source("device sign-in failed")),
        }
    };
    let mut url = endpoints
        .portal
        .join("federation/credentials")
        .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
    url.query_pairs_mut()
        .append_pair("account_id", account_id)
        .append_pair("role_name", role_name);
    let response = http
        .get(url)
        .header("x-amz-sso_bearer_token", access_token.as_str())
        .send()
        .await
        .map_err(|error| super::http::transport(crate::CloudProvider::S3, error))?;
    let response = super::http::checked(crate::CloudProvider::S3, response).await?;
    let value: serde_json::Value = response
        .json()
        .await
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    let role = &value["roleCredentials"];
    Ok(AdminCredential::AccessKey {
        access_key_id: text(role, "accessKeyId")?,
        secret_access_key: SecretText::new(text(role, "secretAccessKey")?),
        session_token: Some(SecretText::new(text(role, "sessionToken")?)),
        iam_endpoint: None,
    })
}

#[cfg(test)]
#[path = "admin_access_tests.rs"]
mod tests;
