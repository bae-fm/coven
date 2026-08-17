//! Fetch the authenticated account's email from each OAuth provider.
//!
//! Called once, right after a joining device authenticates: OAuth folder sharing
//! (`CloudHome::set_access`) shares to the joining member's provider-account email, so the joiner
//! must report which email its fresh tokens belong to. The token is fresh at this
//! point, so each fetch is a single bearer request with no refresh logic.

use serde::Deserialize;

use super::http::{ensure_ok, ok_json, NotFound};
use super::CloudHomeError;
use crate::oauth::OAuthTokens;

/// Google's OpenID userinfo endpoint. `{ "email": "..." }` for the
/// `userinfo.email` scope granted at auth time.
#[derive(Deserialize)]
struct GoogleUserInfo {
    email: String,
}

/// Dropbox `/2/users/get_current_account`. The account's primary email.
#[derive(Deserialize)]
struct DropboxAccount {
    email: String,
}

/// Microsoft Graph `/me`. Work/school accounts populate `mail`; consumer
/// (personal) accounts return `mail: null` and carry the address in
/// `userPrincipalName` instead.
#[derive(Deserialize)]
struct GraphUser {
    mail: Option<String>,
    #[serde(rename = "userPrincipalName")]
    user_principal_name: Option<String>,
}

/// Resolve a Graph `/me` body to an email: prefer `mail`, fall back to
/// `userPrincipalName`, and error if neither is present rather than defaulting to
/// an empty string that would later be shared to nobody.
fn graph_email(user: GraphUser) -> Result<String, CloudHomeError> {
    user.mail.or(user.user_principal_name).ok_or_else(|| {
        CloudHomeError::Transport(
            "OneDrive /me returned neither mail nor userPrincipalName".to_string(),
        )
    })
}

pub(crate) async fn fetch_google(tokens: &OAuthTokens) -> Result<String, CloudHomeError> {
    let resp = reqwest::Client::new()
        .get("https://www.googleapis.com/oauth2/v3/userinfo")
        .bearer_auth(&tokens.access_token)
        .send()
        .await
        .map_err(|e| CloudHomeError::transport("fetch Google account email".to_string(), e))?;
    let resp = ensure_ok(resp, "fetch Google account email", NotFound::Status).await?;
    let info: GoogleUserInfo = ok_json(resp, "parse Google userinfo").await?;
    Ok(info.email)
}

pub(crate) async fn fetch_dropbox(tokens: &OAuthTokens) -> Result<String, CloudHomeError> {
    // This endpoint takes no arguments; Dropbox requires a literal `null` JSON body.
    let resp = reqwest::Client::new()
        .post("https://api.dropboxapi.com/2/users/get_current_account")
        .bearer_auth(&tokens.access_token)
        .json(&serde_json::Value::Null)
        .send()
        .await
        .map_err(|e| CloudHomeError::transport("fetch Dropbox account email".to_string(), e))?;
    let resp = ensure_ok(resp, "fetch Dropbox account email", NotFound::Status).await?;
    let account: DropboxAccount = ok_json(resp, "parse Dropbox account").await?;
    Ok(account.email)
}

pub(crate) async fn fetch_onedrive(tokens: &OAuthTokens) -> Result<String, CloudHomeError> {
    let resp = reqwest::Client::new()
        .get("https://graph.microsoft.com/v1.0/me")
        .bearer_auth(&tokens.access_token)
        .send()
        .await
        .map_err(|e| CloudHomeError::transport("fetch OneDrive account email".to_string(), e))?;
    let resp = ensure_ok(resp, "fetch OneDrive account email", NotFound::Status).await?;
    let user: GraphUser = ok_json(resp, "parse OneDrive /me").await?;
    graph_email(user)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_userinfo_parses_email() {
        let body = r#"{"sub":"1","email":"alice@gmail.com","email_verified":true}"#;
        let info: GoogleUserInfo = serde_json::from_str(body).unwrap();
        assert_eq!(info.email, "alice@gmail.com");
    }

    #[test]
    fn dropbox_account_parses_email() {
        let body = r#"{"account_id":"dbid:1","email":"bob@dropbox.com","email_verified":true}"#;
        let account: DropboxAccount = serde_json::from_str(body).unwrap();
        assert_eq!(account.email, "bob@dropbox.com");
    }

    #[test]
    fn onedrive_prefers_mail() {
        let body = r#"{"mail":"work@corp.com","userPrincipalName":"work@corp.onmicrosoft.com"}"#;
        let user: GraphUser = serde_json::from_str(body).unwrap();
        assert_eq!(graph_email(user).unwrap(), "work@corp.com");
    }

    #[test]
    fn onedrive_falls_back_to_user_principal_name() {
        // Consumer accounts return mail: null.
        let body = r#"{"mail":null,"userPrincipalName":"carol@outlook.com"}"#;
        let user: GraphUser = serde_json::from_str(body).unwrap();
        assert_eq!(graph_email(user).unwrap(), "carol@outlook.com");
    }

    #[test]
    fn onedrive_errors_when_both_absent() {
        let body = r#"{"mail":null,"userPrincipalName":null}"#;
        let user: GraphUser = serde_json::from_str(body).unwrap();
        assert!(graph_email(user).is_err());
    }
}
