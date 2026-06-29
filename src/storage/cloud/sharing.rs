//! Revoke-by-email sharing shared by Google Drive and OneDrive.
//!
//! Both revoke a member the same way: list the folder's permissions, find the
//! entry whose email matches the member, and DELETE it (tolerating a 404 if it is
//! already gone). They differ only in the list/delete URLs, the JSON array field
//! holding the permissions, and where each entry keeps the email — so the
//! algorithm lives here and each backend supplies those four differences.

use super::http::{ensure_ok, ok_json, NotFound};
use super::oauth_session::OAuthSession;
use super::CloudHomeError;

/// Revoke `member_id`'s access by email. `list_url` lists the folder's
/// permissions; `permissions_field` is the JSON array holding them
/// (`"permissions"` for Drive, `"value"` for OneDrive); `email_of` pulls a
/// permission entry's email; `delete_url` builds the DELETE URL for a permission
/// id. A member with no matching permission is an error (nothing to revoke); a
/// 404 on the delete is success (already gone).
pub async fn revoke_by_email(
    session: &OAuthSession,
    member_id: &str,
    list_url: &str,
    permissions_field: &str,
    email_of: impl Fn(&serde_json::Value) -> Option<String>,
    delete_url: impl Fn(&str) -> String,
) -> Result<(), CloudHomeError> {
    let resp = session
        .api_call(|token| session.client().get(list_url).bearer_auth(token))
        .await?;
    let resp = ensure_ok(resp, "list permissions", NotFound::Status).await?;
    let json: serde_json::Value = ok_json(resp, "parse permissions").await?;

    let permission_id = json[permissions_field].as_array().and_then(|perms| {
        perms.iter().find_map(|p| {
            if email_of(p).map(|e| e.eq_ignore_ascii_case(member_id)) == Some(true) {
                p["id"].as_str().map(String::from)
            } else {
                None
            }
        })
    });
    let Some(permission_id) = permission_id else {
        return Err(CloudHomeError::Storage(format!(
            "no permission found for {member_id}"
        )));
    };

    let url = delete_url(&permission_id);
    let resp = session
        .api_call(|token| session.client().delete(&url).bearer_auth(token))
        .await?;
    match ensure_ok(
        resp,
        &format!("revoke access for {member_id}"),
        NotFound::Status,
    )
    .await
    {
        Ok(_) | Err(CloudHomeError::NotFound(_)) => Ok(()),
        Err(e) => Err(e),
    }
}
