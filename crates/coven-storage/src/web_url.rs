//! The URL properties shared by locations, invitations and upload sessions.

pub(crate) fn is_web_url(url: &url::Url) -> bool {
    matches!(url.scheme(), "https" | "http")
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
}
