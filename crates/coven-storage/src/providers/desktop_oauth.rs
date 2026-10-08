use super::{OAuthError, OAuthPresenter};
use crate::CloudProvider;
use coven_crypto::SecretText;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Desktop sign-in using the system browser and a loopback callback listener.
/// Register `http://localhost:19284/callback` with each provider's client.
pub struct DesktopOAuthPresenter;

#[async_trait::async_trait]
impl OAuthPresenter for DesktopOAuthPresenter {
    fn redirect_uri(&self, _provider: CloudProvider) -> &str {
        "http://localhost:19284/callback"
    }

    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:19284").await?;
        open::that(authorization_url)?;
        receive_redirect(listener).await
    }
}

async fn receive_redirect(listener: tokio::net::TcpListener) -> Result<SecretText, OAuthError> {
    loop {
        let (mut socket, _) = listener.accept().await?;
        let mut data = zeroize::Zeroizing::new(Vec::with_capacity(8192));
        while !data.ends_with(b"\r\n\r\n") {
            if data.len() >= 8192 {
                return Err(OAuthError::InvalidRedirect);
            }
            data.push(socket.read_u8().await?);
        }
        let line = std::str::from_utf8(&data)
            .map_err(|_| OAuthError::InvalidRedirect)?
            .lines()
            .next()
            .ok_or(OAuthError::InvalidRedirect)?;
        let target = line
            .strip_prefix("GET ")
            .and_then(|s| s.strip_suffix(" HTTP/1.1"))
            .ok_or(OAuthError::InvalidRedirect)?;
        if !target.starts_with('/') || target.starts_with("//") {
            return Err(OAuthError::InvalidRedirect);
        }
        let url = url::Url::parse(&format!("http://localhost:19284{target}"))
            .map_err(|_| OAuthError::InvalidRedirect)?;
        if url.path() != "/callback" {
            socket
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            continue;
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n").await?;
        return Ok(SecretText::new(url.into()));
    }
}

#[cfg(test)]
#[path = "desktop_oauth_tests.rs"]
mod tests;
