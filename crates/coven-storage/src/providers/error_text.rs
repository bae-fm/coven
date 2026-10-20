//! How a provider failure that got no reply prints its cause. A transport
//! error's own text is often only "error sending request": the DNS, TLS or
//! connection failure behind it is a cause further down. Every cause is
//! printed, so the failure names what actually went wrong, and each URL in it
//! is cut to its scheme and host, so a signed query, a token in a query string
//! or an object's path never prints.

use std::error::Error;
use std::fmt;

/// `error` and every cause below it, joined by ": ", each with its URLs cut
/// to scheme and host.
pub(crate) struct CauseChain<'a>(pub(crate) &'a (dyn Error + 'static));

impl fmt::Display for CauseChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cause = Some(self.0);
        let mut separator = "";
        while let Some(error) = cause {
            write!(f, "{separator}{}", redact_urls(&error.to_string()))?;
            separator = ": ";
            cause = error.source();
        }
        Ok(())
    }
}

/// An error kept as a provider failure's cause, printed with every cause
/// below it. Its `Debug` prints the same text: the wrapped error's own
/// `Debug` can hold a full URL. `source()` returns the wrapped error.
pub(crate) struct WithCauses<E>(pub(crate) E);

impl<E: Error + 'static> fmt::Display for WithCauses<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        CauseChain(&self.0).fmt(f)
    }
}

impl<E: Error + 'static> fmt::Debug for WithCauses<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("WithCauses")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl<E: Error + 'static> Error for WithCauses<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

/// `error` with its URL cut to scheme, host and port, so its text, its
/// `Debug` and a walk of the causes above it show no path or query.
pub(crate) fn origin_only(error: reqwest::Error) -> reqwest::Error {
    let origin = error
        .url()
        .map(|url| url::Url::parse(&url.origin().ascii_serialization()));
    match origin {
        Some(Ok(origin)) => error.with_url(origin),
        // An opaque origin has no host to keep.
        Some(Err(_)) | None => error.without_url(),
    }
}

/// `text` with each URL cut to its scheme, host and port.
fn redact_urls(text: &str) -> String {
    let mut redacted = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = ["http://", "https://"]
        .iter()
        .filter_map(|scheme| rest.find(scheme))
        .min()
    {
        redacted.push_str(&rest[..start]);
        let end = rest[start..]
            .find(|c: char| c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '>'))
            .map_or(rest.len(), |length| start + length);
        match url::Url::parse(&rest[start..end]) {
            Ok(url) => {
                redacted.push_str(url.scheme());
                redacted.push_str("://");
                redacted.push_str(url.host_str().unwrap_or_default());
                if let Some(port) = url.port() {
                    redacted.push_str(&format!(":{port}"));
                }
                redacted.push_str("/…");
            }
            Err(_) => redacted.push_str("<url>"),
        }
        rest = &rest[end..];
    }
    redacted.push_str(rest);
    redacted
}

#[cfg(test)]
#[path = "error_text_tests.rs"]
mod tests;
