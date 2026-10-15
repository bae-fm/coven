//! Signed requests to an AWS-style query API (IAM), and their XML replies.
//! Requests are signed with AWS Signature Version 4 using the admin access
//! key; the secret never leaves this request (§4).

use crate::{AdminCredential, CloudProvider, StorageError, StorageFailure};
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

/// Where and as what an admin request is signed.
pub(crate) struct QueryService<'a> {
    pub(crate) endpoint: &'a Url,
    pub(crate) region: &'a str,
    pub(crate) service: &'static str,
    pub(crate) version: &'static str,
}

/// What a SigV4 signature covers besides headers and body.
pub(crate) struct SignedRequest<'a> {
    pub(crate) method: &'a str,
    pub(crate) url: &'a Url,
    pub(crate) region: &'a str,
    pub(crate) service: &'a str,
}

/// RFC 3986 percent-encoding of everything but unreserved characters.
fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// An error reply's code with the status it came with.
#[derive(Debug)]
pub(crate) struct QueryError {
    pub(crate) status: u16,
    pub(crate) code: String,
}
impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "admin API returned HTTP {} {}", self.status, self.code)
    }
}
impl std::error::Error for QueryError {}

impl QueryError {
    pub(crate) fn failure(&self) -> StorageFailure {
        match self.code.as_str() {
            "EntityAlreadyExists" => StorageFailure::AlreadyExists,
            "NoSuchEntity" => StorageFailure::NotFound,
            "LimitExceeded" => StorageFailure::QuotaExceeded,
            "Throttling" | "ThrottlingException" | "SlowDown" => StorageFailure::RateLimited,
            "InvalidClientTokenId"
            | "SignatureDoesNotMatch"
            | "ExpiredToken"
            | "InvalidAccessKeyId"
            | "IncompleteSignature" => StorageFailure::Authentication,
            "AccessDenied" | "AccessDeniedException" | "UnauthorizedOperation" => {
                StorageFailure::PermissionDenied
            }
            _ => match self.status {
                401 => StorageFailure::Authentication,
                403 => StorageFailure::PermissionDenied,
                404 => StorageFailure::NotFound,
                409 => StorageFailure::AlreadyExists,
                429 => StorageFailure::RateLimited,
                500..=599 => StorageFailure::Network,
                _ => StorageFailure::Refused,
            },
        }
    }
}

/// Send one signed POST of form parameters; return the reply body. A non-2xx
/// reply becomes an error classified by its code, keeping the code as cause.
pub(crate) async fn send(
    client: &reqwest::Client,
    now: SystemTime,
    admin: &AdminCredential,
    service: &QueryService<'_>,
    action: &str,
    parameters: &[(&str, &str)],
) -> Result<String, StorageError> {
    let AdminCredential::AccessKey {
        access_key_id,
        secret_access_key,
        session_token,
        ..
    } = admin
    else {
        return Err(StorageFailure::InvalidConfiguration
            .with_source("this service's admin API takes an access key"));
    };
    let body = form(action, service.version, parameters);
    let host = match service.endpoint.port() {
        Some(port) => format!("{}:{port}", service.endpoint.host_str().unwrap_or_default()),
        None => service.endpoint.host_str().unwrap_or_default().to_owned(),
    };
    let stamp = amz_date(now);
    let content_type = "application/x-www-form-urlencoded; charset=utf-8";
    let mut headers = vec![
        ("content-type", content_type.to_owned()),
        ("host", host),
        ("x-amz-date", stamp.clone()),
    ];
    if let Some(token) = session_token {
        headers.push(("x-amz-security-token", token.as_str().to_owned()));
    }
    let authorization = authorization(
        access_key_id,
        secret_access_key.as_str(),
        &SignedRequest {
            method: "POST",
            url: service.endpoint,
            region: service.region,
            service: service.service,
        },
        &stamp,
        &headers,
        body.as_bytes(),
    );
    let mut request = client.post(service.endpoint.clone());
    for (name, value) in &headers {
        if *name != "host" {
            request = request.header(*name, value);
        }
    }
    let response = request
        .header("authorization", authorization)
        .body(body)
        .send()
        .await
        .map_err(|error| super::http::transport(CloudProvider::S3, error))?;
    let status = response.status().as_u16();
    let text = response
        .text()
        .await
        .map_err(|error| super::http::transport(CloudProvider::S3, error))?;
    if (200..300).contains(&status) {
        return Ok(text);
    }
    let error = QueryError {
        status,
        code: elements(&text, "Code")?
            .into_iter()
            .next()
            .unwrap_or_default(),
    };
    Err(error.failure().with_source(error))
}

fn form(action: &str, version: &str, parameters: &[(&str, &str)]) -> String {
    let mut form = url::form_urlencoded::Serializer::new(String::new());
    form.append_pair("Action", action);
    form.append_pair("Version", version);
    for (name, value) in parameters {
        form.append_pair(name, value);
    }
    form.finish()
}

pub(crate) fn amz_date(now: SystemTime) -> String {
    let seconds = now
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let days = (seconds / 86_400) as i64;
    let (year, month, day) = civil(days);
    let rest = seconds % 86_400;
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rest / 3600,
        rest / 60 % 60,
        rest % 60
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Hinnant's algorithm).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// The SigV4 Authorization header for a request with these lowercase
/// headers and body. The URL's path and query are canonicalized as signed.
pub(crate) fn authorization(
    access_key_id: &str,
    secret: &str,
    request: &SignedRequest<'_>,
    stamp: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> String {
    let mut headers: Vec<_> = headers.iter().collect();
    headers.sort_by_key(|(name, _)| *name);
    let signed = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect();
    let path = match request.url.path() {
        "" => "/",
        path => path,
    };
    let mut query: Vec<(String, String)> = request
        .url
        .query_pairs()
        .map(|(name, value)| (encode(&name), encode(&value)))
        .collect();
    query.sort();
    let query = query
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!(
        "{}\n{path}\n{query}\n{canonical_headers}\n{signed}\n{}",
        request.method,
        hex::encode(coven_crypto::s3_request_hash(body))
    );
    let date = &stamp[..8];
    let scope = format!("{date}/{}/{}/aws4_request", request.region, request.service);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex::encode(coven_crypto::s3_request_hash(canonical.as_bytes()))
    );
    let signature = coven_crypto::aws_request_signature(
        secret,
        date,
        request.region,
        request.service,
        to_sign.as_bytes(),
    );
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, SignedHeaders={signed}, Signature={}",
        hex::encode(signature)
    )
}

/// The text of every element with this local name, in document order.
pub(crate) fn elements(xml: &str, name: &str) -> Result<Vec<String>, StorageError> {
    let mut found = Vec::new();
    let mut open: Option<String> = None;
    for token in xmlparser::Tokenizer::from(xml) {
        let token = token.map_err(|error| StorageFailure::Protocol.with_source(error))?;
        match token {
            xmlparser::Token::ElementStart { local, .. } if local.as_str() == name => {
                open = Some(String::new());
            }
            xmlparser::Token::Text { text } | xmlparser::Token::Cdata { text, .. } => {
                if let Some(value) = open.as_mut() {
                    value.push_str(&unescape(text.as_str())?);
                }
            }
            xmlparser::Token::ElementEnd {
                end: xmlparser::ElementEnd::Close(_, local),
                ..
            } if local.as_str() == name => {
                found.extend(open.take());
            }
            xmlparser::Token::ElementEnd {
                end: xmlparser::ElementEnd::Empty,
                ..
            } => {
                found.extend(open.take());
            }
            _ => {}
        }
    }
    Ok(found)
}

fn unescape(text: &str) -> Result<String, StorageError> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('&') {
        out.push_str(&rest[..start]);
        let end = rest[start..]
            .find(';')
            .ok_or_else(|| StorageFailure::Protocol.with_source("unterminated XML entity"))?;
        let entity = &rest[start + 1..start + end];
        let character = match entity {
            "amp" => '&',
            "lt" => '<',
            "gt" => '>',
            "quot" => '"',
            "apos" => '\'',
            _ => {
                let code = if let Some(hex) = entity.strip_prefix("#x") {
                    u32::from_str_radix(hex, 16).ok()
                } else {
                    entity.strip_prefix('#').and_then(|n| n.parse().ok())
                };
                code.and_then(char::from_u32)
                    .ok_or_else(|| StorageFailure::Protocol.with_source("unknown XML entity"))?
            }
        };
        out.push(character);
        rest = &rest[start + end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
#[path = "s3_admin_query_tests.rs"]
mod tests;
