//! An in-process S3 server for the adapter's tests, configured as each
//! known service's native behavior, with faults a test can turn on. Its
//! keys reach the whole bucket, as every member key does (§20.7).
use super::*;
use crate::providers::tests::{query, response};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
};
use coven_foundation::clock::{Clock, FixedClock};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Checksum {
    None,
    Scoped,
    Unscoped,
    GoogleHash,
}

/// One S3 service's native behavior; conditions it does not enforce are ignored.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Service {
    pub(crate) signers: &'static [S3Signing],
    pub(crate) single_put: Option<S3SinglePut>,
    pub(crate) completion_enforced: bool,
    pub(crate) start_timestamp: bool,
    pub(crate) partial_visible: bool,
    pub(crate) copy: Option<S3CopyCondition>,
    pub(crate) checksum: Checksum,
    pub(crate) generation: bool,
    pub(crate) v2: bool,
}
pub(crate) const AWS: Service = Service {
    signers: &[S3Signing::AwsSigV4],
    single_put: Some(S3SinglePut::IfNoneMatch),
    completion_enforced: true,
    start_timestamp: true,
    partial_visible: false,
    copy: Some(S3CopyCondition::IfNoneMatch),
    checksum: Checksum::Scoped,
    generation: false,
    v2: true,
};
pub(crate) const GCS: Service = Service {
    signers: &[S3Signing::GoogleHmac, S3Signing::AwsSigV4],
    single_put: Some(S3SinglePut::GenerationMatch),
    completion_enforced: false,
    start_timestamp: false,
    copy: Some(S3CopyCondition::GenerationMatch),
    checksum: Checksum::GoogleHash,
    generation: true,
    ..AWS
};
pub(crate) const R2: Service = Service {
    start_timestamp: false,
    copy: Some(S3CopyCondition::CloudflareIfNoneMatch),
    checksum: Checksum::Unscoped,
    ..AWS
};
pub(crate) const RUSTFS: Service =
    Service { start_timestamp: false, copy: None, checksum: Checksum::None, ..AWS };
/// Enforces conditional completion one request at a time, and the copy
/// condition. Its racing completions both succeeding is invisible to these
/// sequential probes.
pub(crate) const SEAWEEDFS: Service =
    Service { copy: Some(S3CopyCondition::IfNoneMatch), ..RUSTFS };
/// Enforces conditional completion and the copy condition, dates multipart
/// objects by completion and states checksum scope: the plain S3 settings.
pub(crate) const VERSITY_GW: Service = Service { start_timestamp: false, ..AWS };
/// Accepts writes to an occupied name whatever condition is sent, and dates
/// multipart objects by initiation.
pub(crate) const OVERWRITES: Service = Service {
    single_put: None,
    completion_enforced: false,
    copy: None,
    checksum: Checksum::None,
    ..AWS
};

pub(crate) struct Object {
    pub(crate) bytes: Vec<u8>,
    pub(crate) modified: u64,
    pub(crate) etag: u64,
}
pub(crate) struct Upload {
    pub(crate) key: String,
    pub(crate) started: u64,
    pub(crate) parts: BTreeMap<u32, Vec<u8>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LostReply {
    Put,
    Copy,
    Initiate,
    Complete,
}

pub(crate) struct Remote {
    /// The time the server dates objects by, which the test moves.
    pub(crate) clock: Arc<FixedClock>,
    pub(crate) lost_reply: Option<LostReply>,
    pub(crate) hidden: BTreeSet<String>,
    pub(crate) versioning: &'static str,
    pub(crate) versioning_failure: Option<u16>,
    pub(crate) service: Service,
    pub(crate) objects: BTreeMap<String, Object>,
    pub(crate) uploads: BTreeMap<String, Upload>,
    /// Every key ever written, to prove detection stays in its namespace.
    pub(crate) written: Vec<String>,
    pub(crate) next: u64,
    pub(crate) fail_copies: bool,
    /// Refuse every delete, as a key without delete permission does.
    pub(crate) fail_deletes: bool,
    /// The one second every publication is dated, when the server's clock
    /// stands still.
    pub(crate) frozen: Option<u64>,
    /// The status a V2 listing with `max-keys=0` answers instead of a page.
    pub(crate) empty_v2_listing: Option<u16>,
    pub(crate) incomplete_v1_listing: bool,
    pub(crate) bucket_heads: usize,
    /// Answer this many requests with HTTP 429 `SlowDown` before serving.
    pub(crate) throttle: usize,
    /// This many listing requests omit the newest probe object, as a
    /// lagging listing does.
    pub(crate) lagging_listings: usize,
    /// Every listing request served.
    pub(crate) listings: usize,
    /// The prefix of every object listing served, in order.
    pub(crate) listed: Vec<String>,
    /// Every request's method and path, in order.
    pub(crate) requests: Vec<(Method, String)>,
    /// Never answer a part upload, saying so here first: the client stops
    /// there, as a process killed mid-upload does.
    pub(crate) stall_parts: Option<Arc<tokio::sync::Notify>>,
}

impl Remote {
    /// An empty bucket behaving as `service`, dating objects by `clock`.
    pub(crate) fn new(service: Service, clock: Arc<FixedClock>) -> Self {
        Self {
            clock,
            lost_reply: None,
            hidden: BTreeSet::new(),
            versioning: "",
            versioning_failure: None,
            service,
            objects: BTreeMap::new(),
            uploads: BTreeMap::new(),
            written: Vec::new(),
            next: 0,
            fail_copies: false,
            fail_deletes: false,
            frozen: None,
            empty_v2_listing: None,
            incomplete_v1_listing: false,
            bucket_heads: 0,
            throttle: 0,
            lagging_listings: 0,
            listings: 0,
            listed: Vec::new(),
            requests: Vec::new(),
            stall_parts: None,
        }
    }
}

/// The server's current second, as a service dates its objects.
pub(crate) fn seconds(remote: &Remote) -> u64 {
    remote.clock.now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

pub(crate) fn date(secs: u64, format: aws_sdk_s3::primitives::DateTimeFormat) -> String {
    aws_sdk_s3::primitives::DateTime::from_secs(secs as i64).fmt(format).unwrap()
}
pub(crate) fn error(status: u16, code: &str) -> Response<Body> {
    response(status, format!("<Error><Code>{code}</Code></Error>"))
}
/// GoogleHmac moves x-amz-* request headers to x-goog-*.
pub(crate) fn header<'a>(headers: &'a HeaderMap, suffix: &str) -> Option<&'a str> {
    headers
        .get(format!("x-amz-{suffix}"))
        .or(headers.get(format!("x-goog-{suffix}")))
        .map(|value| value.to_str().unwrap())
}
pub(crate) fn sent(headers: &HeaderMap, (name, value): (&str, &str)) -> bool {
    headers.get(name).is_some_and(|sent| sent == value)
}

pub(crate) async fn serve(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let part = method == Method::PUT && query(&uri).contains_key("uploadId");
    let stall = state.lock().unwrap().stall_parts.clone().filter(|_| part);
    if let Some(stalled) = stall {
        stalled.notify_one();
        std::future::pending::<()>().await;
    }
    let mut state = state.lock().unwrap();
    state.requests.push((method.clone(), uri.path().to_owned()));
    if state.throttle > 0 {
        state.throttle -= 1;
        return error(429, "SlowDown");
    }
    let service = state.service;
    let google = headers["authorization"].to_str().unwrap().starts_with("GOOG4-HMAC-SHA256 ");
    let signer = if google { S3Signing::GoogleHmac } else { S3Signing::AwsSigV4 };
    if !service.signers.contains(&signer) {
        return error(400, "InvalidArgument");
    }
    let q = query(&uri);
    let key = uri.path().strip_prefix("/bucket").unwrap().trim_start_matches('/').to_owned();
    if key.is_empty() {
        if q.contains_key("uploads") {
            let prefix = q.get("prefix").expect("scoped multipart listing");
            let contents: String = state
                .uploads
                .iter()
                .filter(|(_, upload)| upload.key.starts_with(prefix))
                .map(|(id, upload)| {
                    format!("<Upload><Key>{}</Key><UploadId>{id}</UploadId></Upload>", upload.key)
                })
                .collect();
            return response(200, format!("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>{contents}</ListMultipartUploadsResult>"));
        }
        if q.contains_key("versioning") {
            if let Some(status) = state.versioning_failure {
                return error(
                    status,
                    match status {
                        403 => "AccessDenied",
                        501 => "NotImplemented",
                        _ => "ServiceUnavailable",
                    },
                );
            }
            return response(
                200,
                format!("<VersioningConfiguration>{}</VersioningConfiguration>", state.versioning),
            );
        }
        return match method {
            Method::HEAD => {
                state.bucket_heads += 1;
                response(403, Vec::new())
            }
            _ => {
                state.listings += 1;
                if state.lagging_listings == 0 {
                    return list(&mut state, &q);
                }
                state.lagging_listings -= 1;
                let newest = state.written.last().cloned().unwrap_or_default();
                let fresh = state.hidden.insert(newest.clone());
                let page = list(&mut state, &q);
                if fresh {
                    state.hidden.remove(&newest);
                }
                page
            }
        };
    }
    if q.contains_key("uploads") {
        state.next += 1;
        let id = state.next.to_string();
        let upload = Upload { key, started: seconds(&state), parts: BTreeMap::new() };
        state.uploads.insert(id.clone(), upload);
        if state.lost_reply == Some(LostReply::Initiate) {
            return error(503, "ServiceUnavailable");
        }
        return response(
            200,
            format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"),
        );
    }
    if let Some(id) = q.get("uploadId") {
        if !state.uploads.contains_key(id) {
            return error(404, "NoSuchUpload");
        }
        return match method {
            Method::PUT => {
                let number = q["partNumber"].parse().unwrap();
                let upload = state.uploads.get_mut(id).unwrap();
                upload.parts.insert(number, body.to_vec());
                Response::builder()
                    .header("etag", format!("\"part-{number}\""))
                    .body(Body::empty())
                    .unwrap()
            }
            Method::POST => {
                if service.completion_enforced
                    && headers.contains_key("if-none-match")
                    && state.objects.contains_key(&key)
                {
                    return error(412, "PreconditionFailed");
                }
                let upload = state.uploads.remove(id).unwrap();
                let modified =
                    if service.start_timestamp { upload.started } else { seconds(&state) };
                let bytes = upload.parts.into_values().flatten().collect();
                publish(&mut state, key.clone(), bytes, modified);
                if state.lost_reply == Some(LostReply::Complete) {
                    state.hidden.insert(key);
                    return error(503, "ServiceUnavailable");
                }
                response(
                    200,
                    "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>",
                )
            }
            _ => {
                state.uploads.remove(id);
                response(204, Vec::new())
            }
        };
    }
    if let Some(source) = header(&headers, "copy-source") {
        if state.fail_copies {
            return error(500, "InternalError");
        }
        let enforced = service.copy.is_some_and(|condition| sent(&headers, condition.header()));
        if enforced && state.objects.contains_key(&key) {
            return error(412, "PreconditionFailed");
        }
        let source = source.strip_prefix("/bucket/").unwrap();
        let bytes = state.objects[source].bytes.clone();
        let modified = seconds(&state);
        publish(&mut state, key.clone(), bytes, modified);
        if state.lost_reply == Some(LostReply::Copy) {
            state.hidden.insert(key);
            return error(503, "ServiceUnavailable");
        }
        return response(200, "<CopyObjectResult><ETag>\"copied\"</ETag></CopyObjectResult>");
    }
    match method {
        Method::PUT => {
            let enforced =
                service.single_put.is_some_and(|condition| sent(&headers, condition.header()));
            if enforced && state.objects.contains_key(&key) {
                return error(412, "PreconditionFailed");
            }
            let modified = seconds(&state);
            publish(&mut state, key.clone(), body.to_vec(), modified);
            if state.lost_reply == Some(LostReply::Put) {
                state.hidden.insert(key);
                return error(503, "ServiceUnavailable");
            }
            response(200, Vec::new())
        }
        Method::DELETE => {
            if state.fail_deletes {
                return error(403, "AccessDenied");
            }
            state.objects.remove(&key);
            response(204, Vec::new())
        }
        _ => object(&state, &key, method == Method::HEAD, &headers),
    }
}

pub(crate) fn publish(state: &mut Remote, key: String, bytes: Vec<u8>, modified: u64) {
    let modified = state.frozen.unwrap_or(modified);
    state.next += 1;
    let etag = state.next;
    state.written.push(key.clone());
    state.objects.insert(key, Object { bytes, modified, etag });
}

pub(crate) fn object(state: &Remote, key: &str, head: bool, headers: &HeaderMap) -> Response<Body> {
    let pending = state.uploads.values().find(|upload| upload.key == key);
    let (bytes, modified, etag) = match (state.objects.get(key), pending) {
        (Some(object), _) => (object.bytes.clone(), object.modified, object.etag),
        (None, Some(upload)) if state.service.partial_visible => {
            (upload.parts.values().flatten().copied().collect(), upload.started, 0)
        }
        _ if head => return response(404, Vec::new()),
        _ => return error(404, "NoSuchKey"),
    };
    let mut reply = Response::builder()
        .header("content-length", bytes.len())
        .header("last-modified", date(modified, aws_sdk_s3::primitives::DateTimeFormat::HttpDate))
        .header("etag", format!("\"{etag}\""));
    if state.service.generation {
        reply = reply.header("x-goog-generation", etag);
    }
    // Checksums only on HEAD: no body for the SDK to validate.
    let requested = header(headers, "checksum-mode") == Some("ENABLED");
    if head {
        reply = match state.service.checksum {
            Checksum::Scoped if requested => reply
                .header("x-amz-checksum-crc32", "AAAAAA==")
                .header("x-amz-checksum-type", "FULL_OBJECT"),
            Checksum::Unscoped if requested => reply.header("x-amz-checksum-crc32", "AAAAAA=="),
            Checksum::GoogleHash => reply.header("x-goog-hash", "crc32c=AAAAAA=="),
            _ => reply,
        };
    }
    let body = if head { Body::empty() } else { Body::from(bytes) };
    reply.body(body).unwrap()
}

pub(crate) fn list(state: &mut Remote, q: &BTreeMap<String, String>) -> Response<Body> {
    let v2 = q.get("list-type").is_some_and(|value| value == "2");
    if v2 && !state.service.v2 {
        return error(501, "NotImplemented");
    }
    if let Some(status) = state.empty_v2_listing {
        if v2 && q.get("max-keys").is_some_and(|value| value == "0") {
            let code = if status == 501 { "NotImplemented" } else { "ServiceUnavailable" };
            return error(status, code);
        }
    }
    let prefix = q.get("prefix").map(String::as_str).unwrap_or("");
    state.listed.push(prefix.to_owned());
    let after = q.get(if v2 { "continuation-token" } else { "marker" });
    let limit: usize = q.get("max-keys").map(|value| value.parse().unwrap()).unwrap_or(1000);
    let entries: Vec<_> = state
        .objects
        .iter()
        .filter(|_| v2 || !state.incomplete_v1_listing)
        .filter(|(key, _)| {
            !state.hidden.contains(*key)
                && key.starts_with(prefix)
                && after.is_none_or(|after| *key > after)
        })
        .collect();
    let truncated = limit > 0 && entries.len() > limit;
    let page = &entries[..limit.min(entries.len())];
    // A delimiter folds each key with one after the prefix into its folder,
    // listed once.
    let delimiter = q.get("delimiter");
    let folder = |key: &str| {
        let delimiter = delimiter?;
        let rest = &key[prefix.len()..];
        rest.find(delimiter.as_str())
            .map(|start| format!("{prefix}{}", &rest[..start + delimiter.len()]))
    };
    let folders: BTreeSet<String> = page.iter().filter_map(|(key, _)| folder(key)).collect();
    let folders: String = folders
        .iter()
        .map(|folder| format!("<CommonPrefixes><Prefix>{folder}</Prefix></CommonPrefixes>"))
        .collect();
    let contents: String = page
        .iter()
        .filter(|(key, _)| folder(key).is_none())
        .map(|(key, object)| {
            format!(
                "<Contents><Key>{key}</Key><Size>{}</Size><LastModified>{}</LastModified><ETag>\"{}\"</ETag></Contents>",
                object.bytes.len(),
                date(object.modified, aws_sdk_s3::primitives::DateTimeFormat::DateTime),
                object.etag
            )
        })
        .collect();
    let next = match (truncated && v2, page.last()) {
        (true, Some((key, _))) => format!("<NextContinuationToken>{key}</NextContinuationToken>"),
        _ => String::new(),
    };
    response(
        200,
        format!("<ListBucketResult><IsTruncated>{truncated}</IsTruncated>{next}{contents}{folders}</ListBucketResult>"),
    )
}
