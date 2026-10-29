#![cfg(test)]

//! An in-process S3 server for detection's tests, configured as each known
//! service's native behavior, with faults a test can turn on.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Checksum {
    None,
    Scoped,
    Unscoped,
    GoogleHash,
}

/// One S3 service's native behavior; conditions it does not enforce are ignored.
#[derive(Clone, Copy, Debug)]
pub(super) struct Service {
    pub(super) signers: &'static [S3Signing],
    pub(super) single_put: Option<S3SinglePut>,
    pub(super) completion_enforced: bool,
    pub(super) start_timestamp: bool,
    pub(super) partial_visible: bool,
    pub(super) copy: Option<S3CopyCondition>,
    pub(super) checksum: Checksum,
    pub(super) generation: bool,
    pub(super) v2: bool,
}
pub(super) const AWS: Service = Service {
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
pub(super) const GCS: Service = Service {
    signers: &[S3Signing::GoogleHmac, S3Signing::AwsSigV4],
    single_put: Some(S3SinglePut::GenerationMatch),
    completion_enforced: false,
    start_timestamp: false,
    copy: Some(S3CopyCondition::GenerationMatch),
    checksum: Checksum::GoogleHash,
    generation: true,
    ..AWS
};
pub(super) const R2: Service = Service {
    start_timestamp: false,
    copy: Some(S3CopyCondition::CloudflareIfNoneMatch),
    checksum: Checksum::Unscoped,
    ..AWS
};
pub(super) const RUSTFS: Service = Service {
    start_timestamp: false,
    copy: None,
    checksum: Checksum::None,
    ..AWS
};
/// Enforces conditional completion one request at a time, and the copy
/// condition. Its racing completions both succeeding is invisible to these
/// sequential probes.
pub(super) const SEAWEEDFS: Service = Service {
    copy: Some(S3CopyCondition::IfNoneMatch),
    ..RUSTFS
};
/// Enforces conditional completion and the copy condition, dates multipart
/// objects by completion and states checksum scope: the plain S3 settings.
pub(super) const VERSITY_GW: Service = Service {
    start_timestamp: false,
    ..AWS
};
/// Accepts writes to an occupied name whatever condition is sent, and dates
/// multipart objects by initiation.
pub(super) const OVERWRITES: Service = Service {
    single_put: None,
    completion_enforced: false,
    copy: None,
    checksum: Checksum::None,
    ..AWS
};

pub(super) struct Object {
    pub(super) bytes: Vec<u8>,
    pub(super) modified: u64,
    pub(super) etag: u64,
}
pub(super) struct Upload {
    pub(super) key: String,
    pub(super) started: u64,
    pub(super) parts: BTreeMap<u32, Vec<u8>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LostReply {
    Put,
    Copy,
    Initiate,
    Complete,
}

pub(super) struct Remote {
    pub(super) lost_reply: Option<LostReply>,
    pub(super) hidden: BTreeSet<String>,
    pub(super) versioning: &'static str,
    pub(super) versioning_failure: Option<u16>,
    pub(super) service: Service,
    pub(super) objects: BTreeMap<String, Object>,
    pub(super) uploads: BTreeMap<String, Upload>,
    /// Every key ever written, to prove detection stays in its namespace.
    pub(super) written: Vec<String>,
    pub(super) next: u64,
    pub(super) fail_copies: bool,
    /// The one second every publication is dated, when the server's clock
    /// stands still.
    pub(super) frozen: Option<u64>,
    /// The status a V2 listing with `max-keys=0` answers instead of a page.
    pub(super) empty_v2_listing: Option<u16>,
    pub(super) incomplete_v1_listing: bool,
    pub(super) bucket_heads: usize,
    /// Answer this many requests with HTTP 429 `SlowDown` before serving.
    pub(super) throttle: usize,
}

pub(super) fn date(secs: u64, format: aws_sdk_s3::primitives::DateTimeFormat) -> String {
    aws_sdk_s3::primitives::DateTime::from_secs(secs as i64)
        .fmt(format)
        .unwrap()
}
pub(super) fn error(status: u16, code: &str) -> Response<Body> {
    response(status, format!("<Error><Code>{code}</Code></Error>"))
}
/// GoogleHmac moves x-amz-* request headers to x-goog-*.
pub(super) fn header<'a>(headers: &'a HeaderMap, suffix: &str) -> Option<&'a str> {
    headers
        .get(format!("x-amz-{suffix}"))
        .or(headers.get(format!("x-goog-{suffix}")))
        .map(|value| value.to_str().unwrap())
}
pub(super) fn sent(headers: &HeaderMap, (name, value): (&str, &str)) -> bool {
    headers.get(name).is_some_and(|sent| sent == value)
}

pub(super) async fn serve(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    if state.throttle > 0 {
        state.throttle -= 1;
        return error(429, "SlowDown");
    }
    let service = state.service;
    let google = headers["authorization"]
        .to_str()
        .unwrap()
        .starts_with("GOOG4-HMAC-SHA256 ");
    let signer = if google {
        S3Signing::GoogleHmac
    } else {
        S3Signing::AwsSigV4
    };
    if !service.signers.contains(&signer) {
        return error(400, "InvalidArgument");
    }
    let q = query(&uri);
    let key = uri
        .path()
        .strip_prefix("/bucket")
        .unwrap()
        .trim_start_matches('/')
        .to_owned();
    if key.is_empty() {
        if q.contains_key("uploads") {
            let prefix = q.get("prefix").expect("scoped multipart listing");
            let contents: String = state
                .uploads
                .iter()
                .filter(|(_, upload)| upload.key.starts_with(prefix))
                .map(|(id, upload)| {
                    format!(
                        "<Upload><Key>{}</Key><UploadId>{id}</UploadId></Upload>",
                        upload.key
                    )
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
                format!(
                    "<VersioningConfiguration>{}</VersioningConfiguration>",
                    state.versioning
                ),
            );
        }
        return match method {
            Method::HEAD => {
                state.bucket_heads += 1;
                response(403, Vec::new())
            }
            _ => list(&state, &q),
        };
    }
    if q.contains_key("uploads") {
        state.next += 1;
        let id = state.next.to_string();
        let upload = Upload {
            key,
            started: now(),
            parts: BTreeMap::new(),
        };
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
                let modified = if service.start_timestamp {
                    upload.started
                } else {
                    now()
                };
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
        let enforced = service
            .copy
            .is_some_and(|condition| sent(&headers, condition.header()));
        if enforced && state.objects.contains_key(&key) {
            return error(412, "PreconditionFailed");
        }
        let source = source.strip_prefix("/bucket/").unwrap();
        let bytes = state.objects[source].bytes.clone();
        publish(&mut state, key.clone(), bytes, now());
        if state.lost_reply == Some(LostReply::Copy) {
            state.hidden.insert(key);
            return error(503, "ServiceUnavailable");
        }
        return response(
            200,
            "<CopyObjectResult><ETag>\"copied\"</ETag></CopyObjectResult>",
        );
    }
    match method {
        Method::PUT => {
            let enforced = service
                .single_put
                .is_some_and(|condition| sent(&headers, condition.header()));
            if enforced && state.objects.contains_key(&key) {
                return error(412, "PreconditionFailed");
            }
            publish(&mut state, key.clone(), body.to_vec(), now());
            if state.lost_reply == Some(LostReply::Put) {
                state.hidden.insert(key);
                return error(503, "ServiceUnavailable");
            }
            response(200, Vec::new())
        }
        Method::DELETE => {
            state.objects.remove(&key);
            response(204, Vec::new())
        }
        _ => object(&state, &key, method == Method::HEAD, &headers),
    }
}

pub(super) fn publish(state: &mut Remote, key: String, bytes: Vec<u8>, modified: u64) {
    let modified = state.frozen.unwrap_or(modified);
    state.next += 1;
    let etag = state.next;
    state.written.push(key.clone());
    state.objects.insert(
        key,
        Object {
            bytes,
            modified,
            etag,
        },
    );
}

pub(super) fn object(state: &Remote, key: &str, head: bool, headers: &HeaderMap) -> Response<Body> {
    let pending = state.uploads.values().find(|upload| upload.key == key);
    let (bytes, modified, etag) = match (state.objects.get(key), pending) {
        (Some(object), _) => (object.bytes.clone(), object.modified, object.etag),
        (None, Some(upload)) if state.service.partial_visible => (
            upload.parts.values().flatten().copied().collect(),
            upload.started,
            0,
        ),
        _ if head => return response(404, Vec::new()),
        _ => return error(404, "NoSuchKey"),
    };
    let mut reply = Response::builder()
        .header("content-length", bytes.len())
        .header(
            "last-modified",
            date(modified, aws_sdk_s3::primitives::DateTimeFormat::HttpDate),
        )
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
    let body = if head {
        Body::empty()
    } else {
        Body::from(bytes)
    };
    reply.body(body).unwrap()
}

pub(super) fn list(state: &Remote, q: &BTreeMap<String, String>) -> Response<Body> {
    let v2 = q.get("list-type").is_some_and(|value| value == "2");
    if v2 && !state.service.v2 {
        return error(501, "NotImplemented");
    }
    if let Some(status) = state.empty_v2_listing {
        if v2 && q.get("max-keys").is_some_and(|value| value == "0") {
            let code = if status == 501 {
                "NotImplemented"
            } else {
                "ServiceUnavailable"
            };
            return error(status, code);
        }
    }
    let prefix = q.get("prefix").map(String::as_str).unwrap_or("");
    if !prefix.starts_with(&format!("{STORE}/")) {
        return error(403, "AccessDenied");
    }
    let after = q.get(if v2 { "continuation-token" } else { "marker" });
    let limit: usize = q
        .get("max-keys")
        .map(|value| value.parse().unwrap())
        .unwrap_or(1000);
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
    let contents: String = page
        .iter()
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
        format!("<ListBucketResult><IsTruncated>{truncated}</IsTruncated>{next}{contents}</ListBucketResult>"),
    )
}
