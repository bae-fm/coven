use super::*;

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

fn sign(method: &str, url: &str, headers: &[(&str, &str)], body: &[u8]) -> String {
    let url = Url::parse(url).unwrap();
    let headers: Vec<(&str, String)> = headers
        .iter()
        .map(|(name, value)| (*name, (*value).to_owned()))
        .collect();
    authorization(
        "AKIDEXAMPLE",
        SECRET,
        &SignedRequest {
            method,
            url: &url,
            region: "us-east-1",
            service: "service",
        },
        "20150830T123600Z",
        &headers,
        body,
    )
}

// AWS's Signature Version 4 test suite: post-x-www-form-urlencoded and
// get-vanilla-query-order-key-case, also recomputed independently.
#[test]
fn signatures_match_the_aws_test_suite() {
    assert_eq!(
        sign(
            "POST",
            "https://example.amazonaws.com/",
            &[
                ("content-type", "application/x-www-form-urlencoded"),
                ("host", "example.amazonaws.com"),
                ("x-amz-date", "20150830T123600Z"),
            ],
            b"Param1=value1",
        ),
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=content-type;host;x-amz-date, \
         Signature=ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a"
    );
    assert_eq!(
        sign(
            "GET",
            "https://example.amazonaws.com/?Param2=value2&Param1=value1",
            &[
                ("x-amz-date", "20150830T123600Z"),
                ("host", "example.amazonaws.com"),
            ],
            b"",
        ),
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=host;x-amz-date, \
         Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"
    );
}

#[test]
fn reply_elements_are_unescaped_and_errors_keep_their_code() {
    let xml = "<R><AccessKey><AccessKeyId>A</AccessKeyId><SecretAccessKey>s&amp;+/&#x41;&#66;</SecretAccessKey></AccessKey><AccessKeyId>B</AccessKeyId><Empty/></R>";
    assert_eq!(elements(xml, "AccessKeyId").unwrap(), ["A", "B"]);
    assert_eq!(elements(xml, "SecretAccessKey").unwrap(), ["s&+/AB"]);
    assert!(elements("<R>&bogus;</R>", "R").is_err());
    for (code, status, failure) in [
        ("EntityAlreadyExists", 409, StorageFailure::AlreadyExists),
        ("NoSuchEntity", 404, StorageFailure::NotFound),
        ("LimitExceeded", 409, StorageFailure::QuotaExceeded),
        ("InvalidClientTokenId", 403, StorageFailure::Authentication),
        ("AccessDenied", 403, StorageFailure::PermissionDenied),
        ("Throttling", 400, StorageFailure::RateLimited),
        ("ServiceFailure", 500, StorageFailure::Network),
    ] {
        assert_eq!(
            QueryError {
                status,
                code: code.into()
            }
            .failure(),
            failure
        );
    }
}

#[test]
fn request_dates_follow_the_clock() {
    assert_eq!(
        amz_date(UNIX_EPOCH + std::time::Duration::from_secs(1_440_938_160)),
        "20150830T123600Z"
    );
}
