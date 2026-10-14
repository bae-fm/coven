use super::*;

#[test]
fn google_v4_scope_derivation_matches_an_independent_signature() {
    assert_eq!(
        hex::encode(gcs_request_signature(
            "secret",
            "20261011",
            "auto",
            b"canonical request string"
        )),
        "e123aa3001552a36fbc1f745d51746e3efbd6414000ee4b74923705978e1c1d4"
    );
    assert_eq!(
        hex::encode(s3_request_hash(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

// AWS's post-x-www-form-urlencoded test vector, recomputed independently.
#[test]
fn aws_v4_scope_derivation_matches_the_aws_test_suite() {
    let to_sign = "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
                   42a5e5bb34198acb3e84da4f085bb7927f2bc277ca766e6d19c73c2154021281";
    let signature = aws_request_signature(
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "20150830",
        "us-east-1",
        "service",
        to_sign.as_bytes(),
    );
    assert_eq!(
        hex::encode(signature),
        "ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a"
    );
}
