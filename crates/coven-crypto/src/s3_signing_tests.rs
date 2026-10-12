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
