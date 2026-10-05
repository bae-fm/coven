use super::*;
use coven_crypto::SecretText;

#[test]
fn credentials_encode_without_capacity_growth() {
    let credentials = StorageCredentials::OAuth(OAuthTokens {
        access_token: SecretText::new("access\"\\\n\0雪".repeat(200)),
        refresh_token: Some(SecretText::new("refresh\"\\\n\0雪".repeat(300))),
        expires_at: None,
    });
    let encoded = credentials.encode().unwrap();
    assert_eq!(encoded.capacity(), encoded.as_bytes().len());
    let StorageCredentials::OAuth(decoded) =
        StorageCredentials::decode(encoded.as_bytes()).unwrap()
    else {
        panic!("wrong credential kind");
    };
    assert_eq!(
        decoded.access_token.as_str(),
        "access\"\\\n\0雪".repeat(200)
    );
}
