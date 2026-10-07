use super::*;
use zeroize::Zeroize;

#[test]
fn text_is_redacted_and_serializes_only_when_explicitly_requested() {
    let secret = SecretText::new("token\"\\\n雪".into());
    assert_eq!(format!("{secret:?}"), "SecretText([REDACTED])");
    let encoded = serde_json::to_string(&secret).unwrap();
    let decoded: SecretText = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.as_str(), secret.as_str());
    let mut clone = secret.clone();
    clone.0.zeroize();
    assert!(clone.as_str().is_empty());
    assert_eq!(secret.as_str(), "token\"\\\n雪");
}
