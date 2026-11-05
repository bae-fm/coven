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

/// Secret bytes and text erase their whole allocation when dropped (§12.4).
#[test]
fn secrets_erase_their_whole_allocation_when_dropped() {
    fn erases<T: zeroize::ZeroizeOnDrop>(_: &T) {}
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(b"short");
    let capacity = bytes.capacity();
    let secret = SecretBytes::new(bytes);
    erases(&secret.0);
    assert_eq!(secret.capacity(), capacity);
    let mut text = String::with_capacity(64);
    text.push_str("short");
    let text = SecretText::new(text);
    erases(&text.0);
    assert!(text.0.capacity() >= 64);
}
