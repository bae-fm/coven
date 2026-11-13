use super::*;

fn signed_in(id: &str, address: &str) -> ProviderAccount {
    ProviderAccount { id: Some(id.into()), address: address.into() }
}

#[test]
fn the_address_is_normalized() {
    let shared = SharedWith::new(" Dan@Example.com ", None).unwrap();
    assert_eq!(shared.address(), "dan@example.com");
}

#[test]
fn an_empty_address_or_id_is_refused() {
    assert!(SharedWith::new("  ", None).is_err());
    assert!(SharedWith::new("dan@example.com", Some(String::new())).is_err());
}

#[test]
fn a_share_with_an_id_compares_ids_only() {
    let shared = SharedWith::new("dan@example.com", Some("dbid:1".into())).unwrap();
    assert!(shared.is(&signed_in("dbid:1", "dan.work@example.com")));
    assert!(!shared.is(&signed_in("dbid:2", "dan@example.com")));
}

#[test]
fn a_share_with_no_id_compares_the_primary_address() {
    let shared = SharedWith::new("Dan@Example.com", None).unwrap();
    assert!(shared.is(&signed_in("dbid:9", "dan@example.com")));
    assert!(!shared.is(&signed_in("dbid:9", "eve@example.com")));
}

#[test]
fn a_share_with_an_id_never_matches_a_sign_in_without_one() {
    let shared = SharedWith::new("dan@example.com", Some("dbid:1".into())).unwrap();
    let signed_in = ProviderAccount { id: None, address: "dan@example.com".into() };
    assert!(!shared.is(&signed_in));
}
