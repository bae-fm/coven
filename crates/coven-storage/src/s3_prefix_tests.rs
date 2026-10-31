use super::*;

#[test]
fn slashes_at_either_end_mean_nothing() {
    for (typed, canonical) in [
        ("", ""),
        ("music", "music"),
        ("music/bae", "music/bae"),
        ("/music/bae/", "music/bae"),
        ("bae-dima/", "bae-dima"),
        ("/bae_dima", "bae_dima"),
        ("///music///", "music"),
    ] {
        let prefix = S3Prefix::parse(typed).unwrap();
        assert_eq!(prefix.as_str(), canonical, "{typed:?}");
        assert_eq!(prefix, S3Prefix::parse(canonical).unwrap(), "{typed:?}");
    }
}

#[test]
fn a_prefix_of_only_slashes_is_the_bucket_root() {
    for typed in ["/", "//", "///"] {
        let prefix = S3Prefix::parse(typed).unwrap();
        assert!(prefix.is_empty(), "{typed:?}");
        assert_eq!(prefix, S3Prefix::default(), "{typed:?}");
        assert_eq!(prefix.key_prefix(), "");
    }
}

#[test]
fn two_slashes_in_a_row_are_refused() {
    for typed in ["music//bae", "/music//bae/", "a/b//c"] {
        assert_eq!(S3Prefix::parse(typed), Err(S3PrefixError::EmptySegment), "{typed:?}");
    }
}

#[test]
fn a_disallowed_character_names_its_folder() {
    for (typed, segment, character) in [
        ("music/bae.lib", "bae.lib", '.'),
        ("my music", "my music", ' '),
        ("music/ bae", " bae", ' '),
        ("café/bae", "café", 'é'),
        ("a/b\\c", "b\\c", '\\'),
        ("music/../bae", "..", '.'),
    ] {
        assert_eq!(
            S3Prefix::parse(typed),
            Err(S3PrefixError::InvalidCharacter { segment: segment.into(), character }),
            "{typed:?}"
        );
    }
}

#[test]
fn a_folder_longer_than_255_bytes_names_itself() {
    let longest = "a".repeat(255);
    assert_eq!(
        S3Prefix::parse(&format!("music/{longest}")).unwrap().as_str(),
        format!("music/{longest}")
    );
    let over = "b".repeat(256);
    assert_eq!(
        S3Prefix::parse(&format!("music/{over}/bae")),
        Err(S3PrefixError::SegmentTooLong { segment: over })
    );
}

#[test]
fn object_keys_sit_under_the_prefix() {
    let store = coven_foundation::id_source::StoreId(uuid::Uuid::from_bytes([1; 16]));
    let floor = ObjectPath::floor(store);
    let files = ObjectPrefix::files(store);
    let id = "01010101-0101-0101-0101-010101010101";
    assert_eq!(S3Prefix::default().key(&floor), format!("{id}/floor"));
    assert_eq!(
        S3Prefix::parse("/music/bae/").unwrap().key(&floor),
        format!("music/bae/{id}/floor")
    );
    assert_eq!(S3Prefix::default().key_prefix_of(&files), format!("{id}/files/"));
    assert_eq!(
        S3Prefix::parse("music").unwrap().key_prefix_of(&files),
        format!("music/{id}/files/")
    );
}

#[test]
fn keys_start_with_the_prefix_and_a_slash() {
    assert_eq!(S3Prefix::parse("dev").unwrap().key_prefix(), "dev/");
    assert_eq!(S3Prefix::parse("/music/bae/").unwrap().key_prefix(), "music/bae/");
}

#[test]
fn recorded_prefixes_decode_only_in_their_canonical_spelling() {
    let prefix = S3Prefix::parse("/music/bae/").unwrap();
    let json = serde_json::to_string(&prefix).unwrap();
    assert_eq!(json, r#""music/bae""#);
    assert_eq!(serde_json::from_str::<S3Prefix>(&json).unwrap(), prefix);
    assert_eq!(serde_json::from_str::<S3Prefix>(r#""""#).unwrap(), S3Prefix::default());
    for recorded in [r#""music/""#, r#""/music""#, r#""/""#, r#""a//b""#, r#""a.b""#] {
        assert!(serde_json::from_str::<S3Prefix>(recorded).is_err(), "{recorded}");
    }
}

#[test]
fn a_refusal_round_trips_through_serde() {
    for error in [
        S3PrefixError::EmptySegment,
        S3PrefixError::InvalidCharacter { segment: "bae.lib".into(), character: '.' },
        S3PrefixError::SegmentTooLong { segment: "b".repeat(256) },
    ] {
        let json = serde_json::to_string(&error).unwrap();
        assert_eq!(serde_json::from_str::<S3PrefixError>(&json).unwrap(), error);
    }
}
