use super::*;
use crate::{
    DropboxLocation, S3Checksums, S3CopyCondition, S3CopyPublication, S3Listing,
    S3MultipartTimestamp, S3Prefix, S3Profile, S3ProfileName, S3ResponseHeaders, S3Revision,
    S3Settings, S3Signing, S3SinglePut,
};
use coven_crypto::SecretText;
use coven_format::code_fields::{pack, FieldReader};

fn s3(endpoint: Option<&str>, profile: Option<S3Profile>, prefix: &str) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "ana-music".into(),
        region: "us-east-1".into(),
        endpoint: endpoint.map(|url| url.parse().unwrap()),
        profile,
        prefix: S3Prefix::parse(prefix).unwrap(),
    }
}

fn detected() -> S3Settings {
    S3Settings {
        signing: S3Signing::AwsSigV4,
        single_put: S3SinglePut::IfNoneMatch,
        copy_publication: S3CopyPublication::Multipart(S3CopyCondition::CloudflareIfNoneMatch),
        multipart_timestamp: S3MultipartTimestamp::Start,
        checksums: S3Checksums::Absent,
        listing: S3Listing::V1,
        revision: S3Revision::ETag,
        response_headers: S3ResponseHeaders::S3,
        request_checksums: true,
    }
}

fn key() -> S3Credentials {
    S3Credentials {
        access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
        secret_access_key: SecretText::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
    }
}

/// One location of each provider and shape, as setup records them.
fn locations() -> Vec<StorageConfig> {
    vec![
        s3(None, None, "bae"),
        s3(None, Some(S3Profile::Named(S3ProfileName::Aws)), ""),
        s3(Some("https://storage.googleapis.com"), None, "bae"),
        s3(
            Some("https://0123456789abcdef.r2.cloudflarestorage.com"),
            Some(S3Profile::Named(S3ProfileName::R2)),
            "",
        ),
        s3(
            Some("http://nas.local:9000"),
            Some(S3Profile::Named(S3ProfileName::RustFs)),
            "music/bae",
        ),
        s3(
            Some("https://s3.example.net/"),
            Some(S3Profile::Custom(detected())),
            "",
        ),
        StorageConfig::Dropbox {
            location: DropboxLocation::AppFolder,
            account_label: None,
        },
        StorageConfig::Dropbox {
            location: DropboxLocation::SharedFolder {
                namespace_id: "12345678901".into(),
            },
            account_label: Some("ana@example.com".into()),
        },
        StorageConfig::OneDrive {
            drive_id: "b!drive".into(),
            folder_id: "01FOLDER".into(),
            account_label: Some("ana@example.com".into()),
        },
        StorageConfig::OneDrive {
            drive_id: "b!drive".into(),
            folder_id: "01FOLDER".into(),
            account_label: None,
        },
        StorageConfig::CloudKit {
            container: "iCloud.fm.bae".into(),
            owner: "_0123456789abcdef".into(),
            zone: "store".into(),
        },
    ]
}

fn reencode(bytes: &[u8]) -> Result<SecretBytes, StorageError> {
    RestoreStorage::decode(bytes)?.encode()
}

fn refusal(bytes: &[u8]) -> StorageFailure {
    RestoreStorage::decode(bytes).err().unwrap().failure()
}

fn packed(location: StorageConfig) -> Vec<u8> {
    RestoreStorage {
        location,
        s3_key: None,
    }
    .encode()
    .unwrap()
    .as_bytes()
    .to_vec()
}

#[test]
fn every_location_round_trips_with_and_without_an_s3_key() {
    for location in locations() {
        let keys = if location.provider() == CloudProvider::S3 {
            vec![None, Some(key())]
        } else {
            vec![None]
        };
        for s3_key in keys {
            let storage = RestoreStorage {
                location: location.clone(),
                s3_key: s3_key.clone(),
            };
            let bytes = storage.encode().unwrap();
            assert_eq!(bytes.capacity(), bytes.as_bytes().len());
            let decoded = RestoreStorage::decode(bytes.as_bytes()).unwrap();
            assert_eq!(decoded.location, location);
            let parts = |key: &Option<S3Credentials>| {
                key.as_ref().map(|key| {
                    (
                        key.access_key_id.clone(),
                        key.secret_access_key.as_str().to_owned(),
                    )
                })
            };
            assert_eq!(parts(&decoded.s3_key), parts(&s3_key));
            assert_eq!(decoded.encode().unwrap().as_bytes(), bytes.as_bytes());
        }
    }
}

#[test]
fn every_truncation_bit_change_and_trailing_byte_is_refused_or_canonical() {
    for location in locations() {
        let s3_key = (location.provider() == CloudProvider::S3).then(key);
        let bytes = RestoreStorage { location, s3_key }.encode().unwrap();
        let bytes = bytes.as_bytes();
        for end in 0..bytes.len() {
            assert_eq!(refusal(&bytes[..end]), StorageFailure::Encoding, "{end}");
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert_eq!(refusal(&trailing), StorageFailure::Encoding);
        let mut changed = bytes.to_vec();
        for index in 0..changed.len() {
            for bit in 0..8 {
                changed[index] ^= 1 << bit;
                if let Ok(encoded) = reencode(&changed) {
                    assert_eq!(encoded.as_bytes(), changed);
                }
                changed[index] ^= 1 << bit;
            }
        }
    }
}

#[test]
fn the_layout_is_the_providers_tag_then_its_own_fields() {
    // S3, `ana-music`, `us-east-1`, no endpoint, no profile, `store`, no key.
    assert_eq!(
        packed(s3(None, None, "store")),
        b"\0\0\0\0\x09ana-music\0\0\0\x09us-east-1\0\0\0\0\0\x05store\0"
    );
    let bytes = RestoreStorage {
        location: s3(None, None, "store"),
        s3_key: Some(S3Credentials {
            access_key_id: "id".into(),
            secret_access_key: SecretText::new("secret".into()),
        }),
    }
    .encode()
    .unwrap();
    assert!(bytes
        .as_bytes()
        .ends_with(b"\x05store\x01\0\0\0\x02id\0\0\0\x06secret"));
    // Only an S3 location says whether a key follows.
    assert_eq!(
        packed(StorageConfig::CloudKit {
            container: "c".into(),
            owner: "o".into(),
            zone: "z".into(),
        }),
        b"\x03\0\0\0\x01c\0\0\0\x01o\0\0\0\x01z"
    );
    assert_eq!(
        packed(StorageConfig::Dropbox {
            location: DropboxLocation::SharedFolder {
                namespace_id: "n".into()
            },
            account_label: Some("a".into()),
        }),
        b"\x01\x01\0\0\0\x01n\x01\0\0\0\x01a"
    );
    assert_eq!(
        packed(StorageConfig::OneDrive {
            drive_id: "d".into(),
            folder_id: "f".into(),
            account_label: None,
        }),
        b"\x02\0\0\0\x01d\0\0\0\x01f\0"
    );
}

#[test]
fn the_format_fixtures_storage_part_is_this_encoding() {
    let fixture = coven_format::test_utils::restore();
    let decoded = RestoreStorage::decode(fixture.storage.as_bytes()).unwrap();
    assert_eq!(
        decoded.location,
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "region".into(),
            endpoint: None,
            profile: None,
            prefix: S3Prefix::parse("store").unwrap(),
        }
    );
    assert!(decoded.s3_key.is_none());
    assert_eq!(
        decoded.encode().unwrap().as_bytes(),
        fixture.storage.as_bytes()
    );
}

#[test]
fn a_key_restores_only_s3_and_must_be_whole() {
    for location in locations() {
        if location.provider() == CloudProvider::S3 {
            continue;
        }
        let storage = RestoreStorage {
            location,
            s3_key: Some(key()),
        };
        assert_eq!(
            storage.encode().err().unwrap().failure(),
            StorageFailure::InvalidConfiguration
        );
    }
    for (id, secret) in [("", "secret"), ("id", "")] {
        let storage = RestoreStorage {
            location: s3(None, None, "bae"),
            s3_key: Some(S3Credentials {
                access_key_id: id.into(),
                secret_access_key: SecretText::new(secret.into()),
            }),
        };
        assert_eq!(
            storage.encode().err().unwrap().failure(),
            StorageFailure::InvalidConfiguration
        );
    }
    // The same key with its one-byte secret cut to nothing is refused on reading.
    let mut bytes = RestoreStorage {
        location: s3(None, None, "bae"),
        s3_key: Some(S3Credentials {
            access_key_id: "id".into(),
            secret_access_key: SecretText::new("s".into()),
        }),
    }
    .encode()
    .unwrap()
    .as_bytes()
    .to_vec();
    bytes.truncate(bytes.len() - 5);
    bytes.extend_from_slice(&[0; 4]);
    assert_eq!(refusal(&bytes), StorageFailure::InvalidConfiguration);
}

#[test]
fn spellings_and_tags_the_encoder_never_writes_are_refused() {
    // An endpoint URL that isn't its own serialization: no trailing slash.
    let canonical = packed(s3(Some("https://s3.example.net"), None, ""));
    let url = b"https://s3.example.net/";
    let at = canonical
        .windows(url.len())
        .position(|window| window == url)
        .unwrap();
    let mut bytes = canonical[..at - 4].to_vec();
    bytes.extend_from_slice(&((url.len() - 1) as u32).to_be_bytes());
    bytes.extend_from_slice(&url[..url.len() - 1]);
    bytes.extend_from_slice(&canonical[at + url.len()..]);
    assert_eq!(refusal(&bytes), StorageFailure::Encoding);
    // A base prefix with a slash at its end.
    let mut bytes = packed(s3(None, None, "bae"));
    bytes.truncate(bytes.len() - 8);
    bytes.extend_from_slice(b"\0\0\0\x04bae/\0");
    assert_eq!(refusal(&bytes), StorageFailure::Encoding);
    // An unknown provider, or Dropbox location.
    let mut bytes = packed(s3(None, None, "bae"));
    bytes[0] = 4;
    assert_eq!(refusal(&bytes), StorageFailure::Encoding);
    let mut bytes = packed(StorageConfig::Dropbox {
        location: DropboxLocation::AppFolder,
        account_label: None,
    });
    bytes[1] = 2;
    assert_eq!(refusal(&bytes), StorageFailure::Encoding);
    // An unknown profile kind, or catalogued name.
    let named = packed(s3(None, Some(S3Profile::Named(S3ProfileName::Aws)), ""));
    // Tag, bucket, region and no endpoint come before the profile.
    let profile = 1 + 13 + 13 + 1;
    assert_eq!(&named[profile..profile + 3], b"\x01\0\0");
    for (index, byte) in [(profile + 1, 2), (profile + 2, 7)] {
        let mut bytes = named.clone();
        bytes[index] = byte;
        assert_eq!(refusal(&bytes), StorageFailure::Encoding);
    }
    // A location that reads but isn't valid: an empty bucket.
    let mut bytes = packed(s3(None, None, ""));
    bytes.splice(1..14, [0, 0, 0, 0]);
    assert_eq!(refusal(&bytes), StorageFailure::InvalidConfiguration);
}

#[test]
fn custom_settings_take_twelve_bits_and_every_unused_value_is_refused() {
    let mut valid = 0;
    for bits in 0..=u16::MAX {
        let bytes = [1, (bits >> 8) as u8, bits as u8];
        let Ok(profile) = S3Profile::unpack(&mut FieldReader::new(&bytes).unwrap()) else {
            continue;
        };
        valid += 1;
        assert_eq!(pack(|out| profile.pack(out)).unwrap().as_bytes(), bytes);
    }
    // Two signings and single PUTs, seven copy publications, two multipart
    // timestamps, three checksum modes, and two of each of the other four.
    assert_eq!(valid, 2 * 2 * 7 * 2 * 3 * 2 * 2 * 2 * 2);
}
