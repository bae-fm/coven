use super::*;

#[test]
fn restore_storage_is_exactly_the_location_for_every_provider() {
    for (location, expected) in [
        (
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "region".into(),
                endpoint: None,
                profile: None,
                prefix: "store".into(),
            },
            r#"{"S3":{"bucket":"bucket","region":"region","endpoint":null,"profile":null,"prefix":"store"}}"#,
        ),
        (
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "region".into(),
                endpoint: Some("http://localhost:9000".parse().unwrap()),
                profile: Some(crate::S3ProfileName::Gcs),
                prefix: "store".into(),
            },
            r#"{"S3":{"bucket":"bucket","region":"region","endpoint":"http://localhost:9000/","profile":"Gcs","prefix":"store"}}"#,
        ),
        (
            StorageConfig::GoogleDrive {
                folder_id: "folder".into(),
            },
            r#"{"GoogleDrive":{"folder_id":"folder"}}"#,
        ),
        (
            StorageConfig::Dropbox {
                location: crate::DropboxLocation::SharedFolder {
                    namespace_id: "folder".into(),
                },
            },
            r#"{"Dropbox":{"location":{"SharedFolder":{"namespace_id":"folder"}}}}"#,
        ),
        (
            StorageConfig::OneDrive {
                drive_id: "drive".into(),
                folder_id: "folder".into(),
            },
            r#"{"OneDrive":{"drive_id":"drive","folder_id":"folder"}}"#,
        ),
        (
            StorageConfig::CloudKit {
                container: "container".into(),
                owner: "owner".into(),
                zone: "zone".into(),
            },
            r#"{"CloudKit":{"container":"container","owner":"owner","zone":"zone"}}"#,
        ),
    ] {
        let bytes = location.encode().unwrap();
        assert_eq!(bytes.as_bytes(), expected.as_bytes());
        assert_eq!(StorageConfig::decode(bytes.as_bytes()).unwrap(), location);
        let mut with_credentials: serde_json::Value = serde_json::from_str(expected).unwrap();
        let payload = with_credentials
            .as_object_mut()
            .unwrap()
            .values_mut()
            .next()
            .unwrap()
            .as_object_mut()
            .unwrap();
        for field in [
            "access_key_id",
            "secret_access_key",
            "access_token",
            "refresh_token",
        ] {
            payload.insert(field.into(), serde_json::json!("secret"));
        }
        assert!(StorageConfig::decode(&serde_json::to_vec(&with_credentials).unwrap()).is_err());
    }
}

#[test]
fn credential_bearing_restore_payloads_are_refused_without_an_alternate_reader() {
    for bytes in [
        br#"{"S3":{"location":{"S3":{"bucket":"bucket","region":"region","endpoint":null,"profile":null,"prefix":"store"}},"credentials":{"access_key_id":"key","secret_access_key":"secret"}}}"#.as_slice(),
        br#"{"Account":{"Dropbox":{"location":{"SharedFolder":{"namespace_id":"folder"}}}}}"#,
        br#"{"S3":{"bucket":"bucket","region":"region","endpoint":"https://key:secret@storage.example","profile":null,"prefix":"store"}}"#,
        br#"{"S3":{"bucket":"","region":"region","endpoint":null,"profile":null,"prefix":"store"}}"#,
    ] {
        assert!(StorageConfig::decode(bytes).is_err());
    }
}
