use super::*;

#[test]
fn pending_asset_retains_staging_after_its_provider_is_dropped() {
    let files = AssetFiles::new().unwrap();
    let asset = files.stage(b"encrypted").unwrap();
    let path = asset.path().to_owned();
    let directory = path.parent().unwrap().to_owned();
    assert_eq!(files.read(&path, 9).unwrap(), b"encrypted");
    drop(files);
    assert_eq!(std::fs::read(&path).unwrap(), b"encrypted");
    asset.close().unwrap();
    assert!(!path.exists());
    assert!(!directory.exists());
}

#[test]
fn oversized_or_missing_assets_fail_instead_of_returning_truncated_bytes() {
    let files = AssetFiles::new().unwrap();
    let asset = files.stage(b"encrypted").unwrap();
    let path = asset.path().to_owned();
    assert_eq!(
        files.read(&path, 8).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    asset.close().unwrap();
    assert_eq!(
        files.read(&path, 9).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
}
