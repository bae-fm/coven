use crate::cloud::{CloudHomeError, ExactUpload, ExactUploadSource};

pub(super) async fn md5(upload: &ExactUpload<'_>) -> Result<String, CloudHomeError> {
    use md5::{Digest, Md5};
    use tokio::io::AsyncReadExt;

    let mut digest = Md5::new();
    match upload.source() {
        ExactUploadSource::Bytes(bytes) => digest.update(bytes),
        ExactUploadSource::File(path) => {
            let mut file = tokio::fs::File::open(path).await?;
            let mut chunk = vec![0_u8; 1024 * 1024];
            loop {
                let read = file.read(&mut chunk).await?;
                if read == 0 {
                    break;
                }
                digest.update(&chunk[..read]);
            }
        }
    }
    Ok(hex::encode(digest.finalize()))
}
