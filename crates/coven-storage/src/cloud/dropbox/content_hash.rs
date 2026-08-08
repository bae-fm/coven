use crate::cloud::{CloudHomeError, ExactUpload, ExactUploadSource};

const BLOCK_SIZE: usize = 4 * 1024 * 1024;

pub(super) fn for_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut aggregate = Sha256::new();
    for block in bytes.chunks(BLOCK_SIZE) {
        aggregate.update(Sha256::digest(block));
    }
    hex::encode(aggregate.finalize())
}

pub(super) async fn for_upload(upload: &ExactUpload<'_>) -> Result<String, CloudHomeError> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncReadExt;

    match upload.source() {
        ExactUploadSource::Bytes(bytes) => Ok(for_bytes(bytes)),
        ExactUploadSource::File(path) => {
            let mut file = tokio::fs::File::open(path).await?;
            let mut aggregate = Sha256::new();
            let mut block = vec![0_u8; BLOCK_SIZE];
            loop {
                let mut filled = 0;
                while filled < block.len() {
                    let read = file.read(&mut block[filled..]).await?;
                    if read == 0 {
                        break;
                    }
                    filled += read;
                }
                if filled == 0 {
                    break;
                }
                aggregate.update(Sha256::digest(&block[..filled]));
                if filled < block.len() {
                    break;
                }
            }
            Ok(hex::encode(aggregate.finalize()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_dropbox_empty_and_single_block_vectors() {
        assert_eq!(
            for_bytes(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            for_bytes(b"abc"),
            "4f8b42c22dd3729b519ba6f68d2da7cc5b2d606d05daed5ad5128cc03e6c6358"
        );
    }
}
