use super::*;

const PATH: &str = "files/1/11111111-1111-1111-1111-111111111111";
const HEADER: [u8; 15] = [38, 0, 1, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 10];

#[test]
fn file_chunk_matches_an_independent_xchacha_vector() {
    // Python ctypes calling libsodium, with an independently encoded context.
    let key = FileKey::from_bytes([0x77; 32]);
    let sealed = key.seal_chunk(PATH, &HEADER, 0, b"hello file");
    assert_eq!(
        hex::encode(&sealed),
        "5621e82de767c089ad29e2bfefbb3bb31f83a3de5a6f63164470"
    );
    assert_eq!(
        key.open_chunk(PATH, &HEADER, 0, &sealed).unwrap(),
        b"hello file"
    );
    assert_eq!(nonce(0), [0; 24]);
    assert_eq!(
        hex::encode(nonce(0x0102030405060708)),
        "000000000000000000000000000000000102030405060708"
    );
    assert_eq!(
        nonce(u64::MAX),
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255]
    );
}

#[test]
fn files_bind_the_key_path_header_and_big_endian_index() {
    let key = FileKey::from_bytes([0x77; 32]);
    let wrong = FileKey::from_bytes([0x78; 32]);
    for index in [0, 1, 7, u64::MAX] {
        let sealed = key.seal_chunk(PATH, &HEADER, index, b"hello file");
        assert_eq!(sealed, key.seal_chunk(PATH, &HEADER, index, b"hello file"));
        assert!(wrong.open_chunk(PATH, &HEADER, index, &sealed).is_err());
        assert!(key
            .open_chunk(
                "files/1/22222222-2222-2222-2222-222222222222",
                &HEADER,
                index,
                &sealed
            )
            .is_err());
        assert!(key.open_chunk(PATH, &HEADER, index ^ 1, &sealed).is_err());
        let context = cipher::context(&[
            b"coven/file-chunk/v1",
            PATH.as_bytes(),
            &HEADER,
            &index.to_be_bytes(),
        ]);
        assert_eq!(
            cipher::open(&[0x77; 32], &nonce(index), &context, &sealed).unwrap(),
            b"hello file"
        );
        for i in 0..HEADER.len() {
            for bit in 0..8 {
                let mut header = HEADER;
                header[i] ^= 1 << bit;
                assert!(key.open_chunk(PATH, &header, index, &sealed).is_err());
            }
        }
        for i in 0..sealed.len() {
            assert!(key.open_chunk(PATH, &HEADER, index, &sealed[..i]).is_err());
            for bit in 0..8 {
                let mut changed = sealed.clone();
                changed[i] ^= 1 << bit;
                assert!(key.open_chunk(PATH, &HEADER, index, &changed).is_err());
            }
        }
        // The right nonce does not compensate for the wrong authenticated index.
        let context = cipher::context(&[
            b"coven/file-chunk/v1",
            PATH.as_bytes(),
            &HEADER,
            &(index ^ 1).to_be_bytes(),
        ]);
        assert!(cipher::open(&[0x77; 32], &nonce(index), &context, &sealed).is_err());
    }
}

#[test]
fn independent_keys_survive_secret_export_without_printing_them() {
    let key = FileKey::generate().unwrap();
    let bytes = key.to_secret_bytes();
    assert_eq!(bytes.as_bytes().len(), 32);
    let restored = FileKey::from_bytes(bytes.as_bytes().try_into().unwrap());
    let sealed = key.seal_chunk(PATH, &HEADER, 0, b"hello file");
    assert_eq!(
        restored.open_chunk(PATH, &HEADER, 0, &sealed).unwrap(),
        b"hello file"
    );
    assert!(FileKey::generate()
        .unwrap()
        .open_chunk(PATH, &HEADER, 0, &sealed)
        .is_err());
    assert_eq!(format!("{key:?}"), "FileKey([REDACTED])");
    assert_eq!(format!("{bytes:?}"), "SecretBytes([REDACTED])");
}
