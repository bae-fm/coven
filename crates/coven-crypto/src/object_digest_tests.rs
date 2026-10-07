use super::*;

#[test]
fn object_digest_matches_sha256_known_answers() {
    for pieces in [vec![b"abc".as_slice()], vec![b"a", b"", b"bc"]] {
        let mut hasher = ObjectHasher::new();
        for piece in pieces {
            hasher.update(piece);
        }
        assert_eq!(
            hex::encode(hasher.finish().as_bytes()),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
    assert_eq!(
        hex::encode(ObjectHasher::new().finish().as_bytes()),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
