use crate::{
    cipher, CircleId, CircleKey, CryptoError, MemberKeys, ObjectHasher, StoreKey,
    SEALED_OBJECT_CHUNK_OVERHEAD,
};
use coven_foundation::id_source::{IdSource, KeyId, SequentialIds};

#[test]
fn object_chunks_bind_every_coordinate_and_use_stored_random_nonces() {
    let key_ids = SequentialIds::new();
    for keys in [
        StoreKey::generate(KeyId(key_ids.new_id()))
            .unwrap()
            .derive(),
        CircleKey::generate(CircleId(uuid::Uuid::from_u128(1)), KeyId(key_ids.new_id()))
            .unwrap()
            .derive(),
    ] {
        for (section, index) in [(0, 0), (1, 2), (u64::MAX, u64::MAX)] {
            for plaintext in [b"".as_slice(), b"row bytes"] {
                let sealed = keys
                    .seal_object_chunk(
                        "devices/1/3",
                        b"cleartext prefix",
                        section,
                        index,
                        plaintext,
                    )
                    .unwrap();
                assert_eq!(sealed.len(), plaintext.len() + SEALED_OBJECT_CHUNK_OVERHEAD);
                let repeated = keys
                    .seal_object_chunk(
                        "devices/1/3",
                        b"cleartext prefix",
                        section,
                        index,
                        plaintext,
                    )
                    .unwrap();
                assert_ne!(&sealed[..24], &repeated[..24]);
                assert_eq!(
                    keys.open_object_chunk(
                        "devices/1/3",
                        b"cleartext prefix",
                        section,
                        index,
                        &sealed
                    )
                    .unwrap(),
                    plaintext
                );
                for (path, part, chunk) in [
                    ("devices/2/3", section, index),
                    ("devices/1/3", section ^ 1, index),
                    ("devices/1/3", section, index ^ 1),
                ] {
                    assert!(matches!(
                        keys.open_object_chunk(path, b"cleartext prefix", part, chunk, &sealed),
                        Err(CryptoError::Authentication)
                    ));
                }
                let foreign = StoreKey::generate(KeyId(key_ids.new_id()))
                    .unwrap()
                    .derive();
                assert!(matches!(
                    foreign.open_object_chunk(
                        "devices/1/3",
                        b"cleartext prefix",
                        section,
                        index,
                        &sealed
                    ),
                    Err(CryptoError::Authentication)
                ));
                for end in 0..sealed.len() {
                    assert!(matches!(
                        (
                            end < 24,
                            keys.open_object_chunk(
                                "devices/1/3",
                                b"cleartext prefix",
                                section,
                                index,
                                &sealed[..end]
                            )
                        ),
                        (true, Err(CryptoError::Malformed))
                            | (false, Err(CryptoError::Authentication))
                    ));
                    let mut altered = sealed.clone();
                    altered[end] ^= 1;
                    assert!(matches!(
                        keys.open_object_chunk(
                            "devices/1/3",
                            b"cleartext prefix",
                            section,
                            index,
                            &altered
                        ),
                        Err(CryptoError::Authentication)
                    ));
                }
                let mut trailing = sealed;
                trailing.push(0);
                assert!(matches!(
                    keys.open_object_chunk(
                        "devices/1/3",
                        b"cleartext prefix",
                        section,
                        index,
                        &trailing
                    ),
                    Err(CryptoError::Authentication)
                ));
            }
        }
    }
}

#[test]
fn swapping_parts_and_reordering_chunks_is_refused_even_with_identical_plaintext() {
    let key_ids = SequentialIds::new();
    let keys = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let coordinates = [(0, 0), (1, 0), (1, 1), (2, 0)];
    let chunks: Vec<_> = coordinates
        .iter()
        .map(|&(section, index)| {
            keys.seal_object_chunk(
                "devices/1/1",
                b"cleartext prefix",
                section,
                index,
                b"same bytes",
            )
            .unwrap()
        })
        .collect();
    for (source, chunk) in chunks.iter().enumerate() {
        for (target, &(section, index)) in coordinates.iter().enumerate() {
            let result =
                keys.open_object_chunk("devices/1/1", b"cleartext prefix", section, index, chunk);
            assert_eq!(result.is_ok(), source == target);
        }
    }
}

#[test]
fn streamed_object_signatures_bind_path_digest_member_and_domain() {
    let keys = MemberKeys::generate().unwrap();
    let path = "devices/1/3";
    let bytes = b"prefix, chunk lengths, nonces, encrypted bytes and authentication tags";
    let mut whole = ObjectHasher::new();
    whole.update(bytes);
    let digest = whole.finish();
    let signature = keys.sign_object(path, &digest);
    for size in 1..=bytes.len() {
        let mut stream = ObjectHasher::new();
        for piece in bytes.chunks(size) {
            stream.update(piece);
        }
        keys.member_id()
            .verify_object(path, &stream.finish(), &signature)
            .unwrap();
    }
    assert!(matches!(
        keys.member_id()
            .verify_object("devices/1/4", &digest, &signature),
        Err(CryptoError::Signature)
    ));
    assert!(matches!(
        MemberKeys::generate()
            .unwrap()
            .member_id()
            .verify_object(path, &digest, &signature),
        Err(CryptoError::Signature)
    ));
    for offset in 0..bytes.len() {
        let mut changed = *bytes;
        changed[offset] ^= 1;
        let mut stream = ObjectHasher::new();
        stream.update(&changed);
        assert!(matches!(
            keys.member_id()
                .verify_object(path, &stream.finish(), &signature),
            Err(CryptoError::Signature)
        ));
    }
    for other_message in [
        bytes.to_vec(),
        digest.as_bytes().to_vec(),
        cipher::context(&[
            b"coven/other-signature/v1",
            path.as_bytes(),
            digest.as_bytes(),
        ]),
        cipher::context(&[path.as_bytes(), digest.as_bytes()]),
    ] {
        assert!(matches!(
            keys.member_id().verify(&other_message, &signature),
            Err(CryptoError::Signature)
        ));
        assert!(matches!(
            keys.member_id()
                .verify_object(path, &digest, &keys.sign(&other_message)),
            Err(CryptoError::Signature)
        ));
    }
    // Pin the protocol message independently of the sign/verify wrappers.
    let message = cipher::context(&[
        b"coven/object-signature/v1",
        path.as_bytes(),
        digest.as_bytes(),
    ]);
    keys.member_id().verify(&message, &signature).unwrap();
}

#[test]
fn prefix_signatures_bind_exact_bytes_path_member_and_a_separate_domain() {
    let keys = MemberKeys::generate().unwrap();
    let path = "snapshots/store/1/3";
    let prefix = b"kind, version, audience, key and positions";
    let signature = keys.sign_prefix(path, prefix);
    let author = keys.member_id();
    author.verify_prefix(path, prefix, &signature).unwrap();
    author
        .verify(
            &cipher::context(&[b"coven/prefix-signature/v1", path.as_bytes(), prefix]),
            &signature,
        )
        .unwrap();
    assert!(matches!(
        author.verify_prefix("snapshots/store/2/3", prefix, &signature),
        Err(CryptoError::Signature)
    ));
    assert!(matches!(
        MemberKeys::generate()
            .unwrap()
            .member_id()
            .verify_prefix(path, prefix, &signature),
        Err(CryptoError::Signature)
    ));
    for index in 0..prefix.len() {
        let mut changed = *prefix;
        changed[index] ^= 1;
        assert!(matches!(
            author.verify_prefix(path, &changed, &signature),
            Err(CryptoError::Signature)
        ));
    }
    let mut hash = ObjectHasher::new();
    hash.update(prefix);
    let digest = hash.finish();
    assert!(matches!(
        author.verify_object(path, &digest, &signature),
        Err(CryptoError::Signature)
    ));
    assert!(matches!(
        author.verify_prefix(path, prefix, &keys.sign_object(path, &digest)),
        Err(CryptoError::Signature)
    ));
}

#[test]
fn chunk_aad_includes_the_whole_prefix_and_big_endian_coordinates() {
    let keys = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]).derive();
    let prefix = b"whole cleartext kind, version, and prefix";
    let section = 0x0102030405060708u64;
    let index = 0x1122334455667788u64;
    let sealed = keys
        .seal_object_chunk("devices/1/3", prefix, section, index, b"payload")
        .unwrap();
    let key = crate::derivation::derive_label(&[17; 32], b"coven/encryption/v1");
    let aad = cipher::context(&[
        b"coven/object-chunk/v1",
        b"devices/1/3",
        prefix,
        &section.to_be_bytes(),
        &index.to_be_bytes(),
    ]);
    assert_eq!(
        cipher::open_random(&key, &aad, &sealed).unwrap(),
        b"payload"
    );
    for i in 0..prefix.len() {
        let mut changed = prefix.to_vec();
        changed[i] ^= 1;
        assert!(matches!(
            keys.open_object_chunk("devices/1/3", &changed, section, index, &sealed),
            Err(CryptoError::Authentication)
        ));
    }
    for aad in [
        cipher::context(&[
            b"coven/object-chunk/v1",
            b"devices/1/3",
            prefix,
            &section.to_le_bytes(),
            &index.to_le_bytes(),
        ]),
        cipher::context(&[
            b"coven/object-chunk/v1",
            b"devices/1/3",
            &section.to_be_bytes(),
            &index.to_be_bytes(),
        ]),
        cipher::context(&[
            b"coven/object/v1",
            b"devices/1/3",
            prefix,
            &section.to_be_bytes(),
            &index.to_be_bytes(),
        ]),
    ] {
        assert!(matches!(
            cipher::open_random(&key, &aad, &sealed),
            Err(CryptoError::Authentication)
        ));
    }
}
