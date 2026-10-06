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
            let sealed = keys
                .seal_object_chunk("devices/1/3", section, index, b"row bytes")
                .unwrap();
            assert_eq!(sealed.len(), 9 + SEALED_OBJECT_CHUNK_OVERHEAD);
            let repeated = keys
                .seal_object_chunk("devices/1/3", section, index, b"row bytes")
                .unwrap();
            assert_ne!(&sealed[..24], &repeated[..24]);
            assert_eq!(
                keys.open_object_chunk("devices/1/3", section, index, &sealed)
                    .unwrap(),
                b"row bytes"
            );
            for (path, part, chunk) in [
                ("devices/2/3", section, index),
                ("devices/1/3", section ^ 1, index),
                ("devices/1/3", section, index ^ 1),
            ] {
                assert!(matches!(
                    keys.open_object_chunk(path, part, chunk, &sealed),
                    Err(CryptoError::Authentication)
                ));
            }
            let foreign = StoreKey::generate(KeyId(key_ids.new_id()))
                .unwrap()
                .derive();
            assert!(foreign
                .open_object_chunk("devices/1/3", section, index, &sealed)
                .is_err());
            assert!(keys.open_object("devices/1/3", &sealed).is_err());
            let one_piece = keys.seal_object("devices/1/3", b"row bytes").unwrap();
            assert!(keys
                .open_object_chunk("devices/1/3", section, index, &one_piece)
                .is_err());
            for end in 0..sealed.len() {
                assert!(keys
                    .open_object_chunk("devices/1/3", section, index, &sealed[..end])
                    .is_err());
                let mut altered = sealed.clone();
                altered[end] ^= 1;
                assert!(keys
                    .open_object_chunk("devices/1/3", section, index, &altered)
                    .is_err());
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
            keys.seal_object_chunk("devices/1/1", section, index, b"same bytes")
                .unwrap()
        })
        .collect();
    for (source, chunk) in chunks.iter().enumerate() {
        for (target, &(section, index)) in coordinates.iter().enumerate() {
            let result = keys.open_object_chunk("devices/1/1", section, index, chunk);
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
    assert!(keys
        .member_id()
        .verify_object("devices/1/4", &digest, &signature)
        .is_err());
    assert!(MemberKeys::generate()
        .unwrap()
        .member_id()
        .verify_object(path, &digest, &signature)
        .is_err());
    for offset in 0..bytes.len() {
        let mut changed = *bytes;
        changed[offset] ^= 1;
        let mut stream = ObjectHasher::new();
        stream.update(&changed);
        assert!(keys
            .member_id()
            .verify_object(path, &stream.finish(), &signature)
            .is_err());
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
        assert!(keys.member_id().verify(&other_message, &signature).is_err());
        assert!(keys
            .member_id()
            .verify_object(path, &digest, &keys.sign(&other_message))
            .is_err());
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
#[should_panic(expected = "storage paths must be nonempty")]
fn object_chunk_requires_a_path() {
    let key_ids = SequentialIds::new();
    let _sealed = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive()
        .seal_object_chunk("", 0, 0, b"header");
}

#[test]
#[should_panic(expected = "storage paths must be nonempty")]
fn object_signature_requires_a_path() {
    let _signature = MemberKeys::generate()
        .unwrap()
        .sign_object("", &ObjectHasher::new().finish());
}
