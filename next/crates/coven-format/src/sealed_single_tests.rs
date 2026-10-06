use super::*;
use crate::{test_utils, Object};
use coven_crypto::{ObjectHasher, StoreKey};

#[test]
fn each_single_frame_envelope_authenticates_its_routing_and_signature() {
    let key = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]);
    let objects = test_utils::objects();
    for (prefix, object, path) in [
        (
            SingleChunkPrefix::StoreLog(key.id()),
            &objects[0],
            "store-log/1/1",
        ),
        (
            SingleChunkPrefix::PostedPositions(key.id()),
            &objects[4],
            "positions/1",
        ),
        (
            SingleChunkPrefix::JoinRequest,
            &objects[3],
            "join-requests/55555555-5555-5555-5555-555555555555",
        ),
    ] {
        let plain = object.encode().unwrap();
        let aad = prefix.encode().unwrap();
        let seal = |plain: &[u8]| match prefix {
            SingleChunkPrefix::JoinRequest => test_utils::invite()
                .secret
                .join_request_key()
                .seal_object_chunk(path, &aad, 0, 0, plain),
            _ => key.derive().seal_object_chunk(path, &aad, 0, 0, plain),
        };
        let open = |aad: &[u8], chunk: &[u8]| match prefix {
            SingleChunkPrefix::JoinRequest => test_utils::invite()
                .secret
                .join_request_key()
                .open_object_chunk(path, aad, 0, 0, chunk),
            _ => key.derive().open_object_chunk(path, aad, 0, 0, chunk),
        };
        let chunk = seal(&plain).unwrap();
        let before_signature = prefix.encode_chunk(&chunk).unwrap();
        let mut hash = ObjectHasher::new();
        hash.update(&before_signature);
        let digest = hash.finish();
        let signature = test_utils::member_keys().sign_object(path, &digest);
        let object = match prefix {
            SingleChunkPrefix::StoreLog(key) => SingleChunkObject::StoreLog {
                key,
                chunk: &chunk,
                signature,
            },
            SingleChunkPrefix::PostedPositions(key) => {
                SingleChunkObject::PostedPositions { key, chunk: &chunk }
            }
            SingleChunkPrefix::JoinRequest => SingleChunkObject::JoinRequest {
                chunk: &chunk,
                signature,
            },
        };
        let bytes = object.encode().unwrap();
        let decoded = SingleChunkObject::decode(&bytes).unwrap();
        assert_eq!(decoded.encode().unwrap(), bytes);
        assert_eq!(decoded.prefix(), prefix);
        assert_eq!(decoded.signed_bytes().unwrap(), before_signature);
        assert_eq!(open(&aad, decoded.chunk()).unwrap(), plain);
        Object::decode(&plain).unwrap();
        if let Some(signature) = decoded.signature() {
            test_utils::member()
                .signing
                .verify_object(path, &digest, signature)
                .unwrap();
        }
        let mut changed_prefix = aad.clone();
        changed_prefix[0] ^= 1;
        assert!(open(&changed_prefix, decoded.chunk()).is_err());
        for end in 0..bytes.len() {
            assert!(SingleChunkObject::decode(&bytes[..end]).is_err());
        }
        let mut changed = bytes.clone();
        changed.push(0);
        assert!(matches!(
            SingleChunkObject::decode(&changed),
            Err(Error::TrailingBytes)
        ));
        changed = bytes.clone();
        changed[2] = 2;
        assert!(matches!(
            SingleChunkObject::decode(&changed),
            Err(Error::UnsupportedVersion(2))
        ));
        changed = bytes.clone();
        changed[aad.len()..aad.len() + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            SingleChunkObject::decode(&changed),
            Err(Error::Limit { .. })
        ));
    }
}

#[test]
fn a_chunk_holds_one_bounded_frame() {
    for length in [0, 1, 6, MAX_OBJECT + 1] {
        assert!(SingleChunkPrefix::JoinRequest
            .encode_chunk(&vec![0; length + SEALED_OBJECT_CHUNK_OVERHEAD])
            .is_err());
    }
    for length in [crate::FRAME_PREFIX_LEN, MAX_OBJECT] {
        let bytes = SingleChunkPrefix::PostedPositions(KeyId(uuid::Uuid::nil()))
            .encode_chunk(&vec![0; length + SEALED_OBJECT_CHUNK_OVERHEAD])
            .unwrap();
        assert!(SingleChunkObject::decode(&bytes).is_ok());
        assert_eq!(bytes.len(), 19 + length + 44);
    }
}
