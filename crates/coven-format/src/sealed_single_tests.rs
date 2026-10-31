use super::*;
use crate::test_utils::STORE_ID;
use crate::{test_utils, Object};
use coven_crypto::ObjectHasher;

/// Maximum complete join-request envelope size.
const MAX_SINGLE_CHUNK_OBJECT: usize = 3 + 4 + MAX_OBJECT + SEALED_OBJECT_CHUNK_OVERHEAD + 64;

#[test]
fn each_single_frame_envelope_authenticates_its_routing_and_signature() {
    for object in [Object::JoinRequest(test_utils::join_request())] {
        let prefix = SingleChunkPrefix::JoinRequest;
        let path =
            crate::path::ObjectPath::join_request(STORE_ID, test_utils::join_request().invite);
        let path = path.as_str();
        let plain = object.encode().unwrap();
        let aad = prefix.encode().unwrap();
        let key = test_utils::invite().secret.join_request_key();
        let seal = |plain: &[u8]| key.seal_object_chunk(path, &aad, 0, 0, plain);
        let open =
            |path: &str, aad: &[u8], chunk: &[u8]| key.open_object_chunk(path, aad, 0, 0, chunk);
        let chunk = seal(&plain).unwrap();
        let before_signature = prefix.encode_chunk(&chunk).unwrap();
        let mut hash = ObjectHasher::new();
        hash.update(&before_signature);
        let digest = hash.finish();
        let signature = test_utils::member_keys().sign_object(path, &digest);
        let object = SingleChunkObject::JoinRequest { chunk: &chunk, signature };
        let bytes = object.encode().unwrap();
        let decoded = SingleChunkObject::decode(&bytes).unwrap();
        assert_eq!(decoded.encode().unwrap(), bytes);
        assert_eq!(decoded.prefix(), prefix);
        assert_eq!(decoded.signed_bytes().unwrap(), before_signature);
        assert_eq!(open(path, &aad, decoded.chunk()).unwrap(), plain);
        assert_eq!(
            Object::decode(&plain).unwrap(),
            Object::JoinRequest(test_utils::join_request())
        );
        test_utils::member().signing.verify_object(path, &digest, decoded.signature()).unwrap();
        let other_store =
            path.replacen(&STORE_ID.to_string(), "20202020-2020-2020-2020-202020202020", 1);
        assert!(open(&other_store, &aad, decoded.chunk()).is_err());
        assert!(test_utils::member()
            .signing
            .verify_object(&other_store, &digest, decoded.signature())
            .is_err());
        let mut changed_prefix = aad.clone();
        changed_prefix[0] ^= 1;
        assert!(open(path, &changed_prefix, decoded.chunk()).is_err());
        for end in 0..bytes.len() {
            assert!(SingleChunkObject::decode(&bytes[..end]).is_err());
        }
        let mut changed = bytes.clone();
        changed.push(0);
        assert!(matches!(SingleChunkObject::decode(&changed), Err(Error::TrailingBytes)));
        changed = bytes.clone();
        changed[2] = 2;
        assert!(matches!(SingleChunkObject::decode(&changed), Err(Error::UnsupportedVersion(2))));
        changed = bytes.clone();
        changed[aad.len()..aad.len() + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(SingleChunkObject::decode(&changed), Err(Error::Limit { .. })));
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
        let mut bytes = SingleChunkPrefix::JoinRequest
            .encode_chunk(&vec![0; length + SEALED_OBJECT_CHUNK_OVERHEAD])
            .unwrap();
        bytes.extend_from_slice(&[0; 64]);
        assert!(SingleChunkObject::decode(&bytes).is_ok());
        assert_eq!(bytes.len(), 3 + length + 44 + 64);
        if length == MAX_OBJECT {
            assert_eq!(bytes.len(), MAX_SINGLE_CHUNK_OBJECT);
        }
    }
}
