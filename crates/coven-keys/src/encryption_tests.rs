use super::*;

const TEST_AAD: &[u8] = b"encryption-test-context";

fn test_key() -> [u8; 32] {
    // Fixed test key for reproducibility
    [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ]
}

fn create_test_service() -> EncryptionService {
    EncryptionService::from_key(test_key())
}

#[test]
fn test_roundtrip_small() {
    let service = create_test_service();
    let plaintext = b"Hello, world!";

    let ciphertext = service.encrypt(plaintext, TEST_AAD);
    let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

    assert_eq!(decrypted, plaintext);
}

/// The streaming sealer (base nonce + per-chunk `seal_chunk`) produces a blob
/// the existing whole-buffer decryptor reads back unchanged, across the
/// boundaries that matter: empty, sub-chunk, exact chunk, and several
/// non-aligned chunks. `encrypt` is built on the sealer, so this also
/// guards the streaming form against drifting from the stored format.
#[test]
fn streaming_sealer_matches_whole_buffer_format() {
    let service = create_test_service();
    for len in [
        0usize,
        1,
        CHUNK_SIZE - 1,
        CHUNK_SIZE,
        CHUNK_SIZE + 1,
        200_000,
    ] {
        let plaintext: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();

        // Seal incrementally, exactly as a streaming upload would.
        let header = SealedBlobHeader::new(
            DEFAULT_BLOB_CHUNK_SIZE,
            plaintext.len() as u64,
            &NoncePolicy::RandomStored,
        );
        let mut sealer = service
            .blob_sealer(header, &NoncePolicy::RandomStored, TEST_AAD)
            .expect("the header records the policy it was built under");
        let mut streamed = header.to_bytes();
        if plaintext.is_empty() {
            streamed.extend(sealer.seal_chunk(&[]));
        } else {
            for chunk in plaintext.chunks(CHUNK_SIZE) {
                streamed.extend(sealer.seal_chunk(chunk));
            }
        }

        assert_eq!(
            streamed.len() as u64,
            chunked_encrypted_len(len as u64),
            "predicted length wrong for len={len}"
        );
        assert_eq!(
            service.decrypt(&streamed, TEST_AAD).unwrap(),
            plaintext,
            "streamed ciphertext failed to round-trip for len={len}"
        );
    }
}

/// `chunked_encrypted_len` predicts the exact byte length `encrypt`
/// produces, across the chunk boundaries that matter — so a streaming upload
/// can announce the final object size before sealing a byte.
#[test]
fn chunked_encrypted_len_matches_encrypt() {
    let service = create_test_service();
    for n in [
        0usize,
        1,
        CHUNK_SIZE - 1,
        CHUNK_SIZE,
        CHUNK_SIZE + 1,
        200_000,
    ] {
        let produced = service.encrypt(&vec![0u8; n], TEST_AAD).len() as u64;
        assert_eq!(
            chunked_encrypted_len(n as u64),
            produced,
            "predicted length wrong for n={n}"
        );
    }
}

#[test]
fn test_roundtrip_exact_chunk() {
    let service = create_test_service();
    let plaintext = vec![0x42u8; CHUNK_SIZE];

    let ciphertext = service.encrypt(&plaintext, TEST_AAD);
    let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

    assert_eq!(decrypted, plaintext);
}

#[test]
fn test_roundtrip_multiple_chunks() {
    let service = create_test_service();
    // 2.5 chunks worth of data
    let plaintext: Vec<u8> = (0..CHUNK_SIZE * 2 + CHUNK_SIZE / 2)
        .map(|i| (i % 256) as u8)
        .collect();

    let ciphertext = service.encrypt(&plaintext, TEST_AAD);
    let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

    assert_eq!(decrypted, plaintext);
}

#[test]
fn test_tamper_detection() {
    let service = create_test_service();
    let plaintext = b"Secret data";

    let mut ciphertext = service.encrypt(plaintext, TEST_AAD);

    // Tamper with the ciphertext (after nonce)
    let tamper_pos = NONCE_SIZE + 5;
    ciphertext[tamper_pos] ^= 0xFF;

    let result = service.decrypt(&ciphertext, TEST_AAD);
    assert!(result.is_err());
}

#[test]
fn truncating_trailing_chunks_fails_to_decrypt() {
    let service = create_test_service();
    let plaintext: Vec<u8> = (0..CHUNK_SIZE * 3).map(|i| (i % 251) as u8).collect();
    let ciphertext = service.encrypt(&plaintext, TEST_AAD);
    let truncated = &ciphertext[..ciphertext.len() - (CHUNK_SIZE + TAG_SIZE)];

    assert!(
        service.decrypt(truncated, TEST_AAD).is_err(),
        "a truncated multi-chunk object must fail, not return a short plaintext",
    );
}

#[test]
fn test_empty_plaintext() {
    let service = create_test_service();
    let plaintext = b"";

    let ciphertext = service.encrypt(plaintext, TEST_AAD);

    // The header, its stored base nonce, and one tag-only chunk.
    assert_eq!(
        ciphertext.len() as u64,
        SEALED_BLOB_HEADER_LEN as u64 + NONCE_SIZE as u64 + TAG_SIZE as u64,
    );

    let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();
    assert_eq!(decrypted, plaintext);
}

#[test]
fn test_single_byte() {
    let service = create_test_service();
    let plaintext = b"x";

    let ciphertext = service.encrypt(plaintext, TEST_AAD);
    let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

    assert_eq!(decrypted, plaintext);
}

#[test]
fn test_different_encryptions_different_ciphertext() {
    let service = create_test_service();
    let plaintext = b"Same message";

    let ciphertext1 = service.encrypt(plaintext, TEST_AAD);
    let ciphertext2 = service.encrypt(plaintext, TEST_AAD);

    // Different nonces = different ciphertext
    assert_ne!(ciphertext1, ciphertext2);

    // Both decrypt to same plaintext
    assert_eq!(service.decrypt(&ciphertext1, TEST_AAD).unwrap(), plaintext);
    assert_eq!(service.decrypt(&ciphertext2, TEST_AAD).unwrap(), plaintext);
}

#[test]
fn test_fingerprint_deterministic() {
    let service = create_test_service();
    assert_eq!(service.fingerprint(), service.fingerprint());
}

#[test]
fn test_fingerprint_different_keys() {
    let service1 = EncryptionService::from_key([0u8; 32]);
    let service2 = EncryptionService::from_key([1u8; 32]);
    assert_ne!(service1.fingerprint(), service2.fingerprint());
}

#[test]
fn key_fingerprint_wire_form_is_strict_lowercase_hex() {
    let fingerprint = create_test_service().seal_key_fingerprint();
    let serialized = serde_json::to_string(&fingerprint).expect("serialize fingerprint");
    assert_eq!(
        serde_json::from_str::<KeyFingerprint>(&serialized).unwrap(),
        fingerprint
    );
    assert!(fingerprint
        .to_string()
        .to_uppercase()
        .parse::<KeyFingerprint>()
        .is_err());
}

#[test]
fn key_fingerprint_is_the_full_sha256_digest() {
    let fingerprint = create_test_service().seal_key_fingerprint();
    let expected: [u8; 32] = Sha256::digest(test_key()).into();

    assert_eq!(fingerprint.as_bytes().as_slice(), expected.as_slice());
    assert_eq!(fingerprint.to_string(), hex::encode(expected));
    assert_eq!(fingerprint.to_string().len(), 64);
    assert!("630dcd2966c43366".parse::<KeyFingerprint>().is_err());
}

#[test]
fn derive_scoped_deterministic() {
    let service = create_test_service();
    let derived1 = service.derive_scoped("rel-123");
    let derived2 = service.derive_scoped("rel-123");
    assert_eq!(derived1.key_bytes(), derived2.key_bytes());
}

#[test]
fn derive_scoped_different_releases() {
    let service = create_test_service();
    let key_a = service.derive_scoped("rel-aaa").key_bytes();
    let key_b = service.derive_scoped("rel-bbb").key_bytes();
    assert_ne!(key_a, key_b);
}

#[test]
fn derive_scoped_different_master_keys() {
    let svc1 = EncryptionService::from_key([0u8; 32]);
    let svc2 = EncryptionService::from_key([1u8; 32]);
    let key1 = svc1.derive_scoped("rel-123").key_bytes();
    let key2 = svc2.derive_scoped("rel-123").key_bytes();
    assert_ne!(key1, key2);
}

#[test]
fn derive_scoped_roundtrip() {
    let master = create_test_service();
    let release_enc = master.derive_scoped("rel-456");
    let plaintext = b"test audio data for this release";

    let encrypted = release_enc.encrypt(plaintext, TEST_AAD);
    let decrypted = release_enc.decrypt(&encrypted, TEST_AAD).unwrap();
    assert_eq!(decrypted, plaintext);

    // Cannot decrypt with master key
    assert!(master.decrypt(&encrypted, TEST_AAD).is_err());

    // Cannot decrypt with wrong release key
    let wrong_enc = master.derive_scoped("rel-999");
    assert!(wrong_enc.decrypt(&encrypted, TEST_AAD).is_err());
}

#[test]
fn master_keyring_from_serialized_accepts_the_current_keyring_format() {
    let keyring = MasterKeyring::generate();
    let serialized = keyring.to_serialized();
    let parsed = MasterKeyring::from_serialized(&serialized).expect("parse a generated keyring");
    assert_eq!(parsed.to_serialized(), serialized);
    assert_eq!(parsed.fingerprint(), keyring.fingerprint());
}

#[test]
fn master_keyring_from_serialized_rejects_raw_hex() {
    let raw_hex = hex::encode(test_key());
    assert!(MasterKeyring::from_serialized(&raw_hex).is_err());
}

#[test]
fn keyring_payload_requires_the_current_json_format() {
    let service = create_test_service()
        .with_appended_generation(2, [9u8; 32])
        .expect("append a generation");
    let payload = service
        .to_keyring_payload()
        .expect("serialize the current keyring payload");
    let parsed = EncryptionService::from_keyring_payload(payload)
        .expect("parse the current keyring payload");

    assert_eq!(parsed.keyring_entries(), service.keyring_entries());
    assert!(EncryptionService::from_keyring_payload(test_key().to_vec()).is_err());
}

#[test]
fn master_keyring_and_encryption_service_convert_without_losing_generations() {
    let service = EncryptionService::from_key(test_key())
        .with_appended_generation(2, [9u8; 32])
        .expect("append a generation");
    let keyring: MasterKeyring = service.clone().into();
    assert_eq!(keyring.fingerprint(), service.fingerprint());
    assert_eq!(
        keyring.to_serialized(),
        service.to_keyring_string().unwrap()
    );

    let round_tripped: EncryptionService = keyring.into();
    assert_eq!(round_tripped.current_generation(), 2);
    assert_eq!(round_tripped.keyring_entries(), service.keyring_entries(),);
}

/// Two owners rotating at once mint two distinct keys at the SAME generation
/// number. A keyring keyed on the generation number would keep only one of
/// them; keyed on fingerprint, both coexist. Every device that folds in the
/// union then selects the same seal key (highest generation, then greatest
/// fingerprint), so a fork converges instead of partitioning — and because
/// merge keeps every key, each side still opens data sealed under the other's.
#[test]
fn same_generation_fork_converges_on_one_seal_key_and_keeps_both() {
    let base = EncryptionService::from_key([1u8; 32]);
    let fork_a = base.with_appended_generation(2, [0xA0u8; 32]).unwrap();
    let fork_b = base.with_appended_generation(2, [0xB0u8; 32]).unwrap();

    let a_then_b = fork_a.merged_with(&fork_b).unwrap();
    let b_then_a = fork_b.merged_with(&fork_a).unwrap();
    assert_eq!(
        a_then_b.fingerprint(),
        b_then_a.fingerprint(),
        "seal selection is order-independent, so both sides converge on one key",
    );
    assert_eq!(
        a_then_b.key_count(),
        3,
        "the base key and both forks are held"
    );
    assert_eq!(a_then_b.current_generation(), 2);

    let sealed_a = fork_a.seal_app_data(b"from owner A", b"ctx");
    let sealed_b = fork_b.seal_app_data(b"from owner B", b"ctx");
    assert_eq!(
        a_then_b.open_app_data(&sealed_a, b"ctx").unwrap(),
        b"from owner A",
    );
    assert_eq!(
        a_then_b.open_app_data(&sealed_b, b"ctx").unwrap(),
        b"from owner B",
    );
}

#[test]
fn keyring_construction_rejects_one_key_at_conflicting_generations() {
    let key = [0x44u8; 32];

    assert!(EncryptionService::from_keyring([(1, key), (2, key)]).is_err());
}

#[test]
fn appending_a_generation_rejects_an_existing_key_fingerprint() {
    let key = [0x55u8; 32];
    let keyring = EncryptionService::from_key_at_generation(1, key);

    assert!(keyring.with_appended_generation(2, key).is_err());
}

#[test]
fn merging_rejects_one_key_at_conflicting_generations() {
    let key = [0x66u8; 32];
    let generation_one = EncryptionService::from_key_at_generation(1, key);
    let generation_two = EncryptionService::from_key_at_generation(2, key);

    assert!(generation_one.merged_with(&generation_two).is_err());
}

#[test]
fn identical_duplicate_keyring_entries_are_deduplicated() {
    let key = [0x77u8; 32];
    let keyring = EncryptionService::from_keyring([(1, key), (1, key)]).expect("identical entries");

    assert_eq!(keyring.key_count(), 1);
}

#[test]
fn merging_identical_keyring_entries_deduplicates_them() {
    let key = [0x88u8; 32];
    let left = EncryptionService::from_key_at_generation(1, key);
    let right = EncryptionService::from_key_at_generation(1, key);
    let merged = left.merged_with(&right).expect("identical entries");

    assert_eq!(merged.key_count(), 1);
}

#[test]
fn master_keyring_debug_redacts_keys() {
    let keyring = MasterKeyring::generate();
    let debug = format!("{keyring:?}");
    assert!(debug.contains("<redacted>"), "{debug}");
}

// =========================================================================
// App-data sealing
// =========================================================================

/// What the pinned v1 fixture wraps: this payload sealed under [`test_key`]
/// with this `aad`. The bytes are
/// `[01][32-byte SHA-256 digest][24-byte nonce][ciphertext ++ tag]` — the
/// version, [`test_key`]'s full fingerprint, then the chunked ciphertext.
const APP_DATA_V1_FIXTURE_PLAINTEXT: &[u8] = b"pinned app-data payload";
const APP_DATA_V1_FIXTURE_AAD: &[u8] = b"pinned-app-data-context";
const APP_DATA_V1_FIXTURE_HEX: &str = concat!(
    "434b4601",
    "630dcd2966c4336691125448bbb25b4ff412a49c732db2c8abc1b8581bd710dd",
    "0101000001001700000000000000",
    "6de9acc52f5174b4edda5cf3d1b8bf2696d8cee905e5174e",
    "237e8ac01170cb0fd15f473d7fa2675addd2dea92ef2fcc5",
    "996c5544a0519c583ce8f24bb21aed",
);

/// The key fingerprint a sealed payload names, read straight out of its
/// header — so the tests below assert the recorded key rather than trusting
/// `open_app_data` to have picked the right one silently.
fn sealed_fingerprint(sealed: &[u8]) -> [u8; 32] {
    KeyTag::read(sealed)
        .expect("a sealed payload carries its key tag")
        .0
}

#[test]
fn seal_app_data_round_trips_and_records_its_version_and_key() {
    let service = create_test_service();
    for payload in [b"".as_slice(), b"x", b"a longer app-data secret value"] {
        let sealed = service.seal_app_data(payload, TEST_AAD);

        let (fingerprint, body) = KeyTag::read(&sealed).expect("a sealed payload is tagged");
        assert_eq!(
            fingerprint,
            service.seal_fingerprint(),
            "the tag names the key it sealed under",
        );
        assert_eq!(
            body.len(),
            chunked_encrypted_len(payload.len() as u64) as usize,
            "the body is exactly the chunked ciphertext, behind the tag",
        );
        assert_eq!(service.open_app_data(&sealed, TEST_AAD).unwrap(), payload);
    }
}

#[test]
fn sealed_app_data_header_carries_the_full_key_digest() {
    let service = create_test_service();
    let plaintext = b"full fingerprint header";
    let sealed = service.seal_app_data(plaintext, TEST_AAD);
    let expected: [u8; 32] = Sha256::digest(test_key()).into();

    let (fingerprint, body) = KeyTag::read(&sealed).expect("a sealed payload is tagged");
    assert_eq!(fingerprint, expected);
    assert_eq!(
        body.len(),
        chunked_encrypted_len(plaintext.len() as u64) as usize
    );
    assert_eq!(service.open_app_data(&sealed, TEST_AAD).unwrap(), plaintext);
}

/// `aad` binds a payload to its context. Opening with a different one must
/// fail, so a payload lifted into another row does not silently open there.
#[test]
fn open_app_data_rejects_a_different_aad() {
    let service = create_test_service();
    let sealed = service.seal_app_data(b"bound to row 42", b"row-42");

    let error = service
        .open_app_data(&sealed, b"row-99")
        .expect_err("a different aad must not open the payload");

    assert!(matches!(error, SealError::Crypto(_)), "{error:?}");
}

#[test]
fn open_app_data_rejects_a_flipped_ciphertext_byte() {
    let service = create_test_service();
    let mut sealed = service.seal_app_data(b"tamper with me", TEST_AAD);
    let last = sealed.len() - 1;
    sealed[last] ^= 0xFF;

    let error = service
        .open_app_data(&sealed, TEST_AAD)
        .expect_err("a tampered payload must fail authentication");

    assert!(matches!(error, SealError::Crypto(_)), "{error:?}");
}

/// A version this build does not read is refused by name, never guessed at
/// — the payload was written by a format we have no decoder for.
#[test]
fn open_app_data_rejects_an_unknown_version() {
    let service = create_test_service();
    let mut sealed = service.seal_app_data(b"a version-1 payload", TEST_AAD);
    sealed.splice(
        ..KeyTag::LEN,
        KeyTag::write_version_for_test(&service.seal_fingerprint(), 2),
    );

    let error = service
        .open_app_data(&sealed, TEST_AAD)
        .expect_err("version 2 must be refused");

    assert!(matches!(error, SealError::UnknownVersion(2)), "{error:?}");
}

/// Rotation does not orphan already-sealed payloads. Each records the key it
/// was sealed under by fingerprint, and a rotated keyring retains every
/// earlier key, so it opens what it sealed before and after.
#[test]
fn open_app_data_survives_rotation_and_each_payload_names_its_key() {
    let before_rotation = create_test_service();
    let sealed_under_1 = before_rotation.seal_app_data(b"sealed before rotating", TEST_AAD);

    let after_rotation = before_rotation
        .with_appended_generation(2, [9u8; 32])
        .expect("rotate the keyring");
    let sealed_under_2 = after_rotation.seal_app_data(b"sealed after rotating", TEST_AAD);

    assert_eq!(
        sealed_fingerprint(&sealed_under_1),
        before_rotation.seal_fingerprint(),
    );
    assert_eq!(
        sealed_fingerprint(&sealed_under_2),
        after_rotation.seal_fingerprint(),
        "sealing after a rotation records the new seal key",
    );

    assert_eq!(
        after_rotation
            .open_app_data(&sealed_under_1, TEST_AAD)
            .unwrap(),
        b"sealed before rotating",
        "the rotated keyring still opens what the old generation sealed",
    );
    assert_eq!(
        after_rotation
            .open_app_data(&sealed_under_2, TEST_AAD)
            .unwrap(),
        b"sealed after rotating",
    );
}

/// A keyring that does not hold the key a payload names — it predates the
/// payload, or the payload is foreign — is a typed error, not a panic and
/// not a decrypt attempt under the wrong key.
#[test]
fn open_app_data_rejects_a_key_the_keyring_lacks() {
    let rotated = create_test_service()
        .with_appended_generation(2, [9u8; 32])
        .expect("rotate the keyring");
    let sealed_under_2 = rotated.seal_app_data(b"sealed under the rotated key", TEST_AAD);

    let fresh_single_key = EncryptionService::from_key([7u8; 32]);
    let error = fresh_single_key
        .open_app_data(&sealed_under_2, TEST_AAD)
        .expect_err("a keyring without the sealing key must not open it");

    assert!(matches!(error, SealError::UnknownKey(_)), "{error:?}");
}

/// The sealed app-data format is a durable storage contract: a host's rows
/// hold these bytes, so a build that stopped opening them would strand the
/// data. This pins one payload sealed under [`test_key`] — if the version
/// byte, the generation encoding, the chunk framing, or the AAD derivation
/// ever changes, this stops opening and says so.
///
/// Generated once from `seal_app_data` itself, then frozen. It is not
/// re-derived at test time on purpose: a fixture that regenerates would
/// still pass against a changed format and pin nothing.
#[test]
fn sealed_app_data_v1_fixture_opens() {
    let sealed = hex::decode(APP_DATA_V1_FIXTURE_HEX).expect("the fixture is valid hex");

    assert_eq!(
        sealed_fingerprint(&sealed),
        EncryptionService::from_key(test_key()).seal_fingerprint(),
    );

    let opened = EncryptionService::from_key(test_key())
        .open_app_data(&sealed, APP_DATA_V1_FIXTURE_AAD)
        .expect("the pinned v1 payload opens");

    assert_eq!(opened, APP_DATA_V1_FIXTURE_PLAINTEXT);
}

const POLICY_TEST_CONTEXT: &[u8] = b"nonce-policy-test-context";

fn derived_policy() -> NoncePolicy {
    NoncePolicy::DerivedFromContext {
        context: POLICY_TEST_CONTEXT.to_vec(),
    }
}

fn seal_whole(service: &EncryptionService, policy: &NoncePolicy, plaintext: &[u8]) -> Vec<u8> {
    let header = SealedBlobHeader::new(
        std::num::NonZeroU32::new(4096).expect("4 KiB"),
        plaintext.len() as u64,
        policy,
    );
    let mut sealer = service
        .blob_sealer(header, policy, TEST_AAD)
        .expect("a header records the policy it was built under");
    let mut sealed = header.to_bytes();
    for index in 0..header.chunk_count() {
        let span = header.plaintext_span(index..index + 1);
        sealed.extend(sealer.seal_chunk(&plaintext[span.start as usize..span.end as usize]));
    }
    sealed
}

/// One format, both policies: a payload sealed under a stored random base and
/// one sealed under a derived base round-trip through the same header, sealer,
/// and opener.
#[test]
fn both_nonce_policies_round_trip_through_one_format() {
    let service = create_test_service();
    for policy in [NoncePolicy::RandomStored, derived_policy()] {
        for len in [0usize, 1, 4095, 4096, 4097, 20_000] {
            let plaintext: Vec<u8> = (0..len).map(|index| (index % 251) as u8).collect();
            let sealed = seal_whole(&service, &policy, &plaintext);

            let header = SealedBlobHeader::parse(&sealed).expect("the payload frames itself");
            assert_eq!(header.plaintext_len(), len as u64);
            assert_eq!(sealed.len() as u64, header.sealed_len());

            let opened = service
                .blob_opener(header, &policy, TEST_AAD)
                .expect("the header records this policy")
                .open_chunks(
                    0..header.chunk_count(),
                    &sealed[header.prefix_len() as usize..],
                )
                .expect("every chunk opens");
            assert_eq!(
                opened, plaintext,
                "{policy:?} failed to round-trip at {len}"
            );
        }
    }
}

/// Only a derived base is deterministic. Sealing the same plaintext twice under
/// one context reproduces identical bytes — which is what makes a stored blob
/// addressable by its content — while a stored random base differs every time.
#[test]
fn only_a_derived_base_seals_deterministically() {
    let service = create_test_service();
    let plaintext = vec![7u8; 10_000];

    let derived = seal_whole(&service, &derived_policy(), &plaintext);
    assert_eq!(derived, seal_whole(&service, &derived_policy(), &plaintext));

    let random = seal_whole(&service, &NoncePolicy::RandomStored, &plaintext);
    assert_ne!(
        random,
        seal_whole(&service, &NoncePolicy::RandomStored, &plaintext),
        "a fresh base nonce must make each sealing distinct",
    );
}

/// A ranged read opens the chunks covering a span and no others, under either
/// policy: the header carries everything the arithmetic needs, and a stored
/// base is read from the header rather than recomputed.
#[test]
fn ranged_reads_open_only_their_chunks_under_both_policies() {
    let service = create_test_service();
    let plaintext: Vec<u8> = (0..20_000).map(|index| (index % 251) as u8).collect();
    for policy in [NoncePolicy::RandomStored, derived_policy()] {
        let sealed = seal_whole(&service, &policy, &plaintext);
        let header = SealedBlobHeader::parse(&sealed).expect("the payload frames itself");
        let opener = service
            .blob_opener(header, &policy, TEST_AAD)
            .expect("the header records this policy");

        for (start, end) in [(0u64, 1u64), (4095, 4098), (8192, 16_384), (0, 20_000)] {
            let chunks = header.covering_chunks(start, end).expect("range is inside");
            let span = header.sealed_span(chunks.clone());
            let window = header.plaintext_span(chunks.clone());
            let opened = opener
                .open_chunks(chunks, &sealed[span.start as usize..span.end as usize])
                .expect("the covering chunks open");

            assert_eq!(
                opened,
                plaintext[window.start as usize..window.end as usize],
                "{policy:?} read {start}..{end} wrong",
            );
        }
    }
}

/// The policy is not a hint a reader can override. A payload sealed under one
/// base and opened as the other is refused before the cipher runs, because the
/// bytes are not the payload the caller asked for.
#[test]
fn opening_under_the_other_nonce_policy_is_refused() {
    let service = create_test_service();
    let plaintext = b"a payload with one base nonce".to_vec();

    for (sealed_under, opened_as) in [
        (NoncePolicy::RandomStored, derived_policy()),
        (derived_policy(), NoncePolicy::RandomStored),
    ] {
        let sealed = seal_whole(&service, &sealed_under, &plaintext);
        let header = SealedBlobHeader::parse(&sealed).expect("the payload frames itself");

        let Err(error) = service.blob_opener(header, &opened_as, TEST_AAD) else {
            panic!("{sealed_under:?} opened as {opened_as:?} was accepted");
        };

        assert!(
            matches!(error, SealedBlobError::NoncePolicyMismatch { .. }),
            "{sealed_under:?} opened as {opened_as:?} gave {error:?}",
        );
    }
}

/// A whole-object payload states its policy in its own header, so a reader
/// learns it from the bytes rather than from which producer wrote them.
#[test]
fn a_whole_object_payload_records_its_stored_random_base() {
    let service = create_test_service();
    let sealed = service.encrypt(b"whole object", TEST_AAD);
    let header = SealedBlobHeader::parse(&sealed).expect("the payload frames itself");

    assert_eq!(
        header.prefix_len(),
        SEALED_BLOB_HEADER_LEN as u64 + NONCE_SIZE as u64,
        "a stored base nonce follows the fixed header",
    );
    assert!(
        service
            .blob_opener(header, &derived_policy(), TEST_AAD)
            .is_err(),
        "a whole object is never openable as a derived-base payload",
    );
}
