use super::*;
use crate::test_utils;
use crate::tests::Generator;

#[test]
fn codes_use_their_assigned_frame_kinds() {
    assert_eq!(test_utils::restore().to_bytes().unwrap()[0], 10);
    assert_eq!(test_utils::invite().to_bytes().unwrap()[0], 11);
}

#[test]
fn binary_code_fixtures_are_canonical_under_every_truncation_and_bit_change() {
    for (restore, text) in [
        (true, include_str!("../fixtures/restore-code.hex")),
        (false, include_str!("../fixtures/invite-code.hex")),
    ] {
        let bytes = crate::tests::hex(text);
        let reencode = |bytes: &[u8]| {
            if restore {
                RestoreCode::from_bytes(bytes)?.to_bytes()
            } else {
                InviteCode::from_bytes(bytes)?.to_bytes()
            }
        };
        let expected = if restore {
            test_utils::restore().to_bytes().unwrap()
        } else {
            test_utils::invite().to_bytes().unwrap()
        };
        assert_eq!(bytes, expected.as_slice());
        assert_eq!(reencode(&bytes).unwrap().as_slice(), bytes);
        for end in 0..bytes.len() {
            assert!(reencode(&bytes[..end]).is_err());
        }
        let mut changed = bytes.clone();
        for index in 0..bytes.len() {
            for bit in 0..8 {
                changed[index] ^= 1 << bit;
                if let Ok(encoded) = reencode(&changed) {
                    assert_eq!(encoded.as_slice(), changed);
                }
                changed[index] ^= 1 << bit;
            }
        }
        changed.push(0);
        assert_eq!(reencode(&changed), Err(Error::TrailingBytes));
        changed = bytes;
        changed[2] = 2;
        assert_eq!(reencode(&changed), Err(Error::UnsupportedVersion(2)));
        changed[2] = 1;
        for kind in [8, 9, if restore { 11 } else { 10 }] {
            changed[0] = kind;
            assert!(reencode(&changed).is_err());
        }
    }
}

#[test]
fn crc_uses_the_published_castagnoli_check_value() {
    assert_eq!(checksum(b"123456789"), 0xe306_9283);
    assert_eq!(checksum(b""), 0);
}
#[test]
fn pinned_text_restores_member_identity_and_location_and_invite_secrets() {
    let expected: Vec<_> = include_str!("../fixtures/codes.txt").lines().collect();
    let restore = test_utils::restore();
    let invite = test_utils::invite();
    assert_eq!(restore.to_text().unwrap().as_str(), expected[0]);
    assert_eq!(invite.to_text().unwrap().as_str(), expected[1]);
    let restored = RestoreCode::from_text(expected[0]).unwrap();
    assert_eq!(restored.to_bytes().unwrap(), restore.to_bytes().unwrap());
    // The unchanged private-pair blob belongs explicitly to Ana's stable id.
    assert_eq!(restored.member, test_utils::member_id());
    assert_eq!(
        restored.member_keys.public_keys(),
        restore.member_keys.public_keys()
    );
    restored
        .member_keys
        .public_keys()
        .signing
        .verify(b"restored", &restored.member_keys.sign(b"restored"))
        .unwrap();
    assert_eq!(restored.storage.as_bytes(), restore.storage.as_bytes());
    let invited = InviteCode::from_text(expected[1]).unwrap();
    assert_eq!(invited.to_bytes().unwrap(), invite.to_bytes().unwrap());
    assert_eq!(
        invited.secret.to_secret_bytes().as_bytes(),
        invite.secret.to_secret_bytes().as_bytes()
    );
    assert_eq!(invited.storage.as_bytes(), invite.storage.as_bytes());
    // A re-binding invite fixes Ana's existing id for the signed request.
    assert_eq!(invited.member, test_utils::member_id());
    assert_eq!(invited.initial_key, test_utils::store_key().id());
    assert_eq!(invited.inviting_writer, DeviceId(1));
    assert_eq!(invited.read_through, 12);
    assert!(format!("{restore:?}").contains("[REDACTED]"));
    assert!(format!("{invite:?}").contains("[REDACTED]"));
    assert!(!format!("{restore:?}").contains("51, 51"));
    assert!(!format!("{invite:?}").contains("102, 102"));
    assert!(RestoreCode::from_text(expected[1]).is_err());
    assert!(InviteCode::from_text(expected[0]).is_err());
}
fn copy_text(text: &str, extra: usize) -> Zeroizing<String> {
    let mut copy = Zeroizing::new(String::with_capacity(text.len() + extra));
    copy.push_str(text);
    copy
}
#[test]
fn single_character_typos_are_detected() {
    for (is_restore, text) in [
        (true, test_utils::restore().to_text().unwrap()),
        (false, test_utils::invite().to_text().unwrap()),
    ] {
        let rejected = |s: &str| {
            if is_restore {
                RestoreCode::from_text(s).is_err()
            } else {
                InviteCode::from_text(s).is_err()
            }
        };
        for offset in 0..text.len() {
            for replacement in 32..127 {
                if text.as_bytes()[offset] == replacement {
                    continue;
                }
                let mut typo = copy_text(&text, 0);
                typo.replace_range(
                    offset..offset + 1,
                    std::str::from_utf8(&[replacement]).unwrap(),
                );
                assert!(rejected(&typo), "accepted typo at {offset}");
            }
            let mut typo = copy_text(&text, 0);
            typo.remove(offset);
            assert!(rejected(&typo));
        }
        for offset in 0..=text.len() {
            for symbol in ALPHABET {
                let mut typo = copy_text(&text, 1);
                typo.insert(offset, *symbol as char);
                assert!(rejected(&typo));
            }
        }
        for end in 0..text.len() {
            assert!(rejected(&text[..end]));
        }
        let mut lower = copy_text(&text, 0);
        lower.make_ascii_lowercase();
        assert!(rejected(&lower));
        assert!(rejected("é👋"));
    }
}
#[test]
fn padding_material_lengths_and_hostile_sizes_are_errors() {
    assert!(matches!(
        text_decode("CVR1-", &"A".repeat(100_000)),
        Err(Error::Limit { .. })
    ));
    assert!(text_decode("CVR1-", "CVR1-A").is_err());
    assert!(matches!(
        text_decode("CVR1-", "CVR1-AB"),
        Err(Error::Invalid {
            rule: Rule::CodePadding,
            ..
        })
    ));
    let mut code = test_utils::restore();
    code.storage = SecretBytes::new(vec![1; MAX_STORAGE + 1]);
    assert!(code.to_text().is_err());
    let mut encoded = test_utils::restore().to_bytes().unwrap();
    // Prefix, store id, member id, key length, then
    // crypto's CVMK header.
    encoded[43] = b'X';
    assert!(matches!(
        RestoreCode::from_bytes(&encoded),
        Err(Error::Material(_))
    ));
    let mut encoded = test_utils::invite().to_bytes().unwrap();
    encoded[124..128].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        InviteCode::from_bytes(&encoded),
        Err(Error::Limit { .. })
    ));
}

#[test]
fn invite_cursor_represents_every_applied_slot() {
    let mut invite = test_utils::invite();
    for number in [0, 12, u64::MAX] {
        invite.read_through = number;
        let decoded = InviteCode::from_text(&invite.to_text().unwrap()).unwrap();
        assert_eq!(decoded.read_through, number);
    }
}

#[test]
fn invite_frames_require_initial_key_and_writer_cursor_evidence() {
    let mut bytes = test_utils::invite().to_bytes().unwrap();
    bytes.drain(92..124);
    let payload_length = (bytes.len() - FRAME_PREFIX_LEN) as u32;
    bytes[3..7].copy_from_slice(&payload_length.to_be_bytes());
    assert!(matches!(
        InviteCode::from_bytes(&bytes),
        Err(Error::Truncated)
    ));
}
#[test]
fn maximum_storage_and_names_use_zeroizing_frame_and_text_types() {
    let mut code = test_utils::restore();
    code.storage = SecretBytes::new(vec![0xab; MAX_STORAGE]);
    let bytes: Zeroizing<Vec<u8>> = code.to_bytes().unwrap();
    let text: Zeroizing<String> = code.to_text().unwrap();
    let decoded = RestoreCode::from_text(&text).unwrap();
    assert_eq!(decoded.to_bytes().unwrap(), bytes);
    let mut invite = test_utils::invite();
    invite.name = "N".repeat(1024);
    invite.storage = SecretBytes::new(vec![0xcd; MAX_STORAGE]);
    assert_eq!(
        InviteCode::from_text(&invite.to_text().unwrap())
            .unwrap()
            .to_bytes()
            .unwrap(),
        invite.to_bytes().unwrap()
    );
}
#[test]
fn arbitrary_code_text_never_panics() {
    let mut source = Generator(0x89ab_cdef_0123_4567u64);
    for n in 0..5000 {
        let mut text = Zeroizing::new(String::with_capacity(5 + 2 * (n % 256)));
        text.push_str(if n % 2 == 0 { "CVR1-" } else { "CVI1-" });
        for _ in 0..n % 256 {
            text.push(char::from(source.next() as u8));
        }
        if let Ok(code) = RestoreCode::from_text(&text) {
            assert_eq!(code.to_text().unwrap(), text);
        }
        if let Ok(code) = InviteCode::from_text(&text) {
            assert_eq!(code.to_text().unwrap(), text);
        }
    }
}

#[test]
fn restore_layout_places_member_immediately_after_store() {
    let code = test_utils::restore();
    let bytes = code.to_bytes().unwrap();
    assert_eq!(&bytes[7..23], code.store.0.as_bytes());
    assert_eq!(&bytes[23..39], code.member.0.as_bytes());
    assert_eq!(&bytes[43..48], b"CVMK\x01");
}
