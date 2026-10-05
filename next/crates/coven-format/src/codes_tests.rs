use super::*;
use crate::test_utils;

#[test]
fn crc_uses_the_published_castagnoli_check_value() {
    assert_eq!(checksum(b"123456789"), 0xe306_9283);
    assert_eq!(checksum(b""), 0);
}
#[test]
fn pinned_text_restores_crypto_owned_keys_and_credentials() {
    let expected: Vec<_> = include_str!("../fixtures/codes.txt").lines().collect();
    let restore = test_utils::restore();
    let invite = test_utils::invite();
    assert_eq!(restore.to_text().unwrap().as_str(), expected[0]);
    assert_eq!(invite.to_text().unwrap().as_str(), expected[1]);
    let restored = RestoreCode::from_text(expected[0]).unwrap();
    assert_eq!(restored.to_bytes().unwrap(), restore.to_bytes().unwrap());
    assert_eq!(
        restored.member_keys.member_id(),
        restore.member_keys.member_id()
    );
    assert_eq!(
        restored.member_keys.sealing_public_key(),
        restore.member_keys.sealing_public_key()
    );
    restored
        .member_keys
        .member_id()
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
    // Prefix, store id, name length, name, key length, then crypto's CVMK header.
    encoded[32] = b'X';
    assert!(matches!(
        RestoreCode::from_bytes(&encoded),
        Err(Error::Material(_))
    ));
    let mut encoded = test_utils::invite().to_bytes().unwrap();
    encoded[76..80].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        InviteCode::from_bytes(&encoded),
        Err(Error::Limit { .. })
    ));
}
#[test]
fn maximum_storage_and_names_use_zeroizing_frame_and_text_types() {
    let mut code = test_utils::restore();
    code.name = "N".repeat(1024);
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
    let mut state = 0x89ab_cdef_0123_4567u64;
    for n in 0..5000 {
        let mut text = Zeroizing::new(String::with_capacity(5 + 2 * (n % 256)));
        text.push_str(if n % 2 == 0 { "CVR1-" } else { "CVI1-" });
        for _ in 0..n % 256 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            text.push(char::from((state & 255) as u8));
        }
        if let Ok(code) = RestoreCode::from_text(&text) {
            assert_eq!(code.to_text().unwrap(), text);
        }
        if let Ok(code) = InviteCode::from_text(&text) {
            assert_eq!(code.to_text().unwrap(), text);
        }
    }
}
