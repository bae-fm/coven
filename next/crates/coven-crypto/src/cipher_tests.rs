use super::*;
use chacha20poly1305::aead::{self, consts::U16, AeadCore, Nonce, TagPosition};

#[test]
fn context_lengths_are_big_endian_and_preserve_empty_components() {
    assert_eq!(
        context(&[b"ab", b"", b"c"]),
        [
            0, 0, 0, 0, 0, 0, 0, 2, b'a', b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            b'c'
        ]
    );
    assert_ne!(context(&[b"ab", b"c"]), context(&[b"a", b"bc"]));
}

struct RefusingCipher;

impl AeadCore for RefusingCipher {
    type NonceSize = U24;
    type TagSize = U16;
    const TAG_POSITION: TagPosition = TagPosition::Postfix;
}

impl Aead for RefusingCipher {
    fn encrypt<'msg, 'aad>(
        &self,
        _nonce: &Nonce<Self>,
        _plaintext: impl Into<Payload<'msg, 'aad>>,
    ) -> aead::Result<Vec<u8>> {
        Err(aead::Error)
    }

    fn decrypt<'msg, 'aad>(
        &self,
        _nonce: &Nonce<Self>,
        _ciphertext: impl Into<Payload<'msg, 'aad>>,
    ) -> aead::Result<Vec<u8>> {
        panic!("the encryption failure test must not decrypt")
    }
}

#[test]
#[should_panic(expected = "XChaCha20-Poly1305 plaintext must fit its block counter")]
fn encryption_rejection_is_an_invariant_violation() {
    // Exercise rejection without allocating a message exceeding the cipher's limit.
    encrypt(&RefusingCipher, &[0; 24], b"context", b"payload");
}
