"""Generate sealed write, entry, snapshot and key-copy fixtures independently of Rust,
using libsodium.

Run with Python 3 and libsodium installed. All keys are public test material.
The plaintext frames and clear prefix come from the checked-in format fixture.
"""

import ctypes as c
import ctypes.util
import hashlib
import hmac
from pathlib import Path


def context(*fields):
    return b"".join(len(field).to_bytes(8, "big") + field for field in fields)


def encryption_key(value):
    extracted = hmac.digest(bytes(32), bytes([value]) * 32, "sha256")
    return hmac.digest(extracted, context(b"coven/encryption/v1") + b"\x01", "sha256")


def checked(result):
    if result != 0:
        raise RuntimeError(f"libsodium failed: {result}")


def chunk(sodium, key, path, prefix, section, index, plaintext):
    fields = (path, prefix, section.to_bytes(8, "big"), index.to_bytes(8, "big"))
    digest = hashlib.sha256(plaintext).digest()
    nonce = hmac.digest(key, context(b"coven/object-nonce/v2", *fields, digest), "sha256")[:24]
    return sealed_chunk(sodium, key, path, prefix, section, index, plaintext, nonce)


def sealed_chunk(sodium, key, path, prefix, section, index, plaintext, nonce):
    fields = (path, prefix, section.to_bytes(8, "big"), index.to_bytes(8, "big"))
    aad = context(b"coven/object-chunk/v1", *fields)
    ciphertext = c.create_string_buffer(len(plaintext) + 16)
    length = c.c_ulonglong()
    checked(sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(
        ciphertext, c.byref(length), plaintext, len(plaintext), aad, len(aad),
        None, nonce, key,
    ))
    assert length.value == len(ciphertext)
    return len(plaintext).to_bytes(4, "big") + nonce + ciphertext.raw


def signing_public(sodium, seed):
    public = c.create_string_buffer(32)
    secret = c.create_string_buffer(64)
    checked(sodium.crypto_sign_seed_keypair(public, secret, seed))
    return public.raw


def sealing_public(sodium, secret):
    public = c.create_string_buffer(32)
    checked(sodium.crypto_scalarmult_base(public, secret))
    return public.raw


def hkdf(secret, info):
    extracted = hmac.digest(bytes(32), secret, "sha256")
    return hmac.digest(extracted, info + b"\x01", "sha256")


def key_copy(sodium, path, audience, key_id, plaintext, ephemeral_seed, nonce):
    """A kind-37 copy to the member whose signing and sealing seeds are 0x33 (D9, D11)."""
    member = signing_public(sodium, bytes([0x33]) * 32)
    recipient = sealing_public(sodium, bytes([0x33]) * 32)
    prefix = bytes([37, 0, 1]) + audience + key_id + member + recipient
    ephemeral = bytes([ephemeral_seed]) * 32
    sender = sealing_public(sodium, ephemeral)
    shared = c.create_string_buffer(32)
    checked(sodium.crypto_scalarmult(shared, ephemeral, recipient))
    kind = b"circle" if audience[0] == 1 else b"store"
    box_context = context(b"coven/sealed-box/v1", kind, path, prefix, sender, recipient)
    key = hkdf(shared.raw, box_context)
    nonce = bytes([nonce]) * 24
    ciphertext = c.create_string_buffer(len(plaintext) + 16)
    length = c.c_ulonglong()
    checked(sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(
        ciphertext, c.byref(length), plaintext, len(plaintext),
        box_context, len(box_context), None, nonce, key,
    ))
    assert length.value == len(ciphertext)
    return prefix + sender + nonce + ciphertext.raw


def signature(sodium, message):
    public = c.create_string_buffer(32)
    secret = c.create_string_buffer(64)
    checked(sodium.crypto_sign_seed_keypair(public, secret, bytes([0x33]) * 32))
    result = c.create_string_buffer(64)
    length = c.c_ulonglong()
    checked(sodium.crypto_sign_detached(
        result, c.byref(length), message, len(message), secret,
    ))
    assert length.value == 64
    return result.raw


def signed(sodium, path, body):
    message = context(b"coven/object-signature/v1", path, hashlib.sha256(body).digest())
    return body + signature(sodium, message)


def main():
    library = ctypes.util.find_library("sodium")
    if library is None:
        raise RuntimeError("libsodium is required to generate these fixtures")
    sodium = c.CDLL(library)
    if sodium.sodium_init() < 0:
        raise RuntimeError("libsodium initialization failed")
    # Declare pointer and 64-bit length arguments explicitly on every platform.
    pointer, length = c.c_void_p, c.c_ulonglong
    sodium.crypto_aead_xchacha20poly1305_ietf_encrypt.argtypes = [
        pointer, pointer, pointer, length, pointer, length, pointer, pointer, pointer,
    ]
    sodium.crypto_sign_seed_keypair.argtypes = [pointer, pointer, pointer]
    sodium.crypto_sign_detached.argtypes = [pointer, pointer, pointer, length, pointer]
    sodium.crypto_scalarmult_base.argtypes = [pointer, pointer]
    sodium.crypto_scalarmult.argtypes = [pointer, pointer, pointer]

    directory = Path(__file__).resolve().parent
    frames = [bytes.fromhex(line) for line in (directory / "v1.hex").read_text().splitlines()]
    store = b"10101010-1010-1010-1010-101010101010/"
    path = store + b"devices/1/3"
    # The write prefix (kind 32) follows the plaintext frames; its header and
    # part chunks follow it, then the snapshot prefix (kind 34) and its chunks.
    write_at = next(i for i, frame in enumerate(frames) if frame[0] == 32)
    snapshot_at = next(i for i, frame in enumerate(frames) if frame[0] == 34)
    prefix = frames[write_at]
    assert frames[write_at + 1][0] == 1 and snapshot_at == write_at + 6
    body = prefix
    for frame, section, index, key in [
        (frames[write_at + 1], 0, 0, encryption_key(17)),
        (frames[write_at + 2], 1, 0, encryption_key(17)),
        (frames[write_at + 3], 1, 1, encryption_key(17)),
        (frames[write_at + 4], 1, 2, encryption_key(17)),
        (frames[write_at + 5], 2, 0, encryption_key(18)),
    ]:
        body += chunk(sodium, key, path, prefix, section, index, frame)
    write = signed(sodium, path, body)

    path = store + b"store-log/1/1"
    prefix = bytes([33]) + frames[0][1:3] + bytes([1]) * 16
    assert frames[0][0] == 4
    body = prefix + chunk(sodium, encryption_key(17), path, prefix, 0, 0, frames[0])
    entry = signed(sodium, path, body)

    # Snapshot chunks use random nonces; these fixed ones are public test data.
    path = store + b"snapshots/store/1/1"
    prefix = frames[snapshot_at]
    body = prefix + signature(sodium, context(b"coven/prefix-signature/v1", path, prefix))
    for index, frame in enumerate(frames[snapshot_at + 1:]):
        body += sealed_chunk(sodium, encryption_key(17), path, prefix, 0, index, frame,
                             bytes([0x60 + index]) * 24)
    snapshot = signed(sodium, path, body)

    # Key copies use random ephemeral keys and nonces; these fixed ones are public.
    store_key_id = bytes([1]) * 16
    store_key = key_copy(sodium, store + b"keys/7/9", b"\x00", store_key_id,
                         store_key_id + bytes([17]) * 32, 0x55, 0x44)
    circle_id, circle_key_id = bytes([2]) * 16, bytes([3]) * 16
    circle_key = key_copy(sodium, store + b"keys/7/10", b"\x01" + circle_id, circle_key_id,
                          circle_id + circle_key_id + bytes([18]) * 32, 0x56, 0x45)
    for name, data in [
        ("sealed-write.hex", write),
        ("sealed-store-log.hex", entry),
        ("sealed-snapshot.hex", snapshot),
        ("sealed-store-key.hex", store_key),
        ("sealed-circle-key.hex", circle_key),
    ]:
        (directory / name).write_text(data.hex() + "\n")


if __name__ == "__main__":
    main()
