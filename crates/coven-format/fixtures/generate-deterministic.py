"""Generate D11 write/entry fixtures independently of Rust, using libsodium.

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
    aad = context(b"coven/object-chunk/v1", *fields)
    ciphertext = c.create_string_buffer(len(plaintext) + 16)
    length = c.c_ulonglong()
    checked(sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(
        ciphertext, c.byref(length), plaintext, len(plaintext), aad, len(aad),
        None, nonce, key,
    ))
    assert length.value == len(ciphertext)
    return len(plaintext).to_bytes(4, "big") + nonce + ciphertext.raw


def signed(sodium, path, body):
    public = c.create_string_buffer(32)
    secret = c.create_string_buffer(64)
    checked(sodium.crypto_sign_seed_keypair(public, secret, bytes([0x33]) * 32))
    message = context(b"coven/object-signature/v1", path, hashlib.sha256(body).digest())
    signature = c.create_string_buffer(64)
    length = c.c_ulonglong()
    checked(sodium.crypto_sign_detached(
        signature, c.byref(length), message, len(message), secret,
    ))
    assert length.value == 64
    return body + signature.raw


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

    directory = Path(__file__).resolve().parent
    frames = [bytes.fromhex(line) for line in (directory / "v1.hex").read_text().splitlines()]
    store = b"10101010-1010-1010-1010-101010101010/"
    path = store + b"devices/1/3"
    # The write prefix follows the frame examples; its header and part chunks follow it.
    first = next(i for i, frame in enumerate(frames) if frame[0] == 32)
    prefix = frames[first]
    assert frames[first + 1][0] == 1
    body = prefix
    for frame, section, index, key in [
        (frames[first + 1], 0, 0, encryption_key(17)),
        (frames[first + 2], 1, 0, encryption_key(17)),
        (frames[first + 3], 1, 1, encryption_key(17)),
        (frames[first + 4], 1, 2, encryption_key(17)),
        (frames[first + 5], 2, 0, encryption_key(18)),
    ]:
        body += chunk(sodium, key, path, prefix, section, index, frame)
    write = signed(sodium, path, body)

    path = store + b"store-log/1/1"
    prefix = bytes([33]) + frames[0][1:3] + bytes([1]) * 16
    assert frames[0][0] == 4
    body = prefix + chunk(sodium, encryption_key(17), path, prefix, 0, 0, frames[0])
    entry = signed(sodium, path, body)
    for name, data in [("sealed-write.hex", write), ("sealed-store-log.hex", entry)]:
        (directory / name).write_text(data.hex() + "\n")


if __name__ == "__main__":
    main()
