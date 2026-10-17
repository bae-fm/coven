"""Generate format-1 fixtures independently of Rust, using hashlib/hmac and libsodium.

Run with Python 3 and libsodium installed. All keys, seeds and fixed nonces are
public test material. With no argument it rewrites the fixtures beside it; with
a directory argument it writes them there instead, which is how
`lib_tests::every_object_and_snapshot_section_has_pinned_bytes` compares this
generator's output with the checked-in bytes.

Admin frames and codes are independently encoded from D2/D6/D13. Write and
snapshot plaintext frames come from v1.hex. Every sealed object, anonymous
box and signature is calculated here, including the outer writes slot and floor.

Ana's stable id is 0x4d repeated. Her original pair has signing and sealing
seeds 0x33, her replacement pair 0x34; Ben (0x42 repeated) has seeds 0x35.
"""

import ctypes as c
import ctypes.util
import hashlib
import hmac
import sys
from pathlib import Path

ANA = bytes([0x4D]) * 16
BEN = bytes([0x42]) * 16
STORE = bytes([0x10]) * 16
CIRCLE = bytes([2]) * 16
STORE_TEXT = b"10101010-1010-1010-1010-101010101010/"


def context(*fields):
    return b"".join(len(field).to_bytes(8, "big") + field for field in fields)


def derive(secret, label):
    extracted = hmac.digest(bytes(32), secret, "sha256")
    return hmac.digest(extracted, context(label) + b"\x01", "sha256")


def encryption_key(value):
    return derive(bytes([value]) * 32, b"coven/encryption/v1")


def authorship_key(value):
    return derive(bytes([value]) * 32, b"coven/row-authorship/v1")


def checked(result):
    if result != 0:
        raise RuntimeError(f"libsodium failed: {result}")


# D2 primitives.
def u8(value):
    return value.to_bytes(1, "big")


def u16(value):
    return value.to_bytes(2, "big")


def u32(value):
    return value.to_bytes(4, "big")


def u64(value):
    return value.to_bytes(8, "big")


def text(value):
    return u32(len(value)) + value


def collection(items):
    return u32(len(items)) + b"".join(items)


def write_id(device, number):
    return u64(device) + u64(number)


def timestamp(ms, counter, device):
    return ms.to_bytes(6, "big") + u16(counter) + u64(device)


def store_audience():
    return b"\x00"


def circle_audience(circle):
    return b"\x01" + circle


def snapshot_id(audience, device, number):
    return audience + u64(device) + u64(number)


def last_claim(device, number, digest):
    assert (number == 0) == (digest is None)
    return frame(6, b"\x0b" + u64(device) + u64(number)
                 + (b"\x00" if digest is None else b"\x01" + digest))


def frame(kind, payload):
    return u8(kind) + u16(1) + u32(len(payload)) + payload


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
    checked(sodium.crypto_sign_seed_keypair(public, secret, bytes([seed]) * 32))
    return public.raw


def sealing_public(sodium, seed):
    public = c.create_string_buffer(32)
    checked(sodium.crypto_scalarmult_base(public, bytes([seed]) * 32))
    return public.raw


def pair(sodium, seed):
    """D2 `MemberKeys`: the signing then the sealing public key."""
    return signing_public(sodium, seed) + sealing_public(sodium, seed)


def anonymous_box(sodium, kind, path, prefix, plaintext, seed, ephemeral_seed, nonce):
    recipient = sealing_public(sodium, seed)
    ephemeral = bytes([ephemeral_seed]) * 32
    sender = sealing_public(sodium, ephemeral_seed)
    shared = c.create_string_buffer(32)
    checked(sodium.crypto_scalarmult(shared, ephemeral, recipient))
    box_context = context(b"coven/sealed-box/v1", kind, path, prefix, sender, recipient)
    extracted = hmac.digest(bytes(32), shared.raw, "sha256")
    key = hmac.digest(extracted, box_context + b"\x01", "sha256")
    nonce = bytes([nonce]) * 24
    ciphertext = c.create_string_buffer(len(plaintext) + 16)
    length = c.c_ulonglong()
    checked(sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(
        ciphertext, c.byref(length), plaintext, len(plaintext),
        box_context, len(box_context), None, nonce, key,
    ))
    assert length.value == len(ciphertext)
    return sender + nonce + ciphertext.raw


def key_copy(sodium, path, audience, key_id, plaintext, seed, ephemeral_seed, nonce):
    """A copy slot addressed to Ana's exact credential (D9, D11)."""
    recipient = sealing_public(sodium, seed)
    prefix = bytes([33, 0, 1, 2]) + bytes([1]) * 16 + b"\x01" + audience + key_id + ANA + recipient
    kind = b"circle" if audience[0] == 1 else b"store"
    sealed = anonymous_box(sodium, kind, path, prefix, plaintext, seed, ephemeral_seed, nonce)
    prefix += sealed[:56] + u32(len(plaintext)) + sealed[56:]
    slot_number = int(path.rsplit(b"/", 1)[1])
    plaintext = slot_header(slot_number, slot_number) + frame(16, b"")
    return signed(sodium, path, prefix + chunk(sodium, encryption_key(17), path, prefix, 0, 0, plaintext))


def access_key_delivery(sodium):
    """Ana delivers Ben an IAM access key at one fixed mint-attempt path."""
    path = STORE_TEXT + b"access-keys/42424242-4242-4242-4242-424242424242/77777777-7777-7777-7777-777777777777"
    prefix = bytes([40, 0, 1]) + BEN + sealing_public(sodium, 0x35) + ANA
    principal = b"\x01" + text(b"coven-0123") + b"\x00" + text(b"coven-member")
    plaintext = text(b"AKIAMEMBER") + text(b"member-secret") + principal
    sealed = anonymous_box(sodium, b"s3-key", path, prefix, plaintext, 0x35, 0x57, 0x46)
    return signed(sodium, path, prefix + text(sealed))


def signature(sodium, message, seed):
    public = c.create_string_buffer(32)
    secret = c.create_string_buffer(64)
    checked(sodium.crypto_sign_seed_keypair(public, secret, bytes([seed]) * 32))
    result = c.create_string_buffer(64)
    length = c.c_ulonglong()
    checked(sodium.crypto_sign_detached(
        result, c.byref(length), message, len(message), secret,
    ))
    assert length.value == 64
    return result.raw


def signed(sodium, path, body, seed=0x33):
    message = context(b"coven/object-signature/v1", path, hashlib.sha256(body).digest())
    return body + signature(sodium, message, seed)


def single_chunk(sodium, kind, key_id, key, path, plaintext, nonce, seed):
    """A join request with a fixed test nonce (D9)."""
    prefix = bytes([kind, 0, 1]) + key_id
    return signed(sodium, path, prefix + sealed_chunk(
        sodium, key, path, prefix, 0, 0, plaintext, bytes([nonce]) * 24), seed)


# D5 part records and their D11 authorship leaves, parsed independently of Rust.
class Reader:
    def __init__(self, data):
        self.data = data
        self.at = 0

    def take(self, n):
        piece = self.data[self.at:self.at + n]
        assert len(piece) == n, "truncated"
        self.at += n
        return piece

    def int(self, n):
        return int.from_bytes(self.take(n), "big")

    def blob(self):
        return self.take(self.int(4))

    def span(self, parse):
        start = self.at
        parse(self)
        return self.data[start:self.at]


def parse_audience(r):
    if r.int(1) == 1:
        r.take(16)


def parse_row(r):
    r.blob()
    r.blob()
    parse_audience(r)


def parse_value(r):
    tag = r.int(1)
    if tag in (1, 2):
        r.take(8)
    elif tag in (3, 4):
        r.blob()


def parse_names(r):
    for _ in range(r.int(4)):
        r.blob()


def parse_column_value(r):
    parse_value(r)
    for _ in range(r.int(4)):
        parse_names(r)
        r.blob()
        parse_names(r)
        parse_row(r)
        r.take(8)


def parse_columns(r):
    """A map<name, ColumnValue>: each name with its length field, then its value."""
    return [(r.span(lambda r: r.blob()), r.span(parse_column_value)) for _ in range(r.int(4))]


def parse_change_id(r):
    r.take(16)
    parse_row(r)


def parse_option(r, parse):
    if r.int(1) == 1:
        parse(r)


def parse_sources(r):
    parse_option(r, parse_change_id)
    for _ in range(r.int(4)):
        r.blob()
        parse_change_id(r)
        r.blob()


def parse_change(r):
    generation = r.int(8)
    operation = r.int(1)
    columns = parse_columns(r) if operation in (0, 1) else []
    return generation, operation, columns


def parse_execution(r, own=None):
    """D5: each step is `sequence | change | key_update`, its change `0` for
    exactly the row change's own (`own`, its bytes) or `1 | change`. The
    returned steps are expanded to `sequence | change | key_update`, as
    their D11 leaves encode them."""
    steps = []
    for _ in range(r.int(4)):
        sequence = r.take(4)
        tag = r.int(1)
        if tag == 0:
            assert own is not None, "an execution record has no own change"
            change = own
        else:
            assert tag == 1
            change = r.span(parse_change)
            assert change != own, "the own change is written once"
        steps.append(sequence + change + r.take(1))
    parse_sources(r)
    return steps


def record_leaves(kind, payload):
    """D11: an insert's creation marker and cells, an update's cells, a
    delete's marker, then one leaf per operation; dismissals have none."""
    r = Reader(payload)
    if kind == 3:
        return []
    row = r.span(parse_row)
    leaves = []
    own = None
    if kind == 2:
        start = r.at
        generation, operation, columns = parse_change(r)
        own = r.data[start:r.at]
        for _ in range(r.int(4)):
            r.blob()
            parse_value(r)
        parse_sources(r)
        creator = r.span(lambda r: parse_option(r, parse_change_id))
        if operation == 0:
            leaves.append(b"\x00" + row + u64(generation) + creator)
        for column, value in columns:
            leaves.append(b"\x01" + row + u64(generation) + u8(operation) + column + value)
        if operation == 2:
            leaves.append(b"\x02" + row + u64(generation))
    else:
        assert kind == 13
    leaves.extend(b"\x03" + row + step for step in parse_execution(r, own))
    assert r.at == len(payload), "trailing record bytes"
    return leaves


def part_leaves(plaintext):
    leaves = []
    r = Reader(plaintext)
    while r.at < len(plaintext):
        kind = r.int(1)
        assert r.int(2) == 1
        leaves.extend(record_leaves(kind, r.blob()))
    return leaves


def authorship_root(key, leaves):
    """RFC 6962's split: the left subtree holds the largest power of two below
    the count. Leaves are keyed with the audience key; branches are not."""
    if not leaves:
        return hashlib.sha256(b"\x02").digest()
    if len(leaves) == 1:
        return hmac.digest(key, b"\x00" + leaves[0], "sha256")
    left = 1
    while left * 2 < len(leaves):
        left *= 2
    return hashlib.sha256(b"\x01" + authorship_root(key, leaves[:left])
                          + authorship_root(key, leaves[left:])).digest()


def part_lengths(header):
    """Each part's plaintext length, from a D5 header frame."""
    r = Reader(header)
    assert r.int(1) == 1 and r.int(2) == 1
    r.take(4 + 32)
    r.take(8)
    r.take(4 + 1)
    lengths = []
    for _ in range(r.int(4)):
        parse_audience(r)
        r.take(8)
        lengths.append(r.int(8))
    return lengths


# D6 admin frames and D9 slot header.
def slot_header(slot, claim):
    return frame(14, u64(1) + u64(claim) + ANA + u64(slot - 1))


def bounded_slot(sodium, slot, kind, plaintext, claim=None):
    path = STORE_TEXT + b"seq/" + str(slot).encode()
    prefix = bytes([33, 0, 1, kind]) + bytes([1]) * 16 + b"\x00"
    body = slot_header(slot, slot if claim is None else claim) + plaintext
    return signed(sodium, path, prefix + chunk(sodium, encryption_key(17), path, prefix, 0, 0, body))


def seal_writes_slot(sodium, slot, claim, writes):
    path = STORE_TEXT + b"seq/" + str(slot).encode()
    prefix = bytes([33, 0, 1, 1]) + bytes([1]) * 16 + b"\x00"
    plaintext = slot_header(slot, claim) + u64(len(writes))
    plaintext += b"".join(u64(len(write)) + write for write in writes)
    body = prefix
    for index, start in enumerate(range(0, len(plaintext), 65536)):
        body += chunk(sodium, encryption_key(17), path, prefix, 0, index, plaintext[start:start + 65536])
    return signed(sodium, path, body)


def changes(sodium):
    """`test_utils::store_changes`, in canonical tag order."""
    initial_hash = hashlib.sha256(bytes([17]) * 32).digest()
    s3_console = b"\x01" + text(b"fixture-access-key") + b"\x00"
    return [
        b"\x00" + STORE + text(b"S") + pair(sodium, 0x33) + s3_console + text(b"D")
        + bytes([1]) * 16 + initial_hash,
        b"\x01" + BEN + pair(sodium, 0x35) + b"\x01" + b"\x00" + text(b"member@example.test"),
        b"\x02" + BEN,
        b"\x03" + ANA + b"\x00",
        b"\x04" + text(b"D") + b"\x00",
        b"\x05" + u64(2),
        b"\x06" + CIRCLE + text(b"C") + bytes([1]) * 16 + initial_hash,
        b"\x07" + CIRCLE + text(b"N"),
        b"\x08" + CIRCLE,
        b"\x09" + CIRCLE + ANA,
        b"\x0a" + CIRCLE + ANA,
        b"\x0b" + u32(2) + u64(7),
        b"\x0d" + u64(8),
        b"\x0e" + ANA + b"\x01" + text(b"replacement-key") + b"\x01" + text(b"coven-member")
        + b"\x00" + text(b"coven-store"),
        b"\x0f" + store_audience() + bytes([3]) * 16 + bytes([0x91]) * 32,
        b"\x0f" + circle_audience(CIRCLE) + bytes([4]) * 16 + bytes([0xA2]) * 32,
        b"\x10" + ANA + pair(sodium, 0x33) + pair(sodium, 0x34),
    ]


def join_request(sodium):
    """Ana's request answering the re-binding invite with her replacement pair."""
    return frame(9, bytes([0x55]) * 16 + ANA + pair(sodium, 0x34) + text(b"D"))


def objects(sodium):
    return [frame(4, change) for change in changes(sodium)] + [join_request(sodium)]


# D13 codes.
def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def code_text(prefix, data):
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
    bits = "".join(f"{byte:08b}" for byte in data + crc32c(data).to_bytes(4, "big"))
    bits += "0" * (-len(bits) % 5)
    return prefix + "".join(alphabet[int(bits[i:i + 5], 2)] for i in range(0, len(bits), 5))


def restore_code():
    """Ana's id with her unchanged private-pair blob and a location without credentials."""
    storage = (b'{"S3":{"bucket":"bucket","region":"region","endpoint":null,'
               b'"profile":null,"prefix":"store"}}')
    keys = b"CVMK\x01" + bytes([0x33]) * 64
    return frame(10, STORE + text(b"S") + ANA + text(keys) + text(storage))


def invite_code():
    """A re-binding invite fixing Ana's existing id."""
    return frame(11, STORE + text(b"S") + bytes([0x55]) * 16 + ANA + bytes([0x66]) * 32
                 + bytes([1]) * 16 + u64(1) + u64(12) + text(b"\x44"))


def load_sodium():
    library = ctypes.util.find_library("sodium")
    # On macOS ctypes searches /usr/local/lib but not Apple-silicon Homebrew's
    # /opt/homebrew/lib, so only Homebrew's own Python finds libsodium there.
    homebrew = Path("/opt/homebrew/lib/libsodium.dylib")
    if library is None and sys.platform == "darwin" and homebrew.exists():
        library = str(homebrew)
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

    return sodium


def copies(sodium):
    store_key_id = bytes([1]) * 16
    store = key_copy(sodium, STORE_TEXT + b"seq/2", b"\x00", store_key_id,
                     store_key_id + bytes([17]) * 32, 0x33, 0x55, 0x44)
    circle_key_id = bytes([3]) * 16
    circle = key_copy(sodium, STORE_TEXT + b"seq/8", b"\x01" + CIRCLE, circle_key_id,
                      CIRCLE + circle_key_id + bytes([18]) * 32, 0x34, 0x56, 0x45)
    return store, circle


def main():
    sodium = load_sodium()
    directory = Path(__file__).resolve().parent
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else directory
    pinned = [bytes.fromhex(line) for line in (directory / "v1.hex").read_text().splitlines()]
    # The object frames come first; the write header (kind 1) starts the rest.
    rest_at = next(i for i, frame_ in enumerate(pinned) if frame_[0] == 1)
    frames = objects(sodium) + pinned[rest_at:]
    (out / "v1.hex").write_text("".join(frame_.hex() + "\n" for frame_ in frames))

    path = b"coven/write/" + STORE_TEXT + b"1/3"
    # The write prefix (kind 32) follows the plaintext frames; its header and
    # part chunks follow it, then the snapshot prefix (kind 34) and its chunks.
    write_at = next(i for i, frame_ in enumerate(frames) if frame_[0] == 32)
    snapshot_at = next(i for i, frame_ in enumerate(frames) if frame_[0] == 34)
    prefix = frames[write_at]
    header = frames[write_at + 1]
    assert header[0] == 1
    # The store part uses the store key (bytes 17), the circle part key 18.
    part_keys = [17, 18]
    header_chunk = chunk(sodium, encryption_key(17), path, prefix, 0, 0, header)
    pieces, at = [], write_at + 2
    for length in part_lengths(header):
        count = -(-length // 65536)
        pieces.append(frames[at:at + count])
        at += count
    assert at == snapshot_at
    roots = [authorship_root(authorship_key(key), part_leaves(b"".join(part)))
             for key, part in zip(part_keys, pieces)]
    tail = b"".join(roots) + signature(sodium, context(
        b"coven/write-header-signature/v1", path, prefix, header_chunk, b"".join(roots)), 0x33)
    body = prefix + header_chunk + tail
    for section, (key, part) in enumerate(zip(part_keys, pieces), 1):
        for index, frame_ in enumerate(part):
            body += chunk(sodium, encryption_key(key), path, prefix, section, index, frame_)
    write = signed(sodium, path, body)

    path = b"coven/write/" + STORE_TEXT + b"1/4"
    prefix = bytes([32, 0, 1]) + bytes([1]) * 16 + collection([])
    header = frame(1, write_id(1, 4) + timestamp(8, 0, 1) + u64(5) + u32(2) + b"\x02" + collection([]))
    header_chunk = chunk(sodium, encryption_key(17), path, prefix, 0, 0, header)
    tail = signature(sodium, context(b"coven/write-header-signature/v1", path, prefix, header_chunk, b""), 0x33)
    migration = signed(sodium, path, prefix + header_chunk + tail)

    entry_object = bounded_slot(sodium, 1, 0, frames[0])
    # The inner object remains fixed when the batch claims another slot.
    writes_slot = seal_writes_slot(sodium, 6, 5, [write, migration])
    # Separate histories whose covered writes were refused or ignored by reset.
    refused_slot = seal_writes_slot(sodium, 9, 8, [write, migration])
    reset_ignored_slot = seal_writes_slot(sodium, 10, 9, [write, migration])

    # Snapshot chunks use random nonces; these fixed ones are public test data.
    path = STORE_TEXT + b"snapshots/store/1/1"
    prefix = frames[snapshot_at]
    body = prefix + signature(
        sodium, context(b"coven/prefix-signature/v1", path, prefix), 0x33)
    for index, frame_ in enumerate(frames[snapshot_at + 1:]):
        body += sealed_chunk(sodium, encryption_key(17), path, prefix, 0, index, frame_,
                             bytes([0x60 + index]) * 24)
    snapshot = signed(sodium, path, body)

    # A second audience has its own coverage and the same active app schema.
    path = STORE_TEXT + b"snapshots/02020202-0202-0202-0202-020202020202/1/1"
    prefix = bytes([34, 0, 1]) + circle_audience(CIRCLE) + bytes([3]) * 16 + u64(8) + b"\x00"
    plaintext = frame(5, snapshot_id(circle_audience(CIRCLE), 1, 1) + u32(2) + u64(0) * 11 + u64(2))
    plaintext += last_claim(1, 7, bytes([0x71]) * 32) + last_claim(2, 1, bytes([0x21]) * 32) + frame(7, b"")
    body = prefix + signature(sodium, context(b"coven/prefix-signature/v1", path, prefix), 0x33)
    body += sealed_chunk(sodium, encryption_key(18), path, prefix, 0, 0, plaintext, bytes([0x69]) * 24)
    circle_snapshot = signed(sodium, path, body)

    # The request is sealed with the invite secret's key and signed by the pair it requests.
    join_object = single_chunk(
        sodium, 36, b"", derive(bytes([0x66]) * 32, b"coven/join-request/v1"),
        STORE_TEXT + b"join-requests/55555555-5555-5555-5555-555555555555",
        join_request(sodium), 0x52, 0x34)

    store_key, circle_key = copies(sodium)
    # The floor carries original copies and announcement bytes, each at its old path.
    announcement = bounded_slot(sodium, 10, 3, frame(15,
        snapshot_id(store_audience(), 1, 1) + u64(9) + b"\x01" + u32(2) + hashlib.sha256(snapshot).digest()), claim=9)
    circle_announcement = bounded_slot(sodium, 9, 3, frame(15,
        snapshot_id(circle_audience(CIRCLE), 1, 1) + u64(8) + b"\x00" + hashlib.sha256(circle_snapshot).digest()), claim=8)
    path = STORE_TEXT + b"floor"
    prefix = bytes([39, 0, 1]) + bytes([1]) * 16 + collection([u64(2) + text(store_key), u64(8) + text(circle_key)])
    plaintext = frame(12, u64(1) + u64(11) + u64(6) + collection([u64(10), u64(9)]))
    plaintext += frame(6, u64(10) + text(announcement)) + frame(6, u64(9) + text(circle_announcement)) + frame(7, b"")
    body = prefix
    for index, start in enumerate(range(0, len(plaintext), 65536)):
        body += sealed_chunk(sodium, encryption_key(17), path, prefix, 0, index,
                             plaintext[start:start + 65536], bytes([0x70 + index]) * 24)
    floor = signed(sodium, path, body)
    # Refused and reset-ignored parts still carry the device's included claim.
    authored = next(piece for piece in frames[:write_at] if piece[0] == 6 and piece[7] == 5)
    assert authored[-1] == 1  # Applied verdict in the source record.
    claim_snapshots = []
    for number, verdict, stored in [(8, b"\x03", refused_slot), (9, b"\x05" + u64(2), reset_ignored_slot)]:
        counts = [0] * 12
        counts[5], counts[11] = 1, 2
        body = frame(5, snapshot_id(store_audience(), 1, number) + u32(2) + b"".join(u64(n) for n in counts))
        body += frame(6, authored[7:-1] + verdict)
        body += last_claim(1, number, hashlib.sha256(stored).digest()) + last_claim(2, 1, bytes([0x21]) * 32) + frame(7, b"")
        claim_snapshots.append(body)
    (out / "snapshot-claims.hex").write_text("".join(body.hex() + "\n" for body in claim_snapshots))
    # Each case retains an actual slot digest after its slot is below the floor.
    # The four cases stop at different coverage and therefore have different last claims.
    settlements = []
    for slot, claim, stored in [(1, 1, entry_object), (2, 2, store_key),
                                (6, 5, writes_slot), (9, 8, circle_announcement)]:
        prefix = bytes([34, 0, 1]) + store_audience() + bytes([1]) * 16 + u64(slot) + b"\x00"
        header = frame(5, snapshot_id(store_audience(), 1, slot) + u32(2) + u64(0) * 11 + u64(1))
        body = header + last_claim(1, claim, hashlib.sha256(stored).digest()) + frame(7, b"")
        settlements.append(prefix + body)
    (out / "snapshot-settlement-claims.hex").write_text("".join(body.hex() + "\n" for body in settlements))
    for name, data in [
        ("sealed-access-key.hex", access_key_delivery(sodium)),
        ("sealed-write.hex", write),
        ("sealed-migration-write.hex", migration),
        ("sealed-circle-snapshot.hex", circle_snapshot),
        ("sealed-announcement.hex", announcement),
        ("sealed-circle-announcement.hex", circle_announcement),
        ("sealed-admin.hex", entry_object),
        ("sealed-snapshot.hex", snapshot),
        ("sealed-writes-slot.hex", writes_slot),
        ("sealed-refused-writes-slot.hex", refused_slot),
        ("sealed-reset-ignored-writes-slot.hex", reset_ignored_slot),
        ("sealed-floor.hex", floor),
        ("sealed-join-request.hex", join_object),
        ("sealed-store-key.hex", store_key),
        ("sealed-circle-key.hex", circle_key),
        ("restore-code.hex", restore_code()),
        ("invite-code.hex", invite_code()),
    ]:
        (out / name).write_text(data.hex() + "\n")
    (out / "codes.txt").write_text(
        code_text("CVR1-", restore_code()) + "\n" + code_text("CVI1-", invite_code()) + "\n")


if __name__ == "__main__":
    main()
