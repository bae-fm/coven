"""Generate format-1 fixtures independently of Rust, using hashlib/hmac and libsodium.

Run with Python 3 and libsodium installed. All keys, seeds and fixed nonces are
public test material. With no argument it rewrites the fixtures beside it; with
a directory argument it writes them there instead, which is how
`lib_tests::every_object_and_snapshot_section_has_pinned_bytes` compares this
generator's output with the checked-in bytes.

The member-bearing plaintext frames (store-log entries, the join request, the
clock and the pending-report grammar) and both codes are encoded here from D2,
D6, D8 and D13. The write, snapshot and posted-positions frames carry no member
data and come from the checked-in `v1.hex`. Sealed objects, key boxes and
signatures are calculated here for every fixture.

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


def entry_id(device, number):
    return u64(device) + u64(number)


def timestamp(ms, counter, device):
    return ms.to_bytes(6, "big") + u16(counter) + u64(device)


def storage_time(seconds, nanos):
    return u64(seconds) + u32(nanos)


def store_audience():
    return b"\x00"


def circle_audience(circle):
    return b"\x01" + circle


def snapshot_id(audience, device, number):
    return audience + u64(device) + u64(number)


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


def key_copy(sodium, path, audience, key_id, plaintext, seed, ephemeral_seed, nonce):
    """A kind-37 copy to Ana's credential whose seeds are `seed` (D9, D11)."""
    recipient = sealing_public(sodium, seed)
    prefix = bytes([37, 0, 1]) + audience + key_id + ANA + recipient
    ephemeral = bytes([ephemeral_seed]) * 32
    sender = sealing_public(sodium, ephemeral_seed)
    shared = c.create_string_buffer(32)
    checked(sodium.crypto_scalarmult(shared, ephemeral, recipient))
    kind = b"circle" if audience[0] == 1 else b"store"
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
    return prefix + sender + nonce + ciphertext.raw


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
    """A one-chunk object (D9): kinds 35/39 name their key; a join request has none."""
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
    for _ in range(2):
        r.take(16 * r.int(4))
    r.take(4 + 1)
    lengths = []
    for _ in range(r.int(4)):
        parse_audience(r)
        r.take(8)
        lengths.append(r.int(8))
    return lengths


# D6 store-log entries.
def entry(change, number=1, stamp=(6, 3, 1), author=ANA):
    payload = entry_id(1, number) + timestamp(*stamp) + author + collection([]) + change
    return frame(4, payload)


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
        b"\x0b" + u32(2) + snapshot_id(store_audience(), 1, 1)
        + storage_time(1_700_000_000, 5),
        b"\x0d" + snapshot_id(circle_audience(CIRCLE), 1, 1),
        b"\x0e" + ANA + b"\x01" + text(b"replacement-key") + b"\x01" + text(b"coven-member")
        + b"\x00" + text(b"coven-store"),
        b"\x0f" + store_audience() + bytes([3]) * 16 + bytes([0x91]) * 32,
        b"\x0f" + circle_audience(CIRCLE) + bytes([4]) * 16 + bytes([0xA2]) * 32,
        b"\x10" + ANA + pair(sodium, 0x34),
    ]


def join_request(sodium):
    """Ana's request answering the re-binding invite with her replacement pair."""
    return frame(9, bytes([0x55]) * 16 + ANA + pair(sodium, 0x34) + text(b"D"))


def creation_marker():
    """D9's creation marker (kind 41): store, then device 1. Unsealed."""
    return bytes([41]) + u16(1) + STORE + u64(1)


def clock():
    return frame(12, u64(1) + collection([entry_id(1, 2)]))


def objects(sodium, positions):
    """`test_utils::objects`: creation, removal, a raise, the join request,
    posted positions, the other changes, then the clock."""
    every = changes(sodium)
    frames = [
        entry(every[0]),
        entry(every[2], number=2, stamp=(7, 0, 1)),
        entry(b"\x0b" + u32(3) + snapshot_id(circle_audience((7).to_bytes(16, "big")), 9, 11)
              + storage_time(1_700_000_100, 0)),
        join_request(sodium),
        positions,
    ]
    frames += [entry(change) for index, change in enumerate(every) if index not in (0, 2, 11)]
    frames.append(clock())
    return frames


# D8 pending reports.
def path_text(path):
    return text(STORE_TEXT + path)


def pending_reports(sodium):
    """The grammar fixture `pinned_pending_frame_covers_the_complete_report_grammar`."""
    key4 = (4).to_bytes(16, "big")
    reports = []
    for failure in range(7):
        reports.append(b"\x00" + entry_id(2, failure + 1) + b"\x00" + u8(failure))
    for index, reason in enumerate([
        b"\x01" + path_text(b"devices/2/3"),
        b"\x02\x00" + path_text(b"store-log/3/1"),
        b"\x02\x01" + u64(3),
        b"\x03" + store_audience() + key4,
        b"\x04\x00" + u32(3),
        b"\x04\x01" + u32(0xFFFF),
    ]):
        reports.append(b"\x00" + entry_id(2, index + 8) + reason)
    reports.append(b"\x01" + entry_id(2, 3) + b"\x00\x04")
    # Ana's original and replacement credentials are distinct subjects,
    # ordered by sealing key after audience, key and member.
    subject_length = 1 + 1 + 16 + 16 + 32
    copies = [
        b"\x02" + store_audience() + key4 + ANA + sealing_public(sodium, 0x33) + b"\x00\x00",
        b"\x02" + store_audience() + key4 + ANA + sealing_public(sodium, 0x34)
        + b"\x03" + store_audience() + key4,
    ]
    reports += sorted(copies, key=lambda report: report[:subject_length])
    for index, reason in enumerate([b"\x00\x07", b"\x05\x00", b"\x05\x01", b"\x05\x02"]):
        reports.append(b"\x03" + u64(1) + (index + 1).to_bytes(16, "big") + reason)
    circle6 = circle_audience((6).to_bytes(16, "big"))
    reports.append(b"\x04" + snapshot_id(circle6, 2, 3) + b"\x03" + circle6 + key4)
    reports.append(b"\x05" + u64(2) + b"\x06\x01")
    payload = u64(1) + collection([]) + collection([]) + u32(1) + collection([])
    return frame(8, payload + collection(reports))


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


def main():
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

    directory = Path(__file__).resolve().parent
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else directory
    pinned = [bytes.fromhex(line) for line in (directory / "v1.hex").read_text().splitlines()]
    # The object frames come first; the write header (kind 1) starts the rest.
    rest_at = next(i for i, frame_ in enumerate(pinned) if frame_[0] == 1)
    positions = [frame_ for frame_ in pinned[:rest_at] if frame_[0] == 8]
    assert len(positions) == 1
    frames = objects(sodium, positions[0]) + pinned[rest_at:]
    (out / "v1.hex").write_text("".join(frame_.hex() + "\n" for frame_ in frames))
    (out / "clock.hex").write_text(clock().hex() + "\n")
    (out / "creation-marker.hex").write_text(creation_marker().hex() + "\n")
    (out / "pending-reports.hex").write_text(pending_reports(sodium).hex() + "\n")

    path = STORE_TEXT + b"devices/1/3"
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

    # Creation is verified with the admin pair it supplies (seeds 0x33).
    path = STORE_TEXT + b"store-log/1/1"
    prefix = bytes([33, 0, 1]) + bytes([1]) * 16
    assert frames[0][0] == 4
    body = prefix + chunk(sodium, encryption_key(17), path, prefix, 0, 0, frames[0])
    entry_object = signed(sodium, path, body)

    # Snapshot chunks use random nonces; these fixed ones are public test data.
    path = STORE_TEXT + b"snapshots/store/1/1"
    prefix = frames[snapshot_at]
    body = prefix + signature(
        sodium, context(b"coven/prefix-signature/v1", path, prefix), 0x33)
    for index, frame_ in enumerate(frames[snapshot_at + 1:]):
        body += sealed_chunk(sodium, encryption_key(17), path, prefix, 0, index, frame_,
                             bytes([0x60 + index]) * 24)
    snapshot = signed(sodium, path, body)

    # Positions, clock and join request use random nonces; these are fixed.
    store_key_id = bytes([1]) * 16
    positions_object = single_chunk(sodium, 35, store_key_id, encryption_key(17),
                                    STORE_TEXT + b"positions/1", positions[0], 0x51, 0x33)
    clock_object = single_chunk(sodium, 39, store_key_id, encryption_key(17),
                                STORE_TEXT + b"clock/1", clock(), 0x39, 0x33)
    # The request is sealed with the invite secret's key and signed by the pair it requests.
    join_object = single_chunk(
        sodium, 36, b"", derive(bytes([0x66]) * 32, b"coven/join-request/v1"),
        STORE_TEXT + b"join-requests/55555555-5555-5555-5555-555555555555",
        join_request(sodium), 0x52, 0x34)

    # Key copies use random ephemeral keys and nonces; these fixed ones are public.
    # Both go to Ana's one id: the store key to her original credential and the
    # circle key to her replacement.
    store_key = key_copy(sodium, STORE_TEXT + b"keys/7/9", b"\x00", store_key_id,
                         store_key_id + bytes([17]) * 32, 0x33, 0x55, 0x44)
    circle_key_id = bytes([3]) * 16
    circle_key = key_copy(sodium, STORE_TEXT + b"keys/7/10", b"\x01" + CIRCLE, circle_key_id,
                          CIRCLE + circle_key_id + bytes([18]) * 32, 0x34, 0x56, 0x45)
    for name, data in [
        ("sealed-write.hex", write),
        ("sealed-store-log.hex", entry_object),
        ("sealed-snapshot.hex", snapshot),
        ("sealed-positions.hex", positions_object),
        ("sealed-clock.hex", clock_object),
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
