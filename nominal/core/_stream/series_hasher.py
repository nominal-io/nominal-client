"""Port of Scout's SeriesHasher + Sip128Hasher for computing deterministic series UUIDs.

This replicates the exact algorithm from:
  shared/nominal-utils/src/main/java/io/nominal/utils/series/SeriesHasher.java
  shared/nominal-utils/src/main/java/io/nominal/utils/series/Sip128Hasher.java

The series UUID is a SipHash-128 (with zero key) of four strings:
  (data_source_uuid, channel, tags_string, data_type)

The tags_string is sorted key:value pairs joined by commas.
"""

from __future__ import annotations

import struct
import uuid


def series_uuid(
    channel: str,
    data_source_uuid: str,
    tags: dict[str, str],
    data_type: str,
) -> uuid.UUID:
    """Compute a deterministic series UUID matching Scout's SeriesHasher."""
    return _sip128_hash_to_uuid([data_source_uuid, channel, _tags_string(tags), data_type])


def _tags_string(tags: dict[str, str]) -> str:
    """Serialize tags to a stable string: sorted key:value pairs joined by commas."""
    sorted_keys = sorted(tags.keys())
    return ",".join(f"{k}:{tags[k]}" for k in sorted_keys)


def _sip128_hash_to_uuid(args: list[str]) -> uuid.UUID:
    """SipHash-128 with zero key, feeding each string's UTF-8 bytes, returning a UUID."""
    hash_bytes = _sip128_hash(args)
    # Java: first 8 bytes = MSB, last 8 bytes = LSB
    msb = int.from_bytes(hash_bytes[:8], "big")
    lsb = int.from_bytes(hash_bytes[8:16], "big")
    return uuid.UUID(int=(msb << 64) | lsb)


def _sip128_hash(args: list[str]) -> bytes:
    """Compute SipHash-128 with a zero key over concatenated string args.

    Implements SipHash-2-4-128 (the BouncyCastle default used by Scout's Java code).
    """
    # Initialize with zero key (16 bytes of zeros)
    k0 = 0
    k1 = 0

    v0 = k0 ^ 0x736F6D6570736575
    v1 = k1 ^ 0x646F72616E646F6D
    v2 = k0 ^ 0x6C7967656E657261
    v3 = k1 ^ 0x7465646279746573

    # SipHash-128: XOR v1 with 0xEE at init
    v1 ^= 0xEE

    # Concatenate all args as bytes (matching Java: each arg fed sequentially)
    data = b"".join(arg.encode("utf-8") for arg in args)

    length = len(data)
    # Process 8-byte blocks
    num_blocks = length // 8
    for i in range(num_blocks):
        m = struct.unpack_from("<Q", data, i * 8)[0]
        v3 ^= m
        for _ in range(2):  # SipHash-2-4: 2 compression rounds
            v0, v1, v2, v3 = _sip_round(v0, v1, v2, v3)
        v0 ^= m

    # Process remaining bytes
    remaining = data[num_blocks * 8 :]
    b = (length & 0xFF) << 56
    for i, byte in enumerate(remaining):
        b |= byte << (8 * i)

    v3 ^= b
    for _ in range(2):
        v0, v1, v2, v3 = _sip_round(v0, v1, v2, v3)
    v0 ^= b

    # Finalization for 128-bit output
    v2 ^= 0xEE
    for _ in range(4):  # SipHash-2-4: 4 finalization rounds
        v0, v1, v2, v3 = _sip_round(v0, v1, v2, v3)
    first_half = v0 ^ v1 ^ v2 ^ v3

    v1 ^= 0xDD
    for _ in range(4):
        v0, v1, v2, v3 = _sip_round(v0, v1, v2, v3)
    second_half = v0 ^ v1 ^ v2 ^ v3

    return struct.pack("<QQ", first_half, second_half)


_MASK64 = 0xFFFFFFFFFFFFFFFF


def _rotl64(x: int, b: int) -> int:
    return ((x << b) | (x >> (64 - b))) & _MASK64


def _sip_round(v0: int, v1: int, v2: int, v3: int) -> tuple[int, int, int, int]:
    v0 = (v0 + v1) & _MASK64
    v1 = _rotl64(v1, 13)
    v1 ^= v0
    v0 = _rotl64(v0, 32)
    v2 = (v2 + v3) & _MASK64
    v3 = _rotl64(v3, 16)
    v3 ^= v2
    v0 = (v0 + v3) & _MASK64
    v3 = _rotl64(v3, 21)
    v3 ^= v0
    v2 = (v2 + v1) & _MASK64
    v1 = _rotl64(v1, 17)
    v1 ^= v2
    v2 = _rotl64(v2, 32)
    return v0, v1, v2, v3
