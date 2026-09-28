"""TDMS time as int64 epoch nanoseconds.

Present-day epoch nanoseconds are ~1.7e18, beyond float64's exact-integer range
of 2^53, so any arithmetic that lets the epoch offset into a float rounds every
timestamp onto a 256 ns grid. Every conversion here keeps the large offset in
int64 and confines float work to the small quantities: a sample's position
times its period, or the fractional part of a seconds value.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from nominal.tdms._reader import NiTimestamp

NS_PER_SECOND = 1_000_000_000

# TDMS counts seconds from 1904-01-01; Unix from 1970-01-01.
NI_EPOCH_OFFSET_S = -2_082_844_800

_TIMESTAMP_FIELDS = {"seconds", "fractions"}


def is_timestamp_dtype(dtype: np.dtype[Any] | None) -> bool:
    """A raw TDMS timestamp channel: a struct of seconds and 2^-64 s fractions."""
    return dtype is not None and dtype.names is not None and set(dtype.names) == _TIMESTAMP_FIELDS


def timestamp_to_ns(value: NiTimestamp) -> int:
    """Epoch nanoseconds for one TDMS timestamp property, exactly, with Python ints."""
    seconds = int(value.seconds) + NI_EPOCH_OFFSET_S
    fraction_ns = (int(value.fractions) * NS_PER_SECOND + (1 << 63)) >> 64
    return seconds * NS_PER_SECOND + fraction_ns


def timestamps_to_ns(values: np.ndarray) -> np.ndarray:
    """Epoch nanoseconds for a TDMS timestamp channel's samples.

    The 2^-64 s fraction is a uint64, so `fraction * 1e9` overflows; split it
    into 32-bit halves and combine in int64. Exact to well under a nanosecond.
    """
    fractions = values["fractions"]
    high = (fractions >> np.uint64(32)).astype(np.int64)
    low = (fractions & np.uint64(0xFFFFFFFF)).astype(np.int64)
    fraction_ns = (high * NS_PER_SECOND + ((low * NS_PER_SECOND) >> 32) + (1 << 31)) >> 32
    seconds = values["seconds"].astype(np.int64) + NI_EPOCH_OFFSET_S
    return seconds * NS_PER_SECOND + fraction_ns


def waveform_ns(start_ns: int, first_position: int, count: int, increment_ns: float) -> np.ndarray:
    """Timestamps for `count` samples from sample index `first_position`.

    Each sample's offset from the start is rounded independently, so a
    non-integer period (1/3 s, 1/30 s) never accumulates error; an integer
    period stays in int64 entirely.
    """
    positions = np.arange(first_position, first_position + count, dtype=np.int64)
    if increment_ns == int(increment_ns):
        offsets = positions * int(increment_ns)
    else:
        offsets = np.rint(positions * increment_ns).astype(np.int64)
    return start_ns + offsets
