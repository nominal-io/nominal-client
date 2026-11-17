"""Test helper for authors of readers."""

from __future__ import annotations

from os import PathLike
from typing import Any

from nominal.io._reader import ReaderSpec
from nominal.io.schema import Chunk


def check_reader(spec: ReaderSpec, sample: str | PathLike[str], **options: Any) -> Chunk:
    """Assert that a reader follows the contract on a sample file, and return what it read.

    Checks that the spec's extensions fit the sample, that ``sniff`` accepts the sample, that every
    chunk is a valid `~nominal.io.schema.Chunk`, and that there is at least one row.

    Use it from a pytest test::

        def test_my_reader():
            check_reader(SPEC, "tests/data/sample.mylog")

    Returns:
        All the chunks joined and sorted by time; call ``.to_pandas()`` on it to look at the data.

    Raises:
        AssertionError: If the reader breaks the contract.
    """
    name = str(sample).lower()
    assert any(name.endswith(e) for e in spec.extensions), f"{sample} doesn't end in any of {spec.extensions}"
    reader = spec.load()
    verdict = reader.sniff(sample)
    assert verdict in (True, None), f"sniff() rejected the sample (returned {verdict!r})"
    chunks = list(reader.read_chunks(sample, **options))
    assert chunks, "read_chunks() yielded nothing for the sample"
    for i, chunk in enumerate(chunks):
        assert isinstance(chunk, Chunk), f"chunk {i} is a {type(chunk).__name__}, not a Chunk"
        try:
            chunk.validate()
        except ValueError as e:
            raise AssertionError(f"chunk {i}: {e}") from e
    joined = Chunk.concat(chunks).sort()
    assert len(joined) > 0, "the sample produced no rows"
    return joined
