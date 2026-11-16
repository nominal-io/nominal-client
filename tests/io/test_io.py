from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pytest

import nominal.io as nio
from nominal.io import Reader, ReaderSpec
from nominal.io.schema import COLUMNS, Chunk, ChunkBuilder, validate_frame
from nominal.io.testing import check_reader


def _chunk(times: list[int], channel: str = "a") -> Chunk:
    builder = ChunkBuilder()
    builder.add(times, channel, [1.0] * len(times))
    return builder.build()


class _Lines(Reader):
    """One chunk per line of ints: each line is a comma-separated list of timestamps."""

    def read_chunks(self, path: Any, **options: Any) -> Iterator[Chunk]:
        for line in Path(path).read_text().splitlines():
            yield _chunk([int(t) for t in line.split(",")], options.get("channel", "a"))


class _Sniffing(Reader):
    """Reads one sample, and accepts files that start with its magic text."""

    def __init__(self, magic: str) -> None:
        self.magic = magic

    def sniff(self, path: Any) -> bool | None:
        return Path(path).read_text().startswith(self.magic)

    def read_chunks(self, path: Any, **options: Any) -> Iterator[Chunk]:
        yield _chunk([1], self.magic)


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    before = {f.name for f in nio.list_formats()}
    yield
    for f in nio.list_formats():
        if f.name not in before:
            nio.unregister(f.name)


def test_validate_frame() -> None:
    """validate_frame accepts a long-form frame and rejects wrong columns or a non-UTC timestamp."""
    df = _chunk([2, 1]).to_pandas()
    assert validate_frame(df) is df
    with pytest.raises(ValueError, match="expected columns"):
        validate_frame(pd.DataFrame({"x": [1]}))
    with pytest.raises(ValueError, match="UTC"):
        validate_frame(df.assign(timestamp=pd.Timestamp("2020-01-01")))


def test_builder_encodes_channels_and_tags() -> None:
    """ChunkBuilder encodes channel names and tag sets once and keeps missing values as NaN."""
    b = ChunkBuilder()
    b.add([3, 1], "a", [1.0, 2.0], {"run": "x"})
    b.add_rows([2, 4, 5], ["b", "a", "b"], [10, None, 7.5], [{}, {"run": "x"}, {}])
    chunk = b.build()
    assert len(b) == len(chunk) == 5
    assert chunk.channel_names == ("a", "b")
    assert chunk.tag_sets == ({"run": "x"}, {})
    assert chunk.strings is None and np.isnan(chunk.values[3])
    assert ChunkBuilder().build().validate().tag_sets == ()


def test_builder_validates() -> None:
    """ChunkBuilder rejects mismatched value counts and boolean samples."""
    with pytest.raises(ValueError, match="2 values for 3 timestamps"):
        ChunkBuilder().add([1, 2, 3], "a", [1.0, 2.0])
    b = ChunkBuilder()
    b.add([1], "a", [True])
    with pytest.raises(TypeError, match="boolean"):
        b.build()


def test_chunk_validate() -> None:
    """Chunk.validate rejects wrong dtypes, out-of-range codes and mismatched lengths."""
    chunk = _chunk([1, 2])
    with pytest.raises(ValueError, match="int64"):
        Chunk(
            chunk.timestamps_ns.astype("int32"),
            chunk.channels,
            chunk.channel_names,
            chunk.values,
            None,
            chunk.tags,
            chunk.tag_sets,
        ).validate()
    with pytest.raises(ValueError, match="code outside"):
        Chunk(
            chunk.timestamps_ns, chunk.channels + 5, chunk.channel_names, chunk.values, None, chunk.tags, chunk.tag_sets
        ).validate()
    with pytest.raises(ValueError, match="rows"):
        Chunk(
            chunk.timestamps_ns, chunk.channels[:1], chunk.channel_names, chunk.values, None, chunk.tags, chunk.tag_sets
        ).validate()


def test_sort_is_stable_and_concat_merges_names() -> None:
    """Chunk.concat merges channel names and tag sets, and sort keeps tied rows in insertion order."""
    b1, b2 = ChunkBuilder(), ChunkBuilder()
    b1.add([5, 1], "a", [1.0, 2.0], {"k": "1"})
    b2.add([1, 3], "b", ["s", "t"], {"k": "2"})
    b2.add([1], "a", [9.0], {"k": "1"})
    joined = Chunk.concat([b1.build(), b2.build()]).sort()
    df = joined.to_pandas()
    assert df["timestamp"].astype("int64").tolist() == [1, 1, 1, 3, 5]
    assert df["channel"].tolist() == ["a", "b", "a", "b", "a"]  # ties keep insertion order
    assert df["value"].tolist() == [2.0, "s", 9.0, "t", 1.0]
    assert df["tags"].tolist()[:3] == [{"k": "1"}, {"k": "2"}, {"k": "1"}]
    assert len(joined.tag_sets) == 2  # the {"k": "1"} set was merged, not duplicated


def test_to_pandas_value_dtype() -> None:
    """to_pandas gives a float64 value column unless a string sample is present, then a mixed object column."""
    assert _chunk([1]).to_pandas()["value"].dtype == "float64"
    b = ChunkBuilder()
    b.add([1, 2], "a", [1.0, "x"])
    df = b.build().to_pandas()
    assert df["value"].dtype == object and df["value"].tolist() == [1.0, "x"]
    assert list(df.columns) == COLUMNS and str(df["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert df["timestamp"].is_monotonic_increasing


def test_to_polars_and_engines() -> None:
    """to_frame builds the polars layout and rejects an unknown engine."""
    b = ChunkBuilder()
    b.add([2, 1], "a", [1.0, "x"], {"run": "r"})
    b.add([3], "b", [None])
    df = b.build().to_frame("polars")
    assert df.columns == ["timestamp", "channel", "value", "value_str", "tags"]
    assert df["timestamp"].dtype == pl.Datetime("ns", "UTC")
    assert df["channel"].to_list() == ["a", "a", "b"]
    assert df["value"].to_list() == [None, 1.0, None]  # a string sample and a missing one are null
    assert df["value_str"].to_list() == ["x", None, None]
    assert df["tags"].to_list()[0] == [{"key": "run", "value": "r"}]
    assert df["tags"].to_list()[2] == []
    assert isinstance(_chunk([1]).to_frame("pandas"), pd.DataFrame)
    with pytest.raises(ValueError, match="unknown engine"):
        _chunk([1]).to_frame("spark")


def test_from_pandas_round_trip() -> None:
    """A pandas frame converts to a Chunk and back unchanged, and a non-long-form frame is rejected."""
    b = ChunkBuilder()
    b.add([2, 1], "a", [1.0, "x"], {"run": "r"})
    b.add([3], "b", [4.0])
    df = b.build().to_pandas()
    back = Chunk.from_pandas(df).to_pandas()
    pd.testing.assert_frame_equal(back, df)
    with pytest.raises(ValueError, match="expected columns"):
        Chunk.from_pandas(pd.DataFrame({"x": [1]}))


def test_read_concatenates_and_sorts(tmp_path: Path) -> None:
    """Read joins a reader's chunks, sorts by time, and passes options to the reader."""
    nio.register(ReaderSpec("lines", (".lines",), _Lines))
    path = tmp_path / "x.lines"
    path.write_text("5,6\n1,2\n")
    chunks = list(nio.read_chunks(path))
    assert [len(c) for c in chunks] == [2, 2]
    df = nio.read(path)
    assert df["timestamp"].astype("int64").tolist() == [1, 2, 5, 6]
    assert nio.read(path, channel="b")["channel"].unique().tolist() == ["b"]


def test_read_engines(tmp_path: Path) -> None:
    """Read returns pandas by default, polars on request, and read_chunks yields Chunks unless given an engine."""
    nio.register(ReaderSpec("lines", (".lines",), _Lines))
    path = tmp_path / "x.lines"
    path.write_text("2,1\n")
    assert isinstance(nio.read(path), pd.DataFrame)
    assert isinstance(nio.read(path, engine="polars"), pl.DataFrame)
    assert all(isinstance(c, Chunk) for c in nio.read_chunks(path))
    assert all(isinstance(c, pd.DataFrame) for c in nio.read_chunks(path, engine="pandas"))
    with pytest.raises(ValueError, match="unknown engine"):
        nio.read(path, engine="spark")


def test_read_empty(tmp_path: Path) -> None:
    """Reading a file with no samples gives an empty frame in either engine."""
    nio.register(ReaderSpec("lines", (".lines",), _Lines))
    path = tmp_path / "x.lines"
    path.write_text("")
    assert nio.read(path).empty
    assert nio.read(path, engine="polars").is_empty()


def test_invalid_chunk_rejected(tmp_path: Path) -> None:
    """A reader that yields something other than a Chunk is reported by name."""

    class Bad(Reader):
        def read_chunks(self, path: Any, **options: Any) -> Iterator[Any]:
            yield pd.DataFrame({"x": [1]})

    nio.register(ReaderSpec("bad", (".bad",), Bad))
    path = tmp_path / "f.bad"
    path.touch()
    with pytest.raises(ValueError, match="not a Chunk"):
        nio.read(path)


def test_longest_extension_wins(tmp_path: Path) -> None:
    """The format claiming the longest matching extension is chosen over a shorter one."""
    nio.register(ReaderSpec("meta", (".x-meta",), lambda: _Sniffing("meta")))
    nio.register(ReaderSpec("plain", (".meta",), lambda: _Sniffing("plain")))
    path = tmp_path / "a.x-meta"
    path.write_text("zzz")
    assert nio.resolve_format(path).name == "meta"


def test_sniff_breaks_ties(tmp_path: Path) -> None:
    """When formats share an extension, the one whose sniff accepts the file is chosen."""
    nio.register(ReaderSpec("one", (".dat",), lambda: _Sniffing("ONE")))
    nio.register(ReaderSpec("two", (".dat",), lambda: _Sniffing("TWO")))
    path = tmp_path / "f.dat"
    path.write_text("TWO...")
    assert nio.resolve_format(path).name == "two"
    assert nio.read(path)["channel"].iloc[0] == "TWO"


def test_ambiguous_raises_and_format_overrides(tmp_path: Path) -> None:
    """An ambiguous file raises an error naming the candidates, and format= resolves it."""
    nio.register(ReaderSpec("one", (".dat",), lambda: _Sniffing("ONE")))
    nio.register(ReaderSpec("two", (".dat",), lambda: _Sniffing("TWO")))
    path = tmp_path / "f.dat"
    path.write_text("neither")
    with pytest.raises(ValueError, match="candidates: one, two.*format="):
        nio.read(path)
    assert nio.read(path, format="one")["channel"].iloc[0] == "ONE"


def test_unknown_extension_and_format(tmp_path: Path) -> None:
    """An unknown extension or format name raises a ValueError."""
    with pytest.raises(ValueError, match="no format reads"):
        nio.read(tmp_path / "f.nope")
    with pytest.raises(ValueError, match="unknown format"):
        nio.read(tmp_path / "f.nope", format="nope")


def test_missing_dependency_is_actionable(tmp_path: Path) -> None:
    """A reader whose module can't be imported raises MissingDependencyError with the install hint."""
    nio.register(ReaderSpec("ghost", (".ghost",), "no_such_pkg_xyz:Reader", install_hint="pip install nominal[ghost]"))
    path = tmp_path / "f.ghost"
    path.touch()
    with pytest.raises(nio.MissingDependencyError, match=r"pip install nominal\[ghost\]"):
        nio.read(path)
    (listed,) = [f for f in nio.list_formats() if f.name == "ghost"]  # listing never imports it
    assert listed.extensions == (".ghost",)


def test_missing_dependency_candidate_does_not_block_sniffing(tmp_path: Path) -> None:
    """A candidate that can't be loaded is skipped while sniffing rather than failing the choice."""
    nio.register(ReaderSpec("ghost", (".dat",), "no_such_pkg_xyz:Reader"))
    nio.register(ReaderSpec("real", (".dat",), lambda: _Sniffing("REAL")))
    path = tmp_path / "f.dat"
    path.write_text("REAL")
    assert nio.resolve_format(path).name == "real"


def test_register_conflicts_and_decorator() -> None:
    """register_reader registers a class, and a duplicate name needs replace=True."""

    @nio.register_reader("deco", [".deco"], description="d")
    class Deco(_Lines):
        pass

    assert [f.description for f in nio.list_formats() if f.name == "deco"] == ["d"]
    with pytest.raises(ValueError, match="already registered"):
        nio.register(ReaderSpec("deco", (".deco",), Deco))
    nio.register(ReaderSpec("deco", (".deco",), Deco), replace=True)


def test_spec_validation() -> None:
    """ReaderSpec rejects extensions without a dot or in uppercase."""
    with pytest.raises(ValueError):
        ReaderSpec("x", ("csv",), _Lines)
    with pytest.raises(ValueError):
        ReaderSpec("x", (".CSV",), _Lines)


def test_entry_point_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discovery keeps good entry points and warns about broken, wrongly typed or misnamed ones."""
    import nominal.io._registry as reg

    good = ReaderSpec("ep-good", (".epg",), _Lines)

    class EP:
        def __init__(self, name: str, obj: Any) -> None:
            self.name, self.value, self._obj = name, "mod:obj", obj

        def load(self) -> Any:
            if isinstance(self._obj, Exception):
                raise self._obj
            return self._obj

    eps = [EP("ep-good", good), EP("ep-broken", ImportError("boom")), EP("ep-wrong", "not a spec"), EP("renamed", good)]
    monkeypatch.setattr(reg, "entry_points", lambda group: eps)
    monkeypatch.setattr(reg, "_discovered", False)
    monkeypatch.setattr(reg, "_specs", {})
    with pytest.warns(RuntimeWarning) as record:
        names = [f.name for f in nio.list_formats()]
    assert names == ["ep-good"]
    assert len(record) == 3  # broken, wrong type, name mismatch


def test_check_reader(tmp_path: Path) -> None:
    """check_reader returns the data for a conforming reader and fails on an empty or mismatched sample."""
    spec = ReaderSpec("lines", (".lines",), _Lines)
    path = tmp_path / "x.lines"
    path.write_text("1,2\n3\n")
    assert len(check_reader(spec, path)) == 3
    path.write_text("")
    with pytest.raises(AssertionError, match="yielded nothing"):
        check_reader(spec, path)
    with pytest.raises(AssertionError, match="doesn't end in"):
        check_reader(spec, tmp_path / "x.other")


def test_builder_keeps_numpy_numbers_numeric() -> None:
    """Integer and float32 numpy values stay in the float64 column, not the string column."""
    b = ChunkBuilder()
    b.add([1, 2, 3], "ints", np.array([1, 2, 3]))
    b.add([1, 2], "f32", [np.float32(1.5), np.float32(2.5)])
    b.add([1], "u8", np.array([7], dtype=np.uint8))
    chunk = b.build()
    assert chunk.strings is None
    assert chunk.values.tolist() == [1.0, 2.0, 3.0, 1.5, 2.5, 7.0]


def test_read_chunks_raises_on_first_iteration(tmp_path: Path) -> None:
    """read_chunks is a generator, so an unknown file type is reported when iterating, not when called."""
    chunks = nio.read_chunks(tmp_path / "f.nope")  # no error yet
    with pytest.raises(ValueError, match="no format reads"):
        next(chunks)


def test_unknown_engine_is_rejected_before_reading(tmp_path: Path) -> None:
    """Read checks the engine before it opens the file."""
    with pytest.raises(ValueError, match="unknown engine"):
        nio.read(tmp_path / "does-not-exist.nope", engine="spark")


def test_import_inside_a_reader_becomes_missing_dependency(tmp_path: Path) -> None:
    """A package a reader imports lazily, when absent, raises MissingDependencyError with the install hint."""

    class NeedsPackage(Reader):
        def read_chunks(self, path: Any, **options: Any) -> Iterator[Chunk]:
            import no_such_package_xyz  # noqa: F401

            yield _chunk([1])

    nio.register(ReaderSpec("needs", (".needs",), NeedsPackage, install_hint="pip install nominal[needs]"))
    path = tmp_path / "f.needs"
    path.touch()
    with pytest.raises(nio.MissingDependencyError, match=r"no_such_package_xyz.*nominal\[needs\]"):
        nio.read(path)


def _discovery_with(monkeypatch: pytest.MonkeyPatch, plugin_load: Any) -> None:
    import nominal.io._registry as reg

    class EP:
        name, value = "plugin", "mod:obj"

        def load(self) -> Any:
            return plugin_load()

    monkeypatch.setattr(reg, "entry_points", lambda group: [EP()])
    monkeypatch.setattr(reg, "_discovered", False)
    monkeypatch.setattr(reg, "_specs", {})
    monkeypatch.setattr(reg, "_readers", {})


def test_plugin_may_register_while_it_is_discovered(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin module that registers a reader or lists formats while importing doesn't deadlock discovery."""
    import threading

    def plugin() -> ReaderSpec:
        nio.register(ReaderSpec("extra", (".extra",), _Lines))  # what @register_reader does at import
        assert [f.name for f in nio.list_formats()] == ["extra"]
        return ReaderSpec("plugin", (".plg",), _Lines)

    _discovery_with(monkeypatch, plugin)
    names: list[str] = []
    worker = threading.Thread(target=lambda: names.extend(f.name for f in nio.list_formats()), daemon=True)
    worker.start()
    worker.join(timeout=5)
    assert not worker.is_alive(), "discovery deadlocked"
    assert names == ["extra", "plugin"]


def test_replacing_a_format_while_it_loads_does_not_cache_the_old_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    """If a format is replaced during its reader's load, the replaced reader isn't cached."""
    replacement = ReaderSpec("swap", (".swap",), lambda: _Sniffing("NEW"))

    def old_factory() -> Reader:
        nio.register(replacement, replace=True)
        return _Sniffing("OLD")

    nio.register(ReaderSpec("swap", (".swap",), old_factory))
    nio.get_reader("swap")  # loads the old reader, and the format is replaced meanwhile
    assert nio.get_reader("swap").magic == "NEW"  # type: ignore[attr-defined]
