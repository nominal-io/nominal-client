from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import pytest

from nominal.core._point_cloud import _TYPE_INFERENCE_SAMPLE_ROWS, _describe_point_cloud_csv

_TIMED_CSV = "x,y,z,t_s\n0,0,0,1.5\n1,1,1,0.25\n2,2,2,9.75\n"
_RGB_HEADER = "x,y,z,color,intensity,ring,label"
_RGB_ROW = ["0,0,0,c04422,0.5,3,kerb"]


def _describe(tmp_path: Path, text: str, **kwargs: Any) -> Any:
    path = tmp_path / "cloud.csv"
    path.write_text(text)
    return _describe_point_cloud_csv(path, **kwargs)


def _config(
    tmp_path: Path, header: str, rows: Sequence[str] = (), overrides: Any = None, **kwargs: Any
) -> dict[str, Any]:
    """The wire import config built from a header and sample rows.

    Goes through the real reader so the assertions cover the whole path from file
    to the payload the backend has to accept.
    """
    text = header + "\n" + "".join(f"{row}\n" for row in rows)
    return _describe(tmp_path, text, column_types=overrides, **kwargs).import_config._to_wire()


# --- the import config the backend has to accept ------------------------------


def test_import_config_emits_the_v2_shape(tmp_path: Path) -> None:
    """geometry_type and columns live under `format`; the rejected v1 shape put them at top level."""
    config = _config(tmp_path, "x,y,z", [])

    assert set(config) == {"archetype", "format"}
    assert config["format"]["kind"] == "csv"
    # The importer's GeometryType enum is PascalCase, not conjure's SCREAMING_CASE.
    assert config["format"]["geometry_type"] == "Point"
    # All seven buckets must be present even when empty, or the request is rejected.
    assert set(config["format"]["columns"]) == {"geometry", "real", "int", "string", "rgb", "normal", "bool"}


def test_xyz_are_geometry_whatever_their_case_and_are_not_attributes(tmp_path: Path) -> None:
    """x/y/z are matched case-insensitively and carried as geometry rather than as attributes."""
    config = _config(tmp_path, "A,X,Y,Z", ["1,0,0,0"])

    assert config["format"]["columns"]["geometry"] == [1, 2, 3]
    assert [a["header"]["name"] for a in config["archetype"]["attributes"]] == ["A"]


def test_archetype_is_ordered_by_bucket_not_by_header(tmp_path: Path) -> None:
    """Attribute k must line up with the k-th slot the importer assigns.

    The importer walks the buckets in the order real, int, string, rgb, normal,
    bool when handing columns their attribute slot. Declaring the archetype in
    header order instead means an attribute is named and typed after one column
    while holding another column's values.
    """
    header = "x,y,z,speed,ring,label,heading,gear"
    config = _config(tmp_path, header, ["0,0,0,1.5,3,kerb,88.5,4"], {"ring": "int", "gear": "int"})

    assert [a["header"]["name"] for a in config["archetype"]["attributes"]] == [
        "speed",
        "heading",  # real
        "ring",
        "gear",  # int
        "label",  # string
    ]
    columns = config["format"]["columns"]
    assert columns["real"] == [3, 6]
    assert columns["int"] == [4, 7]
    assert columns["string"] == [5]


@pytest.mark.parametrize(
    ("kind", "wire_type", "reductions"),
    [
        ("real", {"Real": "IndependentValue"}, ["Min", "Max", "Mean"]),
        # Mean is not a valid sampler for an Int-typed attribute.
        ("int", "Int", ["Min", "Max"]),
        # Strings have no useful scalar aggregation.
        ("string", "String", []),
    ],
)
def test_each_column_type_declares_the_reductions_its_wire_type_allows(
    tmp_path: Path, kind: str, wire_type: object, reductions: list[str]
) -> None:
    """Without reductions an attribute cannot drive ramp colouring or value-range filtering at all."""
    config = _config(tmp_path, "x,y,z,col", ["0,0,0,1"], {"col": kind})  # type: ignore[dict-item]

    attribute = config["archetype"]["attributes"][0]
    assert attribute["header"]["ty"] == wire_type
    assert attribute["reductions"] == reductions


# --- column type inference ----------------------------------------------------


@pytest.mark.parametrize(
    ("values", "bucket"),
    [
        (["1", "2", "3"], "real"),  # integer-looking, but int is never inferred
        (["1", "0.998"], "real"),
        (["-1", "+2"], "real"),
        (["1e3"], "real"),
        (["1", "", "2"], "real"),  # blanks are skipped, not treated as strings
        (["1", "abc"], "string"),
        (["", ""], "string"),  # nothing to measure, so the type that cannot misrepresent it
    ],
)
def test_a_column_is_real_only_when_every_populated_sample_is_numeric(
    tmp_path: Path, values: list[str], bucket: str
) -> None:
    """One non-numeric value anywhere in the sample forces the whole column to string."""
    config = _config(tmp_path, "x,y,z,col", [f"0,0,0,{v}" for v in values])

    assert config["format"]["columns"][bucket] == [3]


def test_integer_looking_columns_are_typed_real_not_int(tmp_path: Path) -> None:
    """Only the first rows are sampled, so an integral sample is no evidence the rest is integral.

    An Int-typed attribute truncates every float the importer reads into it, for
    the whole file and without complaint. Real represents these values exactly,
    so inferring real cannot lose data the way inferring int can.
    """
    config = _config(tmp_path, "x,y,z,count", ["0,0,0,1", "0,0,0,2"])

    assert config["format"]["columns"]["int"] == []
    assert config["format"]["columns"]["real"] == [3]


def test_column_types_override_wins_over_inference(tmp_path: Path) -> None:
    """An explicit override is the only way to get Int, and it replaces what the sample inferred."""
    config = _config(tmp_path, "x,y,z,ring", ["0,0,0,1"], {"ring": "int"})

    assert config["format"]["columns"]["int"] == [3]
    assert config["format"]["columns"]["real"] == []


def test_column_types_override_cannot_reclassify_geometry(tmp_path: Path) -> None:
    """Overriding x/y/z must not pull a geometry column into the attribute buckets."""
    config = _config(tmp_path, "x,y,z,count", ["0,0,0,1"], {"x": "real"})

    assert config["format"]["columns"]["geometry"] == [0, 1, 2]
    assert [a["header"]["name"] for a in config["archetype"]["attributes"]] == ["count"]


def test_inference_reads_past_the_first_row(tmp_path: Path) -> None:
    """A value that only appears in a later row still decides the column's type."""
    rows = "\n".join("0,0,0,1" for _ in range(50)) + "\n0,0,0,unknown"

    described = _describe(tmp_path, f"x,y,z,stress\n{rows}\n")

    assert described.import_config._to_wire()["format"]["columns"]["string"] == [3]


def test_blank_rows_do_not_shift_column_indices(tmp_path: Path) -> None:
    """Blank lines between data rows are skipped rather than sampled as empty columns."""
    described = _describe(tmp_path, "x,y,z,label\n0,0,0,kerb\n\n1,1,1,wall\n")

    assert described.import_config._to_wire()["format"]["columns"]["string"] == [3]


# --- colour -------------------------------------------------------------------


def test_rgb_column_becomes_a_single_rgb_attribute(tmp_path: Path) -> None:
    """One hex column, one Rgb attribute, sorted after the scalar buckets.

    The importer reads the cell with `u8::from_str_radix` over three 2-character
    slices and skips it unless it is exactly six characters, so three separate
    0-255 columns silently produce black rather than an error.
    """
    config = _config(tmp_path, _RGB_HEADER, _RGB_ROW, rgb_column="color")

    assert config["format"]["columns"]["rgb"] == [3]
    colour = config["archetype"]["attributes"][-1]
    assert colour["header"] == {"name": "color", "ty": "Rgb"}
    # Without a reduction the renderer reports the attribute as not colourable.
    assert colour["reductions"] == ["Mean"]
    # The colour column must not also be sampled into the string bucket.
    assert config["format"]["columns"]["string"] == [6]


def test_without_rgb_column_a_hex_column_stays_a_string(tmp_path: Path) -> None:
    """Colour is opt-in: a hex column is just text to a caller who names no rgb_column."""
    config = _config(tmp_path, _RGB_HEADER, _RGB_ROW)

    assert config["format"]["columns"]["rgb"] == []
    assert config["format"]["columns"]["string"] == [3, 6]


# --- the measured time range --------------------------------------------------


def test_time_range_is_measured_over_every_row(tmp_path: Path) -> None:
    """The extent has to come from every row, not the sampled prefix.

    The renderer interpolates the playhead across this range, so a maximum short
    of the real one clips the tail of the cloud. The smallest value here is on
    the second data row and the largest on the last, so a prefix-only scan or a
    first-row guess would both get it wrong.
    """
    described = _describe(tmp_path, _TIMED_CSV, timestamp_column="t_s")

    assert described.time_range_us == (250_000, 9_750_000)


@pytest.mark.parametrize(
    ("unit", "start_us", "end_us"),
    [
        ("seconds", 250_000, 9_750_000),
        ("milliseconds", 250, 9_750),
        ("microseconds", 0, 10),
        ("nanoseconds", 0, 1),
        ("minutes", 15_000_000, 585_000_000),
    ],
)
def test_time_range_is_converted_to_microseconds(tmp_path: Path, unit: str, start_us: int, end_us: int) -> None:
    """The spatial always stores microseconds, whatever unit the column is in."""
    described = _describe(tmp_path, _TIMED_CSV, timestamp_column="t_s", time_unit=unit)

    assert described.time_range_us == (start_us, end_us)


def test_time_range_widens_rather_than_rounding_inward(tmp_path: Path) -> None:
    """Sub-microsecond ends round outward so the range cannot exclude real points."""
    described = _describe(
        tmp_path, "x,y,z,t_us\n0,0,0,1.4\n1,1,1,8.6\n", timestamp_column="t_us", time_unit="microseconds"
    )

    assert described.time_range_us == (1, 9)


def test_time_range_ignores_rows_with_no_time_value(tmp_path: Path) -> None:
    """A blank time cell is a gap in the data, not a zero that widens the range."""
    described = _describe(tmp_path, "x,y,z,t_s\n0,0,0,2\n1,1,1,\n2,2,2,4\n", timestamp_column="t_s")

    assert described.time_range_us == (2_000_000, 4_000_000)


def test_no_time_column_measures_nothing(tmp_path: Path) -> None:
    """A cloud with no time dimension must not pay for the extra pass over the file."""
    described = _describe(tmp_path, _TIMED_CSV)

    assert described.time_range_us is None


# --- rejected input -----------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "kwargs", "message"),
    [
        ("a,b,c\n1,2,3\n", {}, "missing required point-cloud columns x/y/z"),
        ("   \n", {}, "CSV header is empty"),
        ("", {}, "CSV is empty"),
        ("x,y,z,count\n0,0,0,1\n", {"column_types": {"nope": "int"}}, "not in CSV header"),
        ("x,y,z,count\n0,0,0,1\n", {"column_types": {"count": "float"}}, "must be one of"),
        ("x,y,z,color\n0,0,0,c04422\n", {"rgb_column": "nope"}, "is not in the CSV header"),
        (_TIMED_CSV, {"timestamp_column": "nope"}, "is not in the CSV header"),
        ("x,y,z,t_s\n0,0,0,1\n1,1,1,later\n", {"timestamp_column": "t_s"}, "non-numeric value 'later' on line 3"),
        ("x,y,z,t_s\n0,0,0,\n", {"timestamp_column": "t_s"}, "no values to derive a time range"),
        (_TIMED_CSV, {"timestamp_column": "t_s", "time_unit": "fortnights"}, "time_unit must be one of"),
        # Quoting is refused rather than parsed: the importer splits rows on raw commas with no
        # quote handling, so honouring quotes here would compute column indices it never reads,
        # shifting every attribute after the quoted field.
        ('x,y,z,label\n0,0,0,"kerb,left"\n', {}, "CSV quoting is not supported"),
        ('"x","y","z"\n0,0,0\n', {}, "CSV quoting is not supported"),
    ],
)
def test_malformed_input_is_refused_with_a_specific_message(
    tmp_path: Path, text: str, kwargs: dict[str, Any], message: str
) -> None:
    """Every rejection names what is wrong, because all of them happen before the upload starts."""
    with pytest.raises(ValueError, match=message):
        _describe(tmp_path, text, **kwargs)


def test_a_missing_file_is_refused(tmp_path: Path) -> None:
    """A path that does not exist fails before anything else is attempted."""
    with pytest.raises(FileNotFoundError):
        _describe_point_cloud_csv(tmp_path / "nope.csv")


def test_quoting_is_rejected_anywhere_in_the_file(tmp_path: Path) -> None:
    """A quoted field past the sampled prefix has to be caught too.

    The importer splits on raw commas, so a single quoted field shifts every
    attribute after it. Checking only the sampled rows left that undetected for
    any file longer than the sample.
    """
    body = "\n".join("0,0,0,ok" for _ in range(_TYPE_INFERENCE_SAMPLE_ROWS + 500))

    with pytest.raises(ValueError, match="CSV quoting is not supported"):
        _describe(tmp_path, f'x,y,z,label\n{body}\n0,0,0,"kerb,left"\n')


def test_the_quoting_error_names_the_line_the_file_has(tmp_path: Path) -> None:
    """Blank lines must not drift the reported line number away from the real one."""
    with pytest.raises(ValueError, match="line 4"):
        _describe(tmp_path, 'x,y,z\n0,0,0\n\n1,1,"1"\n')
