from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.point_cloud import (
    _build_import_config,
    _classify_column,
    _find_rgb_index,
    _ingest_point_cloud_csv,
    _read_csv_header_and_samples,
    _read_time_range,
)


def _write_csv(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "cloud.csv"
    path.write_text(text)
    return path


# --- import config (the Dagger v2 wire shape) ---------------------------------


def test_build_import_config_emits_v2_shape_with_format_block() -> None:
    """geometry_type and columns live under `format`; the rejected v1 shape put them at top level."""
    config = _build_import_config("x,y,z", [])
    assert set(config) == {"archetype", "format"}
    assert "geometry_type" not in config
    assert "columns" not in config
    assert config["format"]["kind"] == "csv"
    # Dagger's GeometryType enum is PascalCase, not conjure's SCREAMING_CASE.
    assert config["format"]["geometry_type"] == "Point"


def test_build_import_config_selects_xyz_as_geometry_case_insensitively() -> None:
    config = _build_import_config("A,X,Y,Z", ["1,0,0,0"])
    assert config["format"]["columns"]["geometry"] == [1, 2, 3]


def test_build_import_config_names_every_column_bucket() -> None:
    """Dagger requires all seven column buckets to be present, even when empty."""
    columns = _build_import_config("x,y,z", [])["format"]["columns"]
    assert set(columns) == {"geometry", "real", "int", "string", "rgb", "normal", "bool"}


def test_build_import_config_classifies_and_indexes_attributes() -> None:
    config = _build_import_config("x,y,z,count,stress,tag", ["0,0,0,3,0.5,ok"])
    columns = config["format"]["columns"]
    assert columns["int"] == [3]
    assert columns["real"] == [4]
    assert columns["string"] == [5]

    by_name = {a["header"]["name"]: a for a in config["archetype"]["attributes"]}
    assert by_name["count"]["header"]["ty"] == "Int"
    assert by_name["stress"]["header"]["ty"] == {"Real": "IndependentValue"}
    assert by_name["tag"]["header"]["ty"] == "String"


def test_build_import_config_excludes_geometry_from_attributes() -> None:
    config = _build_import_config("x,y,z,count", ["0,0,0,1"])
    assert [a["header"]["name"] for a in config["archetype"]["attributes"]] == ["count"]


def test_int_attributes_get_min_max_but_not_mean() -> None:
    """Mean is not a valid sampler for an Int-typed attribute."""
    config = _build_import_config("x,y,z,count", ["0,0,0,1"])
    assert config["archetype"]["attributes"][0]["reductions"] == ["Min", "Max"]


def test_real_attributes_get_min_max_mean() -> None:
    config = _build_import_config("x,y,z,stress", ["0,0,0,0.5"])
    assert config["archetype"]["attributes"][0]["reductions"] == ["Min", "Max", "Mean"]


def test_string_attributes_get_no_reductions() -> None:
    config = _build_import_config("x,y,z,tag", ["0,0,0,ok"])
    assert config["archetype"]["attributes"][0]["reductions"] == []


def test_column_types_override_wins_over_inference() -> None:
    config = _build_import_config("x,y,z,count", ["0,0,0,1"], {"count": "real"})
    assert config["format"]["columns"]["real"] == [3]
    assert config["format"]["columns"]["int"] == []


def test_column_types_override_ignores_geometry_columns() -> None:
    config = _build_import_config("x,y,z,count", ["0,0,0,1"], {"x": "real"})
    assert config["format"]["columns"]["geometry"] == [0, 1, 2]
    assert [a["header"]["name"] for a in config["archetype"]["attributes"]] == ["count"]


def test_missing_geometry_columns_raise() -> None:
    with pytest.raises(ValueError, match="missing required point-cloud columns x/y/z"):
        _build_import_config("a,b,c", ["1,2,3"])


def test_empty_header_raises() -> None:
    with pytest.raises(ValueError, match="CSV header is empty"):
        _build_import_config("   ", [])


def test_unknown_override_column_raises() -> None:
    with pytest.raises(ValueError, match="not in CSV header"):
        _build_import_config("x,y,z,count", ["0,0,0,1"], {"nope": "int"})


def test_invalid_override_type_raises() -> None:
    with pytest.raises(ValueError, match="must be one of"):
        _build_import_config("x,y,z,count", ["0,0,0,1"], {"count": "float"})  # type: ignore[dict-item]


# --- column classification ----------------------------------------------------


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (["1", "2", "3"], "int"),
        (["1", "0.998"], "real"),  # a later float promotes an int-looking first row
        (["1", "abc"], "string"),
        (["", ""], "string"),  # all-empty defaults to string
        (["1", "", "2"], "int"),  # blanks are skipped, not treated as strings
        (["-1", "+2"], "int"),
        (["1e3"], "real"),
    ],
)
def test_classify_column(values: list[str], expected: str) -> None:
    assert _classify_column(values) == expected


def test_type_inference_samples_beyond_the_first_row(tmp_path: Path) -> None:
    """A float appearing only in a later row still promotes the column to real."""
    rows = "\n".join("0,0,0,1" for _ in range(50)) + "\n0,0,0,0.5"
    path = _write_csv(tmp_path, f"x,y,z,stress\n{rows}\n")
    header, samples = _read_csv_header_and_samples(path)
    config = _build_import_config(header, samples)
    assert config["format"]["columns"]["real"] == [3]


# --- csv reading --------------------------------------------------------------


def test_read_csv_skips_blank_lines_and_caps_samples(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "x,y,z\n1,1,1\n\n2,2,2\n3,3,3\n")
    header, samples = _read_csv_header_and_samples(path, n_samples=2)
    assert header == "x,y,z"
    assert samples == ["1,1,1", "2,2,2"]


def test_read_empty_csv_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="CSV is empty"):
        _read_csv_header_and_samples(_write_csv(tmp_path, ""))


# --- ingest submission --------------------------------------------------------


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    clients.resolve_workspace.return_value.rid = "ri.scout.x.workspace.w"
    clients.ingest.ingest.return_value.ingest_job_rid = "ri.scout.x.ingest-job.j"
    return clients


def test_ingest_submits_point_cloud_opts_against_the_existing_asset(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "x,y,z,count\n0,0,0,1\n")
    clients = _clients()

    with patch("nominal.core.point_cloud.upload_multipart_file", return_value="s3://b/cloud.csv") as upload:
        s3_path, job_rid, _ = _ingest_point_cloud_csv(
            clients, "ri.scout.x.spatial.abc", path, channel="pc", tags={"run": "1"}
        )

    assert (s3_path, job_rid) == ("s3://b/cloud.csv", "ri.scout.x.ingest-job.j")
    assert upload.call_args.args[1] == "ri.scout.x.workspace.w"

    opts = clients.ingest.ingest.call_args.args[1].options.point_cloud
    assert opts.source.s3.path == "s3://b/cloud.csv"
    # scout rejects PointCloudIngestTarget.new -- the asset must already exist.
    assert opts.target.existing.spatial_rid == "ri.scout.x.spatial.abc"
    assert opts.target.new is None
    assert opts.channel == "pc"
    assert opts.tags == {"run": "1"}
    assert opts.dagger_import_config["format"]["geometry_type"] == "Point"
    # source_uri is scout's to fill in from the presigned URL.
    assert "source_uri" not in opts.dagger_import_config


def test_ingest_validates_csv_before_uploading(tmp_path: Path) -> None:
    """A malformed CSV must fail before bytes are pushed to object storage."""
    path = _write_csv(tmp_path, "a,b,c\n1,2,3\n")
    clients = _clients()

    with patch("nominal.core.point_cloud.upload_multipart_file") as upload:
        with pytest.raises(ValueError, match="missing required point-cloud columns"):
            _ingest_point_cloud_csv(clients, "ri.scout.x.spatial.abc", path)

    upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()


def test_ingest_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _ingest_point_cloud_csv(_clients(), "ri.scout.x.spatial.abc", tmp_path / "nope.csv")


# --- time range ---------------------------------------------------------------

_TIMED_CSV = "x,y,z,t_s\n0,0,0,1.5\n1,1,1,0.25\n2,2,2,9.75\n"


def test_read_time_range_measures_the_whole_column(tmp_path: Path) -> None:
    """The extent has to come from every row, not the sampled prefix.

    The renderer interpolates the playhead across this range, so a maximum short
    of the real one clips the tail of the cloud. The smallest value here is on
    the second data row and the largest on the last, so a prefix-only scan or a
    first-row guess would both get it wrong.
    """
    path = _write_csv(tmp_path, _TIMED_CSV)

    assert _read_time_range(path, "x,y,z,t_s", "t_s", "s") == (250_000, 9_750_000)


@pytest.mark.parametrize(
    ("unit", "start_us", "end_us"),
    [("s", 250_000, 9_750_000), ("ms", 250, 9_750), ("us", 0, 10), ("ns", 0, 1)],
)
def test_read_time_range_converts_to_microseconds(tmp_path: Path, unit: str, start_us: int, end_us: int) -> None:
    """The asset always stores microseconds, whatever unit the column is in."""
    path = _write_csv(tmp_path, _TIMED_CSV)

    assert _read_time_range(path, "x,y,z,t_s", "t_s", unit) == (start_us, end_us)  # type: ignore[arg-type]


def test_read_time_range_widens_rather_than_rounds_inward(tmp_path: Path) -> None:
    """Sub-microsecond ends round outward so the range cannot exclude real points."""
    path = _write_csv(tmp_path, "x,y,z,t_us\n0,0,0,1.4\n1,1,1,8.6\n")

    assert _read_time_range(path, "x,y,z,t_us", "t_us", "us") == (1, 9)


def test_read_time_range_skips_blank_values(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "x,y,z,t_s\n0,0,0,2\n1,1,1,\n2,2,2,4\n")

    assert _read_time_range(path, "x,y,z,t_s", "t_s", "s") == (2_000_000, 4_000_000)


def test_read_time_range_rejects_unknown_column(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, _TIMED_CSV)

    with pytest.raises(ValueError, match="is not in the CSV header"):
        _read_time_range(path, "x,y,z,t_s", "nope", "s")


def test_read_time_range_rejects_non_numeric_values(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "x,y,z,t_s\n0,0,0,1\n1,1,1,later\n")

    with pytest.raises(ValueError, match="non-numeric value 'later' on line 3"):
        _read_time_range(path, "x,y,z,t_s", "t_s", "s")


def test_read_time_range_rejects_a_column_with_no_values(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "x,y,z,t_s\n0,0,0,\n")

    with pytest.raises(ValueError, match="no values to derive a time range"):
        _read_time_range(path, "x,y,z,t_s", "t_s", "s")


def test_read_time_range_rejects_unknown_unit(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, _TIMED_CSV)

    with pytest.raises(ValueError, match="time_unit must be one of"):
        _read_time_range(path, "x,y,z,t_s", "t_s", "minutes")  # type: ignore[arg-type]


def test_ingest_returns_the_measured_range_rather_than_sending_it(tmp_path: Path) -> None:
    """The ingest API has no field for the range, so the caller gets it back.

    It is recorded on the spatial asset instead, which is where a workbook reads
    it from.
    """
    path = _write_csv(tmp_path, _TIMED_CSV)
    clients = _clients()

    with patch("nominal.core.point_cloud.upload_multipart_file", return_value="s3://b/k.csv"):
        _, _, time_range = _ingest_point_cloud_csv(clients, "ri.scout.x.spatial.abc", path, time_column="t_s")

    assert time_range == (250_000, 9_750_000)
    assert not hasattr(clients.ingest.ingest.call_args.args[1].options.point_cloud, "time_range")


def test_ingest_measures_nothing_for_a_static_cloud(tmp_path: Path) -> None:
    """A cloud with no time dimension must not pay for the extra pass over the file."""
    path = _write_csv(tmp_path, _TIMED_CSV)
    clients = _clients()

    with patch("nominal.core.point_cloud.upload_multipart_file", return_value="s3://b/k.csv"):
        _, _, time_range = _ingest_point_cloud_csv(clients, "ri.scout.x.spatial.abc", path)

    assert time_range is None


def test_ingest_validates_time_column_before_uploading(tmp_path: Path) -> None:
    """A bad time column fails fast, like the geometry check."""
    path = _write_csv(tmp_path, _TIMED_CSV)
    clients = _clients()

    with patch("nominal.core.point_cloud.upload_multipart_file") as upload:
        with pytest.raises(ValueError, match="is not in the CSV header"):
            _ingest_point_cloud_csv(clients, "ri.scout.x.spatial.abc", path, time_column="missing")

    upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()


# --- rgb attributes -----------------------------------------------------------

_RGB_HEADER = "x,y,z,color,intensity,ring,label"
_RGB_ROW = ["0,0,0,c04422,0.5,3,kerb"]


def test_rgb_column_becomes_an_rgb_attribute() -> None:
    """One hex column, one Rgb attribute.

    Quiche reads the cell with `u8::from_str_radix` over three 2-character
    slices and skips it unless it is exactly six characters, so three separate
    0-255 columns silently produce black rather than an error.
    """
    config = _build_import_config(_RGB_HEADER, _RGB_ROW, rgb_column="color")

    assert config["format"]["columns"]["rgb"] == [3]
    colour = config["archetype"]["attributes"][-1]
    assert colour["header"] == {"name": "color", "ty": "Rgb"}
    # Without a reduction the renderer reports the attribute as not colourable.
    assert colour["reductions"] == ["Mean"]


def test_archetype_is_ordered_by_bucket_not_by_header() -> None:
    """Attribute k must line up with the k-th slot quiche assigns.

    Quiche walks the buckets in the order real, int, string, rgb, normal, bool
    when handing columns their attribute slot. Declaring the archetype in header
    order instead means an attribute is named and typed after one column while
    holding another column's values.
    """
    header = "x,y,z,speed,ring,label,heading,gear"
    config = _build_import_config(header, ["0,0,0,1.5,3,kerb,88.5,4"])

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


def test_rgb_attribute_sorts_after_the_scalar_buckets() -> None:
    config = _build_import_config(_RGB_HEADER, _RGB_ROW, rgb_column="color")
    assert [a["header"]["name"] for a in config["archetype"]["attributes"]] == [
        "intensity",
        "ring",
        "label",
        "color",
    ]


def test_rgb_column_is_not_also_classified_as_a_string() -> None:
    config = _build_import_config(_RGB_HEADER, _RGB_ROW, rgb_column="color")
    assert config["format"]["columns"]["string"] == [6]
    assert "color" not in [a["header"]["name"] for a in config["archetype"]["attributes"][:-1]]


def test_rgb_attribute_name_is_overridable() -> None:
    config = _build_import_config(_RGB_HEADER, _RGB_ROW, rgb_column="color", rgb_attribute="paint")
    assert config["archetype"]["attributes"][-1]["header"]["name"] == "paint"


def test_without_rgb_column_the_hex_column_stays_a_string() -> None:
    """Regression: the default must not change for callers that pass no colour."""
    config = _build_import_config(_RGB_HEADER, _RGB_ROW)
    assert config["format"]["columns"]["rgb"] == []
    assert "color" in [a["header"]["name"] for a in config["archetype"]["attributes"]]
    assert config["format"]["columns"]["string"] == [3, 6]


def test_rgb_column_must_exist() -> None:
    with pytest.raises(ValueError, match="is not in the CSV header"):
        _build_import_config(_RGB_HEADER, _RGB_ROW, rgb_column="nope")


def test_find_rgb_index_defaults_to_none() -> None:
    assert _find_rgb_index(["x", "y", "z"], None) == []


def test_ingest_forwards_rgb_column(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, _RGB_HEADER + "\n" + _RGB_ROW[0] + "\n")
    clients = _clients()

    with patch("nominal.core.point_cloud.upload_multipart_file", return_value="s3://b/k.csv"):
        _ingest_point_cloud_csv(clients, "ri.scout.x.spatial.abc", path, rgb_column="color")

    config = clients.ingest.ingest.call_args.args[1].options.point_cloud.dagger_import_config
    assert config["format"]["columns"]["rgb"] == [3]
    assert config["archetype"]["attributes"][-1]["header"]["ty"] == "Rgb"


# --- csv quoting --------------------------------------------------------------


def test_quoted_fields_are_rejected_rather_than_reparsed(tmp_path: Path) -> None:
    """Quoting is refused up front instead of being parsed.

    The importer has no quote handling at all -- quiche splits rows on raw commas
    and counts columns with memchr -- so honouring quotes here would compute column
    indices the importer never uses, shifting every attribute after the quoted
    field. Failing loudly is the only option that cannot corrupt the result.
    """
    path = _write_csv(tmp_path, 'x,y,z,label\n0,0,0,"kerb,left"\n')

    with pytest.raises(ValueError, match="CSV quoting is not supported"):
        _read_csv_header_and_samples(path)


def test_quoted_header_is_rejected(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, '"x","y","z"\n0,0,0\n')

    with pytest.raises(ValueError, match="the header"):
        _read_csv_header_and_samples(path)


def test_ingest_rejects_quoted_csv_before_uploading(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, 'x,y,z,label\n0,0,0,"a,b"\n')
    clients = _clients()

    with patch("nominal.core.point_cloud.upload_multipart_file") as upload:
        with pytest.raises(ValueError, match="CSV quoting is not supported"):
            _ingest_point_cloud_csv(clients, "ri.scout.x.spatial.abc", path)

    upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()
