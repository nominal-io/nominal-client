from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.point_cloud import (
    _build_import_config,
    _classify_column,
    _ingest_point_cloud_csv,
    _read_csv_header_and_samples,
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
        s3_path, job_rid = _ingest_point_cloud_csv(
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
