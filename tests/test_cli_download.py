import pathlib

from nominal.cli.download import _sanitize_dataset_prefix


def test_sanitize_dataset_prefix_removes_absolute_path() -> None:
    rid = "ri.dataset./tmp/pwned"

    assert _sanitize_dataset_prefix(rid) == "pwned"


def test_sanitize_dataset_prefix_handles_empty_suffix() -> None:
    rid = "ri.dataset."

    assert _sanitize_dataset_prefix(rid) == "dataset"


def test_sanitize_dataset_prefix_normalizes_parent_segments() -> None:
    rid = "ri.dataset.../../escape"

    prefix = _sanitize_dataset_prefix(rid)
    out_path = pathlib.Path("/safe/out") / f"{prefix}-part_0.csv"

    assert prefix == "escape"
    assert out_path == pathlib.Path("/safe/out/escape-part_0.csv")
