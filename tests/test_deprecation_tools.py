import inspect

import pytest

from nominal._utils.deprecation_tools import warn_on_deprecated_argument


@warn_on_deprecated_argument("old_arg", "old_arg is deprecated")
def _single_deprecated(*, old_arg: str | None = None) -> None:
    return None


@warn_on_deprecated_argument("outer_arg", "outer_arg is deprecated")
@warn_on_deprecated_argument("inner_arg", "inner_arg is deprecated")
def _stacked_deprecated(
    *,
    outer_arg: str | None = None,
    inner_arg: str | None = None,
) -> None:
    return None


def test_warn_on_deprecated_argument_points_to_callsite_for_single_wrapper():
    with pytest.warns(UserWarning, match="old_arg is deprecated") as record:
        expected_lineno = inspect.currentframe().f_lineno + 1
        _single_deprecated(old_arg="value")

    warning = record[0]
    assert warning.filename == __file__
    assert warning.lineno == expected_lineno


def test_warn_on_deprecated_argument_points_to_callsite_for_inner_stacked_wrapper():
    with pytest.warns(UserWarning, match="inner_arg is deprecated") as record:
        expected_lineno = inspect.currentframe().f_lineno + 1
        _stacked_deprecated(inner_arg="value")

    warning = record[0]
    assert warning.filename == __file__
    assert warning.lineno == expected_lineno


def test_warn_on_deprecated_argument_points_to_callsite_for_both_stacked_wrappers():
    with pytest.warns(UserWarning) as record:
        expected_lineno = inspect.currentframe().f_lineno + 1
        _stacked_deprecated(outer_arg="value", inner_arg="value")

    assert len(record) == 2
    assert all(warning.filename == __file__ for warning in record)
    assert all(warning.lineno == expected_lineno for warning in record)
