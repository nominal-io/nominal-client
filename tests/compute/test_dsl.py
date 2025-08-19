import pytest
from nominal_api import scout_compute_api

from nominal.experimental.compute.dsl import NumericExpr, RangeExpr


@pytest.fixture
def ch1() -> NumericExpr:
    return NumericExpr.channel("asset1", "scope1", "channel1")


@pytest.fixture
def ch2() -> NumericExpr:
    return NumericExpr.channel("asset2", "scope2", "channel2")


@pytest.fixture
def range1(ch1: NumericExpr) -> RangeExpr:
    return ch1.threshold(0, "==")


@pytest.fixture
def range2(ch2: NumericExpr) -> RangeExpr:
    return ch2.threshold(1, "==")


def test_operator_overloads(ch1: NumericExpr, ch2: NumericExpr):
    assert ch1 + ch2 == ch1.plus(ch2)
    assert ch1 - ch2 == ch1.minus(ch2)
    assert ch1 * ch2 == ch1.multiply(ch2)
    assert ch1 / ch2 == ch1.divide(ch2)
    assert ch1**ch2 == ch1.power(ch2)
    assert ch1 % ch2 == ch1.modulo(ch2)
    assert ch1 // ch2 == ch1.floor_divide(ch2)
    assert abs(ch1) == ch1.abs()


def test_numeric_nary_operations(ch1: NumericExpr, ch2: NumericExpr):
    conjure = ch1.sum([ch2])._to_conjure()
    assert conjure.type == "sum"
    assert conjure.sum is not None
    assert len(conjure.sum.inputs) == 2

    conjure = ch1.product([ch2])._to_conjure()
    assert conjure.type == "product"
    assert conjure.product is not None
    assert len(conjure.product.inputs) == 2

    conjure = ch1.min([ch2])._to_conjure()
    assert conjure.type == "min"
    assert conjure.min is not None
    assert len(conjure.min.inputs) == 2

    conjure = ch1.max([ch2])._to_conjure()
    assert conjure.type == "max"
    assert conjure.max is not None
    assert len(conjure.max.inputs) == 2

    conjure = ch1.mean([ch2])._to_conjure()
    assert conjure.type == "mean"
    assert conjure.mean is not None
    assert len(conjure.mean.inputs) == 2


def test_range_nary_operations(range1: RangeExpr, range2: RangeExpr):
    conjure = range1.union([range2])._to_conjure()
    assert conjure.type == "unionRange"
    assert conjure.union_range is not None
    assert len(conjure.union_range.inputs) == 2

    conjure = range1.intersect([range2])._to_conjure()
    assert conjure.type == "intersectRange"
    assert conjure.intersect_range is not None
    assert len(conjure.intersect_range.inputs) == 2


def test_numeric_serialization(ch1: NumericExpr, ch2: NumericExpr, range1: RangeExpr):
    # Test all numeric channel methods serialize successfully
    assert isinstance(ch1.abs()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.acos()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.asin()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.atan2(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.cos()._to_conjure(), scout_compute_api.NumericSeries)

    assert isinstance(ch1.cumulative_sum(100000000)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.derivative("s")._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.integral(100000000, "s")._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.ln()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.log()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.offset(5.0)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.rolling(1000000000, "mean")._to_conjure(), scout_compute_api.NumericSeries)  # 1 second window
    assert isinstance(ch1.scale(2.0)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.sin()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.sqrt()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.tan()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.time_difference()._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.time_difference("s")._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.value_difference()._to_conjure(), scout_compute_api.NumericSeries)

    # Test binary operations
    assert isinstance(ch1.plus(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.minus(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.multiply(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.divide(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.power(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.modulo(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.floor_divide(ch2)._to_conjure(), scout_compute_api.NumericSeries)
    assert isinstance(ch1.filter(range1)._to_conjure(), scout_compute_api.NumericSeries)

    # Test operations that return RangeExpr
    assert isinstance(ch1.threshold(0, "==")._to_conjure(), scout_compute_api.RangeSeries)


def test_range_serialization(range1: RangeExpr, range2: RangeExpr):
    assert isinstance(range1.invert()._to_conjure(), scout_compute_api.RangeSeries)
    assert isinstance(range1.union([range2])._to_conjure(), scout_compute_api.RangeSeries)
    assert isinstance(range1.intersect([range2])._to_conjure(), scout_compute_api.RangeSeries)
