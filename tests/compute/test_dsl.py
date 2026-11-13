from __future__ import annotations

import pytest

from nominal.experimental.compute.dsl import NumericExpr, RangeExpr
from nominal.protos.compute.v1 import expressions_pb2


@pytest.fixture
def ch1() -> NumericExpr:
    return NumericExpr.asset_channel("asset1", "scope1", "channel1")


@pytest.fixture
def ch2() -> NumericExpr:
    return NumericExpr.asset_channel("asset2", "scope2", "channel2")


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
    """N-ary operations include the receiver and all supplied operands in order."""
    for expression, operation in [
        (ch1.sum([ch2]), expressions_pb2.NARY_NUMERIC_OPERATION_SUM),
        (ch1.product([ch2]), expressions_pb2.NARY_NUMERIC_OPERATION_PRODUCT),
        (ch1.min([ch2]), expressions_pb2.NARY_NUMERIC_OPERATION_MIN),
        (ch1.max([ch2]), expressions_pb2.NARY_NUMERIC_OPERATION_MAX),
        (ch1.mean([ch2]), expressions_pb2.NARY_NUMERIC_OPERATION_MEAN),
    ]:
        proto = expression._to_proto()
        assert proto.nary.operation == operation
        assert list(proto.nary.inputs) == [ch1._to_proto(), ch2._to_proto()]


def test_range_nary_operations(range1: RangeExpr, range2: RangeExpr):
    """Range combinations retain the receiver and supplied ranges."""
    assert list(range1.union([range2])._to_proto().union.inputs) == [range1._to_proto(), range2._to_proto()]
    assert list(range1.intersect([range2])._to_proto().intersection.inputs) == [range1._to_proto(), range2._to_proto()]


def test_numeric_serialization(ch1: NumericExpr, ch2: NumericExpr, range1: RangeExpr):
    """All numeric operations construct a typed expression graph."""
    assert isinstance(ch1.abs()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.acos()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.asin()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.atan2(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.cos()._to_proto(), expressions_pb2.NumericExpression)

    assert isinstance(ch1.cumulative_sum(100000000)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.derivative("s")._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.integral(100000000, "s")._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.ln()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.log()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.offset(5.0)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.rolling(1000000000, "mean")._to_proto(), expressions_pb2.NumericExpression)  # 1 second window
    assert isinstance(ch1.scale(2.0)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.sin()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.sqrt()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.tan()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.time_difference()._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.time_difference("s")._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.value_difference()._to_proto(), expressions_pb2.NumericExpression)

    # Test binary operations
    assert isinstance(ch1.plus(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.minus(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.multiply(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.divide(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.power(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.modulo(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.floor_divide(ch2)._to_proto(), expressions_pb2.NumericExpression)
    assert isinstance(ch1.filter(range1)._to_proto(), expressions_pb2.NumericExpression)

    # Test operations that return RangeExpr
    assert isinstance(ch1.threshold(0, "==")._to_proto(), expressions_pb2.RangeExpression)


def test_range_serialization(range1: RangeExpr, range2: RangeExpr):
    """Range operations construct typed graphs."""
    assert isinstance(range1.invert()._to_proto(), expressions_pb2.RangeExpression)
    assert isinstance(range1.union([range2])._to_proto(), expressions_pb2.RangeExpression)
    assert isinstance(range1.intersect([range2])._to_proto(), expressions_pb2.RangeExpression)
