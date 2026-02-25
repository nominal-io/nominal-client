from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import Any as _Any, ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Timestamp(_message.Message):
    __slots__ = ("seconds", "nanos", "picos")
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    NANOS_FIELD_NUMBER: _ClassVar[int]
    PICOS_FIELD_NUMBER: _ClassVar[int]
    seconds: int
    nanos: int
    picos: int
    def __init__(self, seconds: _Optional[int] = ..., nanos: _Optional[int] = ..., picos: _Optional[int] = ...) -> None: ...

class WriteBatchesRequest(_message.Message):
    __slots__ = ("batches", "data_source_rid")
    BATCHES_FIELD_NUMBER: _ClassVar[int]
    DATA_SOURCE_RID_FIELD_NUMBER: _ClassVar[int]
    batches: _containers.RepeatedCompositeFieldContainer[RecordsBatch]
    data_source_rid: str
    def __init__(self, batches: _Optional[_Iterable[_Union[RecordsBatch, _Mapping[str, _Any]]]] = ..., data_source_rid: _Optional[str] = ...) -> None: ...

class RecordsBatch(_message.Message):
    __slots__ = ("channel", "tags", "points")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    channel: str
    tags: _containers.ScalarMap[str, str]
    points: Points
    def __init__(self, channel: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., points: _Optional[_Union[Points, _Mapping[str, _Any]]] = ...) -> None: ...

class Points(_message.Message):
    __slots__ = ("timestamps", "double_points", "string_points", "int_points", "uint64_points")
    TIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_POINTS_FIELD_NUMBER: _ClassVar[int]
    STRING_POINTS_FIELD_NUMBER: _ClassVar[int]
    INT_POINTS_FIELD_NUMBER: _ClassVar[int]
    UINT64_POINTS_FIELD_NUMBER: _ClassVar[int]
    timestamps: _containers.RepeatedCompositeFieldContainer[Timestamp]
    double_points: DoublePoints
    string_points: StringPoints
    int_points: IntPoints
    uint64_points: Uint64Points
    def __init__(self, timestamps: _Optional[_Iterable[_Union[Timestamp, _Mapping[str, _Any]]]] = ..., double_points: _Optional[_Union[DoublePoints, _Mapping[str, _Any]]] = ..., string_points: _Optional[_Union[StringPoints, _Mapping[str, _Any]]] = ..., int_points: _Optional[_Union[IntPoints, _Mapping[str, _Any]]] = ..., uint64_points: _Optional[_Union[Uint64Points, _Mapping[str, _Any]]] = ...) -> None: ...

class DoublePoints(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, points: _Optional[_Iterable[float]] = ...) -> None: ...

class StringPoints(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, points: _Optional[_Iterable[str]] = ...) -> None: ...

class IntPoints(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, points: _Optional[_Iterable[int]] = ...) -> None: ...

class Uint64Points(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, points: _Optional[_Iterable[int]] = ...) -> None: ...
