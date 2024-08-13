# coding=utf-8
from abc import (
    abstractmethod,
)
import builtins
from conjure_python_client import (
    BinaryType,
    ConjureBeanType,
    ConjureDecoder,
    ConjureEncoder,
    ConjureEnumType,
    ConjureFieldDefinition,
    ConjureUnionType,
    OptionalTypeWrapper,
    Service,
)
from requests.adapters import (
    Response,
)
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)

class ingest_api_AbsoluteTimestamp(ConjureUnionType):
    _iso8601: Optional["ingest_api_Iso8601Timestamp"] = None
    _epoch_of_time_unit: Optional["ingest_api_EpochTimestamp"] = None
    _custom_format: Optional["ingest_api_CustomTimestamp"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'iso8601': ConjureFieldDefinition('iso8601', ingest_api_Iso8601Timestamp),
            'epoch_of_time_unit': ConjureFieldDefinition('epochOfTimeUnit', ingest_api_EpochTimestamp),
            'custom_format': ConjureFieldDefinition('customFormat', ingest_api_CustomTimestamp)
        }

    def __init__(
            self,
            iso8601: Optional["ingest_api_Iso8601Timestamp"] = None,
            epoch_of_time_unit: Optional["ingest_api_EpochTimestamp"] = None,
            custom_format: Optional["ingest_api_CustomTimestamp"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (iso8601 is not None) + (epoch_of_time_unit is not None) + (custom_format is not None) != 1:
                raise ValueError('a union must contain a single member')

            if iso8601 is not None:
                self._iso8601 = iso8601
                self._type = 'iso8601'
            if epoch_of_time_unit is not None:
                self._epoch_of_time_unit = epoch_of_time_unit
                self._type = 'epochOfTimeUnit'
            if custom_format is not None:
                self._custom_format = custom_format
                self._type = 'customFormat'

        elif type_of_union == 'iso8601':
            if iso8601 is None:
                raise ValueError('a union value must not be None')
            self._iso8601 = iso8601
            self._type = 'iso8601'
        elif type_of_union == 'epochOfTimeUnit':
            if epoch_of_time_unit is None:
                raise ValueError('a union value must not be None')
            self._epoch_of_time_unit = epoch_of_time_unit
            self._type = 'epochOfTimeUnit'
        elif type_of_union == 'customFormat':
            if custom_format is None:
                raise ValueError('a union value must not be None')
            self._custom_format = custom_format
            self._type = 'customFormat'

    @builtins.property
    def iso8601(self) -> Optional["ingest_api_Iso8601Timestamp"]:
        return self._iso8601

    @builtins.property
    def epoch_of_time_unit(self) -> Optional["ingest_api_EpochTimestamp"]:
        return self._epoch_of_time_unit

    @builtins.property
    def custom_format(self) -> Optional["ingest_api_CustomTimestamp"]:
        return self._custom_format

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_AbsoluteTimestampVisitor):
            raise ValueError('{} is not an instance of ingest_api_AbsoluteTimestampVisitor'.format(visitor.__class__.__name__))
        if self._type == 'iso8601' and self.iso8601 is not None:
            return visitor._iso8601(self.iso8601)
        if self._type == 'epochOfTimeUnit' and self.epoch_of_time_unit is not None:
            return visitor._epoch_of_time_unit(self.epoch_of_time_unit)
        if self._type == 'customFormat' and self.custom_format is not None:
            return visitor._custom_format(self.custom_format)


ingest_api_AbsoluteTimestamp.__name__ = "AbsoluteTimestamp"
ingest_api_AbsoluteTimestamp.__qualname__ = "AbsoluteTimestamp"
ingest_api_AbsoluteTimestamp.__module__ = "ingest_service_api.ingest_api"


class ingest_api_AbsoluteTimestampVisitor:

    @abstractmethod
    def _iso8601(self, iso8601: "ingest_api_Iso8601Timestamp") -> Any:
        pass

    @abstractmethod
    def _epoch_of_time_unit(self, epoch_of_time_unit: "ingest_api_EpochTimestamp") -> Any:
        pass

    @abstractmethod
    def _custom_format(self, custom_format: "ingest_api_CustomTimestamp") -> Any:
        pass


ingest_api_AbsoluteTimestampVisitor.__name__ = "AbsoluteTimestampVisitor"
ingest_api_AbsoluteTimestampVisitor.__qualname__ = "AbsoluteTimestampVisitor"
ingest_api_AbsoluteTimestampVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_AsyncHandle(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'rid': ConjureFieldDefinition('rid', str)
        }

    __slots__: List[str] = ['_rid']

    def __init__(self, rid: str) -> None:
        self._rid = rid

    @builtins.property
    def rid(self) -> str:
        return self._rid


ingest_api_AsyncHandle.__name__ = "AsyncHandle"
ingest_api_AsyncHandle.__qualname__ = "AsyncHandle"
ingest_api_AsyncHandle.__module__ = "ingest_service_api.ingest_api"


class ingest_api_ChannelConfig(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'prefix_tree_delimiter': ConjureFieldDefinition('prefixTreeDelimiter', OptionalTypeWrapper[str])
        }

    __slots__: List[str] = ['_prefix_tree_delimiter']

    def __init__(self, prefix_tree_delimiter: Optional[str] = None) -> None:
        self._prefix_tree_delimiter = prefix_tree_delimiter

    @builtins.property
    def prefix_tree_delimiter(self) -> Optional[str]:
        """
        If set, will construct a prefix tree for channels of the dataset using the given delimiter.
        """
        return self._prefix_tree_delimiter


ingest_api_ChannelConfig.__name__ = "ChannelConfig"
ingest_api_ChannelConfig.__qualname__ = "ChannelConfig"
ingest_api_ChannelConfig.__module__ = "ingest_service_api.ingest_api"


class ingest_api_CompleteMultipartUploadResponse(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'location': ConjureFieldDefinition('location', OptionalTypeWrapper[str])
        }

    __slots__: List[str] = ['_location']

    def __init__(self, location: Optional[str] = None) -> None:
        self._location = location

    @builtins.property
    def location(self) -> Optional[str]:
        return self._location


ingest_api_CompleteMultipartUploadResponse.__name__ = "CompleteMultipartUploadResponse"
ingest_api_CompleteMultipartUploadResponse.__qualname__ = "CompleteMultipartUploadResponse"
ingest_api_CompleteMultipartUploadResponse.__module__ = "ingest_service_api.ingest_api"


class ingest_api_CsvProperties(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'skip_rows_config': ConjureFieldDefinition('skipRowsConfig', OptionalTypeWrapper[ingest_api_SkipRowsConfig])
        }

    __slots__: List[str] = ['_skip_rows_config']

    def __init__(self, skip_rows_config: Optional["ingest_api_SkipRowsConfig"] = None) -> None:
        self._skip_rows_config = skip_rows_config

    @builtins.property
    def skip_rows_config(self) -> Optional["ingest_api_SkipRowsConfig"]:
        return self._skip_rows_config


ingest_api_CsvProperties.__name__ = "CsvProperties"
ingest_api_CsvProperties.__qualname__ = "CsvProperties"
ingest_api_CsvProperties.__module__ = "ingest_service_api.ingest_api"


class ingest_api_CustomTimestamp(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'format': ConjureFieldDefinition('format', str),
            'default_year': ConjureFieldDefinition('defaultYear', OptionalTypeWrapper[int])
        }

    __slots__: List[str] = ['_format', '_default_year']

    def __init__(self, format: str, default_year: Optional[int] = None) -> None:
        self._format = format
        self._default_year = default_year

    @builtins.property
    def format(self) -> str:
        """
        The format string should be in the format of the `DateTimeFormatter` class in Java.
        """
        return self._format

    @builtins.property
    def default_year(self) -> Optional[int]:
        """
        Default year is accepted as an optional field for cases like IRIG time format and will be overridden by year in time format.
        """
        return self._default_year


ingest_api_CustomTimestamp.__name__ = "CustomTimestamp"
ingest_api_CustomTimestamp.__qualname__ = "CustomTimestamp"
ingest_api_CustomTimestamp.__module__ = "ingest_service_api.ingest_api"


class ingest_api_DatasetSpec(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'name': ConjureFieldDefinition('name', OptionalTypeWrapper[str])
        }

    __slots__: List[str] = ['_name']

    def __init__(self, name: Optional[str] = None) -> None:
        self._name = name

    @builtins.property
    def name(self) -> Optional[str]:
        return self._name


ingest_api_DatasetSpec.__name__ = "DatasetSpec"
ingest_api_DatasetSpec.__qualname__ = "DatasetSpec"
ingest_api_DatasetSpec.__module__ = "ingest_service_api.ingest_api"


class ingest_api_DeprecatedNewCsv(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'source': ConjureFieldDefinition('source', ingest_api_IngestSource),
            'name': ConjureFieldDefinition('name', OptionalTypeWrapper[str]),
            'properties': ConjureFieldDefinition('properties', Dict[str, str]),
            'time_column_spec': ConjureFieldDefinition('timeColumnSpec', OptionalTypeWrapper[ingest_api_TimestampMetadata]),
            'channel_config': ConjureFieldDefinition('channelConfig', OptionalTypeWrapper[ingest_api_ChannelConfig])
        }

    __slots__: List[str] = ['_source', '_name', '_properties', '_time_column_spec', '_channel_config']

    def __init__(self, properties: Dict[str, str], source: "ingest_api_IngestSource", channel_config: Optional["ingest_api_ChannelConfig"] = None, name: Optional[str] = None, time_column_spec: Optional["ingest_api_TimestampMetadata"] = None) -> None:
        self._source = source
        self._name = name
        self._properties = properties
        self._time_column_spec = time_column_spec
        self._channel_config = channel_config

    @builtins.property
    def source(self) -> "ingest_api_IngestSource":
        return self._source

    @builtins.property
    def name(self) -> Optional[str]:
        return self._name

    @builtins.property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @builtins.property
    def time_column_spec(self) -> Optional["ingest_api_TimestampMetadata"]:
        return self._time_column_spec

    @builtins.property
    def channel_config(self) -> Optional["ingest_api_ChannelConfig"]:
        return self._channel_config


ingest_api_DeprecatedNewCsv.__name__ = "DeprecatedNewCsv"
ingest_api_DeprecatedNewCsv.__qualname__ = "DeprecatedNewCsv"
ingest_api_DeprecatedNewCsv.__module__ = "ingest_service_api.ingest_api"


class ingest_api_DeprecatedNewDataSource(ConjureUnionType):
    _csv: Optional["ingest_api_DeprecatedNewCsv"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'csv': ConjureFieldDefinition('csv', ingest_api_DeprecatedNewCsv)
        }

    def __init__(
            self,
            csv: Optional["ingest_api_DeprecatedNewCsv"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (csv is not None) != 1:
                raise ValueError('a union must contain a single member')

            if csv is not None:
                self._csv = csv
                self._type = 'csv'

        elif type_of_union == 'csv':
            if csv is None:
                raise ValueError('a union value must not be None')
            self._csv = csv
            self._type = 'csv'

    @builtins.property
    def csv(self) -> Optional["ingest_api_DeprecatedNewCsv"]:
        return self._csv

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_DeprecatedNewDataSourceVisitor):
            raise ValueError('{} is not an instance of ingest_api_DeprecatedNewDataSourceVisitor'.format(visitor.__class__.__name__))
        if self._type == 'csv' and self.csv is not None:
            return visitor._csv(self.csv)


ingest_api_DeprecatedNewDataSource.__name__ = "DeprecatedNewDataSource"
ingest_api_DeprecatedNewDataSource.__qualname__ = "DeprecatedNewDataSource"
ingest_api_DeprecatedNewDataSource.__module__ = "ingest_service_api.ingest_api"


class ingest_api_DeprecatedNewDataSourceVisitor:

    @abstractmethod
    def _csv(self, csv: "ingest_api_DeprecatedNewCsv") -> Any:
        pass


ingest_api_DeprecatedNewDataSourceVisitor.__name__ = "DeprecatedNewDataSourceVisitor"
ingest_api_DeprecatedNewDataSourceVisitor.__qualname__ = "DeprecatedNewDataSourceVisitor"
ingest_api_DeprecatedNewDataSourceVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_DeprecatedTimestampMetadata(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'series_name': ConjureFieldDefinition('seriesName', str),
            'is_absolute': ConjureFieldDefinition('isAbsolute', bool)
        }

    __slots__: List[str] = ['_series_name', '_is_absolute']

    def __init__(self, is_absolute: bool, series_name: str) -> None:
        self._series_name = series_name
        self._is_absolute = is_absolute

    @builtins.property
    def series_name(self) -> str:
        return self._series_name

    @builtins.property
    def is_absolute(self) -> bool:
        return self._is_absolute


ingest_api_DeprecatedTimestampMetadata.__name__ = "DeprecatedTimestampMetadata"
ingest_api_DeprecatedTimestampMetadata.__qualname__ = "DeprecatedTimestampMetadata"
ingest_api_DeprecatedTimestampMetadata.__module__ = "ingest_service_api.ingest_api"


class ingest_api_DeprecatedTriggerIngest(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'source': ConjureFieldDefinition('source', ingest_api_IngestSource),
            'properties': ConjureFieldDefinition('properties', Dict[str, str]),
            'dataset_name': ConjureFieldDefinition('datasetName', OptionalTypeWrapper[str]),
            'timestamp_metadata': ConjureFieldDefinition('timestampMetadata', OptionalTypeWrapper[ingest_api_DeprecatedTimestampMetadata])
        }

    __slots__: List[str] = ['_source', '_properties', '_dataset_name', '_timestamp_metadata']

    def __init__(self, properties: Dict[str, str], source: "ingest_api_IngestSource", dataset_name: Optional[str] = None, timestamp_metadata: Optional["ingest_api_DeprecatedTimestampMetadata"] = None) -> None:
        self._source = source
        self._properties = properties
        self._dataset_name = dataset_name
        self._timestamp_metadata = timestamp_metadata

    @builtins.property
    def source(self) -> "ingest_api_IngestSource":
        return self._source

    @builtins.property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @builtins.property
    def dataset_name(self) -> Optional[str]:
        return self._dataset_name

    @builtins.property
    def timestamp_metadata(self) -> Optional["ingest_api_DeprecatedTimestampMetadata"]:
        return self._timestamp_metadata


ingest_api_DeprecatedTriggerIngest.__name__ = "DeprecatedTriggerIngest"
ingest_api_DeprecatedTriggerIngest.__qualname__ = "DeprecatedTriggerIngest"
ingest_api_DeprecatedTriggerIngest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_Duration(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'seconds': ConjureFieldDefinition('seconds', int),
            'nanos': ConjureFieldDefinition('nanos', int)
        }

    __slots__: List[str] = ['_seconds', '_nanos']

    def __init__(self, nanos: int, seconds: int) -> None:
        self._seconds = seconds
        self._nanos = nanos

    @builtins.property
    def seconds(self) -> int:
        return self._seconds

    @builtins.property
    def nanos(self) -> int:
        return self._nanos


ingest_api_Duration.__name__ = "Duration"
ingest_api_Duration.__qualname__ = "Duration"
ingest_api_Duration.__module__ = "ingest_service_api.ingest_api"


class ingest_api_EpochTimestamp(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'time_unit': ConjureFieldDefinition('timeUnit', ingest_api_TimeUnit)
        }

    __slots__: List[str] = ['_time_unit']

    def __init__(self, time_unit: "ingest_api_TimeUnit") -> None:
        self._time_unit = time_unit

    @builtins.property
    def time_unit(self) -> "ingest_api_TimeUnit":
        return self._time_unit


ingest_api_EpochTimestamp.__name__ = "EpochTimestamp"
ingest_api_EpochTimestamp.__qualname__ = "EpochTimestamp"
ingest_api_EpochTimestamp.__module__ = "ingest_service_api.ingest_api"


class ingest_api_ErrorResult(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'error_type': ConjureFieldDefinition('errorType', ingest_api_ErrorType),
            'message': ConjureFieldDefinition('message', str)
        }

    __slots__: List[str] = ['_error_type', '_message']

    def __init__(self, error_type: str, message: str) -> None:
        self._error_type = error_type
        self._message = message

    @builtins.property
    def error_type(self) -> str:
        return self._error_type

    @builtins.property
    def message(self) -> str:
        return self._message


ingest_api_ErrorResult.__name__ = "ErrorResult"
ingest_api_ErrorResult.__qualname__ = "ErrorResult"
ingest_api_ErrorResult.__module__ = "ingest_service_api.ingest_api"


class ingest_api_FileTypeProperties(ConjureUnionType):
    _csv_properties: Optional["ingest_api_CsvProperties"] = None
    _parquet_properties: Optional["ingest_api_ParquetProperties"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'csv_properties': ConjureFieldDefinition('csvProperties', ingest_api_CsvProperties),
            'parquet_properties': ConjureFieldDefinition('parquetProperties', ingest_api_ParquetProperties)
        }

    def __init__(
            self,
            csv_properties: Optional["ingest_api_CsvProperties"] = None,
            parquet_properties: Optional["ingest_api_ParquetProperties"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (csv_properties is not None) + (parquet_properties is not None) != 1:
                raise ValueError('a union must contain a single member')

            if csv_properties is not None:
                self._csv_properties = csv_properties
                self._type = 'csvProperties'
            if parquet_properties is not None:
                self._parquet_properties = parquet_properties
                self._type = 'parquetProperties'

        elif type_of_union == 'csvProperties':
            if csv_properties is None:
                raise ValueError('a union value must not be None')
            self._csv_properties = csv_properties
            self._type = 'csvProperties'
        elif type_of_union == 'parquetProperties':
            if parquet_properties is None:
                raise ValueError('a union value must not be None')
            self._parquet_properties = parquet_properties
            self._type = 'parquetProperties'

    @builtins.property
    def csv_properties(self) -> Optional["ingest_api_CsvProperties"]:
        return self._csv_properties

    @builtins.property
    def parquet_properties(self) -> Optional["ingest_api_ParquetProperties"]:
        return self._parquet_properties

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_FileTypePropertiesVisitor):
            raise ValueError('{} is not an instance of ingest_api_FileTypePropertiesVisitor'.format(visitor.__class__.__name__))
        if self._type == 'csvProperties' and self.csv_properties is not None:
            return visitor._csv_properties(self.csv_properties)
        if self._type == 'parquetProperties' and self.parquet_properties is not None:
            return visitor._parquet_properties(self.parquet_properties)


ingest_api_FileTypeProperties.__name__ = "FileTypeProperties"
ingest_api_FileTypeProperties.__qualname__ = "FileTypeProperties"
ingest_api_FileTypeProperties.__module__ = "ingest_service_api.ingest_api"


class ingest_api_FileTypePropertiesVisitor:

    @abstractmethod
    def _csv_properties(self, csv_properties: "ingest_api_CsvProperties") -> Any:
        pass

    @abstractmethod
    def _parquet_properties(self, parquet_properties: "ingest_api_ParquetProperties") -> Any:
        pass


ingest_api_FileTypePropertiesVisitor.__name__ = "FileTypePropertiesVisitor"
ingest_api_FileTypePropertiesVisitor.__qualname__ = "FileTypePropertiesVisitor"
ingest_api_FileTypePropertiesVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_InProgressResult(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
        }

    __slots__: List[str] = []



ingest_api_InProgressResult.__name__ = "InProgressResult"
ingest_api_InProgressResult.__qualname__ = "InProgressResult"
ingest_api_InProgressResult.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestDataSource(ConjureUnionType):
    _existing_data_source: Optional[str] = None
    _new_data_source: Optional["ingest_api_DeprecatedNewDataSource"] = None
    _new_data_source_v2: Optional["ingest_api_NewDataSource"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'existing_data_source': ConjureFieldDefinition('existingDataSource', ingest_api_DataSourceRid),
            'new_data_source': ConjureFieldDefinition('newDataSource', ingest_api_DeprecatedNewDataSource),
            'new_data_source_v2': ConjureFieldDefinition('newDataSourceV2', ingest_api_NewDataSource)
        }

    def __init__(
            self,
            existing_data_source: Optional[str] = None,
            new_data_source: Optional["ingest_api_DeprecatedNewDataSource"] = None,
            new_data_source_v2: Optional["ingest_api_NewDataSource"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (existing_data_source is not None) + (new_data_source is not None) + (new_data_source_v2 is not None) != 1:
                raise ValueError('a union must contain a single member')

            if existing_data_source is not None:
                self._existing_data_source = existing_data_source
                self._type = 'existingDataSource'
            if new_data_source is not None:
                self._new_data_source = new_data_source
                self._type = 'newDataSource'
            if new_data_source_v2 is not None:
                self._new_data_source_v2 = new_data_source_v2
                self._type = 'newDataSourceV2'

        elif type_of_union == 'existingDataSource':
            if existing_data_source is None:
                raise ValueError('a union value must not be None')
            self._existing_data_source = existing_data_source
            self._type = 'existingDataSource'
        elif type_of_union == 'newDataSource':
            if new_data_source is None:
                raise ValueError('a union value must not be None')
            self._new_data_source = new_data_source
            self._type = 'newDataSource'
        elif type_of_union == 'newDataSourceV2':
            if new_data_source_v2 is None:
                raise ValueError('a union value must not be None')
            self._new_data_source_v2 = new_data_source_v2
            self._type = 'newDataSourceV2'

    @builtins.property
    def existing_data_source(self) -> Optional[str]:
        return self._existing_data_source

    @builtins.property
    def new_data_source(self) -> Optional["ingest_api_DeprecatedNewDataSource"]:
        return self._new_data_source

    @builtins.property
    def new_data_source_v2(self) -> Optional["ingest_api_NewDataSource"]:
        return self._new_data_source_v2

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_IngestDataSourceVisitor):
            raise ValueError('{} is not an instance of ingest_api_IngestDataSourceVisitor'.format(visitor.__class__.__name__))
        if self._type == 'existingDataSource' and self.existing_data_source is not None:
            return visitor._existing_data_source(self.existing_data_source)
        if self._type == 'newDataSource' and self.new_data_source is not None:
            return visitor._new_data_source(self.new_data_source)
        if self._type == 'newDataSourceV2' and self.new_data_source_v2 is not None:
            return visitor._new_data_source_v2(self.new_data_source_v2)


ingest_api_IngestDataSource.__name__ = "IngestDataSource"
ingest_api_IngestDataSource.__qualname__ = "IngestDataSource"
ingest_api_IngestDataSource.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestDataSourceVisitor:

    @abstractmethod
    def _existing_data_source(self, existing_data_source: str) -> Any:
        pass

    @abstractmethod
    def _new_data_source(self, new_data_source: "ingest_api_DeprecatedNewDataSource") -> Any:
        pass

    @abstractmethod
    def _new_data_source_v2(self, new_data_source_v2: "ingest_api_NewDataSource") -> Any:
        pass


ingest_api_IngestDataSourceVisitor.__name__ = "IngestDataSourceVisitor"
ingest_api_IngestDataSourceVisitor.__qualname__ = "IngestDataSourceVisitor"
ingest_api_IngestDataSourceVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestProgressV2(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'start_time': ConjureFieldDefinition('startTime', str),
            'end_time': ConjureFieldDefinition('endTime', OptionalTypeWrapper[str]),
            'ingest_status': ConjureFieldDefinition('ingestStatus', ingest_api_IngestStatusV2),
            'incalculable': ConjureFieldDefinition('incalculable', OptionalTypeWrapper[bool])
        }

    __slots__: List[str] = ['_start_time', '_end_time', '_ingest_status', '_incalculable']

    def __init__(self, ingest_status: "ingest_api_IngestStatusV2", start_time: str, end_time: Optional[str] = None, incalculable: Optional[bool] = None) -> None:
        self._start_time = start_time
        self._end_time = end_time
        self._ingest_status = ingest_status
        self._incalculable = incalculable

    @builtins.property
    def start_time(self) -> str:
        """
        Timestamp at start of ingest
        """
        return self._start_time

    @builtins.property
    def end_time(self) -> Optional[str]:
        """
        Timestamp at end of ingest, empty if still in progress
        """
        return self._end_time

    @builtins.property
    def ingest_status(self) -> "ingest_api_IngestStatusV2":
        """
        Status of ingest, contains error if failed
        """
        return self._ingest_status

    @builtins.property
    def incalculable(self) -> Optional[bool]:
        """
        Whether ingest duration can be reliably calculated
        """
        return self._incalculable


ingest_api_IngestProgressV2.__name__ = "IngestProgressV2"
ingest_api_IngestProgressV2.__qualname__ = "IngestProgressV2"
ingest_api_IngestProgressV2.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestRunDataSource(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'data_source': ConjureFieldDefinition('dataSource', ingest_api_IngestDataSource),
            'time_offset_spec': ConjureFieldDefinition('timeOffsetSpec', OptionalTypeWrapper[ingest_api_TimeOffsetSpec])
        }

    __slots__: List[str] = ['_data_source', '_time_offset_spec']

    def __init__(self, data_source: "ingest_api_IngestDataSource", time_offset_spec: Optional["ingest_api_TimeOffsetSpec"] = None) -> None:
        self._data_source = data_source
        self._time_offset_spec = time_offset_spec

    @builtins.property
    def data_source(self) -> "ingest_api_IngestDataSource":
        return self._data_source

    @builtins.property
    def time_offset_spec(self) -> Optional["ingest_api_TimeOffsetSpec"]:
        return self._time_offset_spec


ingest_api_IngestRunDataSource.__name__ = "IngestRunDataSource"
ingest_api_IngestRunDataSource.__qualname__ = "IngestRunDataSource"
ingest_api_IngestRunDataSource.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestRunRequest(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'rid': ConjureFieldDefinition('rid', OptionalTypeWrapper[str]),
            'title': ConjureFieldDefinition('title', str),
            'description': ConjureFieldDefinition('description', str),
            'start_time': ConjureFieldDefinition('startTime', ingest_api_UtcTimestamp),
            'end_time': ConjureFieldDefinition('endTime', OptionalTypeWrapper[ingest_api_UtcTimestamp]),
            'properties': ConjureFieldDefinition('properties', Dict[ingest_api_PropertyName, ingest_api_PropertyValue]),
            'labels': ConjureFieldDefinition('labels', List[ingest_api_Label]),
            'run_prefix': ConjureFieldDefinition('runPrefix', OptionalTypeWrapper[str]),
            'data_sources': ConjureFieldDefinition('dataSources', Dict[ingest_api_DataSourceRefName, ingest_api_IngestRunDataSource])
        }

    __slots__: List[str] = ['_rid', '_title', '_description', '_start_time', '_end_time', '_properties', '_labels', '_run_prefix', '_data_sources']

    def __init__(self, data_sources: Dict[str, "ingest_api_IngestRunDataSource"], description: str, labels: List[str], properties: Dict[str, str], start_time: "ingest_api_UtcTimestamp", title: str, end_time: Optional["ingest_api_UtcTimestamp"] = None, rid: Optional[str] = None, run_prefix: Optional[str] = None) -> None:
        self._rid = rid
        self._title = title
        self._description = description
        self._start_time = start_time
        self._end_time = end_time
        self._properties = properties
        self._labels = labels
        self._run_prefix = run_prefix
        self._data_sources = data_sources

    @builtins.property
    def rid(self) -> Optional[str]:
        """
        If a run with the same rid already exists, the run will be updated.
        """
        return self._rid

    @builtins.property
    def title(self) -> str:
        return self._title

    @builtins.property
    def description(self) -> str:
        return self._description

    @builtins.property
    def start_time(self) -> "ingest_api_UtcTimestamp":
        return self._start_time

    @builtins.property
    def end_time(self) -> Optional["ingest_api_UtcTimestamp"]:
        return self._end_time

    @builtins.property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @builtins.property
    def labels(self) -> List[str]:
        return self._labels

    @builtins.property
    def run_prefix(self) -> Optional[str]:
        """
        for example, SIM, HTL, FLT
        """
        return self._run_prefix

    @builtins.property
    def data_sources(self) -> Dict[str, "ingest_api_IngestRunDataSource"]:
        return self._data_sources


ingest_api_IngestRunRequest.__name__ = "IngestRunRequest"
ingest_api_IngestRunRequest.__qualname__ = "IngestRunRequest"
ingest_api_IngestRunRequest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestRunResponse(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'run_rid': ConjureFieldDefinition('runRid', ingest_api_RunRid)
        }

    __slots__: List[str] = ['_run_rid']

    def __init__(self, run_rid: str) -> None:
        self._run_rid = run_rid

    @builtins.property
    def run_rid(self) -> str:
        return self._run_rid


ingest_api_IngestRunResponse.__name__ = "IngestRunResponse"
ingest_api_IngestRunResponse.__qualname__ = "IngestRunResponse"
ingest_api_IngestRunResponse.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestService(Service):
    """
    The Ingest Service handles the data ingestion into Nominal/Clickhouse.
    """

    def deprecated_trigger_ingest(self, auth_header: str, trigger_ingest: "ingest_api_DeprecatedTriggerIngest") -> "ingest_api_TriggeredIngest":

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
        }

        _path_params: Dict[str, Any] = {
        }

        _json: Any = ConjureEncoder().default(trigger_ingest)

        _path = '/ingest/v1/trigger-ingest'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_TriggeredIngest, self._return_none_for_unknown_union_types)

    def trigger_ingest(self, auth_header: str, trigger_ingest: "ingest_api_TriggerIngest") -> "ingest_api_TriggeredIngest":
        """
        Triggers an ingest job for the given data source.
The ingest job will be processed asynchronously.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
        }

        _path_params: Dict[str, Any] = {
        }

        _json: Any = ConjureEncoder().default(trigger_ingest)

        _path = '/ingest/v1/trigger-ingest-v2'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_TriggeredIngest, self._return_none_for_unknown_union_types)

    def ingest_run(self, auth_header: str, request: "ingest_api_IngestRunRequest") -> "ingest_api_IngestRunResponse":
        """
        Creates a run and ingests data sources to be added to the run.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
        }

        _path_params: Dict[str, Any] = {
        }

        _json: Any = ConjureEncoder().default(request)

        _path = '/ingest/v1/ingest-run'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_IngestRunResponse, self._return_none_for_unknown_union_types)

    def ingest_video(self, auth_header: str, ingest_video: "ingest_api_IngestVideoRequest") -> "ingest_api_IngestVideoResponse":
        """
        Ingests video data from a S3 Nominal upload bucket.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
        }

        _path_params: Dict[str, Any] = {
        }

        _json: Any = ConjureEncoder().default(ingest_video)

        _path = '/ingest/v1/ingest-video'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_IngestVideoResponse, self._return_none_for_unknown_union_types)


ingest_api_IngestService.__name__ = "IngestService"
ingest_api_IngestService.__qualname__ = "IngestService"
ingest_api_IngestService.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestSource(ConjureUnionType):
    _s3: Optional["ingest_api_S3IngestSource"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            's3': ConjureFieldDefinition('s3', ingest_api_S3IngestSource)
        }

    def __init__(
            self,
            s3: Optional["ingest_api_S3IngestSource"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (s3 is not None) != 1:
                raise ValueError('a union must contain a single member')

            if s3 is not None:
                self._s3 = s3
                self._type = 's3'

        elif type_of_union == 's3':
            if s3 is None:
                raise ValueError('a union value must not be None')
            self._s3 = s3
            self._type = 's3'

    @builtins.property
    def s3(self) -> Optional["ingest_api_S3IngestSource"]:
        return self._s3

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_IngestSourceVisitor):
            raise ValueError('{} is not an instance of ingest_api_IngestSourceVisitor'.format(visitor.__class__.__name__))
        if self._type == 's3' and self.s3 is not None:
            return visitor._s3(self.s3)


ingest_api_IngestSource.__name__ = "IngestSource"
ingest_api_IngestSource.__qualname__ = "IngestSource"
ingest_api_IngestSource.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestSourceVisitor:

    @abstractmethod
    def _s3(self, s3: "ingest_api_S3IngestSource") -> Any:
        pass


ingest_api_IngestSourceVisitor.__name__ = "IngestSourceVisitor"
ingest_api_IngestSourceVisitor.__qualname__ = "IngestSourceVisitor"
ingest_api_IngestSourceVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestStatus(ConjureEnumType):

    IN_PROGRESS = 'IN_PROGRESS'
    '''IN_PROGRESS'''
    COMPLETED = 'COMPLETED'
    '''COMPLETED'''
    FAILED = 'FAILED'
    '''FAILED'''
    UNKNOWN = 'UNKNOWN'
    '''UNKNOWN'''

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


ingest_api_IngestStatus.__name__ = "IngestStatus"
ingest_api_IngestStatus.__qualname__ = "IngestStatus"
ingest_api_IngestStatus.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestStatusV2(ConjureUnionType):
    _success: Optional["ingest_api_SuccessResult"] = None
    _error: Optional["ingest_api_ErrorResult"] = None
    _in_progress: Optional["ingest_api_InProgressResult"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'success': ConjureFieldDefinition('success', ingest_api_SuccessResult),
            'error': ConjureFieldDefinition('error', ingest_api_ErrorResult),
            'in_progress': ConjureFieldDefinition('inProgress', ingest_api_InProgressResult)
        }

    def __init__(
            self,
            success: Optional["ingest_api_SuccessResult"] = None,
            error: Optional["ingest_api_ErrorResult"] = None,
            in_progress: Optional["ingest_api_InProgressResult"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (success is not None) + (error is not None) + (in_progress is not None) != 1:
                raise ValueError('a union must contain a single member')

            if success is not None:
                self._success = success
                self._type = 'success'
            if error is not None:
                self._error = error
                self._type = 'error'
            if in_progress is not None:
                self._in_progress = in_progress
                self._type = 'inProgress'

        elif type_of_union == 'success':
            if success is None:
                raise ValueError('a union value must not be None')
            self._success = success
            self._type = 'success'
        elif type_of_union == 'error':
            if error is None:
                raise ValueError('a union value must not be None')
            self._error = error
            self._type = 'error'
        elif type_of_union == 'inProgress':
            if in_progress is None:
                raise ValueError('a union value must not be None')
            self._in_progress = in_progress
            self._type = 'inProgress'

    @builtins.property
    def success(self) -> Optional["ingest_api_SuccessResult"]:
        return self._success

    @builtins.property
    def error(self) -> Optional["ingest_api_ErrorResult"]:
        return self._error

    @builtins.property
    def in_progress(self) -> Optional["ingest_api_InProgressResult"]:
        return self._in_progress

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_IngestStatusV2Visitor):
            raise ValueError('{} is not an instance of ingest_api_IngestStatusV2Visitor'.format(visitor.__class__.__name__))
        if self._type == 'success' and self.success is not None:
            return visitor._success(self.success)
        if self._type == 'error' and self.error is not None:
            return visitor._error(self.error)
        if self._type == 'inProgress' and self.in_progress is not None:
            return visitor._in_progress(self.in_progress)


ingest_api_IngestStatusV2.__name__ = "IngestStatusV2"
ingest_api_IngestStatusV2.__qualname__ = "IngestStatusV2"
ingest_api_IngestStatusV2.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestStatusV2Visitor:

    @abstractmethod
    def _success(self, success: "ingest_api_SuccessResult") -> Any:
        pass

    @abstractmethod
    def _error(self, error: "ingest_api_ErrorResult") -> Any:
        pass

    @abstractmethod
    def _in_progress(self, in_progress: "ingest_api_InProgressResult") -> Any:
        pass


ingest_api_IngestStatusV2Visitor.__name__ = "IngestStatusV2Visitor"
ingest_api_IngestStatusV2Visitor.__qualname__ = "IngestStatusV2Visitor"
ingest_api_IngestStatusV2Visitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestVideoRequest(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'sources': ConjureFieldDefinition('sources', List[ingest_api_IngestSource]),
            'properties': ConjureFieldDefinition('properties', Dict[ingest_api_PropertyName, ingest_api_PropertyValue]),
            'labels': ConjureFieldDefinition('labels', List[ingest_api_Label]),
            'title': ConjureFieldDefinition('title', OptionalTypeWrapper[str]),
            'description': ConjureFieldDefinition('description', OptionalTypeWrapper[str]),
            'timestamps': ConjureFieldDefinition('timestamps', ingest_api_VideoTimestampManifest)
        }

    __slots__: List[str] = ['_sources', '_properties', '_labels', '_title', '_description', '_timestamps']

    def __init__(self, labels: List[str], properties: Dict[str, str], sources: List["ingest_api_IngestSource"], timestamps: "ingest_api_VideoTimestampManifest", description: Optional[str] = None, title: Optional[str] = None) -> None:
        self._sources = sources
        self._properties = properties
        self._labels = labels
        self._title = title
        self._description = description
        self._timestamps = timestamps

    @builtins.property
    def sources(self) -> List["ingest_api_IngestSource"]:
        return self._sources

    @builtins.property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @builtins.property
    def labels(self) -> List[str]:
        return self._labels

    @builtins.property
    def title(self) -> Optional[str]:
        return self._title

    @builtins.property
    def description(self) -> Optional[str]:
        return self._description

    @builtins.property
    def timestamps(self) -> "ingest_api_VideoTimestampManifest":
        return self._timestamps


ingest_api_IngestVideoRequest.__name__ = "IngestVideoRequest"
ingest_api_IngestVideoRequest.__qualname__ = "IngestVideoRequest"
ingest_api_IngestVideoRequest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_IngestVideoResponse(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'video_rid': ConjureFieldDefinition('videoRid', str),
            'async_handle': ConjureFieldDefinition('asyncHandle', ingest_api_AsyncHandle)
        }

    __slots__: List[str] = ['_video_rid', '_async_handle']

    def __init__(self, async_handle: "ingest_api_AsyncHandle", video_rid: str) -> None:
        self._video_rid = video_rid
        self._async_handle = async_handle

    @builtins.property
    def video_rid(self) -> str:
        return self._video_rid

    @builtins.property
    def async_handle(self) -> "ingest_api_AsyncHandle":
        return self._async_handle


ingest_api_IngestVideoResponse.__name__ = "IngestVideoResponse"
ingest_api_IngestVideoResponse.__qualname__ = "IngestVideoResponse"
ingest_api_IngestVideoResponse.__module__ = "ingest_service_api.ingest_api"


class ingest_api_InitiateMultipartUploadRequest(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'filename': ConjureFieldDefinition('filename', str),
            'filetype': ConjureFieldDefinition('filetype', str)
        }

    __slots__: List[str] = ['_filename', '_filetype']

    def __init__(self, filename: str, filetype: str) -> None:
        self._filename = filename
        self._filetype = filetype

    @builtins.property
    def filename(self) -> str:
        return self._filename

    @builtins.property
    def filetype(self) -> str:
        return self._filetype


ingest_api_InitiateMultipartUploadRequest.__name__ = "InitiateMultipartUploadRequest"
ingest_api_InitiateMultipartUploadRequest.__qualname__ = "InitiateMultipartUploadRequest"
ingest_api_InitiateMultipartUploadRequest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_InitiateMultipartUploadResponse(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'upload_id': ConjureFieldDefinition('uploadId', str),
            'key': ConjureFieldDefinition('key', str)
        }

    __slots__: List[str] = ['_upload_id', '_key']

    def __init__(self, key: str, upload_id: str) -> None:
        self._upload_id = upload_id
        self._key = key

    @builtins.property
    def upload_id(self) -> str:
        return self._upload_id

    @builtins.property
    def key(self) -> str:
        return self._key


ingest_api_InitiateMultipartUploadResponse.__name__ = "InitiateMultipartUploadResponse"
ingest_api_InitiateMultipartUploadResponse.__qualname__ = "InitiateMultipartUploadResponse"
ingest_api_InitiateMultipartUploadResponse.__module__ = "ingest_service_api.ingest_api"


class ingest_api_Iso8601Timestamp(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
        }

    __slots__: List[str] = []



ingest_api_Iso8601Timestamp.__name__ = "Iso8601Timestamp"
ingest_api_Iso8601Timestamp.__qualname__ = "Iso8601Timestamp"
ingest_api_Iso8601Timestamp.__module__ = "ingest_service_api.ingest_api"


class ingest_api_NewDataSource(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'source': ConjureFieldDefinition('source', ingest_api_IngestSource),
            'properties': ConjureFieldDefinition('properties', Dict[ingest_api_PropertyName, ingest_api_PropertyValue]),
            'labels': ConjureFieldDefinition('labels', List[ingest_api_Label]),
            'description': ConjureFieldDefinition('description', OptionalTypeWrapper[str]),
            'name': ConjureFieldDefinition('name', OptionalTypeWrapper[str]),
            'time_column_spec': ConjureFieldDefinition('timeColumnSpec', OptionalTypeWrapper[ingest_api_TimestampMetadata]),
            'file_type_properties': ConjureFieldDefinition('fileTypeProperties', OptionalTypeWrapper[ingest_api_FileTypeProperties]),
            'channel_config': ConjureFieldDefinition('channelConfig', OptionalTypeWrapper[ingest_api_ChannelConfig])
        }

    __slots__: List[str] = ['_source', '_properties', '_labels', '_description', '_name', '_time_column_spec', '_file_type_properties', '_channel_config']

    def __init__(self, labels: List[str], properties: Dict[str, str], source: "ingest_api_IngestSource", channel_config: Optional["ingest_api_ChannelConfig"] = None, description: Optional[str] = None, file_type_properties: Optional["ingest_api_FileTypeProperties"] = None, name: Optional[str] = None, time_column_spec: Optional["ingest_api_TimestampMetadata"] = None) -> None:
        self._source = source
        self._properties = properties
        self._labels = labels
        self._description = description
        self._name = name
        self._time_column_spec = time_column_spec
        self._file_type_properties = file_type_properties
        self._channel_config = channel_config

    @builtins.property
    def source(self) -> "ingest_api_IngestSource":
        return self._source

    @builtins.property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @builtins.property
    def labels(self) -> List[str]:
        return self._labels

    @builtins.property
    def description(self) -> Optional[str]:
        return self._description

    @builtins.property
    def name(self) -> Optional[str]:
        return self._name

    @builtins.property
    def time_column_spec(self) -> Optional["ingest_api_TimestampMetadata"]:
        return self._time_column_spec

    @builtins.property
    def file_type_properties(self) -> Optional["ingest_api_FileTypeProperties"]:
        return self._file_type_properties

    @builtins.property
    def channel_config(self) -> Optional["ingest_api_ChannelConfig"]:
        return self._channel_config


ingest_api_NewDataSource.__name__ = "NewDataSource"
ingest_api_NewDataSource.__qualname__ = "NewDataSource"
ingest_api_NewDataSource.__module__ = "ingest_service_api.ingest_api"


class ingest_api_NoTimestampManifest(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'starting_timestamp': ConjureFieldDefinition('startingTimestamp', ingest_api_UtcTimestamp),
            'scale_parameter': ConjureFieldDefinition('scaleParameter', OptionalTypeWrapper[ingest_api_ScaleParameter])
        }

    __slots__: List[str] = ['_starting_timestamp', '_scale_parameter']

    def __init__(self, starting_timestamp: "ingest_api_UtcTimestamp", scale_parameter: Optional["ingest_api_ScaleParameter"] = None) -> None:
        self._starting_timestamp = starting_timestamp
        self._scale_parameter = scale_parameter

    @builtins.property
    def starting_timestamp(self) -> "ingest_api_UtcTimestamp":
        return self._starting_timestamp

    @builtins.property
    def scale_parameter(self) -> Optional["ingest_api_ScaleParameter"]:
        """
        A field that specifies that the frame rate of the video does not match the frame rate of the camera | i.e. a slowed down or sped up video. Can specify either the camera frame rate or the absolute end time.
        """
        return self._scale_parameter


ingest_api_NoTimestampManifest.__name__ = "NoTimestampManifest"
ingest_api_NoTimestampManifest.__qualname__ = "NoTimestampManifest"
ingest_api_NoTimestampManifest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_ParquetProperties(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'unit_metadata_key': ConjureFieldDefinition('unitMetadataKey', OptionalTypeWrapper[str])
        }

    __slots__: List[str] = ['_unit_metadata_key']

    def __init__(self, unit_metadata_key: Optional[str] = None) -> None:
        self._unit_metadata_key = unit_metadata_key

    @builtins.property
    def unit_metadata_key(self) -> Optional[str]:
        """
        If set, extracts the unit for series from the column metadata. The unit is expected to be the value
corresponding to this key.
        """
        return self._unit_metadata_key


ingest_api_ParquetProperties.__name__ = "ParquetProperties"
ingest_api_ParquetProperties.__qualname__ = "ParquetProperties"
ingest_api_ParquetProperties.__module__ = "ingest_service_api.ingest_api"


class ingest_api_Part(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'part_number': ConjureFieldDefinition('partNumber', int),
            'etag': ConjureFieldDefinition('etag', str)
        }

    __slots__: List[str] = ['_part_number', '_etag']

    def __init__(self, etag: str, part_number: int) -> None:
        self._part_number = part_number
        self._etag = etag

    @builtins.property
    def part_number(self) -> int:
        return self._part_number

    @builtins.property
    def etag(self) -> str:
        return self._etag


ingest_api_Part.__name__ = "Part"
ingest_api_Part.__qualname__ = "Part"
ingest_api_Part.__module__ = "ingest_service_api.ingest_api"


class ingest_api_PartWithSize(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'part_number': ConjureFieldDefinition('partNumber', int),
            'etag': ConjureFieldDefinition('etag', str),
            'size': ConjureFieldDefinition('size', int)
        }

    __slots__: List[str] = ['_part_number', '_etag', '_size']

    def __init__(self, etag: str, part_number: int, size: int) -> None:
        self._part_number = part_number
        self._etag = etag
        self._size = size

    @builtins.property
    def part_number(self) -> int:
        return self._part_number

    @builtins.property
    def etag(self) -> str:
        return self._etag

    @builtins.property
    def size(self) -> int:
        return self._size


ingest_api_PartWithSize.__name__ = "PartWithSize"
ingest_api_PartWithSize.__qualname__ = "PartWithSize"
ingest_api_PartWithSize.__module__ = "ingest_service_api.ingest_api"


class ingest_api_RelativeTimestamp(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'time_unit': ConjureFieldDefinition('timeUnit', ingest_api_TimeUnit)
        }

    __slots__: List[str] = ['_time_unit']

    def __init__(self, time_unit: "ingest_api_TimeUnit") -> None:
        self._time_unit = time_unit

    @builtins.property
    def time_unit(self) -> "ingest_api_TimeUnit":
        return self._time_unit


ingest_api_RelativeTimestamp.__name__ = "RelativeTimestamp"
ingest_api_RelativeTimestamp.__qualname__ = "RelativeTimestamp"
ingest_api_RelativeTimestamp.__module__ = "ingest_service_api.ingest_api"


class ingest_api_S3IngestSource(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'path': ConjureFieldDefinition('path', str)
        }

    __slots__: List[str] = ['_path']

    def __init__(self, path: str) -> None:
        self._path = path

    @builtins.property
    def path(self) -> str:
        return self._path


ingest_api_S3IngestSource.__name__ = "S3IngestSource"
ingest_api_S3IngestSource.__qualname__ = "S3IngestSource"
ingest_api_S3IngestSource.__module__ = "ingest_service_api.ingest_api"


class ingest_api_ScaleParameter(ConjureUnionType):
    _true_frame_rate: Optional[float] = None
    _ending_timestamp: Optional["ingest_api_UtcTimestamp"] = None
    _scale_factor: Optional[float] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'true_frame_rate': ConjureFieldDefinition('trueFrameRate', float),
            'ending_timestamp': ConjureFieldDefinition('endingTimestamp', ingest_api_UtcTimestamp),
            'scale_factor': ConjureFieldDefinition('scaleFactor', float)
        }

    def __init__(
            self,
            true_frame_rate: Optional[float] = None,
            ending_timestamp: Optional["ingest_api_UtcTimestamp"] = None,
            scale_factor: Optional[float] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (true_frame_rate is not None) + (ending_timestamp is not None) + (scale_factor is not None) != 1:
                raise ValueError('a union must contain a single member')

            if true_frame_rate is not None:
                self._true_frame_rate = true_frame_rate
                self._type = 'trueFrameRate'
            if ending_timestamp is not None:
                self._ending_timestamp = ending_timestamp
                self._type = 'endingTimestamp'
            if scale_factor is not None:
                self._scale_factor = scale_factor
                self._type = 'scaleFactor'

        elif type_of_union == 'trueFrameRate':
            if true_frame_rate is None:
                raise ValueError('a union value must not be None')
            self._true_frame_rate = true_frame_rate
            self._type = 'trueFrameRate'
        elif type_of_union == 'endingTimestamp':
            if ending_timestamp is None:
                raise ValueError('a union value must not be None')
            self._ending_timestamp = ending_timestamp
            self._type = 'endingTimestamp'
        elif type_of_union == 'scaleFactor':
            if scale_factor is None:
                raise ValueError('a union value must not be None')
            self._scale_factor = scale_factor
            self._type = 'scaleFactor'

    @builtins.property
    def true_frame_rate(self) -> Optional[float]:
        return self._true_frame_rate

    @builtins.property
    def ending_timestamp(self) -> Optional["ingest_api_UtcTimestamp"]:
        """
        the timestamp corresponding to absolute starting timestamp plus absolute duration of the video.
        """
        return self._ending_timestamp

    @builtins.property
    def scale_factor(self) -> Optional[float]:
        """
        the scale factor can be used to calculate whether media duration differs from a video's | real duration, and if so, the true frame rate of the camera. The video time will thus be scaled | by the ratio of the real duration to media duration, or media frame rate to true frame rate.
        """
        return self._scale_factor

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_ScaleParameterVisitor):
            raise ValueError('{} is not an instance of ingest_api_ScaleParameterVisitor'.format(visitor.__class__.__name__))
        if self._type == 'trueFrameRate' and self.true_frame_rate is not None:
            return visitor._true_frame_rate(self.true_frame_rate)
        if self._type == 'endingTimestamp' and self.ending_timestamp is not None:
            return visitor._ending_timestamp(self.ending_timestamp)
        if self._type == 'scaleFactor' and self.scale_factor is not None:
            return visitor._scale_factor(self.scale_factor)


ingest_api_ScaleParameter.__name__ = "ScaleParameter"
ingest_api_ScaleParameter.__qualname__ = "ScaleParameter"
ingest_api_ScaleParameter.__module__ = "ingest_service_api.ingest_api"


class ingest_api_ScaleParameterVisitor:

    @abstractmethod
    def _true_frame_rate(self, true_frame_rate: float) -> Any:
        pass

    @abstractmethod
    def _ending_timestamp(self, ending_timestamp: "ingest_api_UtcTimestamp") -> Any:
        pass

    @abstractmethod
    def _scale_factor(self, scale_factor: float) -> Any:
        pass


ingest_api_ScaleParameterVisitor.__name__ = "ScaleParameterVisitor"
ingest_api_ScaleParameterVisitor.__qualname__ = "ScaleParameterVisitor"
ingest_api_ScaleParameterVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_SignPartResponse(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'url': ConjureFieldDefinition('url', str),
            'headers': ConjureFieldDefinition('headers', Dict[str, str])
        }

    __slots__: List[str] = ['_url', '_headers']

    def __init__(self, headers: Dict[str, str], url: str) -> None:
        self._url = url
        self._headers = headers

    @builtins.property
    def url(self) -> str:
        return self._url

    @builtins.property
    def headers(self) -> Dict[str, str]:
        return self._headers


ingest_api_SignPartResponse.__name__ = "SignPartResponse"
ingest_api_SignPartResponse.__qualname__ = "SignPartResponse"
ingest_api_SignPartResponse.__module__ = "ingest_service_api.ingest_api"


class ingest_api_SkipRowsConfig(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'header_row_index': ConjureFieldDefinition('headerRowIndex', int),
            'data_start_row_index': ConjureFieldDefinition('dataStartRowIndex', int)
        }

    __slots__: List[str] = ['_header_row_index', '_data_start_row_index']

    def __init__(self, data_start_row_index: int, header_row_index: int) -> None:
        self._header_row_index = header_row_index
        self._data_start_row_index = data_start_row_index

    @builtins.property
    def header_row_index(self) -> int:
        return self._header_row_index

    @builtins.property
    def data_start_row_index(self) -> int:
        return self._data_start_row_index


ingest_api_SkipRowsConfig.__name__ = "SkipRowsConfig"
ingest_api_SkipRowsConfig.__qualname__ = "SkipRowsConfig"
ingest_api_SkipRowsConfig.__module__ = "ingest_service_api.ingest_api"


class ingest_api_SuccessResult(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
        }

    __slots__: List[str] = []



ingest_api_SuccessResult.__name__ = "SuccessResult"
ingest_api_SuccessResult.__qualname__ = "SuccessResult"
ingest_api_SuccessResult.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TimeOffsetSpec(ConjureUnionType):
    _nanos: Optional["ingest_api_Duration"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'nanos': ConjureFieldDefinition('nanos', ingest_api_Duration)
        }

    def __init__(
            self,
            nanos: Optional["ingest_api_Duration"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (nanos is not None) != 1:
                raise ValueError('a union must contain a single member')

            if nanos is not None:
                self._nanos = nanos
                self._type = 'nanos'

        elif type_of_union == 'nanos':
            if nanos is None:
                raise ValueError('a union value must not be None')
            self._nanos = nanos
            self._type = 'nanos'

    @builtins.property
    def nanos(self) -> Optional["ingest_api_Duration"]:
        return self._nanos

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_TimeOffsetSpecVisitor):
            raise ValueError('{} is not an instance of ingest_api_TimeOffsetSpecVisitor'.format(visitor.__class__.__name__))
        if self._type == 'nanos' and self.nanos is not None:
            return visitor._nanos(self.nanos)


ingest_api_TimeOffsetSpec.__name__ = "TimeOffsetSpec"
ingest_api_TimeOffsetSpec.__qualname__ = "TimeOffsetSpec"
ingest_api_TimeOffsetSpec.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TimeOffsetSpecVisitor:

    @abstractmethod
    def _nanos(self, nanos: "ingest_api_Duration") -> Any:
        pass


ingest_api_TimeOffsetSpecVisitor.__name__ = "TimeOffsetSpecVisitor"
ingest_api_TimeOffsetSpecVisitor.__qualname__ = "TimeOffsetSpecVisitor"
ingest_api_TimeOffsetSpecVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TimeUnit(ConjureEnumType):

    HOURS = 'HOURS'
    '''HOURS'''
    MINUTES = 'MINUTES'
    '''MINUTES'''
    SECONDS = 'SECONDS'
    '''SECONDS'''
    MILLISECONDS = 'MILLISECONDS'
    '''MILLISECONDS'''
    MICROSECONDS = 'MICROSECONDS'
    '''MICROSECONDS'''
    NANOSECONDS = 'NANOSECONDS'
    '''NANOSECONDS'''
    UNKNOWN = 'UNKNOWN'
    '''UNKNOWN'''

    def __reduce_ex__(self, proto):
        return self.__class__, (self.name,)


ingest_api_TimeUnit.__name__ = "TimeUnit"
ingest_api_TimeUnit.__qualname__ = "TimeUnit"
ingest_api_TimeUnit.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TimestampManifest(ConjureBeanType):
    """
    The timestamp manifest files will contain a list of absolute timestamps, in nanoseconds, that correspond to 
each frame in a video. Each file should be of type JSON and store a single list, the length of which equals
the number of frames in its corresponding video.
    """

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'sources': ConjureFieldDefinition('sources', List[ingest_api_IngestSource])
        }

    __slots__: List[str] = ['_sources']

    def __init__(self, sources: List["ingest_api_IngestSource"]) -> None:
        self._sources = sources

    @builtins.property
    def sources(self) -> List["ingest_api_IngestSource"]:
        return self._sources


ingest_api_TimestampManifest.__name__ = "TimestampManifest"
ingest_api_TimestampManifest.__qualname__ = "TimestampManifest"
ingest_api_TimestampManifest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TimestampMetadata(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'series_name': ConjureFieldDefinition('seriesName', str),
            'timestamp_type': ConjureFieldDefinition('timestampType', ingest_api_TimestampType)
        }

    __slots__: List[str] = ['_series_name', '_timestamp_type']

    def __init__(self, series_name: str, timestamp_type: "ingest_api_TimestampType") -> None:
        self._series_name = series_name
        self._timestamp_type = timestamp_type

    @builtins.property
    def series_name(self) -> str:
        return self._series_name

    @builtins.property
    def timestamp_type(self) -> "ingest_api_TimestampType":
        return self._timestamp_type


ingest_api_TimestampMetadata.__name__ = "TimestampMetadata"
ingest_api_TimestampMetadata.__qualname__ = "TimestampMetadata"
ingest_api_TimestampMetadata.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TimestampType(ConjureUnionType):
    _relative: Optional["ingest_api_RelativeTimestamp"] = None
    _absolute: Optional["ingest_api_AbsoluteTimestamp"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'relative': ConjureFieldDefinition('relative', ingest_api_RelativeTimestamp),
            'absolute': ConjureFieldDefinition('absolute', ingest_api_AbsoluteTimestamp)
        }

    def __init__(
            self,
            relative: Optional["ingest_api_RelativeTimestamp"] = None,
            absolute: Optional["ingest_api_AbsoluteTimestamp"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (relative is not None) + (absolute is not None) != 1:
                raise ValueError('a union must contain a single member')

            if relative is not None:
                self._relative = relative
                self._type = 'relative'
            if absolute is not None:
                self._absolute = absolute
                self._type = 'absolute'

        elif type_of_union == 'relative':
            if relative is None:
                raise ValueError('a union value must not be None')
            self._relative = relative
            self._type = 'relative'
        elif type_of_union == 'absolute':
            if absolute is None:
                raise ValueError('a union value must not be None')
            self._absolute = absolute
            self._type = 'absolute'

    @builtins.property
    def relative(self) -> Optional["ingest_api_RelativeTimestamp"]:
        return self._relative

    @builtins.property
    def absolute(self) -> Optional["ingest_api_AbsoluteTimestamp"]:
        return self._absolute

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_TimestampTypeVisitor):
            raise ValueError('{} is not an instance of ingest_api_TimestampTypeVisitor'.format(visitor.__class__.__name__))
        if self._type == 'relative' and self.relative is not None:
            return visitor._relative(self.relative)
        if self._type == 'absolute' and self.absolute is not None:
            return visitor._absolute(self.absolute)


ingest_api_TimestampType.__name__ = "TimestampType"
ingest_api_TimestampType.__qualname__ = "TimestampType"
ingest_api_TimestampType.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TimestampTypeVisitor:

    @abstractmethod
    def _relative(self, relative: "ingest_api_RelativeTimestamp") -> Any:
        pass

    @abstractmethod
    def _absolute(self, absolute: "ingest_api_AbsoluteTimestamp") -> Any:
        pass


ingest_api_TimestampTypeVisitor.__name__ = "TimestampTypeVisitor"
ingest_api_TimestampTypeVisitor.__qualname__ = "TimestampTypeVisitor"
ingest_api_TimestampTypeVisitor.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TriggerIngest(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'source': ConjureFieldDefinition('source', ingest_api_IngestSource),
            'properties': ConjureFieldDefinition('properties', Dict[ingest_api_PropertyName, ingest_api_PropertyValue]),
            'labels': ConjureFieldDefinition('labels', List[ingest_api_Label]),
            'dataset_name': ConjureFieldDefinition('datasetName', OptionalTypeWrapper[str]),
            'dataset_description': ConjureFieldDefinition('datasetDescription', OptionalTypeWrapper[str]),
            'timestamp_metadata': ConjureFieldDefinition('timestampMetadata', OptionalTypeWrapper[ingest_api_TimestampMetadata]),
            'file_type_properties': ConjureFieldDefinition('fileTypeProperties', OptionalTypeWrapper[ingest_api_FileTypeProperties]),
            'channel_config': ConjureFieldDefinition('channelConfig', OptionalTypeWrapper[ingest_api_ChannelConfig])
        }

    __slots__: List[str] = ['_source', '_properties', '_labels', '_dataset_name', '_dataset_description', '_timestamp_metadata', '_file_type_properties', '_channel_config']

    def __init__(self, labels: List[str], properties: Dict[str, str], source: "ingest_api_IngestSource", channel_config: Optional["ingest_api_ChannelConfig"] = None, dataset_description: Optional[str] = None, dataset_name: Optional[str] = None, file_type_properties: Optional["ingest_api_FileTypeProperties"] = None, timestamp_metadata: Optional["ingest_api_TimestampMetadata"] = None) -> None:
        self._source = source
        self._properties = properties
        self._labels = labels
        self._dataset_name = dataset_name
        self._dataset_description = dataset_description
        self._timestamp_metadata = timestamp_metadata
        self._file_type_properties = file_type_properties
        self._channel_config = channel_config

    @builtins.property
    def source(self) -> "ingest_api_IngestSource":
        return self._source

    @builtins.property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @builtins.property
    def labels(self) -> List[str]:
        return self._labels

    @builtins.property
    def dataset_name(self) -> Optional[str]:
        return self._dataset_name

    @builtins.property
    def dataset_description(self) -> Optional[str]:
        return self._dataset_description

    @builtins.property
    def timestamp_metadata(self) -> Optional["ingest_api_TimestampMetadata"]:
        return self._timestamp_metadata

    @builtins.property
    def file_type_properties(self) -> Optional["ingest_api_FileTypeProperties"]:
        return self._file_type_properties

    @builtins.property
    def channel_config(self) -> Optional["ingest_api_ChannelConfig"]:
        """
        If absent, will default to a channel config that constructs a prefix tree with `.` as the delimiter.
        """
        return self._channel_config


ingest_api_TriggerIngest.__name__ = "TriggerIngest"
ingest_api_TriggerIngest.__qualname__ = "TriggerIngest"
ingest_api_TriggerIngest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_TriggeredIngest(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'dataset_rid': ConjureFieldDefinition('datasetRid', str),
            'async_handle': ConjureFieldDefinition('asyncHandle', OptionalTypeWrapper[ingest_api_AsyncHandle])
        }

    __slots__: List[str] = ['_dataset_rid', '_async_handle']

    def __init__(self, dataset_rid: str, async_handle: Optional["ingest_api_AsyncHandle"] = None) -> None:
        self._dataset_rid = dataset_rid
        self._async_handle = async_handle

    @builtins.property
    def dataset_rid(self) -> str:
        return self._dataset_rid

    @builtins.property
    def async_handle(self) -> Optional["ingest_api_AsyncHandle"]:
        return self._async_handle


ingest_api_TriggeredIngest.__name__ = "TriggeredIngest"
ingest_api_TriggeredIngest.__qualname__ = "TriggeredIngest"
ingest_api_TriggeredIngest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_UtcTimestamp(ConjureBeanType):

    @builtins.classmethod
    def _fields(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'seconds_since_epoch': ConjureFieldDefinition('secondsSinceEpoch', int),
            'offset_nanoseconds': ConjureFieldDefinition('offsetNanoseconds', OptionalTypeWrapper[int])
        }

    __slots__: List[str] = ['_seconds_since_epoch', '_offset_nanoseconds']

    def __init__(self, seconds_since_epoch: int, offset_nanoseconds: Optional[int] = None) -> None:
        self._seconds_since_epoch = seconds_since_epoch
        self._offset_nanoseconds = offset_nanoseconds

    @builtins.property
    def seconds_since_epoch(self) -> int:
        return self._seconds_since_epoch

    @builtins.property
    def offset_nanoseconds(self) -> Optional[int]:
        return self._offset_nanoseconds


ingest_api_UtcTimestamp.__name__ = "UtcTimestamp"
ingest_api_UtcTimestamp.__qualname__ = "UtcTimestamp"
ingest_api_UtcTimestamp.__module__ = "ingest_service_api.ingest_api"


class ingest_api_VideoTimestampManifest(ConjureUnionType):
    _no_manifest: Optional["ingest_api_NoTimestampManifest"] = None
    _timestamp_manifests: Optional["ingest_api_TimestampManifest"] = None

    @builtins.classmethod
    def _options(cls) -> Dict[str, ConjureFieldDefinition]:
        return {
            'no_manifest': ConjureFieldDefinition('noManifest', ingest_api_NoTimestampManifest),
            'timestamp_manifests': ConjureFieldDefinition('timestampManifests', ingest_api_TimestampManifest)
        }

    def __init__(
            self,
            no_manifest: Optional["ingest_api_NoTimestampManifest"] = None,
            timestamp_manifests: Optional["ingest_api_TimestampManifest"] = None,
            type_of_union: Optional[str] = None
            ) -> None:
        if type_of_union is None:
            if (no_manifest is not None) + (timestamp_manifests is not None) != 1:
                raise ValueError('a union must contain a single member')

            if no_manifest is not None:
                self._no_manifest = no_manifest
                self._type = 'noManifest'
            if timestamp_manifests is not None:
                self._timestamp_manifests = timestamp_manifests
                self._type = 'timestampManifests'

        elif type_of_union == 'noManifest':
            if no_manifest is None:
                raise ValueError('a union value must not be None')
            self._no_manifest = no_manifest
            self._type = 'noManifest'
        elif type_of_union == 'timestampManifests':
            if timestamp_manifests is None:
                raise ValueError('a union value must not be None')
            self._timestamp_manifests = timestamp_manifests
            self._type = 'timestampManifests'

    @builtins.property
    def no_manifest(self) -> Optional["ingest_api_NoTimestampManifest"]:
        return self._no_manifest

    @builtins.property
    def timestamp_manifests(self) -> Optional["ingest_api_TimestampManifest"]:
        return self._timestamp_manifests

    def accept(self, visitor) -> Any:
        if not isinstance(visitor, ingest_api_VideoTimestampManifestVisitor):
            raise ValueError('{} is not an instance of ingest_api_VideoTimestampManifestVisitor'.format(visitor.__class__.__name__))
        if self._type == 'noManifest' and self.no_manifest is not None:
            return visitor._no_manifest(self.no_manifest)
        if self._type == 'timestampManifests' and self.timestamp_manifests is not None:
            return visitor._timestamp_manifests(self.timestamp_manifests)


ingest_api_VideoTimestampManifest.__name__ = "VideoTimestampManifest"
ingest_api_VideoTimestampManifest.__qualname__ = "VideoTimestampManifest"
ingest_api_VideoTimestampManifest.__module__ = "ingest_service_api.ingest_api"


class ingest_api_VideoTimestampManifestVisitor:

    @abstractmethod
    def _no_manifest(self, no_manifest: "ingest_api_NoTimestampManifest") -> Any:
        pass

    @abstractmethod
    def _timestamp_manifests(self, timestamp_manifests: "ingest_api_TimestampManifest") -> Any:
        pass


ingest_api_VideoTimestampManifestVisitor.__name__ = "VideoTimestampManifestVisitor"
ingest_api_VideoTimestampManifestVisitor.__qualname__ = "VideoTimestampManifestVisitor"
ingest_api_VideoTimestampManifestVisitor.__module__ = "ingest_service_api.ingest_api"


class upload_api_UploadService(Service):
    """
    The Upload Service manages file uploads to S3.
    """

    def initiate_multipart_upload(self, auth_header: str, upload_request: "ingest_api_InitiateMultipartUploadRequest") -> "ingest_api_InitiateMultipartUploadResponse":
        """
        Initiates a multipart upload to S3.
Does not directly upload any parts, but returns an uploadId used to upload parts.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
        }

        _path_params: Dict[str, Any] = {
        }

        _json: Any = ConjureEncoder().default(upload_request)

        _path = '/upload/v1/multipart-upload'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_InitiateMultipartUploadResponse, self._return_none_for_unknown_union_types)

    def list_parts(self, auth_header: str, key: str, upload_id: str) -> List["ingest_api_PartWithSize"]:
        """
        Lists the parts that have been uploaded for a given uploadId.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
            'key': key,
        }

        _path_params: Dict[str, Any] = {
            'uploadId': upload_id,
        }

        _json: Any = None

        _path = '/upload/v1/multipart-upload/{uploadId}'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'GET',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), List[ingest_api_PartWithSize], self._return_none_for_unknown_union_types)

    def sign_part(self, auth_header: str, key: str, part_number: int, upload_id: str) -> "ingest_api_SignPartResponse":
        """
        Signs an upload request for a single part.
Returns a URL that will execute the upload without further authentication.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
            'key': key,
            'partNumber': part_number,
        }

        _path_params: Dict[str, Any] = {
            'uploadId': upload_id,
        }

        _json: Any = None

        _path = '/upload/v1/multipart-upload/{uploadId}'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_SignPartResponse, self._return_none_for_unknown_union_types)

    def complete_multipart_upload(self, auth_header: str, key: str, upload_id: str, parts: List["ingest_api_Part"] = None) -> "ingest_api_CompleteMultipartUploadResponse":
        """
        Completes a multipart upload to S3. This should be called after all parts have been uploaded.
        """
        parts = parts if parts is not None else []

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
            'key': key,
        }

        _path_params: Dict[str, Any] = {
            'uploadId': upload_id,
        }

        _json: Any = ConjureEncoder().default(parts)

        _path = '/upload/v1/multipart-upload/{uploadId}/complete'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_CompleteMultipartUploadResponse, self._return_none_for_unknown_union_types)

    def abort_multipart_upload(self, auth_header: str, key: str, upload_id: str) -> str:
        """
        Aborts a multipart upload to S3.
Frees storage used by previously uploaded parts and prevents further uploads to the same uploadId.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
            'key': key,
        }

        _path_params: Dict[str, Any] = {
            'uploadId': upload_id,
        }

        _json: Any = None

        _path = '/upload/v1/multipart-upload/{uploadId}/abort'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            json=_json)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_Ignored, self._return_none_for_unknown_union_types)

    def upload_file(self, auth_header: str, body: Any, file_name: str, size_bytes: Optional[int] = None) -> str:
        """
        Uploads a file to S3. Intended for smaller files.
        """

        _headers: Dict[str, Any] = {
            'Accept': 'application/json',
            'Content-Type': 'application/octet-stream',
            'Authorization': auth_header,
        }

        _params: Dict[str, Any] = {
            'fileName': file_name,
            'sizeBytes': size_bytes,
        }

        _path_params: Dict[str, Any] = {
        }

        _data: Any = body

        _path = '/upload/v1/upload-file'
        _path = _path.format(**_path_params)

        _response: Response = self._request(
            'POST',
            self._uri + _path,
            params=_params,
            headers=_headers,
            data=_data)

        _decoder = ConjureDecoder()
        return _decoder.decode(_response.json(), ingest_api_S3Path, self._return_none_for_unknown_union_types)


upload_api_UploadService.__name__ = "UploadService"
upload_api_UploadService.__qualname__ = "UploadService"
upload_api_UploadService.__module__ = "ingest_service_api.upload_api"


ingest_api_RunRid = str

ingest_api_Label = str

ingest_api_PropertyName = str

ingest_api_DataSourceRefName = str

ingest_api_PropertyValue = str

ingest_api_Ignored = str

ingest_api_S3Path = str

ingest_api_ErrorType = str

ingest_api_DataSourceRid = str

