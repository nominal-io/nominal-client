import chdb
from chdb import session as chs
import polars as pl

import pandas as pd
import pyarrow as pa
from uuid import uuid4
import matplotlib.pyplot as plt

import os
import requests
from requests.utils import CaseInsensitiveDict
from conjure_python_client import ServiceConfiguration
from functools import cache

import keyring as kr

from ._utils import create_service
from .cloud import get_base_url


TOKEN = kr.get_password("Nominal API", "python-client")

# wrappers for conjure service apis

from _api.scout_catalog import CatalogService
catalog = create_service(CatalogService, get_base_url())

from _api.timeseries_logicalseries import LogicalSeriesService
logical_series = create_service(LogicalSeriesService, get_base_url())

from _api.timeseries_seriescache import SeriesCacheService
series_cache = create_service(SeriesCacheService, get_base_url())

from _api.scout import RunService
runs = create_service(RunService, get_base_url())

def _create_dataset(filename: str, bucket: str, key: str, ts_name: str):
    from _api.scout_catalog import (
        TimestampType, CreateDataset, Handle, S3Handle, DatasetOriginMetadata, TimestampMetadata, AbsoluteTimestamp, Iso8601Timestamp
    )
    return catalog.create_or_update_dataset(TOKEN, CreateDataset(
        name=filename,
        handle=Handle(s3=S3Handle(bucket, key)),
        labels=[],
        metadata={},
        origin_metadata=DatasetOriginMetadata(
            timestamp_metadata=TimestampMetadata(
                series_name=ts_name,
                timestamp_type=TimestampType(absolute=AbsoluteTimestamp(iso8601=Iso8601Timestamp()))
            )
        ),
        properties={})
    )


from _api.timeseries_logicalseries_api import BatchCreateLogicalSeriesRequest, CreateLogicalSeries
from _api.timeseries_seriescache_api import CachedSeries, CreateCachedSeriesRequest
def _create_logical_series(requests: list[CreateLogicalSeries]):
    return logical_series.batch_create_logical_series(
        TOKEN,
        BatchCreateLogicalSeriesRequest(requests=requests))

def _create_run(name: str, datasets: list["Dataset"], start_time: int, end_time: int):
    from _api.scout_run_api import (
        CreateRunRequest, UtcTimestamp,
        DataSourceRefName, CreateRunDataSource,
        DataSource
    )
    data_sources = [
        (ds.filename, CreateRunDataSource(
            data_source=DataSource(dataset=ds.dataset_rid),
            series_tags={}
        ))
        for ds in datasets
    ]
    return runs.create_run(TOKEN, CreateRunRequest(
        title=name,
        start_time=UtcTimestamp(seconds_since_epoch=start_time),
        end_time=UtcTimestamp(seconds_since_epoch=end_time),
        data_sources=dict(data_sources),
        attachments=[],
        description="",
        properties={},
        labels=[],
        links=[]
    ))

def _complete_ingest(dataset_rid: str, start_epoch: int, end_epoch: int, logical_series):
    from _api.scout_catalog import UpdateBoundsRequest, Bounds, UpdateIngestStatusV2, IngestStatusV2, SuccessResult
    from _api.datasource import Timestamp as ds_Timestamp, TimestampType as ds_TimestampType
    from _api.timeseries_seriescache_api import Timestamp as cache_Timestamp
    print("updating bounds")
    catalog.update_bounds(TOKEN, UpdateBoundsRequest(bounds=Bounds(
        start=ds_Timestamp(seconds=start_epoch, nanos=0),
        end=ds_Timestamp(seconds=end_epoch, nanos=0),
        type=ds_TimestampType.ABSOLUTE
    )), dataset_rid)

    from _api.timeseries_seriescache_api import CreateCachedSeriesRequest, CachedSeries
    cache_reqs = []
    for ser in logical_series.responses:
        cache_reqs.append(CachedSeries(
            logical_series_rid=ser.rid,
            start_timestamp=cache_Timestamp(seconds=start_epoch, nanos=0),
            end_timestamp=cache_Timestamp(seconds=end_epoch, nanos=0),
            series_data_type=ser.series_data_type
        ))
    print("creating cached series")
    series_cache.create_cached_series(TOKEN, CreateCachedSeriesRequest(series_to_cache=cache_reqs))
    print("updating ingest status")
    catalog.update_dataset_ingest_status_v2(TOKEN, UpdateIngestStatusV2(
        status=IngestStatusV2(success=SuccessResult()),dataset_uuid=dataset_rid.split(".")[-1]
    ))


class Dataset:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        print("Reading dataset schema...", flush=True)
        self.df = pl.scan_csv(csv_path, try_parse_dates=True)
        self.uploaded = False
        # assume ts col is first col for now
        self.ts_col = next(iter(self.df.collect_schema().keys()))
        print(f"Using {self.ts_col} as timeseries column", flush=True)
        self._init_sess()
        self._ingest()

    def _repr_html_(self, *args, **kwargs):
        return self.df.collect()._repr_html_(*args, **kwargs)

    def _ingest(self):
        print(f"Reading file {self.csv_path} into clickhouse", flush=True)
        create_res = self.sess.query(f"""
        CREATE TABLE nominal.tmp_csv ENGINE=Log() AS SELECT * FROM file('{self.csv_path}', 'CSVWithNames')
        """)
        csv_rows = int(str(self.sess.query(f"SELECT count(*) FROM nominal.tmp_csv")).strip())
        print(f"Read {csv_rows} into temporary table", flush=True)
        self.col_series = {}
        query_str = ""
        for col in self.df.collect_schema():
            # don't ingest the timestamp
            if col == self.ts_col:
                continue
            series_id = str(uuid4())
            tbl = 'dataset_string'
            func = 'toString'
            col_type = 'string'
            if self.df.collect_schema().get(col).is_numeric():
                tbl = 'dataset_float64'
                func = 'toFloat64'
                col_type = 'float64'

            self.col_series[col] = (series_id, col_type)

            query_str += f"""INSERT INTO nominal.{tbl} SELECT
            '{series_id}' AS series, parseDateTime64BestEffort({self.ts_col}, 3) AS timestamp,
            {func}({col}) AS value FROM nominal.tmp_csv;
            """
        print("Ingesting...", flush=True)
        self.sess.query(query_str)
        print("Ingestion complete!", flush=True)

        self.sess.query("DROP TABLE nominal.tmp_csv")

    def schema(self):
        return self.df.collect_schema()

    def sync(self):
        if self.uploaded:
            print(f"dataset {self.dataset_rid} already created", flush=True)
        self._upload_file_s3()
        if not hasattr(self, 'dataset_rid'):
            s3_split = self.s3_path.replace("s3://", "").split("/")
            dataset = _create_dataset(
                self.filename,
                s3_split[0],
                "/".join(s3_split[1:]),
                self.ts_col)
            self.dataset_rid = dataset.rid

        print(f'dataset rid: {self.dataset_rid}')

        if not hasattr(self, 'logical_series'):
            print("creating logical series")
            def _req(idx, series_id, series_name, series_type):
                from _api.timeseries_logicalseries_api import (
                    SeriesDataType, Channel, Locator, CsvLocatorV2, DataSourceRid
                )
                t = SeriesDataType.DOUBLE
                if series_type == "string":
                    t = SeriesDataType.STRING

                return CreateLogicalSeries(
                    channel=Channel(series_name),
                    locator=Locator(
                        csv_v2=CsvLocatorV2(
                            s3_path=self.s3_path,
                            index=idx,
                            time_index=0
                        )),
                    id_locator=series_id,
                    data_source_rid=DataSourceRid(self.dataset_rid),
                    series_data_type=t)
            i = 1
            reqs = []
            for (series_name, (series_id, series_type)) in ds.col_series.items():
                reqs.append(_req(i, series_id, series_name, series_type))
                i = i + 1
            series = _create_logical_series(reqs)
            print("created logical series")
            self.logical_series = series

        print(f'created {len(self.logical_series.responses)} series', flush=True)

        # TODO: replace this with safer ingest / insert workflow
        if not hasattr(self, 'remote_insert'):
            for series_type in ["string", "float64"]:
                remote_func = remote_tbl_fmt % series_type
                print(f"inserting all {series_type} columns into remote ch")
                self.sess.query(f"""
                INSERT INTO FUNCTION
                    {remote_func}
                SELECT * FROM nominal.dataset_{series_type}
                """)
            self.remote_insert = True

        print("inserted data into remote")

        if not self.uploaded:
            (ts_min, ts_max) = self._bounds()

            _complete_ingest(self.dataset_rid, ts_min, ts_max, self.logical_series)
            self.uploaded = True
            print("completed ingest")

    @cache
    def _bounds(self):
        bounds = self.df.select(
                pl.min("source_time").dt.epoch("s").alias("min"),
                pl.max("source_time").dt.epoch("s").alias("max")).collect()
        return (bounds["min"].item(), bounds["max"].item())

    # todo - merge this with the existing _upload_s3 function, it's mostly copy-pasted wholesale
    def _upload_file_s3(self):
        if hasattr(self, 's3_path'):
            print(f"already uploaded dataset {self.csv_path} to {self.s3_path}")
            return

        self.filename = os.path.basename(self.csv_path)

        csv_file = open(self.csv_path, 'rb')
        csv_buffer_size_bytes = os.stat(self.csv_path).st_size

        print(
            "\nUploading: [bold green]{0}[/bold green]\nto {1}\n = {2} bytes".format(
                self.filename, get_base_url, csv_buffer_size_bytes
            )
        )

        # Make POST request to upload data file to S3
        resp = requests.post(
            url=f"{get_base_url}/upload/v1/upload-file?fileName={self.filename}",
            data=csv_file.read(),
            params={"sizeBytes": csv_buffer_size_bytes},
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/octet-stream"
            },
        )

        if resp.status_code == 200:
            self.s3_path = resp.text.strip('"')
            print("\nUpload to S3 successful. S3 bucket:", self.s3_path)
        else:
            print("\n{0} error during upload to S3:\n".format(resp.status_code), resp.json())

    def _init_sess(self):
        if hasattr(self, 'sess'):
            return
        self.sess = chs.Session()
        self.sess.query("CREATE DATABASE IF NOT EXISTS nominal")
        self.sess.query("""
        CREATE TABLE IF NOT EXISTS nominal.dataset_float64 (series String,
        timestamp DateTime64(9) CODEC(Delta, ZSTD), value Float64)
        ENGINE=ReplacingMergeTree() ORDER BY (series, timestamp)
        """)
        self.sess.query("""
        CREATE TABLE IF NOT EXISTS nominal.dataset_string (series String,
        timestamp DateTime64(9) CODEC(Delta, ZSTD), value String)
        ENGINE=ReplacingMergeTree() ORDER BY (series, timestamp)
        """)

    def run(self, name: str):
        (ts_min, ts_max) = self._bounds()
        return Run(name, [self], ts_min, ts_max)

    def _series(self, series: str):
        if series not in self.col_series:
            return None
        return self.col_series[series]


class Run:
    def __init__(self, name: str, sources: list[Dataset], start: int, end: int):
        self.name = name
        self.sources = sources
        self.start = start
        self.end = end

    def series(self, series: str):
        for ds in self.sources:
            maybe_ser = ds._series(series)
            if maybe_ser:
                series_id, series_type = maybe_ser
                return Series.from_ch(self, ds.sess, series, series_id, series_type)

    def sync(self):
        if hasattr(self, 'run_rid'):
            return

        (ts_min, ts_max) = self.sources[0]._bounds()
        for ds in self.sources:
            ds.sync()
            ds_min, ds_max = ds._bounds()
            ts_min = min(ts_min, ds_min)
            ts_max = max(ts_max, ds_max)

        print(f"creating run with {len(self.sources)} datasources", flush=True)

        resp = _create_run(self.name, self.sources, ts_min, ts_max)
        print(f"created run {resp.rid}")
        self.run_rid = resp.rid

from _api.scout_compute_api import (
    RawNumericSeriesNode,
    DerivativeSeriesNode,
    ScaleSeriesNode,
    UnaryArithmeticSeriesNode,
    UnaryArithmeticOperation,
    TimeUnit
)

class Series:
    @staticmethod
    def from_ch(run: Run, sess, name: str, series_id: str, series_type: str) -> "Series":
        base_query = f"SELECT timestamp, value FROM nominal.dataset_{series_type} WHERE series='{series_id}'"
        return Series(run, sess, name, base_query, RawNumericSeriesNode(name=name))

    def __init__(self, run: Run, sess: chs.Session, name: str, query: str, conjure):
        self.run = run
        self.sess = sess
        self.name = name
        self.base_query = query
        self.conjure = conjure

    def _with_transform(self, name: str, query: str, conjure):
        return Series(self.run, self.sess, name, query, conjure)

    def __repr__(self):
        return f"{self.name} - {self.count()} rows"

    @cache
    def pandas(self):
        res = self.sess.query(self.base_query, "Arrow")
        raw_arrow = pa.RecordBatchFileReader(res.bytes()).read_all()
        return raw_arrow.to_pandas()

    @cache
    def count(self):
        res = self.sess.query(f"SELECT count(*) FROM ({self.base_query})")
        return int(str(res).strip())

    def preview(self):
        df = self.pandas()
        plt.figure(figsize=(10,6))
        plt.plot(df['timestamp'], df['value'], marker='o')

    def derivative(self, time_unit: TimeUnit = TimeUnit.SECONDS):
        conversion_factors = {
            TimeUnit.DAYS: 24 * 60 * 60 * 1_000_000_000,
            TimeUnit.HOURS: 60 * 60 * 1_000_000_000,
            TimeUnit.MINUTES: 60 * 1_000_000_000,
            TimeUnit.SECONDS: 1_000_000_000,
            TimeUnit.MILLISECONDS: 1_000_000,
            TimeUnit.MICROSECONDS: 1_000,
            TimeUnit.NANOSECONDS: 1,
        }
        time_unit_nanos = conversion_factors[time_unit]

        query = f"""
        SELECT timestamp, value, rowNumberInAllBlocks() as row_num FROM (
            SELECT timestamp,
                   (value - lagInFrame(value, 1, NAN) OVER (ORDER BY timestamp)) /
                   ((toUnixTimestamp64Nano(timestamp) - toUnixTimestamp64Nano(lagInFrame(timestamp, 1)
                   OVER (ORDER BY timestamp))) / {time_unit_nanos}) AS value
            FROM ({self.base_query})
        ) WHERE row_num > 0
        """
        conjure = DerivativeSeriesNode(input=self.conjure, time_unit=TimeUnit.NANOSECONDS)
        return self._with_transform(f"derivative({self.name}, {time_unit_nanos})", query, conjure)

    def scale(self, scalar: float):
        query = f"""
        SELECT timestamp, value * {scalar} AS value FROM ({self.base_query})
        """
        conjure = ScaleSeriesNode(input=self.conjure, scalar=scalar)
        return self._with_transform(f"scale({self.name}, {scalar})", query, conjure)

    def unary(self, op: UnaryArithmeticOperation):
        op_sql = op.value.lower()
        query = f"SELECT timestamp, {op_sql}(value) AS value FROM ({self.base_query})"
        conjure = UnaryArithmeticSeriesNode(input=self.conjure, operation=op)
        return self._with_transform(f"{op_sql}({self.name})", query, conjure)

    def sync(self):
        self.run.sync()


class Chart:
    def __init__(self, name: str, *series):
        self.name = name
        self.series = series

    def show(self):
        plt.figure(figsize=(10, 6))
        for ser in self.series:
            df = ser.pandas()
            plt.plot(df['timestamp'], df['value'], label=ser.name)

        plt.xlabel('Time')
        plt.title(self.name)
        plt.legend()

        plt.grid(True)
        plt.show()

    def sync(self):
        for s in self.series:
            s.sync()

class Workbook:
    def __init__(self, name, *charts):
        self.charts = charts
        self.uploaded = False

    def preview(self):
        for chart in self.charts:
            chart.show()

    def sync(self):
        if self.uploaded:
            raise Exception("workbook already uploaded")

        for c in self.charts:
            c.sync()
        
