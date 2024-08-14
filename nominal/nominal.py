import os
import io
import copy
import requests
from dateutil import parser
import polars as pl
import keyring as kr
from datetime import datetime
import jsondiff as jd
from jsondiff import diff
from math import floor
from rich import print
from .utils import PayloadFactory, default_filename

ENDPOINTS = dict(
    file_upload = '{}/upload/v1/upload-file?fileName={}',
    dataset_upload = '{}/ingest/v1/trigger-ingest-v2',
    run_upload = '{}/ingest/v1/ingest-run',
    run_retrieve = '{}/scout/v1/run/{}', # GET
    run_update = '{}/scout/v1/run/{}' # PUT
)

BASE_URLS = dict(
    STAGING="https://api-staging.gov.nominal.io/api",
    PROD="https://api.gov.nominal.io/api",
)


def set_base_url(base_url: str = "STAGING"):
    """
    Usage:
    import nominal as nm
    nm.set_base_url('PROD')

    TODO
    ----
    Default is staging. Change to prod after beta period.
    """
    if base_url in BASE_URLS.keys():
        os.environ["NOMINAL_BASE_URL"] = BASE_URLS[base_url]
    else:
        os.environ["NOMINAL_BASE_URL"] = base_url


def get_base_url():
    if "NOMINAL_BASE_URL" not in os.environ:
        set_base_url()  # set to default
    return os.environ["NOMINAL_BASE_URL"]


def get_app_base_url():
    """
    eg, https://app-staging.gov.nominal.io

    TODO
    ----
    This won't work for custom domains
    """
    return get_base_url().rstrip("/api").replace("api", "app")


def set_token(token):
    if token is None:
        print("Retrieve your access token from [link]{0}/sandbox[/link]".format(get_base_url()))
    kr.set_password("Nominal API", "python-client", token)


class Dataset(pl.DataFrame):
    """
    Dataset inherits from Polars DataFrame for its rich display, ingestion, and wrangling capabilities.

    Parameters
    ----------
    data : various, optional
        The input data for the dataset. This can be in any format supported by Polars DataFrame.
    filename : str, optional
        The name of the dataset file. Default is None.
    overwrite : bool, optional
        A flag to indicate whether to overwrite an existing file during upload. Default is False.
    properties : dict, optional
        A dictionary of additional properties associated with the dataset. Default is an empty dictionary.
    description : str, optional
        A brief description of the dataset. Default is an empty string.

    Attributes
    ----------
    s3_path : str or None
        The S3 path where the dataset is stored after upload. Initially None.
    filename : str
        The name of the dataset file.
    properties : dict
        A dictionary of additional properties associated with the dataset.
    description : str
        A brief description of the dataset.
    rid : str or None
        The dataset's RID (Resource ID) after registration on the Nominal platform. Initially None.
    dataset_link : str
        A URL link to the dataset on the Nominal platform. Initially an empty string.

    Methods
    -------
    upload(overwrite=False)
        Uploads and registers the dataset on the Nominal platform.
    """

    def __init__(
        self, df: pl.DataFrame = None, filename: str = None, rid: str = None, properties: dict = dict(), description: str = ""
    ):
        if df is not None:
            print('DataFrame received')            
            dft = Ingest.set_ts_index(df)

        super().__init__(dft)

        self.s3_path = None
        self.filename = filename
        self.properties = properties
        self.description = description
        self.rid = rid
        self.dataset_link = ""

    def __get_headers(self, content_type: str = 'json') -> dict:
        TOKEN = kr.get_password('Nominal API', 'python-client')
        return {
            "Authorization": "Bearer {}".format(TOKEN),
            "Content-Type": "application/{0}".format(content_type),
        }

    def __upload_file(self, overwrite: bool) -> requests.Response:
        """
        Uploads dataframe to S3 as a file.

        Returns:
        Response object from the REST call.
        """

        if self.s3_path is not None and not overwrite:
            print(
                "\nThis Dataset is already uploaded to an S3 bucket:\n{0}\nTry [code]upload(overwrite = True)[/code] to overwrite it.".format(
                    self.s3_path
                )
            )
            return

        # Create a default dataset name
        if self.filename is None:
            self.filname = default_filename("DATASET")

        csv_file_buffer = io.BytesIO()
        self.write_csv(csv_file_buffer)

        # Get the size of the buffer in bytes
        csv_file_buffer.seek(0, os.SEEK_END)
        csv_buffer_size_bytes = csv_file_buffer.tell()
        csv_file_buffer.seek(0)

        print(
            "\nUploading: [bold green]{0}[/bold green]\nto {1}\n = {2} bytes".format(
                self.filename, get_base_url(), csv_buffer_size_bytes
            )
        )

        # Make POST request to upload data file to S3
        resp = requests.post(
            url=ENDPOINTS["file_upload"].format(get_base_url(), self.filename),
            data=csv_file_buffer.read(),
            params={"sizeBytes": csv_buffer_size_bytes},
            headers=self.__get_headers(content_type="octet-stream"),
        )

        if resp.status_code == 200:
            self.s3_path = resp.text.strip('"')
            print("\nUpload to S3 successful.\nS3 bucket:\n", self.s3_path)
        else:
            print("\n{0} error during upload to S3:\n".format(resp.status_code), resp.json())

        return resp

    def upload(self, overwrite: bool = False):
        """
        Registers Dataset in Nominal on Nominal platform.

        Endpoint:
        /ingest/v1/trigger-ingest-v2

        Returns:
        Response object from the REST call.
        """

        s3_upload_resp = self.__upload_file(overwrite)

        if isinstance(s3_upload_resp, dict):
            if s3_upload_resp.status_code != 200:
                print("Aborting Dataset registration")
                return

        if self.s3_path is None:
            print("Cannnot register Dataset on Nominal - Dataset.s3_path is not set")
            return

        print("\nRegistering [bold green]{0}[/bold green] on {1}".format(self.filename, get_base_url()))

        payload = dict(
            url=ENDPOINTS["dataset_upload"].format(get_base_url()),
            json=PayloadFactory.dataset_trigger_ingest(self),
            headers=self.__get_headers(),
        )

        resp = requests.post(url=payload["url"], json=payload["json"], headers=payload["headers"])

        if resp.status_code == 200:
            self.rid = resp.json()["datasetRid"]
            self.dataset_link = "{0}/data-sources/{1}".format(get_app_base_url(), self.rid)
            print("\nDataset RID: ", self.rid)
            print("\nDataset Link: ", "[link={0}]{0}[/link]\n".format(self.dataset_link))
        else:
            print("\n{0} error registering Dataset on Nominal:\n".format(resp.status_code), resp.json())

        return resp


class Ingest:
    """
    Handles ingestion of various tabular and video file formats.

    This class provides static and instance methods for ingesting data from various formats, such as CSV and Parquet files,
    and for setting a timestamp index column in the ingested data. The ingested data is returned as a `Dataset` object.

    Methods
    -------
    set_ts_index(df, ts_col)
        Sets a timestamp index for the provided DataFrame. This method adds internal columns for the datetime in Python format,
        ISO 8601 format, and Unix timestamp format.

    read_csv(path, ts_col=None)
        Reads a CSV file from the specified path and returns a `Dataset` object with a timestamp index set.

    read_parquet(path, ts_col=None)
        Reads a Parquet file from the specified path and returns a `Dataset` object with a timestamp index set.

    Notes
    -----
    TODO: Consider using Ibis for database source connectivity.
    TODO: Implement video ingest functionality.
    """

    @staticmethod
    def set_ts_index(df: pl.DataFrame, ts_col: str = None) -> pl.DataFrame:
        """
        Sets a timestamp index for the provided DataFrame.

        This method attempts to infer the timestamp column if one is not specified. It adds internal columns to the
        DataFrame: '_python_datetime' and '_unix_timestamp'. The DataFrame is then sorted by the '_python_datetime' column.

        Parameters
        ----------
        df : polars.DataFrame
            The DataFrame for which the timestamp index will be set.
        ts_col : str, optional
            The name of the column to use as the timestamp. If None, the method will attempt to infer the timestamp column.

        Returns
        -------
        polars.DataFrame
            The modified DataFrame with the timestamp index set.
        """
        if ts_col is None:
            # Infer timestamp column
            for col in df.columns:
                try:
                    dt = parser.parse(df[col][0])
                    if type(dt) is datetime:
                        ts_col = col
                        break
                except Exception:
                    pass

        if ts_col is not None:
            try:
                df.drop_in_place("_python_datetime")
                df.drop_in_place("_unix_timestamp")
            except Exception:
                pass
            datetime_series = pl.Series("_python_datetime", [parser.parse(dt_str) for dt_str in df[ts_col]])
            unix_series = pl.Series("_unix_timestamp", [dt.timestamp() for dt in datetime_series])
            df.insert_column(-1, datetime_series)
            df.insert_column(-1, unix_series)
            df = df.sort("_python_datetime")  # Datasets must be sorted in order to upload to Nominal
        else:
            print('A Dataset must have at least one column that is a timestamp.')
            print('Please specify which column is a date or datetime with the [code]ts_col[/code] parameter.')

        return df

    def read_csv(self, path: str, ts_col: str = None) -> Dataset:
        dfc = pl.read_csv(path)
        dft = self.set_ts_index(dfc, ts_col)
        return Dataset(dft, filename=os.path.basename(path))

    def read_parquet(self, path: str, ts_col: str = None) -> Dataset:
        dfp = pl.read_parquet(path)
        dft = self.set_ts_index(dfp, ts_col)
        return Dataset(dft, filename=os.path.basename(path))


class Run:
    '''
    Python representation of a Nominal Run.

    Parameters
    ----------
    path : str, optional
        A single file path to a dataset. If provided, it will be added to `paths`. Default is None.
    paths : list of str, optional
        A list of file paths to datasets. Default is an empty list.
    datasets : list of Dataset, optional
        A list of `Dataset` objects to be included in the run. Default is an empty list.
    properties : list of str, optional
        A list of properties associated with the run. Default is an empty list.
    title : str, optional
        The title of the run. Default is None, which will generate a default filename.
    description : str, optional
        A brief description of the run. Default is an empty string.
    start : str or datetime, optional
        The start time for the run. Can be a string or a datetime object. Default is None.
    end : str or datetime, optional
        The end time for the run. Can be a string or a datetime object. Default is None.

    Attributes
    ----------
    title : str
        The title of the run. Defaults to a timestamped, autogenerated filename if not provided.
    description : str
        A brief description of the run.
    properties : dict
        A dict of properties associated with the run.
    datasets : list of Dataset
        A list of `Dataset` objects associated with the run.        
    domain : dict
        A dictionary containing 'START' and 'END' time domain for the run.
    datasets_domain : dict
        A dictionary holding the overall 'START' and 'END' domain from the datasets.

    Methods
    -------
    upload()
        Uploads the run and its datasets to Nominal.
    ''' 

    def __print_human_readable_endpoint(self, endpoint):
            '''
            Print the Run datetime endpoints in a human-readable form
            '''
            print('Run {} time:'.format(endpoint))
            unix_seconds = self._domain[endpoint]['SECONDS'] + self._domain[endpoint]['NANOS']*10e9
            print('Unix: ', unix_seconds)
            datetime_endpoint = datetime.fromtimestamp(unix_seconds)
            print('Datetime: ', datetime_endpoint)

    def __setattr__(self, k: str, v) -> None:
        '''
        Convenience method to allow setting Run endpoints as human-readable strings
        '''
        if k in ['start', 'end']:
            endpoint = k.upper()
            self._domain[endpoint]['DATETIME'] = parser.parse(v)
            self.__set_run_unix_timestamp_domain([endpoint])
            self.__print_human_readable_endpoint(endpoint)
        else:
            super().__setattr__(k, v)

    def __getattr__(self, k: str) -> None:
        if k in ['start', 'end']:
            self.__print_human_readable_endpoint(k.upper())
        else:
            super().__getattr__(k)

    def __init__(self,
                 rid: str = None,
                 path: str = None,
                 paths: list[str] = [],
                 datasets: list[Dataset] = [],
                 properties: dict = {},
                 title: str = None,
                 description: str = '',               
                 start: str = None, 
                 end: str = None,
                 cloud: dict = {}):

        if title is None:
            self.title = default_filename('RUN')
        self.description = description
        self.properties = properties
        self._domain = {'START': {}, 'END': {}}

        if rid is not None:
            # Attempt to retrieve run by its resource ID (rid)
            resp = requests.get(
                headers = self.__get_headers(),
                url = ENDPOINTS['run_retrieve'].format(get_base_url(), rid)
            )
            if resp.status_code == 200:                
                self.cloud = resp.json()
                print('Cloud response:')
                print(self.cloud)
                print('... Downloaded to Run.cloud')

                # Assign Run metadata to local Run object metadata
                local_metadata = ['rid', 'description', 'title', 'start', 'end', 'properties', 'labels']
                cloud_metadata = list(self.cloud.keys())
                for md_key in local_metadata:
                    if md_key in cloud_metadata:
                        # Override local value with cloud value
                        setattr(self, md_key, self.cloud[md_key])
                    elif md_key == 'start':
                        self._domain['START']['SECONDS'] = self.cloud['startTime']['secondsSinceEpoch']
                        self._domain['START']['NANOS'] = self.cloud['startTime']['offsetNanoseconds']
                    elif md_key == 'end':
                        self._domain['END']['SECONDS'] = self.cloud['endTime']['secondsSinceEpoch']
                        self._domain['END']['NANOS'] = self.cloud['endTime']['offsetNanoseconds']                        
            else:
                print('There was an error retrieving Run with rid = {0}'.format(rid))
                print('Make sure that your rid is correct and from [link]{0}[/link]'.format(get_app_base_url()))
                print(resp.json())
            return

        if path is not None:
            paths = [path]

        if len(paths) == 0 and len(datasets) == 0:
            print("Please provide a list of Datasets or list of paths for this Run")
            return

        if len(paths) > 0:
            self.datasets = [Ingest().read_csv(fp) for fp in paths]
        else:
            self.datasets = datasets

        mins = []
        maxs = []
        for ds in self.datasets:
            mins.append(ds["_python_datetime"].min())
            maxs.append(ds["_python_datetime"].max())
        self.datasets_domain = dict(START=min(mins), END=max(maxs))

        self.__set_run_datetime_boundary('START', start)
        self.__set_run_datetime_boundary('END', end)
        self.__set_run_unix_timestamp_domain()

    def __set_run_datetime_boundary(self, key: str, str_datetime: any):
        '''
        Set start & end boundary variables for Run
        '''
        if str_datetime is None:
            self._domain[key]['DATETIME'] = self.datasets_domain[key]
        elif type(str_datetime) is datetime:
            self._domain[key]['DATETIME'] = str_datetime
        elif type(str_datetime) is str:
            self._domain[key]['DATETIME'] = parser.parse(str_datetime)

    def __set_run_unix_timestamp_domain(self, endpoints = ['START', 'END']):
        '''
        Set start & end boundary variables for Run
        '''
        for key in endpoints:
            dt = self._domain[key]['DATETIME']
            unix = dt.timestamp()
            seconds = floor(unix)
            self._domain[key]['SECONDS'] = seconds
            self._domain[key]['NANOS'] = floor((unix - seconds) / 1e9)

    def __get_headers(self, content_type: str = "json") -> dict:
        TOKEN = kr.get_password("Nominal API", "python-client")
        return {
            "Authorization": "Bearer {}".format(TOKEN),
            "Content-Type": "application/{0}".format(content_type),
        }

    def diff(self):
        '''
        Compare local and cloud Run instances
        '''
        if self.cloud is None:
            print('No Run instance has been downloaded from the cloud')
            print('Download a run with [code]r = Run(rid = RID)[/code]')            
            return

        local_copy = PayloadFactory.run_upload(self)
        cloud_copy = copy.deepcopy(self.cloud)

        # rm datasources - we're not comparing those
        del cloud_copy['dataSources']
        del local_copy['dataSources']

        def rm_deletions_and_datasources(rd):
            if jd.delete in rd:
                del rd[jd.delete]
        
        run_diff_labeled = diff(cloud_copy, local_copy, syntax='explicit')
        rm_deletions_and_datasources(run_diff_labeled)
        print(run_diff_labeled)

        run_diff_unlabeled = diff(cloud_copy, local_copy)
        rm_deletions_and_datasources(run_diff_unlabeled)
        return run_diff_unlabeled

    def update(self):
        '''
        Updating run metadata is done in 4 steps:
        1.  Download a Run: r = Run(rid = RID)
        2.  Update something about the Run: r.title = 'Runs with Friends'
        3.  [Optional] Inspect a diff between the cloud and local versions: r.diff()
        4.  r.update()
        By design, no changes are synced with the cloud without an explicit call to update()
        At the moment, only Run start, end, and metadata can be updated (not datasources)
        '''

        if self.rid is None or self.cloud is None:
            print('No Run instance has been downloaded from the cloud')
            print('Download a run with [code]r = Run(rid = RID)[/code]')             

        rd = self.diff() # rd = "run diff"
        if len(rd) == 0:
            print('No difference between Run.cloud and the local Run instance')
            return

        # Make PUT request to update Run
        resp = requests.put(
                    url = ENDPOINTS['run_update'].format(get_base_url(), self.rid),
                    json = rd,
                    headers = self.__get_headers(),
                )
        
        if resp.status_code == 200:
            self.cloud = resp.json()
            print('\nUpdated Run on Nominal:')
            print('[link]{0}/runs/{1}[/link]'.format(get_app_base_url(), self.cloud['runNumber']))
        else:
            print('\n{0} error updating Run on Nominal:\n'.format(resp.status_code), resp.json())        

    def upload(self) -> requests.Response:
        """
        Uploads the run and its datasets to Nominal.

        Returns
        -------
        requests.Response
            The response object from the REST call.
        """
        datasets_payload = dict()

        for ds in self.datasets:
            # First, check if Run Datasets have been uploaded to S3
            if ds.s3_path is None:
                ds.upload()
            datasets_payload[ds.filename] = PayloadFactory.create_unix_datasource(ds)

        run_payload = PayloadFactory.run_upload(self, datasets_payload)

        # Make POST request to register Run and Datasets on Nominal
        resp = requests.post(
                    url = ENDPOINTS['run_upload'].format(get_base_url()),
                    json = run_payload,
                    headers = self.__get_headers(),
                )
        
        self.last_upload_payload = run_payload

        if resp.status_code == 200:
            self.rid = resp.json()["runRid"]
            print("\nRun RID: ", self.rid)
        else:
            print("\n{0} error registering Run on Nominal:\n".format(resp.status_code), resp.json())

        return resp
