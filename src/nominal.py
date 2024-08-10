import os
import io
import requests
from dateutil import parser
import polars as pl
import keyring as kr 
from datetime import datetime
from math import floor
from rich import print
from utils import default_filename, PayloadFactory


ENDPOINTS = dict(
    file_upload = '{}/upload/v1/upload-file?fileName={}',
    dataset_upload = '{}/ingest/v1/trigger-ingest-v2',
    run_upload = '{}/ingest/v1/ingest-run'
)

def get_api_domain():
    # TODO store in env variable and make settable
    return 'https://api-staging.gov.nominal.io/api'

def set_token(token):
    if token is None:
        print('Retrieve your access token from https://app-staging.gov.nominal.io/sandbox')
    kr.set_password('Nominal API', 'python-client', token)

class Dataset(pl.DataFrame):
    '''
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
    '''

    def __init__(self, 
                 data: any = None, 
                 filename: str = None, 
                 overwrite: bool = False, 
                 properties: dict = dict(), 
                 description: str = ''):
        super().__init__(data)

        self.s3_path = None
        self.filename = filename
        self.properties = properties
        self.description = description
        self.rid = None
        self.dataset_link = ''

    def __get_headers(self, content_type: str = 'json') -> dict:
        TOKEN = kr.get_password('Nominal API', 'python-client')
        return {
            "Authorization": "Bearer {}".format(TOKEN),
            "Content-Type": "application/{0}".format(content_type),
        }

    def __upload_file(self, overwrite: bool) -> requests.Response:
        '''
        Uploads dataframe to S3 as a file.
        
        Returns:
        Response object from the REST call.
        '''

        if self.s3_path is not None and not overwrite:
            print('\nThis Dataset is already uploaded to an S3 bucket:\n{0}\nTry [code]upload(overwrite = True)[/code] to overwrite it.'.format(self.s3_path))
            return

        # Create a default dataset name
        if self.filename is None:
            self.filname = default_filename('DATASET')

        csv_file_buffer = io.BytesIO()
        self.write_csv(csv_file_buffer)

        # Get the size of the buffer in bytes
        csv_file_buffer.seek(0, os.SEEK_END)
        csv_buffer_size_bytes = csv_file_buffer.tell()
        csv_file_buffer.seek(0)

        print('\nUploading: [bold green]{0}[/bold green]\nto {1}\n = {2} bytes'
              .format(self.filename, get_api_domain(), csv_buffer_size_bytes))

        # Make POST request to upload data file to S3
        resp = requests.post(
                    url = ENDPOINTS['file_upload'].format(get_api_domain(), self.filename),
                    data = csv_file_buffer.read(),
                    params = {"sizeBytes": csv_buffer_size_bytes},
                    headers = self.__get_headers(content_type = 'octet-stream'),
                )

        if resp.status_code == 200:
            self.s3_path = resp.text.strip('"')
            print('\nUpload to S3 successful.\nS3 bucket:\n', self.s3_path)
        else:
            print('\n{0} error during upload to S3:\n'.format(resp.status_code), resp.json())

        return resp

    def upload(self, overwrite: bool = False):        
        '''
        Registers Dataset in Nominal on Nominal platform.

        Endpoint:
        /ingest/v1/trigger-ingest-v2

        Returns:
        Response object from the REST call.
        '''

        s3_upload_resp = self.__upload_file(overwrite)

        if isinstance(s3_upload_resp, dict):
            if s3_upload_resp.status_code != 200:
                print('Aborting Dataset registration')
                return

        if self.s3_path is None:
            print('Cannnot register Dataset on Nominal - Dataset.s3_path is not set')
            return

        print('\nRegistering [bold green]{0}[/bold green] on {1}'.format(self.filename, get_api_domain()))

        payload = dict(
            url = ENDPOINTS['dataset_upload'].format(get_api_domain()),
            json = PayloadFactory.dataset_trigger_ingest(self),
            headers = self.__get_headers()
        )

        resp = requests.post(url = payload['url'], json = payload['json'], headers = payload['headers'])

        if resp.status_code == 200:
            self.rid = resp.json()['datasetRid']
            self.dataset_link = 'https://app-staging.gov.nominal.io/data-sources/{0}'.format(self.rid)
            print('\nDataset RID: ', self.rid)
            print('\nDataset Link: ', '[link={0}]{0}[/link]\n'.format(self.dataset_link))
        else:
            print('\n{0} error registering Dataset on Nominal:\n'.format(resp.status_code), resp.json())                

        return resp

class Ingest:
    '''
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
    '''

    @staticmethod
    def set_ts_index(df: pl.DataFrame, ts_col: str = None) -> pl.DataFrame:
        '''
        Sets a timestamp index for the provided DataFrame.

        This method attempts to infer the timestamp column if one is not specified. It adds three internal columns to the
        DataFrame: '_python_datetime', '_iso_8601', and '_unix'. The DataFrame is then sorted by the '_python_datetime' column.

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
        '''        
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
                df.drop_in_place('_python_datetime')
                df.drop_in_place('_iso_8601')
                df.drop_in_place('_unix')
            except Exception:
                pass
            datetime_series = pl.Series('_python_datetime', [parser.parse(dt_str) for dt_str in df[ts_col]])
            iso_8601_series = pl.Series('_iso_8601', [dt.isoformat() + '.000Z' for dt in datetime_series])
            unix_series = pl.Series('_unix', [dt.timestamp() for dt in datetime_series])
            df.insert_column(0, datetime_series)
            df.insert_column(0, iso_8601_series)
            df.insert_column(0, unix_series)            
            df = df.sort('_python_datetime') # Datasets must be sorted in order to upload to Nominal
        else:
            print('A Dataset must have at least one column that is a timestamp. Please specify which column is a date or datetime with the `ts_col` parameter.')

        return df

    def read_csv(self, path: str, ts_col: str = None) -> Dataset:
        dfc = pl.read_csv(path)
        dft = self.set_ts_index(dfc, ts_col)
        return Dataset(dft, filename = os.path.basename(path))

    def read_parquet(self, path: str, ts_col: str = None) -> Dataset:
        dfp = pl.read_parquet(path)
        dft = self.set_ts_index(dfp, ts_col)
        return Dataset(dft, filename = os.path.basename(path))

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
    properties : list of str
        A list of properties associated with the run.
    datasets : list of Dataset
        A list of `Dataset` objects associated with the run.        
    run_domain : dict
        A dictionary containing 'START' and 'END' time run_domain for the run.
    datasets_domain : dict
        A dictionary holding the overall 'START' and 'END' datetime run_domain derived from the datasets.

    Methods
    -------
    upload()
        Uploads the run and its datasets to Nominal.
    '''    
    def __init__(self,
                 path: str = None,
                 paths: list[str] = [],
                 datasets: list[Dataset] = [],
                 properties: list[str] = [],
                 title: str = None,
                 description: str = '',               
                 start: str = None, 
                 end: str = None):
        
        self.description = description
        self.properties = properties
        self.run_domain = {'START': {}, 'END': {}}

        if path is not None:
            paths = [path]

        if len(paths) == 0 and len(datasets) == 0:
            print('Please provide a list of Datasets or list of paths for this Run')
            return

        if len(paths) > 0:
            self.datasets = [Ingest().read_csv(fp) for fp in paths]
        else:
            self.datasets = datasets

        mins = []
        maxs = []
        for ds in self.datasets:
            mins.append(ds['_python_datetime'].min())
            maxs.append(ds['_python_datetime'].max())
        self.datasets_domain = dict(START = min(mins), END = max(maxs))

        self.__set_run_datetime_boundary('START', start)
        self.__set_run_datetime_boundary('END', end)

        self.__set_run_unix_run_domain()

        if title is None:
            self.title = default_filename('RUN')

    def __set_run_datetime_boundary(self, key: str, str_datetime: any):
        '''
        Set start & end boundary variables for Run
        '''
        # If an explicit start/end timestamp is not provided,
        # use the min/max of the combined Datasets domain
        if str_datetime is None:
            self.run_domain[key]['DATETIME'] = self.datasets_domain[key]
        elif type(str_datetime) is datetime:
            self.run_domain[key]['DATETIME'] = str_datetime
        elif type(str_datetime) is str:
            self.run_domain[key]['DATETIME'] = parser.parse(str_datetime)

    def __set_run_unix_run_domain(self):
        '''
        Set start & end boundary variables for Run
        '''        
        for key in ['START', 'END']:
            dt = self.run_domain[key]['DATETIME']
            unix = dt.timestamp()
            seconds = floor(unix)
            self.run_domain[key]['SECONDS'] = seconds
            self.run_domain[key]['NANOS'] = floor((unix - seconds) / 1e9)

    def __get_headers(self, content_type: str = 'json') -> dict:
        TOKEN = kr.get_password('Nominal API', 'python-client')
        return {
            "Authorization": "Bearer {}".format(TOKEN),
            "Content-Type": "application/{0}".format(content_type),
        }

    def upload(self) -> requests.Response:
        '''
        Uploads the run and its datasets to Nominal.

        Returns
        -------
        requests.Response
            The response object from the REST call.
        '''
        datasets_payload = dict()

        for ds in self.datasets:
            # First, check if Run Datasets have been uploaded to S3
            if ds.s3_path is None:
                ds.upload()
            datasets_payload[ds.filename] = PayloadFactory.create_unix_datasource(ds)
        
        run_payload = PayloadFactory.run_upload(self, datasets_payload)

        # Make POST request to register Run and Datasets on Nominal
        resp = requests.post(
                    url = ENDPOINTS['run_upload'].format(get_api_domain()),
                    json = run_payload,
                    headers = self.__get_headers(),
                )
        
        self.last_payload = run_payload

        if resp.status_code == 200:
            self.rid = resp.json()['runRid']
            print('\nRun RID: ', self.rid)
        else:
            print('\n{0} error registering Run on Nominal:\n'.format(resp.status_code), resp.json())    

        return resp
