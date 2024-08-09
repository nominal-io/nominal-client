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
    # eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlRzdk01QWw1aFZqU09yVEtwTlRtLSJ9.eyJub21pbmFsIjp7Im9yZ2FuaXphdGlvbl91dWlkIjoiN2Q4MDJkNGUtN2YxYy00NWI5LWJhMDUtZjdmNjMyMzUwNGQ2IiwidXNlcl91dWlkIjoiMmVkOGJkYjItYmFkOC00MGY5LWE3ZmItZWFkMjY3N2MxNTc5In0sImlzcyI6Imh0dHBzOi8vbm9taW5hbC1wcmQudXMuYXV0aDAuY29tLyIsInN1YiI6ImF1dGgwfDY2YTE4M2Q4OWE0OWZhMDg0NWVjZDUxMiIsImF1ZCI6WyJodHRwczovL2FwaS5nb3Yubm9taW5hbC5pbyIsImh0dHBzOi8vbm9taW5hbC1wcmQudXMuYXV0aDAuY29tL3VzZXJpbmZvIl0sImlhdCI6MTcyMzAzOTg1OSwiZXhwIjoxNzIzMTI2MjU5LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIG9mZmxpbmVfYWNjZXNzIiwib3JnX2lkIjoib3JnX2s3cWFvUklHRFRTYmFXNDMiLCJhenAiOiJMbU9KSjFUTDBqWE1JeTBWalA1ZnZ1MUdmZU15dGNVdCJ9.ZizfvNoxQjWInP51f4ccojwh_hlny4DZGVzB07_AkCRyKODGlI5IS25kHPdD-1xPNFP2KCIt1UFUr-IrxucbxT35Phub-IiaNxVS14oa9S1v6OpzToHeMq3jJSDJh4vceFtxdClZL1hMK1L6zLH_KuEEYYtO5s138QjAyqTA1i-hguJnjQxX0b79KZEp82P4tNtBTbi0OnEj1R2cjqftBVi_UxL9vts-wla-7TVo27ocSUppI7vkE_Yc15zvlxQ_mcO2QtPMZmgm3XCQijs5Yp4h3UA6MV-5HNZJPmPMPJA25HM5OMKQeESE7sGGsO-DVDaztOqi_QWEiAnWcmFlUQ
    kr.set_password('Nominal API', 'python-client', token)

class Dataset(pl.DataFrame):
    """
    Dataset inherits from polars dataframes to inheit their rich display, ingestion, and wrangling capabilities.
    Dataset attempts to infer the datetime column. If it finds one, it adds two new internal convenience columns:
    Dataset['_python_datetime'] and Dataset['_iso_8601'].
    Dataset['_python_datetime'] is used as the canonical timestamp column when uploading to Nominal.
    """
    def __init__(self, data = None, filename = None, overwrite = False, properties = dict(), description = ''):
        super().__init__(data)

        self.s3_path = None
        self.filename = filename
        self.properties = properties
        self.description = description
        self.rid = None
        self.dataset_link = ''

    def __get_headers(self, content_type = 'json'):
        TOKEN = kr.get_password('Nominal API', 'python-client')
        return {
            "Authorization": "Bearer {}".format(TOKEN),
            "Content-Type": "application/{0}".format(content_type),
        }

    def __upload_file(self, overwrite):
        """
        Uploads dataframe to S3 as a file.
        
        Returns:
        Response object from the REST call.
        """

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

    def upload(self, overwrite = False):        
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
    """
    Handles ingestions of various tabular and video file formats
    TODO: Consider ibis for database source connectivity?
    """
    def __init__(self):
        pass

    @staticmethod
    def set_ts_index(df, ts_col):
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
        
    def read_csv(self, path, ts_col = None):
        dfc = pl.read_csv(path)
        dft = self.set_ts_index(dfc, ts_col)
        return Dataset(dft, filename = os.path.basename(path))

class Run:
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
        self.boundaries = {'START': {}, 'END': {}}

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

        self.__set_run_unix_boundaries()

        if title is None:
            self.title = default_filename('RUN')

    def __set_run_datetime_boundary(self, key, str_datetime):
        '''
        Set start & end boundary variables for Run
        '''
        # If an explicit start/end timestamp is not provided,
        # use the min/max of the combined Datasets domain
        if str_datetime is None:
            self.boundaries[key]['DATETIME'] = self.datasets_domain[key]
        elif type(str_datetime) is datetime:
            self.boundaries[key]['DATETIME'] = str_datetime
        elif type(str_datetime) is str:
            self.boundaries[key]['DATETIME'] = parser.parse(str_datetime)

    def __set_run_unix_boundaries(self):
        '''
        Set start & end boundary variables for Run
        '''        
        for key in ['START', 'END']:
            dt = self.boundaries[key]['DATETIME']
            unix = dt.timestamp()
            seconds = floor(unix)
            self.boundaries[key]['SECONDS'] = seconds
            self.boundaries[key]['NANOS'] = floor((unix - seconds) / 1e9)

    def __get_headers(self, content_type = 'json'):
        TOKEN = kr.get_password('Nominal API', 'python-client')
        return {
            "Authorization": "Bearer {}".format(TOKEN),
            "Content-Type": "application/{0}".format(content_type),
        }

    def upload(self):

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

        return resp
