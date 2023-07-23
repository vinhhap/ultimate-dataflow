import logging
import typing
import fnmatch
import pandas as pd
import paramiko
from io import StringIO
from google.cloud import storage

class GCSConnector:
    def __init__(self, bucket_name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.client = None
    
    def _get_client(self) -> storage.Client:
        return storage.Client()
        
    def list_file(self, file_path: str, file_pattern: str='*') -> list:
        if self.client is None:
            self.client = self._get_client()
        bucket = self.client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=file_path)
        matched_files_list = [
            f'gs://{self.bucket_name}/{blob.name}' 
            for blob in blobs 
            if fnmatch.fnmatch(blob.name, file_pattern)
        ]
        return matched_files_list

class SFTPConnector:
    def __init__(self, conn_info: dict, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_info = conn_info
        self.sftp_client = None

    def _get_sftp_client(self) -> paramiko.SFTPClient:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        pkey = self.conn_info.get('pkey')
        if pkey:
            private_key = StringIO(pkey)
            self.conn_info['pkey'] = paramiko.RSAKey.from_private_key(private_key)
        ssh_client.connect(**self.conn_info)
        sftp_client = ssh_client.open_sftp()
        return sftp_client

    def close_connection(self) -> None:
        if self.sftp_client is not None:
            self.sftp_client.close()

    def list_files(self, file_path: str, file_pattern: str='*') -> list:
        if self.sftp_client is None:
            self.sftp_client = self._get_sftp_client()
        file_list = self.sftp_client.listdir(f'{file_path}')
        matched_files_list = fnmatch.filter(file_list, file_pattern)
        matched_files_list = [f'sftp://{file_path}/{f}' for f in matched_files_list]
        return matched_files_list

    def read_csv(self, file_path: str, read_options: dict={}) -> pd.DataFrame:
        if self.sftp_client is None:
            self.sftp_client = self._get_sftp_client()
        remote_file = self.sftp_client.open(file_path, 'r')
        remote_file.prefetch()
        df = pd.read_csv(remote_file, **read_options) # type: ignore
        df = df.convert_dtypes(dtype_backend="pyarrow")
        remote_file.close()
        return df

    def write_csv(self, file_path: str, df: pd.DataFrame, sep: str='|', write_options: dict={}) -> None:
        if self.sftp_client is None:
            self.sftp_client = self._get_sftp_client()
        with self.sftp_client.open(file_path, 'w') as f:
            df.to_csv(f, index=False, sep=sep, **write_options) # type: ignore