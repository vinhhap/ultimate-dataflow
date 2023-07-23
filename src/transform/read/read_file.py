import apache_beam as beam
import typing
import logging
from helper.argparser import RunParam
import glob
import boto3
from azure.storage.blob import BlobServiceClient
from helper.secret import get_secret
from source_sink import GCSConnector, SFTPConnector
import pandas as pd

class ListFiles(beam.DoFn):
    def _list_files_gcs(self, source: str) -> list:
        bucket_name = source.split('/')[2]
        path_components = source.split('/')
        file_path = '/'.join(path_components[3:-1])
        file_pattern = path_components[-1]
        connector = GCSConnector(bucket_name)
        return connector.list_file(file_path, file_pattern)

    def _list_file_s3(self, source: str, sink_connection_secret: str) -> list:
        secret = get_secret(sink_connection_secret)
        s3 = boto3.resource('s3', **secret)
        bucket_name = source.split('/')[2]
        interface = source.split('/')[0]
        bucket = s3.Bucket(bucket_name)
        obj_list = bucket.objects.filter(Prefix='/'.join(source.split('/')[3:]))
        return [f'{interface}://{bucket_name}/{obj.key}' for obj in obj_list]

    def _list_file_azure(self, source: str, sink_connection_secret: str) -> list:
        secret = get_secret(sink_connection_secret)
        blob_service_client = BlobServiceClient.from_connection_string(source, **secret)
        container_name = source.split('/')[2]
        interface = source.split('/')[0]
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with='/'.join(source.split('/')[3:]))
        return [f'{interface}://{container_name}/{blob.name}' for blob in blob_list]

    def _list_file_sftp(self, source: str, sink_connection_secret: str) -> list:
        secret = get_secret(sink_connection_secret)
        connector = SFTPConnector(secret)
        path_components = source.split('/')
        file_path = "/".join(path_components[2:-1]) + "/"
        file_pattern = path_components[-1]
        connector.close_connection()
        return connector.list_files(file_path, file_pattern)

    def _list_file_local(self, source: str) -> list:
        return glob.glob(source)

    def _choose_storage_service(self, source: str, sink_connection_secret: str):
        if source.startswith('gs://'):
            return self._list_files_gcs(source)
        elif source.startswith(('s3://', 's3a://', 's3n://')):
            return self._list_file_s3(source, sink_connection_secret)
        elif source.startswith(('az://', 'abfs://', 'abfss://')):
            return self._list_file_azure(source, sink_connection_secret)
        elif source.startswith(('sftp://', 'ftp://')):
            return self._list_file_sftp(source, sink_connection_secret)
        else:
            return self._list_file_local(source)

    def process(self, context: RunParam):
        # Extract info from context
        source = context.source
        source_connection_secret = context.source_connection_secret
        files_list = self._choose_storage_service(source, source_connection_secret)
        logging.info(f"Found {len(files_list)} files matched with pattern {source}")
        return files_list

class ReadCsv(beam.DoFn):
    def __init__(self, source_sink: RunParam, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source_sink = source_sink

    def process(self, context: str):
        # Extract info from attribute & context
        file_path = context
        run_param = self.source_sink

        # Extract info from run_param
        source_connection_secret = run_param.source_connection_secret
        pipeline_options = run_param.pipeline_options
        read_options = pipeline_options.get('read_options', {})

        # Read and return result as pd.DataFrame
        logging.info(f"Reading data from {file_path}")
        if file_path.startswith('sftp://'):
            file_path = file_path.replace('sftp://', '')
            connector = SFTPConnector(get_secret(source_connection_secret))
            df = connector.read_csv(file_path, read_options)
            connector.close_connection()
        else:
            df = pd.read_csv(file_path, **read_options)
        yield df