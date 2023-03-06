"""Connnector and methods accessing S3"""
import os
import logging
from io import BytesIO
import pandas as pd

import boto3

class S3BucketConnector():
    """
    Class for interacting with S3 Buckets
    """
    def __init__(self, access_key: str, secret_key: str, default_region: str, endpoint_url: str, bucket:str) -> None:
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key]
                                     )
        self._s3 = self.session.resource(service_name='s3', endpoint_url= endpoint_url, region_name =os.environ[default_region])
        self._bucket = self._s3.Bucket(bucket)
    
    def write_df_to_s3(self, df: pd.DataFrame, key: str):
        """
        Write the Data Frame to the S3 Bucket

        :param bucket: access key for accessing S3
        :param out_buffer: BytesIO, formart that should be written
        :param key: target key of saved file

        returns:
            Calls __put_objetc() method
        """
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        return self.__put_objetc(out_buffer, key)
    
    def __put_objetc(self, out_buffer: BytesIO , key: str):
        """
        Helper funcion for write_df_to_s3(), for logging informations
        
        :param out_buffer: BytesIO, formart that should be written
        :param key: target key of saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
