"""
File to store constants
"""

from enum import Enum

class S3FileTypes(Enum):
    """
    suported file types for S3BucketConector
    """
    CSV = 'csv'
    PARQUET = 'parquet'

class MetaProcessFormat(Enum):
    """
    formation for MetaProcess class
    """