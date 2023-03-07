"""Connnector and methods accessing CVM"""
import os
import logging
from io import BytesIO
import pandas as pd
import requests
from zipfile import ZipFile


class CvmConnector():
    """
    Class for interacting with CVM website
    """
    def __init__(self, cvm_url: str, file_format: str, prefix_name: str, year: str, month: str):
        """
        Constructor for CvmConnector

        :param cvm_url: Url to CVM source of data. A Zipfile.
        """
        self._logger = logging.getLogger(__name__)
        self._cvm_url = cvm_url
        self._file_format = file_format
        self._prefix_name = prefix_name
        self._year = year
        self._month = month
    
    def get_csv_file(self, zip_file, i):
        """
        
        """
        with ZipFile(BytesIO(zip_file.content)) as zip_file:
            csv_name = self._prefix_name + str(i) + '_' + self._year + self._month + '.csv'
            with zip_file.open(csv_name) as cda_fi:
                df = pd.read_csv(cda_fi, sep = ';', encoding="ISO-8859-1", low_memory=False)
        return df

    def dowload_zip_file(self):
        """
        Zipfile requester 
        """
        url = self._cvm_url + self._year + self._month + self._file_format
        zip_file = requests.get(url)
        if zip_file.status_code == requests.codes.OK:
            return zip_file

    
