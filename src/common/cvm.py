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
    def __init__(self, cvm_url:str) -> None:
        """
        Constructor for CvmConnector

        :param cvm_url: Url to CVM source of data. A Zipfile.
        """
        self._logger = logging.getLogger(__name__)
        self._cvm_url = cvm_url
    
    def get_csv_file(self, zip_file, i):
        """
        
        """
        with ZipFile(BytesIO(zip_file.content)) as zip_file:
            csv_name = 'cda_fi_BLC_' + str(i) + '_202212.csv'
            with zip_file.open(csv_name) as cda_fi:
                df = pd.read_csv(cda_fi, sep = ';', encoding="ISO-8859-1", low_memory=False)
        return df

    def dowload_zip_file(self):
        """
        Zipfile requester 
        """
        zip_file = requests.get(self._cvm_url)
        if zip_file.status_code == requests.codes.OK:
            return zip_file

    
