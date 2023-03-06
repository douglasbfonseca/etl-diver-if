"""ETL Component"""

import logging
from typing import NamedTuple
import pandas as pd
from datetime import datetime

from src.common.s3 import S3BucketConnector
from src.common.cvm import CvmConnector

class TransformerConfig(NamedTuple):
    """
    Class for source configuration data

    :param denom_social: social fund name
    :param cnpj_fundo: CNPJ of the fund
    :param percentual_ativo: percent of the asset in the fund
    :param vl_mercado: value of the asset
    :param vl_mercado_x: value of the asset
    :param vl_mercado_y: value of the fund
    :param columns_filtred: used columns
    :param columns_renamed: used colmns renamed
    :param gb_cnpj_ativo: cnpj and ativo to been grouped by
    :param vl_mercado_fundo: value of the fund
    :param tp_ativo: name of the asset
    
    """

    denom_social: str
    cnpj_fundo: str
    percentual_ativo: str
    vl_mercado: str
    vl_mercado_x: str
    vl_mercado_y: str
    columns_filtred: list[str]
    columns_renamed: list[str]
    gb_cnpj_ativo: list[str]
    vl_mercado_fundo: str
    tp_ativo: str

class TargetConfig(NamedTuple):
    """
    Class for source configuration data
    
    :param trg_key: root of targuet name in the bucket
    :param trg_format: targuet format
    """

    trg_key: str
    trg_format: str

class FundosDiverETL():
    """
    Reads the CVM data, transforms and writes the transformed to target 
    """

    def __init__(self, cvm_src: CvmConnector,
                 s3_bucket_trg: S3BucketConnector,
                 transformer_args: TransformerConfig,
                 trg_args: TargetConfig):
        """
        Constructor for ETL Transformer

        :param CVM_src: connection to CVM website
        :param s3_bucket_trg: connection to target bucket
        :param transformer_args : NamedTuple class with configuration data
        :param trg_args : NamedTuple class with configuration data
        """
        
        self._logger = logging.getLogger(__name__)
        self.cvm_src = cvm_src
        self.s3_bucket_trg = s3_bucket_trg
        self.transf_args = transformer_args
        self.trg_args = trg_args

    def extract(self):
        """
        Extract data from CVM website

        returns:
            Aggregated Pandas DataFrame with all source data
        """
        zip_file = self.cvm_src.dowload_zip_file()
        data_frame = pd.concat([self.cvm_src.get_csv_file(zip_file, i) for i in range(1,3)])
        self._logger.info('Extracting CVM source files finished.')
        return data_frame


    def transform(self, data_frame: pd.DataFrame):
        """
        Transform data

        :param data_frame: Aggregated Pandas DataFrame with all data needed

        returns:
            Transformed Pandas Data Frame
        """
    
        # inputing funds with missing social names 
        data_frame[self.transf_args.denom_social] = data_frame[self.transf_args.denom_social]\
            .fillna("Sem denominacao social NaN")

        # Calculating percentage of assets in the funds
        vl_fundo = pd.DataFrame(data_frame.groupby(by=self.transf_args.cnpj_fundo)\
            .aggregate("sum")[self.transf_args.vl_mercado])\
                .reset_index()
        data_frame = pd.merge(data_frame, vl_fundo, how = 'inner', on = self.transf_args.cnpj_fundo)
        data_frame[self.transf_args.percentual_ativo] = data_frame[self.transf_args.vl_mercado_x]/data_frame[self.transf_args.vl_mercado_y]
        
        # renaming used columns and drops unnecessary ones
        data_frame = data_frame[self.transf_args.columns_filtred]
        data_frame = data_frame.rename(columns={self.transf_args.vl_mercado_x: self.transf_args.vl_mercado,
                                                self.transf_args.vl_mercado_y: self.transf_args.vl_mercado_fundo})
        data_frame = data_frame.groupby(by=self.transf_args.gb_cnpj_ativo)\
            .aggregate("sum").reset_index()
        data_frame = data_frame.drop(columns=[self.transf_args.vl_mercado, self.transf_args.vl_mercado_fundo])
        
        self._logger.info('Applying transformations to the data finished.')
        return data_frame

    def load(self, data_frame):
        key = self.trg_args.trg_key + datetime.today().strftime('%Y-%m-%d_%H%M%S') + self.trg_args.trg_format
        self.s3_bucket_trg.write_df_to_s3(data_frame, key)
        self._logger.info('Target data successfully written.')
        return True

    def etl_report(self):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        self.load(data_frame)
        return True