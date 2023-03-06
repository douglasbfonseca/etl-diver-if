"""Running the Xetra ETL application"""
import argparse
import logging
import logging.config

import yaml

from src.common.s3 import S3BucketConnector
from src.common.cvm import CvmConnector
from src.transformers.etl_transformer import FundosDiverETL, TransformerConfig, TargetConfig


def main():
    """
      entry point to run the ETL job
    """
    # Parsing YAML file
    parser = argparse.ArgumentParser(description='Run the ETL job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))

    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # reading CVM and s3 configuration
    cvm_config = config['cvm']
    s3_config = config['s3']
    # creating the CVM and S3BucketConnector class instances for source and target
    cvm_connector = CvmConnector(cvm_url=cvm_config['cvm_url'])
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      default_region=s3_config['default_region'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])
    # reading transformer arguments
    transformer_args = TransformerConfig(**config['transformer_args'])
    # reading target configuration
    target_config = TargetConfig(**config['target'])
    # reading meta file configuration
    #meta_config = config['meta']
    
    # creating ETL class instance
    logger.info('ETL job started')
    fudos_diver_etl = FundosDiverETL(cvm_connector,
                                     s3_bucket_trg,
                                     transformer_args,
                                     target_config)
    # running etl job for xetra report1
    fudos_diver_etl.etl_report()
    logger.info('ETL job finished.')


if __name__ == '__main__':
    main()