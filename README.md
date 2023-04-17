### Diversification of investment funds ETL job

#### Abstract

The data source is CVM open data portal (https://dados.cvm.gov.br/)

The transformation calculates the diversification of investment funds, bringing the percentage of ownership of each asset.

The target is a S3 bucket AWS

#### Running
python -m run ...\configs\config.yml
