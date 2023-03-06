### ETL de diversificação dos fundos de investimento

#### Resumo
Projeto pessoal de ETL com fins de desenvolvimento profissional.

A fonte de dados é o portal de dados abertos da CVM (https://dados.cvm.gov.br/)

A transformação calcula a diversificação do fundos de investimento, trazendo o percentual de paticipação de cada ativo.

O destino é um S3 bucket AWS

#### Rodando
python -m run ...\configs\config.yml
