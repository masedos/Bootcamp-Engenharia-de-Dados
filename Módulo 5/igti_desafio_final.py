from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
import boto3
import os
from sqlalchemy import create_engine
from airflow.models import Variable

# Usando a novÃ­ssima Taskflow API
default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 30, 18, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False
}

@dag(default_args=default_args, schedule_interval=None, description="ETL de dados do IBGE para Data Lake e DW")
def desafio_final_etl():
    """
    Um flow para obter dados do IBGE de uma base MongoDB, da API de microrregiÃµes do IBGE,
    depositar no datalake no S3 e no DW num postgresql tambÃ©m hosteado na AWS
    """
    @task
    def extrai_mongo():
        import pymongo
        import pandas as pd
        client = pymongo.MongoClient("mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net/ibge?retryWrites=true&w=majority")
        db = client.ibge
        pnad_collec = db.pnadc20203
        df = pd.DataFrame(list(pnad_collec.find()))
        df.to_csv('/tmp/pnadc20203.csv', index=False, encoding='utf-8', sep=';')
        return "/tmp/pnadc20203.csv"

    @task
    def extrai_api():
        import pandas as pd
        res = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados/MG/mesorregioes")
        resjson = json.loads(res.text)
        df = pd.DataFrame(resjson)[['id', 'nome']]
        df.to_csv("/tmp/dimensao_mesorregioes_mg.csv", sep=';', index=False, encoding='utf-8')
        return "/tmp/dimensao_mesorregioes_mg.csv"

    @task
    def upload_to_s3(file_name):
        print(f"Got filename: {file_name}")
        aws_access_key_id     = Variable.get("aws_access_key_id")
        aws_secret_access_key = Variable.get("aws_secret_access_key")


        s3_client = boto3.client(
            's3', 
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3_client.upload_file(file_name, "ney-testes", file_name[5:])
    
    @task
    def write_to_postgres(csv_file_path):
        import pandas as pd
        aws_postgres_password = Variable.get("aws_postgres_password")
        conn = create_engine(f'postgresql://airflow:{aws_postgres_password}@<endpoint>:5432/postgres')
        df = pd.read_csv(csv_file_path, sep=';')
        if csv_file_path == "/tmp/pnadc20203.csv":
            df = df.loc[(df.idade >= 20) & (df.idade <= 40) & (df.sexo == 'Mulher')]
        df['dt_inclusao_registro'] = datetime.today()
        df.to_sql(csv_file_path[5:-4], conn, index=False, if_exists="replace", method='multi', chunksize=1000)


    mongo    = extrai_mongo()
    api      = extrai_api()
    up_mongo = upload_to_s3(mongo)
    up_api   = upload_to_s3(api)
    wr_mongo = write_to_postgres(mongo)
    wr_api   = write_to_postgres(api)

desafio_final = desafio_final_etl()