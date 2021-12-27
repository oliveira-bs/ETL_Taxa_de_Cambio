# Importando as bibliotecas 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from random import uniform
import requests
import json
import pandas as pd
from datetime import datetime, date, timedelta,time
import pathlib
import os
import glob
import shutil

import findspark
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkFiles
from pyspark.sql.types import *

findspark.init()

from General_Pipeline_API_Currencies import run_pipeline

default_args = {
    'start_date': datetime(2020, 1, 1)
} 

spark = SparkSession \
    .builder \
    .appName("ETL API_awesome") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def _select_currencies(**kwargs):
    currency = 'USD-BRL,EUR-BRL,BTC-BRL'
    path_DB = f'{os.getcwd()}/ETL_Cotacao_Moeda/'

    print(f'Dag path: {os.getcwd()}')
    
    def list_currency(currency):
        str_currency = currency.split('-')
        str_currency = ''.join(str_currency)
        items_currency = str_currency.split(",")
        return (items_currency)

    items_currency = list_currency(currency)
    kwargs['ti'].xcom_push(key='currency', value=currency)
    kwargs['ti'].xcom_push(key='path_DB', value=path_DB)
    kwargs['ti'].xcom_push(key='items_currency', value=items_currency)


def _api_conection(**kwargs):
    currency = kwargs['ti'].xcom_pull(key='currency')
    r = requests.get(f'https://economia.awesomeapi.com.br/last/{currency}')
    print('Conexão realizada!')

    json_df = r.json()
    kwargs['ti'].xcom_push(key='json_df', value=json_df)
    print(json_df)

def _json_respose(**kwargs):
    json_df = kwargs['ti'].xcom_pull(key='json_df')
    path_DB = kwargs['ti'].xcom_pull(key='path_DB')
    items_currency = kwargs['ti'].xcom_pull(key='items_currency')

    def json_organized(json_df, items_currency):
        list_data_currency = []
        for i in items_currency:
            json_df_currency = json_df[i]
            list_data_currency.append(json_df_currency)
        return list_data_currency

    json_data = json_organized(json_df, items_currency)

    kwargs['ti'].xcom_push(key='json_data', value=json_data)

def _research_DB_parquet(**kwargs):
    path_DB = kwargs['ti'].xcom_pull(key='path_DB')

    list_files =[]    
    for file in glob.glob(path_DB +"DB_*"):
        list_files.append(file)
    list_files = sorted(list_files, key=lambda t: -os.stat(t).st_mtime)
    try:
        name_parquet = list_files[0]
        print("f{name_parquet} localizado!")
    except:
        name_parquet = "Empty_file_parquet"
        print(f'Error ({name_parquet}): DB inexistente')
    kwargs['ti'].xcom_push(key='parquet_DB', value=name_parquet)

def _consolidate_DB_parquet(**kwargs):
    path_DB = kwargs['ti'].xcom_pull(key='path_DB')
    json_data = kwargs['ti'].xcom_pull(key='json_data')
    parquet_DB = kwargs['ti'].xcom_pull(key='parquet_DB')

    def database_acess(path_DB, parquet_DB):
        try:
            df_quote_currency_db = spark.read.parquet(parquet_DB)
            print(f"Database {parquet_DB} acessado com sucesso!")
        except:
            emp_RDD = spark.sparkContext.emptyRDD()
            columns = StructType([StructField('Conversão', StringType(), True),
                        StructField('Currenc1', StringType(), True),
                        StructField('Currenc2', StringType(), True),
                        StructField('Máximo', FloatType(), True),
                        StructField('Mínimo', FloatType(), True),
                        StructField('Variação', FloatType(), True),
                        StructField('Porcent_Variação', FloatType(), True),
                        StructField('Compra', FloatType(), True),
                        StructField('Venda', FloatType(), True),
                        StructField('Timesamp', IntegerType(), True),
                        StructField('Data', DateType(), True)
                        ])
            print("Criando Database...")
            df_quote_currency_db = spark.createDataFrame(data = emp_RDD, schema = columns)
            print("Database Criado...")
        df_quote_currency_db.show()
        return df_quote_currency_db
    
    df_quote_currency_db = database_acess(path_DB, parquet_DB)

    def df_json_string(json_data):
        for i in range(0, len(json_data)):
            df_api = spark.createDataFrame([(json_data[i])])
            df_api = df_api.select("name","code","codein","high","low"\
                ,"varBid","pctChange","bid","ask","timestamp","create_date")
            if i == 0:
                df_currency_now = df_api
            else:
                df_currency_now = df_currency_now.union(df_api)
        columns=['Conversão','Currenc1','Currenc2','Máximo','Mínimo'\
            ,'Variação','Porcent_Variação','Compra','Venda','Timesamp','Data']
        df_currency_now = df_currency_now.toDF(*columns)
        return df_currency_now

    df_currency_now = df_json_string(json_data)

    def concat_db(df_quote_currency_db, df_currency_now):
        df_quote_currency_db = df_quote_currency_db.union(df_currency_now)
        print("Dados da API inseridos na dase de dados parquet")
        return df_quote_currency_db

    df_quote_currency_db = concat_db(df_quote_currency_db, df_currency_now)

    def write_db_parquet(df_quote_currency_db, parquet_DB, path_DB):
        print("Iniciando a consolidação da base de dados...")
        new_parquet_DB = f"DB_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            df_quote_currency_db.write.format("parquet").save(path_DB+new_parquet_DB)
            print(f"{new_parquet_DB} salvo com sucesso !")
            path_parquet_pre_data = parquet_DB
            shutil.rmtree(path_parquet_pre_data) #%rm -r $path_parquet_pre_data
            print(f"DataBase anterior excluída: ({parquet_DB})")

        except OSError as e:
            print("!!!!! Error de escrita !!!!!")
            print("Error: %s - %s." % (e.filename, e.strerror))
        return new_parquet_DB

    db_parquet_now = write_db_parquet(df_quote_currency_db, parquet_DB, path_DB)

    kwargs['ti'].xcom_push(key='db_parquet_now', value=db_parquet_now)

with DAG('ETL_API_Currencies', schedule_interval= timedelta(seconds= 60), catchup= False, default_args=default_args) as dag:

    select_currencies = PythonOperator(
        task_id='select_currencies',
        python_callable=_select_currencies
    )

    api_conection = PythonOperator(
        task_id='api_conection',
        python_callable=_api_conection
    )

    json_respose = PythonOperator(
        task_id='json_respose',
        python_callable=_json_respose
    )

    research_DB_parquet = PythonOperator(
        task_id='research_DB_parquet',
        python_callable=_research_DB_parquet
    )

    consolidate_DB_parquet = PythonOperator(
        task_id='consolidate_DB_parquet',
        python_callable=_consolidate_DB_parquet
    )

    select_currencies >> api_conection >> json_respose >> consolidate_DB_parquet
    research_DB_parquet >> consolidate_DB_parquet