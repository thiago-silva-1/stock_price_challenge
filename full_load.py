import requests
import os
import json
from spark import spark
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructField, StructType, StringType
from dotenv import load_dotenv
from weekday import get_start_date

def check_data():
    full_load = False
    try:
        df = spark.read.json('bronze_layer')
        max_date = df.select(max(col('date_partition')).alias('max_date')).collect()[0]
        print(max_date["max_date"])
        return {'full_load': full_load, 'max_date': max_date["max_date"]}
    except:
        full_load = True
        return {'full_load': full_load, 'max_date': None}

def build_params(arguments):
    load_dotenv()
    token = os.getenv('API_TOKEN')
    params = {
        'api_token': token
    }
    if arguments["full_load"] == True:
        params["date_from"] = get_start_date()
        print('No data stored, full load needed')
    else:
        params["date_from"] = arguments["max_date"]
        print(f'Requesting data from date {arguments["max_date"]}')

    return params

def build_bronze_schema():
    bronze_schema = StructType([
        StructField("close", StringType(), nullable=True),
        StructField("date", StringType(), nullable=True),
        StructField("high", StringType(), nullable=True),
        StructField("low", StringType(), nullable=True),
        StructField("open", StringType(), nullable=True),
        StructField("symbol", StringType(), nullable=True),
        StructField("volume", StringType(), nullable=True)
        ]
    )

    return bronze_schema

def iterate(params):
    symbols = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'TSLA', 'NFLX', 'UNH', 'JNJ', 'GOOGL', 'BAC']
    url = 'https://api.stockdata.org/v1/data/eod'
    all_data=[]
    for company in symbols:
        params["symbols"] = company
        print(f'Trying to get data from {company}')
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            content = response.json()
            print(content["meta"])
            for item in content["data"]:
                item["symbol"] = params["symbols"]
                all_data.append(item)
            print(f'Got all data from {company}')
        except Exception as e:
            print(f'Error in {company} stock request.\nError message: {e}')
    return all_data

def build_bronze(data, schema):
    df_bronze = spark.createDataFrame(data, schema=schema)\
    .withColumn('date_partition', to_date('date'))

    return df_bronze

def overwrite_bronze(df_bronze):
    df_bronze.write\
        .mode('overwrite')\
        .option('partitionOverwriteMode', 'dynamic')\
        .partitionBy('date_partition')\
        .json('bronze_layer', mode='overwrite')
    
    return None

def execute():
    arguments = check_data()
    params = build_params(arguments)
    schema = build_bronze_schema()
    data = iterate(params)
    bronze_df = build_bronze(data=data, schema=schema)
    overwrite_bronze(df_bronze=bronze_df)

execute()