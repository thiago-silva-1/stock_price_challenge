from spark import spark
import pyspark.sql.functions as F

def build_gold():
    silver_path = 'silver_parquet'
    df_gold_raw = spark.read.parquet(silver_path)
    df_gold = df_gold_raw.select('symbol', 'date_partition', 'open', 'close')\
            .withColumn('preco', F.round(((F.col('open')+F.col('close'))/2), 2))\
            .withColumnRenamed('symbol', 'codigo_acao')\
            .withColumnRenamed('date_partition', 'date')\
            .select('codigo_acao', 'date', 'preco')

    return df_gold

def write_gold(df_gold):
    gold_path = 'gold_parquet'
    df_gold.write\
        .mode('overwrite')\
        .option('partitionOverwriteMode', 'dynamic')\
        .partitionBy('date')\
        .parquet(gold_path, mode='overwrite')

    return None

def execute_gold():
    df_gold = build_gold()
    write_gold(df_gold)

execute_gold()