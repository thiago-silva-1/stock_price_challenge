from spark import spark
from pyspark.sql.types import StructField, StructType, FloatType, IntegerType, StringType, TimestampType
import pyspark.sql.functions as F


# def create_silver_schema():
#     schema = StructType([
#         StructField("close", FloatType(), nullable=True),
#         StructField("date", TimestampType(), nullable=True),
#         StructField("high", FloatType(), nullable=True),
#         StructField("low", FloatType(), nullable=True),
#         StructField("open", FloatType(), nullable=True),
#         StructField("symbol", StringType(), nullable=True),
#         StructField("volume", IntegerType(), nullable=True)
#     ]
#     )

#     return schema


def build_silver():
    read_path = 'bronze_layer'
    df_schema = spark.read.json(read_path)
    df_silver_raw = df_schema.select('symbol', 'date', 'open', 'low', 'high', 'close', 'volume', 'date_partition')
    #df_silver = df_silver.withColumn('date_partition', F.to_date('date'))
    
    return df_silver_raw

def assert_data_type(df_raw):
    df_silver = df_raw.withColumn('close', F.col('close').cast(FloatType()))\
    .withColumn('date', F.col('date').cast(TimestampType()))\
    .withColumn('high', F.col('high').cast(FloatType()))\
    .withColumn('low', F.col('low').cast(FloatType()))\
    .withColumn('open', F.col('open').cast(FloatType()))\
    .withColumn('symbol', F.col('symbol').cast(StringType()))\
    .withColumn('volume', F.col('volume').cast(IntegerType()))\
    
    return df_silver

def write_silver(df_silver):
    write_path = 'silver_parquet'

    df_silver.write\
    .mode('overwrite')\
    .option('partitionOverwriteMode', 'dynamic')\
    .partitionBy('date_partition')\
    .parquet(write_path, mode='overwrite')

    return None

def execute_silver():
    silver_raw = build_silver()
    df_silver = assert_data_type(silver_raw)
    write_silver(df_silver)

execute_silver()
# df_silver.printSchema()
# df_silver.show()

# df_silver.write\
#     .mode('overwrite')\
#     .option('partitionOverwriteMode', 'dynamic')\
#     .partitionBy('date_partition')\
#     .parquet(write_path, mode='overwrite')