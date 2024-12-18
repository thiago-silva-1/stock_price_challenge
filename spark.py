from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName('spark_session')\
    .getOrCreate()