import __main__
from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession
def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    spark_builder = (
        SparkSession
            .builder
            .master(master)
            .appName(app_name))
    spark_sess = spark_builder.getOrCreate()
    #spark_logger = logging.Log4j(spark_sess)
    return spark_sess