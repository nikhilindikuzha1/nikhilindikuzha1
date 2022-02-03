
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from dependencies.spark import start_spark

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.

def extract_data(spark):

    extractordf=(spark.read.format("json").load("/Users/n0t01pn/PycharmProjects/nikhilindikuzha1-data-engineering-test/input/recipes-000.json"))
    extractordf.show()

def main():

    spark= start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    extract_data(spark)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()