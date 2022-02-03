import unittest

import json

from pyspark.sql.functions import mean

from dependencies.spark import start_spark
from maintrigger.main import extract_data

class SparkETLTests(unittest.TestCase):


    def setUp(self):

        self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark = start_spark(app_name='my_etl_job',files=['configs/etl_config.json'])
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        self.spark.stop()

    def test_transform_data(self):

        input_data = (self.spark.read.format("json").load("/Users/n0t01pn/PycharmProjects/nikhilindikuzha1-data-engineering-test/tests/test_data/recipes-000.json"))

        self.assertEqual(input_data.count(), 1)





if __name__ == '__main__':
    unittest.main()