import unittest
import os
from pyspark.sql import SparkSession

class TestPySpark(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("EHR_ETL_Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.stop()
        finally:
            cls.spark = None

    def test_read_sample_csv(self):
        test_data_path = os.path.join(os.path.dirname(__file__), "../data/test_data.csv")
        df = self.spark.read.csv(test_data_path, header=True, inferSchema=True)
        df.show(5)
        self.assertGreater(df.count(), 0)  # check dataframe is not empty

if __name__ == "__main__":
    unittest.main()