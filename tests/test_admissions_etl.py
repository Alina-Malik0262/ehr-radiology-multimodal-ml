# tests/test_admissions_etl.py

import unittest
from pyspark.sql import SparkSession
from src.etl.admissions_etl import AdmissionsPreprocessor

class TestAdmissionsPreprocessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestAdmissionsETL").getOrCreate()
        cls.preprocessor = AdmissionsPreprocessor(cls.spark)
        # Sample data for testing
        cls.sample_data = [
            (1, 101, "2023-01-01", "2023-01-05"),
            (1, 102, "2023-01-20", "2023-01-25"),
            (2, 201, "2023-02-01", "2023-02-10"),
            (2, 202, "2023-03-01", "2023-03-05"),
        ]
        cls.columns = ["subject_id", "hadm_id", "admittime", "dischtime"]
        cls.df = cls.spark.createDataFrame(cls.sample_data, cls.columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_drop_duplicates(self):
        df_dup = self.df.union(self.df)
        df_clean = self.preprocessor.drop_duplicates(df_dup)
        self.assertEqual(df_clean.count(), self.df.count())

    def test_drop_missing(self):
        df_missing = self.df.union(self.spark.createDataFrame([(None, None, None, 1)], self.columns))
        df_clean = self.preprocessor.drop_missing(df_missing)
        self.assertEqual(df_clean.count(), self.df.count())

    def test_select_columns(self):
        df_extra = self.df.withColumn("extra", self.df.subject_id + 1)
        df_selected = self.preprocessor.select_columns(df_extra)
        self.assertEqual(set(df_selected.columns), set(self.columns))

    def test_add_readmission_flag(self):
        sample_data = [
            (1, 101, "2023-01-01", "2023-01-05"),
            (1, 102, "2023-01-20", "2023-01-25"),  # within 30 days → flag=1 for first row
            (2, 201, "2023-02-01", "2023-02-10"),
            (2, 202, "2023-03-01", "2023-03-05"),  # >30 days → flag=0 for first row
            (3, 301, "2023-01-01", "2023-01-10"),
            (3, 302, "2023-03-01", "2023-03-10"),  # 2 months apart → flag=0 for first row
        ]
        columns = ["subject_id", "hadm_id", "admittime", "dischtime"]
        self.df = self.spark.createDataFrame(sample_data, columns)
        df_flagged = self.preprocessor.add_readmission_flag(self.df)
        flagged_rows = df_flagged.filter(df_flagged.subject_id == 1).orderBy("admittime").collect()
        self.assertEqual(flagged_rows[0]["readmitted_30d"], 1)  # readmitted within 30 days for subject 1

        flagged_rows_2 = df_flagged.filter(df_flagged.subject_id == 2).orderBy("admittime").collect()
        self.assertEqual(flagged_rows_2[0]["readmitted_30d"], 1)  # readmitted within 19 days

        flagged_rows_3 = df_flagged.filter(df_flagged.subject_id == 3).orderBy("admittime").collect()
        self.assertEqual(flagged_rows_3[0]["readmitted_30d"], 0)  # 2 months apart no readmit flag for subject 3


def test_preprocess(self):
        # Save sample data to a temporary CSV for read_data test
        temp_path = "/tmp/test_admissions.csv"
        self.df.write.csv(temp_path, header=True, mode="overwrite")
        df_processed, _ = self.preprocessor.preprocess(temp_path)
        expected_cols = set(self.columns + ["readmitted_30d"])
        self.assertTrue(expected_cols.issubset(set(df_processed.columns)))
        self.assertGreater(df_processed.count(), 0)

if __name__ == "__main__":
    unittest.main()