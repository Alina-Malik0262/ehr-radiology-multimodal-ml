import pyspark.sql.functions as F
from pyspark.sql import Window

class AdmissionsPreprocessor:
    def __init__(self, spark=None):
        # Initialize with an existing SparkSession or expect it to be provided later
        self.spark = spark

    def read_data(self, input_path):
        # Read CSV file with header and schema inference
        return self.spark.read.csv(input_path, header=True, inferSchema=True)

    def drop_duplicates(self, df):
        # Drop fully duplicate rows across all columns
        df = df.drop_duplicates()
        return df

    def drop_missing(self, df):
        # Drop rows where any of the key columns are missing
        return df.dropna(subset=["subject_id", "admittime", "dischtime"])

    def select_columns(self, df):
        # Keep only the relevant columns needed for readmission logic
        return df.select("subject_id", "hadm_id", "admittime", "dischtime")

    def add_readmission_flag(self, df):
        # Define a window to look at each patient's admissions in order of time
        window = Window.partitionBy("subject_id").orderBy("admittime")

        # Get the next admission and next hadm_id for comparison
        next_admit = F.lead("admittime").over(window)
        next_hadm = F.lead("hadm_id").over(window)

        # If the next admission is within 30 days after discharge and exists → flag = 1
        readmit_30d = F.when(
            (F.datediff(next_admit, F.col("dischtime")) <= 30) & (next_hadm.isNotNull()), 1
        ).otherwise(0)

        # Add the readmission flag column
        return df.withColumn("readmitted_30d", readmit_30d)

    def preprocess(self, input_path):
        # Full preprocessing pipeline
        df = self.read_data(input_path)
        df = self.drop_duplicates(df)
        df = self.drop_missing(df)
        df = self.select_columns(df)
        df = self.add_readmission_flag(df)

        return df, self.spark