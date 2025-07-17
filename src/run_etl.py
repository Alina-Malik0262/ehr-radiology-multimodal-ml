# src/run_etl.py

from src.etl.admissions_etl import AdmissionsPreprocessor
from pyspark.sql import SparkSession
from src.utils.util import find_file, get_output_path, write_single_csv

def run_admissions_etl(spark):
    input_filename = "admissions.csv"
    output_dir = "../data/tabular"  # relative to src/
    output_filename = "admissions_processed.csv"
    input_path = find_file(input_filename)
    output_path = get_output_path(output_dir, output_filename)
    preprocessor = AdmissionsPreprocessor(spark)
    df, _ = preprocessor.preprocess(input_path)
    df = df.orderBy("subject_id", "admittime")
    write_single_csv(df, output_dir, output_filename)
    print(f"Processed data saved to {output_path}")

def main():
    spark = SparkSession.builder.appName("Admissions_ETL").getOrCreate()
    run_admissions_etl(spark)
    spark.stop()

if __name__ == "__main__":
    main()