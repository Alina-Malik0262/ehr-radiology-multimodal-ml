import os
import gzip
import shutil
import pyspark.sql.functions as F

def find_file(filename, base_dir=None):
    """
    Recursively search for a file with the given filename starting from base_dir.
    If base_dir is None, uses the project root (parent of 'src').
    """
    if base_dir is None:
        # Get project root by going up from src/utils/util.py
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    for root, dirs, files in os.walk(base_dir):
        if filename in files:
            return os.path.join(root, filename)
    raise FileNotFoundError(f"{filename} not found in {base_dir}")

def decompress_gz_file(filename):
    gz_path = find_file(filename)
    csv_path = gz_path[:-3] if gz_path.endswith('.gz') else gz_path + '.csv'
    with gzip.open(gz_path, 'rt') as f_in, open(csv_path, 'w') as f_out:
        shutil.copyfileobj(f_in, f_out)
    return csv_path


def get_output_path(directory, filename):
    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, filename)

def cast_to_date(df, col_name, new_col_name):
    return df.withColumn(new_col_name, F.col(col_name).cast("date"))


def write_single_csv(df, output_dir, output_filename):
    os.makedirs(output_dir, exist_ok=True)
    temp_dir = os.path.join(output_dir, "temp_output")
    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")
    part_file = next((f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".csv")), None)
    if not part_file:
        raise FileNotFoundError("No part file found in temporary output directory.")
    src = os.path.join(temp_dir, part_file)
    dst = os.path.join(output_dir, output_filename)
    shutil.move(src, dst)
    shutil.rmtree(temp_dir)
    print(f"Single CSV saved to {dst}")