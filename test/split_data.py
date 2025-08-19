import json
import math
import os
import time
import psycopg2
import random
import math
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import execute
import plot 
import shutil
from test import *

# spilt tbl file into multiple smaller files
def split_tbl_file(tbl_file, output_dir, num_batches):
    lines = []
    with open(tbl_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
    # TODO: here we spilit tbl file average by line,but we can also use random split
    total_lines = len(lines)
    batch_size = math.ceil(total_lines / num_batches)

    base_name = os.path.basename(tbl_file)
    name, _ = os.path.splitext(base_name)

    for i in range(num_batches):
        batch_path = os.path.join(output_dir, f"{name}_batch_{i}.tbl")
        with open(batch_path, "w", encoding="utf-8") as f:
            batch_lines = lines[i * batch_size : (i + 1) * batch_size]
            f.writelines(batch_lines)

# split all tbl files in the tbl_data directory
def split_all_tbl_files(split_nums=10):
    os.makedirs(BATCH_DIR, exist_ok=True)
    for file in os.listdir(TBL_DIR):
        if file.endswith(".tbl"):
            full_path = os.path.join(TBL_DIR, file)
            print(f"Splitting {file}")
            split_tbl_file(full_path, BATCH_DIR, split_nums)

# here we default provide 10GB data, and we split it into 10 batches, each batch is 1GB
def main():
    split_all_tbl_files(10)
    return 

if __name__ == "__main__":
    main()