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

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "localhost",
    "port": "44444"
}

SQL_DIR = "./tpch_query_slow"
TBL_DIR = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test/tbl_data"            # data dir before split
BATCH_DIR = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test/tbl_batches"       # data dir after split
CREATE_FILE = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test/tpch-create.sql"         # create table file
NUM_BATCHES = 3                 # spilt nums


# spilt tbl file into multiple smaller files
def split_tbl_file(tbl_file, output_dir, num_batches=10):
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
def split_all_tbl_files():
    os.makedirs(BATCH_DIR, exist_ok=True)
    for file in os.listdir(TBL_DIR):
        if file.endswith(".tbl"):
            full_path = os.path.join(TBL_DIR, file)
            print(f"Splitting {file}")
            split_tbl_file(full_path, BATCH_DIR, NUM_BATCHES)


def simple_test():
    cancel_query = 0
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    random.shuffle(sql_files)

    for sql_file in sql_files:
        sql_path = os.path.join(SQL_DIR, sql_file)
        print(f"Executing: {sql_path}")
        
        start_time = time.time()
        status = execute.execute_sql_file(cursor, sql_path)
        end_time = time.time()
        
        if status == "CANCELLED":
            cancel_query += 1
        else:
            elapsed = end_time - start_time
            print(f"query elapsed: {elapsed}")

    cursor.close()
    conn.close()

    print("All queries executed.")
    print(f"Total cancelled queries: {cancel_query}")

def test_single_same_batchs():
    total_cancel_query = 0
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    random.shuffle(sql_files)

    query_time_map = {sql_file: [] for sql_file in sql_files}


    for batch_index in range(NUM_BATCHES):
        cancel_query = 0
        print(f"\n========== Executing Batch {batch_index + 1}/{NUM_BATCHES} ==========")
        for sql_file in sql_files:
            sql_path = os.path.join(SQL_DIR, sql_file)
            print(f"Executing: {sql_path}")
            
            start_time = time.time() 
            #status = execute_sql_file(cursor, sql_path)
            end_time = time.time()
            
            if status == "CANCELLED":
                query_time_map[sql_file].append(-1)
                cancel_query += 1
                total_cancel_query += 1
            else:
                elapsed = end_time - start_time
                query_time_map[sql_file].append(elapsed)
                print(f"query elapsed: {elapsed}")
        print(f"[ batch {batch_index + 1}] Finish selecting data , cancel query: {cancel_query}")
    
    cursor.close()
    conn.close()

    print("All queries executed.")
    print(f"Total cancelled queries: {total_cancel_query}")
    plot.plot_query_times(query_time_map)
    plot.write_query_times_to_excel(query_time_map)

def qppnet_plan_collector(sql_files,conn):
    cursor = conn.cursor()
    total_cancelled = 0
    query_time_map = {sql_file: [] for sql_file in sql_files}

    for batch_index in range(NUM_BATCHES):
        print(f"\n========== Executing Batch {batch_index + 1}/{NUM_BATCHES} ==========")
        cancel_query = 0
        #import data
        print(f"[ batch {batch_index + 1} ] Begin importing data...")
        for tbl_name in ["lineitem", "orders", "part","partsupp", "customer", "nation", "region", "supplier"]:
            tbl_path = f"{BATCH_DIR}/{tbl_name}_batch_{batch_index}.tbl"
            print(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
            cursor.execute(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
        print(f"[ batch {batch_index + 1}] Finish importing data...")
        
        print(f"[ batch {batch_index + 1}] Begin selecting data...")
        
        for sql_file in sql_files:
            sql_path = os.path.join(SQL_DIR, sql_file)
            print(f"Executing: {sql_path}")
            
            start_time = time.time()
            status = execute.execute_sql_file(cursor, sql_path)
            end_time = time.time()

            if status == "CANCELLED":
                query_time_map[sql_file].append(-1)
                cancel_query += 1
                total_cancelled += 1
            else:
                elapsed = end_time - start_time
                query_time_map[sql_file].append(elapsed)
        print(f"[ batch {batch_index + 1}] Finish selecting data , cancel query: {cancel_query}")
    return 
    
def test1():
    #split_all_tbl_files() 

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    # CREATE TABLE

    print("Creating tables...")
    for tbl_name in ["lineitem", "orders", "part"," partsupp", "customer", "nation", "region", "supplier"]:
        cursor.execute(f"DROP TABLE IF EXISTS {tbl_name} CASCADE")
    status = execute.execute_sql_file(cursor, CREATE_FILE,False)

    sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    random.shuffle(sql_files)

    total_cancelled = 0
    query_time_map = {sql_file: [] for sql_file in sql_files}

    # test num bratches unit test : import + select
    for batch_index in range(NUM_BATCHES):
        print(f"\n========== Executing Batch {batch_index + 1}/{NUM_BATCHES} ==========")
        cancel_query = 0
        #import data
        print(f"[ batch {batch_index + 1} ] Begin importing data...")
        for tbl_name in ["lineitem", "orders", "part","partsupp", "customer", "nation", "region", "supplier"]:
            tbl_path = f"{BATCH_DIR}/{tbl_name}_batch_{batch_index}.tbl"
            print(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
            cursor.execute(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
        print(f"[ batch {batch_index + 1}] Finish importing data...")

        # select data
        print(f"[ batch {batch_index + 1}] Begin selecting data...")
        for sql_file in sql_files:
            sql_path = os.path.join(SQL_DIR, sql_file)
            print(f"Executing: {sql_path}")
            
            start_time = time.time()
            status = execute.execute_sql_file(cursor, sql_path)
            end_time = time.time()

            if status == "CANCELLED":
                query_time_map[sql_file].append(-1)
                cancel_query += 1
                total_cancelled += 1
            else:
                elapsed = end_time - start_time
                query_time_map[sql_file].append(elapsed)
            #conn.commit()
        print(f"[ batch {batch_index + 1}] Finish selecting data , cancel query: {cancel_query}")

    cursor.close()
    conn.close()
    print("All queries executed.")
    print(f"Total cancelled queries: {total_cancelled}")
    plot.plot_query_times(query_time_map)    
    plot.write_query_times_to_excel(query_time_map)


def main():
    #test_single_same_batchs()
    #simple_test()
    test1()

if __name__ == "__main__":
    main()
