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
import time, torch
import multiprocessing as mp
from functools import partial
from multiprocessing import Manager
import re
import execute

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "localhost",
    "port": "44444"
}

SQL_DIR = "./tpch_query"
OUTPUT_DIR = "./qppnet/data/pgdata"
# data dir after split
BATCH_DIR = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test2/tbl_batches"       
# create table file
CREATE_FILE = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test2/tpch-create.sql"   
PKEYS_FILE = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test2/tpch-pkeys.sql"
FKEYS_FILE = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test2/tpch-alter.sql"
CREATE_IDX_FILE = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test2/tpch-index.sql"
DROP_IDX_FILE = "/home/yyk/Sqms-On-Postgresql/contrib/sqms/test2/tpch-index-drop.sql"

def classify_sql_statements(raw_sql):
    """
    将原始 SQL 按 ; 分割并分类为 create/select/drop 三类语句。
    """
    statements = [stmt.strip() for stmt in raw_sql.split(';') if stmt.strip()]
    create_stmts, select_stmts, drop_stmts = [], [], []

    for stmt in statements:
        normalized = re.sub(r'--.*', '', stmt).strip().lower()  # 去掉单行注释和空白
        first_word = re.match(r'^\s*(\w+)', normalized)
        if not first_word:
            continue
        keyword = first_word.group(1)
        if keyword == 'create':
            create_stmts.append(stmt)
        elif keyword == 'select':
            select_stmts.append(stmt)
        elif keyword == 'drop':
            drop_stmts.append(stmt)
        else:
            print(f"未识别的 SQL 语句类型：{stmt[:30]}...")

    return create_stmts, select_stmts, drop_stmts


def wrap_plan_for_qppnet(plan_dict):
    return {
        "Plan": plan_dict,
        "Slice statistics": []
    }

def init_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn

def process_one_sql_for_plan(sql_file, repeat):
    conn = init_conn()
    cursor = conn.cursor()
    sql_path = os.path.join(SQL_DIR, sql_file)
    sql_name = os.path.splitext(sql_file)[0]
    output_path = os.path.join(OUTPUT_DIR, f"{sql_name}.txt")

    print(f"Start processing {sql_file}")
    success_count = 0

    with open(sql_path, 'r', encoding='utf-8') as f:
        raw_sql = f.read()
    
    create_stmts, select_stmts, drop_stmts = classify_sql_statements(raw_sql)

    with open(output_path, 'w', encoding='utf-8') as f_out:
        for i in range(repeat):
            try:
                # 执行 CREATE
                for create_stmt in create_stmts:
                    try:
                        cursor.execute(create_stmt)
                        #print(f"[{sql_file} #{i}] Create view successfully")
                    except Exception as e:
                        print(f"[{sql_file} #{i}] Create failed: {e}")
                        conn.rollback()

                # 执行 EXPLAIN SELECT
                for select_stmt in select_stmts:
                    try:
                        explain_sql = f"EXPLAIN (FORMAT JSON, ANALYZE) {select_stmt}"
                        cursor.execute(explain_sql)
                        plan = cursor.fetchall()
                        if plan and isinstance(plan[0][0], list):
                            plan_dict = plan[0][0][0]['Plan']
                            f_out.write(json.dumps(plan_dict) + "\n")
                            success_count += 1
                        else:
                            print(f"[{sql_file} #{i}] Invalid plan format")
                    except Exception as e:
                        print(f"[{sql_file} #{i}] Error during EXPLAIN: {e}")
                        conn.rollback()

                # 执行 DROP
                for drop_stmt in drop_stmts:
                    try:
                        cursor.execute(drop_stmt)
                        conn.commit()
                    except Exception as e:
                        print(f"[{sql_file} #{i}] Drop failed: {e}")
                        conn.rollback()

            except Exception as e:
                print(f"[{sql_file} #{i}] Outer error: {e}")
                conn.rollback()

    cursor.close()
    conn.close()
    print(f"Finished {sql_file} with {success_count}/{repeat} successful plans.")

def process_one_sql_for_average_time(sql_file,query_time_map,repeat):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    time_out = False
    SQL_DIR = "tpch_query_slow2_rep"
    for batch_index in range(repeat):
        sql_path = os.path.join(SQL_DIR, sql_file)
        print(f"Executing: {sql_path}")
                
        start_time = time.time()
        status = execute.execute_sql_file(cursor, sql_path)
        end_time = time.time()

        if status == "CANCELLED":
            query_time_map[sql_file] = 10
            time_out = True
            break
        else:
            query_time_map[sql_file]+= (end_time - start_time)
            time_out = False
    
    if time_out == False:
        query_time_map[sql_file] = query_time_map[sql_file] / repeat
    cursor.close()
    conn.close()
    return

def InitEnv(conn,init_data_size = 2):
    cursor = conn.cursor()
    #re-create tables
    print("Creating tables...")
    for tbl_name in ["lineitem", "orders", "part"," partsupp", "customer", "nation", "region", "supplier"]:
        cursor.execute(f"DROP TABLE IF EXISTS {tbl_name} CASCADE")
    status = execute.execute_sql_file(cursor,CREATE_FILE,False)
    #clear index
    status = execute.execute_sql_file(cursor,DROP_IDX_FILE,False)
    #set primary key
    status = execute.execute_sql_file(cursor,PKEYS_FILE,False)
    #set foreign key
    #status = execute.execute_sql_file(cursor,FKEYS_FILE,False)
    #import data
    for batch_index in range(init_data_size):
        print(f"[ batch {batch_index + 1} ] Begin importing data...")
        for tbl_name in ["lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier"]:
            tbl_path = f"{BATCH_DIR}/{tbl_name}_batch_{batch_index}.tbl"
            print(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
            cursor.execute(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
            print(f"[ batch {batch_index + 1}] Finish importing data...")

    cursor.execute(f"set statement_timeout = '10s'")
    cursor.close()
    return


# Collect QppNet plan for each query
def QppNetPlanCollector(num_workers=16):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    data_size = 5
    repeat = 276
    SQL_DIR = "./tpch_query_mini"
    #InitEnv(conn,data_size)
    conn.close()

    sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    with mp.Pool(processes=num_workers) as pool:
        pool.map(partial(process_one_sql_for_plan, repeat=repeat), sql_files)
    return 

# Collect workload averag etime for each query 
# We run wokload 3 times , and take the average for each query
# Then we can determine a slow query threshold
def WokloadTimeCollector(num_workers=8): 
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    data_size = 5
    repeat = 5
    InitEnv(conn,data_size)
    conn.close()
    order_file_path = "./sql_order2.txt"
    with open(order_file_path, "r") as f:
        sql_files = [line.strip() for line in f.readlines()]
  
    #sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    query_time_map = {sql_file: 0.00 for sql_file in sql_files}
    with Manager() as manager:
        shared_query_time_map = manager.dict(query_time_map) 
        with mp.Pool(processes=num_workers) as pool:
            pool.map(partial(process_one_sql_for_average_time, query_time_map=shared_query_time_map, repeat=repeat), sql_files)
    
        plot.write_average_query_times_to_excel(shared_query_time_map)
    return

if __name__ == "__main__":
    #QppNetPlanCollector(8)
    WokloadTimeCollector(8)
