import os
import psycopg2
import random

# 配置 PostgreSQL 连接信息
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    # "password": "your_password",
    "host": "localhost",
    "port": "44444"
}

#target files path
SQL_DIR = "/home/yyk/Sqms-On-Postgresql/tpch_query"

def execute_sql_file(cursor, file_path):

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    sql = "".join(lines[3:])
    
    if not sql.strip():
        print(f"Skipping {file_path}: No valid SQL found.")
        return
    
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        print(f"Results from {file_path}:")
        for row in results:
            print(row)
    except psycopg2.ProgrammingError:
        print(f"Executed {file_path} (no results to fetch)")

def main():
  
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    random.shuffle(sql_files) 

    for sql_file in sql_files:
        sql_path = os.path.join(SQL_DIR, sql_file)
        print(f"Executing: {sql_path}")
        execute_sql_file(cursor, sql_path)
        conn.commit()

    cursor.close()
    conn.close()
    print("All queries executed.")

if __name__ == "__main__":
    main()
