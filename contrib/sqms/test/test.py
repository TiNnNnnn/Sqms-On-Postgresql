import os
import psycopg2
import random

# 配置 PostgreSQL 连接信息
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "localhost",
    "port": "44444"
}

# 目标 SQL 文件路径
SQL_DIR = "./tpch_query_slow"

def execute_sql_file(cursor, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    sql = "".join(lines[3:])
    if not sql.strip():
        print(f"Skipping {file_path}: No valid SQL found.")
        return "SKIPPED"

    try:
        cursor.execute(sql)
        if cursor.description: 
            results = cursor.fetchall()
            print(f"Results from {file_path}:")
            for row in results:
                print(row)
        else:
            print(f"Executed {file_path} (no results to fetch)")
        return "OK"

    except psycopg2.OperationalError as e:
        if "canceling statement due to user request" in str(e):
            print(f"Query cancelled by user in {file_path}")
            return "CANCELLED"
        else:
            print(f"OperationalError in {file_path}: {e}")
            return "ERROR"

    except psycopg2.ProgrammingError as e:
        print(f"ProgrammingError in {file_path}: {e}")
        return "ERROR"

def main():
    cancel_query = 0
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    random.shuffle(sql_files)

    for sql_file in sql_files:
        sql_path = os.path.join(SQL_DIR, sql_file)
        print(f"Executing: {sql_path}")
        status = execute_sql_file(cursor, sql_path)
        if status == "CANCELLED":
            cancel_query += 1
        conn.commit()

    cursor.close()
    conn.close()

    print("All queries executed.")
    print(f"Total cancelled queries: {cancel_query}")

if __name__ == "__main__":
    main()
