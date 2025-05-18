import math
import os
import time
import psycopg2
import random
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# execute single sql
def execute_sql_file(cursor, file_path,select = True):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    sql = "".join(lines)
    if select:
        # Skip the first three lines (comments)
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
    

#execute exaplain sql
def execute_explain_sql_file(cursor, file_path,select = True):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    sql = "".join(lines)
    sql = "explain "+ sql 
    if select:
        # Skip the first three lines (comments)
        sql = "".join(lines[3:])
        if not sql.strip():
            print(f"Skipping {file_path}: No valid SQL found.")
            return "SKIPPED"

    cursor.execute(sql)
    if cursor.description: 
        results = cursor.fetchall()
        print(f"Results from {file_path}:")
        for row in results:
            print(row)
        return results
    else:
        print(f"Executed {file_path} (no results to fetch)")
    return "OK"