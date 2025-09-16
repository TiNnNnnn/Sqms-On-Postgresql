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
import time
from enum import Enum

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "host": "localhost",
    "port": "55555"
}

class TestType(Enum):
    SQMS = 0,
    QPPNET = 1,
    EXSIST = 2
    UNKNOW = 3

def create_timestamped_folder(base_dir="output"):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    folder_path = os.path.join(base_dir, timestamp)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

TEST_HOME="/SSD/00/yyk/Sqms-On-Postgresql/contrib/sqms/test"
# create table file
CREATE_FILE = TEST_HOME + "/tpch-create.sql"       
PKEYS_FILE = TEST_HOME + "/tpch-pkeys.sql"
FKEYS_FILE = TEST_HOME + "/tpch-alter.sql"
CREATE_IDX_FILE = TEST_HOME + "/tpch-index.sql"
DROP_IDX_FILE = TEST_HOME + "/tpch-index-drop.sql"