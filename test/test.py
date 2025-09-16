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
# import torch
# from qppnet.model_arch import QPPNet
# from qppnet.dataset.terrier_tpch_dataset.terrier_utils import TerrierTPCHDataSet
# from qppnet.dataset.postgres_tpch_dataset.tpch_utils import PSQLTPCHDataSet
# from qppnet.dataset.oltp_dataset.oltp_utils import OLTPDataSet
import argparse
from collections import OrderedDict

parser = argparse.ArgumentParser(description='QPPNet Arg Parser')

# qppnet args
parser.add_argument('--data_dir', type=str, default='./qppnet/data/pgdata',
                    help='Dir containing train data')
parser.add_argument('--dataset', type=str, default='PSQLTPCH',
                    help='Select dataset [PSQLTPCH | TerrierTPCH | OLTP]')
parser.add_argument('--test_time', action='store_true', default='true',
                    help='if in testing mode')
parser.add_argument('-dir', '--save_dir', type=str, default='qppnet/saved_model',
                    help='Dir to save model weights (default: ./saved_model)')
parser.add_argument('--lr', type=float, default=1e-3,
                    help='Learning rate (default: 1e-3)')
parser.add_argument('--scheduler', action='store_true')
parser.add_argument('--step_size', type=int, default=1000,
                    help='step_size for StepLR scheduler (default: 1000)')
parser.add_argument('--gamma', type=float, default=0.95,
                    help='gamma in Adam (default: 0.95)')
parser.add_argument('--SGD', action='store_true',
                    help='Use SGD as optimizer with momentum 0.9')
parser.add_argument('--batch_size', type=int, default=32,
                    help='Batch size used in training (default: 32)')
parser.add_argument('-s', '--start_epoch', type=int, default=0,
                    help='Epoch to start training with (default: 0)')
parser.add_argument('-t', '--end_epoch', type=int, default=200,
                    help='Epoch to end training (default: 200)')
parser.add_argument('-epoch_freq', '--save_latest_epoch_freq', type=int, default=100)
parser.add_argument('-logf', '--logfile', type=str, default='train_loss.txt')
parser.add_argument('--mean_range_dict', type=str)

def save_opt(opt, logf):
    """Print and save options
    It will print both current options and default values(if different).
    It will save options into a text file / [checkpoints_dir] / opt.txt
    """
    message = ''
    message += '----------------- Options ---------------\n'
    for k, v in sorted(vars(opt).items()):
        comment = ''
        default = parser.get_default(k)
        if v != default:
            comment = '\t[default: %s]' % str(default)
        message += '{:>25}: {:<30}{}\n'.format(str(k), str(v), comment)
    message += '----------------- End -------------------'
    print(message)
    logf.write(message)
    logf.write('\n')

#db config
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

TEST_HOME="/SSD/00/yyk/Sqms-On-Postgresql/contrib/sqms/test"

# workload dir
SQL_DIR = "./tpch_query_slow"
# data dir before split
TBL_DIR = TEST_HOME + "/tbl_data"           
# data dir after split
BATCH_DIR = TEST_HOME + "/tbl_batches"       
# create table file
CREATE_FILE = TEST_HOME + "/tpch-create.sql"       
PKEYS_FILE = TEST_HOME + "/tpch-pkeys.sql"
FKEYS_FILE = TEST_HOME + "/tpch-alter.sql"
CREATE_IDX_FILE = TEST_HOME + "/tpch-index.sql"
DROP_IDX_FILE = TEST_HOME + "/tpch-index-drop.sql"
# spilt nums
NUM_BATCHES = 1
# output dir
OUTPUT_DIR = "./output"

def create_timestamped_folder(base_dir="output"):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    folder_path = os.path.join(base_dir, timestamp)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

# predict time for a query using qppnet
# def get_time():
    opt = parser.parse_args()
    if opt.dataset == "PSQLTPCH":
        dataset = PSQLTPCHDataSet(opt)

    print("dataset_size", dataset.datasize)
    torch.set_default_tensor_type(torch.FloatTensor)
    qpp = QPPNet(opt)
    
    offset = 0
    query = 1
    rq_results = []
    time_results = {}
    print(f"dataset.num_grps: {len(dataset.num_grps)}")
    for grpnum in dataset.num_grps:
        print('-'*90)
        print(f'query: {query} evaluation.')
        query += 1
        
        test_data_q = dataset.test_dataset[0:1]
        
        offset += grpnum
        qpp.evaluate(test_data_q)
        rq_results.append(qpp.last_rq)
        time_results[query-1] = (qpp.last_pred_time, qpp.last_actual_time)
    
    assert(len(time_results) == 1)
    for i, rq in enumerate(rq_results):
        print(f" query: {i+1}, rq: {rq}", 'time:', time_results[i+1])
    return (list(time_results.values())[0][0],list(time_results.values())[0][1],rq_results[0])


# read create index statements from a file
def extract_create_index_statements(file_path):
    index_statements = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.upper().startswith("CREATE INDEX"):
                index_statements.append(line if line.endswith(";") else line + ";")
    return index_statements


def wrap_plan_for_qppnet(plan_dict):
    return {
        "Plan": plan_dict,
        "Slice statistics": []
    }

# def qppnet_plan_collector(sql_files, conn):
    cursor = conn.cursor()
    output_dir = "./qppnet/data/pgdata"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    opt = parser.parse_args()

    total_cancelled = 0
    query_time_map = {sql_file: [] for sql_file in sql_files}
    actual_time_map = {sql_file: [] for sql_file in sql_files}

    for batch_index in range(NUM_BATCHES):
        print(f"\n========== Executing Batch {batch_index + 1}/{NUM_BATCHES} ==========")
        cancel_query = 0

        print(f"[ batch {batch_index + 1}] Begin selecting data...")
        for sql_file in sql_files:
            sql_path = os.path.join(SQL_DIR, sql_file)
            print(f"Executing: {sql_path}")
            
            start_time = time.time()
            plan_json = execute.execute_explain_sql_file(cursor, sql_path)
            end_time = time.time()
            
            if plan_json != "":
                wrapped_plan = wrap_plan_for_qppnet(plan_json)
                output_path = os.path.join(output_dir, f"{sql_file}.txt")
                with open(output_path, 'a') as f:
                    f.write(json.dumps(wrapped_plan) + "\n")

                t,rq = get_time()
                query_time_map[sql_file].append(str(t* 0.100)  +"|"+str(end_time - start_time) + "|" + str(rq))

                if os.path.exists(output_path):
                    os.remove(output_path)
                else:
                    print(f"{output_path} deltet failed, fatal error")
                    return -1
            else:
                print(f"error plan")
                return -1

        print(f"[ batch {batch_index + 1}] Finish selecting data , cancel query: {cancel_query}")
    cursor.close()
    conn.close()
    plot.plot_query_times(query_time_map)    
    plot.write_query_times_to_excel(query_time_map)
    print("All queries executed.")
    return

#create tables and import data
def InitEnv(conn,type: TestType, import_data: bool = False, init_data_size: int = 5):
    cursor = conn.cursor()
    #re-create tables

    if(import_data):
        print("Creating tables...")
        for tbl_name in ["lineitem", "orders", "part"," partsupp", "customer", "nation", "region", "supplier"]:
            cursor.execute(f"DROP TABLE IF EXISTS {tbl_name} CASCADE")
        status = execute.execute_sql_file(cursor,CREATE_FILE,False)
        #clear index
        status = execute.execute_sql_file(cursor,DROP_IDX_FILE,False)
        #set primary key
        status = execute.execute_sql_file(cursor,PKEYS_FILE,False)
        #set foreign key (MARK: don't set foregin key)
        #status = execute.execute_sql_file(cursor,FKEYS_FILE,False)
        for batch_index in range(init_data_size):
            print(f"[ batch {batch_index + 1} ] Begin importing data...")
            for tbl_name in ["lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier"]:
                tbl_path = f"{BATCH_DIR}/{tbl_name}_batch_{batch_index}.tbl"
                print(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
                cursor.execute(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
                print(f"[ batch {batch_index + 1}] Finish importing data...")
    else:
        print("table and data have exisit, skip crete table and import data")
    if type == TestType.SQMS:
        cursor.execute(f"set sqms.query_min_duration = '5s'")
    cursor.close()
    return 

def replicate_sql_files(src_dir, dst_dir, k):
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    original_files = [f for f in os.listdir(src_dir) if f.endswith(".sql")]
    new_files = []

    for file_name in original_files:
        base_name = os.path.splitext(file_name)[0]
        src_path = os.path.join(src_dir, file_name)
        for i in range(1, k + 1):
            # build new file name
            new_name = f"{base_name}_{i}.sql"
            dest_path = os.path.join(dst_dir, new_name)
            # copy src file content to dest file
            if os.path.exists(dest_path):
                raise FileExistsError(f"dst file has exist: {dest_path}")
            shutil.copyfile(src_path, dest_path)
            new_files.append(new_name)
    return new_files

def get_names_with_value_one(excel_path):
    df = pd.read_excel(excel_path)
    filtered = df[df.iloc[:, 2] == 1]
    name_list = filtered.iloc[:, 1].tolist()
    return name_list

# static workload test
def StaticWorkloadTest(type,impor_data: bool, init_data_size: int):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    sql_files = []
    
    batch_size = 3
    SQL_DIR = "./tpch_query_slow"
    order_file_path = "./sql_order.txt"
    rep_sql_file_path = SQL_DIR + "_rep"
    InitEnv(conn,type,False,init_data_size)
    
    # init qppnet and prepare plan dir
    temp_plan_dir = "./qppnet/data/pgdata"
    if type == TestType.QPPNET:
        if os.path.exists(temp_plan_dir):
            shutil.rmtree(temp_plan_dir)
        os.makedirs(temp_plan_dir)
        opt = parser.parse_args()

    cursor = conn.cursor()

    if 0 == os.path.exists(rep_sql_file_path) and type == TestType.SQMS:
        sql_files = replicate_sql_files(SQL_DIR, rep_sql_file_path, batch_size)
        sql_files = [f for f in os.listdir(rep_sql_file_path) if f.endswith(".sql")]
        random.shuffle(sql_files)
    
        if os.path.exists(order_file_path):
            with open(order_file_path, "r") as f:
                sql_files = [line.strip() for line in f.readlines()]
        else:
            sql_files = [f for f in os.listdir(rep_sql_file_path) if f.endswith(".sql")]
            random.shuffle(sql_files)
            with open(order_file_path, "w") as f:
                for fname in sql_files:
                    f.write(fname + "\n")

    if type == TestType.EXSIST:
        sql_files = get_names_with_value_one(TEST_HOME + "/output/20250615_152306/query_run_time.xlsx")
    
    total_sqls = len(sql_files)
    index_list = extract_create_index_statements(CREATE_IDX_FILE)

    # query total time in every btach
    query_time_map = OrderedDict() 
    # run time of each query, if the query has been cancel, it will be consider as -1
    query_run_map = OrderedDict()
    # cancel query name
    cancel_query_map = { i+1: [] for i in range(batch_size)}
    index_interval = total_sqls // len(index_list) if index_list else total_sqls

    run_cnt = 0             # query run success
    total_cnt = 0           # all querys try to run = run_cnt + total_cancelled
    total_cancelled = 0     # query cancelled
    current_total_time = 0.0
    idx_offset = 0

    for sql_file in sql_files:
        sql_path = os.path.join(rep_sql_file_path, sql_file)
        print(f"Executing: {sql_path}")
        total_cnt += 1

        if type != 2:
            start_time = time.time()
            status = execute.execute_sql_file(cursor, sql_path)
            end_time = time.time()
            
            if status == "CANCELLED":
                total_cancelled += 1
                query_run_map[total_cnt] = [sql_file,-1]
            elif status == "TIMEOUT":
                run_cnt += 1
                current_total_time += 20
                query_run_map[total_cnt] = [sql_file,20]
            else:
                elapsed = end_time - start_time
                current_total_time += elapsed
                query_run_map[total_cnt] = [sql_file,elapsed]
                run_cnt += 1
        # else: 
        #     # run qppnet test
        #     start_time = time.time()
        #     plan_json = execute.execute_explain_sql_file(conn, sql_path)
            
        #     end_time = time.time()

        #     if plan_json != None:
        #         wrapped_plan = wrap_plan_for_qppnet(plan_json)
        #         temp_plan_path = os.path.join(temp_plan_dir, f"{sql_file}.txt")
        #         with open(temp_plan_path, 'a') as f:
        #             f.write(json.dumps(wrapped_plan) + "\n")

        #         pred_t,actual_t,rq = get_time()
        #         pred_t = pred_t / 10.00
        #         actual_t = actual_t / 10.00
        #         if pred_t >= 5.00:
        #             total_cancelled += 1
        #             query_run_map[total_cnt] = [sql_file,-1,pred_t,actual_t,rq]
        #         else:
        #             elapsed = end_time - start_time
        #             current_total_time += elapsed
        #             query_run_map[total_cnt] = [sql_file,1,pred_t,actual_t,rq]
        #             run_cnt += 1

        #         if os.path.exists(temp_plan_path):
        #             os.remove(temp_plan_path)
        #         else:
        #             print(f"{temp_plan_path} deltet failed, fatal error")
        #             return -1
        #     else:
        #         print(f"error plan")
        #         continue

        # mark time when 10 querys has run
        if total_cnt % 5 == 0:
            query_time_map[total_cnt] = current_total_time
            print(f"[Info] Recorded time after {run_cnt} successful queries: {current_total_time:.2f}s")

        #try to create a new index after each index_interval 
        # if total_cnt % index_interval == 0 and idx_offset < len(index_list):
        #     print(f"[Index] Creating index: {index_list[idx_offset]}")
        #     cursor.execute(index_list[idx_offset])
        #     idx_offset += 1

    print(f"Total queries attempted: {total_cnt}")
    print(f"Total cancelled queries: {total_cancelled}")
    print(f"Total successful queries: {run_cnt}")

    output_folder = create_timestamped_folder("output")
    plot.plot_query_time(query_time_map, title="Query Time vs Count", output_path=os.path.join(output_folder, "query_time_progress.png"))
    plot.write_batch_query_time_to_excel(query_time_map, output_path=os.path.join(output_folder, "query_batch_time.xlsx"))
    plot.write_query_time_to_excel(query_run_map, output_path=os.path.join(output_folder, "query_run_time.xlsx"))
    
    cursor.close()
    conn.close()
    return

def main():

    StaticWorkloadTest(TestType.SQMS,False,5)
    # conn = psycopg2.connect(**DB_CONFIG)
    # conn.autocommit = True
    # cursor = conn.cursor()

    # sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith(".sql")]
    # random.shuffle(sql_files)

    # qppnet_plan_collector(sql_files,conn)
    print("Finish Test.")

if __name__ == "__main__":
    main()