import psycopg2
import os 
import time 
import execute,plot
from collections import OrderedDict
from common import DB_CONFIG, TestType, create_timestamped_folder, \
        CREATE_FILE,PKEYS_FILE,FKEYS_FILE,CREATE_IDX_FILE,DROP_IDX_FILE


class BaseTest:
    def __init__(self, test_type: TestType,sql_dir: str,data_dir: str):
        self.type = test_type 
        self.sql_dir = sql_dir
        self.timeout = 99999
        self.data_dir = None

class IncreaseTest(BaseTest):
    def __init__(self, batch_sz: int,init_data_size: int, data_dir: str):
        super().__init__(TestType.SQMS)
        self.batch_size = batch_sz
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.init_data_size = init_data_size
        self.data_dir = data_dir
       
    def test_multi_batches(self, need_init : bool): 
        if need_init:
            self.init_data()
        for i in range(self.batch_size):
            self.test_single_batch(i)
    
    def test_single_batch(self, i: int):    
        self.insert_data()

        query_time_map = OrderedDict() 
        query_run_map = OrderedDict()
        run_cnt = 0             # query run success
        cancel_cnt = 0          # query cancelled
        total_cnt = 0           # all querys try to run = run_cnt + total_cancelled
        current_total_time = 0.0

        cursor = self.conn.cursor()
        sql_files = [f for f in os.listdir(self.sql_dir) if f.endswith(".sql")]
        for sql_file in sql_files:
            sql_path = os.path.join(self.sql_dir, sql_file)
            print(f"Executing: {sql_path} ...")
            start_time = time.time()
            status = execute.execute_sql_file(cursor, sql_path)
            end_time = time.time()

            total_cnt += 1
            elapsed = end_time - start_time
            current_total_time += elapsed
            
            if status == "CANCELLED":
                cancel_cnt += 1
                query_run_map[total_cnt] = [sql_file,-1]
            else:
                run_cnt += 1
                query_run_map[total_cnt] = [sql_file,elapsed]

            if total_cnt % 5 == 0:
                query_time_map[total_cnt] = current_total_time
                print(f"[Info] Recorded time after {run_cnt} successful queries: {current_total_time:.2f}s")

        print(f"Total queries attempted: {total_cnt}")
        print(f"Total cancelled queries: {cancel_cnt}")
        print(f"Total successful queries: {run_cnt}")

        output_folder = create_timestamped_folder("output")
        plot.plot_query_time(query_time_map, title="Query Time vs Count", output_path=os.path.join(output_folder, "query_time_progress.png"))
        plot.write_batch_query_time_to_excel(query_time_map, output_path=os.path.join(output_folder, "query_batch_time.xlsx"))
        plot.write_query_time_to_excel(query_run_map, output_path=os.path.join(output_folder, "query_run_time.xlsx"))
    
    def import_data(self):
        return  
        
    def init_data(self):
        cursor = self.conn.cursor()
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
        for batch_index in range(self.init_data_size):
            print(f"[ batch {batch_index + 1} ] Begin importing data...")
            for tbl_name in ["lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier"]:
                tbl_path = f"{self.data_dir}/{tbl_name}_batch_{batch_index}.tbl"
                print(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
                cursor.execute(f"copy {tbl_name} from '{tbl_path}' with delimiter as '|' NULL '' ")
                print(f"[ batch {batch_index + 1}] Finish importing data...")
        return 
