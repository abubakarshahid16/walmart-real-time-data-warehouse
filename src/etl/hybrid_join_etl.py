import argparse
import csv
import threading
import time
from collections import defaultdict, deque
from dateutil import parser as date_parser
import pandas as pd
import mysql.connector

# ============================================================
# CONFIGURATION & TUNING PARAMETERS
# ============================================================
hS = 10000             # Hash Table Slots
vP = 500               # Partition Size
STREAM_SLEEP = 0.0001  
COMMIT_BATCH_SIZE = 1000 

class HybridJoinETL:

    def __init__(self, db_config, paths):
        print("\n" + "="*60)
        print("[INIT] INITIALIZING HYBRID JOIN SYSTEM")
        print("="*60)
        self.db_config = db_config
        self.paths = paths
        self.stream_buffer = deque()
        self.stream_lock = threading.Lock()
        self.hash_table = defaultdict(list)
        self.hash_slots_used = 0
        self.queue = deque()
        self.producer_done = False
        self.facts_to_commit = 0
        self.seen_orders = set() 

        self.load_and_preprocess_master_data()
        self._partition_product_master()

        self.prod_to_partition = {}
        for i, part in enumerate(self.partitions):
            for pid in part["Product_ID"].astype(str).tolist():
                self.prod_to_partition[pid] = i

        self.conn = mysql.connector.connect(**db_config)
        self.conn.autocommit = False 
        print(f"[INIT] Connected to MySQL Database: {db_config['database']}")
        
        self.cache = {
            'customer': {},
            'store': {},
            'supplier': {},
            'product': {},
            'date': {}
        }
        self._preload_dimension_caches()

        print("[INIT] System Ready. Waiting for Data Stream...")

    def load_and_preprocess_master_data(self):
        print("[PREPROCESS] Cleaning Master Data...")
        self.product_df = pd.read_csv(self.paths["product"], dtype=str).fillna("Unknown")
        self.customer_df = pd.read_csv(self.paths["customer"], dtype=str).fillna("Unknown")

        self.product_df.drop_duplicates(subset=['Product_ID'], inplace=True)
        self.customer_df.drop_duplicates(subset=['Customer_ID'], inplace=True)

        self.price_col_name = None
        possible_price_cols = ['price', 'Price', 'price$', 'Price$']
        for col in possible_price_cols:
            if col in self.product_df.columns:
                self.price_col_name = col
                break
        
        if not self.price_col_name:
            raise KeyError(f"Price column missing. Checked: {possible_price_cols}")
        
        self.product_df[self.price_col_name] = (
            self.product_df[self.price_col_name]
            .astype(str).str.replace('$', '', regex=False)
        )
        self.product_df[self.price_col_name] = pd.to_numeric(self.product_df[self.price_col_name], errors="coerce").fillna(0.0)
        
        self.product_df["supplierID"] = pd.to_numeric(self.product_df["supplierID"], errors='coerce').fillna(0).astype(int)
        self.product_df["storeID"] = pd.to_numeric(self.product_df["storeID"], errors='coerce').fillna(0).astype(int)
        print("[PREPROCESS] Data Cleaned Successfully.")

    def _preload_dimension_caches(self):
        print("[CACHE] Warming up Memory Cache...")
        cursor = self.conn.cursor()
        
        tables = [
            ("DimCustomer", "Customer_ID", "CustomerKey", "customer"),
            ("DimProduct", "Product_ID", "ProductKey", "product"),
            ("DimDate", "FullDate", "DateKey", "date"),
            ("DimStore", "StoreID", "StoreKey", "store"),
            ("DimSupplier", "SupplierID", "SupplierKey", "supplier")
        ]
        
        for table, id_col, key_col, cache_name in tables:
            try:
                cursor.execute(f"SELECT {id_col}, {key_col} FROM {table}")
                for row in cursor:
                    self.cache[cache_name][str(row[0])] = row[1]
            except:
                print(f"[CACHE] Warning: Table {table} not found or empty.")
                
        cursor.close()
        print("[CACHE] Cache Process Complete.")

    def _partition_product_master(self):
        self.partitions = []
        total = len(self.product_df)
        count = (total + vP - 1) // vP
        print(f"[PARTITION] Simulated Disk: Split {total} products into {count} partitions.")
        for i in range(count):
            start = i * vP
            end = min(start + vP, total)
            self.partitions.append(self.product_df.iloc[start:end].copy())

    def producer_stream(self):
        print("[PRODUCER] Starting Data Stream...")
        count = 0
        with open(self.paths["stream"], "r", newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                unique_id = f"{row['orderID']}_{row['Product_ID']}"
                if unique_id in self.seen_orders:
                    continue
                self.seen_orders.add(unique_id)

                txn = {
                    "orderID": row["orderID"],
                    "Customer_ID": row["Customer_ID"],
                    "Product_ID": row["Product_ID"],
                    "Quantity": int(row["quantity"]),
                    "Date": row["date"]
                }
                
                with self.stream_lock:
                    self.stream_buffer.append(txn)
                count += 1
                
                if count % 1000 == 0:
                    time.sleep(STREAM_SLEEP)
                    
        self.producer_done = True
        print(f"[PRODUCER] Finished. Pushed {count} unique records.")

    def get_or_create_customer(self, cid, cursor):
        cid = str(cid)
        if cid in self.cache['customer']: return self.cache['customer'][cid]

        match = self.customer_df[self.customer_df["Customer_ID"] == cid]
        d = match.iloc[0].to_dict() if not match.empty else {}
        
        ms = d.get("Marital_Status", 0)
        ms_int = int(ms) if str(ms).isdigit() else 0
        
        insert = "INSERT INTO DimCustomer (Customer_ID, Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        vals = (cid, d.get("Gender"), d.get("Age"), d.get("Occupation"), d.get("City_Category"), d.get("Stay_In_Current_City_Years"), ms_int)
        cursor.execute(insert, vals)
        
        key = cursor.lastrowid
        self.cache['customer'][cid] = key 
        return key

    def get_or_create_supplier(self, sid, sname, cursor):
        sid = str(sid)
        if sid in self.cache['supplier']: return self.cache['supplier'][sid]
        
        cursor.execute("INSERT INTO DimSupplier (SupplierID, SupplierName) VALUES (%s,%s)", (sid, sname))
        key = cursor.lastrowid
        self.cache['supplier'][sid] = key
        return key

    def get_or_create_store(self, store_id, store_name, cursor):
        sid = str(store_id)
        if sid in self.cache['store']: return self.cache['store'][sid]
        
        cursor.execute("INSERT INTO DimStore (StoreID, StoreName) VALUES (%s,%s)", (sid, store_name))
        key = cursor.lastrowid
        self.cache['store'][sid] = key
        return key

    def get_or_create_product(self, product_row, supplier_key, store_key, cursor):
        pid = str(product_row["Product_ID"])
        if pid in self.cache['product']: return self.cache['product'][pid]
        
        insert = "INSERT INTO DimProduct (Product_ID, Product_Category, Price, SupplierKey, StoreKey) VALUES (%s,%s,%s,%s,%s)"
        vals = (pid, product_row["Product_Category"], float(product_row[self.price_col_name]), supplier_key, store_key)
        cursor.execute(insert, vals)
        
        key = cursor.lastrowid
        self.cache['product'][pid] = key
        return key

    def get_or_create_date(self, dt_str, cursor):
        try:
            dt = date_parser.parse(dt_str).date()
            full_date = dt.isoformat()
        except:
            full_date = "2017-01-01"
            dt = date_parser.parse(full_date).date()

        if full_date in self.cache['date']: return self.cache['date'][full_date]

        month = dt.month
        season = "Winter" if month in (12, 1, 2) else "Spring" if month in (3, 4, 5) else "Summer" if month in (6, 7, 8) else "Fall"
        insert = "INSERT INTO DimDate (FullDate, Day, Month, MonthName, Quarter, Year, Week, Weekday, Season) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        vals = (full_date, dt.day, month, dt.strftime("%B"), (month - 1) // 3 + 1, dt.year, int(dt.strftime("%U")), dt.strftime("%A"), season)
        cursor.execute(insert, vals)
        
        key = cursor.lastrowid
        self.cache['date'][full_date] = key
        return key

    def insert_fact(self, txn, product_row, cursor):
        quantity = int(txn["Quantity"])
        price = float(product_row[self.price_col_name])
        revenue = round(quantity * price, 2)
        
        supplier_key = self.get_or_create_supplier(product_row["supplierID"], product_row["supplierName"], cursor)
        store_key = self.get_or_create_store(product_row["storeID"], product_row["storeName"], cursor)
        customer_key = self.get_or_create_customer(txn["Customer_ID"], cursor)
        product_key = self.get_or_create_product(product_row, supplier_key, store_key, cursor)
        date_key = self.get_or_create_date(txn["Date"], cursor)
        
        insert = "INSERT INTO FactSales (CustomerKey, ProductKey, SupplierKey, StoreKey, DateKey, Order_ID, Quantity, Revenue) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(insert, (customer_key, product_key, supplier_key, store_key, date_key, txn["orderID"], quantity, revenue))
        self.facts_to_commit += 1

    def hybridjoin(self):
        print("[HYBRIDJOIN] Engine Started.")
        total_matches = 0
        cursor = self.conn.cursor()

        while True:
            w = hS - self.hash_slots_used
            if w > 0:
                with self.stream_lock:
                    while self.stream_buffer and w > 0:
                        txn = self.stream_buffer.popleft()
                        key = txn["Product_ID"]
                        self.hash_table[key].append(txn)
                        self.queue.append(key)
                        self.hash_slots_used += 1
                        w -= 1

            if self.producer_done and not self.hash_table:
                break

            if not self.queue:
                time.sleep(0.05)
                continue

            oldest_key = self.queue.popleft()
            if oldest_key not in self.prod_to_partition:
                if oldest_key in self.hash_table:
                    self.hash_slots_used -= len(self.hash_table[oldest_key])
                    del self.hash_table[oldest_key]
                continue

            pidx = self.prod_to_partition[oldest_key]
            partition = self.partitions[pidx]
            
            for _, prod_row in partition.iterrows():
                pid = str(prod_row["Product_ID"])
                if pid in self.hash_table:
                    for txn in self.hash_table[pid]:
                        self.insert_fact(txn, prod_row, cursor)
                        total_matches += 1
                        self.hash_slots_used -= 1
                    del self.hash_table[pid]

            if self.facts_to_commit >= COMMIT_BATCH_SIZE:
                self.conn.commit()
                print(f"[HYBRIDJOIN] COMMIT | {self.facts_to_commit} facts inserted. Total matches: {total_matches}")
                self.facts_to_commit = 0
        
        if self.facts_to_commit > 0:
            self.conn.commit()
            print(f"[HYBRIDJOIN] FINAL COMMIT | {self.facts_to_commit} facts. Total matches: {total_matches}")
        
        cursor.close()
        print("[HYBRIDJOIN] Completed.")

    def close(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()

def main():
    db_config = {"host": "localhost", "user": "root", "password": "abubakar123", "database": "BlackDW"}
    paths = {
        "stream": "D:/semster6/data_warehouse/PROJECT/transactional_data.csv",
        "product": "D:/semster6/data_warehouse/PROJECT/product_master_data.csv",
        "customer": "D:/semster6/data_warehouse/PROJECT/customer_master_data.csv"
    }

    etl = HybridJoinETL(db_config=db_config, paths=paths)

    try:
        producer = threading.Thread(target=etl.producer_stream)
        consumer = threading.Thread(target=etl.hybridjoin)

        producer.start()
        consumer.start()

        producer.join()
        consumer.join()

        print("[MAIN] HYBRIDJOIN ETL completed successfully.")

    except Exception as e:
        import traceback
        print(f"[MAIN] An error occurred: {e}")
        traceback.print_exc()
    finally:
        etl.close()

if __name__ == "__main__":
    main()
