from clickhouse_connect import get_client
from dotenv import load_dotenv
import os
import pandas as pd
load_dotenv()


class ClickHouse:
    def __init__(self,path :str,data_path : str,table_name:str):
        self.client = get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            port= os.getenv("CLICKHOUSE_PORT"),
            username= os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASS"),
            database=os.getenv("CLICKHOUSE_DB")
        )
        self.table_name = table_name
        self.path = path 
        self.data_path = data_path

    def get_data(self):
        data_path = self.path/self.data_path
        self.df : pd.DataFrame = pd.read_csv(data_path)
        print("GET DATA SUCCEED")

    def casting_dtypes(self):
        self.df['datetime']       = pd.to_datetime(self.df['datetime'], errors='coerce')
        self.df['is_maker']       = self.df['is_maker'].astype(bool)
        self.df['best_match']     = self.df['best_match'].astype(bool)
        self.df['trade_id']       = pd.to_numeric(self.df['trade_id'], errors='coerce')
        self.df['first_trade_id'] = pd.to_numeric(self.df['first_trade_id'], errors='coerce')
        self.df['last_trade_id']  = pd.to_numeric(self.df['last_trade_id'], errors='coerce')
        self.df['timestamp_utc']  = pd.to_numeric(self.df['timestamp_utc'], errors='coerce')
        self.df['price']          = pd.to_numeric(self.df['price'], errors='coerce')
        self.df['quantity']       = pd.to_numeric(self.df['quantity'], errors='coerce')
        print("CASTING DATA SUCCEED")
    
    def load_to_clickhouse(self):
        self.client.insert(table = self.table_name,data=self.df)
        print(f"LOAD TO {self.table_name} SUCCEED")
