import pandas as pd
from datetime import datetime



class LOAD:
    def __init__(self,path ,data_path : str, s3_path: str , symbol : str):
        self.data_path = path/data_path
        self.s3_path = s3_path
        self.symbol = symbol

    def get_data(self):
        self.df : pd.DataFrame = pd.read_csv(self.data_path)
        self.date_time : datetime = pd.to_datetime(self.df['datetime']).max()
        self.date_str : str = self.date_time.strftime("%Y-%m-%d")
        self.ts :str = str(int(self.date_time.timestamp()*1000))
        dt = self.date_time.strftime("%Y-%m-%d %H:%m:%S.%f")
        print(f"START LOADING AT {dt}")


    def load_to_S3(self):
        path = f"{self.s3_path}/{self.date_str}/{self.symbol}_{self.ts}.parquet"
        self.df.to_parquet(path,engine="fastparquet")
        print(f"LOAD TO {path} SUCCEEDED")
