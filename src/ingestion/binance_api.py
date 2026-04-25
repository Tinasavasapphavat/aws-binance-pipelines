import pandas as pd
import requests 
from datetime import datetime


class BINANCE:
    def __init__(self, url : str ,symbol : str, limit: int, path , param_path:str):
        self.url : str = url
        self.symbol : str = symbol
        self.limit : int = limit
        self.param_path = path / param_path
        self.path = path
        

    def get_latest_ts(self):
        try:
            
            with open(self.param_path,"r") as f:
                content = f.read().strip()
            start = int(content) if content else 1767225600000
            self.start_ts : int = start
            self.end_ts : int = start + 3600000
            dt_start = datetime.utcfromtimestamp(self.start_ts/1000).strftime("%Y-%m-%d %H:%M:%S")
            dt_end = datetime.utcfromtimestamp(self.end_ts/1000).strftime("%Y-%m-%d %H:%M:%S")
            print(f"Fetching {dt_start} - {dt_end}")
        except Exception as e:
            raise e

        
    def get_data(self):
        lst = []
        start = self.start_ts
        end = self.end_ts
        try:
            while start < end:
                params : dict = {
                                    "symbol" : self.symbol,
                                    "startTime": start,
                                    "endTime": end ,
                                    "limit": self.limit
                                    }

                res = requests.get(self.url,params=params)
                if res.status_code == 200:
                    data = res.json()
                else:
                   res.raise_for_status() 

                if len(data) == self.limit:
                    end = (start+end)//2
                else:
                    dt_start = datetime.utcfromtimestamp(start/1000).strftime("%Y-%m-%d %H:%M:%S")
                    dt_end = datetime.utcfromtimestamp(end/1000).strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Fetching {dt_start} - {dt_end} : {len(data)} rows")
                    df = pd.DataFrame(data)
                    if len(df) != 0:
                        lst.append(df)
                    start = end+1
                    end = (self.end_ts + start)//2
            self.df = pd.concat(lst,axis = 0).reset_index(drop= True)
            self.max_ts = int(end+1)
            print(f"FETCH {self.symbol} SUCCEEDED")

        except Exception as e:
            raise e
    

    def save_tmp(self,raw_path):
        try:
            path = self.path/raw_path
            self.df.to_csv(path,index=False)
            print("SAVE RAW DATA SUCCEEDED")
        except Exception as e:
            print("SAVE RAW DATA FAILED")
            raise e

    def update_latest_ts(self):
        try:
            with open(self.param_path,"w") as f:
                f.write(str(self.max_ts))
            print("UPDATE NEW TIMESTAMP SUCCEEDED")
        except Exception as e:
            raise e
