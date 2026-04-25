import pandas as pd
import yaml
from datetime import datetime

class PROCESS:
    def __init__(self,symbol,config:dict,path , raw_path : str,transform_path:str):
        self.symbol : str = symbol
        self.dtypes = config['data']['dtypes']
        self.columns = config['data']['columns']
        self.transform_path = path/transform_path
        raw_path = path / raw_path
        self.df : pd.DataFrame= pd.read_csv(raw_path)
        

    def cast_dtypes(self):
        for col , dt in self.dtypes.items():
            self.df[col] = self.df[col].astype(dt)
        print("CASTING DATA TYPE SUCCEEDED")
        
    def rename_columns(self):
        self.df = self.df.rename(columns = self.columns)
        print("RENAME COLUMNS SUCCEEDED")
    
    def transform(self):
        self.df['datetime'] = self.df['timestamp_utc'].apply( lambda x : datetime.utcfromtimestamp(x/1000))
        print("EDIT INFO SUCCEEDED")

    def save_tmp(self):
        try:
            self.df.to_csv(self.transform_path,index=False)
            print("SAVE TRANSFORMED DATA SUCCEEDED")
        except Exception as e:
            print("SAVE RAW DATA FAILED")
            raise e
    
