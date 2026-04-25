from src.ingestion.binance_api import BINANCE
import yaml 
from src.utils.utils import get_base_dir
import sys

BASE_DIR = get_base_dir()
config_path = BASE_DIR / "config" / "config.yaml"
symbol = "BTCUSDT"
with open(config_path,'r') as f:
    config = yaml.safe_load(f)

url = config['main']['url']
limit = config['main']['limit']
raw_path = config['pipelines'][symbol]['raw_path']
param_path = config['pipelines'][symbol]['param_path']

def main():
    process = BINANCE(url,symbol,limit,BASE_DIR,param_path)
    process.get_latest_ts()
    process.get_data()
    process.save_tmp( raw_path= raw_path )
    process.update_latest_ts()

if __name__ == "__main__":
    print(f"START FETCHING: {symbol}")
    main()
    print("FETCHING SUCCEEDED")

