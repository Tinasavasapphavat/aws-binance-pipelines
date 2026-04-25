from src.destination.clickhouse import ClickHouse
import yaml 
from src.utils.utils import get_base_dir

BASE_DIR = get_base_dir()

config_path = BASE_DIR / "config" / "config.yaml"

symbol = "ETHUSDT"
with open(config_path,'r') as f:
    config = yaml.safe_load(f)

transform_path = config['pipelines'][symbol]['transform_path']
table_name = config['pipelines'][symbol]['clickhouse_table']

def main():
    process = ClickHouse(BASE_DIR,transform_path,table_name)
    process.get_data()
    process.casting_dtypes()
    process.load_to_clickhouse() 


if __name__ == "__main__":
    print(f"START LOADING: {symbol}")
    main()
    print("LOAD SUCCEEDED")