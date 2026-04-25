from src.destination.s3 import LOAD
import yaml 
from src.utils.utils import get_base_dir

BASE_DIR = get_base_dir()

config_path = BASE_DIR / "config" / "config.yaml"

symbol = "ETHUSDT"
with open(config_path,'r') as f:
    config = yaml.safe_load(f)

transform_path = config['pipelines'][symbol]['transform_path']
s3_path = config['pipelines'][symbol]['s3_path']

def main():
    process = LOAD(BASE_DIR,transform_path,s3_path,symbol)
    process.get_data()
    process.load_to_S3() 


if __name__ == "__main__":
    print(f"START LOADING: {symbol}")
    main()
    print("LOAD SUCCEEDED")