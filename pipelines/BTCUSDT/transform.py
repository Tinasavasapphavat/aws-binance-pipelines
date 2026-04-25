from src.processing.process import PROCESS
import yaml 
from src.utils.utils import get_base_dir

BASE_DIR = get_base_dir()

config_path = BASE_DIR / "config" / "config.yaml"
schema_path = BASE_DIR / "config" / "schema.yaml"

symbol = "BTCUSDT"
with open(config_path,'r') as f:
    config = yaml.safe_load(f)

with open(schema_path,'r') as f:
    schema = yaml.safe_load(f)

raw_path = config['pipelines'][symbol]['raw_path']
transform_path = config['pipelines'][symbol]['transform_path']


def main():
    process = PROCESS(symbol=symbol,config = schema,path=BASE_DIR,raw_path=raw_path,transform_path=transform_path)
    process.cast_dtypes()
    process.rename_columns()
    process.transform()
    process.save_tmp()


if __name__ == "__main__":
    print(f"START TRANSFORM: {symbol}")
    main()
    print("TRANSFORM SUCCEEDED")
