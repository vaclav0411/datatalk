from pathlib import Path
from prefect_sqlalchemy import DatabaseCredentials

color = 'green'
dataset_file = 'test'

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR.joinpath('templates')
base_path = BASE_DIR.joinpath(f"data/{color}")
path = base_path.joinpath(f"{dataset_file}.parquet")
print(base_path, path)

database_block = DatabaseCredentials.load("postgre")

print(database_block.get_engine())