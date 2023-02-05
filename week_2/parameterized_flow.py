from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect.filesystems import LocalFileSystem
from prefect_sqlalchemy import DatabaseCredentials


BASE_DIR = Path(__file__).resolve().parent


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


# @task()
# def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
#     """Write DataFrame out locally as parquet file"""
#     path = Path(f"data/{color}/{dataset_file}.parquet")
#     df.to_parquet(path, compression="gzip")
#     return path

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    base_path = BASE_DIR.joinpath(f"data/{color}")
    path = base_path.joinpath(f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return base_path

# @task()
# def write_gcs(path: Path) -> None:
#     """Upload local parquet file to GCS"""
#     gcs_block = GcsBucket.load("zoom-gcs")
#     gcs_block.upload_from_path(from_path=path, to_path=path)
#     return

# @task()
# def write_to_local(path: Path) -> None:
#     """Upload local parquet file to GCS"""
#     local_file_system_block = LocalFileSystem.load("local")
#     local_file_system_block.put_directory(local_path=path)
#     return

@task()
def write_to_postgree(df: pd.DataFrame) -> None:
    database_block = DatabaseCredentials.load("postgre")
    df.to_sql(
        'yellow_taxi'
        , con=database_block.get_engine()(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    # path = write_local(df_clean, color, dataset_file)
    write_to_postgree(df_clean)


@flow()
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2019
    etl_parent_flow(months, year, color)
