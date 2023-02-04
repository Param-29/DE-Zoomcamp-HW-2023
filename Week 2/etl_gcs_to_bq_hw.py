from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-block-name")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(retries=3)
def extract_from_gcs_modified(params: list) -> list:
    """Download trip data from GCS"""
    paths = list()
    for lst in params:
        color = lst[0]
        year = lst[1]
        month = lst[2]

        gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
        gcs_block = GcsBucket.load("gcs-block-name")
        gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
        path = Path(f"../data/{gcs_path}")
        paths.append(path)
    return paths


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True)
def transform_all(paths: list) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.DataFrame()
    df_lst = list()
    for i in range(len(paths)):
        path = paths[i]
        df_tmp = pd.read_parquet(path)
        print(f"pre: missing passenger count: {df_tmp['passenger_count'].sum()}")
        # df["passenger_count"].fillna(0, inplace=True)
        print(f"post: missing passenger count: {df_tmp['passenger_count'].sum()}")
        df_lst.append(df_tmp)
    df = pd.concat(df_lst)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-pgm-creds")

    df.to_gbq(
        destination_table="ny_rides.rides",
        project_id="quick-ray-375906",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""

    params =[
        ["yellow", 2019, 2],
        ["yellow", 2019, 3],
    ]
    

    paths = extract_from_gcs_modified(params)
    df = transform_all(paths)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
