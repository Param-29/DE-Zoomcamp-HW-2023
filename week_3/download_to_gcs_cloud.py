# wek_to_gcs_fhv.py
import os
from google.cloud import storage


def upload_to_bucket(blob_name, bucket_name, csv_name):
    """ Upload data to a bucket"""
     
    # Explicitly use service account credentials by specifying the private key
    # file.
    path_to_file = f'data/{csv_name}'
    storage_client = storage.Client.from_service_account_json(
        '/home/param/Downloads/GCloud-json/quick-ray-375906-15748deb6a49.json')

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(f'{blob_name}/{csv_name}')
    blob.upload_from_filename(path_to_file)
    
    #returns a public url
    return blob.public_url


def download_data(month, year):
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02}.csv.gz'
    csv_name = f'yellow_tripdata_{year}-{month:02}.csv.gz'
    print(f"Working on getting data from {url}....")
    os.system(f"axel {url} -o data/{csv_name}")
    print(f"Data stored in data/{csv_name}....")

    return csv_name


if __name__ == '__main__':
    
    
    
    years = [2019]
    for year in years:
        for month in range(1,4):
            download_data(month, year)
        
            out = upload_to_bucket(
                blob_name='data/yellow', 
                bucket_name="prefect-dee",
                csv_name = f'yellow_tripdata_{year}-{month:02}.csv.gz'
                )
            print(out)