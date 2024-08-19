import pandas as pd
import gzip
import shutil
import os
from google.cloud import storage, bigquery
from concurrent.futures import ThreadPoolExecutor

def ndjson_to_csv_chunks(ndjson_file, chunk_size=10000):
    for chunk in pd.read_json(ndjson_file, lines=True, chunksize=chunk_size):
        csv_chunk_file = f"data_chunk_{chunk_size}.csv"
        chunk.to_csv(csv_chunk_file, index=False)
        yield csv_chunk_file

def compress_and_upload_chunk(bucket_name, chunk_file):
    gzip_file = chunk_file + '.gz'
    
    # Compress the CSV file with gzip
    with open(chunk_file, 'rb') as f_in:
        with gzip.open(gzip_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gzip_file)
    blob.upload_from_filename(gzip_file)
    
    print(f"Uploaded {gzip_file} to {bucket_name}")

    os.remove(chunk_file)
    os.remove(gzip_file)

def create_bigquery_table(dataset_id, table_id, schema):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception as e:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {table_id}.")

def import_to_bigquery(dataset_id, table_id, uri):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Data imported to BigQuery table {table_id}.")

ndjson_file = 'downloaded_file.ndjson'
bucket_name = 'bucket-name'
dataset_id = 'dataset-id'
table_id = 'table-id'
schema = [
    bigquery.SchemaField("Product_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("SKU", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Product_URL", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Retail_Price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Thumbnail_URL", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Search_Keywords", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Category", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Category_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Brand", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Child_SKU", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Child_Price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Color", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Color_Family", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Color_Swatches", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Size", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Shoe_Size", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Pants_Size", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Occasion", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Season", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Badges", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Rating_Avg", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Rating_Count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Inventory_Count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Date_Created", "TIMESTAMP", mode="NULLABLE")
]

def transform_and_load():
    with ThreadPoolExecutor() as executor:
        futures = []
        for csv_chunk_file in ndjson_to_csv_chunks(ndjson_file):
            future = executor.submit(compress_and_upload_chunk, bucket_name, csv_chunk_file)
            futures.append(future)
        
        for future in futures:
            future.result()

    create_bigquery_table(dataset_id, table_id, schema)

    # Import data from GCS to BigQuery
    uri = f"gs://{bucket_name}/data_chunk_*.gz"
    import_to_bigquery(dataset_id, table_id, uri)
