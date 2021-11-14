import os
import gcsfs

from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage


def read_in_chunks(file, chunk_size=1024):
    """
    Read a file in chuck of lines
        :param file: file object
        :param chunk_size: Default chunk size: 1k
        :return: chunk of lines from file
    """
    while True:
        data = file.readlines(chunk_size)
        if not data:
            break
        yield data


def clean_row(row, header=False):
    """
    :param row: raw input
    :return: cleaned date
    """
    row = row.encode("ascii", "replace").decode().replace("\n", "")
    columns = row.split(';')
    processed_row = ""
    for column in columns:
        column = column.strip('"')
        processed_row = f"{processed_row};{column}"

    if header:
        return f"ingestion_date;{processed_row}"

    ingest_time = str(datetime.now())
    return f"{ingest_time};{processed_row}\n"


def main(event, context):
    """
    trigger function for cloud function
    :param event: event payload
    :param context: metadata for the event
    """
    project = os.environ['PROJECT']
    input_bucket = event['bucket']
    file_name = event['name']
    output_bucket = os.environ['OUTPUT_BUCKET']
    fs = gcsfs.GCSFileSystem(project=project)

    # print(event, context)

    gcs_client = storage.Client()
    input_b = gcs_client.bucket(input_bucket)
    output_path = f"gs://{output_bucket}/{file_name}"

    input_blob = input_b.get_blob(f"{file_name}")

    with input_blob.open("rt") as r:
        with fs.open(output_path, 'w') as w:
            for piece in read_in_chunks(r):
                for row in piece:
                    row = clean_row(row)
                    w.write(row)
        w.close()
    r.close()

    bq_table_name = file_name.split('/')[-1].replace('.csv', '')
    bq_table_id = '{}.{}.{}'.format(project, os.environ['BQ_DATASET_NAME'], bq_table_name)
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",  # WRITE_APPEND / WRITE_TRUNCATE
        ignore_unknown_values=True,
        allow_jagged_rows=True,
        skip_leading_rows=1,
        field_delimiter=';'
    )
    try:
        load_job = bq_client.load_table_from_uri(
            output_path, bq_table_id, job_config=job_config
        )
        load_job.result()
        if load_job.errors:
            print(f"Big Query Load Job Failed :: {load_job.errors}")
            return 0
        print("Big Query Load Job Successful")
        return load_job.output_rows
    except Exception as e:
        print(f"Error occurred while loading data to bigquery :: {e}")
        return 0

