
import google.cloud.storage as storage
import google.cloud.bigquery as bigquery
import os

def csv_in_gcs_to_table(event, context):

    client = bigquery.Client()
    bucket = os.environ["BUCKETNAME"]
    object_name = event['FILENAME']
    file_to_table_dict = os.environ(["FILE_TO_TABLE_MAPPING"])
    table_id = ""
    for file_name in file_to_table_dict:
        if object_name.startswith(file_name):
            table_id = file_to_table_dict[file_name]
            break
    bq_table  = bigquery.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="|",
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
    )

    uri = "gs://{}/{}".format(bucket, object_name)

    load_job = client.load_table_from_uri(uri,
                                          table_id,
                                          job_config=job_config)
    load_job.result()
