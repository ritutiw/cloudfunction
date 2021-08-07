def gcs_to_bq(event, context):
    import re
    from google.cloud import storage
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    bq_client = bigquery.Client()
    bucket = storage.Client().bucket("BUCKET")
    for blob in bucket.list_blobs(prefix="FOLDER/"):
        if ".csv" in blob.name: #Checking for csv blobs as list_blobs also returns folder_name
           job_config = bigquery.LoadJobConfig(
               autodetect=True,
               skip_leading_rows=1,
               write_disposition="WRITE_TRUNCATE",
               source_format=bigquery.SourceFormat.CSV,
    
           )
           csv_filename = re.findall(r".*/(.*).csv",blob.name) #Extracting file name for BQ's table id
           bq_table_id = "project-name.dataset-name."+csv_filename[0] # Determining table name
       
        uri = "gs://bucket-name/"+blob.name
        print(uri)
        load_job = bq_client.load_table_from_uri(
                   uri, bq_table_id, job_config=job_config
               )  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = bq_client.get_table(bq_table_id)  # Make an API request.
        print("Table {} uploaded.".format(bq_table_id))
