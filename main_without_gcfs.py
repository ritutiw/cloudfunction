from google.cloud import bigquery
from google.cloud import storage
import csv
import os
import tempfile


bucket=os.getenv('BUCKET')
client = bigquery.Client()
dataset_id = os.environ['DATASET']
dataset_ref = client.dataset(dataset_id)
storage_client = storage.Client()
bigquery_client = bigquery.Client()
destinationbucket = os.getenv('DESTINATION_BUCKET')
project = os.environ['PROJECT']


class Load():
# function to read blob line by line,replace and write into temp file 
    def load_gcs_bq(self, event,config):
        file_name= event['name']
        bucket = storage_client.get_bucket(event['bucket'])
        blob = bucket.get_blob(file_name)
        uri ="gs://destinationbucket/file_name.csv"
        new_blob = destinationbucket.blob(file_name)
        with tempfile.TemporaryFile("W+") as temp_file:
            with blob.open() as blob_file:
                blob_file.readline() 
                for line in blob_file:
                    new_lines=line.replace('\\"','')
                    temp_file.write(new_lines) 
                temp_file.seek(0)
                new_blob.upload_from_file(temp_file)
                upload_file_to_bq(uri)

#function to upload tempfile into bigquery             
    def upload_file_to_bq(self,uri):      
        table_name = self.file_name.split('/')[-1].replace('.csv', '')
        table_id = '{}.{}.{}'.format(project, os.environ['DATASET_NAME'], table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect= 'TRUE'
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.quote_character=''
        job_config.field_delimiter=";"
        job_config.encoding='UTF-8'
        job_config.write_disposition="WRITE_APPEND"
        job_config.skip_leading_rows=1
        table_id=dataset_ref.table(os.environ['TABLE'])

        load_job=bigquery_client.load_table_from_uri(self.uri, table_id, job_config=job_config)
        load_job.result()  # wait for table load to complete.
        print('Job finished.')
        print('Loaded {} rows.'.format(table_id.num_rows))
