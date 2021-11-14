import csv
import datetime
import gcsfs
import os
from google.cloud import bigquery


def create_csv_with_ingest_time(project, inputBucket, inputFileName, outputBucket):
    # We would use GCSFS python package to communicate with our GCS bucket (Both read and write)
    fs = gcsfs.GCSFileSystem(project=project)
    # Assigning a consistant ingest_time
    ingest_time = str(datetime.datetime.now())
    inputFilePath = '{}/{}'.format(inputBucket, inputFileName)
    # The output csv file, i.e. the file with the extra "ingestion_date" column
    # will have the same name as the uploaded file and appended with the ingestion_date
    # e.g. my-project-1548066830003-temp/sample_2021-09-06 22:53:06.462719.csv
    outputFilePath = '{}/{}_{}.csv'.format(outputBucket, inputFileName.replace('.csv', ''), ingest_time)
    try:
        with fs.open(inputFilePath, 'r') as csvinput:
            with fs.open(outputFilePath, 'w') as csvoutput:
                writer = csv.writer(csvoutput, delimiter=';', lineterminator='\n')
                reader = csv.reader(csvinput, delimiter=';')
                all = []
                # Skip the 1st row for header
                row = next(reader)
                # Add ingestion_date in the 1st row as column name
                row.append('ingestion_date')
                all.append(row)
                for row in reader:
                    row.append(ingest_time)
                    all.append(row)
                writer.writerows(all)
                outputFilePath = 'gs://' + outputFilePath
                print("Creation of csv file success - ", outputFilePath)
                # Number of rows would be length - 1 as 1st row is header
                print("Number of rows - ", len(all) - 1)
    except:
        # If there are any errors, we would return None
        print("Error occured during csv read/write")
        return None
    return outputFilePath


def load_csv_to_bigQuery(inputCsvFile, bqTableId):
    client = bigquery.Client()
    # BQ load job configs - We would automatically detect the schema of the file, 1st row would be header, del will be ';'
    jobConfig = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        field_delimiter=';'
    )
    try:
        loadJob = client.load_table_from_uri(
            inputCsvFile, bqTableId, job_config=jobConfig
        )
        loadJob.result()
        if loadJob.errors is None:
            print("Big Query Load Job Successful")
        else:
            print("Big Query Load Job Failed")
            print(loadJob.errors)
            return 0
        return loadJob.output_rows
    except:
        # In case of any error/exceptions in the BQ load job we would return 0 as the row count
        print("Error occured while loading data to bigquery")
        return 0


def file_finalized(event, context):
    project = os.environ['PROJECT']
    inputBucket = event['bucket']
    fileName = event['name']
    outputBucket = os.environ['OUTPUT_BUCKET']

    outputFilePath = create_csv_with_ingest_time(project, inputBucket, fileName, outputBucket)
    if outputFilePath is not None:
        bqTableId = '{}.{}.{}'.format(project, os.environ['BQ_DATASET_NAME'], os.environ['BQ_TABLE_NAME'])
        numberOfRows = load_csv_to_bigQuery(outputFilePath, bqTableId)
        print("Wrote {} rows to destination".format(numberOfRows))
