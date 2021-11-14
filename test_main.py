from google.cloud import storage


class TestCloudFunction:
    project_name = "mylearning-329506"

    def upload_file_to_gcs(self, bucket_name, source, destination, project_name):
        storage_client = storage.Client(project=project_name)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination)
        blob.upload_from_filename(source)
        print('File {} uploaded to {}'.format(
            source,
            destination
        ))

    def test_case(self):
        self.upload_file_to_gcs('meril-learning', 'files/dict_lbutton_translation.csv', 'input/dict_lbutton_translation.csv', self.project_name)
        self.upload_file_to_gcs('meril-learning', 'files/dim_client_suggest.csv', 'input/dim_client_suggest.csv', self.project_name)
        self.upload_file_to_gcs('meril-learning', 'files/dim_transalation.csv', 'input/dim_transalation.csv', self.project_name)
        assert True
