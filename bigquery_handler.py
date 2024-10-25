import logging
from google.cloud import bigquery
from google.cloud import storage
import json

class BigQueryHandler:
    def __init__(self, config):
        self.project_id = config['project_id']
        self.dataset_id = config['dataset_id']
        self.bucket_name = config['bucket_name']  # GCS bucket for temporary file storage
        self.client = bigquery.Client(project=self.project_id)
        self.storage_client = storage.Client()

    def upload_to_gcs(self, data, gcs_path, chunk_size):
        """Upload newline-delimited JSON data to GCS with error handling and logging."""
        try:
            logging.info(f"Uploading data to GCS path: {gcs_path} in bucket {self.bucket_name}.")
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)
            
            total_records = len(data)
            logging.info(f"Uploading data to GCS file in chunks. It can take few minutes. Total Rows to insert are {total_records}.")
            # Open a writable stream to GCS
            with blob.open("w") as f:
                buffer = io.StringIO()  # In-memory string buffer for newline-delimited JSON
                for i in range(0, total_records, chunk_size):
                    chunk = data[i:i + chunk_size]
                    # Stream data chunk by chunk
                    for record in chunk:
                        buffer.write(json.dumps(record) + '\n')  # Write JSON records to the buffer

                    # Flush the buffer into the writable GCS stream
                    f.write(buffer.getvalue())
                    buffer.seek(0)  # Reset buffer position
                    buffer.truncate(0)  # Clear the buffer
                    
                    logging.info(f"Uploaded chunk {i // chunk_size + 1} of {len(data)} records.")

            logging.info(f"Successfully uploaded all data to {gcs_path} in GCS bucket {self.bucket_name}.")
        except Exception as e:
            logging.error(f"Failed to upload data to GCS: {e}")
            raise

    def load_from_gcs_to_bigquery(self, table_name, gcs_path, schema):
        """Load data from GCS into BigQuery, with logging and error handling."""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(table_name)

            # Set up load job configuration
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite table
            )

            # Load data from GCS to BigQuery
            uri = f"gs://{self.bucket_name}/{gcs_path}"
            logging.info(f"Starting BigQuery load job from {uri} to table {table_name}.")
            load_job = self.client.load_table_from_uri(uri, table_ref, job_config=job_config)

            # Wait for the job to complete
            load_job.result()
            logging.info(f"Successfully loaded rows {load_job.output_rows} from {uri} into BigQuery table {table_name}.")
            if load_job.errors:
                logging.error(f"Errors occurred during BigQuery load job: {load_job.errors}")
                raise RuntimeError(f"BigQuery load job encountered errors: {load_job.errors}")

        except Exception as e:
            logging.error(f"Failed to load data into BigQuery from GCS: {e}")
            raise

    def insert_into_bigquery_in_bathes(self, table_name, data, chunk_size):
        """Insert data into BigQuery using GCS as staging, with proper logging and error handling."""
        try:
            # Define schema based on the first record
            schema = [bigquery.SchemaField(field, "STRING") for field in data[0].keys()]

            # Generate a unique path for the temporary file in GCS
            gcs_path = f"temp/{table_name}_data.json"

            # Upload the data to GCS
            logging.info(f"Starting data upload to GCS for table {table_name}.")
            self.upload_to_gcs(data, gcs_path, chunk_size)

            # Load the data from GCS into BigQuery
            logging.info(f"Starting data load into BigQuery for table {table_name}.")
            self.load_from_gcs_to_bigquery(table_name, gcs_path, schema)

        except Exception as e:
            logging.error(f"Failed to insert data into BigQuery for table {table_name}: {e}")
            raise

