## License
This project is not licensed. If you want to use, please contact me. 

# Odoo API Data Loader to Google BigQuery

This project extracts data from Odoo REST API endpoints, loads it into Google Cloud Storage, and then into Google BigQuery. The code is designed to run as a Google Cloud Function.

## Overview

This solution is developed to automate data extraction from an Odoo ERP system using its REST API. The extracted data is temporarily stored in Google Cloud Storage, and then loaded into Google BigQuery for further analysis and reporting. The entire workflow is managed as a Google Cloud Function to enable serverless execution and scalability.

### Features
- Extracts data from various Odoo API endpoints.
- Loads extracted data into Google Cloud Storage as an intermediate step.
- Imports data into Google BigQuery for easy querying and analysis.
- Designed to run as a serverless Google Cloud Function.

## Project Structure

- **main.py**: Entry point for the Google Cloud Function. Manages the extraction and loading process.
- **odoo_api.py**: Handles interactions with the Odoo REST API endpoints to extract data.
- **bigquery_handler.py**: Manages loading data from Google Cloud Storage into BigQuery.
- **utils.py**: Contains helper functions used throughout the project.
- **requirements.txt**: Lists the required dependencies for the project.

## Setup Instructions

1. **Install Dependencies**
   Make sure to install all the required Python libraries. These can be found in the `requirements.txt` file:
   ```sh
   pip install -r requirements.txt
   ```
   Key dependencies include:
   - `google-cloud-bigquery`: To interact with BigQuery.
   - `google-cloud-storage`: To work with Google Cloud Storage.
   - `requests`: To interact with the Odoo API.

2. **Configure Google Cloud Function**
   - Deploy the Google Cloud Function using the Google Cloud Console or CLI.
   - Set the entry point to `main.handler`.
   - Ensure that the necessary IAM permissions are granted for accessing Cloud Storage and BigQuery.

3. **Environment Variables**
   - Set environment variables for Odoo credentials, Google Cloud project details, and other required settings in the Google Cloud Console.

## Usage

- Deploy the function to Google Cloud.
- The function can be triggered manually or on a schedule (e.g., using Google Cloud Scheduler).
- Upon triggering, the function will:
  1. Fetch data from the specified Odoo API endpoints.
  2. Store the data in Google Cloud Storage.
  3. Load the data into BigQuery for analysis.

## How the Code Works

1. **Data Extraction**: The `odoo_api.py` script is responsible for interacting with the Odoo REST API. It sends HTTP requests to specified endpoints to extract data, handles pagination if necessary, and ensures the data is retrieved in a format suitable for further processing.

2. **Intermediate Storage**: Once the data is extracted, it is saved as a CSV or JSON file in Google Cloud Storage. This step provides a backup of the data and serves as an intermediate staging area before loading into BigQuery.

3. **Data Loading**: The `bigquery_handler.py` script takes care of loading the data from Google Cloud Storage into BigQuery. It creates or updates the relevant BigQuery tables, using schemas defined within the code to ensure the data is properly structured.

4. **Main Function Flow**: The `main.py` file is the entry point that orchestrates the entire process. It uses helper functions from `utils.py` to handle tasks such as logging, error handling, and formatting data before storage or loading.

5. **Google Cloud Function**: The entire solution is designed to run in a serverless environment using Google Cloud Functions. This makes it scalable, easy to deploy, and cost-effective as it runs only when triggered.

## Limitations

1. **Rate Limits**: The Odoo REST API may have rate limits that can affect data extraction, especially when dealing with large datasets. To handle this, the code includes basic retry mechanisms, but repeated failures may still occur if rate limits are exceeded.

2. **Data Volume**: For very large datasets, storing data in Google Cloud Storage and then loading it into BigQuery can become slow and potentially costly. The current implementation is optimized for moderate data sizes, and performance might degrade with very high data volumes.

3. **Error Handling**: While the code includes error handling for common issues (e.g., network errors, missing data), it may not cover all edge cases, particularly those involving unexpected API responses or data inconsistencies.

4. **Schema Changes**: If the structure of the data in Odoo changes, the BigQuery schema may need manual updates. The current implementation assumes a stable schema for both extraction and loading.

5. **Limited Customization**: The current implementation is designed to extract specific datasets from Odoo. Adding new endpoints or modifying the data transformation logic requires changes to the codebase.

6. **Dependency on Internet Connectivity**: The solution requires stable internet connectivity for accessing the Odoo API and Google Cloud services. Any interruptions in connectivity could lead to partial or failed data loads.

7. **Latency**: Since the solution relies on multiple stages (extraction, storage, and loading), there can be latency issues, especially when dealing with large datasets or network bottlenecks. Optimizing the data flow might be necessary for time-sensitive applications.

## Dependencies

Here are the key dependencies used in the project:

- **google-cloud-bigquery==3.4.0**: For interacting with Google BigQuery.
- **google-cloud-storage**: For handling operations with Google Cloud Storage.
- **requests==2.26.0**: For making HTTP requests to the Odoo API.
- **functions-framework==3.0.0**: To run Google Cloud Functions locally.
- **numpy==1.23.5** and **pyarrow==10.0.1**: Used for data processing.

For the complete list of dependencies, see the `requirements.txt` file【9†source】.

## Contributing
Feel free to open issues or contribute to improve the functionality or efficiency of the project. Any contribution is welcome!

