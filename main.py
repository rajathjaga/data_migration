import os
import json
import pandas as pd
import boto3
from sqlalchemy import create_engine
from dotenv import load_dotenv
from scripts.s3_operations import upload_to_s3
from scripts.load_to_rds import load_to_rds
import zipfile
import requests
from io import BytesIO

load_dotenv()

if __name__ == "__main__":
    # Constants
    DATA_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    DOWNLOAD_PATH = "./data/source.zip"
    EXTRACT_TO = "./data/unzipped_folder"
    S3_BUCKET = "my-etl-project-bucket"
    RDS_DB = os.getenv("RDS_DB")

    # Ensure directories exist
    os.makedirs(os.path.dirname(DOWNLOAD_PATH), exist_ok=True)
    os.makedirs(EXTRACT_TO, exist_ok=True)

    # Step 1: Download and Unzip Data
    HEADERS = {
        "User-Agent": "Your Name user@email.com",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov"
    }

    # print("Downloading data...")
    # session = requests.Session()
    # response = session.get(DATA_URL, headers=HEADERS, stream=True)

    # if response.status_code == 200:
    #     with open(DOWNLOAD_PATH, "wb") as f:
    #         for chunk in response.iter_content(chunk_size=1024):
    #             f.write(chunk)
    #     print("Download successful.")
    # else:
    #     print(f"Failed to download. Status Code: {response.status_code}")
    #     exit(1)
    
    # # Step 2: Extract the zip file
    # print("Extracting zip file...")
    # with zipfile.ZipFile(DOWNLOAD_PATH, 'r') as zip_ref:
    #     zip_ref.extractall(EXTRACT_TO)
    # print("Extraction complete.")

    # Step 3: AWS Setup - Upload raw JSON files to S3
    print("Uploading raw files to S3...")
    file_count = 0
    for file_name in os.listdir(EXTRACT_TO):
        if file_count >= 5:
            break
            
        file_path = os.path.join(EXTRACT_TO, file_name)
        upload_to_s3(S3_BUCKET, file_path, f"raw/{file_name}")
        file_count += 1

    # Step 2: Process JSON files (only the 5 uploaded files)
    print("Processing JSON files...")
    all_data = []
    file_count = 0
    
    for file_name in os.listdir(EXTRACT_TO):
        if file_count >= 5:
            break
            
        file_path = os.path.join(EXTRACT_TO, file_name)
        
        if file_name.endswith('.json'):
            with open(file_path, 'r') as json_file:
                try:
                    data = json.load(json_file)
                    if isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)
                    file_count += 1
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from {file_name}, skipping...")

    # Convert extracted data to JSON format
    os.makedirs(os.path.dirname('./data/concatenated_data.json'), exist_ok=True)
    json_output_path = './data/concatenated_data.json'
    with open(json_output_path, 'w') as json_file:
        json.dump(all_data, json_file)

    # Upload transformed data to S3
    print("Uploading transformed data to S3...")
    upload_to_s3(S3_BUCKET, json_output_path, "transformed/transformed_data.json")

    # Load data into Amazon RDS
    print("Loading data into RDS...")
    engine = create_engine(RDS_DB)
    table_name = 'company_financial_data'
    load_to_rds(engine, table_name, json_output_path)

    print("ETL pipeline executed successfully.")