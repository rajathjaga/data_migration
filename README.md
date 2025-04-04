# Data Migration and Transformation Tool for Amazon RDS Data Warehouses

## Problem Statement

You have a URL that points to a zip file. The zip file contains multiple JSON files. The JSON files contain multiple documents with various data structures. Your goal is to download the zip file from the URL, extract the data from the JSON files, store it in Amazon S3, and load it into Amazon RDS. You want to use Python or PySpark to perform these tasks. You may use any libraries or tools that are necessary to complete the task.

## Overview

This data migration tool automates the process of downloading, extracting, transforming, and loading data from JSON files. The transformed data is uploaded to an AWS S3 bucket, and the final processed data is stored in an RDS database.

## Features

- **Extract:** Downloads and unzips raw JSON data files, then uploads the raw files to an S3 bucket.
- **Load:** Uploads transformed JSON data to S3 and RDS.
- **Combine:** Merges transformed JSON data from all sources into a single JSON file and uploads it to S3.

## Installation & Setup

1. **Clone the repository**

   ```sh
   https://github.com/rajathjaga/data_migration.git
   cd data_migration
   ```

2. **Install dependencies**

   ```sh
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   Create a `.env` file and add the following:

   ```sh
   RDS_DB=postgresql://username:password@hostname:port/database
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   ```

## Running the  Pipeline

Execute the pipeline with:

```sh
python main.py
```

## AWS S3 & RDS Integration

- **Uploads raw and transformed JSON data** to an S3 bucket.
- **Loads processed JSON data into RDS** using SQLAlchemy.
- **Final merged dataset** is stored as `combined_data.json` in S3.

