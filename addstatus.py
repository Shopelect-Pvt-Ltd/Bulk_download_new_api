#!/usr/bin/env python3
import os
import boto3
import csv
from urllib.parse import urlparse
import logging
import sys

# Set up logging to both console and a file
logging.basicConfig(filename='.logss', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Specify your AWS credentials and region
aws_access_key_id = 'AKIAWVKQBF2LSY6UFLHK'
aws_secret_access_key = 'DODUkWVTyjhWd/gvOHaJdcWVE+6JAZOoefYcY/Ch'
AWS_REGION = "ap-south-1"

# Initialize the S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=AWS_REGION)

def download_files_from_s3(s3_url, file_name, airline_name):
    # Parse the S3 URL
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc.split('.')[0]  # Extracting only the bucket name
    prefix = parsed_url.path.lstrip('/')

    logger.info(f"Bucket Name: {bucket_name}")

    # List objects in the S3 bucket with the specified prefix
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    except Exception as e:
        logger.error(f"Error listing objects in bucket: {e}")
        return "Error"

    # Download each object to the destination folder
    for obj in response.get('Contents', []):
        key = obj['Key']
        folder = "/Users/finkraft/dev/bulkdownloadUI/pavitra"
        destination_path = f"{folder}/{airline_name}/{file_name}"

        # Create directories if they do not exist
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        # Download the object
        try:
            s3.download_file(bucket_name, key, destination_path)
            logger.info(f"Downloaded {key} to {destination_path}")
        except Exception as e:
            logger.error(f"Error downloading {key}: {e}")
            return "Error"

    return "Success"

def process_s3_links_csv(csv_path):
    new_csv_path = csv_path.replace(".csv", "_status.csv")

    with open(csv_path, 'r') as csvfile, open(new_csv_path, 'w', newline='') as new_csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)  # Get the header
        header.append("Status")  # Add "Status" column to header
        csv_writer = csv.writer(new_csvfile)
        csv_writer.writerow(header)  # Write the new header to the new CSV

        for row in csv_reader:
            s3_link_url = row[0].strip()
            file_name = row[1].strip()
            airline_name = row[2].strip()

            status = download_files_from_s3(s3_link_url, file_name, airline_name)

            # Append the status to the row
            row.append(status)

            # Write the updated row to the new CSV
            csv_writer.writerow(row)

# Example usage:
# csv_path = "/Users/finkraft/dev/bulkdownload/Invoice_Location_for_Manual_Bulk_Download.csv"

# process_s3_links_csv(csv_path)

# if __name__ == "__main__":
#     file_path = sys.argv[1]
#     print("filename: ", file_path)
#     process_s3_links_csv(file_path)



# def run_program_in_background(command):
#     try:
#         subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#         print(f"Program '{command}' started in the background.")
#     except Exception as e:
#         print(f"An error occurred: {str(e)}")