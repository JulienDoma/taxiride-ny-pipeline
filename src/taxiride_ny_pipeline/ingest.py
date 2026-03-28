"""
NYC Taxi dataset ingestion module.

This module implements a simple ingestion pipeline that:
1. Scrapes the NYC Taxi dataset webpage to discover available parquet files.
2. Filters links for yellow taxi trip datasets for a given year.
3. Checks the Google Cloud Storage bucket to avoid reprocessing files that
   already exist in the raw data layer.
4. Downloads missing datasets locally.
5. Uploads them to GCS using a partitioned raw data layout.

The module is designed as a lightweight data ingestion step typically used in
a data engineering pipeline before transformation and processing stages.

Data Flow
---------
Source (NYC Taxi website)
    -> Link discovery via HTML parsing
    -> Filtering by dataset type and year
    -> Local raw storage (`data/raw/<year>/`)
    -> Cloud raw layer in GCS
       (`raw/yellow/year=<year>/filename.parquet`)

Command-Line Usage
------------------
The module can be executed from the command line with an optional year
parameter:

    python <script_name>.py --year 2026

If no year is provided, the default value is used, which is N.

Dependencies
------------
- requests
- beautifulsoup4
- google-cloud-storage
- argparse
- standard Python libraries (os, time, typing)

Notes
-----
- Authentication to Google Cloud Storage requires a valid service account
  credentials JSON file.
- The ingestion logic avoids duplicate processing by comparing discovered
  datasets with files already present in the bucket.
- A delay is applied between downloads to limit request rate to the source
  dataset host.
"""

import os
import time
import argparse
from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup
from google.cloud import storage

from taxiride_ny_pipeline.params import BUCKET_NAME

URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0"


def get_dataset_links(year: str):
    """
    Retrieve dataset links for a given year that are not already present in the
    configured Google Cloud Storage bucket.

    This function scrapes a webpage for available dataset links, filters them to
    keep only parquet files related to yellow taxi trips for the specified year,
    and compares them with the files already stored in the bucket. Only links
    corresponding to files not yet uploaded are returned.

    Parameters
    ----------
    year : str
        Year used to filter the dataset links (e.g., "2026").

    Returns
    -------
    list[str]
        List of dataset URLs that are not yet stored in the GCS bucket.

    Notes
    -----
    - The function expects global variables `URL` and `AGENT` to be defined.
    - Authentication to GCS relies on a service account JSON file.
    - The bucket structure is assumed to follow:
      `raw/yellow/year=<year>/filename.parquet`
    """
    # Get links from the webpage
    response = requests.get(
        URL,
        headers={"User-Agent": AGENT},
    )
    soup = BeautifulSoup(response.content, "html.parser")
    links = [a.get("href") for a in soup.find_all("a") if a.get("href")]

    # Filter links containing only what i need
    dataset_links = [
        link.strip()
        for link in links
        if all(sub in link for sub in ("parquet", "yellow", year))
    ]

    # Get list of existing files in GCS
    client = storage.Client().from_service_account_json(
        json_credentials_path="gcp/bucket-access.json"
    )
    prefix = f"raw/yellow/year={year}/"
    bucket = client.bucket(BUCKET_NAME)

    existing_files = {
        blob.name.split("/")[-1] for blob in bucket.list_blobs(prefix=prefix)
    }

    # Only return links that are not yet uploaded
    return [link for link in dataset_links if link.split("/")[-1] not in existing_files]


def download_file(links: List[str], year: str):
    """
    Download dataset files from provided links and upload them to a Google Cloud
    Storage bucket using a structured raw data layout.

    The function ensures the local raw directory for the specified year exists,
    downloads each dataset if it is not already present locally, and uploads the
    file to the configured GCS bucket under a partitioned path. A delay is applied
    between iterations to avoid overwhelming the remote data source.

    Parameters
    ----------
    links : list[str]
        List of dataset URLs to download.
    year : str
        Year used to organize the raw data directory locally and in the GCS
        destination path.

    Notes
    -----
    - The function expects a global `AGENT` variable to define the HTTP user agent.
    - Authentication to Google Cloud Storage uses a service account JSON file.
    - Local storage structure: `data/raw/<year>/filename.parquet`
    - GCS destination structure: `raw/yellow/year=<year>/filename.parquet`
    - Existing local files are not re-downloaded but are still uploaded to GCS.
    - A fixed delay is applied between downloads to reduce load on the source
    website.
    """
    # Define the local folder to store datas
    raw_path = f"data/raw/{year}/"
    os.makedirs(raw_path, exist_ok=True)

    # Initialize GCS client from gcp folder file (linked to the terraform service account - respecting PLP)
    client = storage.Client().from_service_account_json(
        json_credentials_path="gcp/bucket-access.json"
    )

    # Define which bucket to use
    bucket = client.bucket(BUCKET_NAME)

    # For loop applied to all files not already downloaded
    for link in links:

        # Get file name and path from data folder
        filename = link.split("/")[-1]
        full_path = os.path.join(raw_path, filename)

        # Skip download if file already exists locally, otherwise download the file
        if os.path.exists(full_path):
            print(f"Skipped download, file already exists: {full_path}")
        else:
            r = requests.get(link, headers={"User-Agent": AGENT})
            with open(full_path, "wb") as f:
                f.write(r.content)
            print(f"Downloaded {full_path}")

        # Upload to GCS
        destination_path = f"raw/yellow/year={year}/{filename}"
        blob = bucket.blob(destination_path)
        blob.upload_from_filename(full_path)
        print(f"Uploaded to gs://{BUCKET_NAME}/{destination_path}")

        time.sleep(5)  # don't overflow NYC website


def main():
    """
    Entry point for the dataset ingestion workflow.

    This function parses command-line arguments to determine the target year,
    retrieves the corresponding dataset links from the source website, and
    triggers the download and upload process to local storage and Google
    Cloud Storage.

    Parameters
    ----------
    None

    Notes
    -----
    - The year can be provided through the CLI using the `--year` argument.
    - If no year is specified, the default value is used (N).
    - The function orchestrates two main steps:
    1. Retrieve dataset links filtered by year.
    2. Download datasets and upload them to the configured GCS bucket.
    - The expected valid range for datasets is between 2009 and 2026.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year",
        dest="year",
        default=str(datetime.today().year),
        help="Choose a year between 2009 and 2026",
    )
    args, _ = parser.parse_known_args()

    links = get_dataset_links(year=args.year)
    download_file(links, year=args.year)


if __name__ == "__main__":
    main()
