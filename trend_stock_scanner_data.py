import pickle
from datetime import timedelta
from pathlib import Path
from pandas import DataFrame

import boto3
import streamlit as st

from logger import app_log as log

# for local development create .streamlit/secrets.toml
AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
AWS_REGION = "eu-central-1"
AWS_BUCKET_NAME = "trend-stock-scanner-data"

HTTP_REQUEST_RETRIES = 3
HTTP_REQUEST_TIMEOUT_SECONDS = 15
HTTP_RESPONSE_BUFFER_SIZE = 1048576

OUT_DIR = Path(__file__).parent
TABLES_PICKLE = "tables.pickle"
SCANNED_PICKLE = "scanned.pickle"
EXTRAS_PICKLE = "extras.pickle"
DEFAULT_FILE_LIST = [TABLES_PICKLE, SCANNED_PICKLE, EXTRAS_PICKLE]

session = boto3.Session(
    aws_access_key_id=st.secrets[AWS_ACCESS_KEY_ID],
    aws_secret_access_key=st.secrets[AWS_SECRET_ACCESS_KEY],
    region_name=AWS_REGION,
)


def _download_file(file_name, transfer_callback=None):
    downloaded_file_path = OUT_DIR / file_name
    s3 = session.resource("s3")
    s3.Bucket(AWS_BUCKET_NAME).Object(file_name).download_file(
        downloaded_file_path, Callback=transfer_callback
    )
    return downloaded_file_path


def _get_file_size(file_name):
    s3 = session.client("s3")
    resp = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=file_name)
    return resp["ContentLength"]


def _progress_tracker(file_name, total_size_bytes):
    def track_progress(bytes_downloaded):
        log.info(
            "Downloaded [%d/%d] bytes of file %s",
            bytes_downloaded,
            total_size_bytes,
            file_name
        )
    return track_progress


@st.cache_resource(
    ttl=timedelta(minutes=10), show_spinner="Downloading market data ..."
)
def download_pickle_files() -> dict:
    for file_name in DEFAULT_FILE_LIST:
        log.info("Downloading file %s", file_name)
        total_file_size = _get_file_size(file_name)
        _download_file(file_name, _progress_tracker(file_name, total_file_size))
        log.info("Downloaded file %s", file_name)

    result = {}
    for key in DEFAULT_FILE_LIST:
        with open(OUT_DIR / key, "rb") as handle:
            result[key] = pickle.load(handle)
    return result


@st.cache_data(ttl=timedelta(minutes=10), show_spinner="Loading prices ...")
def load_prices() -> DataFrame:
    pickle_files = download_pickle_files()
    return pickle_files[SCANNED_PICKLE]


@st.cache_data(ttl=timedelta(minutes=10), show_spinner="Loading tables ...")
def load_tables() -> DataFrame:
    pickle_files = download_pickle_files()
    return pickle_files[TABLES_PICKLE]


@st.cache_data(ttl=timedelta(minutes=10), show_spinner="Loading tables ...")
def load_market_internals() -> DataFrame:
    pickle_files = download_pickle_files()
    return pickle_files[EXTRAS_PICKLE]
