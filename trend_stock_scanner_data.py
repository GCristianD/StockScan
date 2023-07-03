import pickle
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from zipfile import ZipFile
from pandas import DataFrame

import urllib3
import streamlit as st

from logger import app_log as log

GITLAB_USER_SECRET = "GITLAB_USER_ID"
GITLAB_TOKEN_SECRET = "GITLAB_ACCESS_TOKEN"
GITLAB_BASE_URL = "https://gitlab.com/api/v4"
GITLAB_PROJECT_ID = "47320184"

HTTP_REQUEST_RETRIES = 3
HTTP_REQUEST_TIMEOUT_SECONDS = 15
HTTP_RESPONSE_BUFFER_SIZE = 1048576

OUT_DIR = Path(__file__).parent
DOWNLOADED_FILE_NAME = "artifacts.zip"
TABLES_PICKLE = "tables.pickle"
SCANNED_PICKLE = "scanned.pickle"
EXTRAS_PICKLE = "extras.pickle"

DEFAULT_FILE_LIST = [TABLES_PICKLE, SCANNED_PICKLE, EXTRAS_PICKLE]


def _create_request_pool() -> urllib3.PoolManager:
    password = st.secrets[GITLAB_TOKEN_SECRET]
    request_headers = {
        "Authorization": f"Bearer {password}"
    }
    return urllib3.PoolManager(headers=request_headers)


def _get(http: urllib3.PoolManager, url: str) -> urllib3.BaseHTTPResponse:
    return http.request(
        "GET", url, retries=HTTP_REQUEST_RETRIES, timeout=HTTP_REQUEST_TIMEOUT_SECONDS
    )


def _download_file(http: urllib3.PoolManager, url: str, out_dir: Path) -> Path:
    response = http.request(
        "GET", url, retries=HTTP_REQUEST_RETRIES, timeout=HTTP_REQUEST_TIMEOUT_SECONDS, preload_content=False
    )
    assert response.status == 200
    file_path = out_dir / DOWNLOADED_FILE_NAME
    try:
        log.info("download start (%s)", file_path)
        with open(file_path, "wb") as file:
            while True:
                data = response.read(HTTP_RESPONSE_BUFFER_SIZE)
                if not data:
                    break
                file.write(data)
        log.info("downloaded successful (%s)", file_path)
        return file_path
    except Exception as exc:
        log.error("download error (%s)", file_path, exc_info=exc)
    finally:
        response.release_conn()


def _is_market_data_schedule(obj: dict) -> bool:
    return obj["ref"] == "main" and obj["description"] == "Update Market Data"


def _get_update_market_data_schedule(http: urllib3.PoolManager) -> dict:
    resp = _get(http, f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/pipeline_schedules")
    assert resp.status == 200
    schedules = list(filter(_is_market_data_schedule, resp.json()))
    assert len(schedules) == 1
    resp = _get(http, f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/pipeline_schedules/{schedules[0]['id']}")
    assert resp.status == 200
    return resp.json()


def _get_pipeline_details(http: urllib3.PoolManager, pipeline_id: int) -> dict:
    resp = _get(http, f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/pipelines/{pipeline_id}")
    assert resp.status == 200
    return resp.json()


def _is_get_data_job(obj: dict) -> bool:
    return obj["name"] == "get-data" and len(obj["artifacts"]) > 0


def _get_pipeline_get_data_job(http: urllib3.PoolManager, pipeline_id: int) -> dict:
    log.info("fetch info about get-data job in pipeline %d", pipeline_id)
    resp = _get(http, f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/pipelines/{pipeline_id}/jobs")
    assert resp.status == 200
    jobs = list(filter(_is_get_data_job, resp.json()))
    assert len(jobs) > 0
    log.info("got job id: %(id)d, url: %(web_url)s", jobs[0])
    return jobs[0]


def _wait_last_pipeline_success(http: urllib3.PoolManager, pipeline_schedule: dict) -> int:
    log.info("waiting for '%(description)s' latest pipeline to finish successfully", pipeline_schedule)
    now = datetime.utcnow()
    timeout = timedelta(seconds=600)
    last_pipeline = pipeline_schedule["last_pipeline"]
    piepline_details = _get_pipeline_details(http, last_pipeline["id"])
    while datetime.utcnow() - now < timeout and piepline_details["status"] != "success":
        sleep(1.0)
        piepline_details = _get_pipeline_details(http, last_pipeline["id"])
    if piepline_details["status"] != "success":
        raise ValueError(f"pipeline {piepline_details['id']} is in status {piepline_details['status']} after {timeout}")
    log.info("'%(description)s' latest pipeline completed successfully", pipeline_schedule)
    return piepline_details["id"]


def _fetch_gitlab_artifacts(out_dir: Path) -> Path:
    http = _create_request_pool()
    pipeline_schedule = _get_update_market_data_schedule(http)
    pipeline_id = _wait_last_pipeline_success(http, pipeline_schedule)
    get_data_job = _get_pipeline_get_data_job(http, pipeline_id)

    return _download_file(
        http, f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/jobs/{get_data_job['id']}/artifacts", out_dir
    )


@st.cache_resource(ttl=timedelta(seconds=10), show_spinner="Downloading market data ...")
def download_pickle_files(file_names: list[str]) -> dict:
    if len(file_names) < 1:
        return {}
    local_artifacts_file_path = _fetch_gitlab_artifacts(out_dir=OUT_DIR)
    assert local_artifacts_file_path.exists() and local_artifacts_file_path.is_file()
    with ZipFile(local_artifacts_file_path, "r") as artifacts_zip:
        artifacts_zip.extractall(OUT_DIR, file_names)
    local_artifacts_file_path.unlink()

    result = {}
    for key in file_names:
        with open(OUT_DIR/key, "rb") as handle:
            result[key] = pickle.load(handle)
    return result


@st.cache_data(ttl=timedelta(hours=24), show_spinner="Loading prices ...")
def load_prices() -> DataFrame:
    pickle_files = download_pickle_files(DEFAULT_FILE_LIST)
    return pickle_files[SCANNED_PICKLE]


@st.cache_data(ttl=timedelta(hours=24), show_spinner="Loading tables ...")
def load_tables() -> DataFrame:
    pickle_files = download_pickle_files(DEFAULT_FILE_LIST)
    return pickle_files[TABLES_PICKLE]
