import pickle
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZipFile
from pandas import DataFrame

import urllib3
import streamlit as st

GITLAB_USER_SECRET = "GITLAB_USER_ID"
GITLAB_TOKEN_SECRET = "GITLAB_ACCESS_TOKEN"
GITLAB_BASE_URL = "https://gitlab.com/api/v4/"
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
    try:
        file_path = out_dir / DOWNLOADED_FILE_NAME
        with open(file_path, "wb") as file:
            while True:
                data = response.read(HTTP_RESPONSE_BUFFER_SIZE)
                if not data:
                    break
                file.write(data)
        return file_path
    finally:
        response.release_conn()


def _filter_relevant_artifact(job: dict) -> bool:
    artifacts = job["artifacts"]
    if len(artifacts) < 1:
        return False
    for artifact in artifacts:
        if artifact["file_type"] == "archive" and artifact["filename"] == DOWNLOADED_FILE_NAME:
            return True
    return False


def _created_at_timestamp(obj: dict) -> datetime:
    created_at_str = obj.get("created_at")
    if created_at_str is not None and isinstance(created_at_str, str):
        return datetime.fromisoformat(created_at_str)
    return datetime.utcnow()


def _get_most_recent_successful_job(http: urllib3.PoolManager):
    resp = _get(http, f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/jobs?scope=success")
    assert resp.status == 200
    jobs = resp.json()
    assert len(jobs) > 0

    job_projections = [
        {
            key: job[key]
            for key in {"id", "name", "artifacts", "created_at"}
        }
        for job in jobs
        if _filter_relevant_artifact(job)
    ]
    return max(job_projections, key=_created_at_timestamp)


def _fetch_gitlab_artifacts(out_dir: Path) -> Path:
    http = _create_request_pool()
    job = _get_most_recent_successful_job(http)
    return _download_file(http, f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/jobs/{job['id']}/artifacts", out_dir)


@st.cache_resource(ttl=timedelta(hours=24), show_spinner="Downloading market data ...")
def download_pickle_files(file_names: list[str]) -> dict:
    if len(file_names) < 1:
        return {}
    should_download = any(not (OUT_DIR / file_name).exists() for file_name in file_names)
    if should_download:
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
