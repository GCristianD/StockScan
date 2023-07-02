import urllib3
import streamlit as st

GITLAB_USER_SECRET = "GITLAB_USER_ID"
GITLAB_TOKEN_SECRET = "GITLAB_ACCESS_TOKEN"
GITLAB_BASE_URL = "https://gitlab.com/api/v4/"
GITLAB_PROJECT_ID = "47320184"


def _get(url: str) -> urllib3.BaseHTTPResponse:
    username = st.secrets[GITLAB_USER_SECRET]
    password = st.secrets[GITLAB_TOKEN_SECRET]
    request_headers = urllib3.make_headers(
        basic_auth=f"{username}:{password}",
        disable_cache=True,
    )
    return urllib3.request("GET", url, headers=request_headers, retries=3, timeout=15)


def _fetch_gitlab_artifacts():
    return _get(f"{GITLAB_BASE_URL}/projects/{GITLAB_PROJECT_ID}/jobs")


if __name__ == "__main__":
    resp = _fetch_gitlab_artifacts()
    print(resp.json())
