import datetime as dt
import logging
import os
import re
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from threading import local
from typing import Any, Dict, Optional
from uuid import uuid4

import requests

from config import (
    CDSE_OAUTH_CLIENT_ID,
    CDSE_PASSWORD,
    CDSE_USERNAME,
    HTTP_TIMEOUT_SECONDS,
    USGS_SERVICE_URL,
    USGS_TOKEN,
    USGS_USERNAME,
)

CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

_http = local()
_CDSE_TOKEN_CACHE = ("", 0.0)
_USGS_TOKEN_CACHE = ("", 0.0)


def http_session() -> requests.Session:
    # Each processing/search thread owns its connection pool.
    if not hasattr(_http, "session"):
        _http.session = requests.Session()
    return _http.session


def normalize_target_date(value: str) -> str:
    if not value:
        return "unknown"
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y%m%d")
    except ValueError:
        return value[:10].replace("-", "")


def _raise_for_status(response: requests.Response) -> None:
    if response.status_code >= 400:
        # Signed download URLs can contain credentials.
        raise requests.HTTPError(f"HTTP {response.status_code}", response=response)


def _retry_delay(exc: Exception, attempt: int) -> Optional[float]:
    delay = 2 ** (attempt - 1)
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code
        if status not in (408, 429) and status < 500:
            return None
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after:
            try:
                seconds = int(retry_after) if retry_after.isdecimal() else (
                    parsedate_to_datetime(retry_after).timestamp() - time.time()
                )
                return max(delay, seconds)
            except (ValueError, OverflowError):
                pass
    return delay


def request_json(method: str, url: str, *, retries: int = 1, **kwargs) -> Any:
    for attempt in range(1, retries + 1):
        try:
            with http_session().request(method, url, timeout=HTTP_TIMEOUT_SECONDS, **kwargs) as response:
                _raise_for_status(response)
                return response.json()
        except requests.RequestException as exc:
            delay = _retry_delay(exc, attempt)
            if attempt == retries or delay is None:
                raise
            time.sleep(delay)


class IncompleteDownload(OSError):
    pass


def _receive_download(response: requests.Response, partial: Path, validator_path: Path, offset: int) -> None:
    previous_validator = validator_path.read_text(encoding="utf-8") if offset and validator_path.exists() else ""
    etag = response.headers.get("ETag", "")
    validator = (etag if not etag.startswith("W/") else "") or response.headers.get("Last-Modified", "")
    same_version = not (validator and previous_validator and validator != previous_validator)
    content_range = response.headers.get("Content-Range", "")

    if response.status_code == 416:
        complete_range = re.fullmatch(r"bytes \*/(\d+)", content_range)
        if offset and same_version and complete_range and int(complete_range[1]) == offset:
            return
        partial.unlink(missing_ok=True)
        validator_path.unlink(missing_ok=True)
        raise IncompleteDownload("Saved byte range no longer matches the remote file")

    _raise_for_status(response)
    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
        raise IncompleteDownload("Expected an unencoded download")

    if response.status_code == 206:
        byte_range = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
        if not byte_range or not same_version:
            partial.unlink(missing_ok=True)
            validator_path.unlink(missing_ok=True)
            raise IncompleteDownload("Invalid partial response or changed remote file")
        start, end, total = map(int, byte_range.groups())
        if start != offset or not start <= end < total:
            partial.unlink(missing_ok=True)
            validator_path.unlink(missing_ok=True)
            raise IncompleteDownload("Server returned a different byte range")
        mode = "ab"
        expected_end = end + 1
    elif response.status_code == 200:
        mode = "wb"
        total = expected_end = int(response.headers["Content-Length"]) if "Content-Length" in response.headers else None
    else:
        raise IncompleteDownload(f"Unexpected download status {response.status_code}")

    with partial.open(mode) as handle:
        if validator:
            validator_path.write_text(validator, encoding="utf-8")
        elif mode == "wb":
            validator_path.unlink(missing_ok=True)
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
        size = handle.tell()

    if not size or (expected_end is not None and size != expected_end) or (total is not None and size != total):
        if expected_end is not None and size > expected_end:
            partial.unlink(missing_ok=True)
            validator_path.unlink(missing_ok=True)
        raise IncompleteDownload(f"Received {size} bytes of {total or 'unknown'}")


def download_file(
    url: str,
    output_path: str,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
) -> str:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".part")
    validator_path = partial.with_suffix(".part.validator")

    for attempt in range(1, max_retries + 1):
        request_headers = {**(headers or {}), "Accept-Encoding": "identity"}
        offset = partial.stat().st_size if partial.exists() else 0
        if offset:
            request_headers["Range"] = f"bytes={offset}-"
            if validator_path.exists():
                request_headers["If-Range"] = validator_path.read_text(encoding="utf-8")
            logging.info("Resuming %s at byte %d", target.name, offset)

        try:
            with http_session().get(url, headers=request_headers, stream=True, timeout=HTTP_TIMEOUT_SECONDS) as response:
                _receive_download(response, partial, validator_path, offset)
        except (requests.RequestException, IncompleteDownload) as exc:
            delay = _retry_delay(exc, attempt)
            if delay is None:
                raise
            logging.warning("Download %s attempt %d/%d failed: %s",
                            target.name, attempt, max_retries, type(exc).__name__)
            if attempt == max_retries:
                if isinstance(exc, (requests.HTTPError, IncompleteDownload)):
                    raise
                raise IncompleteDownload(f"Transfer interrupted: {type(exc).__name__}") from None
            time.sleep(delay)
        else:
            partial.replace(target)
            validator_path.unlink(missing_ok=True)
            return str(target)
    raise ValueError("max_retries must be positive")


def get_cdse_access_token(force_refresh: bool = False) -> str:
    global _CDSE_TOKEN_CACHE
    token, expiry = _CDSE_TOKEN_CACHE
    now = time.monotonic()
    if not force_refresh and token and expiry - 30 > now:
        return token
    username = os.getenv("CDSE_USERNAME", CDSE_USERNAME)
    password = os.getenv("CDSE_PASSWORD", CDSE_PASSWORD)
    if not username or not password:
        raise RuntimeError("Missing CDSE_USERNAME or CDSE_PASSWORD")
    payload = request_json("POST", CDSE_TOKEN_URL, data={
        "client_id": CDSE_OAUTH_CLIENT_ID, "grant_type": "password",
        "username": username, "password": password,
    })
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("CDSE token response missing access_token")
    _CDSE_TOKEN_CACHE = token, now + int(payload.get("expires_in", 3600))
    return token


def download_from_copernicus(url: str, output_path: str, token: Optional[str] = None) -> str:
    token = token or get_cdse_access_token()
    try:
        return download_file(url, output_path, headers={"Authorization": "Bearer " + token})
    except requests.HTTPError as exc:
        if exc.response.status_code != 401:
            raise
    token = get_cdse_access_token(force_refresh=True)
    return download_file(url, output_path, headers={"Authorization": "Bearer " + token})


def get_usgs_dataset_id(satellite: str, level: str) -> str:
    if satellite in ("Landsat 4", "Landsat 5"):
        prefix = "landsat_tm_c2_"
    elif satellite == "Landsat 7":
        prefix = "landsat_etm_c2_"
    elif satellite in ("Landsat 8", "Landsat 9", "Landsat 8-9"):
        prefix = "landsat_ot_c2_"
    else:
        raise ValueError(f"Invalid Landsat satellite: {satellite}")
    return prefix + level.lower()


def get_usgs_access_token(force_refresh: bool = False) -> str:
    global _USGS_TOKEN_CACHE
    token, expiry = _USGS_TOKEN_CACHE
    now = time.monotonic()
    if not force_refresh and token and expiry - 30 > now:
        return token
    username = os.getenv("USGS_USERNAME", USGS_USERNAME)
    user_token = os.getenv("USGS_TOKEN", USGS_TOKEN)
    if not username or not user_token:
        raise RuntimeError("Missing USGS_USERNAME or USGS_TOKEN")
    token = usgs_send_request("login-token", {"username": username, "token": user_token})
    if not token:
        raise RuntimeError("USGS login-token did not return token data")
    _USGS_TOKEN_CACHE = token, now + 50 * 60
    return token


def usgs_send_request(endpoint: str, data: Dict[str, Any], api_key: Optional[str] = None) -> Any:
    headers = {"X-Auth-Token": api_key} if api_key else {}
    retries = 3 if endpoint in {"scene-search", "download-options", "download-retrieve"} else 1
    payload = request_json("POST", USGS_SERVICE_URL.rstrip("/") + "/" + endpoint.lstrip("/"),
                           retries=retries, json=data, headers=headers)
    if payload.get("errorCode"):
        message = payload.get("errorMessage", "") if endpoint != "login-token" else ""
        raise RuntimeError(f"USGS {endpoint}: {payload['errorCode']} {message}".strip())
    return payload.get("data")


def request_usgs_download_url(
    scene_id: str,
    product_id: str,
    dataset_name: str,
    label_prefix: str = "landsat",
) -> str:
    if not scene_id or not product_id:
        raise ValueError("USGS download task needs a scene ID and bundle product ID")
    token = get_usgs_access_token()
    label = f"{label_prefix}-{uuid4().hex}"
    request_result = usgs_send_request("download-request", {
        "downloads": [{"entityId": scene_id, "productId": product_id}],
        "label": label,
    }, token) or {}
    for entry in request_result.get("availableDownloads", []):
        if entry.get("url"):
            return str(entry["url"])

    if not request_result.get("preparingDownloads"):
        raise RuntimeError(f"USGS returned no download for {dataset_name}/{scene_id}")

    delay = 2
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        time.sleep(min(delay, max(0, deadline - time.monotonic())))
        retrieved = usgs_send_request("download-retrieve", {"label": label}, token) or {}
        for entry in retrieved.get("available", []):
            if entry.get("url"):
                return str(entry["url"])
        delay = min(delay * 2, 20)
    raise RuntimeError(f"USGS download URL not ready for {scene_id}")


def download_from_usgs(scene_id: str, product_id: str, dataset_name: str, output_path: str) -> str:
    url = request_usgs_download_url(scene_id, product_id, dataset_name)
    try:
        return download_file(url, output_path)
    except requests.HTTPError as exc:
        if exc.response.status_code not in (401, 403):
            raise
    return download_file(request_usgs_download_url(scene_id, product_id, dataset_name), output_path)


def getfromCopernicus(url: str, output_path: str, token: str) -> bool:
    try:
        download_from_copernicus(url, output_path, token=token)
        return True
    except Exception as exc:
        logging.error("Download failed: %s", exc)
        return False
