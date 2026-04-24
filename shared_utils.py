import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

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

_CDSE_TOKEN_CACHE: Dict[str, Union[float, str]] = {"access_token": "", "expires_at": 0.0}
_USGS_TOKEN_CACHE: Dict[str, Union[float, str]] = {"access_token": "", "expires_at": 0.0}


def normalize_target_date(value: str) -> str:
    if not value:
        return "unknown"
    cleaned = value.replace("Z", "+00:00")
    try:
        import datetime as dt

        return dt.datetime.fromisoformat(cleaned).strftime("%Y%m%d")
    except ValueError:
        return value[:10].replace("-", "")


def download_file(
        url: str,
        output_path: str,
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
) -> str:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(target.suffix + ".part")

    for attempt in range(1, max_retries + 1):
        # 每次循环都拷贝一份干净的 headers，防止污染
        req_headers = headers.copy() if headers else {}

        # 1. 检查本地是否已有未下完的 .part 文件
        initial_file_size = 0
        if temp_path.exists():
            initial_file_size = temp_path.stat().st_size
            # 告诉服务器：请从 initial_file_size 这个字节开始传
            req_headers["Range"] = f"bytes={initial_file_size}-"
            logging.info("Resuming download from byte %s", initial_file_size)

        try:
            with requests.get(
                    url,
                    headers=req_headers,
                    stream=True,
                    timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:

                # 如果服务器支持断点续传，会返回 206 Partial Content
                # 如果返回 200 OK，说明服务器忽略了 Range 头，打算从头重传
                if response.status_code == 206:
                    mode = "ab"  # 追加模式
                elif response.status_code == 200:
                    mode = "wb"  # 覆盖模式
                    initial_file_size = 0  # 重置概念大小
                    logging.warning("Server ignored Range header, restarting download from 0")
                else:
                    response.raise_for_status()

                # 2. 动态决定是以 'ab' 追加还是 'wb' 覆盖的方式打开文件
                with temp_path.open(mode) as handle:
                    # 加大 chunk_size 到 8MB，减少 I/O 阻塞造成的断流
                    for chunk in response.iter_content(chunk_size=8192 * 1024):
                        if chunk:
                            handle.write(chunk)

            # 下载完整且成功，重命名为正式文件
            temp_path.replace(target)
            return str(target)

        except Exception as exc:
            logging.warning("Download failed attempt %s/%s: %s", attempt, max_retries, exc)
            if attempt == max_retries:
                # 达到最大重试次数时，我们保留 .part 文件，不执行 temp_path.unlink()
                # 这样下次脚本跑起来或者放到另外的网络环境下，依然可以从断点继续！
                raise
            time.sleep(5 * attempt)

    return str(target)

# ----------------------------
# CDSE helpers
# ----------------------------
def get_cdse_access_token(force_refresh: bool = False) -> str:
    username = os.getenv("CDSE_USERNAME", CDSE_USERNAME)
    password = os.getenv("CDSE_PASSWORD", CDSE_PASSWORD)
    if not username or not password:
        raise RuntimeError("Missing CDSE credentials. Set CDSE_USERNAME and CDSE_PASSWORD.")

    now = time.time()
    cached_token = str(_CDSE_TOKEN_CACHE.get("access_token") or "")
    cached_expiry = float(_CDSE_TOKEN_CACHE.get("expires_at") or 0)
    if not force_refresh and cached_token and cached_expiry - 30 > now:
        return cached_token

    payload = {
        "client_id": CDSE_OAUTH_CLIENT_ID,
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    response = requests.post(CDSE_TOKEN_URL, data=payload, timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    token_payload = response.json()
    token = token_payload.get("access_token")
    if not token:
        raise RuntimeError("CDSE token response missing access_token")

    expires_in = int(token_payload.get("expires_in", 3600))
    _CDSE_TOKEN_CACHE["access_token"] = token
    _CDSE_TOKEN_CACHE["expires_at"] = now + expires_in
    return token


def download_from_copernicus(url: str, output_path: str, token: Optional[str] = None) -> str:
    working_token = token or get_cdse_access_token()
    headers = {"Authorization": "Bearer " + working_token}
    try:
        return download_file(url, output_path, headers=headers, max_retries=3)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 401:
            refreshed = get_cdse_access_token(force_refresh=True)
            headers = {"Authorization": "Bearer " + refreshed}
            return download_file(url, output_path, headers=headers, max_retries=3)
        raise


# ----------------------------
# USGS M2M helpers
# ----------------------------
def get_usgs_dataset_id(satellite: str, level: str) -> str:
    level_lower = level.lower()
    if satellite in ["Landsat 4", "Landsat 5"]:
        return "landsat_tm_c2_" + level_lower
    if satellite == "Landsat 7":
        return "landsat_etm_c2_" + level_lower
    if satellite in ["Landsat 8", "Landsat 9", "Landsat 8-9"]:
        return "landsat_ot_c2_" + level_lower
    raise ValueError("Invalid Landsat satellite: " + satellite)


def get_usgs_access_token(force_refresh: bool = False) -> str:
    username = os.getenv("USGS_USERNAME", USGS_USERNAME)
    user_token = os.getenv("USGS_TOKEN", USGS_TOKEN)
    if not username or not user_token:
        raise RuntimeError("Missing USGS credentials. Set USGS_USERNAME and USGS_TOKEN.")

    now = time.time()
    cached_token = str(_USGS_TOKEN_CACHE.get("access_token") or "")
    cached_expiry = float(_USGS_TOKEN_CACHE.get("expires_at") or 0)
    if not force_refresh and cached_token and cached_expiry - 30 > now:
        return cached_token

    response = requests.post(
        USGS_SERVICE_URL.rstrip("/") + "/login-token",
        json={"username": username, "token": user_token},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errorCode"):
        raise RuntimeError(payload.get("errorCode") + ": " + str(payload.get("errorMessage")))

    token = payload.get("data")
    if not token:
        raise RuntimeError("USGS login-token did not return token data.")

    # USGS token lifetime is long enough for one run; refresh every 50 minutes.
    _USGS_TOKEN_CACHE["access_token"] = token
    _USGS_TOKEN_CACHE["expires_at"] = now + 50 * 60
    return token


def usgs_send_request(endpoint: str, data: Dict[str, Any], api_key: Optional[str] = None) -> Any:
    headers = {}
    if api_key:
        headers["X-Auth-Token"] = api_key

    response = requests.post(
        USGS_SERVICE_URL.rstrip("/") + "/" + endpoint.lstrip("/"),
        json=data,
        headers=headers,
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errorCode"):
        raise RuntimeError(str(payload.get("errorCode")) + ": " + str(payload.get("errorMessage")))
    return payload.get("data")


def request_usgs_download_url(
    scene_id: str,
    product_id: str,
    dataset_name: str,
    label_prefix: str = "landsat",
) -> str:
    token = get_usgs_access_token()
    label = label_prefix + "-" + scene_id + "-" + str(int(time.time()))

    request_payload = {
        "downloads": [{"entityId": scene_id, "productId": product_id}],
        "label": label,
    }
    request_result = usgs_send_request("download-request", request_payload, token) or {}

    for entry in request_result.get("availableDownloads", []):
        url = entry.get("url")
        if url:
            return str(url)

    preparing = request_result.get("preparingDownloads", [])
    if not preparing:
        raise RuntimeError("USGS download-request returned no available/preparing downloads for " + scene_id)

    max_poll = 15
    for _ in range(max_poll):
        time.sleep(20)
        retrieve_result = usgs_send_request("download-retrieve", {"label": label}, token) or {}
        for entry in retrieve_result.get("available", []):
            url = entry.get("url")
            if url:
                return str(url)

    raise RuntimeError("USGS download URL not ready after polling: " + scene_id)


def download_from_usgs(scene_id: str, product_id: str, dataset_name: str, output_path: str) -> str:
    url = request_usgs_download_url(scene_id=scene_id, product_id=product_id, dataset_name=dataset_name)
    return download_file(url=url, output_path=output_path, headers=None, max_retries=3)


# Backward-compatible alias for old imports.
def getfromCopernicus(url: str, output_path: str, token: str) -> bool:
    try:
        download_from_copernicus(url, output_path, token=token)
        return True
    except Exception as exc:
        logging.error("Download failed: %s", exc)
        return False
