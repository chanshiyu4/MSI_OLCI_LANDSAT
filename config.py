import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def env_flag(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


# --- 基础路径与消息队列配置 ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
DOWNLOAD_QUEUE_NAME = os.getenv("DOWNLOAD_QUEUE_NAME", "download_acolite_queue")
ACOLITE_TASK_QUEUE = os.getenv("ACOLITE_TASK_QUEUE", "acolite_run_queue")
POSTPROCESS_QUEUE_NAME = os.getenv("POSTPROCESS_QUEUE_NAME", "mosaic_postprocess_queue")

# --- 目录路径配置 ---
TEMP_DIR = os.getenv("TEMP_DIR") or str(BASE_DIR / "data" / "raw")
ACOLITE_OUT_DIR = os.getenv("ACOLITE_OUT_DIR") or str(BASE_DIR / "data" / "acolite")
FINAL_OUTPUT_DIR = os.getenv("FINAL_OUTPUT_DIR") or str(BASE_DIR / "data" / "products")
# 存放所有湖泊 SHP 文件的目录
MASK_SHP_DIR = os.getenv("MASK_SHP_DIR") or str(BASE_DIR / "lake_shp")

# --- ACOLITE 环境配置 ---
ACOLITE_CLI_PATH = os.getenv("ACOLITE_CLI_PATH") or str(BASE_DIR / "launch_acolite.py")
ACOLITE_PYTHON = os.getenv("ACOLITE_PYTHON") or sys.executable
ACOLITE_EXPORT_L2R = env_flag("ACOLITE_EXPORT_L2R", True)
ACOLITE_KEEP_NETCDF = env_flag("ACOLITE_KEEP_NETCDF", True)

# --- 系统运行参数 ---
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))
MONITOR_LOOKBACK_DAYS = int(os.getenv("MONITOR_LOOKBACK_DAYS", "1"))
SEARCH_PAGE_SIZE = max(1, min(1000, int(os.getenv("SEARCH_PAGE_SIZE", "100"))))
CLEAN_ACOLITE_TEMP = env_flag("CLEAN_ACOLITE_TEMP")
KEEP_RAW_CACHE = env_flag("KEEP_RAW_CACHE")
GDAL_THREADS = max(1, int(os.getenv("GDAL_THREADS", "1")))

# --- 欧空局 CDSE 账号配置 (Sentinel) ---
CDSE_OAUTH_CLIENT_ID = os.getenv("CDSE_OAUTH_CLIENT_ID", "cdse-public")
CDSE_USERNAME = os.getenv("CDSE_USERNAME", "")
CDSE_PASSWORD = os.getenv("CDSE_PASSWORD", "")

# --- USGS M2M 账号配置 (Landsat) ---
USGS_USERNAME = os.getenv("USGS_USERNAME", "")
USGS_TOKEN = os.getenv("USGS_TOKEN", "")
USGS_SERVICE_URL = os.getenv("USGS_SERVICE_URL", "https://m2m.cr.usgs.gov/api/api/json/stable/")
USGS_CLOUD_COVER = int(os.getenv("USGS_CLOUD_COVER", "100"))

# --- 启用的传感器 ---
ENABLED_SENSOR_KEYS = {
    value.strip().upper()
    for value in os.getenv("ENABLED_SENSOR_KEYS", "MSI,OLCI,LANDSAT").split(",")
    if value.strip()
}

SENSORS: List[Dict[str, Any]] = [
    {
        "key": "MSI",
        "provider": "cdse",
        "collection": "SENTINEL-2",
        "name_filter": "MSIL1C",
        "platform_name": "Sentinel-2",
        "archive_ext": "zip",
    },
    {
        "key": "OLCI",
        "provider": "cdse",
        "collection": "SENTINEL-3",
        "name_filter": "OL_1_EFR___",
        "platform_name": "Sentinel-3",
        "archive_ext": "zip",
    },
    {
        "key": "LANDSAT",
        "provider": "usgs",
        "platform_name": "Landsat 8-9",
        "satellite": "Landsat 8-9",
        "product_level": "L1",
        "archive_ext": "tar",
    },
]


def get_enabled_sensors() -> List[Dict[str, Any]]:
    return [sensor for sensor in SENSORS if sensor["key"].upper() in ENABLED_SENSOR_KEYS]


def _validate_bbox(lake_id: str, bbox: List[float]) -> None:
    if len(bbox) != 4:
        raise ValueError(f"Lake '{lake_id}' bbox must have 4 numbers: [lon_min, lat_min, lon_max, lat_max]")
    lon_min, lat_min, lon_max, lat_max = bbox
    if (
        not all(math.isfinite(value) for value in bbox)
        or not -180 <= lon_min < lon_max <= 180
        or not -90 <= lat_min < lat_max <= 90
    ):
        raise ValueError(f"Lake '{lake_id}' has invalid WGS84 bounds: {bbox}")


def load_lakes(shp_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Read lake bounds without loading geopandas in the workers."""
    import geopandas as gpd

    path = (shp_dir or Path(MASK_SHP_DIR)).resolve()
    if not path.is_dir():
        raise FileNotFoundError(f"Shapefile directory not found: {path}")

    shp_files = sorted(path.glob("*.shp"))
    if not shp_files:
        raise ValueError(f"No .shp files found in {path}")

    lakes: List[Dict[str, Any]] = []

    for shp_file in shp_files:
        uid = shp_file.stem
        gdf = gpd.read_file(shp_file, columns=[])
        if gdf.crs is None:
            raise ValueError(f"Shapefile '{shp_file}' is missing its CRS")
        if gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        bbox = list(map(float, gdf.total_bounds))
        _validate_bbox(uid, bbox)
        west, south, east, north = bbox

        lakes.append({
            "uid": uid,
            "uname": uid,
            "bbox": bbox,
            "extent_wgs84": bbox,
            "limit": [south, west, north, east],  # ACOLITE limit order
            "shp_path": str(shp_file),
        })

    return lakes
