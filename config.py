import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
load_dotenv()  # 自动加载项目根目录下的 .env 文件
# --- 基础路径与消息队列配置 ---
BASE_DIR = Path(__file__).resolve().parent

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
DOWNLOAD_QUEUE_NAME = os.getenv("DOWNLOAD_QUEUE_NAME", "download_acolite_queue")
ACOLITE_TASK_QUEUE = os.getenv("ACOLITE_TASK_QUEUE", "acolite_run_queue")
POSTPROCESS_QUEUE_NAME = os.getenv("POSTPROCESS_QUEUE_NAME", "mosaic_postprocess_queue")

# --- 目录路径配置 ---
TEMP_DIR = os.getenv("TEMP_DIR", r"")
ACOLITE_OUT_DIR = os.getenv("ACOLITE_OUT_DIR", r"")
FINAL_OUTPUT_DIR = os.getenv("FINAL_OUTPUT_DIR", r"")
# 存放所有湖泊 SHP 文件的目录
MASK_SHP_DIR = os.getenv("MASK_SHP_DIR", r"")

# --- ACOLITE 环境配置 ---
ACOLITE_CLI_PATH = os.getenv("ACOLITE_CLI_PATH", r"launch_acolite.py")
ACOLITE_PYTHON = os.getenv("ACOLITE_PYTHON", r"miniconda3\envs\acolite\python.exe")

# --- 系统运行参数 ---
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))
MONITOR_LOOKBACK_DAYS = int(os.getenv("MONITOR_LOOKBACK_DAYS", "1"))
SEARCH_PAGE_SIZE = int(os.getenv("SEARCH_PAGE_SIZE", "100"))
CLEAN_ACOLITE_TEMP = os.getenv("CLEAN_ACOLITE_TEMP", "0").strip().lower() in {"1", "true", "yes", "on"}

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


def _validate_bbox(lake_id: str, bbox: List[Any]) -> None:
    if len(bbox) != 4:
        raise ValueError(f"Lake '{lake_id}' bbox must have 4 numbers: [lon_min, lat_min, lon_max, lat_max]")
    lon_min, lat_min, lon_max, lat_max = bbox
    if lon_min >= lon_max or lat_min >= lat_max:
        raise ValueError(f"Lake '{lake_id}' bbox order is invalid: {bbox}")


def load_lakes(shp_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    自动扫描目录下的所有 .shp 文件，动态生成湖泊字典。
    """
    # 局部导入 geopandas，防止拖慢不需要解析 SHP 的 Worker（如 Acolite Worker）的启动速度
    import geopandas as gpd

    path = shp_dir or Path(MASK_SHP_DIR)
    if not path.exists():
        raise FileNotFoundError(f"Shapefile directory not found: {path}")

    # 自动扫描目录下所有的 .shp 文件
    shp_files = list(path.glob("*.shp"))
    if not shp_files:
        raise ValueError(f"No .shp files found in {path}")

    lakes: List[Dict[str, Any]] = []

    for shp_file in shp_files:
        # 1. 动态生成 UID (例如从 "Dianchi.shp" 提取 "Dianchi")
        uid = shp_file.stem
        uname = uid

        # 2. 读取矢量文件并统一转换到 WGS84 坐标系 (EPSG:4326)
        gdf = gpd.read_file(shp_file)
        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        # 3. 提取外接矩形 total_bounds (返回: lon_min, lat_min, lon_max, lat_max)
        bounds = gdf.total_bounds

        # 必须转为标准的 Python float，否则后续 JSON 序列化在 RabbitMQ 传输时会报错
        lon_min, lat_min, lon_max, lat_max = [float(x) for x in bounds]
        bbox = [lon_min, lat_min, lon_max, lat_max]

        # 4. 校验坐标合理性
        _validate_bbox(uid, bbox)

        # 5. 组装成标准任务字典
        lakes.append(
            {
                "uid": uid,
                "uname": uname,
                "bbox": bbox,
                "extent_wgs84": bbox,
                "limit": [lat_min, lon_min, lat_max, lon_max],  # ACOLITE 专用范围: 南, 西, 北, 东
                "shp_path": str(shp_file)  # 将 SHP 绝对路径传递给后处理 Worker 进行高精度掩膜裁剪
            }
        )

    return lakes
