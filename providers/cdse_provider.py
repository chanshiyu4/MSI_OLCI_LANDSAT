import datetime as dt
from typing import Any, Dict, List
from urllib.parse import urljoin

from config import SEARCH_PAGE_SIZE
from shared_utils import normalize_target_date, request_json

CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"


def _to_cdse_time(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _build_aoi_polygon(bbox: List[float]) -> str:
    west, south, east, north = bbox
    vertices = [(west, south), (east, south), (east, north), (west, north), (west, south)]
    return "POLYGON((" + ", ".join(f"{lon} {lat}" for lon, lat in vertices) + "))"


def search_cdse_scenes(
    lake: Dict[str, Any],
    sensor: Dict[str, Any],
    start_time: dt.datetime,
    end_time: dt.datetime,
) -> List[Dict[str, Any]]:
    aoi = _build_aoi_polygon(lake["bbox"])
    params = {
        "$filter": (
            f"Collection/Name eq '{sensor['collection']}' and contains(Name,'{sensor['name_filter']}')"
            f" and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}')"
            f" and ContentDate/Start gt {_to_cdse_time(start_time)}"
            f" and ContentDate/Start lt {_to_cdse_time(end_time)}"
        ),
        "$select": "Id,Name,ContentDate",
        "$top": SEARCH_PAGE_SIZE,
        "$orderby": "ContentDate/Start desc",
    }
    scenes = []
    url = CATALOG_URL
    while url:
        page = request_json("GET", url, retries=3, params=params)
        for item in page.get("value", []):
            scene_id, name = item.get("Id"), item.get("Name")
            if scene_id and name:
                scenes.append({
                    "provider": "cdse",
                    "sensor_key": sensor["key"],
                    "platform": sensor["platform_name"],
                    "scene_id": scene_id,
                    "scene_name": name,
                    "target_date": normalize_target_date((item.get("ContentDate") or {}).get("Start") or ""),
                    "download_url": f"https://download.dataspace.copernicus.eu/odata/v1/Products({scene_id})/$value",
                    "archive_ext": sensor.get("archive_ext", "zip"),
                })
        next_url = page.get("@odata.nextLink") or page.get("@OData.nextLink")
        url = urljoin(url, next_url) if next_url else None
        params = None
    return scenes
