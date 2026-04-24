import datetime as dt
from typing import Any, Dict, List

import requests

from config import HTTP_TIMEOUT_SECONDS, SEARCH_PAGE_SIZE
from shared_utils import normalize_target_date

CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"


def _to_cdse_time(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _build_aoi_polygon(bbox: List[float]) -> str:
    lon_min, lat_min, lon_max, lat_max = bbox
    return (
        "POLYGON(("
        + str(lon_min)
        + " "
        + str(lat_min)
        + ", "
        + str(lon_min)
        + " "
        + str(lat_max)
        + ", "
        + str(lon_max)
        + " "
        + str(lat_max)
        + ", "
        + str(lon_max)
        + " "
        + str(lat_min)
        + ", "
        + str(lon_min)
        + " "
        + str(lat_min)
        + "))"
    )


def search_cdse_scenes(
    lake: Dict[str, Any],
    sensor: Dict[str, Any],
    start_time: dt.datetime,
    end_time: dt.datetime,
) -> List[Dict[str, Any]]:
    aoi_wkt = _build_aoi_polygon(lake["bbox"])
    start_text = _to_cdse_time(start_time)
    end_text = _to_cdse_time(end_time)
    filter_text = (
        "contains(Name,'"
        + str(sensor["name_filter"])
        + "') and Collection/Name eq '"
        + str(sensor["collection"])
        + "' and OData.CSC.Intersects(area=geography'SRID=4326;"
        + aoi_wkt
        + "') and ContentDate/Start gt "
        + start_text
        + " and ContentDate/Start lt "
        + end_text
    )

    items: List[Dict[str, Any]] = []
    skip = 0
    while True:
        params = {
            "$filter": filter_text,
            "$top": SEARCH_PAGE_SIZE,
            "$skip": skip,
            "$orderby": "ContentDate/Start desc",
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(CATALOG_URL, params=params, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
        page_items = response.json().get("value", [])
        items.extend(page_items)
        if len(page_items) < SEARCH_PAGE_SIZE:
            break
        skip += SEARCH_PAGE_SIZE

    scenes: List[Dict[str, Any]] = []
    for item in items:
        scene_id = item.get("Id")
        scene_name = item.get("Name")
        if not scene_id or not scene_name:
            continue
        start_value = item.get("ContentDate", {}).get("Start", "")
        target_date = normalize_target_date(str(start_value))
        scenes.append(
            {
                "provider": "cdse",
                "sensor_key": sensor["key"],
                "platform": sensor["platform_name"],
                "scene_id": scene_id,
                "scene_name": scene_name,
                "target_date": target_date,
                "download_url": "https://zipper.dataspace.copernicus.eu/odata/v1/Products(" + str(scene_id) + ")/$value",
                "archive_ext": sensor.get("archive_ext", "zip"),
            }
        )
    return scenes
