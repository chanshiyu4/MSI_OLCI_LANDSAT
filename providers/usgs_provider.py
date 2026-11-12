import datetime as dt
import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List

from config import SEARCH_PAGE_SIZE, USGS_CLOUD_COVER
from shared_utils import get_usgs_access_token, get_usgs_dataset_id, normalize_target_date, usgs_send_request

DOWNLOAD_OPTIONS_BATCH_SIZE = 100


def search_usgs_scenes(
    lake: Dict[str, Any],
    sensor: Dict[str, Any],
    start_time: dt.datetime,
    end_time: dt.datetime,
) -> List[Dict[str, Any]]:
    token = get_usgs_access_token()
    dataset = get_usgs_dataset_id(sensor.get("satellite", "Landsat 8-9"), sensor.get("product_level", "L1"))
    west, south, east, north = lake["bbox"]
    payload = {
        "datasetName": dataset,
        "maxResults": SEARCH_PAGE_SIZE,
        "startingNumber": 1,
        "sceneFilter": {
            "spatialFilter": {
                "filterType": "mbr",
                "lowerLeft": {"latitude": south, "longitude": west},
                "upperRight": {"latitude": north, "longitude": east},
            },
            "acquisitionFilter": {
                "start": start_time.strftime("%Y-%m-%d"),
                "end": end_time.strftime("%Y-%m-%d"),
            },
            "cloudCoverFilter": {"min": 0, "max": USGS_CLOUD_COVER},
        },
    }

    scenes = []
    while True:
        page = usgs_send_request("scene-search", payload, token) or {}
        results = page.get("results") or []
        for scene in results:
            entity_id = scene.get("entityId") or scene.get("entityID")
            if not entity_id:
                continue
            name = scene.get("displayId") or str(entity_id)
            date = scene.get("acquisitionDate") or scene.get("startTime")
            if not date:
                date = (scene.get("temporalCoverage") or {}).get("startDate")
            if not date and name.startswith(("LC08_", "LC09_", "LO08_", "LO09_")):
                parts = name.split("_")
                if len(parts) > 3:
                    date = parts[3]
            scenes.append({
                "provider": "usgs",
                "sensor_key": sensor["key"],
                "platform": sensor["platform_name"],
                "scene_id": str(entity_id),
                "scene_name": name,
                "target_date": normalize_target_date(date or ""),
                "archive_ext": sensor.get("archive_ext", "tar"),
                "usgs_dataset_name": dataset,
            })
        next_record = int(page.get("nextRecord") or 0)
        if not results or next_record <= payload["startingNumber"]:
            break
        payload["startingNumber"] = next_record
    return scenes


def find_bundle_ids(dataset: str, scene_ids: List[str]) -> Dict[str, str]:
    token = get_usgs_access_token()
    bundles = {}
    for index in range(0, len(scene_ids), DOWNLOAD_OPTIONS_BATCH_SIZE):
        batch = scene_ids[index:index + DOWNLOAD_OPTIONS_BATCH_SIZE]
        products = usgs_send_request("download-options", {"datasetName": dataset, "entityIds": batch}, token) or []
        for product in products:
            name = str(product.get("productName") or product.get("displayName") or "").casefold()
            scene_id = str(product.get("entityId") or "")
            if (scene_id and product.get("id") and product.get("available")
                    and product.get("downloadSystem") != "folder" and "bundle" in name):
                bundles.setdefault(scene_id, str(product["id"]))
    return bundles


def resolve_usgs_products(scenes: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_dataset = defaultdict(list)
    for scene in scenes:
        by_dataset[scene["usgs_dataset_name"]].append(scene)

    ready = []
    for dataset, tasks in by_dataset.items():
        bundle_ids = find_bundle_ids(dataset, [task["scene_id"] for task in tasks])
        for task in tasks:
            bundle_id = bundle_ids.get(task["scene_id"])
            if bundle_id:
                task["usgs_product_id"] = bundle_id
                ready.append(task)
            else:
                logging.warning("No available Landsat product bundle for %s", task["scene_name"])
    return ready
