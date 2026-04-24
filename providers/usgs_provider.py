import datetime as dt
from typing import Any, Dict, List

from config import USGS_CLOUD_COVER
from shared_utils import get_usgs_access_token, get_usgs_dataset_id, normalize_target_date, usgs_send_request


def search_usgs_scenes(
    lake: Dict[str, Any],
    sensor: Dict[str, Any],
    start_time: dt.datetime,
    end_time: dt.datetime,
) -> List[Dict[str, Any]]:
    token = get_usgs_access_token()
    dataset_name = get_usgs_dataset_id(
        satellite=str(sensor.get("satellite", "Landsat 8-9")),
        level=str(sensor.get("product_level", "L1")),
    )

    lon_min, lat_min, lon_max, lat_max = lake["bbox"]
    spatial_filter = {
        "filterType": "mbr",
        "lowerLeft": {"latitude": lat_min, "longitude": lon_min},
        "upperRight": {"latitude": lat_max, "longitude": lon_max},
    }
    search_payload = {
        "datasetName": dataset_name,
        "sceneFilter": {
            "spatialFilter": spatial_filter,
            "acquisitionFilter": {
                "start": start_time.strftime("%Y-%m-%d"),
                "end": end_time.strftime("%Y-%m-%d"),
            },
            "cloudCoverFilter": {"min": 0, "max": USGS_CLOUD_COVER},
        },
    }

    scene_search = usgs_send_request("scene-search", search_payload, token) or {}
    scene_results = scene_search.get("results", [])
    if not scene_results:
        return []

    entity_ids = []
    scene_meta: Dict[str, Dict[str, Any]] = {}
    for scene in scene_results:
        entity_id = scene.get("entityId") or scene.get("entityID")
        if not entity_id:
            continue
        entity_ids.append(entity_id)
        scene_meta[str(entity_id)] = scene

    if not entity_ids:
        return []

    options_payload = {"datasetName": dataset_name, "entityIds": entity_ids}
    download_options = usgs_send_request("download-options", options_payload, token) or []

    product_by_entity: Dict[str, List[str]] = {}
    for product in download_options:
        entity_id = str(product.get("entityId") or "")
        product_id = str(product.get("id") or "")
        if not entity_id or not product_id:
            continue
        if product.get("available") and product.get("downloadSystem") != "folder":
            product_by_entity.setdefault(entity_id, []).append(product_id)

    scenes: List[Dict[str, Any]] = []
    for entity_id in entity_ids:
        entity_key = str(entity_id)
        product_ids = product_by_entity.get(entity_key, [])
        if not product_ids:
            continue
        first_product = product_ids[0]
        meta = scene_meta.get(entity_key, {})
        scene_name = str(meta.get("displayId") or entity_key)
        date_raw = str(meta.get("acquisitionDate") or meta.get("startTime") or "")
        target_date = normalize_target_date(date_raw)

        scenes.append(
            {
                "provider": "usgs",
                "sensor_key": sensor["key"],
                "platform": sensor["platform_name"],
                "scene_id": entity_key,
                "scene_name": scene_name,
                "target_date": target_date,
                "download_url": "",
                "archive_ext": sensor.get("archive_ext", "tar"),
                "usgs_dataset_name": dataset_name,
                "usgs_product_id": first_product,
            }
        )

    return scenes
