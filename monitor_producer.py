import datetime as dt
import json
import logging
import time
from typing import Any, Dict, List, Tuple

import pika

from config import DOWNLOAD_QUEUE_NAME, MONITOR_LOOKBACK_DAYS, RABBITMQ_HOST, get_enabled_sensors, load_lakes
from providers import PROVIDER_SEARCHERS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - PRODUCER - %(message)s")


def get_mq_channel() -> Tuple:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=DOWNLOAD_QUEUE_NAME, durable=True)
    return connection, channel


def build_registry_key(scene: Dict[str, Any]) -> str:
    return str(scene.get("sensor_key", "")) + ":" + str(scene.get("scene_id", ""))


def merge_task(scene_registry: Dict[str, Dict[str, Any]], scene: Dict[str, Any], lake: Dict[str, Any]) -> None:
    key = build_registry_key(scene)
    if key not in scene_registry:
        scene_registry[key] = {
            "provider": scene.get("provider"),
            "sensor_key": scene.get("sensor_key"),
            "platform": scene.get("platform"),
            "scene_id": scene.get("scene_id"),
            "scene_name": scene.get("scene_name"),
            "target_date": scene.get("target_date"),
            "download_url": scene.get("download_url", ""),
            "archive_ext": scene.get("archive_ext", "zip"),
            "usgs_dataset_name": scene.get("usgs_dataset_name", ""),
            "usgs_product_id": scene.get("usgs_product_id", ""),
            "lakes": [],
        }

    existing_uids = {entry["uid"] for entry in scene_registry[key]["lakes"]}
    if lake["uid"] not in existing_uids:
        scene_registry[key]["lakes"].append(
            {
                "uid": lake["uid"],
                "uname": lake["uname"],
                "limit": lake["limit"],
                "extent_wgs84": lake["extent_wgs84"],
                "shp_path": lake.get("shp_path", "")
            }
        )


def monitor_and_dispatch() -> None:
    # 修复 DeprecationWarning 警告，使用带时区的 UTC 时间
    end_time = dt.datetime.now(dt.timezone.utc)
    start_time = end_time - dt.timedelta(days=MONITOR_LOOKBACK_DAYS)
    lakes = load_lakes()
    sensors = get_enabled_sensors()

    if not sensors:
        raise RuntimeError("No sensors enabled. Check ENABLED_SENSOR_KEYS in environment.")

    scene_registry: Dict[str, Dict[str, Any]] = {}

    for lake in lakes:
        for sensor in sensors:
            time.sleep(2)  # 每次检索前强制休眠 2 秒，防止被欧空局 API 拉黑
            provider = str(sensor.get("provider"))
            searcher = PROVIDER_SEARCHERS.get(provider)
            if searcher is None:
                logging.error("No provider searcher registered for provider=%s", provider)
                continue

            try:
                scenes = searcher(
                    lake=lake,
                    sensor=sensor,
                    start_time=start_time,
                    end_time=end_time,
                )
            except Exception as exc:
                logging.error(
                    "Search failed lake=%s sensor=%s provider=%s error=%s",
                    lake["uid"],
                    sensor["key"],
                    provider,
                    exc,
                )
                continue

            for scene in scenes:
                merge_task(scene_registry, scene, lake)

    connection, channel = get_mq_channel()
    try:
        for task in scene_registry.values():
            lake_names = [lake["uname"] for lake in task["lakes"]]
            logging.info(
                "Dispatch provider=%s sensor=%s scene=%s lakes=%s",
                task["provider"],
                task["sensor_key"],
                task["scene_name"],
                lake_names,
            )
            channel.basic_publish(
                exchange="",
                routing_key=DOWNLOAD_QUEUE_NAME,
                body=json.dumps(task, ensure_ascii=False),
                properties=pika.BasicProperties(delivery_mode=2),
            )
    finally:
        connection.close()

    logging.info("Producer finished. Dispatched %d scene tasks.", len(scene_registry))


if __name__ == "__main__":
    monitor_and_dispatch()