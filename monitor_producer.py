import datetime as dt
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from config import DOWNLOAD_QUEUE_NAME, MONITOR_LOOKBACK_DAYS, get_enabled_sensors, load_lakes
from pipeline_queue import publish, queue_channel
from providers import PROVIDER_SEARCHERS
from providers.usgs_provider import resolve_usgs_products


def _collect_scenes(provider, sensors, lakes, start_time, end_time):
    started = time.perf_counter()
    search = PROVIDER_SEARCHERS[provider]
    scenes = {}
    errors = 0
    for lake in lakes:
        for sensor in sensors:
            try:
                matches = search(lake=lake, sensor=sensor, start_time=start_time, end_time=end_time)
            except Exception as exc:
                logging.error("Search failed lake=%s sensor=%s: %s", lake["uid"], sensor["key"], exc)
                errors += 1
                continue
            for scene in matches:
                key = (scene["sensor_key"], scene["scene_id"])
                task = scenes.setdefault(key, {**scene, "lakes": {}})
                task["lakes"][lake["uid"]] = lake

    tasks = list(scenes.values())
    for task in tasks:
        task["lakes"] = list(task["lakes"].values())
    if provider == "usgs":
        try:
            tasks = resolve_usgs_products(tasks)
        except Exception as exc:
            logging.error("USGS bundle lookup failed: %s", exc)
            tasks = []
            errors += 1
    logging.info("provider=%s scenes=%d search_errors=%d duration=%.2fs",
                 provider, len(tasks), errors, time.perf_counter() - started)
    return tasks, errors


def monitor_and_dispatch() -> None:
    end_time = dt.datetime.now(dt.timezone.utc)
    start_time = end_time - dt.timedelta(days=MONITOR_LOOKBACK_DAYS)
    sensors = get_enabled_sensors()
    if not sensors:
        raise ValueError("No sensors enabled. Check ENABLED_SENSOR_KEYS")
    lakes = load_lakes()
    logging.info("Searching %d lakes for %s over %d days", len(lakes),
                 ", ".join(sensor["key"] for sensor in sensors), MONITOR_LOOKBACK_DAYS)
    by_provider = defaultdict(list)
    for sensor in sensors:
        by_provider[sensor["provider"]].append(sensor)

    dispatched = errors = 0
    with queue_channel(DOWNLOAD_QUEUE_NAME) as (connection, channel):
        with ThreadPoolExecutor(max_workers=len(by_provider)) as pool:
            pending = {
                pool.submit(_collect_scenes, provider, entries, lakes, start_time, end_time)
                for provider, entries in by_provider.items()
            }
            while pending:
                for future in [future for future in pending if future.done()]:
                    tasks, search_errors = future.result()
                    pending.remove(future)
                    errors += search_errors
                    for task in tasks:
                        publish(channel, DOWNLOAD_QUEUE_NAME, task)
                        dispatched += 1
                if pending:
                    connection.process_data_events(time_limit=0.2)

    logging.info("Producer dispatched %d scene tasks with %d search errors", dispatched, errors)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - PRODUCER - %(message)s")
    monitor_and_dispatch()
