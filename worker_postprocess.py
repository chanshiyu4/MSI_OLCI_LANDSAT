import glob
import json
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import pika
from osgeo import gdal

from config import CLEAN_ACOLITE_TEMP, FINAL_OUTPUT_DIR, MASK_SHP_DIR, POSTPROCESS_QUEUE_NAME, RABBITMQ_HOST

logging.basicConfig(level=logging.INFO, format="%(asctime)s - POSTPROCESS - %(message)s")

PARAMETER_PATTERN = re.compile(r"(Rrs_\d+|rhos_\d+|rhot_\d+|rhorc_\d+|fai)$", re.IGNORECASE)


def collect_parameter_groups(acolite_dir: str) -> Dict[str, List[str]]:
    groups: Dict[str, List[str]] = {}
    for tif_path in glob.glob(os.path.join(acolite_dir, "**", "*.tif"), recursive=True):
        stem = Path(tif_path).stem
        match = PARAMETER_PATTERN.search(stem)
        if not match:
            continue
        parameter = match.group(1)
        groups.setdefault(parameter, []).append(tif_path)
    return groups


def build_warp_options(shp_path: Optional[str]) -> gdal.WarpOptions:
    has_mask = bool(shp_path)
    return gdal.WarpOptions(
        creationOptions=["COMPRESS=LZW", "TILED=YES"],
        dstSRS="EPSG:4326",
        # 核心抠图参数
        cutlineDSName=shp_path if has_mask else None,
        cropToCutline=has_mask,
        # 增加 Alpha 通道，让湖泊之外的陆地背景强制变为透明，而不污染数据真实的 0 值
        dstAlpha=has_mask,
        srcNodata=0,
        dstNodata=0,
    )


def process_gis_mosaic(task: Dict[str, Any]) -> int:
    lake_uid = str(task.get("lake_uid", ""))
    target_date = str(task.get("target_date", ""))
    acolite_dir = str(task.get("acolite_dir", ""))

    # 🌟 核心对接：获取由 Producer 一路传过来的精准 SHP 路径
    task_shp_path = task.get("shp_path", "")

    if not lake_uid or not target_date or not acolite_dir:
        raise ValueError(f"Invalid postprocess task: {task}")
    if not os.path.isdir(acolite_dir):
        raise FileNotFoundError(f"ACOLITE output directory does not exist: {acolite_dir}")

    lake_out_dir = os.path.join(FINAL_OUTPUT_DIR, lake_uid, target_date)
    os.makedirs(lake_out_dir, exist_ok=True)

    # 🌟 优先使用任务中带过来的 SHP 路径，如果没有则退化为按 UID 去配置目录里猜
    shp_path = None
    if task_shp_path and os.path.exists(task_shp_path):
        shp_path = task_shp_path
    else:
        shp_candidate = os.path.join(MASK_SHP_DIR, f"{lake_uid}.shp")
        if os.path.exists(shp_candidate):
            shp_path = shp_candidate

    if shp_path is None:
        logging.warning("No shapefile mask found for lake=%s. Using full rectangular extent output.", lake_uid)
    else:
        logging.info("Found shapefile mask for lake=%s: %s", lake_uid, shp_path)

    parameter_groups = collect_parameter_groups(acolite_dir)
    if not parameter_groups:
        raise RuntimeError(f"No geotiff parameters found in {acolite_dir}")

    # 将找到的 SHP 路径传给裁剪配置器
    options_clip = build_warp_options(shp_path)

    success_count = 0
    for parameter in sorted(parameter_groups.keys()):
        input_tifs = sorted(parameter_groups[parameter])
        vrt_path = os.path.join(acolite_dir, f"temp_{lake_uid}_{parameter}.vrt")
        final_tif = os.path.join(lake_out_dir, f"{lake_uid}_{target_date}_{parameter}.tif")

        logging.info("Mosaicking & Clipping lake=%s parameter=%s input_count=%s", lake_uid, parameter, len(input_tifs))
        vrt_ds = gdal.BuildVRT(vrt_path, input_tifs)
        if vrt_ds is None:
            raise RuntimeError(f"BuildVRT failed for parameter {parameter}")
        vrt_ds = None

        # 执行终极掩膜裁剪
        warp_ds = gdal.Warp(final_tif, vrt_path, options=options_clip)
        if warp_ds is None:
            raise RuntimeError(f"Warp failed for parameter {parameter}")
        warp_ds = None

        if os.path.exists(vrt_path):
            os.remove(vrt_path)
        success_count += 1

    if success_count == 0:
        raise RuntimeError(f"No output was generated for lake={lake_uid} date={target_date}")

    if CLEAN_ACOLITE_TEMP:
        shutil.rmtree(acolite_dir, ignore_errors=True)

    logging.info("Postprocess complete lake=%s date=%s parameter_count=%s", lake_uid, target_date, success_count)
    return success_count


def callback(ch, method, properties, body):
    try:
        task = json.loads(body)
        processed = process_gis_mosaic(task)
        logging.info("Postprocess task acknowledged. Generated %s parameters.", processed)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except RuntimeError as exc:
        message = str(exc)
        if "No geotiff parameters found" in message:
            logging.error("Postprocess drop task (no tif found, not requeue): %s", message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logging.error("Postprocess runtime error: %s", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    except FileNotFoundError as exc:
        # Upstream ACOLITE output disappeared or was not produced. Requeueing causes endless loops.
        logging.error("Postprocess drop task (missing input dir): %s", exc)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        logging.error("Postprocess failed: %s", exc)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=POSTPROCESS_QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=POSTPROCESS_QUEUE_NAME, on_message_callback=callback)

    logging.info("Postprocess worker started. Waiting for queue=%s", POSTPROCESS_QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()