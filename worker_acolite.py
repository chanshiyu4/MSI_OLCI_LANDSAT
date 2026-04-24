import glob
import json
import logging
import os
import re
import shutil
import subprocess
import traceback
from typing import Any, Dict, List

import pika

from config import (
    ACOLITE_CLI_PATH,
    ACOLITE_OUT_DIR,
    ACOLITE_PYTHON,
    POSTPROCESS_QUEUE_NAME,
    RABBITMQ_HOST,
)

ACOLITE_TASK_QUEUE = "acolite_run_queue"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - WORKER-ACOLITE - %(message)s")

CLEAN_TEMP_ON_FAILURE = os.getenv("CLEAN_TEMP_ON_FAILURE", "0").strip().lower() in {"1", "true", "yes", "on"}


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", name)


def normalize_target_date(task: Dict[str, Any]) -> str:
    value = str(task.get("target_date", "")).replace("-", "")
    if re.fullmatch(r"\d{8}", value):
        return value
    scene_name = str(task.get("scene_name", ""))
    match = re.search(r"(\d{8})", scene_name)
    return match.group(1) if match else "unknown"


def build_l2w_parameters(sensor_key: str) -> str:
    return "Rrs_*"


def run_acolite(
        input_product_dir: str,
        limit: List[float],
        lake_uid: str,
        target_date: str,
        scene_tag: str,
        sensor_key: str,
) -> str:
    lake_out_dir = os.path.join(ACOLITE_OUT_DIR, lake_uid, target_date)
    os.makedirs(lake_out_dir, exist_ok=True)

    settings_file = os.path.join(lake_out_dir, f"settings_{sanitize_filename(scene_tag)}.txt")

    # 记录完整日志的文件路径
    acolite_log_file = os.path.join(lake_out_dir, f"acolite_full_run_{sanitize_filename(scene_tag)}.log")

    # 坐标顺序已修复为 ACOLITE 规范: South, West, North, East
    limit_str = f"{limit[0]},{limit[1]},{limit[2]},{limit[3]}"
    l2w_parameters = build_l2w_parameters(sensor_key)

    is_sentinel_3 = "OLCI" in scene_tag or "S3" in scene_tag or sensor_key.upper() == "OLCI"

    with open(settings_file, "w", encoding="utf-8") as handle:
        handle.write(f"inputfile={input_product_dir}\n")
        handle.write(f"output={lake_out_dir}\n")
        handle.write(f"limit={limit_str}\n")

        # 大气校正基础设置
        handle.write("aerosol_correction=dark_spectrum\n")

        handle.write("dsf_path_reflectance=tiled\n")
        # 将云掩膜阈值从默认的 0.02 提升到 0.1，防止太湖藻类/泥沙高反被当成云抹除
        handle.write("l1_mac_th=0.1\n")
        # 只要有 0.1% 的有效水体像素，就强行计算气溶胶，不许报错退出
        handle.write("dsf_min_tile_fraction=0.001\n")
        # 输出 DSF 气溶胶计算过程参数，方便排查
        handle.write("dsf_write_tiled_parameters=True\n")

        handle.write(f"l2w_parameters={l2w_parameters}\n")
        handle.write("l2w_mask=False\n")

        if is_sentinel_3:
            handle.write("output_projection=True\n")
            handle.write("reproject_outputs=L2R,L2W\n")
            handle.write("output_projection_epsg=4326\n")
            handle.write("output_projection_resolution=0.0027,0.0027\n")
            handle.write("out_proj=EPSG:4326\n")
            handle.write("reproject_resolution=0.0027\n")
        else:
            handle.write("output_projection=True\n")
            handle.write("map_projected=True\n")

        # 强制保留所有中间文件用于排查
        handle.write("l1r_nc_delete=False\n")
        handle.write("l2r_nc_delete=False\n")
        handle.write("l2w_nc_delete=False\n")
        handle.write("l2r_export_geotiff=True\n")
        handle.write("l2w_export_geotiff=True\n")

    # 环境变量彻底重建
    acolite_env_dir = os.path.dirname(ACOLITE_PYTHON)
    custom_env = os.environ.copy()

    for key in ['GDAL_DATA', 'GDAL_DRIVER_PATH', 'PROJ_LIB', 'PROJ_DATA', 'PROJ_DEBUG']:
        if key in custom_env:
            del custom_env[key]

    acolite_paths = [
        acolite_env_dir,
        os.path.join(acolite_env_dir, "Library", "mingw-w64", "bin"),
        os.path.join(acolite_env_dir, "Library", "usr", "bin"),
        os.path.join(acolite_env_dir, "Library", "bin"),
        os.path.join(acolite_env_dir, "Scripts"),
    ]
    custom_env["PATH"] = os.pathsep.join(acolite_paths) + os.pathsep + custom_env.get("PATH", "")

    custom_env["PROJ_LIB"] = os.path.join(acolite_env_dir, "Library", "share", "proj")
    custom_env["GDAL_DATA"] = os.path.join(acolite_env_dir, "Library", "share", "gdal")
    custom_env["GDAL_DRIVER_PATH"] = os.path.join(acolite_env_dir, "Library", "lib", "gdalplugins")

    before_tifs = set(glob.glob(os.path.join(lake_out_dir, "**", "*.tif"), recursive=True))

    logging.info("Calling ACOLITE subprocess for scene: %s", scene_tag)

    # 运行 ACOLITE
    result = subprocess.run(
        [ACOLITE_PYTHON, ACOLITE_CLI_PATH, "--cli", f"--settings={settings_file}"],
        env=custom_env,
        capture_output=True,
        text=True,
        check=False
    )

    # 🌟 核心日志增强：把 ACOLITE 所有的标准输出和错误输出无删减写入文件
    with open(acolite_log_file, "w", encoding="utf-8") as f:
        f.write("=== ACOLITE SETTINGS ===\n")
        with open(settings_file, "r") as sf:
            f.write(sf.read())
        f.write("\n=== STDOUT ===\n")
        f.write(result.stdout or "No stdout")
        f.write("\n=== STDERR ===\n")
        f.write(result.stderr or "No stderr")
        f.write("\n=== RETURN CODE ===\n")
        f.write(str(result.returncode))

    if result.returncode != 0:
        err_msg = (result.stderr or "")[-500:]
        raise RuntimeError(
            f"ACOLITE crashed! Return code {result.returncode}. Check full log at: {acolite_log_file}\nTail error: {err_msg}")

    after_tifs = set(glob.glob(os.path.join(lake_out_dir, "**", "*.tif"), recursive=True))
    created_tifs = after_tifs - before_tifs

    if not created_tifs:
        stdout_tail = (result.stdout or "")[-500:]
        raise RuntimeError(
            f"ACOLITE finished but NO TIF was produced. Check full log at: {acolite_log_file}\nTail info: {stdout_tail}")

    return lake_out_dir


def cleanup_temp_files(extract_path: str, archive_path: str) -> None:
    if extract_path and os.path.exists(extract_path):
        shutil.rmtree(extract_path, ignore_errors=True)
    if archive_path and os.path.exists(archive_path):
        try:
            os.remove(archive_path)
        except:
            pass


def callback(ch, method, properties, body):
    MAX_RETRIES = 3
    headers = properties.headers or {}
    retry_count = headers.get('retry_count', 0)

    try:
        task = json.loads(body)
        input_dir = task.get("input_product_dir")
        lakes = task.get("lakes") or []
        scene_name = task.get("scene_name")
        sensor_key = task.get("sensor_key")
        target_date = normalize_target_date(task)

        if not input_dir or not os.path.exists(input_dir):
            raise FileNotFoundError(f"Input product directory not found: {input_dir}")

        process_success = True
        try:
            for lake in lakes:
                logging.info("Processing ACOLITE: Lake=%s, Scene=%s", lake['uid'], scene_name)
                out_dir = run_acolite(
                    input_product_dir=input_dir,
                    limit=lake['limit'],
                    lake_uid=lake['uid'],
                    target_date=target_date,
                    scene_tag=scene_name,
                    sensor_key=sensor_key
                )

                next_task = {
                    "scene_name": scene_name,
                    "lake_uid": lake['uid'],
                    "lake_name": lake.get('uname', lake['uid']),
                    "target_date": target_date,
                    "acolite_dir": out_dir,
                    "sensor_key": sensor_key,
                    "shp_path": task.get("shp_path", "")
                }
                ch.basic_publish(
                    exchange="",
                    routing_key=POSTPROCESS_QUEUE_NAME,
                    body=json.dumps(next_task, ensure_ascii=False),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
        except Exception as e:
            process_success = False
            raise e
        finally:
            if process_success or CLEAN_TEMP_ON_FAILURE:
                cleanup_temp_files(task.get("temp_extract_path"), task.get("temp_archive_path"))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as exc:
        logging.error("Acolite Worker failed (Attempt %d/%d): %s", retry_count + 1, MAX_RETRIES, exc)
        if retry_count < MAX_RETRIES:
            headers['retry_count'] = retry_count + 1
            ch.basic_publish(
                exchange='',
                routing_key=method.routing_key,
                body=body,
                properties=pika.BasicProperties(delivery_mode=2, headers=headers)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logging.critical("Acolite Worker permanently failed for scene: %s", body)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=3600))
    channel = connection.channel()

    channel.queue_declare(queue=ACOLITE_TASK_QUEUE, durable=True)
    channel.queue_declare(queue=POSTPROCESS_QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=ACOLITE_TASK_QUEUE, on_message_callback=callback)

    logging.info("ACOLITE worker started. Listening on queue: %s", ACOLITE_TASK_QUEUE)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()