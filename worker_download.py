import json
import logging
import os
import re
import shutil
import traceback
import zipfile
import tarfile
from typing import Any, Dict, List

import pika

# 导入配置和工具函数
from config import (
    DOWNLOAD_QUEUE_NAME,
    RABBITMQ_HOST,
    TEMP_DIR,
)

# 与后置 Worker 对接的队列
ACOLITE_TASK_QUEUE = "acolite_run_queue"

from shared_utils import download_from_copernicus, download_from_usgs

logging.basicConfig(level=logging.INFO, format="%(asctime)s - WORKER-DOWNLOAD - %(message)s")

# 环境变量：失败时是否清理临时文件
CLEAN_TEMP_ON_FAILURE = os.getenv("CLEAN_TEMP_ON_FAILURE", "0").strip().lower() in {"1", "true", "yes", "on"}

os.makedirs(TEMP_DIR, exist_ok=True)


# --- 辅助逻辑函数 ---

def sanitize_filename(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", name)


def get_archive_extension(task: Dict[str, Any]) -> str:
    ext = str(task.get("archive_ext") or "").lower().strip(".")
    if ext in {"zip", "tar", "tgz", "gz"}:
        return ext
    provider = str(task.get("provider", "")).lower()
    return "tar" if provider == "usgs" else "zip"


def download_scene_archive(task: Dict[str, Any], archive_path: str) -> str:
    provider = str(task.get("provider", "")).lower()
    if provider == "cdse":
        url = task.get("download_url")
        if not url: raise ValueError("CDSE task missing download_url")
        return download_from_copernicus(url=url, output_path=archive_path)
    if provider == "usgs":
        return download_from_usgs(
            scene_id=task.get("scene_id"),
            product_id=task.get("usgs_product_id"),
            dataset_name=task.get("usgs_dataset_name"),
            output_path=archive_path
        )
    raise ValueError(f"Unsupported provider: {provider}")


def extract_archive(archive_path: str, extract_path: str, archive_ext: str) -> None:
    if archive_ext.lower() == "zip":
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)
    else:
        with tarfile.open(archive_path, "r:*") as tar_ref:
            tar_ref.extractall(extract_path)


def resolve_input_product_dir(extract_path: str, sensor_key: str) -> str:
    # 🌟 修复 1：加回对 Landsat 数据的支持识别
    sensor = sensor_key.upper()
    if sensor == "LANDSAT":
        mtl_candidates: List[str] = []
        for root, _, files in os.walk(extract_path):
            if any(name.upper().endswith("_MTL.TXT") for name in files):
                mtl_candidates.append(root)
        if mtl_candidates:
            return sorted(mtl_candidates, key=len)[0]
        return extract_path

    # 查找 Sentinel 数据的 .SAFE 或 .SEN3 目录
    for root, dirs, _ in os.walk(extract_path):
        for name in dirs:
            if name.lower().endswith((".safe", ".sen3")):
                return os.path.join(root, name)
    return extract_path


# --- 核心业务逻辑 ---

def process_only_download(task: Dict[str, Any], channel) -> str:
    scene_name = str(task.get("scene_name") or "")
    sensor_key = str(task.get("sensor_key") or "")
    scene_id = str(task.get("scene_id") or "")

    archive_ext = get_archive_extension(task)
    archive_suffix = ".zip" if archive_ext == "zip" else ".tar"
    safe_scene_name = sanitize_filename(scene_name)
    archive_path = os.path.join(TEMP_DIR, safe_scene_name + archive_suffix)

    # 🌟 修复 2：彻底解决 Windows 长路径与文件夹命名冲突问题，采用唯一 scene_id
    unique_suffix = sanitize_filename(scene_id) if scene_id else safe_scene_name[-15:]
    extract_path = os.path.join(TEMP_DIR, f"ext_{unique_suffix}")
    success_marker = os.path.join(extract_path, ".extraction_success")

    # 1. 下载逻辑
    if not os.path.exists(archive_path):
        logging.info("Downloading provider=%s scene=%s", task.get("provider"), scene_name)
        download_scene_archive(task, archive_path)

    # 2. 解压逻辑（带标志文件验证）
    if not os.path.exists(success_marker):
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path, ignore_errors=True)
        os.makedirs(extract_path, exist_ok=True)
        logging.info("Extracting %s...", scene_name)
        extract_archive(archive_path, extract_path, archive_ext)
        with open(success_marker, "w") as f:
            f.write("ok")

    # 3. 确定最终产品目录
    input_product_dir = resolve_input_product_dir(extract_path, sensor_key)

    # 4. 构建发送给 Acolite Worker 的新任务
    # 巧妙之处：利用 copy()，Producer 新加进来的 "shp_path" 会被原封不动地传给下一站
    acolite_task = task.copy()
    acolite_task["input_product_dir"] = input_product_dir
    acolite_task["temp_extract_path"] = extract_path  # 传递路径以便后续清理
    acolite_task["temp_archive_path"] = archive_path

    # 发送到 ACOLITE 计算队列
    channel.basic_publish(
        exchange="",
        routing_key=ACOLITE_TASK_QUEUE,
        body=json.dumps(acolite_task, ensure_ascii=False),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    return scene_name


def callback(ch, method, properties, body):
    # 引入重试机制
    MAX_RETRIES = 3
    headers = properties.headers or {}
    retry_count = headers.get('retry_count', 0)

    try:
        task = json.loads(body)
        scene = process_only_download(task, ch)
        logging.info("Download & Extraction successful: %s", scene)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        logging.error("Download failed (Attempt %d/%d): %s", retry_count + 1, MAX_RETRIES, exc)
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
            logging.critical("Download permanently failed: %s", body)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=3600))
    channel = connection.channel()

    # 声明输入和输出队列
    channel.queue_declare(queue=DOWNLOAD_QUEUE_NAME, durable=True)
    channel.queue_declare(queue=ACOLITE_TASK_QUEUE, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=DOWNLOAD_QUEUE_NAME, on_message_callback=callback)

    logging.info("DOWNLOAD worker started. Monitoring: %s", DOWNLOAD_QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()