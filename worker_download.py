import logging
import os
import stat
import tarfile
import time
import zipfile
import zlib
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from config import ACOLITE_OUT_DIR, ACOLITE_TASK_QUEUE, DOWNLOAD_QUEUE_NAME, FINAL_OUTPUT_DIR, TEMP_DIR
from pipeline_files import (
    completed_files,
    contained_path,
    directory_lock,
    final_revision,
    mask_path,
    postprocess_task,
    product_dir,
    remove_work_dir,
    sanitize_filename,
    scene_key,
)
from pipeline_queue import run_worker
from providers.usgs_provider import find_bundle_ids
from shared_utils import download_from_copernicus, download_from_usgs


class InvalidSceneArchive(ValueError):
    pass


def get_archive_extension(task):
    extension = str(task.get("archive_ext") or "").lower().strip(".")
    if extension in {"zip", "tar", "tgz", "gz"}:
        return extension
    return "tar" if task.get("provider") == "usgs" else "zip"


def raw_scene_paths(task):
    root = Path(TEMP_DIR).resolve()
    key = scene_key(task)
    name = sanitize_filename(str(task.get("scene_name") or key)).strip(".")
    if not name or len(name) > 160 or Path(name).is_reserved():
        name = key
    suffix = ".zip" if get_archive_extension(task) == "zip" else ".tar"
    archive = contained_path(root, root / (name + suffix))
    extraction = contained_path(root, root / f"ext_{key}")
    return archive, extraction


def download_scene_archive(task, archive_path):
    provider = task.get("provider")
    if provider == "cdse":
        url = task.get("download_url") or ""
        endpoint = urlsplit(url)
        if endpoint.scheme != "https" or endpoint.hostname not in {
            "zipper.dataspace.copernicus.eu", "download.dataspace.copernicus.eu",
        }:
            raise ValueError("Expected a CDSE HTTPS download URL")
        return download_from_copernicus(url, str(archive_path))
    if provider == "usgs":
        scene_id = task.get("scene_id")
        dataset = task.get("usgs_dataset_name")
        product = task.get("usgs_product_id")
        if not product and scene_id and dataset:
            product = find_bundle_ids(dataset, [scene_id]).get(scene_id)
        return download_from_usgs(scene_id, product, dataset, str(archive_path))
    raise ValueError(f"Unsupported provider: {provider}")


def _validate_member(name):
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name or ":" in name:
        raise InvalidSceneArchive(f"Unsafe archive member: {name}")


def extract_archive(archive_path, extract_path, archive_ext):
    if archive_ext == "zip":
        with zipfile.ZipFile(archive_path) as archive:
            for member in archive.infolist():
                _validate_member(member.filename)
                if stat.S_ISLNK(member.external_attr >> 16):
                    raise InvalidSceneArchive(f"Archive link is unsupported: {member.filename}")
            archive.extractall(extract_path)
    else:
        extract_options = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        with tarfile.open(archive_path, "r|*") as archive:
            for member in archive:
                _validate_member(member.name)
                if not (member.isfile() or member.isdir()):
                    raise InvalidSceneArchive(f"Archive member is not a regular file or directory: {member.name}")
                archive.extract(member, extract_path, set_attrs=False, **extract_options)


def resolve_input_product_dir(extract_path, sensor_key):
    sensor = str(sensor_key).upper()
    for root, _, files in os.walk(extract_path):
        names = {name.upper() for name in files}
        if sensor == "LANDSAT" and any(name.endswith("_MTL.TXT") for name in names):
            return Path(root)
        if sensor == "MSI" and ("MTD_MSIL1C.XML" in names or (names and root.lower().endswith(".safe"))):
            return Path(root)
        if sensor == "OLCI" and ("XFDUMANIFEST.XML" in names or (names and root.lower().endswith(".sen3"))):
            return Path(root)
    raise InvalidSceneArchive(f"No {sensor} product found in {extract_path}")


def ensure_scene_input(task, archive_path, extract_path):
    marker = extract_path / ".extraction_success"
    if marker.is_file():
        product = marker.read_text(encoding="utf-8").strip()
        if product == "ok":  # Old cache markers did not record the product folder.
            try:
                return resolve_input_product_dir(extract_path, task["sensor_key"])
            except InvalidSceneArchive:
                pass
        elif product:
            product_dir = (extract_path / product).resolve()
            if product_dir.is_relative_to(extract_path) and product_dir.is_dir():
                return product_dir

    remove_work_dir(TEMP_DIR, extract_path)
    if not archive_path.is_file():
        started = time.perf_counter()
        download_scene_archive(task, archive_path)
        duration = time.perf_counter() - started
        size_mib = archive_path.stat().st_size / (1024 * 1024)
        logging.info("Downloaded %s in %.2fs (%.1f MiB, %.1f MiB/s)",
                     archive_path.name, duration, size_mib, size_mib / max(duration, 0.001))

    started = time.perf_counter()
    extract_path.mkdir(parents=True, exist_ok=True)
    try:
        extract_archive(archive_path, extract_path, get_archive_extension(task))
        product = resolve_input_product_dir(extract_path, task["sensor_key"])
    except (InvalidSceneArchive, zipfile.BadZipFile, tarfile.TarError, EOFError, zlib.error):
        archive_path.unlink(missing_ok=True)
        raise
    marker.write_text(product.relative_to(extract_path).as_posix(), encoding="utf-8")
    logging.info("Extracted %s in %.2fs", archive_path.name, time.perf_counter() - started)
    return product


def _lake_has_intermediate_or_final(task, lake):
    job = postprocess_task(task, lake)
    if completed_files(product_dir(FINAL_OUTPUT_DIR, job), final_revision(job, mask_path(job))):
        return True
    return bool(completed_files(product_dir(ACOLITE_OUT_DIR, job), job["revision"]))


def prepare_download(task):
    lakes = task["lakes"]
    if not lakes:
        raise ValueError("Download task has no lakes")
    archive_path, extract_path = raw_scene_paths(task)
    if not all(_lake_has_intermediate_or_final(task, lake) for lake in lakes):
        with directory_lock(archive_path):
            product = ensure_scene_input(task, archive_path, extract_path)
        task["input_product_dir"] = str(product)
        task["temp_extract_path"] = str(extract_path)
        task["temp_archive_path"] = str(archive_path)
    else:
        logging.info("Reusing processed scene %s", task.get("scene_name"))
    yield task


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - DOWNLOAD - %(message)s")
    run_worker(DOWNLOAD_QUEUE_NAME, prepare_download, ACOLITE_TASK_QUEUE)


if __name__ == "__main__":
    main()
