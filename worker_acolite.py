import logging
import os
import subprocess
from pathlib import Path

from config import (
    ACOLITE_CLI_PATH,
    ACOLITE_EXPORT_L2R,
    ACOLITE_KEEP_NETCDF,
    ACOLITE_OUT_DIR,
    ACOLITE_PYTHON,
    ACOLITE_TASK_QUEUE,
    FINAL_OUTPUT_DIR,
    KEEP_RAW_CACHE,
    POSTPROCESS_QUEUE_NAME,
    TEMP_DIR,
)
from pipeline_files import (
    PARAMETER_PATTERN,
    completed_files,
    directory_lock,
    final_revision,
    mark_complete,
    mask_path,
    postprocess_task,
    product_dir,
    remove_work_dir,
)
from pipeline_queue import run_worker
from worker_download import ensure_scene_input, raw_scene_paths


def _acolite_environment(python):
    env = os.environ.copy()
    for name in ("GDAL_DATA", "GDAL_DRIVER_PATH", "PROJ_LIB", "PROJ_DATA", "PROJ_DEBUG"):
        env.pop(name, None)
    env["PYTHONUTF8"] = "1"
    if os.name == "nt":
        prefix = python.parent
        paths = [
            prefix, prefix / "Library" / "mingw-w64" / "bin", prefix / "Library" / "usr" / "bin",
            prefix / "Library" / "bin", prefix / "Scripts",
        ]
        env["PATH"] = os.pathsep.join([*(str(path) for path in paths), env.get("PATH", "")])
        for name, directory in (
            ("PROJ_LIB", "share/proj"), ("GDAL_DATA", "share/gdal"), ("GDAL_DRIVER_PATH", "lib/gdalplugins"),
        ):
            path = prefix / "Library" / directory
            if path.is_dir():
                env[name] = str(path)
    return env


def run_acolite(input_dir, limit, job, output_dir):
    python = Path(ACOLITE_PYTHON).resolve()
    cli = Path(ACOLITE_CLI_PATH).resolve()
    for path in (python, cli):
        if not path.is_file():
            raise FileNotFoundError(f"ACOLITE executable or launcher not found: {path}")

    remove_work_dir(ACOLITE_OUT_DIR, output_dir)
    output_dir.mkdir(parents=True)
    settings = {
        "inputfile": str(input_dir),
        "output": str(output_dir),
        "limit": ",".join(map(str, limit)),
        "aerosol_correction": "dark_spectrum",
        "dsf_path_reflectance": "tiled",
        "l1_mac_th": 0.1,
        "dsf_min_tile_fraction": 0.001,
        "dsf_write_tiled_parameters": ACOLITE_KEEP_NETCDF,
        "l2w_parameters": "Rrs_*",
        "l2w_mask": False,
        "output_projection": True,
        "l1r_delete_netcdf": not ACOLITE_KEEP_NETCDF,
        "l2r_delete_netcdf": not ACOLITE_KEEP_NETCDF,
        "l2w_delete_netcdf": not ACOLITE_KEEP_NETCDF,
        "l2r_export_geotiff": ACOLITE_EXPORT_L2R,
        "l2w_export_geotiff": True,
    }
    if job["sensor_key"] == "OLCI":
        settings.update({
            "reproject_outputs": "L2R,L2W",
            "output_projection_epsg": 4326,
            "output_projection_resolution": "0.0027,0.0027",
            "out_proj": "EPSG:4326",
            "reproject_resolution": 0.0027,
        })
    else:
        settings["map_projected"] = True

    settings_file = output_dir / "settings.txt"
    settings_file.write_text("".join(f"{key}={value}\n" for key, value in settings.items()), encoding="utf-8")
    log_file = output_dir / "acolite.log"
    logging.info("ACOLITE lake=%s scene=%s", job["lake_uid"], job["scene_name"])
    with log_file.open("wb") as log:
        result = subprocess.run(
            [str(python), str(cli), "--cli", f"--settings={settings_file}"],
            env=_acolite_environment(python), stdout=log, stderr=subprocess.STDOUT, check=False,
        )
    if result.returncode:
        raise RuntimeError(f"ACOLITE exited with code {result.returncode}: {log_file}")

    files = sorted(path for path in output_dir.rglob("*.tif") if PARAMETER_PATTERN.search(path.stem))
    if not any("_rrs_" in path.stem.lower() for path in files):
        raise RuntimeError(f"ACOLITE produced no Rrs GeoTIFFs: {log_file}")
    mark_complete(output_dir, job["revision"], files)


def process_acolite(task):
    lakes = task["lakes"]
    if not lakes:
        raise ValueError("ACOLITE task has no lakes")
    archive_path, extract_path = raw_scene_paths(task)
    input_dir = Path(task["input_product_dir"]) if task.get("input_product_dir") else None
    failures = []
    with directory_lock(archive_path):
        for lake in lakes:
            try:
                job = postprocess_task(task, lake)
                output_dir = product_dir(ACOLITE_OUT_DIR, job)
                with directory_lock(output_dir):
                    final_dir = product_dir(FINAL_OUTPUT_DIR, job)
                    if completed_files(final_dir, final_revision(job, mask_path(job))):
                        continue
                    if not completed_files(output_dir, job["revision"]):
                        if input_dir is None or not input_dir.is_dir():
                            input_dir = ensure_scene_input(task, archive_path, extract_path)
                        run_acolite(input_dir, lake["limit"], job, output_dir)
                    job["acolite_dir"] = str(output_dir)
                yield job
            except Exception as exc:
                failures.append(str(lake.get("uid", "")))
                logging.error("ACOLITE lake=%s scene=%s: %s", lake.get("uid"), task.get("scene_name"), exc)

        if failures:
            raise RuntimeError(f"ACOLITE failed for scene {task.get('scene_name')}, lakes {', '.join(failures)}")
        if not KEEP_RAW_CACHE:
            try:
                remove_work_dir(TEMP_DIR, extract_path)
                archive_path.unlink(missing_ok=True)
            except OSError as exc:
                logging.warning("Could not clean raw scene %s: %s", task.get("scene_name"), exc)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - ACOLITE - %(message)s")
    run_worker(ACOLITE_TASK_QUEUE, process_acolite, POSTPROCESS_QUEUE_NAME)


if __name__ == "__main__":
    main()
