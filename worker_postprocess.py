import logging
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory

from osgeo import gdal

from config import ACOLITE_OUT_DIR, CLEAN_ACOLITE_TEMP, FINAL_OUTPUT_DIR, GDAL_THREADS, POSTPROCESS_QUEUE_NAME
from pipeline_files import (
    PARAMETER_PATTERN,
    completed_files,
    directory_lock,
    final_revision,
    mark_complete,
    mask_path,
    product_dir,
    remove_work_dir,
    sanitize_filename,
    target_date,
)
from pipeline_queue import run_worker


def _legacy_sources(task, directory):
    # Older messages pointed to a shared lake/date folder. Use the scene's export log when available.
    log = directory / f"acolite_full_run_{sanitize_filename(str(task.get('scene_name') or ''))}.log"
    if log.is_file():
        with log.open(encoding="utf-8", errors="replace") as handle:
            paths = {
                Path(line.strip()[6:]).resolve()
                for line in handle
                if line.startswith("Wrote ") and line.strip().lower().endswith(".tif")
            }
        if paths and all(path.is_relative_to(directory) for path in paths):
            return sorted(paths)

    files = sorted(path for path in directory.rglob("*.tif") if PARAMETER_PATTERN.search(path.stem))
    products = {path.stem.rsplit("_L2", 1)[0] for path in files}
    if len(products) > 1:
        raise ValueError(f"Scene export log required for legacy directory with multiple products: {directory}")
    return files


def collect_parameter_groups(files):
    groups = defaultdict(list)
    for path in files:
        match = PARAMETER_PATTERN.search(path.stem)
        if match:
            parameter = match.group(1).lower()
            if parameter.startswith("rrs_"):
                parameter = "Rrs_" + parameter[4:]
            groups[parameter].append(str(path))
    return groups


def build_warp_options(shp_path):
    return gdal.WarpOptions(
        format="GTiff",
        dstSRS="EPSG:4326",
        creationOptions=["COMPRESS=LZW", "TILED=YES", "BIGTIFF=IF_SAFER", f"NUM_THREADS={GDAL_THREADS}"],
        multithread=GDAL_THREADS > 1,
        warpOptions=[f"NUM_THREADS={GDAL_THREADS}"],
        cutlineDSName=str(shp_path) if shp_path else None,
        cropToCutline=bool(shp_path),
        dstAlpha=bool(shp_path),
    )


def _warp_parameter(inputs, destination, options):
    if len(inputs) == 1:
        source = inputs[0]
    else:
        source = gdal.BuildVRT("", inputs, strict=True)
        if source is None:
            raise RuntimeError(f"Could not build mosaic for {destination.name}")
    result = gdal.Warp(str(destination), source, options=options)
    if result is None:
        raise RuntimeError(f"Could not write {destination.name}")
    result.FlushCache()
    result = None


def process_gis_mosaic(task):
    source_dir = Path(task["acolite_dir"]).resolve()
    output_dir = product_dir(FINAL_OUTPUT_DIR, task)
    scene_dir = product_dir(ACOLITE_OUT_DIR, task)
    shp_path = mask_path(task)
    revision = final_revision(task, shp_path)
    with directory_lock(scene_dir):
        done = completed_files(output_dir, revision)
        if done:
            return len(done)
        previous = completed_files(output_dir)

        if not source_dir.is_dir():
            raise FileNotFoundError(f"ACOLITE output directory not found: {source_dir}")
        if task.get("revision"):
            files = completed_files(source_dir, task["revision"])
        else:
            files = _legacy_sources(task, source_dir)
        groups = collect_parameter_groups(files)
        if not groups:
            raise ValueError(f"No GeoTIFF bands found for {task.get('scene_name')} in {source_dir}")
        if shp_path is None:
            logging.warning("No mask for lake %s, using the image extent", task["lake_uid"])

        output_dir.mkdir(parents=True, exist_ok=True)
        options = build_warp_options(shp_path)
        prefix = f"{task['lake_uid']}_{target_date(task)}_"
        with TemporaryDirectory(prefix=".warp-", dir=output_dir) as temp_dir:
            outputs = []
            for parameter, inputs in sorted(groups.items()):
                destination = Path(temp_dir) / f"{prefix}{parameter}.tif"
                _warp_parameter(inputs, destination, options)
                outputs.append(destination)
            final_files = [path.replace(output_dir / path.name) for path in outputs]
            for obsolete in set(previous).difference(final_files):
                obsolete.unlink(missing_ok=True)
            mark_complete(output_dir, revision, final_files)

        if CLEAN_ACOLITE_TEMP and source_dir == scene_dir:
            try:
                remove_work_dir(ACOLITE_OUT_DIR, scene_dir)
            except OSError as exc:
                logging.warning("Could not clean ACOLITE files in %s: %s", scene_dir, exc)
        logging.info("Postprocessed %d bands in %s", len(final_files), output_dir)
        return len(final_files)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - POSTPROCESS - %(message)s")
    gdal.UseExceptions()
    run_worker(POSTPROCESS_QUEUE_NAME, process_gis_mosaic)


if __name__ == "__main__":
    main()
