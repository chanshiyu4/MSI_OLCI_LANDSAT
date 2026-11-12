import hashlib
import json
import re
import shutil
from pathlib import Path

from filelock import FileLock

from config import ACOLITE_CLI_PATH, ACOLITE_EXPORT_L2R, ACOLITE_KEEP_NETCDF, MASK_SHP_DIR

PARAMETER_PATTERN = re.compile(r"(Rrs_\d+|rhos_\d+|rhot_\d+|rhorc_\d+|fai)$", re.IGNORECASE)
COMPLETE_MARKER = ".pipeline-complete.json"


def sanitize_filename(value):
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def scene_key(task):
    identity = str(task.get("scene_id") or task.get("scene_name") or "")
    if not identity:
        raise ValueError("Task has no scene identity")
    safe = sanitize_filename(identity).strip(".")
    if safe == identity and len(safe) <= 48 and not Path(safe).is_reserved():
        return safe
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]


def target_date(task):
    date = str(task.get("target_date") or "").replace("-", "")
    if re.fullmatch(r"[0-9]{8}", date):
        return date
    match = re.search(r"[0-9]{8}", str(task.get("scene_name") or ""))
    return match.group() if match else "unknown"


def contained_path(root, path):
    root = Path(root).resolve()
    path = Path(path).absolute()
    resolved = path.resolve()
    if resolved == root or not resolved.is_relative_to(root) or resolved != path:
        raise ValueError(f"Expected a direct path below {root}: {path}")
    return path


def remove_work_dir(root, directory):
    path = contained_path(root, directory)
    if path.exists():
        shutil.rmtree(path)


def directory_lock(directory):
    directory = Path(directory)
    directory.parent.mkdir(parents=True, exist_ok=True)
    return FileLock(str(directory) + ".lock")


def product_dir(root, task):
    lake = str(task["lake_uid"])
    if lake in ("", ".", "..") or any(char in lake for char in '/\\:<>|?*"'):
        raise ValueError(f"Invalid lake UID: {lake!r}")
    sensor = sanitize_filename(str(task.get("sensor_key") or "unknown").upper()).strip(".") or "UNKNOWN"
    root = Path(root).resolve()
    return contained_path(root, root / lake / target_date(task) / sensor / scene_key(task))


def postprocess_task(scene, lake):
    cli = Path(ACOLITE_CLI_PATH).resolve()
    revision = _signature([
        1, lake["limit"], ACOLITE_EXPORT_L2R, ACOLITE_KEEP_NETCDF, str(cli), _file_stamp(cli),
    ])
    return {
        "scene_name": scene.get("scene_name"),
        "scene_id": scene.get("scene_id"),
        "sensor_key": scene.get("sensor_key"),
        "lake_uid": lake["uid"],
        "lake_name": lake.get("uname", lake["uid"]),
        "target_date": target_date(scene),
        "shp_path": lake.get("shp_path") or scene.get("shp_path") or "",
        "revision": revision,
    }


def _file_stamp(path):
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return [stat.st_size, stat.st_mtime_ns]


def _signature(parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode("utf-8")).hexdigest()


def mask_path(task):
    explicit = task.get("shp_path")
    fallback = Path(MASK_SHP_DIR) / (task["lake_uid"] + ".shp")
    for candidate in ([Path(explicit), fallback] if explicit else [fallback]):
        if candidate.is_file():
            return candidate.resolve()
    if explicit:
        raise FileNotFoundError(f"Shapefile not found for {task['lake_uid']}: {explicit}")
    return None


def final_revision(task, mask):
    sidecars = (".shp", ".shx", ".dbf", ".prj", ".cpg")
    stamps = [_file_stamp(mask.with_suffix(suffix)) for suffix in sidecars] if mask else []
    return _signature([task.get("revision"), str(mask), stamps])


def completed_files(directory, revision=None):
    try:
        metadata = json.loads((directory / COMPLETE_MARKER).read_text(encoding="utf-8"))
        if (revision is not None and metadata["revision"] != revision) or not metadata["files"]:
            return []
        files = [contained_path(directory, directory / name) for name in metadata["files"]]
        return files if all(path.is_file() and path.stat().st_size for path in files) else []
    except (FileNotFoundError, ValueError, KeyError, TypeError):
        return []


def mark_complete(directory, revision, files):
    marker = directory / COMPLETE_MARKER
    temporary = marker.with_suffix(".tmp")
    metadata = {"revision": revision, "files": [path.relative_to(directory).as_posix() for path in files]}
    temporary.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    temporary.replace(marker)
