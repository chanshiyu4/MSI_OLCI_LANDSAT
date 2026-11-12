import io
import sys
import tarfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pika
import pytest
from osgeo import gdal, ogr, osr

import monitor_producer
import pipeline_queue
import shared_utils
import worker_acolite
import worker_download
import worker_postprocess
from config import SENSORS
from pipeline_files import mark_complete, product_dir
from providers import cdse_provider, usgs_provider


class DownloadResponse:
    def __init__(self, status, headers, data=b""):
        self.status_code = status
        self.headers = headers
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_content(self, chunk_size):
        yield self.data


@pytest.mark.parametrize(
    "response, expected, restarts",
    [
        (DownloadResponse(206, {"Content-Range": "bytes 3-5/6", "ETag": '"v1"'}, b"def"), b"abcdef", False),
        (DownloadResponse(416, {"Content-Range": "bytes */3"}), b"abc", False),
        (DownloadResponse(206, {"Content-Range": "bytes 2-5/6"}, b"cdef"), b"updated", True),
        (DownloadResponse(200, {"Content-Length": "7"}, b"updated"), b"updated", False),
    ],
)
def test_resume_checks_range_and_final_size(monkeypatch, tmp_path, response, expected, restarts):
    target = tmp_path / "scene.zip"
    target.with_suffix(".zip.part").write_bytes(b"abc")
    target.with_suffix(".zip.part.validator").write_text('"v1"', encoding="utf-8")
    responses = [response, DownloadResponse(200, {"Content-Length": "7"}, b"updated")]
    session = Mock()
    session.get.side_effect = responses
    monkeypatch.setattr(shared_utils, "http_session", lambda: session)
    monkeypatch.setattr(shared_utils.time, "sleep", Mock())

    shared_utils.download_file("https://example.test/scene", str(target))
    assert target.read_bytes() == expected
    headers = session.get.call_args_list[0].kwargs["headers"]
    assert (headers["Range"], headers["If-Range"]) == ("bytes=3-", '"v1"')
    assert session.get.call_count == (2 if restarts else 1)
    if restarts:
        assert "Range" not in session.get.call_args.kwargs["headers"]


def test_truncated_download_stays_partial(monkeypatch, tmp_path):
    target = tmp_path / "scene.zip"
    session = Mock()
    session.get.return_value = DownloadResponse(200, {"Content-Length": "6"}, b"abc")
    monkeypatch.setattr(shared_utils, "http_session", lambda: session)
    with pytest.raises(shared_utils.IncompleteDownload):
        shared_utils.download_file("https://example.test/scene", str(target), max_retries=1)
    assert not target.exists()
    assert target.with_suffix(".zip.part").read_bytes() == b"abc"


def test_download_refreshes_unauthorized_token_immediately(monkeypatch, tmp_path):
    session = Mock()
    session.get.side_effect = [DownloadResponse(401, {}), DownloadResponse(200, {"Content-Length": "3"}, b"zip")]
    get_token = Mock(return_value="new")
    sleep = Mock()
    monkeypatch.setattr(shared_utils, "http_session", lambda: session)
    monkeypatch.setattr(shared_utils, "get_cdse_access_token", get_token)
    monkeypatch.setattr(shared_utils.time, "sleep", sleep)
    shared_utils.download_from_copernicus("https://example.test/scene", str(tmp_path / "scene.zip"), token="old")
    get_token.assert_called_once_with(force_refresh=True)
    assert session.get.call_args.kwargs["headers"]["Authorization"] == "Bearer new"
    sleep.assert_not_called()


def test_rate_limit_uses_retry_after(monkeypatch, tmp_path):
    session = Mock()
    session.get.side_effect = [
        DownloadResponse(429, {"Retry-After": "6"}),
        DownloadResponse(200, {"Content-Length": "3"}, b"zip"),
    ]
    sleep = Mock()
    monkeypatch.setattr(shared_utils, "http_session", lambda: session)
    monkeypatch.setattr(shared_utils.time, "sleep", sleep)
    shared_utils.download_file("https://example.test/scene", str(tmp_path / "scene.zip"))
    sleep.assert_called_once_with(6)


def test_catalogue_follows_cdse_next_link(monkeypatch):
    second_page = cdse_provider.CATALOG_URL + "?%24skip=1"
    search = Mock(side_effect=[
        {"value": [{"Id": "a", "Name": "A"}], "@OData.nextLink": second_page},
        {"value": [{"Id": "b", "Name": "B"}]},
    ])
    monkeypatch.setattr(cdse_provider, "request_json", search)
    date = datetime(2026, 4, 16, tzinfo=timezone.utc)
    scenes = cdse_provider.search_cdse_scenes({"bbox": [0, 0, 1, 1]}, SENSORS[0], date, date)
    assert [scene["scene_id"] for scene in scenes] == ["a", "b"]
    assert search.call_args_list[0].kwargs["params"]["$select"] == "Id,Name,ContentDate"
    assert search.call_args.args == ("GET", second_page)
    assert search.call_args.kwargs["params"] is None


def test_usgs_pages_and_looks_up_one_bundle_per_unique_scene(monkeypatch):
    calls = []

    def request(endpoint, payload, token):
        calls.append((endpoint, payload.copy()))
        if endpoint == "scene-search":
            start = payload["startingNumber"]
            return {
                "results": [{"entityId": str(start), "displayId": f"LC08_L1TP_121038_20260416_{start}_02_T1"}],
                "nextRecord": 2 if start == 1 else 0,
            }
        return [
            {"entityId": "1", "id": "band", "productName": "Band 1", "available": True},
            {"entityId": "1", "id": "bundle-1", "productName": "Level-1 Product Bundle", "available": True},
            {"entityId": "2", "id": "bundle-2", "productName": "Level-1 Product Bundle", "available": True},
        ]

    monkeypatch.setattr(usgs_provider, "get_usgs_access_token", lambda: "test")
    monkeypatch.setattr(usgs_provider, "usgs_send_request", request)
    date = datetime(2026, 4, 16, tzinfo=timezone.utc)
    lakes = [{"uid": uid, "bbox": [0, 0, 1, 1]} for uid in ("A", "B")]
    scenes, errors = monitor_producer._collect_scenes("usgs", [SENSORS[2]], lakes, date, date)

    assert not errors
    assert [payload["startingNumber"] for endpoint, payload in calls if endpoint == "scene-search"] == [1, 2, 1, 2]
    assert [payload["entityIds"] for endpoint, payload in calls if endpoint == "download-options"] == [["1", "2"]]
    assert [(scene["usgs_product_id"], scene["target_date"]) for scene in scenes] == [
        ("bundle-1", "20260416"), ("bundle-2", "20260416")
    ]
    assert all(len(scene["lakes"]) == 2 for scene in scenes)


@pytest.mark.parametrize("previous, destination", [(0, "in"), (pipeline_queue.MAX_RETRIES, "in.failed")])
def test_retry_and_failure_messages_are_confirmed_before_ack(previous, destination):
    def process(_):
        yield {"downstream": True}
        raise ValueError("simulated processing failure")

    method = SimpleNamespace(delivery_tag=1)
    props = pika.BasicProperties(headers={"retry_count": previous})
    channel = Mock(is_closed=False)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pipeline_queue._handle_delivery(Mock(), channel, pool, "in", "out", process, method, props, b"{}")
    sent, retry, ack = channel.mock_calls
    assert sent.kwargs["routing_key"] == "out"
    assert retry.kwargs["routing_key"] == destination and retry.kwargs["mandatory"]
    assert ack[0] == "basic_ack"


def test_long_work_pumps_connection_and_publish_failure_leaves_delivery_unacked():
    heartbeat = Event()

    def process(_):
        assert heartbeat.wait(timeout=2)
        yield {"done": True}

    connection = Mock()
    connection.process_data_events.side_effect = lambda **_: heartbeat.set()
    channel = Mock(is_closed=False)
    channel.basic_publish.side_effect = pika.exceptions.NackError([])
    with ThreadPoolExecutor(max_workers=1) as pool, pytest.raises(pika.exceptions.NackError):
        pipeline_queue._handle_delivery(connection, channel, pool, "in", "out", process,
                                        SimpleNamespace(delivery_tag=1), pika.BasicProperties(), b"{}")
    assert heartbeat.is_set()
    channel.basic_ack.assert_not_called()


def test_stream_extracts_landsat_bundle(tmp_path):
    archive = tmp_path / "scene.tar"
    extract = tmp_path / "raw"
    with tarfile.open(archive, "w") as handle:
        info = tarfile.TarInfo("landsat/LC09_TEST_MTL.TXT")
        info.size = 8
        handle.addfile(info, io.BytesIO(b"metadata"))
    worker_download.extract_archive(archive, extract, "tar")
    assert worker_download.resolve_input_product_dir(extract, "LANDSAT") == extract / "landsat"


def test_acolite_retry_only_recomputes_failed_lake(monkeypatch, tmp_path):
    raw, intermediate, final = (tmp_path / name for name in ("raw", "intermediate", "final"))
    input_dir = raw / "ext_example"
    input_dir.mkdir(parents=True)
    monkeypatch.setattr(worker_download, "TEMP_DIR", str(raw))
    monkeypatch.setattr(worker_acolite, "TEMP_DIR", str(raw))
    monkeypatch.setattr(worker_acolite, "ACOLITE_OUT_DIR", str(intermediate))
    monkeypatch.setattr(worker_acolite, "FINAL_OUTPUT_DIR", str(final))
    monkeypatch.setattr(worker_acolite, "KEEP_RAW_CACHE", False)
    monkeypatch.setattr(worker_acolite, "mask_path", lambda job: None)
    calls = []

    def run_acolite(input_dir, limit, job, output_dir):
        lake = job["lake_uid"]
        calls.append(lake)
        if lake == "B" and calls.count("B") == 1:
            raise RuntimeError("interrupted correction")
        output_dir.mkdir()
        tif = output_dir / "sample_L2W_Rrs_443.tif"
        tif.write_bytes(b"test raster")
        mark_complete(output_dir, job["revision"], [tif])

    monkeypatch.setattr(worker_acolite, "run_acolite", run_acolite)
    scene = {"scene_name": "example", "scene_id": "example", "target_date": "20260416", "sensor_key": "MSI",
             "input_product_dir": str(input_dir),
             "lakes": [{"uid": lake, "limit": [0, 0, 1, 1]} for lake in ("A", "B")]}
    work = worker_acolite.process_acolite(scene)
    assert next(work)["lake_uid"] == "A"
    with pytest.raises(RuntimeError, match="lakes B"):
        next(work)
    assert input_dir.exists()
    assert [task["lake_uid"] for task in worker_acolite.process_acolite(scene)] == ["A", "B"]
    assert calls == ["A", "B", "B"]
    assert not input_dir.exists()


def test_postprocessing_mosaics_multiple_tiles_without_writing_a_vrt(tmp_path):
    gdal.UseExceptions()
    reference = osr.SpatialReference()
    reference.ImportFromEPSG(4326)
    inputs = []
    for x in (0, 2):
        tile = str(tmp_path / f"tile{x}.tif")
        raster = gdal.GetDriverByName("GTiff").Create(tile, 2, 2, 1, gdal.GDT_Float32)
        raster.SetGeoTransform((x, 1, 0, 2, 0, -1))
        raster.SetSpatialRef(reference)
        raster.GetRasterBand(1).Fill(x + 1)
        raster = None
        inputs.append(tile)
    result = tmp_path / "result.tif"
    worker_postprocess._warp_parameter(inputs, result, worker_postprocess.build_warp_options(None))
    mosaic = gdal.Open(str(result))
    assert mosaic.ReadAsArray().tolist() == [[1, 1, 3, 3], [1, 1, 3, 3]]
    assert not list(tmp_path.glob("*.vrt"))


def test_scene_pipeline_uses_lake_mask_and_reuses_finished_files(monkeypatch, tmp_path):
    gdal.UseExceptions()
    raw, intermediate, final = (tmp_path / name for name in ("raw", "intermediate", "final"))
    raw.mkdir()
    for worker in (worker_download, worker_acolite):
        monkeypatch.setattr(worker, "TEMP_DIR", str(raw))
    for worker in (worker_download, worker_acolite, worker_postprocess):
        monkeypatch.setattr(worker, "ACOLITE_OUT_DIR", str(intermediate))
        monkeypatch.setattr(worker, "FINAL_OUTPUT_DIR", str(final))
    monkeypatch.setattr(worker_acolite, "KEEP_RAW_CACHE", False)
    monkeypatch.setattr(worker_postprocess, "CLEAN_ACOLITE_TEMP", True)

    raster = tmp_path / "sample.tif"
    reference = osr.SpatialReference()
    reference.ImportFromEPSG(4326)
    dataset = gdal.GetDriverByName("GTiff").Create(str(raster), 4, 4, 1, gdal.GDT_Float32)
    dataset.SetGeoTransform((0, 1, 0, 4, 0, -1))
    dataset.SetSpatialRef(reference)
    data = np.ones((4, 4), dtype=np.float32)
    data[3, 0] = 0
    data[3, 1] = np.nan
    dataset.GetRasterBand(1).SetNoDataValue(np.nan)
    dataset.GetRasterBand(1).WriteArray(data)
    dataset = None

    shp_path = tmp_path / "lake.shp"
    shapes = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(str(shp_path))
    layer = shapes.CreateLayer("lake", reference, ogr.wkbPolygon)
    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetGeometry(ogr.CreateGeometryFromWkt("POLYGON ((0 0, 0 4, 4 0, 0 0))"))
    layer.CreateFeature(feature)
    shapes = None

    fake_cli = tmp_path / "acolite.py"
    fake_cli.write_text(
        "import sys, shutil\nfrom pathlib import Path\n"
        "settings = dict(line.split('=', 1) for line in "
        "Path(sys.argv[2].split('=', 1)[1]).read_text(encoding='utf-8').splitlines())\n"
        f"shutil.copyfile({str(raster)!r}, Path(settings['output']) / 'sample_L2W_Rrs_443.tif')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(worker_acolite, "ACOLITE_CLI_PATH", str(fake_cli))
    monkeypatch.setattr(worker_acolite, "ACOLITE_PYTHON", sys.executable)
    lake = {"uid": "lake", "limit": [0, 0, 4, 4], "shp_path": str(shp_path)}
    scene = {"scene_name": "example.SAFE", "scene_id": "example", "target_date": "20260416",
             "sensor_key": "MSI", "provider": "cdse", "lakes": [lake]}
    archive, extraction = worker_download.raw_scene_paths(scene)
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("example.SAFE/MTD_MSIL1C.xml", "<metadata/>")

    download = next(worker_download.prepare_download(scene.copy()))
    jobs = list(worker_acolite.process_acolite(download))
    assert len(jobs) == 1 and jobs[0]["shp_path"] == str(shp_path)
    assert not archive.exists() and not extraction.exists()
    job = jobs[0]
    assert worker_postprocess.process_gis_mosaic(job) == 1
    assert not Path(job["acolite_dir"]).exists()
    out = product_dir(final, job) / "lake_20260416_Rrs_443.tif"
    dataset = gdal.Open(str(out))
    bands = dataset.ReadAsArray()
    dataset = None
    assert bands[0, 3, 0] == 0 and bands[1, 3, 0] > 0
    assert bands[1, 3, 1] == 0 and bands[1, 0, 3] == 0
    assert product_dir(final, {**job, "sensor_key": "OLCI"}) != out.parent
    assert product_dir(final, {**job, "scene_id": "another"}) != out.parent

    replay = next(worker_download.prepare_download(scene.copy()))
    assert "input_product_dir" not in replay
    assert not list(worker_acolite.process_acolite(replay))
    assert worker_postprocess.process_gis_mosaic(job) == 1
